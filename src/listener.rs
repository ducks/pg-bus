//! One listener per process: it reads each deliverable message once and
//! hands it to every subscriber through the in-memory feed.
//!
//! It wakes on NOTIFY (sent when a publishing transaction commits). A
//! message can also become deliverable without a notification, when an
//! older, unrelated transaction ends, so while messages wait behind the
//! horizon it looks again on a short backoff, and when idle it still looks
//! every `idle_poll` in case a notification was missed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use sqlx::postgres::PgListener;

use crate::health::{health_on, stall_report};
use crate::stats::Counters;
use crate::{Config, Position, Shared, backlog_on, pending_after};

/// Messages read per query; a full batch is followed by another at once.
const BATCH: i64 = 500;

pub(crate) async fn run(pool: PgPool, config: Config, shared: Arc<Shared>, mut cursor: Position) {
    let counters = &shared.counters;
    let mut retry = config.min_poll;
    let mut connected_before = false;
    loop {
        let mut listener = match connect(&pool, &config.schema).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("pg-bus: listener could not connect: {e}");
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(config.max_poll);
                continue;
            }
        };
        if connected_before {
            Counters::add(&counters.listener_reconnects, 1);
            tracing::debug!("pg-bus: listener reconnected");
        }
        connected_before = true;
        retry = config.min_poll;
        let mut wait = config.min_poll;
        let mut stall = StallWatch::default();
        loop {
            match drain(&pool, &config.schema, &shared, &mut cursor).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("pg-bus: listener could not read the backlog: {e}");
                    tokio::time::sleep(config.max_poll).await;
                    continue;
                }
            }
            Counters::add(&counters.listener_queries, 1);
            let pending = pending_after(&pool, &config.schema, cursor)
                .await
                .unwrap_or(true);
            if pending {
                stall.check(&pool, &config).await;
            }
            let delay = if pending {
                let d = wait;
                wait = (wait * 2).min(config.max_poll);
                d
            } else {
                wait = config.min_poll;
                config.idle_poll
            };
            match next_notification(&mut listener, delay).await {
                Ok(Woke::Notified | Woke::Timeout) => {}
                // PgListener reconnected by itself; notifications may have
                // been lost meanwhile, which the next drain covers.
                Ok(Woke::Reconnected) => {
                    Counters::add(&counters.listener_reconnects, 1);
                    tracing::debug!("pg-bus: listener reconnected");
                }
                Err(e) => {
                    tracing::warn!("pg-bus: listener connection lost: {e}");
                    break;
                }
            }
        }
    }
}

/// Rate limits for the stall warning: looks every `CHECK` while messages
/// wait, warns at most every `REPEAT`.
#[derive(Default)]
struct StallWatch {
    checked: Option<Instant>,
    warned: Option<Instant>,
}

impl StallWatch {
    const CHECK: Duration = Duration::from_secs(10);
    const REPEAT: Duration = Duration::from_secs(60);

    async fn check(&mut self, pool: &PgPool, config: &Config) {
        if self.checked.is_some_and(|c| c.elapsed() < Self::CHECK) {
            return;
        }
        self.checked = Some(Instant::now());
        let health = match health_on(pool, &config.schema).await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("pg-bus: could not check for stalls: {e}");
                return;
            }
        };
        if let Some(report) = stall_report(&health, config.stall_warning)
            && self.warned.is_none_or(|w| w.elapsed() >= Self::REPEAT)
        {
            self.warned = Some(Instant::now());
            tracing::warn!("{report}");
        }
    }
}

async fn connect(pool: &PgPool, schema: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(schema).await?;
    Ok(listener)
}

/// Sends every deliverable message after the cursor, moving it along.
/// Each goes into the recent ring before the feed: a subscriber joins the
/// feed before reading the ring, so it finds a message in one or the
/// other.
async fn drain(
    pool: &PgPool,
    schema: &str,
    shared: &Shared,
    cursor: &mut Position,
) -> Result<(), crate::Error> {
    let counters = &shared.counters;
    loop {
        let batch = backlog_on(pool, schema, *cursor, None, BATCH).await?;
        Counters::add(&counters.listener_queries, 1);
        let full = batch.len() as i64 == BATCH;
        if !batch.is_empty() {
            tracing::debug!("pg-bus: listener read {} message(s)", batch.len());
        }
        Counters::add(&counters.delivered, batch.len() as u64);
        for message in batch {
            *cursor = message.position;
            let message = Arc::new(message);
            shared
                .recent
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(message.clone());
            // No receivers is fine: nobody is subscribed in this process.
            let _ = shared.feed.send(message);
        }
        *counters
            .listener_position
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(*cursor);
        if !full {
            return Ok(());
        }
    }
}

/// Why the listener woke.
enum Woke {
    Notified,
    Timeout,
    Reconnected,
}

/// Waits for a notification or `delay`, whichever comes first.
async fn next_notification(
    listener: &mut PgListener,
    delay: Duration,
) -> Result<Woke, sqlx::Error> {
    tokio::select! {
        received = listener.try_recv() => {
            if received?.is_none() {
                return Ok(Woke::Reconnected);
            }
        }
        _ = tokio::time::sleep(delay) => return Ok(Woke::Timeout),
    }
    // One read of the backlog answers every notification already here, so
    // take them all, buffered or still unread on the socket (a zero timeout
    // polls once; try_recv is cancel-safe). Without this, a burst of N
    // commits costs N rounds of queries long after it ends.
    loop {
        while listener.next_buffered().is_some() {}
        match tokio::time::timeout(Duration::ZERO, listener.try_recv()).await {
            Ok(Ok(Some(_))) => continue,
            Ok(Ok(None)) => return Ok(Woke::Reconnected),
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(Woke::Notified),
        }
    }
}
