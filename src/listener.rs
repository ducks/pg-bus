//! One listener per process: it reads each deliverable message once and
//! hands it to every subscriber through the in-memory feed.
//!
//! It wakes on NOTIFY (sent when a publishing transaction commits). A
//! message can also become deliverable without a notification, when an
//! older, unrelated transaction ends, so while messages wait behind the
//! horizon it looks again on a short backoff, and when idle it still looks
//! every `idle_poll` in case a notification was missed.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::broadcast;

use crate::{Config, Message, Position, backlog_on, pending_after};

/// Messages read per query; a full batch is followed by another at once.
const BATCH: i64 = 500;

pub(crate) async fn run(
    pool: PgPool,
    config: Config,
    feed: broadcast::Sender<Arc<Message>>,
    mut cursor: Position,
) {
    let mut retry = config.min_poll;
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
        retry = config.min_poll;
        let mut wait = config.min_poll;
        loop {
            match drain(&pool, &config.schema, &feed, &mut cursor).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("pg-bus: listener could not read the backlog: {e}");
                    tokio::time::sleep(config.max_poll).await;
                    continue;
                }
            }
            let pending = pending_after(&pool, &config.schema, cursor)
                .await
                .unwrap_or(true);
            let delay = if pending {
                let d = wait;
                wait = (wait * 2).min(config.max_poll);
                d
            } else {
                wait = config.min_poll;
                config.idle_poll
            };
            match next_notification(&mut listener, delay).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("pg-bus: listener connection lost: {e}");
                    break;
                }
            }
        }
    }
}

async fn connect(pool: &PgPool, schema: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(schema).await?;
    Ok(listener)
}

/// Sends every deliverable message after the cursor, moving it along.
async fn drain(
    pool: &PgPool,
    schema: &str,
    feed: &broadcast::Sender<Arc<Message>>,
    cursor: &mut Position,
) -> Result<(), crate::Error> {
    loop {
        let batch = backlog_on(pool, schema, *cursor, None, BATCH).await?;
        let full = batch.len() as i64 == BATCH;
        for message in batch {
            *cursor = message.position;
            // No receivers is fine: nobody is subscribed in this process.
            let _ = feed.send(Arc::new(message));
        }
        if !full {
            return Ok(());
        }
    }
}

/// Waits for a notification or `delay`, whichever comes first. A
/// reconnect (PgListener does it itself) wakes it too, since
/// notifications may have been lost meanwhile.
async fn next_notification(listener: &mut PgListener, delay: Duration) -> Result<(), sqlx::Error> {
    tokio::select! {
        received = listener.try_recv() => received.map(|_| ()),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}
