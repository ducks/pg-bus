//! pg-bus: a message bus on PostgreSQL.
//!
//! Messages are published inside the caller's transaction, kept in a
//! backlog table, and delivered to subscribers that resume from a cursor.
//!
//! # Ordering
//!
//! Ids come from a sequence at insert, but transactions commit in their own
//! order, so a cursor over ids alone would skip a message whose transaction
//! commits after a later one was delivered. Each message therefore records
//! the id of the transaction that wrote it (`xid8`), and a message is only
//! delivered once every older transaction has finished: below the current
//! snapshot's `xmin`. Delivery runs in `(xid, id)` order, and since nothing
//! can commit below the horizon afterwards, a [`Position`] in that order
//! never skips a committed message. The cost is that a long-running write
//! transaction anywhere in the database holds delivery back until it ends.

mod health;
mod listener;
mod recent;
mod schema;
#[cfg(feature = "axum")]
pub mod sse;
mod stats;
mod subscription;
mod trim;

use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use tokio::sync::broadcast;

pub use health::{Blocker, Health, stall_report};
pub use stats::Stats;
pub use subscription::{Item, Subscription};
pub use trim::Trim;

/// Where a subscriber is in the bus: the last message it has seen, as
/// `(transaction id, message id)`. Opaque to clients, which hand back its
/// string form (an SSE `Last-Event-ID`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    xid: u64,
    id: i64,
}

impl Position {
    /// Before every message.
    pub const START: Position = Position { xid: 0, id: 0 };
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.xid, self.id)
    }
}

impl FromStr for Position {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || Error::InvalidPosition(s.to_string());
        let (xid, id) = s.split_once('-').ok_or_else(invalid)?;
        Ok(Position {
            xid: xid.parse().map_err(|_| invalid())?,
            id: id.parse().map_err(|_| invalid())?,
        })
    }
}

/// A delivered message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub position: Position,
    pub channel: String,
    pub data: Value,
    /// `None`: every subscriber of the channel; otherwise the tags a
    /// subscriber needs at least one of.
    pub audience: Option<Vec<String>>,
}

/// Which messages a subscriber receives: those on its channels whose
/// audience is everyone or shares a tag with the subscriber's.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub channels: Vec<String>,
    pub tags: Vec<String>,
}

impl Filter {
    pub fn matches(&self, message: &Message) -> bool {
        self.channels.contains(&message.channel)
            && match &message.audience {
                None => true,
                Some(audience) => audience.iter().any(|a| self.tags.contains(a)),
            }
    }
}

#[derive(Debug)]
pub enum Error {
    Db(sqlx::Error),
    InvalidPosition(String),
    InvalidSchema(String),
    /// The bus was dropped while a subscriber waited.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db(e) => write!(f, "pg-bus: database: {e}"),
            Error::InvalidPosition(s) => write!(f, "pg-bus: not a position: {s:?}"),
            Error::InvalidSchema(s) => write!(f, "pg-bus: not a schema name: {s:?}"),
            Error::Closed => write!(f, "pg-bus: the bus was dropped"),
        }
    }
}

impl std::error::Error for Error {}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Db(e)
    }
}

/// How the bus runs.
#[derive(Debug, Clone)]
pub struct Config {
    /// The schema holding the backlog; also the NOTIFY channel's name.
    pub schema: String,
    /// Messages a subscriber may fall behind the live feed before it
    /// catches up from the backlog instead.
    pub capacity: usize,
    /// How often the listener looks again while committed messages wait
    /// behind an older transaction (the delay doubles up to `max_poll`).
    pub min_poll: Duration,
    pub max_poll: Duration,
    /// Without a notification, the listener still looks this often.
    pub idle_poll: Duration,
    /// The listener logs a warning (at most once a minute) when a committed
    /// message has waited longer than this behind an open transaction.
    pub stall_warning: Duration,
    /// Recent messages each process keeps in memory, so subscribers
    /// resuming from a recent position catch up without a query.
    pub recent: usize,
    /// How old a message must be before [`Bus::trim`] may delete it: longer
    /// than any listener could still be behind. A subscriber whose position
    /// is within a process's recent ring is caught up from memory without a
    /// gap check, which holds only if trim never deletes a message that
    /// listener has not read yet.
    pub trim_grace: Duration,
    /// The background trim: on by default with [`Trim`]'s defaults; `None`
    /// leaves trimming to calls of [`Bus::trim`].
    pub trim: Option<Trim>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            schema: "pg_bus".to_string(),
            capacity: 1024,
            min_poll: Duration::from_millis(20),
            max_poll: Duration::from_secs(1),
            idle_poll: Duration::from_secs(30),
            stall_warning: Duration::from_secs(30),
            recent: 10_000,
            trim_grace: Duration::from_secs(60),
            trim: Some(Trim::default()),
        }
    }
}

/// The bus: publishing, the backlog, and one listener per process feeding
/// its subscribers.
#[derive(Clone)]
pub struct Bus {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    config: Config,
    shared: Arc<Shared>,
    /// Where the listener started, for stats before it reads anything.
    start: Position,
    listener: tokio::task::JoinHandle<()>,
    trimmer: tokio::task::JoinHandle<()>,
}

/// What the listener and the subscriptions in one process share: the live
/// feed, the recent ring and the counters.
pub(crate) struct Shared {
    pub(crate) feed: broadcast::Sender<Arc<Message>>,
    pub(crate) recent: Mutex<recent::Recent>,
    pub(crate) counters: stats::Counters,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.listener.abort();
        self.trimmer.abort();
    }
}

/// The columns a message is read with, in [`Message`]'s order plus the
/// position. The transaction id comes out as text under its own name: an
/// output column called `xid` would be what an unqualified `ORDER BY xid`
/// sorts by, and as text "1000000" sorts before "999997".
const COLUMNS: &str = "messages.xid::text AS xid_text, messages.id, messages.channel, messages.data, messages.audience";

type Row = (String, i64, String, Value, Option<Vec<String>>);

fn message(row: Row) -> Result<Message, Error> {
    let (xid, id, channel, data, audience) = row;
    let xid = xid.parse().map_err(|_| Error::InvalidPosition(xid))?;
    Ok(Message {
        position: Position { xid, id },
        channel,
        data,
        audience,
    })
}

impl Bus {
    /// Creates the schema if needed and starts the listener.
    pub async fn start(pool: PgPool, config: Config) -> Result<Bus, Error> {
        schema::validate(&config.schema)?;
        schema::migrate(&pool, &config.schema).await?;
        let (feed, _) = broadcast::channel(config.capacity);
        let start = horizon_on(&pool).await?;
        let shared = Arc::new(Shared {
            feed,
            recent: Mutex::new(recent::Recent::new(start, config.recent)),
            counters: stats::Counters::default(),
        });
        let listener = tokio::spawn(listener::run(
            pool.clone(),
            config.clone(),
            shared.clone(),
            start,
        ));
        let trimmer = tokio::spawn(trim::run(pool.clone(), config.clone()));
        Ok(Bus {
            inner: Arc::new(Inner {
                pool,
                config,
                shared,
                start,
                listener,
                trimmer,
            }),
        })
    }

    fn schema(&self) -> &str {
        &self.inner.config.schema
    }

    /// Writes a message in the caller's transaction. It is delivered once
    /// that transaction commits, and never if it rolls back. Returns its
    /// position, which a client that made the change can resume from.
    ///
    /// The transaction cannot then use two-phase commit: publishing queues a
    /// NOTIFY, and Postgres refuses to PREPARE a transaction that has one.
    pub async fn publish(
        &self,
        conn: &mut PgConnection,
        channel: &str,
        data: &Value,
        audience: Option<&[String]>,
    ) -> Result<Position, Error> {
        let schema = self.schema();
        let (xid, id): (String, i64) = sqlx::query_as(&format!(
            "INSERT INTO {schema}.messages (channel, data, audience) VALUES ($1, $2, $3) \
             RETURNING xid::text, id"
        ))
        .bind(channel)
        .bind(data)
        .bind(audience)
        .fetch_one(&mut *conn)
        .await?;
        let xid = xid.parse().map_err(|_| Error::InvalidPosition(xid))?;
        // Postgres sends it at commit, once per transaction however many
        // messages it wrote.
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(schema)
            .execute(&mut *conn)
            .await?;
        Ok(Position { xid, id })
    }

    /// What waits behind the delivery horizon and the transactions holding
    /// it, for monitoring (see [`Health`]).
    pub async fn health(&self) -> Result<Health, Error> {
        health::health_on(&self.inner.pool, self.schema()).await
    }

    /// The position a new subscriber starts from: after every message
    /// already deliverable, so it receives what commits from now on.
    pub async fn now(&self) -> Result<Position, Error> {
        horizon_on(&self.inner.pool).await
    }

    /// Deliverable messages after `from`, oldest first, at most `limit`.
    /// A message committed a moment ago may not be here yet: it waits until
    /// every older write transaction has ended (see the crate docs).
    pub async fn backlog(
        &self,
        from: Position,
        filter: &Filter,
        limit: i64,
    ) -> Result<Vec<Message>, Error> {
        backlog_on(&self.inner.pool, self.schema(), from, Some(filter), limit).await
    }

    /// True when messages after `from` were trimmed away: the subscriber
    /// has missed some and should start over from fresh state. Not per
    /// channel: a trim on any channel counts, so a subscriber may reload
    /// when it did not need to, but never misses one it did.
    pub async fn trimmed_after(&self, from: Position) -> Result<bool, Error> {
        let schema = self.schema();
        let trimmed: Option<(String, i64)> = sqlx::query_as(&format!(
            "SELECT trimmed_xid::text, trimmed_id FROM {schema}.state"
        ))
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some((xid, id)) = trimmed else {
            return Ok(false);
        };
        let xid: u64 = xid.parse().map_err(|_| Error::InvalidPosition(xid))?;
        Ok(from < Position { xid, id })
    }

    /// Deletes delivered messages older than `max_age`, and on each channel
    /// all but the newest `keep_per_channel`. Returns how many went. Never
    /// deletes a message younger than [`Config::trim_grace`]: a listener may
    /// not have read it yet, and its subscribers would miss it unawares.
    /// Waits while another process trims (the background task, see
    /// [`Config::trim`], does not: it skips the round).
    pub async fn trim(&self, max_age: Duration, keep_per_channel: i64) -> Result<u64, Error> {
        let config = &self.inner.config;
        let deleted = trim::trim_on(
            &self.inner.pool,
            &config.schema,
            config.trim_grace,
            max_age,
            keep_per_channel,
            true,
        )
        .await?;
        Ok(deleted.unwrap_or(0))
    }

    /// Subscribes from `from` (see [`Bus::now`] for a new subscriber):
    /// the backlog first, then the live feed.
    pub fn subscribe(&self, from: Position, filter: Filter) -> Subscription {
        Subscription::new(self.clone(), from, filter)
    }

    /// This process's counters (see [`Stats`]).
    pub fn stats(&self) -> Stats {
        let shared = &self.inner.shared;
        shared
            .counters
            .snapshot(shared.feed.receiver_count(), self.inner.start)
    }

    pub(crate) fn feed(&self) -> broadcast::Receiver<Arc<Message>> {
        self.inner.shared.feed.subscribe()
    }

    pub(crate) fn counters(&self) -> &stats::Counters {
        &self.inner.shared.counters
    }

    /// The recent messages after `after`, if this process still holds all
    /// of them in memory.
    pub(crate) fn recent_after(
        &self,
        after: Position,
        filter: &Filter,
        limit: usize,
    ) -> Option<Vec<Message>> {
        self.inner
            .shared
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .after(after, filter, limit)
    }
}

/// The delivery horizon as a position: every transaction below the
/// snapshot's xmin has finished, and ids start at 1, so `(xmin, 0)` is
/// after all of their messages and before any later transaction's.
pub(crate) async fn horizon_on(pool: &PgPool) -> Result<Position, Error> {
    let xmin: String = sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text")
        .fetch_one(pool)
        .await?;
    let xid = xmin.parse().map_err(|_| Error::InvalidPosition(xmin))?;
    Ok(Position { xid, id: 0 })
}

/// Deliverable messages after `from` in `(xid, id)` order: written by
/// transactions below the snapshot's xmin, so none can commit behind them.
pub(crate) async fn backlog_on(
    pool: &PgPool,
    schema: &str,
    from: Position,
    filter: Option<&Filter>,
    limit: i64,
) -> Result<Vec<Message>, Error> {
    let filter_sql = if filter.is_some() {
        "AND channel = ANY($4) AND (audience IS NULL OR audience && $5)"
    } else {
        ""
    };
    let sql = format!(
        "SELECT {COLUMNS} FROM {schema}.messages \
         WHERE (xid, id) > ($1::text::xid8, $2) \
           AND xid < pg_snapshot_xmin(pg_current_snapshot()) {filter_sql} \
         ORDER BY messages.xid, messages.id LIMIT $3"
    );
    let mut q = sqlx::query_as::<_, Row>(&sql)
        .bind(from.xid.to_string())
        .bind(from.id)
        .bind(limit);
    if let Some(f) = filter {
        q = q.bind(&f.channels).bind(&f.tags);
    }
    q.fetch_all(pool).await?.into_iter().map(message).collect()
}

/// Whether any message after `from` is still waiting on an older
/// transaction.
pub(crate) async fn pending_after(
    pool: &PgPool,
    schema: &str,
    from: Position,
) -> Result<bool, Error> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.messages WHERE (xid, id) > ($1::text::xid8, $2))"
    ))
    .bind(from.xid.to_string())
    .bind(from.id)
    .fetch_one(pool)
    .await?)
}
