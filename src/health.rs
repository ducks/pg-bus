//! Seeing delivery stall. A message is delivered once every older write
//! transaction has ended, so one left open (a connection idle in a
//! transaction, a forgotten prepared transaction) holds every later message
//! back without an error anywhere. [`Bus::health`](crate::Bus::health)
//! reports what is waiting and what holds it; the listener logs a warning
//! when the wait grows past [`Config::stall_warning`](crate::Config).

use std::fmt::Write as _;
use std::time::Duration;

use sqlx::PgPool;

use crate::Error;

/// What waits behind the delivery horizon, and why.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Health {
    /// Committed messages not deliverable yet.
    pub waiting: i64,
    /// How long the oldest of them has waited, counted from the start of
    /// the transaction that wrote it.
    pub oldest_wait: Option<Duration>,
    /// The open transactions older than the newest waiting message: the
    /// ones holding it back, longest-running first (at most 10).
    pub blockers: Vec<Blocker>,
}

/// An open transaction holding delivery back.
#[derive(Debug, Clone, PartialEq)]
pub struct Blocker {
    /// The backend running it; `None` for a prepared transaction, which
    /// belongs to no connection and only ends with COMMIT or ROLLBACK
    /// PREPARED.
    pub pid: Option<i32>,
    /// A prepared transaction's global id.
    pub prepared: Option<String>,
    /// How long it has been open (prepared, for a prepared transaction).
    pub age: Option<Duration>,
    /// `pg_stat_activity.state`, e.g. "idle in transaction". Postgres hides
    /// it, like the query, from roles that may not see other sessions.
    pub state: Option<String>,
    /// The last statement it ran.
    pub query: Option<String>,
}

/// pid, prepared gid, age in seconds, state, query.
type BlockerRow = (
    Option<i32>,
    Option<String>,
    Option<f64>,
    Option<String>,
    Option<String>,
);

fn seconds(s: Option<f64>) -> Option<Duration> {
    s.filter(|s| s.is_finite() && *s >= 0.0)
        .map(Duration::from_secs_f64)
}

pub(crate) async fn health_on(pool: &PgPool, schema: &str) -> Result<Health, Error> {
    let (waiting, oldest): (i64, Option<f64>) = sqlx::query_as(&format!(
        "SELECT count(*), EXTRACT(EPOCH FROM now() - min(created_at))::float8 \
         FROM {schema}.messages WHERE xid >= pg_snapshot_xmin(pg_current_snapshot())"
    ))
    .fetch_one(pool)
    .await?;
    let mut health = Health {
        waiting,
        oldest_wait: seconds(oldest),
        blockers: Vec::new(),
    };
    if waiting == 0 {
        return Ok(health);
    }
    // (No max(xid8): PostgreSQL 13 has no such aggregate.)
    // In-progress transaction ids older than the newest waiting message,
    // matched to their sessions and prepared transactions. pg_stat_activity
    // and pg_prepared_xacts show 32-bit ids, so the 64-bit ones compare by
    // their low 32 bits.
    let rows: Vec<BlockerRow> =
        sqlx::query_as(&format!(
            "WITH snap AS (SELECT pg_current_snapshot() AS s), \
                  held AS (SELECT m.xid AS newest FROM {schema}.messages m, snap \
                           WHERE m.xid >= pg_snapshot_xmin(snap.s) \
                           ORDER BY m.xid DESC LIMIT 1), \
                  running AS (SELECT x::text::numeric % 4294967296 AS low \
                              FROM snap, held, pg_snapshot_xip(snap.s) AS x WHERE x < held.newest) \
             SELECT a.pid, NULL::text, EXTRACT(EPOCH FROM now() - a.xact_start)::float8, a.state, a.query \
             FROM running r JOIN pg_stat_activity a \
               ON a.backend_xid IS NOT NULL AND a.backend_xid::text::numeric = r.low \
             UNION ALL \
             SELECT NULL::int, p.gid, EXTRACT(EPOCH FROM now() - p.prepared)::float8, NULL::text, NULL::text \
             FROM running r JOIN pg_prepared_xacts p ON p.transaction::text::numeric = r.low \
             ORDER BY 3 DESC NULLS LAST LIMIT 10"
        ))
        .fetch_all(pool)
        .await?;
    health.blockers = rows
        .into_iter()
        .map(|(pid, prepared, age, state, query)| Blocker {
            pid,
            prepared,
            age: seconds(age),
            state,
            query,
        })
        .collect();
    Ok(health)
}

/// The warning the listener logs when the oldest waiting message has
/// waited longer than `threshold`; `None` while delivery keeps up.
pub fn stall_report(health: &Health, threshold: Duration) -> Option<String> {
    let wait = health.oldest_wait?;
    if health.waiting == 0 || wait < threshold {
        return None;
    }
    let mut report = format!(
        "pg-bus: delivery stalled: {} message(s) waiting, the oldest for {}s, behind ",
        health.waiting,
        wait.as_secs()
    );
    if health.blockers.is_empty() {
        report.push_str("an open transaction that could not be identified");
        return Some(report);
    }
    for (i, b) in health.blockers.iter().enumerate() {
        if i > 0 {
            report.push_str("; ");
        }
        match (&b.prepared, b.pid) {
            (Some(gid), _) => {
                let _ = write!(report, "prepared transaction {gid:?}");
            }
            (None, Some(pid)) => {
                let _ = write!(report, "pid {pid}");
            }
            (None, None) => report.push_str("a transaction"),
        }
        if let Some(age) = b.age {
            let _ = write!(report, " open {}s", age.as_secs());
        }
        if let Some(state) = &b.state {
            let _ = write!(report, " ({state})");
        }
        if let Some(query) = b.query.as_deref().filter(|q| !q.is_empty()) {
            let short: String = query.chars().take(80).collect();
            let _ = write!(report, ", last: {short}");
        }
    }
    Some(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocked(wait: u64) -> Health {
        Health {
            waiting: 3,
            oldest_wait: Some(Duration::from_secs(wait)),
            blockers: vec![
                Blocker {
                    pid: Some(4242),
                    prepared: None,
                    age: Some(Duration::from_secs(95)),
                    state: Some("idle in transaction".into()),
                    query: Some("UPDATE posts SET raw = $1 WHERE id = $2".into()),
                },
                Blocker {
                    pid: None,
                    prepared: Some("deploy-17".into()),
                    age: Some(Duration::from_secs(3600)),
                    state: None,
                    query: None,
                },
            ],
        }
    }

    #[test]
    fn quiet_while_delivery_keeps_up() {
        assert_eq!(
            stall_report(&Health::default(), Duration::from_secs(30)),
            None
        );
        assert_eq!(stall_report(&blocked(29), Duration::from_secs(30)), None);
    }

    #[test]
    fn names_what_holds_delivery_back() {
        let report = stall_report(&blocked(31), Duration::from_secs(30)).unwrap();
        assert_eq!(
            report,
            "pg-bus: delivery stalled: 3 message(s) waiting, the oldest for 31s, behind \
             pid 4242 open 95s (idle in transaction), last: UPDATE posts SET raw = $1 WHERE id = $2; \
             prepared transaction \"deploy-17\" open 3600s"
        );
    }

    #[test]
    fn says_so_when_the_blocker_is_hidden() {
        let mut health = blocked(40);
        health.blockers.clear();
        assert!(
            stall_report(&health, Duration::from_secs(30))
                .unwrap()
                .ends_with("behind an open transaction that could not be identified")
        );
    }
}
