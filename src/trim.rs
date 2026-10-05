//! Keeping the backlog bounded: by age, and to the newest messages on each
//! channel, by a background task or on demand.
//!
//! The query never ranks the whole table. It walks the distinct channels
//! through the (channel, xid, id) index, one jump per channel (a loose
//! index scan), finds each channel's cutoff with one more index lookup,
//! and deletes up to it; the age limit uses the created_at index. The
//! count and the newest deleted position come back from SQL, not every
//! deleted row.

use std::time::Duration;

use sqlx::PgPool;

use crate::{Config, Error, Position};

/// How the background task trims: every `every`, deleting messages older
/// than `max_age` and all but the newest `keep_per_channel` on each
/// channel. MessageBus's defaults: 7 days, 1000 per channel.
#[derive(Debug, Clone, PartialEq)]
pub struct Trim {
    pub every: Duration,
    pub max_age: Duration,
    pub keep_per_channel: i64,
}

impl Default for Trim {
    fn default() -> Self {
        Trim {
            every: Duration::from_secs(60),
            max_age: Duration::from_secs(7 * 24 * 3600),
            keep_per_channel: 1000,
        }
    }
}

/// Trims once. With `wait`, waits for another process's trim to finish;
/// without, returns `None` when one is running.
pub(crate) async fn trim_on(
    pool: &PgPool,
    schema: &str,
    grace: Duration,
    max_age: Duration,
    keep_per_channel: i64,
    wait: bool,
) -> Result<Option<u64>, Error> {
    let mut tx = pool.begin().await?;
    let key = format!("pg-bus trim:{schema}");
    if wait {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(&key)
            .execute(&mut *tx)
            .await?;
    } else {
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
            .bind(&key)
            .fetch_one(&mut *tx)
            .await?;
        if !locked {
            return Ok(None);
        }
    }
    // Every reference to the deleted rows' xid is qualified: the text form
    // selected at the end would otherwise be what ORDER BY sorts.
    let (count, xid, id): (i64, Option<String>, Option<i64>) = sqlx::query_as(&format!(
        "WITH RECURSIVE channels(channel) AS ( \
             (SELECT channel FROM {schema}.messages ORDER BY channel LIMIT 1) \
             UNION ALL \
             SELECT (SELECT m.channel FROM {schema}.messages m \
                     WHERE m.channel > c.channel ORDER BY m.channel LIMIT 1) \
             FROM channels c WHERE c.channel IS NOT NULL), \
         cutoffs AS ( \
             SELECT c.channel, cut.xid, cut.id FROM channels c, \
             LATERAL (SELECT m.xid, m.id FROM {schema}.messages m WHERE m.channel = c.channel \
                      ORDER BY m.xid DESC, m.id DESC OFFSET $2 LIMIT 1) cut \
             WHERE c.channel IS NOT NULL), \
         doomed AS ( \
             SELECT m.id FROM {schema}.messages m JOIN cutoffs k ON m.channel = k.channel \
             WHERE (m.xid, m.id) <= (k.xid, k.id) \
             UNION \
             SELECT m.id FROM {schema}.messages m \
             WHERE m.created_at < now() - make_interval(secs => $1)), \
         deleted AS ( \
             DELETE FROM {schema}.messages m USING doomed d \
             WHERE m.id = d.id AND m.xid < pg_snapshot_xmin(pg_current_snapshot()) \
               AND m.created_at < now() - make_interval(secs => $3) \
             RETURNING m.xid, m.id) \
         SELECT (SELECT count(*) FROM deleted), \
                (SELECT deleted.xid::text FROM deleted \
                 ORDER BY deleted.xid DESC, deleted.id DESC LIMIT 1), \
                (SELECT deleted.id FROM deleted \
                 ORDER BY deleted.xid DESC, deleted.id DESC LIMIT 1)"
    ))
    .bind(max_age.as_secs_f64())
    .bind(keep_per_channel.max(0))
    .bind(grace.as_secs_f64())
    .fetch_one(&mut *tx)
    .await?;
    // The newest position trimmed: a cursor below it may have missed
    // something.
    if let (Some(xid), Some(id)) = (xid, id) {
        let xid: u64 = xid.parse().map_err(|_| Error::InvalidPosition(xid))?;
        let p = Position { xid, id };
        sqlx::query(&format!(
            "UPDATE {schema}.state SET trimmed_xid = $1::text::xid8, trimmed_id = $2 \
             WHERE ($1::text::xid8, $2) > (trimmed_xid, trimmed_id)"
        ))
        .bind(p.xid.to_string())
        .bind(p.id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(Some(count as u64))
}

/// The background task: trims on its schedule, skipping a round while
/// another process is trimming.
pub(crate) async fn run(pool: PgPool, config: Config) {
    let Some(policy) = config.trim.clone() else {
        return;
    };
    loop {
        tokio::time::sleep(policy.every).await;
        let trimmed = trim_on(
            &pool,
            &config.schema,
            config.trim_grace,
            policy.max_age,
            policy.keep_per_channel,
            false,
        )
        .await;
        if let Err(e) = trimmed {
            tracing::warn!("pg-bus: trim failed: {e}");
        }
    }
}
