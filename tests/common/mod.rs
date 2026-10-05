//! Shared by the test binaries: a bus in a schema of its own, against the
//! shell's PostgreSQL (`DATABASE_URL`, `db_start`).
#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pg_bus::{Bus, Config, Filter, Message, Position};
use serde_json::Value;
use sqlx::PgPool;

static NEXT: AtomicU32 = AtomicU32::new(0);

pub async fn bus_with(capacity: usize) -> (Bus, PgPool) {
    let (bus, pool, _) = bus_in_schema(capacity).await;
    (bus, pool)
}

/// A bus, its pool and the schema it runs in.
pub async fn bus_in_schema(capacity: usize) -> (Bus, PgPool, String) {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL (run inside nix-shell)");
    let pool = PgPool::connect(&url).await.unwrap();
    drop_stale_schemas(&pool).await;
    let schema = format!(
        "t_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
    let config = Config {
        schema: schema.clone(),
        capacity,
        ..Config::default()
    };
    (
        Bus::start(pool.clone(), config).await.unwrap(),
        pool,
        schema,
    )
}

/// Drops the schemas of test processes that have exited (`t_<pid>_<n>`,
/// the pid no longer running), one process at a time.
async fn drop_stale_schemas(pool: &PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('pg-bus test cleanup'))")
        .execute(&mut *conn)
        .await
        .unwrap();
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT nspname::text FROM pg_namespace WHERE nspname ~ '^t_[0-9]+_[0-9]+$'",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    for name in names {
        let pid = name.split('_').nth(1).unwrap_or_default();
        if !std::path::Path::new(&format!("/proc/{pid}")).exists() {
            sqlx::query(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"))
                .execute(&mut *conn)
                .await
                .unwrap();
        }
    }
    sqlx::query("SELECT pg_advisory_unlock(hashtext('pg-bus test cleanup'))")
        .execute(&mut *conn)
        .await
        .unwrap();
}

pub async fn bus() -> (Bus, PgPool) {
    bus_with(1024).await
}

pub fn everyone(channels: &[&str]) -> Filter {
    Filter {
        channels: channels.iter().map(|c| c.to_string()).collect(),
        tags: Vec::new(),
    }
}

/// Publishes one message in a transaction of its own and commits it.
pub async fn publish(
    bus: &Bus,
    pool: &PgPool,
    channel: &str,
    data: Value,
    audience: Option<&[&str]>,
) -> Position {
    let audience: Option<Vec<String>> = audience.map(|a| a.iter().map(|s| s.to_string()).collect());
    let mut tx = pool.begin().await.unwrap();
    let position = bus
        .publish(&mut tx, channel, &data, audience.as_deref())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    position
}

/// The backlog once `n` messages are deliverable. A committed message
/// waits while any older write transaction is open, and tests running
/// alongside hold some open on purpose.
pub async fn settled(bus: &Bus, from: Position, filter: &Filter, n: usize) -> Vec<Message> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let messages = bus.backlog(from, filter, 100).await.unwrap();
        if messages.len() >= n || tokio::time::Instant::now() > deadline {
            return messages;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
