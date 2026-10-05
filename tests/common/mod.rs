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
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL (run inside nix-shell)");
    let pool = PgPool::connect(&url).await.unwrap();
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
        schema,
        capacity,
        ..Config::default()
    };
    (Bus::start(pool.clone(), config).await.unwrap(), pool)
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
) {
    let audience: Option<Vec<String>> = audience.map(|a| a.iter().map(|s| s.to_string()).collect());
    let mut tx = pool.begin().await.unwrap();
    bus.publish(&mut tx, channel, &data, audience.as_deref())
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
