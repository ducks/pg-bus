//! Trimming: to the newest messages per channel, by age, in the background,
//! and from several processes at once.

mod common;

use std::time::Duration;

use common::{everyone, publish, settled, start_bus};
use pg_bus::{Bus, Config, Trim};
use serde_json::json;
use sqlx::PgPool;

/// The data left on a channel, oldest first.
async fn left(pool: &PgPool, schema: &str, channel: &str) -> Vec<i64> {
    sqlx::query_scalar(&format!(
        "SELECT (data #>> '{{}}')::bigint FROM {schema}.messages WHERE channel = $1 ORDER BY xid, id"
    ))
    .bind(channel)
    .fetch_all(pool)
    .await
    .unwrap()
}

fn no_grace(c: &mut Config) {
    c.trim_grace = Duration::ZERO;
    c.trim = None;
}

#[tokio::test(flavor = "multi_thread")]
async fn each_channel_keeps_its_newest() {
    let (bus, pool, schema) = start_bus(no_grace).await;
    let start = bus.now().await.unwrap();
    let channels = ["/a", "/b", "/c", "/d", "/e"];
    for n in 1..=7 {
        for c in channels {
            publish(&bus, &pool, c, json!(n), None).await;
        }
    }
    settled(&bus, start, &everyone(&channels), 35).await;
    assert_eq!(bus.trim(Duration::from_secs(3600), 3).await.unwrap(), 20);
    for c in channels {
        assert_eq!(left(&pool, &schema, c).await, vec![5, 6, 7], "{c}");
    }
    // Nothing more to do.
    assert_eq!(bus.trim(Duration::from_secs(3600), 3).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn old_messages_go_whatever_the_channel_holds() {
    let (bus, pool, schema) = start_bus(no_grace).await;
    let start = bus.now().await.unwrap();
    for n in 1..=3 {
        publish(&bus, &pool, "/old", json!(n), None).await;
    }
    for n in 1..=2 {
        publish(&bus, &pool, "/new", json!(n), None).await;
    }
    settled(&bus, start, &everyone(&["/old", "/new"]), 5).await;
    sqlx::query(&format!(
        "UPDATE {schema}.messages SET created_at = now() - interval '2 hours' WHERE channel = '/old'"
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(bus.trim(Duration::from_secs(3600), 1000).await.unwrap(), 3);
    assert_eq!(left(&pool, &schema, "/old").await, Vec::<i64>::new());
    assert_eq!(left(&pool, &schema, "/new").await, vec![1, 2]);
    // A cursor from before them has missed something.
    assert!(bus.trimmed_after(start).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_background_task_trims_on_its_own() {
    let (bus, pool, schema) = start_bus(|c| {
        c.trim_grace = Duration::ZERO;
        c.trim = Some(Trim {
            every: Duration::from_millis(100),
            keep_per_channel: 2,
            ..Trim::default()
        });
    })
    .await;
    for n in 1..=6 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = left(&pool, &schema, "/a").await;
        if rows == vec![5, 6] {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "still {rows:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(bus);
}

#[tokio::test(flavor = "multi_thread")]
async fn processes_trimming_together_take_turns() {
    let (bus, pool, schema) = start_bus(no_grace).await;
    // A second process on the same schema, with its own trim task.
    let other = Bus::start(
        pool.clone(),
        Config {
            schema: schema.clone(),
            trim_grace: Duration::ZERO,
            trim: Some(Trim {
                every: Duration::from_millis(20),
                keep_per_channel: 2,
                ..Trim::default()
            }),
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let start = bus.now().await.unwrap();
    for n in 1..=20 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    settled(&bus, start, &everyone(&["/a"]), 2).await;
    // Explicit trims alongside the task: they wait their turn, none fails.
    let trims: Vec<_> = (0..4)
        .map(|_| {
            let bus = bus.clone();
            tokio::spawn(async move { bus.trim(Duration::from_secs(3600), 2).await })
        })
        .collect();
    for t in trims {
        t.await.unwrap().unwrap();
    }
    assert_eq!(left(&pool, &schema, "/a").await, vec![19, 20]);
    drop(other);
}
