//! Catch-ups from memory: a subscriber resuming from a recent position is
//! served by the listener's ring, without the database.

mod common;

use std::time::Duration;

use common::{everyone, publish, start_bus};
use pg_bus::{Item, Message, Position, Subscription};
use serde_json::{Value, json};

async fn next(sub: &mut Subscription) -> Result<Message, pg_bus::Error> {
    match tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await
        .expect("an item within 10 s")?
    {
        Item::Message(m) => Ok(m),
        Item::Gap => panic!("unexpected gap"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recent_positions_catch_up_without_the_database() {
    let (bus, pool, schema) = start_bus(|c| c.recent = 100).await;
    let mut live = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    for n in 1..=3 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    // Delivered live: the listener has all three, so they are in its ring.
    let first = next(&mut live).await.unwrap();
    next(&mut live).await.unwrap();
    next(&mut live).await.unwrap();

    // Take the backlog away: only memory can answer now.
    sqlx::query(&format!(
        "ALTER TABLE {schema}.messages RENAME TO messages_away"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let mut resumed = bus.subscribe(first.position, everyone(&["/a"]));
    let got: Vec<Value> = vec![
        next(&mut resumed).await.unwrap().data,
        next(&mut resumed).await.unwrap().data,
    ];
    // A position older than the ring needs the table, and fails without it.
    let mut old = bus.subscribe(Position::START, everyone(&["/a"]));
    let fallback = next(&mut old).await;
    sqlx::query(&format!(
        "ALTER TABLE {schema}.messages_away RENAME TO messages"
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(got, vec![json!(2), json!(3)]);
    assert!(fallback.is_err(), "{fallback:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn positions_older_than_the_ring_come_from_the_database() {
    let (bus, pool, _) = start_bus(|c| c.recent = 4).await;
    let start = bus.now().await.unwrap();
    let mut live = bus.subscribe(start, everyone(&["/a"]));
    for n in 1..=10 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    for _ in 1..=10 {
        next(&mut live).await.unwrap();
    }
    // The ring holds the last four; from the start, the rest come from the
    // table, in order, then the ring's.
    let mut from_start = bus.subscribe(start, everyone(&["/a"]));
    let mut got = Vec::new();
    for _ in 1..=10 {
        got.push(next(&mut from_start).await.unwrap().data);
    }
    assert_eq!(got, (1..=10).map(|n| json!(n)).collect::<Vec<_>>());
}
