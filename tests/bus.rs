//! Against the shell's PostgreSQL (`DATABASE_URL`, `db_start`). Each test
//! runs in a schema of its own.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pg_bus::{Bus, Config, Filter, Item, Message, Position};
use serde_json::{Value, json};
use sqlx::PgPool;

static NEXT: AtomicU32 = AtomicU32::new(0);

async fn bus_with(capacity: usize) -> (Bus, PgPool) {
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

async fn bus() -> (Bus, PgPool) {
    bus_with(1024).await
}

fn everyone(channels: &[&str]) -> Filter {
    Filter {
        channels: channels.iter().map(|c| c.to_string()).collect(),
        tags: Vec::new(),
    }
}

/// Publishes one message in a transaction of its own and commits it.
async fn publish(bus: &Bus, pool: &PgPool, channel: &str, data: Value, audience: Option<&[&str]>) {
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
async fn settled(bus: &Bus, from: Position, filter: &Filter, n: usize) -> Vec<Message> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let messages = bus.backlog(from, filter, 100).await.unwrap();
        if messages.len() >= n || tokio::time::Instant::now() > deadline {
            return messages;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn data(messages: &[Message]) -> Vec<Value> {
    messages.iter().map(|m| m.data.clone()).collect()
}

async fn next_message(sub: &mut pg_bus::Subscription) -> Message {
    match tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await
        .expect("a message within 10 s")
        .unwrap()
    {
        Item::Message(m) => m,
        Item::Gap => panic!("unexpected gap"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn committed_messages_are_delivered_and_rolled_back_ones_never() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    bus.publish(&mut tx, "/a", &json!("rolled back"), None)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    publish(&bus, &pool, "/a", json!("committed"), None).await;

    let messages = settled(&bus, start, &everyone(&["/a"]), 1).await;
    assert_eq!(data(&messages), vec![json!("committed")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_that_commits_late_is_not_skipped() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();
    let mut sub = bus.subscribe(start, everyone(&["/a"]));

    // A takes the lower transaction id and stays open; B commits first.
    let mut a = pool.begin().await.unwrap();
    bus.publish(&mut a, "/a", &json!("a"), None).await.unwrap();
    let mut b = pool.begin().await.unwrap();
    bus.publish(&mut b, "/a", &json!("b"), None).await.unwrap();
    b.commit().await.unwrap();

    // B waits behind A: delivering it now would move cursors past A.
    let held = bus.backlog(start, &everyone(&["/a"]), 100).await.unwrap();
    assert!(held.is_empty(), "{held:?}");

    a.commit().await.unwrap();
    let messages = settled(&bus, start, &everyone(&["/a"]), 2).await;
    assert_eq!(data(&messages), vec![json!("a"), json!("b")]);
    assert!(messages[0].position < messages[1].position);

    // The live subscriber gets both, in that order, though B committed
    // first.
    assert_eq!(next_message(&mut sub).await.data, json!("a"));
    assert_eq!(next_message(&mut sub).await.data, json!("b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribers_resume_from_their_position() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();
    for n in 1..=3 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    let all = settled(&bus, start, &everyone(&["/a"]), 3).await;
    assert_eq!(data(&all), vec![json!(1), json!(2), json!(3)]);

    // A client that saw the first message hands its position back as a
    // string (Last-Event-ID) and gets the rest.
    let seen: Position = all[0].position.to_string().parse().unwrap();
    let mut sub = bus.subscribe(seen, everyone(&["/a"]));
    assert_eq!(next_message(&mut sub).await.data, json!(2));
    assert_eq!(next_message(&mut sub).await.data, json!(3));
    assert_eq!(sub.position(), all[2].position);
}

#[tokio::test(flavor = "multi_thread")]
async fn channels_and_audiences_filter_delivery() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();
    publish(&bus, &pool, "/topic/1", json!("public"), None).await;
    publish(
        &bus,
        &pool,
        "/topic/1",
        json!("for user 3"),
        Some(&["user:3"]),
    )
    .await;
    publish(
        &bus,
        &pool,
        "/topic/1",
        json!("for staff"),
        Some(&["group:staff"]),
    )
    .await;
    publish(&bus, &pool, "/topic/2", json!("other channel"), None).await;

    let user3 = Filter {
        channels: vec!["/topic/1".into()],
        tags: vec!["user:3".into(), "group:trust_level_1".into()],
    };
    let got = settled(&bus, start, &user3, 2).await;
    assert_eq!(data(&got), vec![json!("public"), json!("for user 3")]);

    let anonymous = everyone(&["/topic/1", "/topic/2"]);
    let got = settled(&bus, start, &anonymous, 2).await;
    assert_eq!(data(&got), vec![json!("public"), json!("other channel")]);

    // The live feed filters the same way.
    let mut sub = bus.subscribe(bus.now().await.unwrap(), user3);
    publish(
        &bus,
        &pool,
        "/topic/1",
        json!("for staff again"),
        Some(&["group:staff"]),
    )
    .await;
    publish(
        &bus,
        &pool,
        "/topic/1",
        json!("for user 3 again"),
        Some(&["user:3"]),
    )
    .await;
    assert_eq!(next_message(&mut sub).await.data, json!("for user 3 again"));
}

#[tokio::test(flavor = "multi_thread")]
async fn live_subscribers_get_messages_as_they_commit() {
    let (bus, pool) = bus().await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/live"]));
    let publisher = {
        let (bus, pool) = (bus.clone(), pool.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            publish(&bus, &pool, "/live", json!("hello"), None).await;
        })
    };
    assert_eq!(next_message(&mut sub).await.data, json!("hello"));
    publisher.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_behind_the_feed_catches_up_from_the_backlog() {
    let (bus, pool) = bus_with(2).await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    // Read nothing until well past the feed's capacity of 2.
    for n in 1..=10 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut got = Vec::new();
    for _ in 1..=10 {
        got.push(next_message(&mut sub).await.data);
    }
    assert_eq!(got, (1..=10).map(|n| json!(n)).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn trimming_keeps_the_newest_and_reports_gaps() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();
    for n in 1..=5 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    publish(&bus, &pool, "/b", json!("b"), None).await;
    let before = settled(&bus, start, &everyone(&["/a"]), 5).await;
    settled(&bus, start, &everyone(&["/a", "/b"]), 6).await;

    // Two per channel are kept.
    let deleted = bus.trim(Duration::from_secs(3600), 2).await.unwrap();
    assert_eq!(deleted, 3);
    let after = bus
        .backlog(start, &everyone(&["/a", "/b"]), 100)
        .await
        .unwrap();
    assert_eq!(data(&after), vec![json!(4), json!(5), json!("b")]);

    // A cursor from before the trim has missed messages: it is told so
    // first, then gets what remains.
    let mut stale = bus.subscribe(start, everyone(&["/a"]));
    let first = tokio::time::timeout(Duration::from_secs(10), stale.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, Item::Gap);
    assert_eq!(next_message(&mut stale).await.data, json!(4));

    // A cursor past what was trimmed has not.
    assert!(!bus.trimmed_after(before[2].position).await.unwrap());
    assert!(bus.trimmed_after(before[1].position).await.unwrap());
}

#[test]
fn positions_round_trip_as_strings() {
    let p: Position = "742-15".parse().unwrap();
    assert_eq!(p.to_string(), "742-15");
    for bad in ["", "742", "742-", "-15", "a-1", "1-b", "1-2-3"] {
        assert!(bad.parse::<Position>().is_err(), "{bad}");
    }
    assert!(Position::START < p);
}
