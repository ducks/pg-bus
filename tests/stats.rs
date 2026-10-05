//! Bus::stats counts what the process does, and the listener survives
//! losing its connection.

mod common;

use std::time::Duration;

use common::{everyone, publish, start_bus};
use pg_bus::{Item, Message, Position, Subscription};
use serde_json::json;

async fn next(sub: &mut Subscription) -> Message {
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
async fn counts_deliveries_catch_ups_and_subscribers() {
    let (bus, pool, _) = start_bus(|c| c.recent = 100).await;
    assert_eq!(bus.stats().subscribers, 0);
    let mut live = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    assert_eq!(bus.stats().subscribers, 1);
    for n in 1..=3 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    let first = next(&mut live).await;
    next(&mut live).await;
    let last = next(&mut live).await;

    let stats = bus.stats();
    assert!(stats.delivered >= 3, "{stats:?}");
    assert!(stats.listener_position >= last.position, "{stats:?}");
    assert!(stats.listener_queries >= 1, "{stats:?}");
    let memory = stats.catch_ups_from_memory;
    let database = stats.catch_ups_from_database;
    assert!(
        memory >= 1,
        "the first catch-up is from the ring: {stats:?}"
    );

    // Resuming within the ring is answered from memory.
    let mut resumed = bus.subscribe(first.position, everyone(&["/a"]));
    next(&mut resumed).await;
    assert!(bus.stats().catch_ups_from_memory > memory);
    // From before the listener started, it reads the table.
    let mut old = bus.subscribe(Position::START, everyone(&["/a"]));
    next(&mut old).await;
    assert!(bus.stats().catch_ups_from_database > database);

    assert_eq!(bus.stats().subscribers, 3);
    drop((live, resumed, old));
    let stats = bus.stats();
    assert_eq!(stats.subscribers, 0);
    assert_eq!(stats.out_of_order, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn counts_subscribers_falling_behind() {
    let (bus, pool, _) = start_bus(|c| c.capacity = 2).await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    // Caught up and following the live feed (of 2) first.
    publish(&bus, &pool, "/a", json!(0), None).await;
    next(&mut sub).await;
    for n in 1..=10 {
        publish(&bus, &pool, "/a", json!(n), None).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while bus.stats().delivered < 11 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for _ in 1..=10 {
        next(&mut sub).await;
    }
    assert!(bus.stats().lagged >= 1, "{:?}", bus.stats());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_listener_does_not_query() {
    let (bus, pool, _) = start_bus(|_| {}).await;
    publish(&bus, &pool, "/a", json!("settle"), None).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while bus.stats().delivered < 1 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = bus.stats().listener_queries;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let after = bus.stats().listener_queries;
    assert!(after - before <= 2, "{} queries while idle", after - before);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_listener_survives_losing_its_connection() {
    let (bus, pool, schema) = start_bus(|_| {}).await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    publish(&bus, &pool, "/a", json!("before"), None).await;
    assert_eq!(next(&mut sub).await.data, json!("before"));

    // Kill the listener's own connection, the one that ran LISTEN.
    let killed: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = $1",
    )
    .bind(format!("LISTEN \"{schema}\""))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(killed, vec![true]);

    // Published while it was gone and after: both arrive, in order.
    publish(&bus, &pool, "/a", json!("during"), None).await;
    publish(&bus, &pool, "/a", json!("after"), None).await;
    assert_eq!(next(&mut sub).await.data, json!("during"));
    assert_eq!(next(&mut sub).await.data, json!("after"));
    assert!(bus.stats().listener_reconnects >= 1, "{:?}", bus.stats());
}

/// Each commit sends a notification; after a burst the listener must not
/// keep working through them one at a time (it once ran two queries per
/// notification for seconds after the burst ended).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_costs_nothing_once_delivered() {
    let (bus, pool, _) = start_bus(|_| {}).await;
    let publishers: Vec<_> = (0..8)
        .map(|_| {
            let (bus, pool) = (bus.clone(), pool.clone());
            tokio::spawn(async move {
                for n in 0..50 {
                    publish(&bus, &pool, "/a", json!(n), None).await;
                }
            })
        })
        .collect();
    for p in publishers {
        p.await.unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while bus.stats().delivered < 400 {
        assert!(tokio::time::Instant::now() < deadline, "{:?}", bus.stats());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = bus.stats().listener_queries;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = bus.stats().listener_queries;
    assert!(
        after - before <= 2,
        "{} queries after the burst",
        after - before
    );
}

/// With Config::listen_url, the listener connects there and not through
/// the pool (which may go through a transaction-mode pooler).
#[tokio::test(flavor = "multi_thread")]
async fn the_listener_uses_its_own_url_when_given() {
    let url = std::env::var("DATABASE_URL").unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let listen_url = format!("{url}{separator}application_name=pg-bus-listener");
    let (bus, pool, schema) = start_bus(|c| c.listen_url = Some(listen_url)).await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    publish(&bus, &pool, "/a", json!("via listen_url"), None).await;
    assert_eq!(next(&mut sub).await.data, json!("via listen_url"));
    let names: Vec<String> =
        sqlx::query_scalar("SELECT application_name FROM pg_stat_activity WHERE query = $1")
            .bind(format!("LISTEN \"{schema}\""))
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(names, vec!["pg-bus-listener".to_string()]);
}
