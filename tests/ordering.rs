//! The ordering guarantee: every subscriber gets every committed message
//! after its starting position, once, in position order, whatever order
//! transactions commit in, however far behind it falls, and across
//! reconnects. Checked against the exact positions the publishers wrote.
//!
//! The soak runs for `SOAK_SECONDS` (default 1) in the normal suite; run it
//! longer with `SOAK_SECONDS=600 cargo nextest run --test ordering`.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{bus_in_schema, bus_with, everyone, publish, start_bus};
use pg_bus::{Bus, Item, Position};
use serde_json::json;
use sqlx::PgPool;

/// What the ordering rests on: transactions that commit one after another
/// get increasing ids, whichever connection they run on.
#[tokio::test(flavor = "multi_thread")]
async fn sequential_transactions_get_increasing_ids() {
    let (_bus, pool) = bus_with(16).await;
    let mut last = 0u64;
    for _ in 0..20 {
        let mut tx = pool.begin().await.unwrap();
        let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let xid: u64 = xid.parse().unwrap();
        assert!(xid > last, "{xid} after {last}");
        last = xid;
    }
}

/// Only a transaction that has written holds delivery back: a read-only
/// one has no id, so it cannot sit below the horizon.
#[tokio::test(flavor = "multi_thread")]
async fn only_writing_transactions_hold_the_horizon() {
    let (bus, pool) = bus_with(16).await;
    let mut reader = pool.begin().await.unwrap();
    sqlx::query("SELECT 1").execute(&mut *reader).await.unwrap();
    let assigned: Option<String> =
        sqlx::query_scalar("SELECT pg_current_xact_id_if_assigned()::text")
            .fetch_one(&mut *reader)
            .await
            .unwrap();
    assert_eq!(assigned, None);

    let mut writer = pool.begin().await.unwrap();
    let held = bus
        .publish(&mut writer, "/a", &json!("held"), None)
        .await
        .unwrap();
    // While the writer is open, the horizon is at or below it.
    assert!(bus.now().await.unwrap() <= held);
    writer.commit().await.unwrap();
    reader.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_returns_the_position_delivered() {
    let (bus, pool) = bus_with(16).await;
    let mut sub = bus.subscribe(bus.now().await.unwrap(), everyone(&["/a"]));
    let written = publish(&bus, &pool, "/a", json!("x"), None).await;
    let delivered = match tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await
        .unwrap()
        .unwrap()
    {
        Item::Message(m) => m.position,
        Item::Gap => panic!("gap"),
    };
    assert_eq!(delivered, written);
}

/// xorshift: enough randomness for interleavings, no dependency.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// One publisher: transactions of one to three messages, some held open
/// so later ones commit first, some rolled back. Records the positions of
/// what it committed.
async fn publisher(
    bus: Bus,
    pool: PgPool,
    seed: u64,
    until: tokio::time::Instant,
    recorded: Arc<Mutex<Vec<Position>>>,
) {
    let mut rng = Rng(seed);
    while tokio::time::Instant::now() < until {
        let mut tx = pool.begin().await.unwrap();
        let mut written = Vec::new();
        for _ in 0..=rng.below(3) {
            written.push(
                bus.publish(&mut tx, "/a", &json!(seed), None)
                    .await
                    .unwrap(),
            );
        }
        if rng.below(3) == 0 {
            tokio::time::sleep(Duration::from_millis(rng.below(8))).await;
        }
        if rng.below(7) == 0 {
            tx.rollback().await.unwrap();
        } else {
            tx.commit().await.unwrap();
            recorded.lock().unwrap().extend(written);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// How a subscriber reads: from its start, slowly or
/// not, reconnecting from its position every so often.
struct Reader {
    start: Position,
    delay: Duration,
    reconnect_every: Option<usize>,
}

/// Reads until publishing is over and it has every committed message after
/// its start, or until 30 s pass without a new one (then the test reports
/// what is missing).
async fn reader(
    bus: Bus,
    how: Reader,
    done: Arc<AtomicBool>,
    recorded: Arc<Mutex<Vec<Position>>>,
) -> Vec<Position> {
    let mut sub = bus.subscribe(how.start, everyone(&["/a"]));
    let mut got: Vec<Position> = Vec::new();
    let mut progress = tokio::time::Instant::now();
    let complete = |got: &Vec<Position>| {
        done.load(Ordering::SeqCst)
            && got.len()
                >= recorded
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|p| **p > how.start)
                    .count()
    };
    loop {
        if complete(&got) || progress.elapsed() > Duration::from_secs(30) {
            return got;
        }
        match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
            Ok(item) => match item.unwrap() {
                Item::Message(m) => {
                    got.push(m.position);
                    progress = tokio::time::Instant::now();
                }
                Item::Gap => panic!("gap without a trim"),
            },
            Err(_) => continue,
        }
        // A pause every 50 messages falls well behind a feed of 8.
        if !how.delay.is_zero() && got.len().is_multiple_of(50) {
            tokio::time::sleep(how.delay * 20).await;
        }
        if let Some(every) = how.reconnect_every
            && got.len().is_multiple_of(every)
        {
            // A dropped connection resuming from its last event id.
            let from = sub.position();
            sub = bus.subscribe(from, everyone(&["/a"]));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_subscriber_gets_exactly_the_committed_sequence() {
    // A panic in any task (the out-of-order check in a reader) ends the
    // run at once with its message, instead of waiting for the others.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::exit(101);
    }));
    let seconds: u64 = std::env::var("SOAK_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    // A small ring, so catch-ups cross between memory and the table.
    let (bus, pool, _) = start_bus(|c| {
        c.capacity = 8;
        c.recent = 64;
    })
    .await;
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));
    let start = bus.now().await.unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(seconds);

    let mut readers = Vec::new();
    for (delay, reconnect_every) in [(0, None), (1, None), (3, None), (0, Some(17)), (2, Some(5))] {
        let how = Reader {
            start,
            delay: Duration::from_millis(delay),
            reconnect_every,
        };
        readers.push((
            start,
            tokio::spawn(reader(bus.clone(), how, done.clone(), recorded.clone())),
        ));
    }
    let publishers: Vec<_> = (1..=6)
        .map(|seed| {
            tokio::spawn(publisher(
                bus.clone(),
                pool.clone(),
                seed * 7919,
                until,
                recorded.clone(),
            ))
        })
        .collect();
    // Late joiners start from wherever the bus is partway through.
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(seconds * 250)).await;
        let joined = bus.now().await.unwrap();
        let how = Reader {
            start: joined,
            delay: Duration::ZERO,
            reconnect_every: None,
        };
        readers.push((
            joined,
            tokio::spawn(reader(bus.clone(), how, done.clone(), recorded.clone())),
        ));
    }
    for p in publishers {
        p.await.unwrap();
    }
    done.store(true, Ordering::SeqCst);

    let mut all = recorded.lock().unwrap().clone();
    all.sort();
    assert!(!all.is_empty());
    for (from, handle) in readers {
        let got = handle.await.unwrap();
        let expected: Vec<Position> = all.iter().copied().filter(|p| *p > from).collect();
        assert_eq!(
            got.len(),
            expected.len(),
            "from {from}: got {} of {}",
            got.len(),
            expected.len()
        );
        assert_eq!(got, expected, "from {from}");
    }
}

/// Transaction ids order as numbers across a change in digit count. As
/// text, "10" sorts before "9": that sort once delivered messages out of
/// order and made the listener skip some, whenever a batch crossed a power
/// of ten (999999 to 1000000).
#[tokio::test(flavor = "multi_thread")]
async fn positions_order_numerically_across_digit_boundaries() {
    let (bus, pool, schema) = bus_in_schema(16).await;
    // Long-finished transaction ids, so all four are deliverable.
    sqlx::query(&format!(
        "INSERT INTO {schema}.messages (xid, channel, data) VALUES \
           ('100', '/a', '100'), ('9', '/a', '9'), ('99', '/a', '99'), ('10', '/a', '10')"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let order = |messages: Vec<pg_bus::Message>| -> Vec<serde_json::Value> {
        messages.into_iter().map(|m| m.data).collect()
    };
    let expected = vec![json!(9), json!(10), json!(99), json!(100)];
    let messages = bus
        .backlog(Position::START, &everyone(&["/a"]), 100)
        .await
        .unwrap();
    assert_eq!(order(messages), expected);
    // In batches that end at a boundary, too.
    let first = bus
        .backlog(Position::START, &everyone(&["/a"]), 1)
        .await
        .unwrap();
    let rest = bus
        .backlog(first[0].position, &everyone(&["/a"]), 100)
        .await
        .unwrap();
    assert_eq!(
        order(first)
            .into_iter()
            .chain(order(rest))
            .collect::<Vec<_>>(),
        expected
    );
    // And through a subscription.
    let mut sub = bus.subscribe(Position::START, everyone(&["/a"]));
    let mut got = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .unwrap()
            .unwrap()
        {
            Item::Message(m) => got.push(m.data),
            Item::Gap => panic!("gap"),
        }
    }
    assert_eq!(got, expected);
}
