//! Rough numbers for pg-bus against a local PostgreSQL: publish
//! throughput, delivery latency, fan-out to many subscribers in one
//! process, and the listener's own queries. Prints a markdown
//! report (BENCH.md keeps the runs).
//!
//!     DATABASE_URL=... cargo run --release --example bench
//!     DATABASE_URL=... cargo run --example bench -- --quick   # a smoke run
//!
//! It works in a schema of its own (bench_<pid>) and drops it at the end.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pg_bus::{Bus, Config, Filter, Item};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// How big a run is.
struct Scale {
    publishers: usize,
    transactions_each: usize,
    rate: u64,
    seconds: u64,
    fanout: Vec<usize>,
    fanout_messages: usize,
}

impl Scale {
    fn full() -> Scale {
        Scale {
            publishers: 8,
            transactions_each: 2000,
            rate: 500,
            seconds: 5,
            fanout: vec![1, 100, 1000],
            fanout_messages: 200,
        }
    }

    fn quick() -> Scale {
        Scale {
            publishers: 2,
            transactions_each: 20,
            rate: 100,
            seconds: 1,
            fanout: vec![1, 10],
            fanout_messages: 10,
        }
    }
}

/// The value at `p` (0 to 100) of sorted samples, nearest rank.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn micros_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Latencies in ms of a message whose data is its send time in µs.
fn latency_ms(data: &serde_json::Value) -> f64 {
    let sent = data.as_u64().unwrap_or(0);
    micros_now().saturating_sub(sent) as f64 / 1000.0
}

fn summary(mut samples: Vec<f64>) -> String {
    samples.sort_by(|a, b| a.total_cmp(b));
    format!(
        "{:.1} | {:.1} | {:.1}",
        percentile(&samples, 50.0),
        percentile(&samples, 99.0),
        samples.last().copied().unwrap_or(f64::NAN)
    )
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1).map(str::to_string))
        })
        .and_then(|kb| kb.parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(f64::NAN)
}

async fn publish_one(
    bus: &Bus,
    pool: &PgPool,
    channel: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    bus.publish(&mut tx, channel, &json!(micros_now()), None)
        .await?;
    tx.commit().await?;
    Ok(())
}

fn on(channels: &[&str]) -> Filter {
    Filter {
        channels: channels.iter().map(|c| c.to_string()).collect(),
        tags: Vec::new(),
    }
}

/// Phase 1: publishers committing one message per transaction, flat out.
async fn throughput(
    bus: &Bus,
    pool: &PgPool,
    s: &Scale,
) -> Result<String, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let tasks: Vec<_> = (0..s.publishers)
        .map(|_| {
            let (bus, pool) = (bus.clone(), pool.clone());
            let n = s.transactions_each;
            tokio::spawn(async move {
                for _ in 0..n {
                    publish_one(&bus, &pool, "/throughput").await.unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await?;
    }
    let total = s.publishers * s.transactions_each;
    let rate = total as f64 / started.elapsed().as_secs_f64();
    Ok(format!(
        "| publish, {} publishers, 1 message per transaction | {total} messages | {rate:.0} messages/s |",
        s.publishers
    ))
}

/// Phase 2: one subscriber, messages at a steady rate: publish to receipt.
async fn latency(
    bus: &Bus,
    pool: &PgPool,
    s: &Scale,
) -> Result<String, Box<dyn std::error::Error>> {
    let total = (s.rate * s.seconds) as usize;
    let mut sub = bus.subscribe(bus.now().await?, on(&["/latency"]));
    let reader = tokio::spawn(async move {
        let mut samples = Vec::with_capacity(total);
        while samples.len() < total {
            if let Ok(Item::Message(m)) = sub.next().await {
                samples.push(latency_ms(&m.data));
            }
        }
        samples
    });
    let gap = Duration::from_micros(1_000_000 / s.rate);
    let mut tick = tokio::time::interval(gap);
    for _ in 0..total {
        tick.tick().await;
        publish_one(bus, pool, "/latency").await?;
    }
    let samples = reader.await?;
    Ok(format!(
        "| latency, 1 subscriber, {} messages/s | ms p50 / p99 / max | {} |",
        s.rate,
        summary(samples)
    ))
}

/// Phase 3: many subscribers in one process, each getting every message.
async fn fanout(
    bus: &Bus,
    pool: &PgPool,
    s: &Scale,
    subscribers: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let rss_before = rss_mb();
    let start = bus.now().await?;
    let readers: Vec<_> = (0..subscribers)
        .map(|_| {
            let mut sub = bus.subscribe(start, on(&["/fanout"]));
            let n = s.fanout_messages;
            tokio::spawn(async move {
                let mut samples = Vec::with_capacity(n);
                while samples.len() < n {
                    if let Ok(Item::Message(m)) = sub.next().await {
                        samples.push(latency_ms(&m.data));
                    }
                }
                samples
            })
        })
        .collect();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    for _ in 0..s.fanout_messages {
        tick.tick().await;
        publish_one(bus, pool, "/fanout").await?;
    }
    let rss_during = rss_mb();
    let mut samples = Vec::new();
    for r in readers {
        samples.extend(r.await?);
    }
    Ok(format!(
        "| fan-out, {subscribers} subscribers, 100 messages/s | ms p50 / p99 / max, RSS +{:.0} MB | {} |",
        rss_during - rss_before,
        summary(samples)
    ))
}

/// Phase 4: the listener's own queries per second (Bus::stats), idle and
/// with messages arriving every 10 ms.
async fn listener_cost(
    bus: &Bus,
    pool: &PgPool,
    s: &Scale,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let window = Duration::from_secs(s.seconds * 2);
    let mut rows = Vec::new();

    let before = bus.stats().listener_queries;
    tokio::time::sleep(window).await;
    let idle = (bus.stats().listener_queries - before) as f64 / window.as_secs_f64();
    rows.push(format!("| listener, idle | queries/s | {idle:.2} |"));

    let before = bus.stats().listener_queries;
    let started = Instant::now();
    let mut published = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    while started.elapsed() < window {
        tick.tick().await;
        publish_one(bus, pool, "/cost").await?;
        published += 1;
    }
    let queries = bus.stats().listener_queries - before;
    let seconds = started.elapsed().as_secs_f64();
    rows.push(format!(
        "| listener, 100 messages/s | queries/s, per message | {:.1}, {:.2} |",
        queries as f64 / seconds,
        queries as f64 / published as f64
    ));
    Ok(rows)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let quick = std::env::args().any(|a| a == "--quick");
    let s = if quick { Scale::quick() } else { Scale::full() };
    let url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(40)
        .connect(&url)
        .await?;
    let schema = format!("bench_{}", std::process::id());
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await?;
    let bus = Bus::start(
        pool.clone(),
        Config {
            schema: schema.clone(),
            trim: None,
            ..Config::default()
        },
    )
    .await?;

    let mut report = vec![
        "| measure | unit | result |".to_string(),
        "|---|---|---|".to_string(),
    ];
    report.push(throughput(&bus, &pool, &s).await?);
    report.push(latency(&bus, &pool, &s).await?);
    for &n in &s.fanout {
        report.push(fanout(&bus, &pool, &s, n).await?);
    }
    report.extend(listener_cost(&bus, &pool, &s).await?);
    println!("{}", report.join("\n"));

    drop(bus);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentiles_by_nearest_rank() {
        let s: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&s, 50.0), 50.0);
        assert_eq!(percentile(&s, 99.0), 99.0);
        assert_eq!(percentile(&s, 100.0), 100.0);
        assert_eq!(percentile(&s, 0.0), 1.0);
        assert_eq!(percentile(&[7.0], 99.0), 7.0);
        assert!(percentile(&[], 50.0).is_nan());
    }
}
