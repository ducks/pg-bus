//! Stalls: a committed message waiting behind an open transaction shows up
//! in Bus::health, along with the transaction holding it.

mod common;

use std::time::Duration;

use common::{bus, bus_in_schema, publish};
use pg_bus::{Bus, Health};
use serde_json::json;

/// Health once `done` holds of it (or 10 s pass): other tests' open
/// transactions can hold the horizon too, briefly.
async fn health_when(bus: &Bus, done: impl Fn(&Health) -> bool) -> Health {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let health = bus.health().await.unwrap();
        if done(&health) || tokio::time::Instant::now() > deadline {
            return health;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_waits_on_an_idle_bus() {
    let (bus, _pool) = bus().await;
    assert_eq!(bus.health().await.unwrap(), Health::default());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_transaction_shows_up_as_the_blocker() {
    let (bus, pool) = bus().await;

    // The holder writes, so it has a transaction id, and stays open.
    let mut holder = pool.begin().await.unwrap();
    bus.publish(&mut holder, "/a", &json!("held"), None)
        .await
        .unwrap();
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    // A later message commits, and waits behind it.
    publish(&bus, &pool, "/a", json!("waiting"), None).await;

    let health = bus.health().await.unwrap();
    assert_eq!(health.waiting, 1);
    assert!(health.oldest_wait.is_some());
    let blocker = health
        .blockers
        .iter()
        .find(|b| b.pid == Some(holder_pid))
        .unwrap_or_else(|| panic!("{holder_pid} not among {:?}", health.blockers));
    assert_eq!(blocker.state.as_deref(), Some("idle in transaction"));
    assert!(
        blocker
            .query
            .as_deref()
            .unwrap_or_default()
            .contains("pg_backend_pid")
    );
    assert!(blocker.prepared.is_none());

    // Past the threshold, the report names it.
    let report = pg_bus::stall_report(&health, Duration::ZERO).unwrap();
    assert!(report.contains(&format!("pid {holder_pid}")), "{report}");

    // Once it ends, both messages are delivered and nothing waits.
    holder.commit().await.unwrap();
    let health = health_when(&bus, |h| h.waiting == 0).await;
    assert_eq!(health, Health::default());
}

/// Two-phase commit, for the tests that need it: on when
/// max_prepared_transactions > 0 (nix/postgres.nix sets it; restart with
/// db_stop and db_start after changing it).
async fn prepared_transactions_on(pool: &sqlx::PgPool) -> bool {
    let max: String = sqlx::query_scalar("SHOW max_prepared_transactions")
        .fetch_one(pool)
        .await
        .unwrap();
    if max == "0" {
        eprintln!("not run: max_prepared_transactions is 0 (restart the shell's PostgreSQL)");
    }
    max != "0"
}

/// A prepared transaction belongs to no session and only ends with COMMIT
/// or ROLLBACK PREPARED: forgotten, it holds delivery back for good. Here
/// it is some other work's, writing to a table of its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_forgotten_prepared_transaction_shows_up_as_the_blocker() {
    let (bus, pool, schema) = bus_in_schema(1024).await;
    if !prepared_transactions_on(&pool).await {
        return;
    }
    sqlx::query(&format!("CREATE TABLE {schema}.other_work (n int)"))
        .execute(&pool)
        .await
        .unwrap();
    let gid = format!("pg-bus-test-{}", std::process::id());
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
    sqlx::query(&format!("INSERT INTO {schema}.other_work VALUES (1)"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query(&format!("PREPARE TRANSACTION '{gid}'"))
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    publish(&bus, &pool, "/a", json!("waiting"), None).await;

    let health = bus.health().await.unwrap();
    let found = health
        .blockers
        .iter()
        .any(|b| b.prepared.as_deref() == Some(gid.as_str()) && b.pid.is_none());
    // Clean up before asserting, so a failure leaves nothing behind.
    sqlx::query(&format!("ROLLBACK PREPARED '{gid}'"))
        .execute(&pool)
        .await
        .unwrap();
    assert!(found, "{gid} not among {:?}", health.blockers);
    let report = pg_bus::stall_report(&health, Duration::ZERO).unwrap();
    assert!(
        report.contains(&format!("prepared transaction {gid:?}")),
        "{report}"
    );
}

/// Publishing sends a NOTIFY, and Postgres will not PREPARE a transaction
/// that has: a transaction that publishes cannot use two-phase commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_that_publishes_cannot_be_prepared() {
    let (bus, pool) = bus().await;
    if !prepared_transactions_on(&pool).await {
        return;
    }
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
    bus.publish(&mut conn, "/a", &json!("x"), None)
        .await
        .unwrap();
    let error = sqlx::query("PREPARE TRANSACTION 'pg-bus-refused'")
        .execute(&mut *conn)
        .await
        .unwrap_err();
    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();
    assert!(error.to_string().contains("NOTIFY"), "{error}");
}
