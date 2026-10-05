# pg-bus

A message bus on PostgreSQL, in the spirit of Discourse's MessageBus:
channels with a backlog, subscribers that resume from where they left off,
and per-message audiences, without Redis.

- **Transactional publish.** A message is a row written in the caller's
  transaction: it exists only if that transaction commits.
- **Resumable.** Subscribers hold a cursor and catch up from the backlog
  after a reconnect; nothing committed is skipped.
- **Audiences.** A message can be limited to subscribers holding any of
  its tags (`user:3`, `group:10`); the application decides which tags a
  subscriber holds.
- **SSE first.** Delivered as server-sent events with `Last-Event-ID`,
  with a long-poll fallback.

## Install

```toml
[dependencies]
pg-bus = "20261005"
# with server-sent events and long polls for axum:
pg-bus = { version = "20261005", features = ["axum"] }
```

Versions are dates (YYYYMMDD.0.N); each release is a new major version.
Needs PostgreSQL 13 or newer and Rust 1.88 or newer. The bus creates its
schema (`pg_bus` by default) on start.

## Use

```rust
use pg_bus::{Bus, Config, Filter};

let bus = Bus::start(pool.clone(), Config::default()).await?;

// Publishing: inside the transaction that makes the change.
let mut tx = pool.begin().await?;
// ... insert the post ...
bus.publish(&mut tx, "/topic/35", &json!({"post_id": 52}), None).await?;
let audience = ["user:3".to_string()];
bus.publish(&mut tx, "/notification/3", &json!({"unread": 4}), Some(&audience)).await?;
tx.commit().await?; // delivered from here; a rollback delivers nothing

// Rendering a page: hand the client the position to start from.
let since = bus.now().await?;

// Subscribing: the application decides channels and tags.
let filter = Filter {
    channels: vec!["/topic/35".into(), "/notification/3".into()],
    tags: vec!["user:3".into(), "group:10".into()],
};
let mut sub = bus.subscribe(since, filter);
while let Ok(item) = sub.next().await { /* Item::Message or Item::Gap */ }
```

With the `axum` feature, `pg_bus::sse` turns a subscription into a
server-sent events response (`events`, ids are positions, so a browser's
`EventSource` resumes by itself), reads `Last-Event-ID`
(`last_event_id`), and answers long polls (`poll`). Streams end after
`Options::max_lifetime` (10 minutes by default, jittered), so a reconnect
re-runs the application's access checks.

Ordering: a message is delivered once every older write transaction has
ended, so a cursor never skips one that commits late. A long-running write
transaction anywhere in the database delays delivery until it ends.

## Limits

- **PostgreSQL 13 or newer.** Positions use `xid8` and
  `pg_current_xact_id`; `Bus::start` refuses older servers.
- **One listening connection per process,** held open for LISTEN. It must
  reach PostgreSQL directly or through a pooler in session mode: LISTEN
  does not work through pgbouncer in transaction mode. When the pool goes
  through one, set `Config::listen_url` to a direct URL for the listener.
- **Publish throughput is bounded by NOTIFY.** A transaction that sends a
  notification takes a global lock at commit, so publishing transactions
  commit one at a time. BENCH.md has about 4,000 single-message
  transactions a second on a laptop; batching several messages into one
  transaction sends one notification.
- **No two-phase commit.** Publishing queues a NOTIFY, and PostgreSQL
  refuses to PREPARE a transaction that has one.
- **Long write transactions delay delivery.** A message is delivered once
  every older write transaction in the database has ended, so one left
  open (idle in transaction, a forgotten prepared transaction) holds back
  everything after it. Nothing is lost, and `Bus::health` and the
  listener's warning name the transaction.
- **Gaps are not per channel.** A subscriber resuming from before a trim
  is told it may have missed messages even when the trim was on another
  channel. It reloads when it need not, never the other way round.
- **Memory:** each process keeps its most recent messages
  (`Config::recent`, default 10,000) for catch-ups, and each subscription
  queues at most a batch.
- **Channels match exactly;** there are no wildcards or prefixes.
- **One order across channels:** delivery follows transaction ids (roughly
  when each transaction first wrote), the same for every subscriber; a
  transaction that commits late is held back, not delivered out of turn.
  There are no per-channel sequence numbers.

## Development

```bash
nix-shell
db_start      # local PostgreSQL on port 5443 with a pg_bus_test database
make test
make lint     # fmt-check, clippy, tests
```

## License

MIT
