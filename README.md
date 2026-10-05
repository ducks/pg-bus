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

Status: in development.

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

## Development

```bash
nix-shell
db_start      # local PostgreSQL on port 5443 with a pg_bus_test database
make test
make lint     # fmt-check, clippy, tests
```

## License

MIT
