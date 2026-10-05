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

## Development

```bash
nix-shell
db_start      # local PostgreSQL on port 5443 with a pg_bus_test database
make test
make lint     # fmt-check, clippy, tests
```

## License

MIT
