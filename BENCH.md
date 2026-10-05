# Bench

`cargo run --release --example bench` (examples/bench.rs) against the
shell's PostgreSQL. Newest run first. Read the caveats before the numbers.

Caveats:

- One laptop, client and database on the same machine, no network.
  Other people's processes were running (load average around 6), so runs
  vary by a factor of about 2.
- Latency is publish (just before commit) to receipt by the subscriber,
  in one process. It includes the NOTIFY round trip and the listener's
  backlog read.
- The listener rows count its own queries (Bus::stats). At a steady rate
  it wakes once per message and runs a backlog read and a waiting check;
  commits that arrive within one round trip share a wake-up.

## 2026-10-05, listener draining every pending notification

AMD Ryzen 9 7940HS (16 threads), 92 GB, NixOS, PostgreSQL 16.10 local.

| measure | unit | result |
|---|---|---|
| publish, 8 publishers, 1 message per transaction | 16000 messages | 4158 messages/s |
| latency, 1 subscriber, 500 messages/s | ms p50 / p99 / max | 2.1 / 4.1 / 5.5 |
| fan-out, 1 subscriber, 100 messages/s | ms p50 / p99 / max, RSS +0 MB | 1.9 / 2.3 / 2.7 |
| fan-out, 100 subscribers, 100 messages/s | ms p50 / p99 / max, RSS +0 MB | 1.9 / 2.2 / 2.6 |
| fan-out, 1000 subscribers, 100 messages/s | ms p50 / p99 / max, RSS +2 MB | 2.2 / 2.6 / 4.3 |
| listener, idle | queries/s | 0.00 |
| listener, 100 messages/s | queries/s, per message | 200.0, 2.00 |

## How the listener's cost came down

Each committed transaction sends one NOTIFY. At first the listener ran a
round of queries per notification, so a burst of N commits cost N rounds
long after it ended: 1109 messages/s published, latency p50/p99
10.4/24.7 ms, while it worked through the queue. Taking the buffered
notifications in one go (PgListener::next_buffered) helped: 4743
messages/s, 1.5/2.8 ms. It was not the whole fix: next_buffered only
returns what sqlx has already read, and the rest waited unread on the
socket, so the listener still ran about 1077 queries a second for
seconds after a burst, which the counters added in Bus::stats showed.
Now it also takes everything already on the socket, and after a burst of
1600 messages it does 798 queries in all and none afterwards.
