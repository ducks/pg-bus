# Bench

`cargo run --release --example bench` (examples/bench.rs) against the
shell's PostgreSQL. Newest run first. Read the caveats before the numbers.

Caveats:

- One laptop, client and database on the same machine, no network.
  Other people's processes were running (load average around 4 to 6),
  so runs vary by a factor of about 2.
- Latency is publish (just before commit) to receipt by the subscriber,
  in one process. It includes the NOTIFY round trip and the listener's
  backlog read.
- The "whole database" rows count every transaction in the database
  (pg_stat_database), autovacuum included. They are an upper bound on
  what the listener costs, not a measurement of it.

## 2026-10-05

AMD Ryzen 9 7940HS (16 threads), 92 GB, NixOS, PostgreSQL 16.10 local,
pg-bus at the coalesced-notification fix.

| measure | unit | result |
|---|---|---|
| publish, 8 publishers, 1 message per transaction | 16000 messages | 4743 messages/s |
| latency, 1 subscriber, 500 messages/s | ms p50 / p99 / max | 1.5 / 2.8 / 3.4 |
| fan-out, 1 subscriber, 100 messages/s | ms p50 / p99 / max, RSS +0 MB | 1.5 / 2.6 / 2.9 |
| fan-out, 100 subscribers, 100 messages/s | ms p50 / p99 / max, RSS +0 MB | 1.7 / 3.0 / 3.1 |
| fan-out, 1000 subscribers, 100 messages/s | ms p50 / p99 / max, RSS +2 MB | 1.9 / 3.3 / 4.2 |
| whole database, idle (includes autovacuum) | transactions/s | 28.5 |
| whole database besides the publishes, 100 messages/s | transactions/s | 91.3 |

Before the fix, the same machine: 1109 messages/s, latency p50 / p99
10.4 / 24.7 ms. The listener took one buffered notification per round
of queries, so a burst of N commits cost N rounds long after it ended,
and that work competed with the publishers.

Under steady load the listener does about one query per publish: it
wakes for each commit's notification and reads the backlog. Commits
arriving faster than a round trip share a wake-up.
