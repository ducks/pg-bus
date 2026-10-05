//! Counters for one process's bus, for metrics and debugging. Each counts
//! from when the bus started.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Position;

/// A snapshot of [`Bus::stats`](crate::Bus::stats).
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// Subscriptions alive in this process.
    pub subscribers: usize,
    /// The last position the listener read.
    pub listener_position: Position,
    /// Messages the listener read and handed to this process's subscribers.
    pub delivered: u64,
    /// Queries the listener ran: backlog reads and waiting-message checks.
    pub listener_queries: u64,
    /// Times the listener's connection was established again.
    pub listener_reconnects: u64,
    /// Subscription catch-ups answered from the recent ring.
    pub catch_ups_from_memory: u64,
    /// Subscription catch-ups that read the backlog table.
    pub catch_ups_from_database: u64,
    /// Times a subscriber fell behind the live feed and caught up instead.
    pub lagged: u64,
    /// Messages a subscription skipped because they came out of order.
    /// Always 0 unless something is wrong; logged as an error when not.
    pub out_of_order: u64,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) delivered: AtomicU64,
    pub(crate) listener_queries: AtomicU64,
    pub(crate) listener_reconnects: AtomicU64,
    pub(crate) catch_ups_from_memory: AtomicU64,
    pub(crate) catch_ups_from_database: AtomicU64,
    pub(crate) lagged: AtomicU64,
    pub(crate) out_of_order: AtomicU64,
    pub(crate) listener_position: Mutex<Option<Position>>,
}

impl Counters {
    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, subscribers: usize, start: Position) -> Stats {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        Stats {
            subscribers,
            listener_position: self
                .listener_position
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .unwrap_or(start),
            delivered: get(&self.delivered),
            listener_queries: get(&self.listener_queries),
            listener_reconnects: get(&self.listener_reconnects),
            catch_ups_from_memory: get(&self.catch_ups_from_memory),
            catch_ups_from_database: get(&self.catch_ups_from_database),
            lagged: get(&self.lagged),
            out_of_order: get(&self.out_of_order),
        }
    }
}
