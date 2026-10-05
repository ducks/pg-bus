//! A subscriber: the backlog from its position first, then the live feed.
//!
//! The feed is joined before the backlog is read, so a message is in one
//! or the other (or both, and then skipped the second time: delivery is in
//! position order, and anything at or before the position has been seen).
//! A subscriber that falls more than the feed's capacity behind goes back
//! to the backlog from its position.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::stats::Counters;
use crate::{Bus, Error, Filter, Message, Position};

/// Messages read from the backlog per query.
const BATCH: i64 = 500;

/// What a subscription yields.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Message(Message),
    /// Messages after the starting position were trimmed from the backlog
    /// before they could be read: the subscriber has missed some and should
    /// reload its state. Yielded once, first; delivery goes on after it.
    Gap,
}

pub struct Subscription {
    bus: Bus,
    filter: Filter,
    position: Position,
    feed: broadcast::Receiver<Arc<Message>>,
    queue: VecDeque<Message>,
    catching_up: bool,
    gap_checked: bool,
}

impl Subscription {
    pub(crate) fn new(bus: Bus, from: Position, filter: Filter) -> Subscription {
        let feed = bus.feed();
        Subscription {
            bus,
            filter,
            position: from,
            feed,
            queue: VecDeque::new(),
            catching_up: true,
            gap_checked: false,
        }
    }

    /// The last position this subscription has passed, delivered or not:
    /// resuming from it misses nothing.
    pub fn position(&self) -> Position {
        self.position
    }

    /// The next item, waiting for one if none is due.
    ///
    /// Cancel-safe: state only changes after each await completes, so
    /// dropping the future (a timeout) loses nothing and the next call
    /// picks up where this one was.
    pub async fn next(&mut self) -> Result<Item, Error> {
        if !self.gap_checked {
            // Held in memory from here on: nothing after it was trimmed
            // away, and no query is needed to say so.
            let in_memory = self
                .bus
                .recent_after(self.position, &self.filter, 0)
                .is_some();
            let trimmed = !in_memory && self.bus.trimmed_after(self.position).await?;
            self.gap_checked = true;
            if trimmed {
                return Ok(Item::Gap);
            }
        }
        loop {
            if let Some(message) = self.queue.pop_front() {
                // Delivery is in strictly increasing position: anything
                // else is a bug, loud in tests, skipped in release.
                if message.position <= self.position {
                    debug_assert!(
                        false,
                        "pg-bus: {} delivered after {} ({})",
                        message.position, self.position, message.channel
                    );
                    Counters::add(&self.bus.counters().out_of_order, 1);
                    tracing::error!(
                        "pg-bus: skipped {} after {}: out of order",
                        message.position,
                        self.position
                    );
                    continue;
                }
                self.position = message.position;
                return Ok(Item::Message(message));
            }
            if self.catching_up {
                // From memory when this process still holds everything after
                // the position, else from the backlog table.
                let batch = match self
                    .bus
                    .recent_after(self.position, &self.filter, BATCH as usize)
                {
                    Some(batch) => {
                        Counters::add(&self.bus.counters().catch_ups_from_memory, 1);
                        batch
                    }
                    None => {
                        let batch = self.bus.backlog(self.position, &self.filter, BATCH).await?;
                        Counters::add(&self.bus.counters().catch_ups_from_database, 1);
                        tracing::debug!(
                            "pg-bus: caught up {} message(s) from the backlog table after {}",
                            batch.len(),
                            self.position
                        );
                        batch
                    }
                };
                if (batch.len() as i64) < BATCH {
                    self.catching_up = false;
                }
                self.queue.extend(batch);
                continue;
            }
            match self.feed.recv().await {
                Ok(message) => {
                    if message.position <= self.position {
                        continue;
                    }
                    if self.filter.matches(&message) {
                        self.queue.push_back((*message).clone());
                    } else {
                        self.position = message.position;
                    }
                }
                // Fell behind the feed: the backlog has everything after
                // the position.
                Err(RecvError::Lagged(n)) => {
                    Counters::add(&self.bus.counters().lagged, 1);
                    tracing::debug!("pg-bus: subscriber fell {n} message(s) behind the feed");
                    self.catching_up = true;
                }
                Err(RecvError::Closed) => return Err(Error::Closed),
            }
        }
    }
}
