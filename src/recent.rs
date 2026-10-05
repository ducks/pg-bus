//! The listener's most recent messages, kept in memory so that a
//! subscriber resuming from a recent position (a browser reconnecting, a
//! wave of them after a deploy) catches up without querying the database.
//!
//! The ring holds every message the listener delivered after `from`, in
//! order, so it can answer for any position at or after `from`. When it is
//! full, the oldest message goes and `from` moves up to it.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::{Filter, Message, Position};

pub(crate) struct Recent {
    from: Position,
    messages: VecDeque<Arc<Message>>,
    capacity: usize,
}

impl Recent {
    /// An empty ring that answers for positions from `from`, where the
    /// listener starts.
    pub(crate) fn new(from: Position, capacity: usize) -> Recent {
        Recent {
            from,
            messages: VecDeque::with_capacity(capacity.min(4096)),
            capacity,
        }
    }

    /// Adds the next message the listener delivers.
    pub(crate) fn push(&mut self, message: Arc<Message>) {
        if self.capacity == 0 {
            self.from = message.position;
            return;
        }
        if self.messages.len() == self.capacity
            && let Some(oldest) = self.messages.pop_front()
        {
            self.from = oldest.position;
        }
        self.messages.push_back(message);
    }

    /// The messages after `after` that `filter` takes, at most `limit`, if
    /// the ring holds everything after `after`; `None` sends the caller
    /// to the database.
    pub(crate) fn after(
        &self,
        after: Position,
        filter: &Filter,
        limit: usize,
    ) -> Option<Vec<Message>> {
        if after < self.from {
            return None;
        }
        // Positions increase along the ring: skip what is at or before
        // `after` by binary search.
        let start = self.messages.partition_point(|m| m.position <= after);
        Some(
            self.messages
                .range(start..)
                .filter(|m| filter.matches(m))
                .take(limit)
                .map(|m| (**m).clone())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(id: i64, channel: &str) -> Arc<Message> {
        Arc::new(Message {
            position: format!("100-{id}").parse().unwrap(),
            channel: channel.into(),
            data: json!(id),
            audience: None,
        })
    }

    fn filter(channels: &[&str]) -> Filter {
        Filter {
            channels: channels.iter().map(|c| c.to_string()).collect(),
            tags: Vec::new(),
        }
    }

    fn ids(messages: &[Message]) -> Vec<i64> {
        messages.iter().map(|m| m.data.as_i64().unwrap()).collect()
    }

    #[test]
    fn answers_from_its_start_and_sends_older_positions_away() {
        let start: Position = "100-0".parse().unwrap();
        let mut ring = Recent::new(start, 3);
        for id in 1..=5 {
            ring.push(message(id, "/a"));
        }
        // Holds 3, 4, 5: complete after 2.
        let a = filter(&["/a"]);
        assert_eq!(ring.after(start, &a, 10), None);
        assert_eq!(ring.after("100-1".parse().unwrap(), &a, 10), None);
        assert_eq!(
            ids(&ring.after("100-2".parse().unwrap(), &a, 10).unwrap()),
            vec![3, 4, 5]
        );
        assert_eq!(
            ids(&ring.after("100-4".parse().unwrap(), &a, 10).unwrap()),
            vec![5]
        );
        assert_eq!(
            ids(&ring.after("100-5".parse().unwrap(), &a, 10).unwrap()),
            Vec::<i64>::new()
        );
        // Past what the listener has delivered: nothing yet, the feed
        // brings the rest.
        assert_eq!(
            ids(&ring.after("100-9".parse().unwrap(), &a, 10).unwrap()),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn filters_and_limits() {
        let start: Position = "100-0".parse().unwrap();
        let mut ring = Recent::new(start, 10);
        for id in 1..=6 {
            ring.push(message(id, if id % 2 == 0 { "/even" } else { "/odd" }));
        }
        assert_eq!(
            ids(&ring.after(start, &filter(&["/even"]), 10).unwrap()),
            vec![2, 4, 6]
        );
        assert_eq!(
            ids(&ring.after(start, &filter(&["/odd"]), 2).unwrap()),
            vec![1, 3]
        );
    }

    #[test]
    fn a_ring_of_none_only_answers_at_the_listener() {
        let start: Position = "100-0".parse().unwrap();
        let mut ring = Recent::new(start, 0);
        ring.push(message(1, "/a"));
        let a = filter(&["/a"]);
        assert_eq!(ring.after(start, &a, 10), None);
        assert_eq!(
            ids(&ring.after("100-1".parse().unwrap(), &a, 10).unwrap()),
            Vec::<i64>::new()
        );
    }
}
