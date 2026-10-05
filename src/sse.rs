//! Delivery over HTTP with axum (the `axum` feature): server-sent events,
//! and a long-poll fallback for networks that mangle streamed responses.
//!
//! These are building blocks, not routes: the application authenticates
//! the request and decides the [`Filter`] (channels, and the tags the
//! subscriber holds), then hands over.
//!
//! The starting position should come from the page the client loaded
//! ([`Bus::now`] when it was rendered), so nothing published between the
//! render and the connection is missed; after that the browser's
//! `EventSource` resends the last event id itself on every reconnect.

use std::convert::Infallible;
use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{StreamExt, stream};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{Bus, Error, Filter, Item, Position, Subscription};

/// The `Last-Event-ID` header as a position, if present and well formed.
pub fn last_event_id(headers: &HeaderMap) -> Option<Position> {
    headers.get("last-event-id")?.to_str().ok()?.parse().ok()
}

/// How an SSE stream runs.
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    /// A comment this often keeps idle connections (and proxies) open.
    pub heartbeat: Duration,
    /// The stream ends after about this long (within 10% either way, so
    /// clients that connected together do not all reconnect together).
    /// The browser reconnects with its last event id and the application
    /// checks the request again, so a subscriber who lost access to a
    /// channel stops getting it at the next reconnect. `None`: never.
    pub max_lifetime: Option<Duration>,
    /// Sent first, as the stream's `retry:`: how long a browser waits
    /// before reconnecting.
    pub retry: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            heartbeat: Duration::from_secs(25),
            max_lifetime: Some(Duration::from_secs(600)),
            retry: Some(Duration::from_secs(1)),
        }
    }
}

/// `lifetime` moved by up to 10% either way, by `spread` in [0, 1).
fn jittered(lifetime: Duration, spread: f64) -> Duration {
    lifetime.mul_f64(0.9 + 0.2 * spread.clamp(0.0, 1.0))
}

/// A spread in [0, 1) from the clock, different enough between
/// connections without a random number generator.
fn spread() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000_000) / 1_000_000.0
}

/// A subscription as an SSE response. Each message is a `message` event
/// with the position as its id and `{"channel", "data"}` as its data; a
/// gap is a `gap` event without an id. An error, or the end of its
/// lifetime, ends the stream; the client reconnects with its last event
/// id and misses nothing.
pub fn events(subscription: Subscription, options: Options) -> Response {
    let deadline = options
        .max_lifetime
        .map(|l| tokio::time::Instant::now() + jittered(l, spread()));
    let retry = options
        .retry
        .map(|r| Ok::<_, Infallible>(Event::default().retry(r)));
    let messages = stream::unfold(Some(subscription), move |state| async move {
        let mut sub = state?;
        let next = match deadline {
            // next is cancel-safe: ending here loses nothing.
            Some(at) => tokio::time::timeout_at(at, sub.next()).await.ok()?,
            None => sub.next().await,
        };
        match next {
            Ok(Item::Message(m)) => {
                let event = Event::default()
                    .event("message")
                    .id(m.position.to_string())
                    .data(json!({ "channel": m.channel, "data": m.data }).to_string());
                Some((Ok::<_, Infallible>(event), Some(sub)))
            }
            Ok(Item::Gap) => Some((Ok(Event::default().event("gap").data("{}")), Some(sub))),
            Err(e) => {
                tracing::warn!("{e}");
                None
            }
        }
    });
    let stream = stream::iter(retry).chain(messages);
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(options.heartbeat))
        .into_response();
    // nginx buffers proxied responses unless told not to.
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifetimes_spread_within_ten_percent() {
        let ten = Duration::from_secs(600);
        assert_eq!(jittered(ten, 0.0), Duration::from_secs(540));
        assert_eq!(jittered(ten, 0.5), Duration::from_secs(600));
        assert!(jittered(ten, 0.999_999) < Duration::from_secs(660));
        assert_eq!(jittered(ten, 7.0), Duration::from_secs(660));
        let s = spread();
        assert!((0.0..1.0).contains(&s));
    }
}

/// What a long poll answers: the messages after the position (possibly
/// none), the position to poll from next, and whether messages were
/// trimmed before they could be read.
#[derive(Debug, Serialize)]
pub struct Poll {
    pub messages: Vec<Polled>,
    pub position: String,
    pub gap: bool,
}

#[derive(Debug, Serialize)]
pub struct Polled {
    pub id: String,
    pub channel: String,
    pub data: Value,
}

/// Messages after `from`: at once if any are due, else the first that
/// arrives within `wait` along with any that came with it.
pub async fn poll(
    bus: &Bus,
    from: Position,
    filter: Filter,
    wait: Duration,
) -> Result<Poll, Error> {
    let mut sub = bus.subscribe(from, filter);
    let mut out = Poll {
        messages: Vec::new(),
        position: from.to_string(),
        gap: false,
    };
    let mut timeout = wait;
    while let Ok(item) = tokio::time::timeout(timeout, sub.next()).await {
        match item? {
            Item::Message(m) => out.messages.push(Polled {
                id: m.position.to_string(),
                channel: m.channel,
                data: m.data,
            }),
            Item::Gap => out.gap = true,
        }
        // After the first, only what is already due.
        if !out.messages.is_empty() {
            timeout = Duration::ZERO;
        }
    }
    out.position = sub.position().to_string();
    Ok(out)
}
