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
use futures_util::stream;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{Bus, Error, Filter, Item, Position, Subscription};

/// The `Last-Event-ID` header as a position, if present and well formed.
pub fn last_event_id(headers: &HeaderMap) -> Option<Position> {
    headers.get("last-event-id")?.to_str().ok()?.parse().ok()
}

/// A subscription as an SSE response. Each message is a `message` event
/// with the position as its id and `{"channel", "data"}` as its data; a
/// gap is a `gap` event without an id. Comments every `heartbeat` keep
/// idle connections open. An error ends the stream; the client reconnects
/// with its last event id.
pub fn events(subscription: Subscription, heartbeat: Duration) -> Response {
    let stream = stream::unfold(Some(subscription), |state| async move {
        let mut sub = state?;
        match sub.next().await {
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
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(heartbeat))
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
