//! The axum building blocks, wired the way an application would: it picks
//! the position and the filter, pg-bus delivers.
#![cfg(feature = "axum")]

mod common;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::Response;
use axum::routing::get;
use common::{bus, everyone, publish, settled};
use http_body_util::BodyExt;
use pg_bus::sse::{Options, events, last_event_id, poll};
use pg_bus::{Bus, Position};
use serde::Deserialize;
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Deserialize)]
struct Since {
    since: Option<String>,
    /// The stream's max_lifetime, for the tests that end one.
    lifetime_ms: Option<u64>,
}

/// The resume position: Last-Event-ID on a reconnect, else the position
/// the page was rendered at, else now.
async fn from(bus: &Bus, headers: &HeaderMap, since: Option<String>) -> Position {
    match last_event_id(headers).or_else(|| since.and_then(|s| s.parse().ok())) {
        Some(p) => p,
        None => bus.now().await.unwrap(),
    }
}

fn app(bus: Bus) -> Router {
    Router::new()
        .route(
            "/events",
            get(
                |State(bus): State<Bus>, headers: HeaderMap, Query(q): Query<Since>| async move {
                    let from = from(&bus, &headers, q.since).await;
                    let options = Options {
                        heartbeat: Duration::from_millis(200),
                        max_lifetime: q.lifetime_ms.map(Duration::from_millis),
                        retry: Some(Duration::from_millis(1500)),
                    };
                    events(bus.subscribe(from, everyone(&["/chat"])), options)
                },
            ),
        )
        .route(
            "/poll",
            get(
                |State(bus): State<Bus>, headers: HeaderMap, Query(q): Query<Since>| async move {
                    let from = from(&bus, &headers, q.since).await;
                    let polled = poll(&bus, from, everyone(&["/chat"]), Duration::from_millis(500))
                        .await
                        .unwrap();
                    axum::Json(polled)
                },
            ),
        )
        .with_state(bus)
}

async fn get_response(bus: &Bus, uri: &str, headers: &[(&str, &str)]) -> Response {
    let mut request = Request::get(uri);
    for (k, v) in headers {
        request = request.header(*k, *v);
    }
    app(bus.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// Reads the event stream until `until` appears in it (or 10 s pass).
async fn read_until(response: &mut Response, until: &str) -> String {
    let mut text = String::new();
    let body = response.body_mut();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !text.contains(until) {
        let frame = tokio::time::timeout_at(deadline, body.frame())
            .await
            .unwrap_or_else(|_| panic!("no {until:?} in {text:?}"))
            .expect("the stream ended")
            .unwrap();
        if let Ok(data) = frame.into_data() {
            text.push_str(std::str::from_utf8(&data).unwrap());
        }
    }
    text
}

/// The `id:` and `data:` of the first message event in a stream's text.
fn first_message(text: &str) -> (String, Value) {
    let event = text
        .split("\n\n")
        .find(|e| e.contains("event: message"))
        .unwrap();
    let field = |name: &str| {
        event
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name}: ")))
            .unwrap()
            .to_string()
    };
    (field("id"), serde_json::from_str(&field("data")).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn events_stream_messages_with_their_positions_as_ids() {
    let (bus, pool) = bus().await;
    let mut response = get_response(&bus, "/events", &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["x-accel-buffering"], "no");
    assert_eq!(headers["cache-control"], "no-cache");

    publish(&bus, &pool, "/elsewhere", json!("not followed"), None).await;
    publish(&bus, &pool, "/chat", json!({"text": "hi"}), None).await;
    let text = read_until(&mut response, "event: message").await;
    let (id, data) = first_message(&text);
    assert_eq!(data, json!({"channel": "/chat", "data": {"text": "hi"}}));
    assert!(id.parse::<Position>().is_ok(), "{id}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_with_last_event_id_gets_what_it_missed() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap();
    publish(&bus, &pool, "/chat", json!(1), None).await;
    publish(&bus, &pool, "/chat", json!(2), None).await;
    let seen = settled(&bus, start, &everyone(&["/chat"]), 2).await;

    let last = seen[0].position.to_string();
    let mut response = get_response(&bus, "/events", &[("last-event-id", &last)]).await;
    let text = read_until(&mut response, "event: message").await;
    let (id, data) = first_message(&text);
    assert_eq!(data["data"], json!(2));
    assert_eq!(id, seen[1].position.to_string());
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_streams_send_heartbeats() {
    let (bus, _pool) = bus().await;
    let mut response = get_response(&bus, "/events", &[]).await;
    // After the retry, axum's keep-alive: an empty comment line.
    let text = read_until(&mut response, "\n\n:").await;
    assert!(text.starts_with("retry: 1500\n\n:"), "{text:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn polls_answer_at_once_wait_for_the_next_or_time_out() {
    let (bus, pool) = bus().await;
    let start = bus.now().await.unwrap().to_string();

    // Nothing comes: an empty answer after the wait, at the same position.
    let response = get_response(&bus, &format!("/poll?since={start}"), &[]).await;
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["messages"], json!([]));
    assert_eq!(body["gap"], json!(false));

    // One arrives during the wait.
    let publisher = {
        let (bus, pool) = (bus.clone(), pool.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            publish(&bus, &pool, "/chat", json!("during"), None).await;
        })
    };
    let response = get_response(&bus, &format!("/poll?since={start}"), &[]).await;
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    publisher.await.unwrap();
    assert_eq!(body["messages"][0]["data"], json!("during"));
    let next = body["position"].as_str().unwrap().to_string();

    // Already waiting: answered at once, from the returned position.
    publish(&bus, &pool, "/chat", json!("waiting"), None).await;
    let response = get_response(&bus, &format!("/poll?since={next}"), &[]).await;
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let data: Vec<Value> = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["data"].clone())
        .collect();
    assert_eq!(data, vec![json!("waiting")]);
}

#[test]
fn last_event_id_ignores_missing_and_malformed_headers() {
    let mut headers = HeaderMap::new();
    assert_eq!(last_event_id(&headers), None);
    headers.insert("last-event-id", "nonsense".parse().unwrap());
    assert_eq!(last_event_id(&headers), None);
    headers.insert("last-event-id", "7-3".parse().unwrap());
    assert_eq!(last_event_id(&headers), Some("7-3".parse().unwrap()));
}

/// Reads the stream until it ends (or 10 s pass); the text it carried.
async fn read_to_end(response: &mut Response) -> String {
    let mut text = String::new();
    let body = response.body_mut();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout_at(deadline, body.frame())
            .await
            .unwrap_or_else(|_| panic!("still open after 10 s: {text:?}"))
        {
            None => return text,
            Some(frame) => {
                if let Ok(data) = frame.unwrap().into_data() {
                    text.push_str(std::str::from_utf8(&data).unwrap());
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streams_say_how_soon_to_reconnect_first() {
    let (bus, _pool) = bus().await;
    let mut response = get_response(&bus, "/events", &[]).await;
    let text = read_until(&mut response, "\n\n").await;
    assert!(text.starts_with("retry: 1500\n"), "{text:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn streams_end_after_their_lifetime_and_resume_without_loss() {
    let (bus, pool) = bus().await;
    let started = tokio::time::Instant::now();
    let mut response = get_response(&bus, "/events?lifetime_ms=400", &[]).await;
    publish(&bus, &pool, "/chat", json!("before"), None).await;
    let text = read_to_end(&mut response).await;
    let lived = started.elapsed();
    // 400 ms, within 10% either way (plus the time to answer).
    assert!(lived >= Duration::from_millis(360), "{lived:?}");
    assert!(lived < Duration::from_secs(3), "{lived:?}");
    let (id, data) = first_message(&text);
    assert_eq!(data["data"], json!("before"));

    // Published while disconnected: the reconnect gets it.
    publish(&bus, &pool, "/chat", json!("between"), None).await;
    let mut again = get_response(&bus, "/events", &[("last-event-id", &id)]).await;
    let text = read_until(&mut again, "event: message").await;
    assert_eq!(first_message(&text).1["data"], json!("between"));
}
