//! Unit tests for the daemon→client reverse JSON-RPC channel (§12.4).

use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::*;

#[test]
fn screenshot_requests_use_a_shorter_inner_deadline() {
    let timeout = request_timeout(
        "browser.exec",
        &json!({ "actions": [{ "action": "screenshot" }] }),
    );
    assert_eq!(timeout, SCREENSHOT_REVERSE_TIMEOUT);
    assert!(timeout < DEFAULT_REVERSE_TIMEOUT);

    assert_eq!(
        request_timeout(
            "browser.exec",
            &json!({
                "actions": [
                    { "action": "listTabs" },
                    { "action": "screenshot" }
                ]
            }),
        ),
        SCREENSHOT_REVERSE_TIMEOUT,
    );
}

#[test]
fn unrelated_reverse_requests_keep_the_default_deadline() {
    assert_eq!(
        request_timeout(
            "browser.exec",
            &json!({ "actions": [{ "action": "listTabs" }] }),
        ),
        DEFAULT_REVERSE_TIMEOUT,
    );
    assert_eq!(
        request_timeout(
            "host.openExternal",
            &json!({ "actions": [{ "action": "screenshot" }] }),
        ),
        DEFAULT_REVERSE_TIMEOUT,
    );
}

#[tokio::test]
async fn request_round_trips_through_a_mock_client() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        caller
            .request(
                "host.openExternal",
                json!({ "url": "http://localhost:3000" }),
                Duration::from_secs(5),
            )
            .await
    });

    // The mock client receives the reverse request and replies success.
    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["method"], "host.openExternal");
    assert_eq!(req["params"]["url"], "http://localhost:3000");
    let id = req["id"].as_str().unwrap();
    assert!(id.starts_with("rev-"), "reverse ids use the rev- prefix");

    let response = json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": true } });
    assert!(reverse.route_response(&response));

    let result = handle.await.unwrap().expect("client replied success");
    assert_eq!(result["ok"], true);
}

#[tokio::test]
async fn client_error_response_propagates() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        caller
            .request(
                "host.openExternal",
                json!({ "url": "x" }),
                Duration::from_secs(5),
            )
            .await
    });

    let frame = out_rx.recv().await.unwrap();
    let id = serde_json::from_str::<Value>(&frame).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let response = json!({
        "jsonrpc": "2.0", "id": id,
        "error": { "code": -32603, "message": "no handler" }
    });
    assert!(reverse.route_response(&response));

    let err = handle.await.unwrap().expect_err("client replied error");
    assert_eq!(err.code, -32603);
    assert_eq!(err.message, "no handler");
}

#[test]
fn route_response_ignores_non_replies() {
    let (out_tx, _out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);
    // A client request (has `method`) is not a reverse reply.
    assert!(!reverse.route_response(&json!({ "jsonrpc": "2.0", "id": "rev-1", "method": "x" })));
    // A reply for an unknown id is not consumed.
    assert!(!reverse.route_response(&json!({ "jsonrpc": "2.0", "id": "rev-99", "result": {} })));
    // A non-string id (a normal client response shape) is not consumed.
    assert!(!reverse.route_response(&json!({ "jsonrpc": "2.0", "id": 7, "result": {} })));
}

#[tokio::test]
async fn request_times_out_without_a_reply() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);
    // Keep the receiver alive but never reply.
    tokio::spawn(async move { while out_rx.recv().await.is_some() {} });
    let err = reverse
        .request(
            "host.openExternal",
            json!({ "url": "x" }),
            Duration::from_millis(20),
        )
        .await
        .expect_err("must time out");
    assert!(err.message.contains("timed out"));
    assert!(reverse.pending.lock().unwrap().is_empty());
    assert!(
        !reverse.route_response(&json!({
            "jsonrpc": "2.0", "id": "rev-1", "result": { "late": true }
        })),
        "a late response cannot match a timed-out request"
    );
}

#[tokio::test]
async fn request_timeout_includes_outbound_queue_wait() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(1);
    let reverse = ReverseChannel::new(out_tx);
    reverse
        .out_tx
        .send("occupied".to_string())
        .await
        .expect("fill outbound queue");

    let started = tokio::time::Instant::now();
    let err = reverse
        .request(
            "browser.exec",
            json!({ "actions": [{ "action": "screenshot" }] }),
            Duration::from_millis(30),
        )
        .await
        .expect_err("queue wait must time out");
    assert!(err.message.contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(reverse.pending.lock().unwrap().is_empty());
    assert_eq!(out_rx.recv().await.as_deref(), Some("occupied"));
    assert!(out_rx.try_recv().is_err(), "timed-out frame was cancelled");
}

#[tokio::test]
async fn request_fails_when_connection_closed() {
    let (out_tx, out_rx) = mpsc::channel::<String>(8);
    drop(out_rx);
    let reverse = ReverseChannel::new(out_tx);
    let err = reverse
        .request(
            "host.openExternal",
            json!({ "url": "x" }),
            Duration::from_secs(5),
        )
        .await
        .expect_err("closed connection fails");
    assert!(err.message.contains("closed"));
    assert!(reverse.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn blocked_request_fails_when_connection_closes() {
    let (out_tx, out_rx) = mpsc::channel::<String>(1);
    let reverse = ReverseChannel::new(out_tx);
    reverse
        .out_tx
        .send("occupied".to_string())
        .await
        .expect("fill outbound queue");

    let caller = reverse.clone();
    let request = tokio::spawn(async move {
        caller
            .request(
                "browser.exec",
                json!({ "actions": [{ "action": "screenshot" }] }),
                Duration::from_secs(5),
            )
            .await
    });
    while reverse.pending.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    drop(out_rx);

    let err = request
        .await
        .expect("join")
        .expect_err("closed connection wakes blocked sender");
    assert!(err.message.contains("closed"));
    assert!(reverse.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn accepted_request_fails_when_connection_closes() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(1);
    let reverse = ReverseChannel::new(out_tx);
    let caller = reverse.clone();
    let request = tokio::spawn(async move {
        caller
            .request(
                "browser.exec",
                json!({ "actions": [{ "action": "screenshot" }] }),
                Duration::from_secs(5),
            )
            .await
    });
    let frame = out_rx.recv().await.expect("accepted reverse frame");
    assert!(frame.contains("screenshot"));

    reverse.close();
    let err = request
        .await
        .expect("join")
        .expect_err("connection close wakes response waiter");
    assert!(err.message.contains("closed"));
    assert!(reverse.pending.lock().unwrap().is_empty());
}
