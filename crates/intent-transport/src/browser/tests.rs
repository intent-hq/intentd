//! Unit tests for the transport-side `browser.exec` classifier + handler.

use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::*;
use crate::reverse::ReverseChannel;

/// Spawn a mock FE that reads one reverse-RPC frame off `out_rx` and replies
/// with `reply` (already carrying the right `id`). Returns the join handle so
/// the test can assert on the request the daemon actually forwarded. The
/// `out_rx.recv()` is wrapped in a fail-safe `tokio::time::timeout` (repo
/// convention) so a bug that never forwards the frame surfaces as a clear
/// test failure instead of hanging the runtime.
fn mock_fe_replies_with(
    mut out_rx: mpsc::Receiver<String>,
    reverse: ReverseChannel,
    reply: Value,
) -> tokio::task::JoinHandle<Value> {
    tokio::spawn(async move {
        let frame = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("daemon did not forward a reverse RPC within 2s")
            .expect("daemon forwarded a reverse RPC");
        let req: Value = serde_json::from_str(&frame).expect("valid JSON frame");
        let id = req["id"]
            .as_str()
            .expect("reverse id is a string")
            .to_string();
        let mut response = reply;
        if let Some(obj) = response.as_object_mut() {
            obj.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
            obj.insert("id".to_string(), Value::String(id));
        }
        assert!(reverse.route_response(&response));
        req
    })
}

#[tokio::test]
async fn classify_recognizes_browser_exec() {
    let value = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "browser.exec",
        "params": { "actions": [{ "action": "listTabs" }] }
    });
    let req = classify(&value).expect("browser.exec is classified");
    assert!(req.id_present);
    assert!(matches!(req.method, BrowserMethod::Exec));
    assert!(req.params.contains_key("actions"));
}

#[tokio::test]
async fn classify_ignores_other_methods() {
    let value = json!({
        "jsonrpc": "2.0", "id": 1, "method": "host.status"
    });
    assert!(classify(&value).is_none());
}

#[tokio::test]
async fn exec_forwards_and_reshapes_single_action() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [{ "id": "tab-1" }] }
        ]}
    });
    let mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": "tab-1",
        "agentId": "agent-1",
        "workspaceId": "ws-1"
    }))
    .unwrap();
    let shaped = exec(&params, &reverse).await.expect("exec succeeds");
    let forwarded = mock.await.unwrap();
    assert_eq!(forwarded["method"], "browser.exec");
    assert_eq!(forwarded["params"]["actions"][0]["action"], "listTabs");
    assert_eq!(forwarded["params"]["agentId"], "agent-1");
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    assert_eq!(shaped["action"], "listTabs");
    assert_eq!(shaped["result"][0]["id"], "tab-1");
}

#[tokio::test]
async fn exec_forwards_and_reshapes_multi_action() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [] },
            { "action": "screenshot", "success": true, "result": { "base64": "..." } }
        ]}
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }, { "action": "screenshot" }]
    }))
    .unwrap();
    let shaped = exec(&params, &reverse).await.expect("exec succeeds");
    let arr = shaped["results"].as_array().expect("results[] for multi");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[1]["action"], "screenshot");
}

#[tokio::test]
async fn exec_rejects_missing_actions_before_forwarding() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::Map::new();
    let err = exec(&params, &reverse).await.expect_err("must reject");
    assert_eq!(err.code(), browser_ops::INVALID_PARAMS);
    assert!(err.to_string().contains("actions"));
    // Nothing was written to the outbound queue — validation short-circuits.
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await,
        Err(_) | Ok(None)
    ));
}

#[tokio::test]
async fn exec_rejects_empty_actions_before_forwarding() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": []
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("must reject");
    assert_eq!(err.code(), browser_ops::INVALID_PARAMS);
    assert!(err.to_string().contains("empty"));
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await,
        Err(_) | Ok(None)
    ));
}

#[tokio::test]
async fn exec_surfaces_no_frontend_connected_as_proxy_error() {
    let (out_tx, out_rx) = mpsc::channel(8);
    // Drop the receiver immediately: mirrors "no frontend connected" — the
    // outbound queue has no reader, so the reverse channel's send fails.
    drop(out_rx);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse)
        .await
        .expect_err("no frontend connected");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("closed") || err.to_string().contains("browser.exec"));
}

#[tokio::test]
async fn exec_propagates_fe_error_as_proxy_error() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "error": { "code": -32603, "message": "CDP not attached" }
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("FE errored");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("CDP not attached"));
}

#[tokio::test]
async fn exec_surfaces_fe_failure_envelope() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": false, "error": "no tab focused", "results": [] }
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "screenshot" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("failure envelope");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("no tab focused"));
}
