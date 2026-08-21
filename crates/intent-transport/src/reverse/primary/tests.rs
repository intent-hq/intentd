//! Unit tests for [`PrimaryReverseRegistry`] (REV-1): sticky "first-client
//! wins" ordering, RAII deregistration, and the `AgentReverseDispatch`
//! `NoClient` failure mode. Channel identity is checked functionally — a
//! `dispatch` call sends the outbound frame to the primary channel's queue
//! and no other, so the receivers themselves witness the routing decision.

use std::time::Duration;

use intent_core::{AgentReverseDispatch, ReverseDispatchError};
use serde_json::json;
use tokio::sync::mpsc;

use super::PrimaryReverseRegistry;
use crate::reverse::ReverseChannel;

/// Build a `ReverseChannel` whose outbound queue is deep enough that
/// `request()` succeeds up to the timeout.
fn idle_channel() -> (ReverseChannel, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel::<String>(4);
    (ReverseChannel::new(tx), rx)
}

/// Drive one `dispatch` and answer with `result` via `route_response` on the
/// receiver whose channel is expected to be primary. Returns the observed
/// reply.
async fn dispatch_and_reply(
    reg: &PrimaryReverseRegistry,
    channel: &ReverseChannel,
    rx: &mut mpsc::Receiver<String>,
    result: serde_json::Value,
) -> Result<serde_json::Value, ReverseDispatchError> {
    let dispatch = tokio::spawn({
        let reg = reg.clone();
        async move { reg.dispatch("browser.exec", json!({ "actions": [] })).await }
    });
    let frame = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("frame arrives")
        .expect("outbound frame");
    let value: serde_json::Value = serde_json::from_str(&frame).expect("json");
    let id = value
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    channel.route_response(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    }));
    dispatch.await.expect("join")
}

#[test]
fn empty_registry_reports_no_primary() {
    let reg = PrimaryReverseRegistry::new();
    assert!(reg.primary().is_none());
    assert!(!reg.is_connected());
}

#[tokio::test]
async fn dispatch_routes_to_the_first_registered_channel() {
    let reg = PrimaryReverseRegistry::new();
    let (a, mut rx_a) = idle_channel();
    let (_b, mut rx_b) = idle_channel();
    let _g_a = reg.register(a.clone());
    let _g_b = reg.register(_b);
    assert_eq!(reg.len(), 2);

    let out = dispatch_and_reply(&reg, &a, &mut rx_a, json!({ "primary": "a" }))
        .await
        .expect("ok");
    assert_eq!(out, json!({ "primary": "a" }));
    // The secondary channel must never see the outbound frame.
    assert!(rx_b.try_recv().is_err());
}

#[tokio::test]
async fn dropping_the_primary_promotes_the_next_registration() {
    let reg = PrimaryReverseRegistry::new();
    let (_a, mut _rx_a) = idle_channel();
    let (b, mut rx_b) = idle_channel();
    let g_a = reg.register(_a);
    let _g_b = reg.register(b.clone());
    drop(g_a);
    assert_eq!(reg.len(), 1);

    let out = dispatch_and_reply(&reg, &b, &mut rx_b, json!({ "primary": "b" }))
        .await
        .expect("ok");
    assert_eq!(out, json!({ "primary": "b" }));
}

#[tokio::test]
async fn dropping_a_non_primary_leaves_the_head_unchanged() {
    let reg = PrimaryReverseRegistry::new();
    let (a, mut rx_a) = idle_channel();
    let (b, mut _rx_b) = idle_channel();
    let _g_a = reg.register(a.clone());
    let g_b = reg.register(b);
    drop(g_b);
    assert_eq!(reg.len(), 1);

    let out = dispatch_and_reply(&reg, &a, &mut rx_a, json!({ "primary": "a" }))
        .await
        .expect("ok");
    assert_eq!(out, json!({ "primary": "a" }));
}

#[tokio::test]
async fn dispatch_without_clients_reports_no_client() {
    let reg = PrimaryReverseRegistry::new();
    let err = reg
        .dispatch("browser.exec", json!({}))
        .await
        .expect_err("no client");
    assert_eq!(err, ReverseDispatchError::NoClient);
}

#[tokio::test]
async fn dispatch_reports_transport_error_when_channel_is_closed() {
    let reg = PrimaryReverseRegistry::new();
    let (a, rx_a) = idle_channel();
    let _g_a = reg.register(a);
    // Drop the receiver so the outbound queue is closed; `request` should
    // surface a transport error rather than `NoClient`.
    drop(rx_a);
    let err = reg
        .dispatch("browser.exec", json!({}))
        .await
        .expect_err("transport error");
    match err {
        ReverseDispatchError::Transport { .. } => {}
        other @ ReverseDispatchError::NoClient => panic!("unexpected error: {other:?}"),
    }
}
