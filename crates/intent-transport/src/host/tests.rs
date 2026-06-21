//! Unit tests for the `host.*` capability probe fast-path (§5.14).

use serde_json::{json, Value};

use super::*;

#[test]
fn resolve_locality_per_transport_and_override() {
    // No override ⇒ the transport default decides (UDS local, TCP/WSS remote).
    assert!(resolve_is_local(true, None), "UDS ⇒ local");
    assert!(!resolve_is_local(false, None), "TCP/WSS ⇒ remote");
    // `--mode local` / `server.locality=local` forces local even over TCP/WSS.
    assert!(resolve_is_local(false, Some(true)), "override forces local");
    // `--mode remote` / `server.locality=remote` forces remote even over UDS.
    assert!(
        !resolve_is_local(true, Some(false)),
        "override forces remote"
    );
}

#[test]
fn classify_only_matches_host_status() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.status" })).is_some());
    // Notification shape (no id) still classifies; handling returns no frame.
    assert!(classify(&json!({ "jsonrpc": "2.0", "method": "host.status" })).is_some());
    // Other methods / wrong version / bad id fall through.
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.openExternal" })).is_none()
    );
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "host.status" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": [1], "method": "host.status" })).is_none());
}

#[test]
fn status_json_local_includes_all_fields() {
    let v = host_status_json("linux", "x86_64", "build-01", true, Some("wayland"), true);
    assert_eq!(v["os"], "linux");
    assert_eq!(v["arch"], "x86_64");
    assert_eq!(v["hostname"], "build-01");
    assert_eq!(v["hasDisplay"], true);
    assert_eq!(v["locality"], "local");
    assert_eq!(v["displayServer"], "wayland");
}

#[test]
fn status_json_remote_omits_absent_display_server() {
    let v = host_status_json("linux", "x86_64", "build-01", false, None, false);
    assert_eq!(v["locality"], "remote");
    assert_eq!(v["hasDisplay"], false);
    assert_eq!(v.get("displayServer"), None, "omitted when not detected");
}

#[test]
fn handle_status_returns_a_response_frame() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 7, "method": "host.status" })).unwrap();
    let frame = handle(req, true).expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 7);
    assert_eq!(parsed["result"]["locality"], "local");
    assert!(parsed["result"]["os"].is_string());
    assert!(parsed["result"]["arch"].is_string());
    assert!(parsed["result"]["hostname"].is_string());
    assert!(parsed["result"]["hasDisplay"].is_boolean());
}

#[test]
fn handle_remote_reports_remote_locality() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 8, "method": "host.status" })).unwrap();
    let frame = handle(req, false).expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["result"]["locality"], "remote");
}

#[test]
fn handle_notification_gets_no_response() {
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "host.status" })).unwrap();
    assert!(handle(req, true).is_none(), "a notification gets no reply");
}
