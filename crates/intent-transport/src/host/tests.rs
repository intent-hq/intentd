//! Unit tests for the `host.*` capability probe fast-path (§5.14).

use std::sync::Mutex;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::*;
use crate::reverse::ReverseChannel;

/// An [`ExternalOpener`] that records opened URLs and can be told to fail.
struct RecordingOpener {
    ok: bool,
    opened: Mutex<Vec<String>>,
}

impl RecordingOpener {
    fn new(ok: bool) -> Self {
        Self {
            ok,
            opened: Mutex::new(Vec::new()),
        }
    }
}

impl ExternalOpener for RecordingOpener {
    fn open(&self, url: &str) -> Result<(), String> {
        self.opened.lock().unwrap().push(url.to_string());
        if self.ok {
            Ok(())
        } else {
            Err("os opener failed".to_string())
        }
    }
}

/// A reverse channel whose outbound queue is drained (and ignored).
fn idle_reverse() -> ReverseChannel {
    let (tx, mut rx) = mpsc::channel::<String>(8);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    ReverseChannel::new(tx)
}

#[tokio::test]
async fn local_with_display_opens_via_os_opener() {
    let opener = RecordingOpener::new(true);
    open_external("http://x", true, true, &opener, &idle_reverse())
        .await
        .expect("local + display resolves directly");
    assert_eq!(opener.opened.lock().unwrap().as_slice(), ["http://x"]);
}

#[tokio::test]
async fn local_headless_returns_headless_warning() {
    let opener = RecordingOpener::new(true);
    let err = open_external("http://x", true, false, &opener, &idle_reverse())
        .await
        .expect_err("headless host warns");
    assert!(matches!(err, OpenExternalError::Headless(_)));
    assert_eq!(err.code(), -32603);
    assert!(err.to_string().contains("headless"));
    assert!(
        opener.opened.lock().unwrap().is_empty(),
        "the OS opener is never invoked on a headless host"
    );
}

#[tokio::test]
async fn local_opener_failure_surfaces() {
    let opener = RecordingOpener::new(false);
    let err = open_external("http://x", true, true, &opener, &idle_reverse())
        .await
        .expect_err("opener failure propagates");
    assert!(matches!(err, OpenExternalError::Opener(_)));
}

#[tokio::test]
async fn empty_url_is_invalid_params() {
    let opener = RecordingOpener::new(true);
    let err = open_external("", true, true, &opener, &idle_reverse())
        .await
        .expect_err("empty url rejected");
    assert!(matches!(err, OpenExternalError::InvalidUrl(_)));
    assert_eq!(err.code(), -32602);
}

#[tokio::test]
async fn remote_dispatches_to_connected_client() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);
    let opener = RecordingOpener::new(true);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let opener = RecordingOpener::new(true);
        open_external("http://localhost:3000", false, false, &opener, &caller).await
    });

    // The mock FE receives the FE-served request and replies success.
    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["method"], "host.openExternal");
    assert_eq!(req["params"]["url"], "http://localhost:3000");
    let response = json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "ok": true } });
    assert!(reverse.route_response(&response));

    handle.await.unwrap().expect("client opened the url");
    // The daemon host opener is never used on the remote path.
    assert!(opener.opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn remote_client_failure_is_a_proxy_error() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let opener = RecordingOpener::new(true);
        open_external("http://x", false, false, &opener, &caller).await
    });

    let frame = out_rx.recv().await.unwrap();
    let id = serde_json::from_str::<Value>(&frame).unwrap()["id"].clone();
    let response = json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": "no display on client" } });
    assert!(reverse.route_response(&response));

    let err = handle.await.unwrap().expect_err("client failure surfaces");
    assert!(matches!(err, OpenExternalError::Proxy(_)));
}

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
