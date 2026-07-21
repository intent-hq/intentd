//! Unit tests for the `system.*` control fast-path (§5.7).

use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Value};

use super::*;

struct FakeControl {
    status: SystemStatus,
    shutdown_called: AtomicBool,
}

impl FakeControl {
    fn new() -> Self {
        Self {
            status: SystemStatus {
                listen_mode: "both".to_string(),
                uds: true,
                tcp: true,
                port: Some(5180),
                clients: 2,
                agents: 1,
                fingerprint: Some("AB:CD".to_string()),
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                has_display: true,
                max_agents: 20,
                version: "0.1.0".to_string(),
                uptime_seconds: 123,
                cpu_percent: 12.5,
                memory_bytes: 104_857_600,
            },
            shutdown_called: AtomicBool::new(false),
        }
    }
}

impl SystemControl for FakeControl {
    fn status(&self) -> SystemStatus {
        self.status.clone()
    }
    fn request_shutdown(&self) {
        self.shutdown_called.store(true, Ordering::SeqCst);
    }
}

#[test]
fn classify_only_matches_system_methods() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.status" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.shutdown" })).is_some());
    // Non-system methods fall through.
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "workspace.list" })).is_none());
    // Wrong jsonrpc version falls through.
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "system.status" })).is_none());
    // A bad id type falls through (the dispatcher returns -32600).
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": [1], "method": "system.status" })).is_none());
}

#[test]
fn status_json_local_vs_remote_locality() {
    let status = FakeControl::new().status;
    let local = status_json(&status, true);
    assert_eq!(local["host"]["locality"], "local");
    assert_eq!(local["running"], true);
    assert_eq!(local["listenMode"], "both");
    assert_eq!(local["transports"], json!(["uds", "tcp"]));
    assert_eq!(local["port"], 5180);
    assert_eq!(local["clients"], 2);
    assert_eq!(local["agents"], 1);
    assert_eq!(local["maxAgents"], 20);
    assert_eq!(local["version"], "0.1.0");
    assert_eq!(local["uptimeSeconds"], 123);
    assert_eq!(local["cpuPercent"], 12.5);
    assert_eq!(local["memoryBytes"], 104_857_600u64);
    assert_eq!(local["fingerprint"], "AB:CD");
    assert_eq!(local["protocolVersion"], "2.0");
    assert_eq!(local["host"]["os"], "macos");
    assert_eq!(local["host"]["arch"], "aarch64");
    assert_eq!(local["host"]["hasDisplay"], true);

    let remote = status_json(&status, false);
    assert_eq!(remote["host"]["locality"], "remote");
    assert_eq!(remote["protocolVersion"], "2.0");
}

#[test]
fn status_json_uds_only_has_no_port_or_fingerprint() {
    let status = SystemStatus {
        listen_mode: "uds".to_string(),
        uds: true,
        tcp: false,
        port: None,
        clients: 0,
        agents: 0,
        fingerprint: None,
        os: "linux".to_string(),
        arch: "x86_64".to_string(),
        has_display: false,
        max_agents: 8,
        version: "0.1.0".to_string(),
        uptime_seconds: 456,
        cpu_percent: 0.0,
        memory_bytes: 0,
    };
    let v = status_json(&status, true);
    assert_eq!(v["transports"], json!(["uds"]));
    assert_eq!(v["port"], Value::Null);
    assert_eq!(v["fingerprint"], Value::Null);
}

#[test]
fn handle_status_returns_a_response_frame() {
    let control = FakeControl::new();
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 7, "method": "system.status" })).unwrap();
    let frame = handle(req, &control, true).expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 7);
    assert_eq!(parsed["result"]["running"], true);
    assert!(!control.shutdown_called.load(Ordering::SeqCst));
}

#[test]
fn handle_shutdown_triggers_request_and_acks() {
    let control = FakeControl::new();
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "system.shutdown" })).unwrap();
    let frame = handle(req, &control, true).expect("shutdown acks the request");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 9);
    assert_eq!(parsed["result"]["stopping"], true);
    assert!(control.shutdown_called.load(Ordering::SeqCst));
}

#[test]
fn handle_notification_gets_no_response() {
    let control = FakeControl::new();
    // No `id` member ⇒ a notification; shutdown still fires but no frame returns.
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "system.shutdown" })).unwrap();
    assert!(handle(req, &control, true).is_none());
    assert!(control.shutdown_called.load(Ordering::SeqCst));
}
