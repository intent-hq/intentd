//! Unit tests for the `system.*` control fast-path (§5.7).

use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Value};

use super::*;

// `credential_pid` recorder is `Option<Option<_>>`: never-called vs called-with-None.
#[allow(clippy::option_option)]
struct FakeControl {
    status: SystemStatus,
    shutdown_called: AtomicBool,
    import_force: std::sync::Mutex<Option<bool>>,
    credential: Option<(String, String)>,
    credential_pid: std::sync::Mutex<Option<Option<u64>>>,
    update_error: Option<String>,
    update_called: AtomicBool,
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
                build_commit: Some("0123456789abcdef".to_string()),
                uptime_seconds: 123,
                local_ips: vec!["192.168.1.10".to_string(), "10.0.0.5".to_string()],
                hostname: "studio.local".to_string(),
                pretty_hostname: "Clement's Mac Studio".to_string(),
                cpu_percent: 12.5,
                memory_bytes: 104_857_600,
                child_processes: Some(4),
                child_memory_bytes: Some(2_684_354_560),
                child_memory_peak_bytes: Some(5_368_709_120),
                agent_memory_budget_bytes: Some(21_474_836_480),
                agent_memory_charged_bytes: Some(3_221_225_472),
                queued_spawns: Some(1),
                workspaces_disk_available_bytes: Some(250_000_000_000),
                workspaces_disk_total_bytes: Some(1_000_000_000_000),
                file_watch: Some(FileWatchStatus {
                    active_streams: 2,
                    total_roots: 5,
                    failed_roots: 0,
                }),
            },
            shutdown_called: AtomicBool::new(false),
            import_force: std::sync::Mutex::new(None),
            credential: None,
            credential_pid: std::sync::Mutex::new(None),
            update_error: None,
            update_called: AtomicBool::new(false),
        }
    }

    fn with_credential(username: &str, password: &str) -> Self {
        Self {
            credential: Some((username.to_string(), password.to_string())),
            ..Self::new()
        }
    }

    fn with_update_error(message: &str) -> Self {
        Self {
            update_error: Some(message.to_string()),
            ..Self::new()
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
    fn request_update(&self) -> Result<(), String> {
        self.update_called.store(true, Ordering::SeqCst);
        match &self.update_error {
            Some(message) => Err(message.clone()),
            None => Ok(()),
        }
    }
    fn import_legacy(
        &self,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            *self.import_force.lock().unwrap() = Some(force);
            Ok(json!({
                "imported": 1, "updated": 0, "skipped": 0,
                "notes": 2, "comments": 3, "agents": 4, "assets": 5,
                "skipSummary": [], "compatibilityFailures": false,
                "markerWritten": true
            }))
        })
    }
    fn git_credential(
        &self,
        client_pid: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Option<(String, String)>> + Send + '_>> {
        Box::pin(async move {
            *self.credential_pid.lock().unwrap() = Some(client_pid);
            self.credential.clone()
        })
    }
}

#[test]
fn classify_only_matches_system_methods() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.status" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.shutdown" })).is_some());
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.importLegacy" })).is_some()
    );
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.gitCredential" })).is_some()
    );
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "system.requestUpdate" })).is_some()
    );
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
    assert_eq!(local["buildCommit"], "0123456789abcdef");
    assert_eq!(local["uptimeSeconds"], 123);
    assert_eq!(local["cpuPercent"], 12.5);
    assert_eq!(local["memoryBytes"], 104_857_600u64);
    assert_eq!(local["fingerprint"], "AB:CD");
    assert_eq!(local["localIps"], json!(["192.168.1.10", "10.0.0.5"]));
    assert_eq!(local["hostname"], "studio.local");
    assert_eq!(local["prettyHostname"], "Clement's Mac Studio");
    assert_eq!(local["protocolVersion"], crate::protocol::PROTOCOL_VERSION);
    assert_eq!(local["host"]["os"], "macos");
    assert_eq!(local["host"]["arch"], "aarch64");
    assert_eq!(local["host"]["hasDisplay"], true);

    let remote = status_json(&status, false);
    assert_eq!(remote["host"]["locality"], "remote");
    assert_eq!(remote["protocolVersion"], crate::protocol::PROTOCOL_VERSION);
    // The routing fields are served to remote callers too — that is the point:
    // an authenticated WSS client refreshes its host list from system.status.
    assert_eq!(remote["localIps"], json!(["192.168.1.10", "10.0.0.5"]));
    assert_eq!(remote["hostname"], "studio.local");
    assert_eq!(remote["prettyHostname"], "Clement's Mac Studio");
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
        build_commit: None,
        uptime_seconds: 456,
        local_ips: Vec::new(),
        hostname: "intent".to_string(),
        pretty_hostname: "intent".to_string(),
        cpu_percent: 0.0,
        memory_bytes: 0,
        child_processes: None,
        child_memory_bytes: None,
        child_memory_peak_bytes: None,
        agent_memory_budget_bytes: None,
        agent_memory_charged_bytes: None,
        queued_spawns: None,
        workspaces_disk_available_bytes: None,
        workspaces_disk_total_bytes: None,
        file_watch: None,
    };
    let v = status_json(&status, true);
    assert_eq!(v["transports"], json!(["uds"]));
    assert_eq!(v["port"], Value::Null);
    assert_eq!(v["fingerprint"], Value::Null);
    // No routable interfaces still yields an (empty) array, never null.
    assert_eq!(v["localIps"], json!([]));
    assert_eq!(v["hostname"], "intent");
    assert_eq!(
        v["prettyHostname"], "intent",
        "falls back to hostname when no pretty name exists"
    );
    // An unsampled child tree is explicitly null — never a misleading 0, which
    // a bundle would read as "the daemon has no child processes".
    assert_eq!(v["childProcesses"], Value::Null);
    assert_eq!(v["childMemoryBytes"], Value::Null);
    assert_eq!(v["childMemoryPeakBytes"], Value::Null);
    // Budget off ⇒ the budget fields are ABSENT (presence-detected), not null.
    let obj = v.as_object().unwrap();
    // Builds without source metadata omit the additive identity field.
    assert!(!obj.contains_key("buildCommit"));
    assert!(!obj.contains_key("agentMemoryBudgetBytes"));
    assert!(!obj.contains_key("agentMemoryChargedBytes"));
    assert!(!obj.contains_key("queuedSpawns"));
    // No disk sample ⇒ the disk fields are ABSENT (presence-detected), not null.
    assert!(!obj.contains_key("workspacesDiskAvailableBytes"));
    assert!(!obj.contains_key("workspacesDiskTotalBytes"));
    // Watcher registry not started yet ⇒ fileWatch is ABSENT, not null.
    assert!(!obj.contains_key("fileWatch"));
}

/// The descendant-tree fields ride `system.status` so a debug
/// bundle can attribute system memory pressure to agent child processes: the
/// daemon's own `memoryBytes` is a small fraction of what its tree costs.
#[test]
fn status_json_carries_the_child_process_tree_sample() {
    let control = FakeControl::new();
    let v = status_json(&control.status, true);
    assert_eq!(v["childProcesses"], 4);
    assert_eq!(v["childMemoryBytes"], 2_684_354_560u64);
    // The peak is a separate field, not a copy of the instantaneous value: a
    // bundle captured after a burst drains sees baseline in `childMemoryBytes`
    // and the overshoot only in `childMemoryPeakBytes`.
    assert_eq!(v["childMemoryPeakBytes"], 5_368_709_120u64);
    // Own-RSS and tree-RSS are distinct fields; the tree dwarfs the daemon.
    assert_eq!(v["memoryBytes"], 104_857_600u64);
}

/// With the aggregate budget installed (monorepo#2063) the three budget
/// fields ride `system.status`, so a client can render "why is my agent
/// queued" truthfully: the configured ceiling, the bytes admission actually
/// compares, and the spawns currently waiting.
#[test]
fn status_json_carries_the_budget_fields_when_installed() {
    let v = status_json(&FakeControl::new().status, true);
    assert_eq!(v["agentMemoryBudgetBytes"], 21_474_836_480u64);
    assert_eq!(v["agentMemoryChargedBytes"], 3_221_225_472u64);
    assert_eq!(v["queuedSpawns"], 1);

    // Budget installed but no tree sample yet: the budget is inert, so the
    // charged bytes are absent while the ceiling and queue depth still serve.
    let mut status = FakeControl::new().status;
    status.agent_memory_charged_bytes = None;
    status.queued_spawns = Some(0);
    let v = status_json(&status, true);
    assert_eq!(v["agentMemoryBudgetBytes"], 21_474_836_480u64);
    assert!(!v
        .as_object()
        .unwrap()
        .contains_key("agentMemoryChargedBytes"));
    assert_eq!(v["queuedSpawns"], 0);
}

/// The workspaces-root disk fields ride `system.status` so a client can warn
/// when the volume hosting workspace checkouts is running out of space.
#[test]
fn status_json_carries_the_workspaces_disk_fields_when_sampled() {
    let v = status_json(&FakeControl::new().status, true);
    assert_eq!(v["workspacesDiskAvailableBytes"], 250_000_000_000u64);
    assert_eq!(v["workspacesDiskTotalBytes"], 1_000_000_000_000u64);
}

/// The fileWatch object rides `system.status` once the watcher registry is
/// live (intent-hq/intent#3708), so a client — and a debug bundle — can see
/// whether the daemon's watch coverage is degraded (`failedRoots > 0` means
/// file events under those roots are silently missed) rather than digging
/// WARN lines out of the daemon log.
#[test]
fn status_json_carries_the_file_watch_coverage_when_available() {
    let v = status_json(&FakeControl::new().status, true);
    assert_eq!(v["fileWatch"]["activeStreams"], 2);
    assert_eq!(v["fileWatch"]["totalRoots"], 5);
    assert_eq!(v["fileWatch"]["failedRoots"], 0);

    // Degraded coverage renders the failed count verbatim.
    let mut status = FakeControl::new().status;
    status.file_watch = Some(FileWatchStatus {
        active_streams: 1,
        total_roots: 5,
        failed_roots: 3,
    });
    let v = status_json(&status, true);
    assert_eq!(v["fileWatch"]["failedRoots"], 3);
}

#[tokio::test]
async fn handle_status_returns_a_response_frame() {
    let control = FakeControl::new();
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 7, "method": "system.status" })).unwrap();
    let frame = handle(req, &control, true, true)
        .await
        .expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 7);
    assert_eq!(parsed["result"]["running"], true);
    assert!(!control.shutdown_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn handle_shutdown_triggers_request_and_acks() {
    let control = FakeControl::new();
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "system.shutdown" })).unwrap();
    let frame = handle(req, &control, true, true)
        .await
        .expect("shutdown acks the request");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 9);
    assert_eq!(parsed["result"]["stopping"], true);
    assert!(control.shutdown_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn handle_notification_gets_no_response() {
    let control = FakeControl::new();
    // No `id` member ⇒ a notification; shutdown still fires but no frame returns.
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "system.shutdown" })).unwrap();
    assert!(handle(req, &control, true, true).await.is_none());
    assert!(control.shutdown_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn handle_shutdown_remote_rejects_with_uds_only_error() {
    let control = FakeControl::new();
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 11, "method": "system.shutdown" })).unwrap();
    let frame = handle(req, &control, false, false)
        .await
        .expect("remote shutdown gets an error response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 11);
    assert_eq!(parsed["error"]["code"], -32001);
    assert_eq!(
        parsed["error"]["message"],
        "system.shutdown is available over UDS only"
    );
    assert!(!control.shutdown_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn handle_shutdown_remote_notification_is_ignored() {
    let control = FakeControl::new();
    // A remote notification gets no frame back AND must not trigger shutdown.
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "system.shutdown" })).unwrap();
    assert!(handle(req, &control, false, false).await.is_none());
    assert!(!control.shutdown_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn request_update_supervised_returns_ok() {
    let control = FakeControl::new();
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 31, "method": "system.requestUpdate" })).unwrap();
    let frame = handle(req, &control, true, true)
        .await
        .expect("requestUpdate has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 31);
    assert_eq!(parsed["result"], json!({ "ok": true }));
    assert!(control.update_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn request_update_unsupervised_maps_to_internal_error() {
    let control = FakeControl::with_update_error("daemon is not supervised by intentd-sitter");
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 32, "method": "system.requestUpdate" })).unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["id"], 32);
    assert_eq!(parsed["error"]["code"], -32603);
    assert_eq!(
        parsed["error"]["message"],
        "daemon is not supervised by intentd-sitter"
    );
}

#[tokio::test]
async fn request_update_is_served_to_remote_callers() {
    // Unlike system.shutdown, remote (TCP/WSS) callers may trigger an update
    // check — that is the point of the method (a remote FE's update button).
    let control = FakeControl::new();
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 33, "method": "system.requestUpdate" })).unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, false, false).await.unwrap()).unwrap();
    assert_eq!(parsed["result"], json!({ "ok": true }));
    assert!(control.update_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn request_update_notification_fires_without_response() {
    let control = FakeControl::new();
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "system.requestUpdate" })).unwrap();
    assert!(handle(req, &control, true, true).await.is_none());
    assert!(control.update_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn import_legacy_defaults_force_and_returns_counts() {
    let control = FakeControl::new();
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 11, "method": "system.importLegacy", "params": {}
    }))
    .unwrap();
    let frame = handle(req, &control, true, true).await.unwrap();
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["result"]["imported"], 1);
    assert_eq!(parsed["result"]["notes"], 2);
    assert_eq!(parsed["result"]["compatibilityFailures"], false);
    assert_eq!(parsed["result"]["markerWritten"], true);
    assert_eq!(*control.import_force.lock().unwrap(), Some(false));
}

#[tokio::test]
async fn import_legacy_rejects_invalid_force_and_remote_transport() {
    let control = FakeControl::new();
    let invalid = classify(&json!({
        "jsonrpc": "2.0", "id": 12, "method": "system.importLegacy",
        "params": { "force": "yes" }
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(invalid, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");

    let positional = classify(&json!({
        "jsonrpc": "2.0", "id": 13, "method": "system.importLegacy", "params": []
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(positional, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");

    let remote = classify(&json!({
        "jsonrpc": "2.0", "id": 14, "method": "system.importLegacy",
        "params": { "force": true }
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(remote, &control, false, false).await.unwrap()).unwrap();
    assert_eq!(parsed["error"]["code"], -32001);
    assert_eq!(*control.import_force.lock().unwrap(), None);
}

#[tokio::test]
async fn git_credential_returns_credential_and_forwards_pid() {
    let control = FakeControl::with_credential("x-access-token", "gho_secret");
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 21, "method": "system.gitCredential",
        "params": { "pid": 4242, "protocol": "https", "host": "github.com" }
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["id"], 21);
    assert_eq!(parsed["result"]["credential"]["username"], "x-access-token");
    assert_eq!(parsed["result"]["credential"]["password"], "gho_secret");
    assert_eq!(*control.credential_pid.lock().unwrap(), Some(Some(4242)));
}

#[tokio::test]
async fn git_credential_none_yields_null_and_pid_is_lenient() {
    let control = FakeControl::new();
    // In-scope request but no credential available → `credential: null`.
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 22, "method": "system.gitCredential",
        "params": { "protocol": "https", "host": "GitHub.COM" }
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["result"]["credential"], Value::Null);
    assert_eq!(*control.credential_pid.lock().unwrap(), Some(None));

    // A non-numeric pid degrades to None instead of erroring (audit-only).
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 23, "method": "system.gitCredential",
        "params": { "pid": "nope", "protocol": "https", "host": "github.com" }
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, true, true).await.unwrap()).unwrap();
    assert_eq!(parsed["result"]["credential"], Value::Null);
}

#[tokio::test]
async fn git_credential_out_of_scope_yields_null_without_resolver() {
    // The daemon-side gate: anything but https://github.com gets
    // `credential: null` and the resolver never runs — even when a
    // credential would have been available.
    let control = FakeControl::with_credential("x-access-token", "gho_secret");
    for params in [
        json!(null),
        json!({}),
        json!({ "pid": 1 }),
        json!({ "protocol": "https", "host": "gitlab.com" }),
        json!({ "protocol": "https", "host": "api.github.com" }),
        json!({ "protocol": "http", "host": "github.com" }),
        json!({ "protocol": "https" }),
        json!({ "host": "github.com" }),
        json!({ "protocol": 1, "host": "github.com" }),
    ] {
        let req = classify(&json!({
            "jsonrpc": "2.0", "id": 25, "method": "system.gitCredential", "params": params
        }))
        .unwrap();
        let parsed: Value =
            serde_json::from_str(&handle(req, &control, true, true).await.unwrap()).unwrap();
        assert_eq!(parsed["result"]["credential"], Value::Null);
        assert_eq!(*control.credential_pid.lock().unwrap(), None);
    }
}

#[tokio::test]
async fn git_credential_remote_rejects_with_uds_only_error() {
    let control = FakeControl::with_credential("x-access-token", "gho_secret");
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 24, "method": "system.gitCredential", "params": {}
    }))
    .unwrap();
    let parsed: Value =
        serde_json::from_str(&handle(req, &control, false, false).await.unwrap()).unwrap();
    assert_eq!(parsed["error"]["code"], -32001);
    assert_eq!(
        parsed["error"]["message"],
        "system.gitCredential is available over UDS only"
    );
    // The resolver must never run for a remote caller.
    assert_eq!(*control.credential_pid.lock().unwrap(), None);
}
