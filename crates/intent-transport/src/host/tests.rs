//! Unit tests for the `host.*` capability probe fast-path (§5.14) and the
//! additive `host.*` host-services (`checkGit` / `listDirectory` /
//! `directoryStatus` / `checkAuggie` / `findBinary` / `toolAvailability` /
//! `env`).

use std::sync::Mutex;

use intent_core::BoxFuture;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::*;
use crate::reverse::ReverseChannel;

/// Minimal `WorkspaceApi` for the host fast-path tests. `settings_get` returns
/// the trait default (`Err(Internal)`), so `configured_auggie_path` always
/// resolves to `None` and `host.checkAuggie` falls through to discovery.
struct NoopApi;

impl intent_core::WorkspaceApi for NoopApi {}

/// `WorkspaceApi` that returns a configured value for `context.auggiePath` so
/// `host.checkAuggie` exercises the settings-precedence branch in
/// `configured_auggie_path` without hitting real settings storage.
struct AuggiePathApi(String);

impl intent_core::WorkspaceApi for AuggiePathApi {
    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        let v = if path == "context.auggiePath" {
            json!({ "path": path, "value": self.0 })
        } else {
            json!({ "path": path, "value": Value::Null })
        };
        Box::pin(async move { Ok(v) })
    }
}

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
fn classify_matches_host_status_and_host_services() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.status" })).is_some());
    // Notification shape (no id) still classifies; handling returns no frame.
    assert!(classify(&json!({ "jsonrpc": "2.0", "method": "host.status" })).is_some());
    // The additive host-services classify too.
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 2, "method": "host.checkGit" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 2, "method": "host.checkNode" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 2, "method": "host.checkGh" })).is_some());
    assert!(classify(
        &json!({ "jsonrpc": "2.0", "id": 3, "method": "host.listDirectory", "params": { "path": "/tmp" } })
    )
    .is_some());
    assert!(classify(
        &json!({ "jsonrpc": "2.0", "id": 4, "method": "host.directoryStatus", "params": { "path": "/tmp" } })
    )
    .is_some());
    assert!(classify(
        &json!({ "jsonrpc": "2.0", "id": 4, "method": "host.createDirectory", "params": { "path": "/tmp/new" } })
    )
    .is_some());
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 5, "method": "host.checkAuggie" })).is_some()
    );
    assert!(classify(
        &json!({ "jsonrpc": "2.0", "id": 6, "method": "host.findBinary", "params": { "name": "git" } })
    )
    .is_some());
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 7, "method": "host.toolAvailability" }))
            .is_some()
    );
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 8, "method": "host.env" })).is_some());
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "host.providerAuthStatus" }))
            .is_some()
    );
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 10, "method": "host.providerTestPrompt" }))
            .is_some()
    );
    // `host.openExternal` (FE-served reverse RPC) / wrong version / bad id fall through.
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.openExternal" })).is_none()
    );
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "host.status" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": [1], "method": "host.status" })).is_none());
}

#[test]
fn status_json_local_includes_all_fields() {
    let v = host_status_json(
        "linux",
        "x86_64",
        "build-01",
        "Build Server 01",
        true,
        Some("wayland"),
        true,
    );
    assert_eq!(v["os"], "linux");
    assert_eq!(v["arch"], "x86_64");
    assert_eq!(v["hostname"], "build-01");
    assert_eq!(v["prettyHostname"], "Build Server 01");
    assert_eq!(v["hasDisplay"], true);
    assert_eq!(v["locality"], "local");
    assert_eq!(v["displayServer"], "wayland");
}

#[test]
fn status_json_remote_omits_absent_display_server() {
    let v = host_status_json(
        "linux", "x86_64", "build-01", "build-01", false, None, false,
    );
    assert_eq!(v["locality"], "remote");
    assert_eq!(v["hasDisplay"], false);
    assert_eq!(
        v["prettyHostname"], "build-01",
        "falls back to hostname when no pretty name exists"
    );
    assert_eq!(v.get("displayServer"), None, "omitted when not detected");
}

#[tokio::test]
async fn handle_status_returns_a_response_frame() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 7, "method": "host.status" })).unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 7);
    assert_eq!(parsed["result"]["locality"], "local");
    assert!(parsed["result"]["os"].is_string());
    assert!(parsed["result"]["arch"].is_string());
    assert!(parsed["result"]["hostname"].is_string());
    assert!(
        !parsed["result"]["prettyHostname"]
            .as_str()
            .expect("prettyHostname is string")
            .is_empty(),
        "prettyHostname non-empty"
    );
    assert!(parsed["result"]["hasDisplay"].is_boolean());
}

#[tokio::test]
async fn handle_remote_reports_remote_locality() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 8, "method": "host.status" })).unwrap();
    let frame = handle(req, &NoopApi, None, false, &idle_reverse())
        .await
        .expect("status has a response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["result"]["locality"], "remote");
}

#[tokio::test]
async fn handle_notification_gets_no_response() {
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "host.status" })).unwrap();
    assert!(
        handle(req, &NoopApi, None, true, &idle_reverse())
            .await
            .is_none(),
        "a notification gets no reply"
    );
}

#[tokio::test]
async fn handle_check_git_returns_available_boolean() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 10, "method": "host.checkGit" })).unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("checkGit always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 10);
    assert!(
        parsed["result"]["available"].is_boolean(),
        "available is always present"
    );
}

#[tokio::test]
async fn handle_check_node_returns_available_boolean() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 10, "method": "host.checkNode" })).unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("checkNode always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 10);
    assert!(
        parsed["result"]["available"].is_boolean(),
        "available is always present"
    );
}

#[tokio::test]
async fn handle_check_gh_returns_available_boolean() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 10, "method": "host.checkGh" })).unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("checkGh always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 10);
    assert!(
        parsed["result"]["available"].is_boolean(),
        "available is always present"
    );
}

#[tokio::test]
async fn handle_directory_status_requires_path() {
    let req = classify(
        &json!({ "jsonrpc": "2.0", "id": 11, "method": "host.directoryStatus", "params": {} }),
    )
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("missing path produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 11);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_directory_status_reports_existing_directory() {
    // The CWD is guaranteed to exist on every host the test runs on.
    let cwd = std::env::current_dir().unwrap();
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "host.directoryStatus",
        "params": { "path": cwd.to_string_lossy() }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("directoryStatus replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 12);
    assert_eq!(parsed["result"]["exists"], true);
    assert_eq!(parsed["result"]["isDirectory"], true);
}

#[tokio::test]
async fn handle_list_directory_returns_entries_for_cwd() {
    let cwd = std::env::current_dir().unwrap();
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 13,
        "method": "host.listDirectory",
        "params": { "path": cwd.to_string_lossy() }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("listDirectory replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 13);
    assert!(parsed["result"]["entries"].is_array());
    assert!(parsed["result"]["path"].is_string());
    // The additive `favorites` field is always present; `home` always leads.
    let favorites = parsed["result"]["favorites"].as_array().unwrap();
    assert_eq!(favorites[0]["id"], "home");
    assert!(favorites[0]["path"].is_string());
}

#[tokio::test]
async fn handle_create_directory_requires_path() {
    let req = classify(
        &json!({ "jsonrpc": "2.0", "id": 15, "method": "host.createDirectory", "params": {} }),
    )
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("missing path produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 15);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_check_auggie_uses_configured_path() {
    // Even when the configured path doesn't exist on the host, `available:false`
    // is the expected shape. We only assert that the response is well-formed.
    let api = AuggiePathApi("/definitely/does/not/exist/auggie-xyzzy".to_string());
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 14, "method": "host.checkAuggie" })).unwrap();
    let frame = handle(req, &api, None, true, &idle_reverse())
        .await
        .expect("checkAuggie always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 14);
    assert!(parsed["result"]["available"].is_boolean());
    // Resolution-only payload: `version` is retired from `host.checkAuggie`.
    assert!(parsed["result"].get("version").is_none());
}

#[tokio::test]
async fn handle_check_auggie_notification_gets_no_response() {
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "host.checkAuggie" })).unwrap();
    assert!(
        handle(req, &NoopApi, None, true, &idle_reverse())
            .await
            .is_none(),
        "a notification gets no reply"
    );
}

#[tokio::test]
async fn handle_find_binary_requires_name() {
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 20, "method": "host.findBinary", "params": {} }))
            .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("missing name produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 20);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_find_binary_returns_available_boolean() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 21,
        "method": "host.findBinary",
        "params": { "name": "git" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("findBinary always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 21);
    assert!(
        parsed["result"]["available"].is_boolean(),
        "available is always present"
    );
}

#[tokio::test]
async fn handle_find_binary_notification_gets_no_response() {
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "host.findBinary" })).unwrap();
    assert!(
        handle(req, &NoopApi, None, true, &idle_reverse())
            .await
            .is_none(),
        "a missing-name notification gets no reply"
    );
}

#[tokio::test]
async fn handle_tool_availability_returns_default_tool_map() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 22, "method": "host.toolAvailability" }))
        .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("toolAvailability always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 22);
    let tools = parsed["result"]["tools"].as_object().unwrap();
    assert!(tools.contains_key("git"), "git is in the default tool set");
    assert!(tools["git"]["available"].is_boolean());
}

#[tokio::test]
async fn handle_env_returns_path_and_var_names() {
    let req = classify(&json!({ "jsonrpc": "2.0", "id": 23, "method": "host.env" })).unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("env always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 23);
    assert!(parsed["result"]["path"].is_string());
    assert!(parsed["result"]["pathEntries"].is_array());
    assert!(parsed["result"]["enhancedPath"].is_string());
    assert!(parsed["result"]["varNames"].is_array());
}

#[tokio::test]
async fn handle_provider_auth_status_unknown_provider_is_invalid_params() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 24,
        "method": "host.providerAuthStatus",
        "params": { "providerId": "not-a-provider" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("unknown provider produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 24);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_provider_auth_status_rejects_non_string_provider_id() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 25,
        "method": "host.providerAuthStatus",
        "params": { "providerId": 42 }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("non-string providerId produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 25);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_provider_auth_status_rejects_non_bool_force() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 27,
        "method": "host.providerAuthStatus",
        "params": { "force": "yes" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("non-boolean force produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 27);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_provider_auth_status_scoped_to_grok_returns_one_entry() {
    // On hosts without grok the probe never spawns (`authenticated: null`);
    // with grok installed, `grok models` actually runs, bounded by the probe
    // timeout. The assertions are shape-only so both environments pass.
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 26,
        "method": "host.providerAuthStatus",
        "params": { "providerId": "grok" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("providerAuthStatus always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 26);
    let providers = parsed["result"]["providers"].as_array().unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["id"], "grok");
    assert!(providers[0]["authenticated"].is_boolean() || providers[0]["authenticated"].is_null());
}

#[tokio::test]
async fn handle_provider_test_prompt_requires_provider_id() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 28,
        "method": "host.providerTestPrompt",
        "params": {}
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("missing providerId produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 28);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(
        parsed["error"]["message"],
        "Missing required parameter: providerId"
    );
}

#[tokio::test]
async fn handle_provider_test_prompt_unknown_provider_is_invalid_params() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 29,
        "method": "host.providerTestPrompt",
        "params": { "providerId": "not-a-provider" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("unknown provider produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 29);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_provider_test_prompt_rejects_non_string_model() {
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 31,
        "method": "host.providerTestPrompt",
        "params": { "providerId": "codex", "model": 42 }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("non-string model produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 31);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_provider_test_prompt_unsupported_provider_is_structured_result() {
    // unsloth opts out (`supports_test_prompt: false`): the reply is a
    // success frame carrying `{ ok: false, reason: "unsupported" }` — never a
    // wire error, and nothing is resolved or spawned.
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 32,
        "method": "host.providerTestPrompt",
        "params": { "providerId": "unsloth" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("unsupported provider still replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 32);
    assert_eq!(parsed["result"]["ok"], false);
    assert_eq!(parsed["result"]["reason"], "unsupported");
    assert!(parsed["result"]["message"].is_string());
}

#[tokio::test]
async fn handle_find_app_requires_name() {
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 30, "method": "host.findApp", "params": {} }))
            .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("missing name produces an error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 30);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn handle_find_app_notification_gets_no_response() {
    let req = classify(&json!({ "jsonrpc": "2.0", "method": "host.findApp" })).unwrap();
    assert!(
        handle(req, &NoopApi, None, true, &idle_reverse())
            .await
            .is_none(),
        "a missing-name notification gets no reply"
    );
}

#[tokio::test]
async fn handle_find_app_returns_installed_boolean() {
    // The bogus name is safe (no traversal) but will not match a real `.app`
    // bundle on any test host; the shape is what matters here.
    let req = classify(&json!({
        "jsonrpc": "2.0",
        "id": 31,
        "method": "host.findApp",
        "params": { "name": "DefinitelyNotInstalledXyzzy" }
    }))
    .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("findApp always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 31);
    assert!(
        parsed["result"]["installed"].is_boolean(),
        "installed always present"
    );
}

#[tokio::test]
async fn handle_list_installed_editors_returns_editor_array() {
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 32, "method": "host.listInstalledEditors" }))
            .unwrap();
    let frame = handle(req, &NoopApi, None, true, &idle_reverse())
        .await
        .expect("listInstalledEditors always replies");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 32);
    let editors = parsed["result"]["editors"]
        .as_array()
        .expect("editors array");
    assert!(!editors.is_empty(), "default catalog is non-empty");
    for entry in editors {
        assert!(entry["id"].is_string(), "id always present");
        assert!(entry["installed"].is_boolean(), "installed always present");
    }
}

/// An [`EditorLauncher`] that records every launch and can be told to fail.
struct RecordingLauncher {
    ok: bool,
    launches: Mutex<Vec<(String, EditorTarget)>>,
}

impl RecordingLauncher {
    fn new(ok: bool) -> Self {
        Self {
            ok,
            launches: Mutex::new(Vec::new()),
        }
    }
}

impl EditorLauncher for RecordingLauncher {
    fn launch(&self, editor: &ResolvedEditor, target: &EditorTarget) -> Result<(), String> {
        self.launches
            .lock()
            .unwrap()
            .push((editor.id.clone(), target.clone()));
        if self.ok {
            Ok(())
        } else {
            Err("editor launcher failed".to_string())
        }
    }
}

/// Fixture `host.listInstalledEditors` payload with one installed vscode entry
/// (native binary) and one uninstalled xcode entry.
fn editors_payload() -> Value {
    json!({
        "editors": [
            { "id": "vscode", "installed": true, "path": "/usr/local/bin/code", "source": "binary" },
            { "id": "xcode", "installed": false },
        ]
    })
}

#[allow(clippy::similar_names)] // launcher vs the recorded launches - deliberate
#[tokio::test]
async fn open_in_editor_local_short_circuits_via_launcher() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    open_in_editor(
        "vscode",
        "/repo/src/main.rs",
        Some(12),
        Some(3),
        true,
        true,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect("local + display resolves directly");
    let launches = launcher.launches.lock().unwrap();
    assert_eq!(launches.len(), 1);
    assert_eq!(launches[0].0, "vscode");
    assert_eq!(launches[0].1.path, "/repo/src/main.rs");
    assert_eq!(launches[0].1.line, Some(12));
    assert_eq!(launches[0].1.column, Some(3));
}

#[tokio::test]
async fn open_in_editor_local_headless_returns_headless() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    let err = open_in_editor(
        "vscode",
        "/repo/src/main.rs",
        None,
        None,
        true,
        false,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect_err("headless host warns");
    assert!(matches!(err, OpenInEditorError::Headless(_)));
    assert_eq!(err.code(), -32603);
    assert!(launcher.launches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn open_in_editor_local_unknown_editor_is_invalid_params() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    let err = open_in_editor(
        "vim-fantasy",
        "/repo/src/main.rs",
        None,
        None,
        true,
        true,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect_err("unknown editorId rejected");
    assert!(matches!(err, OpenInEditorError::InvalidParams(_)));
    assert_eq!(err.code(), -32602);
}

#[tokio::test]
async fn open_in_editor_local_not_installed_returns_not_installed() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    let err = open_in_editor(
        "xcode",
        "/repo/src/main.rs",
        None,
        None,
        true,
        true,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect_err("uninstalled editor rejected");
    assert!(matches!(err, OpenInEditorError::NotInstalled(_)));
    assert_eq!(err.code(), -32603);
}

#[tokio::test]
async fn open_in_editor_empty_editor_id_is_invalid_params() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    let err = open_in_editor(
        "",
        "/repo/src/main.rs",
        None,
        None,
        true,
        true,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect_err("empty editorId rejected");
    assert!(matches!(err, OpenInEditorError::InvalidParams(_)));
    assert_eq!(err.code(), -32602);
}

#[tokio::test]
async fn open_in_editor_empty_path_is_invalid_params() {
    let launcher = RecordingLauncher::new(true);
    let editors = editors_payload();
    let err = open_in_editor(
        "vscode",
        "",
        None,
        None,
        true,
        true,
        &editors,
        &launcher,
        &idle_reverse(),
    )
    .await
    .expect_err("empty path rejected");
    assert!(matches!(err, OpenInEditorError::InvalidParams(_)));
    assert_eq!(err.code(), -32602);
}

#[tokio::test]
async fn open_in_editor_remote_dispatches_to_connected_client() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);
    let launcher = RecordingLauncher::new(true);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let launcher = RecordingLauncher::new(true);
        let editors = editors_payload();
        open_in_editor(
            "vscode",
            "/repo/src/main.rs",
            Some(7),
            Some(1),
            false,
            false,
            &editors,
            &launcher,
            &caller,
        )
        .await
    });

    // The mock FE receives the FE-served request and replies success.
    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["method"], "host.openInEditor");
    assert_eq!(req["params"]["editorId"], "vscode");
    assert_eq!(req["params"]["path"], "/repo/src/main.rs");
    assert_eq!(req["params"]["line"], 7);
    assert_eq!(req["params"]["column"], 1);
    let id = req["id"].as_str().unwrap();
    assert!(id.starts_with("rev-"), "reverse ids use the rev- prefix");
    let response = json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": true } });
    assert!(reverse.route_response(&response));

    handle.await.unwrap().expect("client opened the editor");
    // The daemon host launcher is never used on the remote path.
    assert!(launcher.launches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn open_in_editor_remote_omits_absent_line_and_column() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let launcher = RecordingLauncher::new(true);
        let editors = editors_payload();
        open_in_editor(
            "vscode",
            "/repo/src/main.rs",
            None,
            None,
            false,
            false,
            &editors,
            &launcher,
            &caller,
        )
        .await
    });

    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["params"].get("line"), None);
    assert_eq!(req["params"].get("column"), None);
    let response = json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "ok": true } });
    assert!(reverse.route_response(&response));
    handle.await.unwrap().expect("client opened the editor");
}

#[tokio::test]
async fn open_in_editor_remote_client_failure_is_proxy_error() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let launcher = RecordingLauncher::new(true);
        let editors = editors_payload();
        open_in_editor(
            "vscode",
            "/repo/src/main.rs",
            None,
            None,
            false,
            false,
            &editors,
            &launcher,
            &caller,
        )
        .await
    });

    let frame = out_rx.recv().await.unwrap();
    let id = serde_json::from_str::<Value>(&frame).unwrap()["id"].clone();
    let response = json!({
        "jsonrpc": "2.0", "id": id,
        "error": { "code": -32603, "message": "editor missing on client" }
    });
    assert!(reverse.route_response(&response));

    let err = handle.await.unwrap().expect_err("client failure surfaces");
    assert!(matches!(err, OpenInEditorError::Proxy(_)));
    assert_eq!(err.code(), -32603);
}

#[test]
fn resolved_editor_from_payload_matches_id() {
    let payload = editors_payload();
    let vscode = ResolvedEditor::from_editors_payload("vscode", &payload).unwrap();
    assert!(vscode.installed);
    assert_eq!(vscode.path.as_deref(), Some("/usr/local/bin/code"));
    assert_eq!(vscode.source.as_deref(), Some("binary"));
    assert!(ResolvedEditor::from_editors_payload("missing", &payload).is_none());
}

/// An [`AppPicker`] that records paths and returns a canned reply.
struct RecordingPicker {
    reply: Result<Option<String>, String>,
    picked: Mutex<Vec<String>>,
}

impl RecordingPicker {
    fn new(reply: Result<Option<String>, String>) -> Self {
        Self {
            reply,
            picked: Mutex::new(Vec::new()),
        }
    }
}

impl AppPicker for RecordingPicker {
    fn pick(&self, path: &str) -> Result<Option<String>, String> {
        self.picked.lock().unwrap().push(path.to_string());
        self.reply.clone()
    }
}

#[tokio::test]
async fn pick_application_empty_path_is_invalid_params() {
    let picker = NoopAppPicker;
    let err = pick_application("", true, &picker, &idle_reverse())
        .await
        .expect_err("empty path rejected");
    assert!(matches!(err, PickApplicationError::InvalidPath(_)));
    assert_eq!(err.code(), -32602);
}

#[tokio::test]
async fn pick_application_local_default_returns_none() {
    let picker = NoopAppPicker;
    let out = pick_application("/repo/README.md", true, &picker, &idle_reverse())
        .await
        .expect("noop picker never fails");
    assert_eq!(out, None);
}

#[tokio::test]
async fn pick_application_local_delegates_to_picker() {
    let picker = RecordingPicker::new(Ok(Some("com.vscode".to_string())));
    let out = pick_application("/repo/README.md", true, &picker, &idle_reverse())
        .await
        .expect("picker succeeded");
    assert_eq!(out.as_deref(), Some("com.vscode"));
    assert_eq!(
        picker.picked.lock().unwrap().as_slice(),
        ["/repo/README.md"]
    );
}

#[tokio::test]
async fn pick_application_local_picker_failure_surfaces() {
    let picker = RecordingPicker::new(Err("picker crashed".to_string()));
    let err = pick_application("/repo/README.md", true, &picker, &idle_reverse())
        .await
        .expect_err("picker failure propagates");
    assert!(matches!(err, PickApplicationError::Picker(_)));
    assert_eq!(err.code(), -32603);
}

#[tokio::test]
async fn pick_application_remote_dispatches_to_connected_client() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);
    let picker = NoopAppPicker;

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let picker = NoopAppPicker;
        pick_application("/repo/README.md", false, &picker, &caller).await
    });

    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["method"], "host.pickApplication");
    assert_eq!(req["params"]["path"], "/repo/README.md");
    let id = req["id"].as_str().unwrap();
    assert!(id.starts_with("rev-"), "reverse ids use the rev- prefix");
    let response = json!({
        "jsonrpc": "2.0", "id": id,
        "result": { "applicationId": "com.jetbrains.intellij" }
    });
    assert!(reverse.route_response(&response));

    let out = handle.await.unwrap().expect("client picked an application");
    assert_eq!(out.as_deref(), Some("com.jetbrains.intellij"));
    // The daemon host picker is never consulted on the remote path.
    let _ = picker;
}

#[tokio::test]
async fn pick_application_remote_missing_application_id_is_none() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let picker = NoopAppPicker;
        pick_application("/repo/README.md", false, &picker, &caller).await
    });

    let frame = out_rx.recv().await.unwrap();
    let id = serde_json::from_str::<Value>(&frame).unwrap()["id"].clone();
    let response = json!({ "jsonrpc": "2.0", "id": id, "result": {} });
    assert!(reverse.route_response(&response));

    let out = handle
        .await
        .unwrap()
        .expect("client replied without a pick");
    assert_eq!(out, None);
}

#[tokio::test]
async fn pick_application_remote_client_failure_is_proxy_error() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let handle = tokio::spawn(async move {
        let picker = NoopAppPicker;
        pick_application("/repo/README.md", false, &picker, &caller).await
    });

    let frame = out_rx.recv().await.unwrap();
    let id = serde_json::from_str::<Value>(&frame).unwrap()["id"].clone();
    let response = json!({
        "jsonrpc": "2.0", "id": id,
        "error": { "code": -32603, "message": "no chooser on client" }
    });
    assert!(reverse.route_response(&response));

    let err = handle.await.unwrap().expect_err("client failure surfaces");
    assert!(matches!(err, PickApplicationError::Proxy(_)));
    assert_eq!(err.code(), -32603);
}

#[test]
fn classify_matches_client_called_open_in_editor() {
    assert!(classify(
        &json!({ "jsonrpc": "2.0", "id": 1, "method": "host.openInEditor",
                 "params": { "editorId": "vscode", "path": "/repo/a.rs" } })
    )
    .is_some());
}

#[tokio::test]
async fn handle_open_in_editor_missing_params_are_invalid() {
    // Missing editorId → -32602 (validated before any locality/display work).
    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "host.openInEditor" })).unwrap();
    let frame = handle(req, &NoopApi, None, false, &idle_reverse())
        .await
        .expect("error response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 9);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
    assert!(parsed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("editorId"));

    // Missing path → -32602.
    let req = classify(
        &json!({ "jsonrpc": "2.0", "id": 10, "method": "host.openInEditor",
                                 "params": { "editorId": "vscode" } }),
    )
    .unwrap();
    let frame = handle(req, &NoopApi, None, false, &idle_reverse())
        .await
        .expect("error response");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
    assert!(parsed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("path"));
}

#[tokio::test]
async fn handle_open_in_editor_remote_round_trips_reverse_rpc() {
    let (out_tx, mut out_rx) = mpsc::channel::<String>(8);
    let reverse = ReverseChannel::new(out_tx);

    let caller = reverse.clone();
    let task = tokio::spawn(async move {
        let req = classify(&json!({ "jsonrpc": "2.0", "id": 11, "method": "host.openInEditor",
                                     "params": { "editorId": "vscode", "path": "/repo/a.rs", "line": 3 } }))
        .unwrap();
        handle(req, &NoopApi, None, false, &caller).await
    });

    // The mock FE receives the re-dispatched reverse request and replies.
    let frame = out_rx.recv().await.unwrap();
    let req: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(req["method"], "host.openInEditor");
    assert_eq!(req["params"]["editorId"], "vscode");
    assert_eq!(req["params"]["path"], "/repo/a.rs");
    assert_eq!(req["params"]["line"], 3);
    assert!(req["id"].as_str().unwrap().starts_with("rev-"));
    let response = json!({ "jsonrpc": "2.0", "id": req["id"], "result": { "ok": true } });
    assert!(reverse.route_response(&response));

    // The client-called trigger resolves with the documented result.
    let frame = task.await.unwrap().expect("response frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 11);
    assert_eq!(parsed["result"]["ok"], true);
}
