//! Over-the-wire `settings.*` slice (PROTOCOL §5.12, §9.8): drive
//! `settings.list/get/update/reset` against the daemon over a temp UDS,
//! proving camelCase shapes, sensitive-value **redaction** (no plaintext ever
//! crosses the wire), atomic-batch validation (`-32602`), and the
//! `settings:changed` event. Uses an in-memory secret store so the test never
//! touches the real OS keychain.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, InMemorySecretStore, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-set-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Issue one JSON-RPC request and return the FULL response (incl. any `error`).
async fn call(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    resp
}

/// Issue one request expecting success; returns `result` and asserts no error.
async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let resp = call(write_half, reader, id, method, params).await;
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    for _ in 0..100 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("subscriber_count never reached {target}");
}

/// Pull the `settings.list` entry for `path`.
fn entry<'a>(list: &'a Value, path: &str) -> &'a Value {
    list["settings"]
        .as_array()
        .expect("settings array")
        .iter()
        .find(|e| e["path"] == path)
        .unwrap_or_else(|| panic!("missing setting {path}"))
}

const SECRET: &str = "ghp_super_secret_token_value_0123456789";

#[tokio::test]
async fn settings_round_trip_redaction_validation_and_event() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    // Publish onto the SAME bus the transport reads; inject an in-memory secret
    // store so sensitive settings never touch the real keychain.
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone())
            .with_secret_store(Arc::new(InMemorySecretStore::default())),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-set-");
    let socket = sock_dir.path().join("uds.sock");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (rpc_read, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rpc_read);

    // settings.list — defaults + a redacted sensitive value (unset → null).
    let list = rpc(&mut w, &mut r, 1, "settings.list", json!({})).await;
    assert_eq!(entry(&list, "git.autoCommit")["value"], json!(true));
    assert_eq!(entry(&list, "git.autoCommit")["type"], "boolean");
    let token = entry(&list, "sourceControl.github.token");
    assert_eq!(token["value"], Value::Null, "unset secret reads as null");
    assert_eq!(token["sensitive"], json!(true));
    assert_eq!(entry(&list, "server.auth.token")["sensitive"], json!(true));
    let linear = entry(&list, "linear.token");
    assert_eq!(linear["value"], Value::Null, "unset secret reads as null");
    assert_eq!(linear["sensitive"], json!(true));

    // settings.get — one definition with its (default) value.
    let got = rpc(
        &mut w,
        &mut r,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(got["value"], json!(true));
    assert_eq!(got["definition"]["type"], "boolean");

    // Validation → -32602, nothing applied (atomic batch).
    for bad in [
        json!([{ "path": "does.not.exist", "value": 1 }]),
        json!([{ "path": "server.port", "value": 99 }]),
        json!([{ "path": "logging.level", "value": "bogus" }]),
        json!([{ "path": "server.auth.token", "value": "x" }]),
    ] {
        let resp = call(
            &mut w,
            &mut r,
            3,
            "settings.update",
            json!({ "changes": bad }),
        )
        .await;
        assert_eq!(resp["error"]["code"], -32602, "expected -32602 for {resp}");
    }
    // Atomic: a valid+invalid batch applies nothing.
    let resp = call(
        &mut w,
        &mut r,
        4,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "server.port", "value": 70000 },
        ] }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    let got = rpc(
        &mut w,
        &mut r,
        5,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(
        got["value"],
        json!(true),
        "nothing applied on a failed batch"
    );

    // Subscribe (no workspace scope → matches the global settings event).
    let (sub_read, mut sw) = connect_retry(&socket).await.into_split();
    let mut sr = BufReader::new(sub_read);
    send(
        &mut sw,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["settings:changed"] },
        }))
        .unwrap(),
    )
    .await;
    let _ = read_json(&mut sr).await;
    wait_for_subscriber_count(&bus, 1).await;

    // settings.update (non-secret) → applied + settings:changed (redacted).
    let applied = rpc(
        &mut w,
        &mut r,
        6,
        "settings.update",
        json!({ "changes": [{ "path": "git.autoCommit", "value": false }] }),
    )
    .await;
    assert_eq!(applied["applied"][0]["path"], "git.autoCommit");
    assert_eq!(applied["applied"][0]["value"], json!(false));
    let ev = read_json(&mut sr).await;
    assert_eq!(ev["method"], "events.event");
    assert_eq!(ev["params"]["event"]["type"], "settings:changed");
    assert_eq!(
        ev["params"]["event"]["data"]["changes"][0],
        json!({ "path": "git.autoCommit", "value": false })
    );
    let got = rpc(
        &mut w,
        &mut r,
        7,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(got["value"], json!(false));

    // settings.update (sensitive) → the secret is persisted to the keychain but
    // NEVER echoed: applied value + event are redacted, get/list stay redacted.
    let applied = rpc(
        &mut w,
        &mut r,
        8,
        "settings.update",
        json!({ "changes": [
            { "path": "sourceControl.github.token", "value": SECRET },
            { "path": "linear.token", "value": SECRET },
        ] }),
    )
    .await;
    let applied_text = serde_json::to_string(&applied).unwrap();
    assert!(
        !applied_text.contains(SECRET),
        "secret leaked in update result"
    );
    assert_ne!(applied["applied"][0]["value"], json!(SECRET));
    assert!(applied["applied"][0]["value"].is_string());
    assert_eq!(applied["applied"][1]["path"], "linear.token");
    assert_ne!(applied["applied"][1]["value"], json!(SECRET));
    assert!(applied["applied"][1]["value"].is_string());
    let ev = read_json(&mut sr).await;
    let ev_text = serde_json::to_string(&ev).unwrap();
    assert!(
        !ev_text.contains(SECRET),
        "secret leaked in settings:changed"
    );

    let got = rpc(
        &mut w,
        &mut r,
        9,
        "settings.get",
        json!({ "path": "sourceControl.github.token" }),
    )
    .await;
    assert!(!serde_json::to_string(&got).unwrap().contains(SECRET));
    assert!(
        got["value"].is_string(),
        "set secret reads as a placeholder"
    );
    assert_ne!(got["value"], json!(SECRET));

    let list = rpc(&mut w, &mut r, 10, "settings.list", json!({})).await;
    assert!(
        !serde_json::to_string(&list).unwrap().contains(SECRET),
        "secret leaked in settings.list"
    );

    // settings.reset → restores the default + emits settings:changed.
    let reset = rpc(
        &mut w,
        &mut r,
        11,
        "settings.reset",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(reset, json!({ "path": "git.autoCommit", "value": true }));
    let ev = read_json(&mut sr).await;
    assert_eq!(ev["params"]["event"]["type"], "settings:changed");
    assert_eq!(
        ev["params"]["event"]["data"]["changes"][0],
        json!({ "path": "git.autoCommit", "value": true })
    );

    // linear.token behaves the same as other secrets: get stays redacted and
    // reset clears the keychain entry (value back to null) + emits the event.
    let got = rpc(
        &mut w,
        &mut r,
        12,
        "settings.get",
        json!({ "path": "linear.token" }),
    )
    .await;
    assert!(!serde_json::to_string(&got).unwrap().contains(SECRET));
    assert!(
        got["value"].is_string(),
        "set secret reads as a placeholder"
    );
    let reset = rpc(
        &mut w,
        &mut r,
        13,
        "settings.reset",
        json!({ "path": "linear.token" }),
    )
    .await;
    assert_eq!(reset, json!({ "path": "linear.token", "value": null }));
    let ev = read_json(&mut sr).await;
    assert_eq!(ev["params"]["event"]["type"], "settings:changed");
    assert_eq!(
        ev["params"]["event"]["data"]["changes"][0],
        json!({ "path": "linear.token", "value": null })
    );

    // `workspace.sshKeyPath` is a **plain non-secret** path setting: the value
    // is the filesystem path to the key (not key material), so `settings.list`
    // must expose `sensitive: false` and `settings.update`/`get` must round-trip
    // the string verbatim — the FE `git`-env consumer needs to read it back to
    // hand it to `git`.
    let ssh_path = "/tmp/id_ed25519_for_agents";
    let list = rpc(&mut w, &mut r, 14, "settings.list", json!({})).await;
    let ssh_entry = entry(&list, "workspace.sshKeyPath");
    // `sensitive` is only emitted for sensitive definitions, so a path setting
    // omits the field entirely (see `SettingDefinition::definition_json`).
    assert!(
        ssh_entry.get("sensitive").is_none(),
        "workspace.sshKeyPath is a path, not a secret"
    );
    assert_eq!(ssh_entry["type"], "string");
    assert_eq!(ssh_entry["value"], Value::Null, "unset path reads as null");
    let applied = rpc(
        &mut w,
        &mut r,
        15,
        "settings.update",
        json!({ "changes": [{ "path": "workspace.sshKeyPath", "value": ssh_path }] }),
    )
    .await;
    assert_eq!(
        applied["applied"][0],
        json!({ "path": "workspace.sshKeyPath", "value": ssh_path }),
        "path setting round-trips in plaintext"
    );
    let _ = read_json(&mut sr).await; // drain the settings:changed event.
    let got = rpc(
        &mut w,
        &mut r,
        16,
        "settings.get",
        json!({ "path": "workspace.sshKeyPath" }),
    )
    .await;
    assert_eq!(got["value"], json!(ssh_path), "get returns plaintext path");
    let list = rpc(&mut w, &mut r, 17, "settings.list", json!({})).await;
    assert_eq!(
        entry(&list, "workspace.sshKeyPath")["value"],
        json!(ssh_path),
        "list returns plaintext path"
    );

    // `workspaceApi.*` — the workspace_api output knobs are plain non-secret
    // TOML-backed settings: list/get expose the defaults with their bounds,
    // update/reset round-trip, and out-of-range / mistyped values → -32602.
    let list = rpc(&mut w, &mut r, 18, "settings.list", json!({})).await;
    let chars = entry(&list, "workspaceApi.maxOutputChars");
    assert_eq!(chars["type"], "number");
    assert_eq!(chars["value"], json!(100_000.0));
    assert_eq!(chars["min"], json!(0.0));
    assert_eq!(chars["max"], json!(10_000_000.0));
    assert!(chars.get("sensitive").is_none());
    let toon = entry(&list, "workspaceApi.toonOutput");
    assert_eq!(toon["type"], "boolean");
    assert_eq!(toon["value"], json!(true));
    let got = rpc(
        &mut w,
        &mut r,
        19,
        "settings.get",
        json!({ "path": "workspaceApi.maxOutputChars" }),
    )
    .await;
    assert_eq!(got["value"], json!(100_000.0));
    assert_eq!(got["definition"]["type"], "number");

    // Catalog validation → -32602, nothing applied.
    for bad in [
        json!([{ "path": "workspaceApi.maxOutputChars", "value": 20_000_000 }]),
        json!([{ "path": "workspaceApi.maxOutputChars", "value": "lots" }]),
        json!([{ "path": "workspaceApi.toonOutput", "value": "yes" }]),
    ] {
        let resp = call(
            &mut w,
            &mut r,
            20,
            "settings.update",
            json!({ "changes": bad }),
        )
        .await;
        assert_eq!(resp["error"]["code"], -32602, "expected -32602 for {resp}");
    }

    // Round-trip: update both, then reset back to the defaults.
    let applied = rpc(
        &mut w,
        &mut r,
        21,
        "settings.update",
        json!({ "changes": [
            { "path": "workspaceApi.maxOutputChars", "value": 250_000 },
            { "path": "workspaceApi.toonOutput", "value": false },
        ] }),
    )
    .await;
    assert_eq!(applied["applied"][0]["path"], "workspaceApi.maxOutputChars");
    assert_eq!(applied["applied"][1]["path"], "workspaceApi.toonOutput");
    assert_eq!(applied["applied"][1]["value"], json!(false));
    let _ = read_json(&mut sr).await; // drain the settings:changed event.
    let got = rpc(
        &mut w,
        &mut r,
        22,
        "settings.get",
        json!({ "path": "workspaceApi.maxOutputChars" }),
    )
    .await;
    assert_eq!(got["value"], json!(250_000));
    let got = rpc(
        &mut w,
        &mut r,
        23,
        "settings.get",
        json!({ "path": "workspaceApi.toonOutput" }),
    )
    .await;
    assert_eq!(got["value"], json!(false));
    let reset = rpc(
        &mut w,
        &mut r,
        24,
        "settings.reset",
        json!({ "path": "workspaceApi.maxOutputChars" }),
    )
    .await;
    assert_eq!(reset["value"], json!(100_000.0));
    let _ = read_json(&mut sr).await; // drain the settings:changed event.
    let reset = rpc(
        &mut w,
        &mut r,
        25,
        "settings.reset",
        json!({ "path": "workspaceApi.toonOutput" }),
    )
    .await;
    assert_eq!(reset["value"], json!(true));
    let _ = read_json(&mut sr).await; // drain the settings:changed event.

    // `agentFeatures.*` — the nine agent feature toggles are plain non-secret
    // TOML-backed booleans defaulting to on: list/get expose the defaults,
    // update/reset round-trip, and mistyped values → -32602.
    let list = rpc(&mut w, &mut r, 26, "settings.list", json!({})).await;
    for path in [
        "agentFeatures.backgroundHooks",
        "agentFeatures.hostExec",
        "agentFeatures.scripts",
        "agentFeatures.terminalAccess",
        "agentFeatures.browserAutomation",
        "agentFeatures.richChatBlocks",
        "agentFeatures.structuredQuestions",
        "agentFeatures.attentionRequests",
        "agentFeatures.prMonitor",
    ] {
        let e = entry(&list, path);
        assert_eq!(e["type"], "boolean", "{path}");
        assert_eq!(e["value"], json!(true), "{path}");
        assert_eq!(e["category"], "agentFeatures", "{path}");
        assert!(e.get("sensitive").is_none(), "{path}");
    }

    // `[prMonitor]` — two non-secret TOML-backed numbers with a floor of 10.
    for (path, default) in [
        ("prMonitor.debounceSeconds", 60.0),
        ("prMonitor.pollSeconds", 30.0),
    ] {
        let e = entry(&list, path);
        assert_eq!(e["type"], "number", "{path}");
        assert_eq!(e["value"], json!(default), "{path}");
        assert_eq!(e["category"], "prMonitor", "{path}");
        assert_eq!(e["min"], json!(10.0), "{path}");
        assert!(e.get("sensitive").is_none(), "{path}");
    }
    let resp = call(
        &mut w,
        &mut r,
        27,
        "settings.update",
        json!({ "changes": [{ "path": "agentFeatures.hostExec", "value": "off" }] }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602, "expected -32602 for {resp}");
    let applied = rpc(
        &mut w,
        &mut r,
        28,
        "settings.update",
        json!({ "changes": [{ "path": "agentFeatures.hostExec", "value": false }] }),
    )
    .await;
    assert_eq!(
        applied["applied"][0],
        json!({ "path": "agentFeatures.hostExec", "value": false })
    );
    let _ = read_json(&mut sr).await; // drain the settings:changed event.
    let got = rpc(
        &mut w,
        &mut r,
        29,
        "settings.get",
        json!({ "path": "agentFeatures.hostExec" }),
    )
    .await;
    assert_eq!(got["value"], json!(false));
    let reset = rpc(
        &mut w,
        &mut r,
        30,
        "settings.reset",
        json!({ "path": "agentFeatures.hostExec" }),
    )
    .await;
    assert_eq!(reset["value"], json!(true));
    let _ = read_json(&mut sr).await; // drain the settings:changed event.

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
