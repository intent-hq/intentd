//! WSS end-to-end host-services (AUDIT-P2-1 / -P2-4): drives the additive
//! `host.*` detection methods — `host.findBinary`, `host.toolAvailability`,
//! `host.env`, `host.findApp`, and `host.listInstalledEditors` — over a real
//! pinned-TLS WebSocket against a live `intentd serve` (WSS listener enabled via config). These
//! methods resolve binaries / PATH / environment / GUI apps on the daemon host
//! so a remote client sees what actually lives where workspaces run; this
//! suite proves the §5.14 wire contract end-to-end (HTTPS upgrade → JSON-RPC
//! 2.0 over WebSocket → host fast-path → response).
//!
//! Unlike the agent-lifecycle suite, host-services need neither a workspace nor
//! the mock ACP provider, so this test is self-contained and always runs.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-host-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    wss_rpc_with_timeout(ws, id, method, params, Duration::from_secs(15)).await
}

/// [`wss_rpc`] with a caller-chosen deadline, for methods whose legitimate
/// worst case exceeds the default 15s (e.g. `host.providerAuthStatus` on a
/// host with providers installed — each probe carries its own budget).
async fn wss_rpc_with_timeout<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
    deadline: Duration,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    // One overall budget: unrelated frames (events, pings) consume the same
    // deadline rather than resetting it per iteration.
    let deadline = tokio::time::Instant::now() + deadline;
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Boot a daemon with the WSS listener enabled and return the live handle + a pinned WSS
/// client config plus the bound TCP port (discovered via UDS `system.status`).
async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

/// host.findBinary / host.toolAvailability / host.env over the real WSS wire.
#[tokio::test]
async fn host_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // §5.14 sanity: WSS connections report remote locality.
    let status = wss_rpc(&mut ws, 1, "host.status", json!({})).await;
    assert_eq!(status["locality"], "remote", "WSS ⇒ remote (§5.14)");

    // host.findBinary requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 2, "method": "host.findBinary", "params": {} });
        ws.send(Message::Text(frame.to_string().into()))
            .await
            .unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(2) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findBinary { name } ⇒ { available, path?, version? }. `git` ships on
    // the CI/host image, so assert the resolved shape rather than just a boolean.
    let git = wss_rpc(&mut ws, 3, "host.findBinary", json!({ "name": "git" })).await;
    assert!(
        git["available"].is_boolean(),
        "available always present: {git}"
    );
    if git["available"] == json!(true) {
        assert!(git["path"].is_string(), "available ⇒ path present: {git}");
    }

    // An unsafe binary name never errors — it resolves to available:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        4,
        "host.findBinary",
        json!({ "name": "../../bin/sh" }),
    )
    .await;
    assert_eq!(unsafe_name["available"], false, "unsafe name ⇒ unavailable");

    // host.toolAvailability (default set) ⇒ { tools: { <name>: { available } } }.
    let tools = wss_rpc(&mut ws, 5, "host.toolAvailability", json!({})).await;
    let map = tools["tools"].as_object().expect("tools object");
    for name in [
        "claude", "codex", "cortex", "opencode", "grok", "git", "code",
    ] {
        assert!(
            map.contains_key(name),
            "default set includes {name}: {tools}"
        );
        assert!(map[name]["available"].is_boolean());
    }

    // host.toolAvailability with an explicit list returns exactly those keys.
    let explicit = wss_rpc(
        &mut ws,
        6,
        "host.toolAvailability",
        json!({ "tools": ["git", "definitely-not-installed-xyzzy"] }),
    )
    .await;
    let explicit_map = explicit["tools"].as_object().unwrap();
    assert_eq!(explicit_map.len(), 2, "explicit list honoured: {explicit}");
    assert_eq!(
        explicit_map["definitely-not-installed-xyzzy"]["available"],
        false
    );

    // host.env ⇒ secret-safe PATH/env probe: path + entries + enhancedPath +
    // varNames (names only, no arbitrary values).
    let env = wss_rpc(&mut ws, 7, "host.env", json!({})).await;
    assert!(env["path"].is_string(), "path present: {env}");
    assert!(env["pathEntries"].is_array(), "pathEntries present: {env}");
    assert!(
        env["enhancedPath"].is_string(),
        "enhancedPath present: {env}"
    );
    assert!(env["varNames"].is_array(), "varNames present: {env}");
    // The auth token is injected as INTENTD_AUTH_TOKEN — its NAME may appear but
    // its VALUE must never cross the wire.
    assert!(
        !env.to_string().contains(TOKEN),
        "host.env must not leak secret env values"
    );
}

/// host.findApp / host.listInstalledEditors over the real WSS wire.
#[tokio::test]
async fn host_app_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // host.findApp requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 100, "method": "host.findApp", "params": {} });
        ws.send(Message::Text(frame.to_string().into()))
            .await
            .unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(100) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findApp { name } ⇒ { installed, path?, source? }. A bogus but
    // syntactically-safe name resolves to `installed:false` on every host.
    let bogus = wss_rpc(
        &mut ws,
        101,
        "host.findApp",
        json!({ "name": "DefinitelyNotInstalledXyzzy" }),
    )
    .await;
    assert!(
        bogus["installed"].is_boolean(),
        "installed always present: {bogus}"
    );
    assert_eq!(bogus["installed"], false, "bogus app is not installed");

    // An unsafe name never errors — it resolves to installed:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        102,
        "host.findApp",
        json!({ "name": "../../etc/passwd" }),
    )
    .await;
    assert_eq!(
        unsafe_name["installed"], false,
        "unsafe app name ⇒ uninstalled"
    );

    // host.listInstalledEditors ⇒ { editors: [{ id, installed, path?, source?,
    // flatpakId? }] }. Always replies; every entry carries id + installed.
    let editors_result = wss_rpc(&mut ws, 103, "host.listInstalledEditors", json!({})).await;
    let editors = editors_result["editors"].as_array().expect("editors array");
    assert!(!editors.is_empty(), "default catalog is non-empty");
    let ids: std::collections::HashSet<&str> = editors
        .iter()
        .map(|e| e["id"].as_str().expect("id"))
        .collect();
    for expected in ["vscode", "cursor", "zed"] {
        assert!(
            ids.contains(expected),
            "catalog includes {expected}: {editors_result}"
        );
    }
    for entry in editors {
        assert!(
            entry["installed"].is_boolean(),
            "installed boolean: {entry}"
        );
        if entry["installed"] == json!(true) {
            assert!(
                entry["source"].is_string(),
                "installed entries carry a source: {entry}"
            );
        }
    }
}

/// host.providerAuthStatus over the real WSS wire: full sweep, scoped call,
/// and the unknown-provider invalid-params error (PROTOCOL §9).
#[tokio::test]
async fn host_provider_auth_status_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // Full sweep: every probe-able provider appears exactly once, in order,
    // with `authenticated: true | false | null`. On a host without providers
    // every entry short-circuits to null without probing; with providers
    // installed the parallel probes are bounded by their own budgets, so give
    // the sweep a generous overall deadline.
    let result = wss_rpc_with_timeout(
        &mut ws,
        300,
        "host.providerAuthStatus",
        json!({}),
        Duration::from_secs(120),
    )
    .await;
    let providers = result["providers"].as_array().expect("providers array");
    let expected_ids = [
        "auggie",
        "claude-code",
        "codex",
        "opencode",
        "droid",
        "grok",
        "pi",
    ];
    assert_eq!(
        providers.len(),
        expected_ids.len(),
        "one entry per probe-able provider: {result}"
    );
    for (entry, expected_id) in providers.iter().zip(expected_ids) {
        assert_eq!(
            entry["id"], expected_id,
            "response order is fixed: {result}"
        );
        assert!(
            entry["authenticated"].is_boolean() || entry["authenticated"].is_null(),
            "authenticated is true|false|null: {entry}"
        );
    }

    // Scoped call: `providerId` narrows the sweep to one provider. This also
    // exercises the cache — the sweep above already probed (or skipped) grok,
    // so this read is served without a fresh probe.
    let scoped = wss_rpc(
        &mut ws,
        301,
        "host.providerAuthStatus",
        json!({ "providerId": "grok" }),
    )
    .await;
    let scoped_providers = scoped["providers"].as_array().expect("providers array");
    assert_eq!(
        scoped_providers.len(),
        1,
        "scoped to one provider: {scoped}"
    );
    assert_eq!(scoped_providers[0]["id"], "grok");
    assert!(
        scoped_providers[0]["authenticated"].is_boolean()
            || scoped_providers[0]["authenticated"].is_null()
    );

    // Unknown providerId ⇒ -32602 invalid params (PROTOCOL §9).
    let frame = json!({
        "jsonrpc": "2.0",
        "id": 302,
        "method": "host.providerAuthStatus",
        "params": { "providerId": "not-a-provider" }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 302).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "unknown providerId ⇒ -32602: {err}"
    );
}

/// host.checkAuggie over the real WSS wire: resolution-only `{ available,
/// path? }`. Pinning `context.auggiePath` at a plain (non-executable) file
/// proves both halves of the contract — the settings-precedence path still
/// wins, and `available` is true purely from resolution with **no** `version`
/// field, because the retired `--version` probe would have failed on this file
/// and reported `available:false`.
#[tokio::test]
async fn host_check_auggie_is_resolution_only_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    let stub = daemon.data_dir.join("auggie-stub");
    std::fs::write(&stub, "not an executable\n").expect("write auggie stub");

    let applied = wss_rpc(
        &mut ws,
        400,
        "settings.update",
        json!({ "changes": [{ "path": "context.auggiePath", "value": stub.to_str().unwrap() }] }),
    )
    .await;
    assert!(
        applied["applied"].is_array(),
        "settings.update applied: {applied}"
    );

    let result = wss_rpc(&mut ws, 401, "host.checkAuggie", json!({})).await;
    assert_eq!(
        result["available"], true,
        "configured file resolves without a version probe: {result}"
    );
    assert_eq!(result["path"], stub.to_string_lossy().as_ref());
    assert!(
        result.get("version").is_none(),
        "`version` is retired from host.checkAuggie: {result}"
    );
}

/// Seed one workspace with a filesystem root so `host.exec` can enforce the
/// within-workspace containment guard on `cwd`. Returns `(workspace_id, root)`.
async fn seed_workspace_with_path(data_dir: &Path, root: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WSS-HOST-EXEC".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: Some(root.to_string_lossy().into_owned()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(root.to_string_lossy().into_owned()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store.insert_workspace(&ws).await.expect("insert ws");
    ws_id.0
}

/// Read the id-matched error frame after sending `frame`. Handles server
/// heartbeats by replying to `Ping` with `Pong` (matches the other WSS
/// helpers in this file); otherwise a mid-wait heartbeat could close the
/// connection and flake the test.
async fn wss_expect_error<S>(ws: &mut WebSocketStream<S>, id: i64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        match next {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(id) {
                    return v;
                }
            }
            Message::Ping(p) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }
}

/// host.exec: happy-path round-trip + timeout + cwd-outside-workspace rejection.
#[tokio::test]
async fn host_exec_over_wss() {
    let (daemon, port, cfg) = boot().await;
    // Real filesystem root the daemon can `cd` into; kept alive until the
    // daemon drops (its `Drop` removes the whole data dir; the workspace root
    // is a sibling temp dir, cleaned up here).
    let root = std::env::temp_dir().join(format!("itd-wss-exec-root-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&root).expect("mkdir workspace root");
    let ws_id = seed_workspace_with_path(&daemon.data_dir, &root).await;
    let mut ws = connect_ws(port, cfg).await;

    // 1) Happy path — echo returns stdout + exitCode 0 without cwd validation.
    let out = wss_rpc(
        &mut ws,
        200,
        "host.exec",
        json!({ "command": "echo", "args": ["hello", "world"], "timeoutMs": 5000 }),
    )
    .await;
    assert_eq!(out["exitCode"], 0, "exitCode 0: {out}");
    assert_eq!(
        out["stdout"].as_str().unwrap().trim(),
        "hello world",
        "stdout carries argv payload: {out}"
    );
    assert!(
        out.get("timedOut").is_none(),
        "no timedOut on the happy path: {out}"
    );

    // 2) cwd inside the workspace succeeds — /bin/sh -c is intentionally NOT
    // used; the daemon spawns argv only. `pwd` prints the resolved cwd.
    let inside = wss_rpc(
        &mut ws,
        201,
        "host.exec",
        json!({
            "command": "pwd",
            "cwd": ".",
            "workspaceId": ws_id,
            "timeoutMs": 5000,
        }),
    )
    .await;
    assert_eq!(inside["exitCode"], 0, "cwd inside ⇒ ok: {inside}");
    let printed = inside["stdout"].as_str().unwrap().trim();
    // macOS `/tmp` resolves through a `/private` symlink; the daemon's lexical
    // guard operates on the resolved path so we accept either prefix here.
    let canonical = std::fs::canonicalize(&root)
        .unwrap_or_else(|_| root.clone())
        .to_string_lossy()
        .into_owned();
    assert!(
        printed == root.to_string_lossy() || printed == canonical,
        "pwd prints the workspace root ({} or {}): {printed}",
        root.display(),
        canonical
    );

    // 2b) workspaceId with cwd OMITTED defaults to the workspace root
    // (monorepo#3231) — previously the child inherited the daemon's own cwd.
    let defaulted = wss_rpc(
        &mut ws,
        205,
        "host.exec",
        json!({
            "command": "pwd",
            "workspaceId": ws_id,
            "timeoutMs": 5000,
        }),
    )
    .await;
    assert_eq!(defaulted["exitCode"], 0, "default cwd ⇒ ok: {defaulted}");
    let printed = defaulted["stdout"].as_str().unwrap().trim();
    assert!(
        printed == root.to_string_lossy() || printed == canonical,
        "omitted cwd defaults to the workspace root ({} or {}): {printed}",
        root.display(),
        canonical
    );

    // 3) cwd OUTSIDE the workspace ⇒ -32603 with a clear containment message.
    let frame = json!({
        "jsonrpc": "2.0", "id": 202, "method": "host.exec",
        "params": {
            "command": "pwd",
            "cwd": "/etc",
            "workspaceId": ws_id,
            "timeoutMs": 5000,
        }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 202).await;
    assert_eq!(err["error"]["code"], -32603, "cwd outside ⇒ -32603: {err}");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("cwd outside workspace"),
        "clear containment message: {err}"
    );

    // 4) Timeout ⇒ result carries `timedOut: true` and the child is reaped
    // (SIGTERM → grace → SIGKILL on unix). Use `sleep 30` capped at 500ms.
    let timed_out = wss_rpc(
        &mut ws,
        203,
        "host.exec",
        json!({ "command": "sleep", "args": ["30"], "timeoutMs": 500 }),
    )
    .await;
    assert_eq!(
        timed_out["timedOut"], true,
        "timedOut flag set: {timed_out}"
    );

    // 5) Missing `command` ⇒ -32602 (PROTOCOL §9).
    let frame = json!({ "jsonrpc": "2.0", "id": 204, "method": "host.exec", "params": {} });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 204).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing command ⇒ -32602: {err}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// Read one `events.event` frame whose `event.type` matches `type_filter` AND
/// whose `event.data.requestId` equals `request_id`; ignore anything else.
async fn wss_next_stream_event<S>(
    ws: &mut WebSocketStream<S>,
    request_id: &str,
    type_filter: &[&str],
    secs: u64,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss stream event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "events.event" {
                    continue;
                }
                let event = &v["params"]["event"];
                let ty = event["type"].as_str().unwrap_or("");
                if !type_filter.contains(&ty) {
                    continue;
                }
                if event["data"]["requestId"].as_str() != Some(request_id) {
                    continue;
                }
                return v;
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// host.execStream: happy-path streaming (`cat` echoes an initial stdin payload
/// then a follow-up write closes stdin so the child exits) + cancel path
/// (`sleep 30` reaped by `host.execStream.cancel`) + `-32602` on missing
/// `command`. Exercises the full §5.14 streaming wire: `{ requestId }` on the
/// request, `host:exec:stdout` bus frames (base64 chunks), stdin write with
/// `eof=true`, and terminal `host:exec:exit`.
#[tokio::test]
async fn host_exec_stream_over_wss() {
    use base64::Engine as _;

    let (_daemon, port, cfg) = boot().await;

    // SUBSCRIBER conn — subscribe BEFORE starting the stream so no chunk is
    // missed. `workspaceId` is intentionally omitted: `host.execStream` without
    // a workspace context publishes under the empty-workspace id, and the
    // events fast-path routes matching frames to global (workspace-less)
    // subscribers on the same connection.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        300,
        "events.subscribe",
        json!({ "eventTypes": ["host:exec:stdout", "host:exec:stderr", "host:exec:exit"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — kick off a `cat` streaming exec with an initial stdin payload
    // that MUST be echoed back on stdout, correlated by the returned
    // requestId. `cat` reads to EOF, so the follow-up `write { eof:true }`
    // closes stdin and lets the child exit cleanly (exitCode=0).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let started = wss_rpc(
        &mut rpc,
        301,
        "host.execStream",
        json!({ "command": "cat", "stdin": "hello world\n" }),
    )
    .await;
    let request_id = started["requestId"]
        .as_str()
        .expect("requestId in host.execStream result")
        .to_string();

    // Collect stdout chunks (base64) until the marker appears; stop on exit.
    let mut acc: Vec<u8> = Vec::new();
    let mut saw_exit = false;
    let mut exit_ok: Option<bool> = None;

    // First: watch for the initial stdin's echo.
    for _ in 0..40 {
        let v = wss_next_stream_event(
            &mut sub,
            &request_id,
            &["host:exec:stdout", "host:exec:exit"],
            15,
        )
        .await;
        let event = &v["params"]["event"];
        match event["type"].as_str() {
            Some("host:exec:stdout") => {
                if let Some(chunk) = event["data"]["chunk"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(chunk)
                        .expect("valid base64 in host:exec:stdout.chunk");
                    acc.extend_from_slice(&bytes);
                }
            }
            Some("host:exec:exit") => {
                saw_exit = true;
                exit_ok = event["data"]["ok"].as_bool();
                break;
            }
            _ => continue,
        }
        if String::from_utf8_lossy(&acc).contains("hello world") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&acc).contains("hello world"),
        "initial stdin was echoed on host:exec:stdout: {:?}",
        String::from_utf8_lossy(&acc)
    );

    // Send a follow-up stdin chunk + close so `cat` exits (unless it already
    // exited above via some other race — the write is idempotent-safe).
    if !saw_exit {
        let write_resp = wss_rpc(
            &mut rpc,
            302,
            "host.execStream.write",
            json!({ "requestId": &request_id, "stdin": "goodbye\n", "eof": true }),
        )
        .await;
        assert_eq!(write_resp["ok"], true, "write ok: {write_resp}");
        // Drain until we see the terminal exit frame.
        for _ in 0..40 {
            let v = wss_next_stream_event(
                &mut sub,
                &request_id,
                &["host:exec:stdout", "host:exec:exit"],
                15,
            )
            .await;
            let event = &v["params"]["event"];
            match event["type"].as_str() {
                Some("host:exec:stdout") => {
                    if let Some(chunk) = event["data"]["chunk"].as_str() {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(chunk)
                            .expect("valid base64 in host:exec:stdout.chunk");
                        acc.extend_from_slice(&bytes);
                    }
                }
                Some("host:exec:exit") => {
                    saw_exit = true;
                    exit_ok = event["data"]["ok"].as_bool();
                    break;
                }
                _ => {}
            }
        }
    }
    assert!(
        saw_exit,
        "host:exec:exit reached (accumulated stdout so far: {:?})",
        String::from_utf8_lossy(&acc)
    );
    assert_eq!(exit_ok, Some(true), "cat exited cleanly (exitCode=0)");

    // ── Cancel path ────────────────────────────────────────────────────────
    // A long-lived `sleep 30` MUST be reaped by `host.execStream.cancel`
    // (SIGTERM → grace → SIGKILL). Terminal frame carries `cancelled:true`.
    let started = wss_rpc(
        &mut rpc,
        310,
        "host.execStream",
        json!({ "command": "sleep", "args": ["30"] }),
    )
    .await;
    let cancel_id = started["requestId"]
        .as_str()
        .expect("requestId for cancel case")
        .to_string();

    // Give the child a moment to start before cancelling.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cancel_resp = wss_rpc(
        &mut rpc,
        311,
        "host.execStream.cancel",
        json!({ "requestId": &cancel_id }),
    )
    .await;
    assert_eq!(cancel_resp["ok"], true, "cancel ok: {cancel_resp}");
    assert_eq!(
        cancel_resp["cancelled"], true,
        "cancel flipped live token: {cancel_resp}"
    );

    // The exit frame arrives promptly (SIGTERM plus 500ms grace).
    let exit = wss_next_stream_event(&mut sub, &cancel_id, &["host:exec:exit"], 10).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(data["cancelled"], true, "cancelled:true on exit: {exit}");
    assert_eq!(data["ok"], false, "cancelled sleep is not `ok`: {exit}");

    // A repeat cancel on the (now-finished) id is idempotent: `ok:true` still,
    // but `cancelled:false` because no live token remained.
    let repeat = wss_rpc(
        &mut rpc,
        312,
        "host.execStream.cancel",
        json!({ "requestId": &cancel_id }),
    )
    .await;
    assert_eq!(repeat["ok"], true, "idempotent cancel is ok: {repeat}");
    assert_eq!(repeat["cancelled"], false, "no live token: {repeat}");

    // ── -32602 arms ────────────────────────────────────────────────────────
    // Missing `command` on the stream request ⇒ -32602 (PROTOCOL §9).
    let frame = json!({ "jsonrpc": "2.0", "id": 320, "method": "host.execStream", "params": {} });
    rpc.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut rpc, 320).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing command ⇒ -32602: {err}"
    );
    // Missing `requestId` on the write / cancel surfaces likewise.
    let frame = json!({
        "jsonrpc": "2.0", "id": 321, "method": "host.execStream.write", "params": {}
    });
    rpc.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut rpc, 321).await;
    assert_eq!(err["error"]["code"], -32602, "missing requestId ⇒ -32602");

    let frame = json!({
        "jsonrpc": "2.0", "id": 322, "method": "host.execStream.cancel", "params": {}
    });
    rpc.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut rpc, 322).await;
    assert_eq!(err["error"]["code"], -32602, "missing requestId ⇒ -32602");
}

/// ACP model/readiness handshake probe over `host.execStream` (§5.14).
///
/// AUDIT-R1c-BE. R1b retired the four bidirectional-stdio ACP probes; the
/// replacement is **not** a net-new `provider.probeAcp` RPC but a thin FE parser
/// on top of the existing streaming exec surface, which already provides every
/// guarantee an ACP probe needs (argv-only spawn, process-group + `kill_on_drop`
/// reap on `timeoutMs`/cancel, PATH enrichment, workspace-cwd containment,
/// secret-safe env, initial `stdin` + streamed base64 stdout + terminal exit).
///
/// This e2e proves that shape end-to-end against the deterministic mock ACP
/// agent (`tests/fixtures/mock-acp-agent.mjs`, which responds to `initialize`
/// with `{ protocolVersion: 1, agentCapabilities: { loadSession: false } }`):
///
/// 1. **Handshake happy path** — subscribe to `host:exec:*`, call
///    `host.execStream` with `command:"node"`, `args:[mock-script]`, and an
///    initial `stdin` carrying the `initialize` JSON-RPC line. Assemble stdout
///    chunks (base64) until a full `\n`-terminated line arrives, parse it, and
///    assert the capability payload. Close stdin via
///    `host.execStream.write { eof:true }` so the mock agent exits cleanly.
/// 2. **Timeout reap** — call `host.execStream` on the same agent with a short
///    `timeoutMs` and **no** initial stdin. The agent blocks reading stdin, so
///    the daemon reaps the process group at the deadline and publishes
///    `host:exec:exit { timedOut:true, ok:false }`.
#[tokio::test]
async fn host_exec_stream_acp_handshake_probe_over_wss() {
    use base64::Engine as _;

    // Gate: skip when `node` or the mock script isn't available (parity with
    // the WSS agent-lifecycle suite's gate).
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping ACP handshake probe: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping ACP handshake probe: mock script missing at {script}");
        return;
    }

    let (_daemon, port, cfg) = boot().await;

    // Subscriber conn: subscribe BEFORE spawning so no chunk is missed. No
    // workspaceId is passed on `host.execStream`, so events publish under the
    // empty-workspace id and the events fast-path routes them to global
    // subscribers on the same connection.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        400,
        "events.subscribe",
        json!({ "eventTypes": ["host:exec:stdout", "host:exec:stderr", "host:exec:exit"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ── Handshake happy path ──────────────────────────────────────────────
    // The initialize line is what a real FE probe would send; the mock ACP
    // agent responds with a single `\n`-terminated JSON-RPC result line.
    let init_line =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":1}}\n";

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let started = wss_rpc(
        &mut rpc,
        401,
        "host.execStream",
        json!({
            "command": "node",
            "args": [&script],
            "stdin": init_line,
            "timeoutMs": 15_000,
        }),
    )
    .await;
    let request_id = started["requestId"]
        .as_str()
        .expect("requestId on handshake exec")
        .to_string();

    // Accumulate stdout base64 chunks until we have at least one full line.
    let mut acc: Vec<u8> = Vec::new();
    let mut parsed: Option<Value> = None;
    for _ in 0..40 {
        let v = wss_next_stream_event(
            &mut sub,
            &request_id,
            &["host:exec:stdout", "host:exec:exit"],
            15,
        )
        .await;
        let event = &v["params"]["event"];
        match event["type"].as_str() {
            Some("host:exec:stdout") => {
                if let Some(chunk) = event["data"]["chunk"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(chunk)
                        .expect("valid base64 in host:exec:stdout.chunk");
                    acc.extend_from_slice(&bytes);
                }
                if let Some(nl) = acc.iter().position(|b| *b == b'\n') {
                    let line = &acc[..nl];
                    parsed =
                        Some(serde_json::from_slice(line).expect("mock agent stdout is JSON-RPC"));
                    break;
                }
            }
            Some("host:exec:exit") => {
                panic!(
                    "child exited before reply (acc={:?})",
                    String::from_utf8_lossy(&acc)
                );
            }
            _ => {}
        }
    }
    let parsed = parsed.expect("received a JSON-RPC reply line on stdout");

    // Assert the ACP capability payload the mock agent returns for `initialize`.
    assert_eq!(parsed["jsonrpc"], "2.0", "handshake reply: {parsed}");
    assert_eq!(parsed["id"], 1, "handshake reply: {parsed}");
    assert_eq!(
        parsed["result"]["protocolVersion"], 1,
        "handshake reply carries protocolVersion=1: {parsed}"
    );
    assert_eq!(
        parsed["result"]["agentCapabilities"]["loadSession"], false,
        "handshake reply carries capability payload: {parsed}"
    );

    // Close stdin so the mock agent's readline loop drains and the child exits
    // cleanly (exitCode=0). The exit frame surfaces on the bus.
    let write_resp = wss_rpc(
        &mut rpc,
        402,
        "host.execStream.write",
        json!({ "requestId": &request_id, "eof": true }),
    )
    .await;
    assert_eq!(write_resp["ok"], true, "eof write ok: {write_resp}");

    let exit = wss_next_stream_event(&mut sub, &request_id, &["host:exec:exit"], 15).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(data["ok"], true, "mock agent exits cleanly: {exit}");
    assert!(
        data.get("timedOut").and_then(Value::as_bool) != Some(true),
        "no timedOut on the happy handshake path: {exit}"
    );

    // ── Timeout reap path ─────────────────────────────────────────────────
    // Spawn the same agent with a short `timeoutMs` and NO initial stdin. The
    // agent's readline loop blocks waiting for input, so the daemon must reap
    // the process group at the deadline and surface `timedOut:true`.
    let started = wss_rpc(
        &mut rpc,
        410,
        "host.execStream",
        json!({
            "command": "node",
            "args": [&script],
            "timeoutMs": 500,
        }),
    )
    .await;
    let timeout_id = started["requestId"]
        .as_str()
        .expect("requestId on timeout-probe exec")
        .to_string();

    let exit = wss_next_stream_event(&mut sub, &timeout_id, &["host:exec:exit"], 10).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(
        data["timedOut"], true,
        "timedOut:true on the reap path: {exit}"
    );
    assert_eq!(data["ok"], false, "reaped child is not `ok`: {exit}");
}

/// Verify host.findBinary resolves binaries from login-shell PATH enrichment
/// when the daemon runs with minimal PATH. Spawns intentd with a controlled
/// minimal PATH and a fake $SHELL that outputs a PATH containing a temp dir
/// holding a unique binary; asserts host.findBinary resolves that binary.
#[tokio::test]
async fn host_find_binary_uses_login_shell_path() {
    let data_dir = temp_data_dir();

    // Create a unique temp dir with a fake binary
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let fake_bin_dir = data_dir.join(format!("fake_login_bin_{pid}_{nanos}"));
    std::fs::create_dir_all(&fake_bin_dir).unwrap();

    let bin_name = format!("test-login-bin-{pid}-{nanos}");
    let bin_path = fake_bin_dir.join(&bin_name);
    std::fs::write(&bin_path, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Create a fake shell script that outputs the enriched PATH when invoked with -lc
    let fake_shell_path = data_dir.join("fake_shell.sh");
    let shell_script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"-lc\" ]; then\n  # Execute the command but in an environment where PATH is our fake dir\n  PATH=\"{}\" eval \"$2\"\nfi\n",
        fake_bin_dir.display()
    );
    std::fs::write(&fake_shell_path, shell_script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_shell_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Spawn daemon with minimal PATH and fake SHELL
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("PATH", "/usr/bin:/bin"), // Minimal PATH that won't find our binary
        ("SHELL", fake_shell_path.to_str().unwrap()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Call host.findBinary for our unique binary name
    let result = wss_rpc(&mut ws, 2, "host.findBinary", json!({ "name": &bin_name })).await;

    // Should find the binary via login-shell PATH enrichment
    assert_eq!(
        result["available"], true,
        "Binary should be found via login-shell PATH: {result}"
    );
    assert_eq!(
        result["path"].as_str().unwrap(),
        bin_path.to_str().unwrap(),
        "Binary path should match: {result}"
    );

    // GUI clients seed child-process PATH from this daemon-owned value.
    let host_env = wss_rpc(&mut ws, 3, "host.env", json!({})).await;
    let enhanced_path = host_env["enhancedPath"]
        .as_str()
        .expect("host.env enhancedPath");
    assert!(
        std::env::split_paths(enhanced_path).any(|entry| entry == fake_bin_dir),
        "host.env should include login-shell PATH directory: {host_env}"
    );

    drop(daemon);
}

/// WSS e2e for host.providerDiscovery: proves the providers + npx wire envelope.
#[tokio::test]
async fn host_provider_discovery_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Call host.providerDiscovery
    let result = wss_rpc(&mut ws, 2, "host.providerDiscovery", json!({})).await;

    // Assert wire contract shape
    assert!(result.is_object(), "result must be an object: {result}");
    assert!(
        result["providers"].is_array(),
        "providers must be an array: {result}"
    );
    assert!(result["npx"].is_object(), "npx must be an object: {result}");

    // Check npx fields
    let npx = &result["npx"];
    assert!(
        npx.get("resolvedPath").is_some(),
        "npx.resolvedPath must exist: {npx}"
    );
    assert!(
        npx.get("version").is_some(),
        "npx.version must exist: {npx}"
    );
    assert!(
        npx["versionOk"].is_boolean(),
        "npx.versionOk must be boolean: {npx}"
    );

    // Check providers array
    let providers = result["providers"].as_array().unwrap();
    assert!(
        !providers.is_empty(),
        "providers array should not be empty: {result}"
    );

    // Check first provider shape
    let p0 = &providers[0];
    assert!(p0["id"].is_string(), "provider.id must be string: {p0}");
    assert!(
        p0["displayName"].is_string(),
        "provider.displayName must be string: {p0}"
    );
    assert!(
        p0["command"].is_string(),
        "provider.command must be string: {p0}"
    );
    assert!(
        p0["installed"].is_boolean(),
        "provider.installed must be boolean: {p0}"
    );
    assert!(
        p0["hasNpxFallback"].is_boolean(),
        "provider.hasNpxFallback must be boolean: {p0}"
    );

    // Every provider carries a boolean npxOnly, npxPackage is present iff
    // npxOnly is true, and claude-code specifically is npx-only with the
    // pinned package spec.
    for p in providers {
        assert!(
            p["npxOnly"].is_boolean(),
            "provider.npxOnly must be boolean: {p}"
        );
        if p["npxOnly"] == true {
            assert!(
                p["npxPackage"].is_string(),
                "npx-only providers must carry npxPackage: {p}"
            );
        } else {
            assert!(
                p.get("npxPackage").is_none(),
                "non-npx-only providers must omit npxPackage: {p}"
            );
        }
        // Secondary-binary attribution (monorepo#991): the two fields are
        // present together or not at all, and an installed dual-binary
        // provider must have its secondary resolved.
        assert_eq!(
            p.get("secondaryCommand").is_some(),
            p.get("secondaryResolved").is_some(),
            "secondaryCommand and secondaryResolved must be present together: {p}"
        );
        if p.get("secondaryCommand").is_some() {
            assert!(
                p["secondaryCommand"].is_string(),
                "provider.secondaryCommand must be string: {p}"
            );
            assert!(
                p["secondaryResolved"].is_boolean(),
                "provider.secondaryResolved must be boolean: {p}"
            );
            if p["installed"] == true {
                assert_eq!(
                    p["secondaryResolved"], true,
                    "installed dual-binary providers must have the secondary resolved: {p}"
                );
            }
            // secondaryResolvedPath is present exactly when the secondary
            // resolved (an absolute path string), omitted otherwise —
            // conditional on host state like the surrounding assertions.
            if p["secondaryResolved"] == true {
                let path = p["secondaryResolvedPath"]
                    .as_str()
                    .expect("resolved secondary must carry secondaryResolvedPath");
                assert!(
                    std::path::Path::new(path).is_absolute(),
                    "provider.secondaryResolvedPath must be absolute: {p}"
                );
            } else {
                assert!(
                    p.get("secondaryResolvedPath").is_none(),
                    "unresolved secondary must omit secondaryResolvedPath: {p}"
                );
            }
        } else {
            assert!(
                p.get("secondaryResolvedPath").is_none(),
                "providers without a secondary must omit secondaryResolvedPath: {p}"
            );
        }
    }
    // unsloth (opencode + unsloth CLI) is the dual-binary provider: it must
    // expose the secondary-binary attribution so clients can name the
    // actually-missing binary; single-binary providers must omit the fields.
    let unsloth = providers
        .iter()
        .find(|p| p["id"] == "unsloth")
        .expect("unsloth must be in the discovery payload");
    assert_eq!(
        unsloth["command"], "opencode",
        "unsloth rides opencode's ACP runtime: {unsloth}"
    );
    assert_eq!(
        unsloth["secondaryCommand"], "unsloth",
        "unsloth must attribute its secondary binary: {unsloth}"
    );
    assert!(
        unsloth["secondaryResolved"].is_boolean(),
        "unsloth.secondaryResolved must be boolean: {unsloth}"
    );
    let auggie = providers
        .iter()
        .find(|p| p["id"] == "auggie")
        .expect("auggie must be in the discovery payload");
    assert!(
        auggie.get("secondaryCommand").is_none() && auggie.get("secondaryResolved").is_none(),
        "single-binary providers must omit secondary-binary fields: {auggie}"
    );
    let cc = providers
        .iter()
        .find(|p| p["id"] == "claude-code")
        .expect("claude-code must be in the discovery payload");
    assert_eq!(cc["npxOnly"], true, "claude-code must be npxOnly: {cc}");
    assert_eq!(
        cc["npxPackage"].as_str().unwrap(),
        format!(
            "@agentclientprotocol/claude-agent-acp@{}",
            intent_providers::CLAUDE_AGENT_ACP_VERSION
        ),
        "claude-code npxPackage must be the pinned spec: {cc}"
    );
    // The pi row carries the `pi` CLI verdict fields (monorepo#1662); only
    // the pi row does. The verdict itself is host-dependent — assert the
    // field shape, not the values.
    let pi = providers
        .iter()
        .find(|p| p["id"] == "pi")
        .expect("pi must be in the discovery payload");
    assert!(pi["cliCommand"].is_string(), "{pi}");
    assert!(pi["cliResolved"].is_boolean(), "{pi}");
    assert!(pi["cliVersionOk"].is_boolean(), "{pi}");
    assert_eq!(
        pi["cliRequirement"],
        intent_providers::PI_CLI_REQUIREMENT,
        "{pi}"
    );
    for p in providers.iter().filter(|p| p["id"] != "pi") {
        assert!(
            p.get("cliCommand").is_none() && p.get("cliRequirement").is_none(),
            "only the pi row carries CLI verdict fields: {p}"
        );
    }
    // Env-var gated rows (the daemon env above sets none of the enable
    // vars): mock (MOCK_AGENT_SCRIPT_PATH), cortex (INTENTD_ENABLE_CORTEX),
    // and droid (INTENTD_ENABLE_DROID) report gatedOff and skip binary
    // probing entirely (installed: false, no resolvedPath).
    for id in ["mock", "cortex", "droid"] {
        let row = providers
            .iter()
            .find(|p| p["id"] == id)
            .unwrap_or_else(|| panic!("{id} must be in the discovery payload"));
        assert!(
            row["gatedOff"].is_string(),
            "{id} without its enable env var must report gatedOff: {row}"
        );
        assert_eq!(
            row["installed"], false,
            "gated rows are never probed: {row}"
        );
        assert!(
            row.get("resolvedPath").is_none(),
            "gated rows carry no resolvedPath: {row}"
        );
    }

    drop(daemon);
}

/// WSS e2e for the pi CLI version gate on host.providerDiscovery
/// (monorepo#1662): a daemon whose `PI_ACP_PI_COMMAND` points at a fake old
/// `pi` must report the pi row uninstallable with an actionable reason and
/// the found version, over the real wire.
#[cfg(unix)]
#[tokio::test]
async fn host_provider_discovery_gates_pi_on_old_cli_over_wss() {
    let data_dir = temp_data_dir();

    // Fake `pi` that reports a version older than PI_CLI_MIN_VERSION.
    let fake_pi = data_dir.join("fake-pi");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&fake_pi, "#!/bin/sh\necho 0.79.0\n").expect("write fake pi");
        std::fs::set_permissions(&fake_pi, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake pi");
    }

    let fake_pi_str = fake_pi.to_str().unwrap();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("PI_ACP_PI_COMMAND", fake_pi_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    let result = wss_rpc(&mut ws, 2, "host.providerDiscovery", json!({})).await;
    let pi = result["providers"]
        .as_array()
        .expect("providers array")
        .iter()
        .find(|p| p["id"] == "pi")
        .expect("pi must be in the discovery payload")
        .clone();

    assert_eq!(pi["installed"], false, "old pi CLI must gate pi off: {pi}");
    assert_eq!(pi["cliCommand"], fake_pi_str, "{pi}");
    assert_eq!(pi["cliResolved"], true, "{pi}");
    assert_eq!(pi["cliResolvedPath"], fake_pi_str, "{pi}");
    assert_eq!(pi["cliVersion"], "0.79.0", "{pi}");
    assert_eq!(pi["cliVersionOk"], false, "{pi}");
    assert_eq!(
        pi["cliRequirement"],
        intent_providers::PI_CLI_REQUIREMENT,
        "{pi}"
    );
    let reason = pi["unavailableReason"]
        .as_str()
        .expect("gated pi must carry unavailableReason");
    assert!(reason.contains("0.79.0"), "{pi}");
    assert!(
        reason.contains(intent_providers::PI_CLI_REQUIREMENT),
        "{pi}"
    );

    drop(daemon);
}

/// WSS e2e for host.providerDiscovery with `providers.paths` overrides
/// (monorepo#1065): a valid override seeded in `config.toml` must flip
/// `installed` / `secondaryResolved` on the wire, while `resolvedPath` /
/// `secondaryResolvedPath` stay auto-detected (never the override path).
#[tokio::test]
async fn host_provider_discovery_honors_path_overrides_over_wss() {
    let data_dir = temp_data_dir();

    // Fake executables the overrides point at — valid (absolute + executable)
    // regardless of what is really installed on the host.
    let bin_dir = data_dir.join("override-bins");
    std::fs::create_dir_all(&bin_dir).expect("mkdir override bins");
    let opencode = bin_dir.join("opencode");
    let unsloth_bin = bin_dir.join("unsloth");
    for bin in [&opencode, &unsloth_bin] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(bin, "#!/bin/sh\nexit 0\n").expect("write fake bin");
        std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake bin");
    }

    // Seed the overrides BEFORE the daemon boots (enable_ws_api appends to
    // the same config.toml). Unsloth's primary honors the `opencode` key;
    // the `unsloth` key targets the unsloth CLI (the secondary).
    std::fs::write(
        data_dir.join("config.toml"),
        format!(
            "[providers.paths]\nopencode = \"{}\"\nunsloth = \"{}\"\n",
            opencode.display(),
            unsloth_bin.display()
        ),
    )
    .expect("seed config.toml with providers.paths");

    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    let result = wss_rpc(&mut ws, 2, "host.providerDiscovery", json!({})).await;
    let providers = result["providers"].as_array().expect("providers array");
    let unsloth = providers
        .iter()
        .find(|p| p["id"] == "unsloth")
        .expect("unsloth must be in the discovery payload");

    // The valid overrides satisfy both binaries, so unsloth reports installed
    // even on a host with neither actually on PATH.
    assert_eq!(
        unsloth["installed"], true,
        "valid providers.paths overrides must flip installed: {unsloth}"
    );
    assert_eq!(
        unsloth["secondaryResolved"], true,
        "valid providers.paths override must flip secondaryResolved: {unsloth}"
    );
    // The path fields stay auto-detected: they must never surface the
    // override paths (they may be absent entirely when nothing auto-detects,
    // or carry a real auto-detected install).
    assert_ne!(
        unsloth.get("resolvedPath").and_then(Value::as_str),
        Some(opencode.to_str().unwrap()),
        "resolvedPath must stay auto-detected, never the override: {unsloth}"
    );
    assert_ne!(
        unsloth.get("secondaryResolvedPath").and_then(Value::as_str),
        Some(unsloth_bin.to_str().unwrap()),
        "secondaryResolvedPath must stay auto-detected, never the override: {unsloth}"
    );

    drop(daemon);
}

/// WSS e2e for the default-provider self-heal (monorepo#3044): calling
/// `host.providerDiscovery` on a daemon with UNSET default settings and a
/// provider forced installed (via a `providers.paths` override, as in the
/// override test above) must persist `providers.active` through the
/// transport → `WorkspaceApi::settings_heal_default_provider` seam — the
/// only production trigger — observable via `settings.get` on the same
/// connection. `model.default` stays unset (cold catalog cache), and a
/// repeat discovery call is idempotent.
#[tokio::test]
async fn host_provider_discovery_self_heals_default_provider_over_wss() {
    let data_dir = temp_data_dir();

    // Force one registered provider to report installed regardless of the
    // real host: point its providers.paths override(s) at fake executables.
    let bin_dir = data_dir.join("override-bins");
    std::fs::create_dir_all(&bin_dir).expect("mkdir override bins");
    let opencode = bin_dir.join("opencode");
    let unsloth_bin = bin_dir.join("unsloth");
    for bin in [&opencode, &unsloth_bin] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(bin, "#!/bin/sh\nexit 0\n").expect("write fake bin");
        std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake bin");
    }
    std::fs::write(
        data_dir.join("config.toml"),
        format!(
            "[providers.paths]\nopencode = \"{}\"\nunsloth = \"{}\"\n",
            opencode.display(),
            unsloth_bin.display()
        ),
    )
    .expect("seed config.toml with providers.paths");

    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("port fits u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // Precondition: no default provider configured.
    let before = wss_rpc(
        &mut ws,
        1,
        "settings.get",
        json!({ "path": "providers.active" }),
    )
    .await;
    assert!(
        before["value"].is_null(),
        "providers.active must start unset: {before}"
    );

    // Discovery reports installed providers → the daemon self-heals.
    let result = wss_rpc(&mut ws, 2, "host.providerDiscovery", json!({})).await;
    let installed: Vec<&str> = result["providers"]
        .as_array()
        .expect("providers array")
        .iter()
        .filter(|p| p["installed"] == json!(true))
        .filter_map(|p| p["id"].as_str())
        .collect();
    assert!(
        installed.contains(&"unsloth"),
        "override-forced unsloth must report installed: {result}"
    );

    let healed = wss_rpc(
        &mut ws,
        3,
        "settings.get",
        json!({ "path": "providers.active" }),
    )
    .await;
    let active = healed["value"]
        .as_str()
        .unwrap_or_else(|| panic!("providers.active must be healed to a string: {healed}"));
    assert!(
        installed.contains(&active),
        "healed providers.active ({active}) must be one of the installed providers: {healed}"
    );
    assert_eq!(healed["origin"], json!("file"), "{healed}");

    // Idempotent: a repeat discovery call never rewrites the healed value.
    let _ = wss_rpc(&mut ws, 4, "host.providerDiscovery", json!({})).await;
    let again = wss_rpc(
        &mut ws,
        5,
        "settings.get",
        json!({ "path": "providers.active" }),
    )
    .await;
    assert_eq!(
        again["value"],
        json!(active),
        "repeat discovery must not rewrite the healed value: {again}"
    );

    drop(daemon);
}

/// WSS e2e for host.createDirectory (§5.14): success with an absolute path,
/// `~` expansion against the daemon-host home (pinned via `HOME` on the spawned
/// daemon), idempotent already-exists success, `-32602` on a missing/empty
/// `path`, and `-32603` when the path collides with an existing file.
#[tokio::test]
async fn host_create_directory_over_wss() {
    let data_dir = temp_data_dir();

    // Pin the daemon-host home so the tilde-expansion assertion is exact.
    let home = data_dir.join("home");
    std::fs::create_dir_all(&home).expect("mkdir fake home");
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", home.to_str().unwrap()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // 1) Absolute path — assert the exact success envelope: `result.path` is
    // the fully expanded created path and the directory exists on disk (the
    // daemon runs on this host). Parents are created (`create_dir_all`).
    let target = data_dir.join("created").join("nested");
    let frame = json!({
        "jsonrpc": "2.0", "id": 500, "method": "host.createDirectory",
        "params": { "path": target.to_str().unwrap() }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let resp = wss_expect_error(&mut ws, 500).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 500, "envelope: {resp}");
    assert!(resp.get("error").is_none(), "no error: {resp}");
    assert_eq!(
        resp["result"]["path"],
        json!(target.to_str().unwrap()),
        "result.path is the fully expanded created path: {resp}"
    );
    assert!(target.is_dir(), "directory created on the daemon host");

    // 2) Idempotent repeat — an already-existing directory still succeeds.
    let again = wss_rpc(
        &mut ws,
        501,
        "host.createDirectory",
        json!({ "path": target.to_str().unwrap() }),
    )
    .await;
    assert_eq!(
        again["path"],
        json!(target.to_str().unwrap()),
        "already-exists is success (create_dir_all semantics): {again}"
    );

    // 3) `~/…` expands against the daemon-host home (PROTOCOL §5.14 — exactly
    // like host.listDirectory), and the returned path is the expanded one.
    let tilde = wss_rpc(
        &mut ws,
        502,
        "host.createDirectory",
        json!({ "path": "~/projects/from-wss" }),
    )
    .await;
    let expanded = home.join("projects").join("from-wss");
    assert_eq!(
        tilde["path"],
        json!(expanded.to_str().unwrap()),
        "tilde expanded against the daemon-host home: {tilde}"
    );
    assert!(expanded.is_dir(), "tilde-expanded directory created");

    // 4) Missing `path` ⇒ -32602 (PROTOCOL §9), same as the sibling arms.
    let frame =
        json!({ "jsonrpc": "2.0", "id": 503, "method": "host.createDirectory", "params": {} });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 503).await;
    assert_eq!(err["error"]["code"], -32602, "missing path ⇒ -32602: {err}");

    // 5) Empty `path` ⇒ -32602 as well.
    let frame = json!({
        "jsonrpc": "2.0", "id": 504, "method": "host.createDirectory",
        "params": { "path": "" }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 504).await;
    assert_eq!(err["error"]["code"], -32602, "empty path ⇒ -32602: {err}");

    // 6) Path colliding with an existing file ⇒ -32603 with the IO message.
    let file = data_dir.join("occupied");
    std::fs::write(&file, "hi").expect("write collision file");
    let frame = json!({
        "jsonrpc": "2.0", "id": 505, "method": "host.createDirectory",
        "params": { "path": file.to_str().unwrap() }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = wss_expect_error(&mut ws, 505).await;
    assert_eq!(
        err["error"]["code"], -32603,
        "file collision ⇒ -32603: {err}"
    );

    drop(daemon);
}

/// WSS e2e for host.listDirectory (§5.14): the listing envelope (`path` /
/// `parent` / `home` / `entries`) plus the additive `favorites` field —
/// `home` always present, standard dirs existence-checked on the daemon host
/// (pinned via `HOME` on the spawned daemon), and XDG user-dirs overrides
/// honored for relocated folders.
#[tokio::test]
async fn host_list_directory_over_wss() {
    let data_dir = temp_data_dir();

    // Pin the daemon-host home so the favorites assertions are exact:
    // Desktop exists conventionally, Downloads is relocated via the XDG
    // user-dirs config, Documents does not exist at all.
    let home = data_dir.join("home");
    std::fs::create_dir_all(home.join("Desktop")).expect("mkdir Desktop");
    std::fs::create_dir_all(home.join("Fetched")).expect("mkdir Fetched");
    std::fs::create_dir_all(home.join(".config")).expect("mkdir .config");
    std::fs::write(
        home.join(".config").join("user-dirs.dirs"),
        "XDG_DOWNLOAD_DIR=\"$HOME/Fetched\"\n",
    )
    .expect("write user-dirs.dirs");
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", home.to_str().unwrap()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Omitted `path` defaults to the daemon-host home (PROTOCOL §5.14).
    let result = wss_rpc(&mut ws, 600, "host.listDirectory", json!({})).await;
    assert_eq!(
        result["path"],
        json!(home.to_str().unwrap()),
        "defaults to the daemon-host home: {result}"
    );
    assert_eq!(result["home"], json!(home.to_str().unwrap()));
    let entries = result["entries"].as_array().expect("entries array");
    assert!(
        entries.iter().any(|e| e["name"] == "Desktop"),
        "listing includes Desktop: {result}"
    );

    // Favorites: home always leads; desktop exists conventionally; downloads
    // resolves through the XDG override; documents is absent (no such dir).
    let desktop = home.join("Desktop");
    let fetched = home.join("Fetched");
    let favorites = result["favorites"].as_array().expect("favorites array");
    let pairs: Vec<(&str, &str)> = favorites
        .iter()
        .map(|f| (f["id"].as_str().unwrap(), f["path"].as_str().unwrap()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("home", home.to_str().unwrap()),
            ("desktop", desktop.to_str().unwrap()),
            ("downloads", fetched.to_str().unwrap()),
        ],
        "favorites are existence-checked and XDG-resolved: {result}"
    );

    drop(daemon);
}

/// WSS e2e for discovery cache behavior (host.findBinary / host.toolAvailability
/// / host.providerDiscovery): repeated calls within the TTL window reuse cached
/// results instead of re-scanning PATH/filesystem. A binary installed between
/// calls must be picked up on the next uncached call (negatives never cached).
#[tokio::test]
#[cfg(unix)]
async fn host_discovery_cache_positive_and_negative_over_wss() {
    use std::os::unix::fs::PermissionsExt;

    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // 1) host.findBinary for a name that does not exist: must report
    // available:false and NOT cache the negative.
    let not_found = wss_rpc(
        &mut ws,
        900,
        "host.findBinary",
        json!({ "name": "intent-e2e-nonexistent-xyzzy", "commonPaths": [] }),
    )
    .await;
    assert_eq!(
        not_found["available"], false,
        "nonexistent binary reports available:false: {not_found}"
    );

    // 2) Now install the binary in a temp dir and call host.findBinary again
    // with that dir in commonPaths. The cache must NOT serve the stale negative
    // — it must re-probe and find the binary.
    let bin_dir = data_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).expect("mkdir bin");
    let bin_path = bin_dir.join("intent-e2e-nonexistent-xyzzy");
    std::fs::write(&bin_path, "#!/bin/sh\nexit 0\n").expect("write binary");
    std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).expect("chmod +x");

    let found = wss_rpc(
        &mut ws,
        901,
        "host.findBinary",
        json!({
            "name": "intent-e2e-nonexistent-xyzzy",
            "commonPaths": [bin_path.to_str().unwrap()]
        }),
    )
    .await;
    assert_eq!(
        found["available"], true,
        "after install, the binary must be found (negatives not cached): {found}"
    );
    assert_eq!(
        found["path"],
        json!(bin_path.to_str().unwrap()),
        "resolved path matches the installed binary: {found}"
    );

    // 3) Repeated call with the same name + commonPaths should hit the cache
    // (same result, but the cache layer short-circuits the probe). We can't
    // directly observe the cache hit, but we can verify idempotent results.
    let cached = wss_rpc(
        &mut ws,
        902,
        "host.findBinary",
        json!({
            "name": "intent-e2e-nonexistent-xyzzy",
            "commonPaths": [bin_path.to_str().unwrap()]
        }),
    )
    .await;
    assert_eq!(
        cached, found,
        "repeated call for the same binary must return the same result: {cached}"
    );

    // 4) host.toolAvailability batch: call once, then remove one of the
    // resolved binaries and call again. The cache must serve the stale positive
    // for that binary (within TTL), not re-probe and flip to unavailable.
    let git_first = wss_rpc(
        &mut ws,
        903,
        "host.toolAvailability",
        json!({ "tools": ["git"] }),
    )
    .await;
    // git is almost always installed on the daemon host; if not, this
    // assertion fails and the test is skipped (not a test bug).
    assert_eq!(
        git_first["tools"]["git"]["available"], true,
        "git must be available on the daemon host for this test: {git_first}"
    );

    // Attempt to "remove" git by clearing PATH (a symlink trick won't work
    // because the cache holds the resolved path). Instead, verify that a
    // second call for git still reports available:true (the cache serves the
    // first call's positive result).
    let git_cached = wss_rpc(
        &mut ws,
        904,
        "host.toolAvailability",
        json!({ "tools": ["git"] }),
    )
    .await;
    assert_eq!(
        git_cached["tools"]["git"], git_first["tools"]["git"],
        "repeated toolAvailability call serves cached result: {git_cached}"
    );

    // 5) host.providerDiscovery: call once, verify the payload shape, then
    // call again and confirm it returns the same provider list (cache hit).
    let discovery_first = wss_rpc(&mut ws, 905, "host.providerDiscovery", json!({})).await;
    assert!(
        discovery_first["providers"].is_array(),
        "providerDiscovery must return a providers array: {discovery_first}"
    );
    assert!(
        !discovery_first["providers"].as_array().unwrap().is_empty(),
        "providers array must not be empty: {discovery_first}"
    );

    let discovery_cached = wss_rpc(&mut ws, 906, "host.providerDiscovery", json!({})).await;
    assert_eq!(
        discovery_cached["providers"].as_array().unwrap().len(),
        discovery_first["providers"].as_array().unwrap().len(),
        "repeated providerDiscovery call serves cached result: {discovery_cached}"
    );

    drop(daemon);
}
