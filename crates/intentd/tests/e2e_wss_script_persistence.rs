//! WSS end-to-end `script.*` persistence: drives the real pinned-TLS WebSocket
//! against a live `intentd serve` (WSS listener enabled via config), creates a script, restarts the
//! daemon on the same data dir, and asserts the definition survives (hydrated
//! with a fresh idle runtime state) — then that `script.remove` unpersists it
//! across another restart. Regression for the registry living only in memory.

#![cfg(unix)]

mod common;

use std::path::Path;
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

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

fn scratch_dir(prefix: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-scr-{prefix}-"))
}

fn spawn_serve(data_dir: &Path) -> Child {
    let log = std::fs::File::options()
        .create(true)
        .append(true)
        .open(data_dir.join("daemon.log"))
        .expect("open daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    common::serve_command()
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let response = wss_rpc_envelope(ws, id, method, params).await;
    assert!(
        response.get("error").is_none(),
        "rpc {method} errored: {response}"
    );
    response["result"].clone()
}

async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text).expect("json frame");
                if value["id"] == json!(id) {
                    return value;
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                let _ = ws.send(Message::Pong(payload)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn next_script_change<S>(
    ws: &mut WebSocketStream<S>,
    subscription_id: &str,
    workspace_id: &str,
    script_id: &str,
    action: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("script change timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("json frame");
                if frame["method"] != "events.event" {
                    continue;
                }
                assert_eq!(frame["jsonrpc"], "2.0", "event envelope: {frame}");
                assert!(frame.get("id").is_none(), "notification has no id: {frame}");
                assert_eq!(
                    frame["params"]["subscriptionId"], subscription_id,
                    "subscription envelope: {frame}"
                );
                let event = &frame["params"]["event"];
                assert_eq!(event["type"], "script:changed", "event type: {frame}");
                assert_eq!(event["workspaceId"], workspace_id, "workspace: {frame}");
                assert_eq!(event["data"]["scriptId"], script_id, "script: {frame}");
                assert_eq!(event["data"]["action"], action, "action: {frame}");
                assert!(event["id"].is_string(), "event id: {frame}");
                assert!(event["timestamp"].is_string(), "timestamp: {frame}");
                assert_eq!(event["actor"]["type"], "system", "actor: {frame}");
                return;
            }
            Some(Ok(Message::Ping(payload))) => {
                let _ = ws.send(Message::Pong(payload)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Boot (or re-boot) a daemon over an existing data dir, returning the child
/// and a pinned-TLS WSS client config for its live port.
async fn boot(data_dir: &Path) -> (Child, u16, Arc<ClientConfig>) {
    let child = spawn_serve(data_dir);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (child, port, client_config(&fingerprint))
}

fn stop(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// A minimal committed git repo for `workspace.create` (the store row is what
/// `script.*` needs; `skipWorktree` keeps provisioning out of the test).
fn create_test_repo() -> tempfile::TempDir {
    let repo = common::test_tempdir("scr-persist-repo-");
    let repo_path = repo.path().to_path_buf();
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(&repo_path)
            .stdout(Stdio::null())
            .status()
            .expect("git spawn");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "--initial-branch=main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write readme");
    git(&["add", "."]);
    git(&["commit", "-m", "initial commit"]);
    repo
}

/// Poll `script.status` until `pred` holds (pure-liveness deadline).
async fn await_status<S, F>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    script_id: &str,
    mut pred: F,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(&Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut id = id_base;
    loop {
        let st = wss_rpc(
            ws,
            id,
            "script.status",
            json!({ "workspaceId": ws_id, "scriptId": script_id }),
        )
        .await;
        if pred(&st) {
            return st;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out awaiting script status, last: {st}"
        );
        id += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Real authenticated WSS contract for script definition changes. Successful
/// create, update, and remove mutations emit ordered workspace-scoped events;
/// causal barrier mutations prove failure silence and workspace isolation.
#[tokio::test]
async fn script_definition_changes_emit_over_authenticated_wss() {
    let data_dir_guard = scratch_dir("change-events");
    let data_dir = data_dir_guard.path().to_path_buf();
    let (child, port, cfg) = boot(&data_dir).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub_a = connect_ws(port, cfg.clone()).await;
    let mut sub_b = connect_ws(port, cfg).await;
    let workspace_a = "ws-script-events-a";
    let workspace_b = "ws-script-events-b";

    let subscribed_a = wss_rpc(
        &mut sub_a,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["script:changed"], "workspaceId": workspace_a }),
    )
    .await;
    let subscription_a = subscribed_a["subscriptionId"]
        .as_str()
        .expect("subscription A id")
        .to_string();
    let subscribed_b = wss_rpc(
        &mut sub_b,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["script:changed"], "workspaceId": workspace_b }),
    )
    .await;
    let subscription_b = subscribed_b["subscriptionId"]
        .as_str()
        .expect("subscription B id")
        .to_string();

    let created = wss_rpc(
        &mut rpc,
        10,
        "script.create",
        json!({
            "workspaceId": workspace_a,
            "name": "dev",
            "command": "pnpm dev",
            "mode": "service",
            "scriptId": "script-a",
        }),
    )
    .await;
    assert_eq!(created["id"], "script-a");
    next_script_change(
        &mut sub_a,
        &subscription_a,
        workspace_a,
        "script-a",
        "created",
    )
    .await;

    let updated = wss_rpc(
        &mut rpc,
        11,
        "script.create",
        json!({
            "workspaceId": workspace_a,
            "name": "dev",
            "command": "pnpm dev --host",
            "mode": "service",
            "scriptId": "script-a",
        }),
    )
    .await;
    assert_eq!(updated["command"], "pnpm dev --host");
    next_script_change(
        &mut sub_a,
        &subscription_a,
        workspace_a,
        "script-a",
        "updated",
    )
    .await;

    let removed = wss_rpc(
        &mut rpc,
        12,
        "script.remove",
        json!({ "workspaceId": workspace_a, "scriptId": "script-a" }),
    )
    .await;
    assert_eq!(removed["ok"], json!(true));
    next_script_change(
        &mut sub_a,
        &subscription_a,
        workspace_a,
        "script-a",
        "removed",
    )
    .await;

    let failed = wss_rpc_envelope(
        &mut rpc,
        13,
        "script.remove",
        json!({ "workspaceId": workspace_a, "scriptId": "missing" }),
    )
    .await;
    assert_eq!(failed["jsonrpc"], "2.0");
    assert_eq!(failed["id"], 13);
    assert!(
        failed["error"].is_object(),
        "missing remove errors: {failed}"
    );

    // A successful same-workspace mutation is a causal stream barrier: if
    // the failed remove emitted anything, it must arrive before this event.
    wss_rpc(
        &mut rpc,
        14,
        "script.create",
        json!({
            "workspaceId": workspace_a,
            "name": "barrier A",
            "command": "true",
            "mode": "command",
            "scriptId": "barrier-a",
        }),
    )
    .await;
    next_script_change(
        &mut sub_a,
        &subscription_a,
        workspace_a,
        "barrier-a",
        "created",
    )
    .await;

    // A workspace-B event is a causal barrier after every workspace-A
    // mutation. Subscriber B must observe only its own filtered event.
    wss_rpc(
        &mut rpc,
        15,
        "script.create",
        json!({
            "workspaceId": workspace_b,
            "name": "barrier B",
            "command": "true",
            "mode": "command",
            "scriptId": "barrier-b",
        }),
    )
    .await;
    next_script_change(
        &mut sub_b,
        &subscription_b,
        workspace_b,
        "barrier-b",
        "created",
    )
    .await;

    drop(rpc);
    drop(sub_a);
    drop(sub_b);
    stop(child);
}

/// `script.create` persists the definition; a daemon restart on the same data
/// dir hydrates it back into `script.list` (fresh idle runtime), and
/// `script.remove` unpersists it across yet another restart.
#[tokio::test]
async fn scripts_survive_daemon_restart_over_wss() {
    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();

    // Boot #1: create a script over WSS.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "script.create",
        json!({
            "workspaceId": "ws-scripts",
            "name": "dev server",
            "command": "npm run dev",
            "mode": "service",
            "cwd": "web",
            "env": { "PORT": "3000" },
            "category": "dev",
            "autoStart": true,
            "scriptId": "persist-1",
        }),
    )
    .await;
    assert_eq!(created["id"], "persist-1");
    drop(ws);
    stop(child);

    // Boot #2: the definition survives with a fresh idle runtime state.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let listed = wss_rpc(
        &mut ws,
        3,
        "script.list",
        json!({ "workspaceId": "ws-scripts" }),
    )
    .await;
    let scripts = listed["scripts"].as_array().expect("scripts array");
    assert_eq!(scripts.len(), 1, "persisted script hydrated: {listed}");
    let entry = &scripts[0];
    assert_eq!(entry["id"], "persist-1");
    assert_eq!(entry["name"], "dev server");
    assert_eq!(entry["command"], "npm run dev");
    assert_eq!(entry["cwd"], "web");
    assert_eq!(entry["env"]["PORT"], "3000");
    assert_eq!(entry["mode"], "service");
    assert_eq!(entry["category"], "dev");
    assert_eq!(entry["autoStart"], true);
    assert_eq!(entry["source"], "user");
    assert_eq!(entry["runtime"]["status"], "idle");
    assert!(
        entry["runtime"].get("previouslyRunning").is_none(),
        "never-started script hydrates without previouslyRunning: {entry}"
    );

    // Remove it, restart again: it stays gone.
    let removed = wss_rpc(
        &mut ws,
        4,
        "script.remove",
        json!({ "workspaceId": "ws-scripts", "scriptId": "persist-1" }),
    )
    .await;
    assert_eq!(removed["ok"], json!(true));
    drop(ws);
    stop(child);

    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let listed = wss_rpc(
        &mut ws,
        5,
        "script.list",
        json!({ "workspaceId": "ws-scripts" }),
    )
    .await;
    assert_eq!(
        listed["scripts"].as_array().expect("scripts array").len(),
        0,
        "removed script stays unpersisted: {listed}"
    );
    drop(ws);
    stop(child);
}

/// A service running when the daemon is killed hydrates on the next boot as
/// `idle` with `previouslyRunning: true` (the stored-on-write `was_running`
/// marker, PROTOCOL §5.8) on both `script.list` and `script.status`; the
/// marker survives a second restart, and `script.stop` on the non-running
/// hydrated script (the FE dismiss affordance) returns ok and durably clears
/// it across yet another restart.
#[tokio::test]
async fn was_running_marker_survives_daemon_kill_over_wss() {
    let data_dir_guard = scratch_dir("marker");
    let data_dir = data_dir_guard.path().to_path_buf();
    let repo_guard = create_test_repo();
    let repo_path = repo_guard.path().to_path_buf();

    // Boot #1: create a workspace + service script, start it, and kill the
    // daemon while the service is running (no stop).
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        1,
        "workspace.create",
        json!({
            "title": "script-marker",
            "repositoryPath": repo_path.to_string_lossy(),
            "skipWorktree": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let script = wss_rpc(
        &mut ws,
        2,
        "script.create",
        json!({
            "workspaceId": ws_id,
            "name": "svc",
            "command": "sleep 3600",
            "mode": "service",
            "scriptId": "marker-1",
        }),
    )
    .await;
    assert_eq!(script["id"], "marker-1");
    let started = wss_rpc(
        &mut ws,
        3,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(started["ok"], json!(true));
    await_status(&mut ws, 10, &ws_id, "marker-1", |st| {
        st["status"] == json!("running")
    })
    .await;
    drop(ws);
    stop(child); // SIGKILL — the running service never sees a stop.

    // Boot #2: hydrated idle with previouslyRunning on status and list.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let st = wss_rpc(
        &mut ws,
        4,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(st["status"], "idle", "hydrates idle: {st}");
    assert_eq!(st["previouslyRunning"], true, "marker surfaced: {st}");
    let listed = wss_rpc(&mut ws, 5, "script.list", json!({ "workspaceId": ws_id })).await;
    let entry = &listed["scripts"].as_array().expect("scripts array")[0];
    assert_eq!(entry["runtime"]["previouslyRunning"], true, "on list too");
    drop(ws);
    stop(child); // Kill again without touching the script.

    // Boot #3: the marker persists across repeated restarts; dismissing via
    // script.stop on the non-running script returns ok and clears it.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let st = wss_rpc(
        &mut ws,
        6,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(st["previouslyRunning"], true, "marker persists: {st}");
    let stopped = wss_rpc(
        &mut ws,
        7,
        "script.stop",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(stopped["ok"], json!(true), "dismiss is ok, not an error");
    let st = wss_rpc(
        &mut ws,
        8,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(st["status"], "idle");
    assert!(
        st.get("previouslyRunning").is_none(),
        "dismiss drops the marker: {st}"
    );
    drop(ws);
    stop(child);

    // Boot #4: the clear was durable.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let st = wss_rpc(
        &mut ws,
        9,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "marker-1" }),
    )
    .await;
    assert_eq!(st["status"], "idle");
    assert!(
        st.get("previouslyRunning").is_none(),
        "cleared marker stays cleared across restart: {st}"
    );
    drop(ws);
    stop(child);
}

/// The `exitCode` sentinel and `error` written for every unobservable
/// terminal state (docs/protocol/methods/scripts.md §5.8, total exit contract).
const EXIT_CODE_UNOBSERVABLE: i64 = -1;
const LOST_AT_DAEMON_STOP_ERROR: &str = "lost: the daemon stopped while the script was running";

/// Serialized `ScriptRuntimeState` omits cleared fields (`skip_serializing_if`);
/// a `null` anywhere in a runtime payload is a contract break.
fn assert_no_nulls(value: &Value, ctx: &str) {
    match value {
        Value::Null => panic!("{ctx}: null field (cleared fields must be omitted)"),
        Value::Object(map) => map.values().for_each(|v| assert_no_nulls(v, ctx)),
        Value::Array(items) => items.iter().for_each(|v| assert_no_nulls(v, ctx)),
        _ => {}
    }
}

/// The hydrated reading of a command script that was running when the daemon
/// stopped: `exited` / `-1` / the lost error, with no run fields and no
/// service-only `previouslyRunning`.
fn assert_lost_command_state(st: &Value, ctx: &str) {
    assert_eq!(st["status"], "exited", "{ctx}: {st}");
    assert_eq!(st["exitCode"], EXIT_CODE_UNOBSERVABLE, "{ctx}: {st}");
    assert_eq!(st["error"], LOST_AT_DAEMON_STOP_ERROR, "{ctx}: {st}");
    assert!(st["restartCount"].is_u64(), "{ctx}: restartCount: {st}");
    assert!(
        st.get("previouslyRunning").is_none(),
        "{ctx}: previouslyRunning is service-only: {st}"
    );
    for absent in ["pid", "startedAt", "stoppedAt", "detectedUrl"] {
        assert!(st.get(absent).is_none(), "{ctx}: {absent} present: {st}");
    }
    assert_no_nulls(st, ctx);
}

/// Read `script:state` events off a subscribed connection until one for
/// `script_id` satisfies `pred`; asserts the §6.5 notification envelope on
/// every frame and returns the matching event `data`.
async fn next_script_state<S, F>(
    ws: &mut WebSocketStream<S>,
    subscription_id: &str,
    workspace_id: &str,
    script_id: &str,
    mut pred: F,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(&Value) -> bool,
{
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("script:state timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("json frame");
                if frame["method"] != "events.event" {
                    continue;
                }
                assert_eq!(frame["jsonrpc"], "2.0", "event envelope: {frame}");
                assert!(frame.get("id").is_none(), "notification has no id: {frame}");
                assert_eq!(
                    frame["params"]["subscriptionId"], subscription_id,
                    "subscription envelope: {frame}"
                );
                let event = &frame["params"]["event"];
                assert_eq!(event["type"], "script:state", "event type: {frame}");
                assert_eq!(event["workspaceId"], workspace_id, "workspace: {frame}");
                assert_eq!(event["data"]["scriptId"], script_id, "script: {frame}");
                assert!(event["id"].is_string(), "event id: {frame}");
                assert!(event["timestamp"].is_string(), "timestamp: {frame}");
                if pred(&event["data"]) {
                    return event["data"].clone();
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                let _ = ws.send(Message::Pong(payload)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Boot a daemon and create a workspace plus a command-mode `sleep` script
/// under it, returning the live client and the workspace id.
async fn boot_with_command_script(
    data_dir: &Path,
    repo_path: &Path,
    title: &str,
    script_id: &str,
) -> (
    Child,
    u16,
    Arc<ClientConfig>,
    WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    String,
) {
    let (child, port, cfg) = boot(data_dir).await;
    let mut ws = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut ws,
        1,
        "workspace.create",
        json!({
            "title": title,
            "repositoryPath": repo_path.to_string_lossy(),
            "skipWorktree": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let script = wss_rpc(
        &mut ws,
        2,
        "script.create",
        json!({
            "workspaceId": ws_id,
            "name": "cmd",
            // timing-guard: a long-lived child the test ends out of band (SIGKILL), never awaited
            "command": "sleep 600",
            "mode": "command",
            "scriptId": script_id,
        }),
    )
    .await;
    assert_eq!(script["id"], script_id);
    (child, port, cfg, ws, ws_id)
}

/// A command script whose process is `SIGKILL`ed out of band (no `script.stop`)
/// reaches `exited` with an `exitCode` on `script.status` within the liveness
/// bound, and the terminal transition is published as a `script:state` event
/// — the run never sits `running` with a dead pid.
#[tokio::test]
async fn out_of_band_kill_reaches_exited_over_wss() {
    let data_dir_guard = scratch_dir("oob-kill");
    let data_dir = data_dir_guard.path().to_path_buf();
    let repo_guard = create_test_repo();
    let (child, port, cfg, mut ws, ws_id) =
        boot_with_command_script(&data_dir, repo_guard.path(), "script-oob-kill", "oob-1").await;
    let mut sub = connect_ws(port, cfg).await;
    let subscribed = wss_rpc(
        &mut sub,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["script:state"], "workspaceId": ws_id }),
    )
    .await;
    let subscription_id = subscribed["subscriptionId"]
        .as_str()
        .expect("subscription id")
        .to_string();

    let started = wss_rpc(
        &mut ws,
        4,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "oob-1" }),
    )
    .await;
    assert_eq!(started["ok"], json!(true));
    let running = await_status(&mut ws, 10, &ws_id, "oob-1", |st| {
        st["status"] == json!("running")
    })
    .await;
    assert_no_nulls(&running, "running status");
    assert!(
        running.get("exitCode").is_none() && running.get("error").is_none(),
        "running carries no terminal fields: {running}"
    );
    let pid = running["pid"].as_u64().expect("running state carries pid");
    let pid = i32::try_from(pid).expect("pid fits in pid_t");

    // Out of band: the supervisor never issues a stop and never sees a
    // `script.stop`; only its exit poll can notice the child is gone.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("SIGKILL the script's process");

    let exited = await_status(&mut ws, 100, &ws_id, "oob-1", |st| {
        st["status"] == json!("exited")
    })
    .await;
    let exit_code = exited["exitCode"]
        .as_i64()
        .unwrap_or_else(|| panic!("exited always carries exitCode: {exited}"));
    assert_ne!(exit_code, 0, "a SIGKILLed child is not a success: {exited}");
    assert!(exited["stoppedAt"].is_string(), "stoppedAt: {exited}");
    assert!(
        exited.get("previouslyRunning").is_none(),
        "previouslyRunning is a hydration marker only: {exited}"
    );
    assert_no_nulls(&exited, "exited status");
    let listed = wss_rpc(&mut ws, 200, "script.list", json!({ "workspaceId": ws_id })).await;
    let entry = &listed["scripts"].as_array().expect("scripts array")[0];
    assert_eq!(entry["id"], "oob-1");
    assert_eq!(entry["runtime"]["status"], "exited", "on list too: {entry}");
    assert_eq!(
        entry["runtime"]["exitCode"], exit_code,
        "on list too: {entry}"
    );
    assert_no_nulls(entry, "listed entry");

    // The terminal transition is published as script:state with the same
    // exitCode the status read reported.
    let event = next_script_state(&mut sub, &subscription_id, &ws_id, "oob-1", |data| {
        data["status"] == json!("exited")
    })
    .await;
    assert_eq!(event["exitCode"], exit_code, "event exitCode: {event}");
    assert!(event["stoppedAt"].is_string(), "event stoppedAt: {event}");
    assert!(
        event["restartCount"].is_u64(),
        "event restartCount: {event}"
    );
    assert_no_nulls(&event, "script:state event");

    drop(sub);
    drop(ws);
    stop(child);
}

/// A command script running when the daemon is `SIGKILL`ed hydrates on the next
/// boot as the terminal `exited` / `exitCode: -1` / lost `error` reading —
/// never `previouslyRunning` (service-only) — on both `script.status` and the
/// `script.list` runtime; the reading is stable across another restart, and
/// `script.start` clears the terminal fields as the new run comes up.
#[tokio::test]
async fn lost_command_script_hydrates_exited_over_wss() {
    let data_dir_guard = scratch_dir("lost-cmd");
    let data_dir = data_dir_guard.path().to_path_buf();
    let repo_guard = create_test_repo();

    // Boot #1: start the command script and kill the daemon mid-run.
    let (child, _port, _cfg, mut ws, ws_id) =
        boot_with_command_script(&data_dir, repo_guard.path(), "script-lost-cmd", "lost-1").await;
    let started = wss_rpc(
        &mut ws,
        3,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "lost-1" }),
    )
    .await;
    assert_eq!(started["ok"], json!(true));
    await_status(&mut ws, 10, &ws_id, "lost-1", |st| {
        st["status"] == json!("running")
    })
    .await;
    drop(ws);
    stop(child); // SIGKILL — the running command never sees a stop.

    // Boot #2: hydrated as the lost terminal reading on status and list.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let st = wss_rpc(
        &mut ws,
        4,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "lost-1" }),
    )
    .await;
    assert_lost_command_state(&st, "boot #2 status");
    let listed = wss_rpc(&mut ws, 5, "script.list", json!({ "workspaceId": ws_id })).await;
    let scripts = listed["scripts"].as_array().expect("scripts array");
    assert_eq!(scripts.len(), 1, "one hydrated script: {listed}");
    assert_eq!(scripts[0]["id"], "lost-1");
    assert_eq!(scripts[0]["mode"], "command");
    assert_lost_command_state(&scripts[0]["runtime"], "boot #2 list runtime");
    drop(ws);
    stop(child); // Kill again without touching the script.

    // Boot #3: the lost reading is stable across repeated restarts; a fresh
    // start clears the terminal fields as the new run comes up.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let st = wss_rpc(
        &mut ws,
        6,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "lost-1" }),
    )
    .await;
    assert_lost_command_state(&st, "boot #3 status");
    let listed = wss_rpc(&mut ws, 7, "script.list", json!({ "workspaceId": ws_id })).await;
    let entry = &listed["scripts"].as_array().expect("scripts array")[0];
    assert_lost_command_state(&entry["runtime"], "boot #3 list runtime");

    let restarted = wss_rpc(
        &mut ws,
        8,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "lost-1" }),
    )
    .await;
    assert_eq!(restarted["ok"], json!(true));
    let st = wss_rpc(
        &mut ws,
        9,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": "lost-1" }),
    )
    .await;
    assert!(
        st["status"] == json!("starting") || st["status"] == json!("running"),
        "start replaces the lost reading synchronously: {st}"
    );
    for cleared in ["exitCode", "error", "stoppedAt", "previouslyRunning"] {
        assert!(
            st.get(cleared).is_none(),
            "{cleared} cleared by start: {st}"
        );
    }
    assert_no_nulls(&st, "post-start status");
    let running = await_status(&mut ws, 20, &ws_id, "lost-1", |st| {
        st["status"] == json!("running")
    })
    .await;
    assert!(running["pid"].is_u64(), "new run carries pid: {running}");
    assert!(
        running.get("exitCode").is_none() && running.get("error").is_none(),
        "new run carries no terminal fields: {running}"
    );
    assert_no_nulls(&running, "running status after restart");
    drop(ws);
    stop(child);
}
