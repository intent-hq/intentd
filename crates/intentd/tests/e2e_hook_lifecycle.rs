//! WSS end-to-end background-hook lifecycle: agents schedule hooks via the
//! `ws.hook.*` MCP bindings (schedule is MCP-only, §6.8), the FE manages them
//! over the wire (`hook.list` / `hook.cancel` / `hook.runNow`), and every
//! lifecycle transition surfaces as a `hook:*` event through
//! `events.subscribe` filters.
//!
//! One daemon boot drives the full story against the mock ACP provider:
//!  1. Agent turn 1 schedules a hook whose immediate validation run
//!     dispatches — the owner is mid-turn (a follow-up `ws.host.exec` sleep
//!     keeps the turn in flight), so the wake QUEUES and is visible via
//!     `agent.getQueue` (`messageMetadata.type == "hook_wake"`), with
//!     `hook:run-completed` + `hook:dispatched` on the wire.
//!  2. Agent turn 2 schedules a watcher hook that reads a seeded note —
//!     `hook:scheduled` observed; `hook.list` reports both hooks;
//!     `hook.runNow` drives `hook:run-started` + `hook:run-completed`.
//!  3. The test plants an EVICT marker in the note over the wire and calls
//!     `hook.runNow` again — the run throws, `hook:evicted` carries
//!     `lastError`, and the owner is woken with the eviction notice
//!     (asserted via `agent.getConversation`).
//!  4. Agent turn 3 schedules a third hook (with a 50-char human-readable
//!     name, the maximum); a SECOND agent finds it via `ws.hook.list` and
//!     tries to cancel it through the MCP route — rejected, hook untouched
//!     (intent-hq/monorepo#1563) — then the FE cancels it: the response
//!     carries the cancelled hook with the name intact, `hook:cancelled`
//!     fires, and the owner is woken with the cancellation notice.
//!  5. Error arms: unknown `hookId` → -32602 on cancel/runNow; `runNow` on a
//!     cancelled hook → -32602; missing params → -32602.
//!  6. State carry-over: agent turn 4 schedules a counter hook that threads
//!     `{ n }` through `hookState` (`{ dispatch: false, state: { n } }` until
//!     `n` reaches 2, then `{ dispatch: true, message }`) — `lastState`
//!     advances in `hook.list` after each run and the hook dispatches on the
//!     run where the carried count reaches the threshold.
//!  7. TTL expiry (v3.1): agent turn 5 schedules three hooks probing the
//!     clamp on the wire — omitted `ttlMs` (the 24-hour default), an in-range
//!     `ttlMs: 7_200_000` (2 h, persists as-is), and `ttlMs: 1` (clamped to
//!     the 10s floor). `hook.list` surfaces each persisted `expiresAt` (the
//!     `createdAt`→`expiresAt` delta is asserted exactly); the short hook's
//!     deadline passes, `hook:expired` fires, the row goes terminal
//!     (`runNow` → -32602), and the owner is woken with the expiry notice.
//!  8. Perpetual hooks: agent turn 6 schedules a `perpetual: true` hook whose
//!     every run dispatches — the validation-run dispatch wakes the owner AND
//!     persists an ACTIVE schedule (`dispatchCount: 1`), a `hook.runNow` fire
//!     wakes again and stays `scheduled` (`dispatchCount: 2`), each wake
//!     says the hook remains active (`hookStillActive: true` in its
//!     metadata), and the TTL finally expires the hook with a notice
//!     reporting runs AND dispatches.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

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
const TOKEN: &str = "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-hook-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
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

/// Send one JSON-RPC frame and return the full id-matched response envelope
/// (success or error). Answers server heartbeats with `Pong`.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("rpc {method} timed out"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    return v;
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

/// [`wss_rpc_raw`] that asserts success and returns `result`.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Read `events.event` frames until one matches `event_type` (and `name`, when
/// given, against `event.data.name`); returns that event object.
async fn next_hook_event<S>(
    ws: &mut WebSocketStream<S>,
    event_type: &str,
    name: Option<&str>,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {event_type} (name={name:?})"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "events.event" {
                    continue;
                }
                let event = v["params"]["event"].clone();
                if event["type"].as_str() != Some(event_type) {
                    continue;
                }
                if let Some(n) = name {
                    if event["data"]["name"].as_str() != Some(n) {
                        continue;
                    }
                }
                return event;
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Drain frames until the next `agent:idle` for `agent_id`, returning its
/// `data` payload (idle-visibility assertions, §6.5 `waitingOnHooks`).
async fn next_agent_idle<S>(ws: &mut WebSocketStream<S>, agent_id: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for agent:idle ({agent_id})"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "events.event" {
                    continue;
                }
                let event = v["params"]["event"].clone();
                if event["type"].as_str() == Some("agent:idle")
                    && event["data"]["agentId"].as_str() == Some(agent_id)
                {
                    return event["data"].clone();
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

/// Mock-agent gate (parity with the WSS agent-lifecycle suite).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// Pre-seed the daemon's DB with a workspace + watched note. The store is
/// closed before the daemon boots so it gets a clean handle.
async fn seed_workspace_and_note(data_dir: &Path) -> (String, String) {
    use intent_core::{
        now_iso, NoteCreate, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
        WorkspaceId, WorkspaceStatus,
    };
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WSS-HOOKS".to_string(),
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
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
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
    let note = services
        .create_note(
            ws_id.clone(),
            NoteCreate {
                title: "Watched".into(),
                content: Some("# Watched\nall clear\n".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    (ws_id.0, note.id.0)
}

/// Poll `agent.getConversation` until some message contains `needle` (async
/// wake delivery), bounded by the shared RPC budget.
async fn await_conversation_contains<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    let mut i = 0i64;
    loop {
        i += 1;
        let conv = wss_rpc(
            ws,
            id_base + i,
            "agent.getConversation",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        let text = conv["messages"].to_string();
        if text.contains(needle) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "conversation never contained {needle:?}; last page: {text}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// Full background-hook lifecycle over the real WSS wire (see module docs).
#[tokio::test]
async fn hook_lifecycle_over_wss() {
    let Some(script) = gate("WSS hook lifecycle E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;

    // Agent-JS payloads, one per prompt marker. The inner hook scripts are
    // JSON-escaped into the outer `ws.hook.schedule` calls.
    let schedule_dispatch_js = format!(
        "return await ws.hook.schedule({{ name: 'dispatcher', code: {}, delayMs: 60000 }});",
        json!("return { dispatch: true, message: 'CI is red' };")
    );
    let watcher_inner = format!(
        "console.log('watcher checked the note'); \
         const n = await ws.note.read('{note_id}'); \
         if ((n.content || '').includes('EVICT')) {{ throw new Error('EVICT marker found'); }} \
         return {{ dispatch: false }};"
    );
    let schedule_watch_js = format!(
        "return await ws.hook.schedule({{ name: 'watcher', code: {}, delayMs: 60000 }});",
        json!(watcher_inner)
    );
    // Maximum-length (50-char) human-readable hook name, scheduled through
    // the production MCP `ws.hook.schedule` route and asserted to round-trip
    // through the wire events and the `hook.cancel` response below.
    let cancel_hook_name = "watch cancelled hook with a fifty character name!!";
    assert_eq!(cancel_hook_name.chars().count(), 50, "name is 50 chars");
    let schedule_cancel_js = format!(
        "return await ws.hook.schedule({{ name: '{cancel_hook_name}', code: {}, delayMs: 60000 }});",
        json!("return { dispatch: false };")
    );
    // Intruder turn (intent-hq/monorepo#1563): a second agent finds the
    // hook through `ws.hook.list` (workspace-wide) and tries to cancel it.
    // The MCP route must reject the cross-agent cancel with an error naming
    // the owning agent.
    let cancel_others_js = format!(
        "const hooks = await ws.hook.list(); \
         const target = hooks.find(h => h.name === '{cancel_hook_name}'); \
         const out = ['found=' + !!target]; \
         try {{ await ws.hook.cancel(target.hookId); out.push('cancel=allowed'); }} \
         catch (e) {{ out.push('cancel=rejected'); \
                     out.push('ownerNamed=' + e.message.includes(target.agentId)); }} \
         return out.join(' ');"
    );
    let counter_inner = "const n = (hookState === null) ? 0 : hookState.n; \
                         if (n >= 2) { return { dispatch: true, message: 'counted ' + n }; } \
                         return { dispatch: false, state: { n: n + 1 } };";
    let schedule_counter_js = format!(
        "return await ws.hook.schedule({{ name: 'counter', code: {}, delayMs: 60000 }});",
        json!(counter_inner)
    );
    // TTL section: three schedules in one turn probe the clamp over the
    // production wire path. Omitted ttlMs takes the 24-hour default; an
    // in-range 2h ttlMs persists as-is; `ttlMs: 1` clamps to the 10s floor,
    // so that hook expires ~10s after creation — well inside the event-read
    // budget — while the 60s delayMs guarantees no second run ever starts.
    let schedule_ttl_js = format!(
        "await ws.hook.schedule({{ name: 'defaultttl', code: {code}, delayMs: 60000 }}); \
         await ws.hook.schedule({{ name: 'midttl', code: {code}, delayMs: 60000, \
         ttlMs: 7200000 }}); \
         return await ws.hook.schedule({{ name: 'shortttl', code: {code}, delayMs: 60000, \
         ttlMs: 1 }});",
        code = json!("return { dispatch: false };")
    );
    // Perpetual section: every run dispatches, so the validation run fires and
    // the hook STILL persists as active. `delayMs: 60000` keeps the cadence out
    // of the way (fires are driven by `hook.runNow`) while `ttlMs: 20000` makes
    // the TTL land inside the event-read budget.
    let perpetual_inner = "const n = (hookState === null) ? 1 : hookState.n + 1; \
                           return { dispatch: true, message: 'perpetual fire ' + n, \
                                    state: { n } };";
    let schedule_perpetual_js = format!(
        "return await ws.hook.schedule({{ name: 'forever', code: {}, delayMs: 60000, \
         ttlMs: 20000, perpetual: true }});",
        json!(perpetual_inner)
    );
    // `firstTurnDelayMs` holds turn 1 open after the schedule tool call so the
    // dispatch wake stays QUEUED behind the in-flight turn long enough for the
    // `agent.getQueue` assertion; queue-drain turns (the wake text matches no
    // rule) resolve with the bare top-level response and run no tool calls.
    let behavior = json!({
        "response": "ok",
        "firstTurnDelayMs": 15_000,
        "rules": [
            {
                "ifPromptContains": "SCHEDULE_DISPATCH",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_dispatch_js, "summary": "schedule dispatcher" },
                },
                "response": "scheduled dispatcher",
            },
            {
                "ifPromptContains": "SCHEDULE_WATCH",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_watch_js, "summary": "schedule watcher" },
                },
                "response": "scheduled watcher",
            },
            {
                "ifPromptContains": "SCHEDULE_CANCELME",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_cancel_js, "summary": "schedule cancelme" },
                },
                "response": "scheduled cancelme",
            },
            {
                "ifPromptContains": "CANCEL_OTHERS",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": cancel_others_js, "summary": "cancel someone else's hook" },
                },
                "emitToolBlocks": true,
                "response": "tried to cancel cancelme",
            },
            {
                "ifPromptContains": "SCHEDULE_COUNTER",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_counter_js, "summary": "schedule counter" },
                },
                "response": "scheduled counter",
            },
            {
                "ifPromptContains": "SCHEDULE_SHORTTTL",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_ttl_js, "summary": "schedule shortttl" },
                },
                "response": "scheduled shortttl",
            },
            {
                "ifPromptContains": "SCHEDULE_PERPETUAL",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": schedule_perpetual_js, "summary": "schedule forever" },
                },
                "response": "scheduled forever",
            },
        ],
    })
    .to_string();

    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — `hook:*` prefix filter (plus agent stream noise we
    // skip) BEFORE any hook exists, proving the wildcard flows through the
    // subscription engine. `agent:idle` rides along for the idle-visibility
    // assertion (§6.5 `waitingOnHooks`).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["hook:*", "agent:idle"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — create the owner agent and drive the turns.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "HookOwner", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // ── 1. Schedule → immediate dispatch → wake queued mid-turn ──────────
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_DISPATCH" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Schedule-path event order for a validation-run dispatch:
    // hook:run-completed then hook:dispatched (no hook:scheduled).
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("dispatcher")).await;
    assert_eq!(completed["data"]["state"], "dispatched", "{completed}");
    let dispatched = next_hook_event(&mut sub, "hook:dispatched", Some("dispatcher")).await;
    assert_eq!(dispatched["data"]["agentId"], json!(agent_id));
    assert!(
        dispatched["data"]["hookId"].is_string(),
        "hookId on the event: {dispatched}"
    );

    // The owner is mid-turn (firstTurnDelayMs), so the wake QUEUED — visible
    // via agent.getQueue with the hook_wake metadata tag.
    let queue = wss_rpc(
        &mut rpc,
        12,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let entries = queue["queue"].as_array().expect("queue array");
    let wake = entries
        .iter()
        .find(|m| m["messageMetadata"]["type"] == json!("hook_wake"))
        .unwrap_or_else(|| panic!("hook_wake entry queued mid-turn: {queue}"));
    assert_eq!(wake["messageMetadata"]["hookName"], "dispatcher");
    // A one-shot dispatch wake tags its metadata hookStillActive=false.
    assert_eq!(
        wake["messageMetadata"]["hookStillActive"],
        json!(false),
        "{wake}"
    );
    assert!(
        wake["content"]
            .as_str()
            .unwrap_or("")
            .contains("[Background hook \"dispatcher\"] CI is red"),
        "wake content carries the dispatch message: {wake}"
    );
    assert!(
        wake["content"]
            .as_str()
            .unwrap_or("")
            .contains("now retired and will not run again"),
        "dispatch wake carries the terminal-state note: {wake}"
    );

    // After the turn ends the queue drains: the wake lands in the transcript.
    await_conversation_contains(
        &mut rpc,
        100,
        &ws_id,
        &agent_id,
        "[Background hook \\\"dispatcher\\\"] CI is red",
    )
    .await;
    // The terminal-state note survives the queue/conversation delivery path.
    await_conversation_contains(
        &mut rpc,
        101,
        &ws_id,
        &agent_id,
        "now retired and will not run again",
    )
    .await;

    // ── 2. Schedule a persisting watcher → hook.list + hook.runNow ───────
    let sent = wss_rpc(
        &mut rpc,
        200,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_WATCH" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Persisting schedule: run-completed (validation run, state scheduled,
    // nextRunAt set) then hook:scheduled.
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("watcher")).await;
    assert_eq!(completed["data"]["state"], "scheduled", "{completed}");
    assert!(
        completed["data"]["nextRunAt"].is_string(),
        "nextRunAt on a persisting run-completed: {completed}"
    );
    let scheduled = next_hook_event(&mut sub, "hook:scheduled", Some("watcher")).await;
    assert_eq!(scheduled["data"]["agentId"], json!(agent_id));
    let watcher_id = scheduled["data"]["hookId"]
        .as_str()
        .expect("watcher hookId")
        .to_string();

    // Idle-visibility (§6.5): the SCHEDULE_WATCH turn's terminal agent:idle
    // is emitted while the watcher hook is active — the payload carries the
    // emit-time `waitingOnHooks` stamp naming the hook with its light
    // metadata only.
    let idle = next_agent_idle(&mut sub, &agent_id).await;
    let waiting = idle["waitingOnHooks"]
        .as_array()
        .unwrap_or_else(|| panic!("agent:idle carries waitingOnHooks with an active hook: {idle}"));
    let watcher_entry = waiting
        .iter()
        .find(|h| h["hookId"] == json!(watcher_id))
        .unwrap_or_else(|| panic!("waitingOnHooks names the active watcher: {idle}"));
    assert_eq!(watcher_entry["name"], "watcher", "{idle}");
    assert!(
        watcher_entry["nextRunAt"].is_string() && watcher_entry["expiresAt"].is_string(),
        "waitingOnHooks entries carry the schedule timestamps: {idle}"
    );
    assert!(
        watcher_entry.get("code").is_none() && watcher_entry.get("lastLogs").is_none(),
        "waitingOnHooks stays light — no code/logs: {idle}"
    );

    // agent.get overlays the same list on the AgentLite projection (§5.5).
    let got = wss_rpc(
        &mut rpc,
        204,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert!(
        got["agent"]["waitingOnHooks"]
            .as_array()
            .is_some_and(|hooks| hooks.iter().any(|h| h["hookId"] == json!(watcher_id))),
        "agent.get serves waitingOnHooks for the hook-owning agent: {got}"
    );

    // FE read: hook.list reports both hooks with the wire `{ hooks }` shape.
    let listed = wss_rpc(&mut rpc, 201, "hook.list", json!({ "workspaceId": ws_id })).await;
    let hooks = listed["hooks"].as_array().expect("hooks array");
    assert_eq!(hooks.len(), 2, "dispatcher + watcher listed: {listed}");
    let watcher = hooks
        .iter()
        .find(|h| h["hookId"] == json!(watcher_id))
        .unwrap_or_else(|| panic!("watcher in hook.list: {listed}"));
    assert_eq!(watcher["name"], "watcher");
    assert_eq!(watcher["state"], "scheduled");
    assert_eq!(watcher["delayMs"], 60_000);
    assert_eq!(watcher["agentId"], json!(agent_id));
    assert_eq!(watcher["runCount"], 1, "validation run counted: {watcher}");
    let dispatcher = hooks
        .iter()
        .find(|h| h["name"] == json!("dispatcher"))
        .unwrap_or_else(|| panic!("dispatcher in hook.list: {listed}"));
    assert_eq!(dispatcher["state"], "dispatched");
    // A one-shot hook's sole fire is still counted: `dispatchCount` means
    // "fires so far" for every hook, not just perpetual ones.
    assert_eq!(dispatcher["dispatchCount"], 1, "{dispatcher}");

    // FE trigger: hook.runNow drives run-started + run-completed (still
    // scheduled — the note has no EVICT marker yet).
    let ran = wss_rpc(
        &mut rpc,
        202,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": watcher_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "runNow ok: {ran}");
    assert_eq!(ran["hookId"], json!(watcher_id));
    let started = next_hook_event(&mut sub, "hook:run-started", Some("watcher")).await;
    assert_eq!(started["data"]["state"], "running", "{started}");
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("watcher")).await;
    assert_eq!(completed["data"]["state"], "scheduled", "{completed}");

    // The run's console capture surfaces as `lastLogs` in hook.list.
    let listed = wss_rpc(&mut rpc, 203, "hook.list", json!({ "workspaceId": ws_id })).await;
    let watcher = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(watcher_id))
        .unwrap_or_else(|| panic!("watcher in hook.list: {listed}"))
        .clone();
    assert_eq!(
        watcher["lastLogs"],
        json!("watcher checked the note"),
        "lastLogs after a logging run: {watcher}"
    );

    // ── 3. Evict path: plant the marker, runNow → throw → hook:evicted ───
    let note = wss_rpc(
        &mut rpc,
        300,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = note["note"]["content"].as_str().expect("note content");
    let updated = wss_rpc(
        &mut rpc,
        301,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": format!("{content}\nEVICT\n"),
        }),
    )
    .await;
    assert!(updated["note"].is_object(), "note updated: {updated}");

    let ran = wss_rpc(
        &mut rpc,
        302,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": watcher_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "second runNow ok: {ran}");
    let evicted = next_hook_event(&mut sub, "hook:evicted", Some("watcher")).await;
    assert_eq!(evicted["data"]["state"], "evicted", "{evicted}");
    assert!(
        evicted["data"]["lastError"]
            .as_str()
            .unwrap_or("")
            .contains("EVICT marker found"),
        "hook:evicted carries lastError: {evicted}"
    );
    // The owner is woken with the eviction notice, ending with the
    // terminal-state note.
    await_conversation_contains(
        &mut rpc,
        310,
        &ws_id,
        &agent_id,
        "was evicted after a failed run",
    )
    .await;
    await_conversation_contains(&mut rpc, 311, &ws_id, &agent_id, "will not run again").await;

    // ── 4. FE cancel: hook:cancelled + owner woken with the notice ───────
    let sent = wss_rpc(
        &mut rpc,
        400,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_CANCELME" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    // The 50-char (maximum) name is accepted by the MCP schedule route and
    // round-trips through the `hook:scheduled` event.
    let scheduled = next_hook_event(&mut sub, "hook:scheduled", Some(cancel_hook_name)).await;
    let cancel_id = scheduled["data"]["hookId"]
        .as_str()
        .expect("cancelme hookId")
        .to_string();

    // Ownership scoping (intent-hq/monorepo#1563): a second agent sees the
    // hook in the workspace-wide `ws.hook.list` but cannot cancel it through
    // the MCP route — the error names the owning agent and the hook is
    // untouched (still scheduled, no `hook:cancelled`, owner not woken).
    let intruder = wss_rpc(
        &mut rpc,
        420,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Intruder", "model": "mock:default" }),
    )
    .await;
    let intruder_id = intruder["agent"]["id"]
        .as_str()
        .expect("intruder id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        421,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": intruder_id, "content": "CANCEL_OTHERS" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    for (i, needle) in ["found=true", "cancel=rejected", "ownerNamed=true"]
        .into_iter()
        .enumerate()
    {
        await_conversation_contains(
            &mut rpc,
            430 + i64::try_from(i).expect("fits in i64") * 20,
            &ws_id,
            &intruder_id,
            needle,
        )
        .await;
    }
    // The hook survived the cross-agent attempt.
    let listed = wss_rpc(&mut rpc, 440, "hook.list", json!({ "workspaceId": ws_id })).await;
    let survivor = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(cancel_id))
        .unwrap_or_else(|| panic!("cancelme still listed: {listed}"))
        .clone();
    assert_eq!(
        survivor["state"], "scheduled",
        "cross-agent cancel left the hook active: {survivor}"
    );
    assert_eq!(survivor["agentId"], json!(agent_id));

    let cancelled = wss_rpc(
        &mut rpc,
        401,
        "hook.cancel",
        json!({ "workspaceId": ws_id, "hookId": cancel_id }),
    )
    .await;
    assert_eq!(cancelled["ok"], json!(true), "{cancelled}");
    assert_eq!(cancelled["hook"]["state"], "cancelled", "{cancelled}");
    assert_eq!(cancelled["hook"]["hookId"], json!(cancel_id));
    assert_eq!(
        cancelled["hook"]["name"],
        json!(cancel_hook_name),
        "50-char name persists and round-trips: {cancelled}"
    );
    let ev = next_hook_event(&mut sub, "hook:cancelled", Some(cancel_hook_name)).await;
    assert_eq!(ev["data"]["hookId"], json!(cancel_id));
    // FE cancel (no agent caller) wakes the owner with the notice.
    await_conversation_contains(
        &mut rpc,
        410,
        &ws_id,
        &agent_id,
        "This hook was cancelled from the app.",
    )
    .await;

    // ── 5. Error arms (PROTOCOL §9): unknown/invalid → -32602 ────────────
    let err = wss_rpc_raw(
        &mut rpc,
        500,
        "hook.cancel",
        json!({ "workspaceId": ws_id, "hookId": "hook-nonexistent" }),
    )
    .await;
    assert_eq!(
        err["error"]["code"], -32602,
        "unknown hookId ⇒ -32602: {err}"
    );
    let err = wss_rpc_raw(
        &mut rpc,
        501,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": "hook-nonexistent" }),
    )
    .await;
    assert_eq!(
        err["error"]["code"], -32602,
        "unknown hookId ⇒ -32602: {err}"
    );
    // runNow on the (now-cancelled) hook: recognized but inactive ⇒ -32602.
    let err = wss_rpc_raw(
        &mut rpc,
        502,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": cancel_id }),
    )
    .await;
    assert_eq!(
        err["error"]["code"], -32602,
        "inactive hook ⇒ -32602: {err}"
    );
    // Missing hookId / workspaceId ⇒ -32602.
    let err = wss_rpc_raw(
        &mut rpc,
        503,
        "hook.cancel",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing hookId ⇒ -32602: {err}"
    );
    let err = wss_rpc_raw(&mut rpc, 504, "hook.list", json!({})).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing workspaceId ⇒ -32602: {err}"
    );

    // ── 6. State carry-over: counter hook dispatches when n reaches 2 ────
    let sent = wss_rpc(
        &mut rpc,
        600,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_COUNTER" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Validation run persists { n: 1 } and the hook stays scheduled.
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("counter")).await;
    assert_eq!(completed["data"]["state"], "scheduled", "{completed}");
    let scheduled = next_hook_event(&mut sub, "hook:scheduled", Some("counter")).await;
    let counter_id = scheduled["data"]["hookId"]
        .as_str()
        .expect("counter hookId")
        .to_string();
    let listed = wss_rpc(&mut rpc, 601, "hook.list", json!({ "workspaceId": ws_id })).await;
    let counter = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(counter_id))
        .unwrap_or_else(|| panic!("counter in hook.list: {listed}"))
        .clone();
    assert_eq!(
        counter["lastState"],
        json!("{\"n\":1}"),
        "validation run persisted its state: {counter}"
    );

    // Run 2: reads the injected { n: 1 }, persists { n: 2 }, stays scheduled.
    let ran = wss_rpc(
        &mut rpc,
        602,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": counter_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "runNow ok: {ran}");
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("counter")).await;
    assert_eq!(completed["data"]["state"], "scheduled", "{completed}");
    let listed = wss_rpc(&mut rpc, 603, "hook.list", json!({ "workspaceId": ws_id })).await;
    let counter = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(counter_id))
        .unwrap_or_else(|| panic!("counter in hook.list: {listed}"))
        .clone();
    assert_eq!(
        counter["lastState"],
        json!("{\"n\":2}"),
        "carried state advanced: {counter}"
    );

    // Run 3: the carried count reaches the threshold — dispatch.
    let ran = wss_rpc(
        &mut rpc,
        604,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": counter_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "runNow ok: {ran}");
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("counter")).await;
    assert_eq!(completed["data"]["state"], "dispatched", "{completed}");
    let dispatched = next_hook_event(&mut sub, "hook:dispatched", Some("counter")).await;
    assert_eq!(dispatched["data"]["hookId"], json!(counter_id));
    // The wake carries the count threaded through hookState.
    await_conversation_contains(
        &mut rpc,
        610,
        &ws_id,
        &agent_id,
        "[Background hook \\\"counter\\\"] counted 2",
    )
    .await;
    let listed = wss_rpc(&mut rpc, 630, "hook.list", json!({ "workspaceId": ws_id })).await;
    let counter = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(counter_id))
        .unwrap_or_else(|| panic!("counter in hook.list: {listed}"))
        .clone();
    assert_eq!(counter["state"], "dispatched");
    assert_eq!(counter["runCount"], 3, "dispatched on run 3: {counter}");

    // ── 7. TTL expiry: short-TTL hook expires, owner woken (v3.1) ────────
    let sent = wss_rpc(
        &mut rpc,
        700,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_SHORTTTL" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let scheduled = next_hook_event(&mut sub, "hook:scheduled", Some("shortttl")).await;
    let ttl_id = scheduled["data"]["hookId"]
        .as_str()
        .expect("shortttl hookId")
        .to_string();

    // hook.list surfaces the persisted expiresAt (ttlMs: 1 clamps to the
    // 10s floor: expiresAt ≈ createdAt + 10s, well under the 24-hour cap).
    let listed = wss_rpc(&mut rpc, 701, "hook.list", json!({ "workspaceId": ws_id })).await;
    let ttl_hook = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(ttl_id))
        .unwrap_or_else(|| panic!("shortttl in hook.list: {listed}"))
        .clone();
    assert!(
        ttl_hook["expiresAt"].is_string(),
        "expiresAt persisted: {ttl_hook}"
    );

    // Clamp coverage on the wire: milliseconds between a listed hook's
    // createdAt and expiresAt (both derive from the same schedule-time
    // instant, so the delta is the persisted clamped ttlMs, exactly).
    let ttl_of = |listed: &Value, name: &str| -> i64 {
        let hook = listed["hooks"]
            .as_array()
            .expect("hooks array")
            .iter()
            .find(|h| h["name"] == json!(name))
            .unwrap_or_else(|| panic!("{name} in hook.list: {listed}"))
            .clone();
        let parse = |field: &str| {
            chrono::DateTime::parse_from_rfc3339(
                hook[field]
                    .as_str()
                    .unwrap_or_else(|| panic!("{name} {field}: {hook}")),
            )
            .unwrap_or_else(|e| panic!("{name} {field} parses: {e}"))
        };
        (parse("expiresAt") - parse("createdAt")).num_milliseconds()
    };
    // Omitted ttlMs → the 24-hour default; in-range 2h ttlMs persists as-is.
    assert_eq!(ttl_of(&listed, "defaultttl"), 86_400_000, "{listed}");
    assert_eq!(ttl_of(&listed, "midttl"), 7_200_000, "{listed}");
    assert_eq!(ttl_of(&listed, "shortttl"), 10_000, "{listed}");

    // The deadline (~10s out) passes without another run: hook:expired with
    // payload parity with hook:cancelled (base data object, no extras).
    let expired = next_hook_event(&mut sub, "hook:expired", Some("shortttl")).await;
    assert_eq!(expired["data"]["state"], "expired", "{expired}");
    assert_eq!(expired["data"]["hookId"], json!(ttl_id));
    assert_eq!(expired["data"]["agentId"], json!(agent_id));

    // Terminal in hook.list; runNow on an expired hook is -32602.
    let listed = wss_rpc(&mut rpc, 702, "hook.list", json!({ "workspaceId": ws_id })).await;
    let ttl_hook = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(ttl_id))
        .unwrap_or_else(|| panic!("shortttl in hook.list: {listed}"))
        .clone();
    assert_eq!(ttl_hook["state"], "expired");
    assert_eq!(
        ttl_hook.get("nextRunAt"),
        None,
        "nextRunAt cleared on expiry: {ttl_hook}"
    );
    let err = wss_rpc_raw(
        &mut rpc,
        703,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": ttl_id }),
    )
    .await;
    assert_eq!(err["error"]["code"], -32602, "expired hook ⇒ -32602: {err}");

    // The owner is woken with the expiry notice naming the reschedule option.
    await_conversation_contains(
        &mut rpc,
        710,
        &ws_id,
        &agent_id,
        "expired after reaching its TTL",
    )
    .await;

    // ── 8. Perpetual: dispatch re-arms until the TTL expires it ───────────
    let sent = wss_rpc(
        &mut rpc,
        800,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SCHEDULE_PERPETUAL" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Schedule-path event order for a PERPETUAL validation-run dispatch:
    // run-completed, dispatched, THEN hook:scheduled — unlike one-shot, the
    // dispatching first run still persists an active schedule.
    let completed = next_hook_event(&mut sub, "hook:run-completed", Some("forever")).await;
    assert_eq!(completed["data"]["state"], "scheduled", "{completed}");
    let dispatched = next_hook_event(&mut sub, "hook:dispatched", Some("forever")).await;
    assert_eq!(dispatched["data"]["state"], "scheduled", "{dispatched}");
    let scheduled = next_hook_event(&mut sub, "hook:scheduled", Some("forever")).await;
    let forever_id = scheduled["data"]["hookId"]
        .as_str()
        .expect("forever hookId")
        .to_string();

    let find_forever = |listed: &Value| -> Value {
        listed["hooks"]
            .as_array()
            .expect("hooks array")
            .iter()
            .find(|h| h["hookId"] == json!(forever_id))
            .unwrap_or_else(|| panic!("forever in hook.list: {listed}"))
            .clone()
    };

    // Still ACTIVE after its own dispatch, with the fire counted.
    let listed = wss_rpc(&mut rpc, 801, "hook.list", json!({ "workspaceId": ws_id })).await;
    let forever = find_forever(&listed);
    assert_eq!(forever["state"], "scheduled", "{forever}");
    assert_eq!(forever["perpetual"], json!(true), "{forever}");
    assert_eq!(forever["runCount"], 1, "{forever}");
    assert_eq!(forever["dispatchCount"], 1, "{forever}");
    assert!(
        forever["nextRunAt"].is_string(),
        "re-armed with a fresh nextRunAt: {forever}"
    );

    // The perpetual fire's wake says the hook stays active until its TTL —
    // instead of the one-shot "retired" note — and its metadata tags the
    // wake hookStillActive=true.
    await_conversation_contains(
        &mut rpc,
        810,
        &ws_id,
        &agent_id,
        "[Background hook \\\"forever\\\"] perpetual fire 1",
    )
    .await;
    await_conversation_contains(&mut rpc, 820, &ws_id, &agent_id, "remains active until").await;
    await_conversation_contains(&mut rpc, 825, &ws_id, &agent_id, "\"hookStillActive\":true").await;

    // A second fire (FE-triggered) wakes again and re-arms again: the hook is
    // still `scheduled` and `dispatchCount` advances, so `hook:dispatched` is
    // non-terminal for a perpetual hook.
    let ran = wss_rpc(
        &mut rpc,
        830,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": forever_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "runNow ok: {ran}");
    let dispatched = next_hook_event(&mut sub, "hook:dispatched", Some("forever")).await;
    assert_eq!(dispatched["data"]["hookId"], json!(forever_id));
    // On the scheduler-loop path the post-dispatch outcome (scheduled vs
    // expired) is resolved and persisted BEFORE `hook:dispatched` is
    // emitted, so `state` reflects the real outcome rather than the
    // transient `running` set at run start — parity with the schedule-time
    // validation path. The event also carries `perpetual`/`dispatchCount`
    // for FE/inspection parity with `hook.list`.
    assert_eq!(dispatched["data"]["state"], "scheduled", "{dispatched}");
    assert_eq!(dispatched["data"]["perpetual"], json!(true), "{dispatched}");
    assert_eq!(
        dispatched["data"]["dispatchCount"],
        json!(2),
        "{dispatched}"
    );
    let rearmed = next_hook_event(&mut sub, "hook:scheduled", Some("forever")).await;
    assert!(
        rearmed["data"]["nextRunAt"].is_string(),
        "re-armed after the second fire: {rearmed}"
    );
    let listed = wss_rpc(&mut rpc, 831, "hook.list", json!({ "workspaceId": ws_id })).await;
    let forever = find_forever(&listed);
    assert_eq!(forever["state"], "scheduled", "{forever}");
    assert_eq!(forever["runCount"], 2, "{forever}");
    assert_eq!(forever["dispatchCount"], 2, "{forever}");
    await_conversation_contains(
        &mut rpc,
        840,
        &ws_id,
        &agent_id,
        "[Background hook \\\"forever\\\"] perpetual fire 2",
    )
    .await;

    // TTL still terminates it (ttlMs: 20000, delayMs: 60000 — no cadence run
    // intervenes), and the expiry notice reports runs AND dispatches rather
    // than the one-shot "without a dispatch" wording.
    let expired = next_hook_event(&mut sub, "hook:expired", Some("forever")).await;
    assert_eq!(expired["data"]["state"], "expired", "{expired}");
    assert_eq!(expired["data"]["hookId"], json!(forever_id));
    let err = wss_rpc_raw(
        &mut rpc,
        850,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": forever_id }),
    )
    .await;
    assert_eq!(err["error"]["code"], -32602, "expired hook ⇒ -32602: {err}");
    await_conversation_contains(
        &mut rpc,
        860,
        &ws_id,
        &agent_id,
        "expired after reaching its TTL (2 runs, 2 dispatches)",
    )
    .await;
}
