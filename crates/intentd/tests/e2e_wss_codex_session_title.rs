//! WSS e2e for the codex `session/new` `_meta.sessionTitle` injection
//! (monorepo#3151): the daemon must send the agent's name out of band as
//! `_meta: { "sessionTitle": … }` on codex `session/new` — on BOTH the
//! initial create and the resume-impossible recreate path — so delegated
//! Codex threads stop titling themselves from the prepended system prompt.
//!
//! The `codex` provider is resolved hermetically via a `providers.paths`
//! override in `config.toml` pointing at a shell wrapper that execs the
//! deterministic mock fixture (the cross-provider-history pattern), and the
//! fixture's `MOCK_AGENT_SESSION_LOG` seam records each `session/new` /
//! `session/load` WITH the request's `_meta` verbatim. Sequence proven on
//! the wire: turn 1 `session/new` carries exactly
//! `{ "sessionTitle": "<agent name>" }` → idle child `SIGKILLed` out-of-band →
//! turn 2 cannot resume (the mock advertises `loadSession: false`), so the
//! recreate path opens a second `session/new` that carries the same `_meta`.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::fmt::Write as _;
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
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// The explicit agent name — the exact string `session/new` must carry as
/// `_meta.sessionTitle` for codex.
const AGENT_NAME: &str = "Codex Title E2E";

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
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-codextitle-{}", &id[..8]));
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

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
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

/// Read one `events.event` notification from a subscriber connection (bounded).
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
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

/// Drain subscriber events until an `agent:stream:end` for `agent_id` arrives.
async fn await_stream_end<S>(sub: &mut WebSocketStream<S>, agent_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"].as_str() == Some(agent_id) {
            return;
        }
    }
    panic!("no agent:stream:end for {agent_id}");
}

/// Mock-agent gate (parity with the WSS lifecycle suite).
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

/// Write an executable shell wrapper that execs the mock fixture under `node`,
/// discarding whatever base args the daemon passes for the impersonated
/// codex provider — the fixture speaks ACP on stdio and ignores argv anyway.
fn write_provider_wrapper(data_dir: &Path, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let wrapper = data_dir.join("fake-codex");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec \"{}\" \"{}\"\n", node.display(), script),
    )
    .expect("write wrapper");
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
        .expect("chmod wrapper");
    wrapper
}

/// Seed `config.toml` with a `providers.paths` override pinning `codex` to the
/// wrapper — the highest-precedence tier of provider binary resolution, so a
/// real codex install (PATH or npx fallback) can never be picked up.
fn seed_codex_path_override(data_dir: &Path, wrapper: &Path) {
    let path = data_dir.join("config.toml");
    let mut text = std::fs::read_to_string(&path).unwrap_or_default();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    let _ = write!(
        text,
        "\n[providers.paths]\ncodex = \"{}\"\n",
        wrapper.display()
    );
    std::fs::write(&path, text).expect("write config.toml");
}

/// Parse the mock fixture's session-lifecycle log: one
/// `{ method, sessionId, pid, meta }` JSON line per `session/new` /
/// `session/load` the child received (`MOCK_AGENT_SESSION_LOG` seam), with
/// the request's `_meta` verbatim (null when absent).
fn read_session_log(path: &Path) -> Vec<(String, Value)> {
    let raw = std::fs::read_to_string(path).expect("read session log");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("session log line json");
            (
                v["method"].as_str().expect("method").to_string(),
                v["meta"].clone(),
            )
        })
        .collect()
}

/// Bounded poll: wait until the pid file has at least `n` lines.
async fn await_pid_lines(path: &Path, n: usize) -> Vec<u32> {
    for _ in 0..400 {
        if let Ok(contents) = tokio::fs::read_to_string(path).await {
            let pids: Vec<u32> = contents
                .lines()
                .filter_map(|l| l.trim().parse().ok())
                .collect();
            if pids.len() >= n {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("pid file {} never reached {n} line(s)", path.display());
}

/// Bounded poll: wait until the daemon log contains `needle`.
async fn await_daemon_log_contains(data_dir: &Path, needle: &str) {
    let log_path = data_dir.join("daemon.log");
    for _ in 0..400 {
        if tokio::fs::read_to_string(&log_path)
            .await
            .unwrap_or_default()
            .contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("daemon log never contained {needle:?}");
}

/// CODEX SESSION TITLE (monorepo#3151): a codex agent's `session/new` carries
/// `_meta: { "sessionTitle": "<agent name>" }` on the wire — on the initial
/// create AND on the recreate after the idle child is killed (the mock
/// advertises `loadSession: false`, so the resume-impossible recreate path
/// opens a fresh `session/new`). The `_meta` must carry sessionTitle ONLY
/// (the system prompt stays on the first-turn prepend, never in `_meta`).
#[tokio::test]
async fn codex_session_new_carries_session_title_meta_over_wss() {
    let Some(script) = gate("WSS codex session-title E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let wrapper = write_provider_wrapper(&data_dir, &script);
    seed_codex_path_override(&data_dir, &wrapper);
    let pid_file = data_dir.join("pids.txt");
    let pid_file_s = pid_file.to_string_lossy().into_owned();
    let session_log = data_dir.join("sessions.txt");
    let session_log_s = session_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "CODEX_TITLE_E2E_REPLY" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PID_FILE", &pid_file_s),
        ("MOCK_AGENT_SESSION_LOG", &session_log_s),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
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

    // SUBSCRIBER conn — events.subscribe BEFORE the turns so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "Codex Title E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — create the agent ON THE CODEX PROVIDER (hermetic wrapper).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": AGENT_NAME,
            "model": "codex:mock-model",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Turn 1: the initial create path opens session/new.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "first sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    let log = read_session_log(&session_log);
    assert_eq!(log.len(), 1, "turn 1 opened exactly one session: {log:?}");
    let (method, meta) = &log[0];
    assert_eq!(method, "session/new", "turn 1 established via create");
    assert_eq!(
        *meta,
        json!({ "sessionTitle": AGENT_NAME }),
        "codex session/new _meta carries the agent name as sessionTitle and nothing else"
    );

    // SIGKILL the idle child out-of-band. The mock advertises
    // `loadSession: false`, so the next message hits the resume-impossible
    // recreate path: a fresh session/new on the respawned child.
    let first_pid = await_pid_lines(&pid_file, 1).await[0];
    let killed = Command::new("kill")
        .args(["-9", &first_pid.to_string()])
        .status()
        .expect("run kill")
        .success();
    assert!(killed, "SIGKILL delivered to idle mock child {first_pid}");
    await_daemon_log_contains(
        &data_dir,
        "idle agent child exited unexpectedly; handle reaped",
    )
    .await;

    // Turn 2: the recreate path must ALSO carry the sessionTitle _meta.
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;

    let log = read_session_log(&session_log);
    assert_eq!(
        log.len(),
        2,
        "turn 2 recreated exactly one more session: {log:?}"
    );
    let (method2, meta2) = &log[1];
    assert_eq!(
        method2, "session/new",
        "recreate opens a fresh session/new (no session/load): {log:?}"
    );
    assert_eq!(
        *meta2,
        json!({ "sessionTitle": AGENT_NAME }),
        "recreate session/new _meta carries the same sessionTitle"
    );
}
