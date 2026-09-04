//! WSS end-to-end: graceful shutdown reaps the FULL provider process tree.
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) against the mock ACP provider,
//! which (via `MOCK_AGENT_TREE_PID_FILE`) spawns a long-lived grandchild
//! (`sleep 300`) that inherits the provider's process group — the bridge-style
//! tree a real provider produces (e.g. an npx-launched MCP bridge). The agent parks
//! mid-turn so the child is guaranteed live, then `system.shutdown` triggers
//! the graceful teardown path and the test asserts BOTH the provider child AND
//! its grandchild are dead within the bounded kill-sweep window (<4s) — the
//! orphan class the parallel shutdown kill sweep + group-kill hardening fixes.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::DaemonGuard;
use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-reap-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
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
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::{Digest, Sha256};
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

/// Read one `events.event` notification from a subscriber connection (bounded;
/// the timeout is total, so heartbeat Pings do not reset it).
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
            Some(d) if !d.is_zero() => d,
            _ => panic!("wss event timed out"),
        };
        let Ok(next) = timeout(remaining, ws.next()).await else {
            panic!("wss event timed out")
        };
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
            other => panic!("expected event frame, got {other:?}"),
        }
    }
}

/// Mock-agent gate (parity with the other WSS E2E suites).
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

/// `true` once `pid` no longer exists as a live process. `kill(pid, 0)`
/// answers ESRCH once the pid is gone; a zombie (dead, awaiting reap by the
/// daemon's kill-sweep wait task or by init after the daemon exits) is still
/// signalable, so treat state `Z` as dead too — the process has terminated,
/// only the wait(2) bookkeeping remains.
fn pid_dead(pid: i32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    match kill(Pid::from_raw(pid), None) {
        Err(Errno::ESRCH) => true,
        Err(_) => false,
        Ok(()) => Command::new("ps")
            .args(["-o", "state=", "-p"])
            .arg(pid.to_string())
            .output()
            .is_ok_and(|o| {
                !o.status.success()
                    || String::from_utf8_lossy(&o.stdout)
                        .trim_start()
                        .starts_with('Z')
            }),
    }
}

/// Graceful shutdown must reap the provider child AND its grandchild within
/// one bounded kill-sweep window. The mock provider spawns `sleep 300` in its
/// process group and reports both pids via `MOCK_AGENT_TREE_PID_FILE`; the
/// agent parks mid-turn (`blockUntilCancel`) so the tree is guaranteed live
/// when `system.shutdown` lands.
#[tokio::test]
async fn shutdown_reaps_provider_child_and_grandchild() {
    let Some(script) = gate("WSS shutdown provider-tree reap E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let ws_id = "ws-reap-test";

    // Pre-seed the workspace so agent.create succeeds (store is closed before
    // the daemon opens the same data dir).
    {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let store = Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open store");
        store
            .insert_workspace(&workspace_seed(&WorkspaceId(ws_id.to_string())))
            .await
            .expect("insert ws");
    }

    let pid_file = data_dir.join("tree-pids.json");
    let behavior = json!({ "blockUntilCancel": true }).to_string();
    common::enable_ws_api(&data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("MOCK_AGENT_SCRIPT_PATH", &script)
        .env("MOCK_AGENT_BEHAVIOR", &behavior)
        .env("MOCK_AGENT_TREE_PID_FILE", &pid_file)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    // Own process group so the DaemonGuard's SIGKILL cannot leak mock children
    // if the test panics before the graceful path runs.
    cmd.process_group(0);
    let child = cmd.spawn().expect("spawn intentd serve");
    let mut daemon = DaemonGuard::new(child, data_dir.clone(), true);
    if !await_uds(&socket).await {
        if let Ok(log) = std::fs::read_to_string(data_dir.join("daemon.log")) {
            eprintln!("Daemon log:\n{log}");
        }
        panic!("daemon did not start");
    }

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(&fp);

    // Subscriber BEFORE the turn so the parked-chunk signal cannot be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Reap Agent", "model": "default", "provider": "mock" }),
    )
    .await;
    assert!(
        created["agent"]["id"].is_string(),
        "agent created: {created}"
    );

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": created["agent"]["id"], "content": "park" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // blockUntilCancel streams one chunk then parks the prompt unresolved —
    // seeing the chunk proves the provider child is live and mid-turn.
    let mut saw_chunk = false;
    for _ in 0..20 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:activity" {
            saw_chunk = true;
            break;
        }
    }
    assert!(saw_chunk, "agent did not signal its parked-turn activity");

    // The mock writes the pid file at process startup, so it must exist by the
    // time a chunk streamed; the short poll only absorbs fs visibility lag.
    let pids: Value = {
        let mut parsed = None;
        for _ in 0..100 {
            if let Ok(raw) = std::fs::read_to_string(&pid_file) {
                if let Ok(v) = serde_json::from_str::<Value>(raw.trim()) {
                    parsed = Some(v);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        parsed.expect("mock never wrote MOCK_AGENT_TREE_PID_FILE")
    };
    let child_pid =
        i32::try_from(pids["childPid"].as_i64().expect("childPid")).expect("pid fits in i32");
    let grandchild_pid = i32::try_from(pids["grandchildPid"].as_i64().expect("grandchildPid"))
        .expect("pid fits in i32");
    assert!(!pid_dead(child_pid), "provider child live before shutdown");
    assert!(!pid_dead(grandchild_pid), "grandchild live before shutdown");

    // Graceful shutdown via the UDS-only control fast-path (PROTOCOL §5.7).
    let started = Instant::now();
    let shutdown = uds_rpc(&socket, 2, "system.shutdown", json!({})).await;
    assert_eq!(shutdown["result"].get("ok"), Some(&json!(true)));

    // BOTH pids must be gone within the bounded kill-sweep window: SIGTERM is
    // group-wide and immediate, the shared grace is 2s, so 4s (scaled only for
    // coverage instrumentation) bounds the whole sweep.
    let budget = common::test_timeout(Duration::from_secs(4));
    loop {
        if pid_dead(child_pid) && pid_dead(grandchild_pid) {
            break;
        }
        assert!(
            started.elapsed() < budget,
            "provider tree survived the kill sweep past {budget:?}: \
             child {child_pid} dead={}, grandchild {grandchild_pid} dead={}",
            pid_dead(child_pid),
            pid_dead(grandchild_pid)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The daemon itself exits cleanly after the sweep.
    let exit_ok = timeout(common::test_timeout(Duration::from_secs(10)), async {
        loop {
            match daemon.child_mut().try_wait() {
                Ok(Some(status)) => return status.success(),
                Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(e) => panic!("failed to wait for daemon: {e}"),
            }
        }
    })
    .await
    .expect("daemon did not exit after system.shutdown");
    assert!(exit_ok, "daemon exited non-zero");
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-REAP".to_string(),
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
        context_links: None,
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
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}
