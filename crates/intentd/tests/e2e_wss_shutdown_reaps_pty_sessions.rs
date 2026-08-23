//! WSS end-to-end: graceful daemon shutdown reaps every daemon-owned PTY
//! session — interactive terminals (`terminal.create`) and running service
//! scripts (`script.start`) — including SIGHUP/SIGTERM-trapping children, the
//! orphan class from monorepo#1526. Boots a real `intentd serve`, spawns both
//! kinds of PTY over the real WSS transport, then drives `system.shutdown`
//! and asserts no child survives the bounded kill sweep.

#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-ptyreap-{}", &id[..8]));
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

/// `true` once `pid` no longer exists as a live process (zombies count as
/// dead — only the wait(2) bookkeeping remains; parity with
/// `e2e_wss_shutdown_reaps_provider_tree`).
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

/// Shell body that backgrounds a TERM+HUP-trapping straggler: the straggler
/// touches `flag` only after its traps are installed and its pid is written to
/// `pidfile`, so the test never observes a pid whose traps are not yet active.
/// The direct shell then sleeps forever (killed by the daemon's group sweep).
fn straggler_body(flag: &Path, pidfile: &Path) -> String {
    format!(
        r#"sh -c 'trap "" TERM HUP; : > "{f}"; sleep 300' & echo $! > "{p}"; while [ ! -e "{f}" ]; do sleep 0.05; done; sleep 300"#,
        f = flag.display(),
        p = pidfile.display()
    )
}

/// Poll `pidfile` until it yields the straggler's pid (traps active by then).
async fn await_straggler_pid(pidfile: &Path) -> i32 {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    loop {
        if let Ok(s) = std::fs::read_to_string(pidfile) {
            if let Ok(pid) = s.trim().parse::<i32>() {
                return pid;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "straggler pid never written to {}",
            pidfile.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// SIGKILL a straggler on drop so a failed test cannot leak a TERM+HUP-
/// trapping `sleep 300` into the suite.
struct KillOnDrop(i32);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-9", &self.0.to_string()])
            .stderr(Stdio::null())
            .status();
    }
}

/// Graceful shutdown must reap BOTH daemon-owned PTY classes — an interactive
/// terminal and a running service script — including their SIGHUP/SIGTERM-
/// trapping descendants, within the bounded kill-sweep window; the daemon then
/// exits cleanly (monorepo#1526).
#[tokio::test]
async fn shutdown_reaps_terminal_and_script_pty_sessions() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let ws_id = "ws-pty-reap";

    // Pre-seed the workspace so terminal.create / script.start succeed (store
    // is closed before the daemon opens the same data dir).
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

    // Terminal straggler: `terminal.create` execs its command directly (no
    // shell argv), so the trap body ships as an executable script file.
    let term_flag = data_dir.join("term-flag");
    let term_pidfile = data_dir.join("term-pid");
    let term_script = data_dir.join("term-trap.sh");
    std::fs::write(
        &term_script,
        format!("#!/bin/sh\n{}\n", straggler_body(&term_flag, &term_pidfile)),
    )
    .expect("write terminal trap script");
    std::fs::set_permissions(&term_script, std::fs::Permissions::from_mode(0o755))
        .expect("chmod terminal trap script");

    // Script straggler: `script.*` runs the command through a login shell, so
    // the body ships inline.
    let script_flag = data_dir.join("script-flag");
    let script_pidfile = data_dir.join("script-pid");
    let script_cmd = straggler_body(&script_flag, &script_pidfile);

    common::enable_ws_api(&data_dir);
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    // Own process group so the DaemonGuard's SIGKILL cannot leak PTY children
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
    let mut ws = connect_ws(port, cfg).await;

    // Interactive terminal running the trap script.
    let term = wss_rpc(
        &mut ws,
        2,
        "terminal.create",
        json!({ "workspaceId": ws_id, "command": term_script.to_string_lossy() }),
    )
    .await;
    assert!(term["terminalId"].is_string(), "terminal created: {term}");

    // Service script running the trap body; started so its supervisor is live.
    let created = wss_rpc(
        &mut ws,
        3,
        "script.create",
        json!({
            "workspaceId": ws_id,
            "name": "trap service",
            "command": script_cmd,
            "mode": "service",
            "scriptId": "trap-svc",
        }),
    )
    .await;
    assert_eq!(created["id"], "trap-svc");
    let started = wss_rpc(
        &mut ws,
        4,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "trap-svc" }),
    )
    .await;
    assert_eq!(started["ok"], json!(true), "script started: {started}");

    // Both stragglers alive with traps installed BEFORE shutdown.
    let term_straggler = await_straggler_pid(&term_pidfile).await;
    let _term_guard = KillOnDrop(term_straggler);
    let script_straggler = await_straggler_pid(&script_pidfile).await;
    let _script_guard = KillOnDrop(script_straggler);
    assert!(!pid_dead(term_straggler), "terminal straggler live");
    assert!(!pid_dead(script_straggler), "script straggler live");

    // Graceful shutdown via the UDS-only control fast-path (PROTOCOL §5.7).
    let started_at = Instant::now();
    let shutdown = uds_rpc(&socket, 5, "system.shutdown", json!({})).await;
    assert_eq!(shutdown["result"].get("ok"), Some(&json!(true)));

    // Both stragglers must be gone within the bounded sweep: scripts are
    // flagged user-stopped synchronously, then one concurrent kill-all covers
    // script and terminal PTYs alike — a single 2s SIGTERM grace wall-clock.
    // The budget is deliberately loose (the regression signal is "stragglers
    // die at all vs. survive forever", not the exact latency) so full-suite
    // CI contention cannot flake it; 10s still sits under the daemon-exit
    // assertion below.
    let budget = common::test_timeout(Duration::from_secs(10));
    loop {
        if pid_dead(term_straggler) && pid_dead(script_straggler) {
            break;
        }
        assert!(
            started_at.elapsed() < budget,
            "PTY children survived the shutdown sweep past {budget:?}: \
             terminal {term_straggler} dead={}, script {script_straggler} dead={}",
            pid_dead(term_straggler),
            pid_dead(script_straggler)
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
        title: "WSS-PTY-REAP".to_string(),
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
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}
