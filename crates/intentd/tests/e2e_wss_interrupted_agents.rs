//! WSS end-to-end for `agent.listInterrupted` (INT-41, agent-resumption phase 1).
//!
//! Boots a real `intentd serve` (WSS listener enabled via config), creates agent sessions in stale
//! in-flight statuses (Active/Processing/Waiting), restarts the daemon, and
//! verifies that `agent.listInterrupted` returns the interrupted agents.
//!
//! Coverage:
//! - Interrupted agents are persisted across restart (crash)
//! - Interrupted agents are persisted on graceful shutdown (system.shutdown RPC)
//! - `agent.listInterrupted` returns pending rows with joined workspace/agent data
//! - Terminal/pending sessions are not captured
//! - Idempotent inserts on second restart

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

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

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-interrupted-")
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
                if v["id"] == json!(id) && v.get("result").is_some() {
                    return v["result"].clone();
                } else if v["id"] == json!(id) {
                    panic!("rpc errored: {v}");
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
/// The timeout is total (not per-frame), so heartbeat Pings do not reset it.
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

#[tokio::test]
async fn interrupted_agents_persisted_across_restart() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let listen = "both";
    let socket = data_dir.join("intentd.sock");

    // Phase 1: Boot daemon, create a workspace, create an agent session with Active status.
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    // Pin resumeInterruptedOnStart=off: this suite asserts pending rows
    // survive a restart, but the `auto` default resumes on headless hosts.
    common::disable_resume_on_start(&data_dir);
    let mut cmd1 = common::serve_command();
    cmd1.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    // Spawn in its own process group to prevent ACP mock process leaks
    #[cfg(unix)]
    cmd1.process_group(0);
    let child1 = cmd1.spawn().expect("spawn intentd serve");
    let mut guard1 = DaemonGuard::new(child1, data_dir.clone(), false);
    if !await_uds(&socket).await {
        let log_path = data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("Daemon log:\n{log}");
        }
        panic!("daemon did not start");
    }

    let ws_id = "ws-interrupted-test";
    let agent_id = format!("agent-{}", Uuid::new_v4().simple());

    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");

    // Seed workspace + agent session with Active status (stale in-flight).
    {
        use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
        let ts = now_iso();
        store
            .insert_workspace(&workspace_seed(&WorkspaceId(ws_id.to_string())))
            .await
            .expect("insert workspace");

        let session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(agent_id.clone()),
            workspace_id: WorkspaceId(ws_id.to_string()),
            backend_session_id: None,
            acp_session_id: None,
            name: "Interrupted Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Active, // stale in-flight
            is_active: true,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts,
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            messages: vec![],
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            notifications_muted: false,
        };
        store
            .insert_agent_session(&session)
            .await
            .expect("insert agent");
    }

    // Kill daemon to simulate restart.
    guard1.child_mut().kill().expect("kill daemon");
    guard1.child_mut().wait().expect("wait daemon");
    drop(guard1);

    // Phase 2: Restart daemon — heal sweep should insert interrupted_agent row.
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    let mut cmd2 = common::serve_command();
    cmd2.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd2.process_group(0);
    let child2 = cmd2.spawn().expect("spawn intentd serve 2");
    let mut guard2 = DaemonGuard::new(child2, data_dir.clone(), false);
    assert!(await_uds(&socket).await, "daemon did not restart");

    // Fetch fingerprint and port for TLS cert pinning.
    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");

    // Open WSS connection.
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // Phase 3: Call agent.listInterrupted over WSS.
    let result = wss_rpc(&mut ws, 2, "agent.listInterrupted", json!({})).await;

    // Verify the response shape.
    let agents = result["agents"].as_array().expect("agents array");
    assert_eq!(agents.len(), 1, "expected 1 interrupted agent");
    let interrupted = &agents[0];
    assert_eq!(interrupted["agentId"].as_str(), Some(agent_id.as_str()));
    assert_eq!(interrupted["workspaceId"].as_str(), Some(ws_id));
    assert_eq!(
        interrupted["workspaceName"].as_str(),
        Some("WSS-INTERRUPTED")
    );
    assert_eq!(interrupted["agentName"].as_str(), Some("Interrupted Agent"));
    assert_eq!(interrupted["prevStatus"].as_str(), Some("active"));
    assert!(interrupted["interruptedAt"].is_string());

    // Phase 4: Restart again — idempotent insert should not duplicate.
    guard2.child_mut().kill().expect("kill daemon 2");
    guard2.child_mut().wait().expect("wait daemon 2");
    drop(guard2);
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    let mut cmd3 = common::serve_command();
    cmd3.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd3.process_group(0);
    let child3 = cmd3.spawn().expect("spawn intentd serve 3");
    let _guard3 = DaemonGuard::process_only(child3);
    assert!(await_uds(&socket).await, "daemon did not restart 2");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint 2")
        .to_string();
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port 2"))
        .expect("value fits in u16");
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    let result = wss_rpc(&mut ws, 4, "agent.listInterrupted", json!({})).await;
    let agents = result["agents"].as_array().expect("agents array 2");
    assert_eq!(agents.len(), 1, "still 1 interrupted agent (idempotent)");
}

/// Mock-agent gate (parity with the UDS E2E suite).
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

// Re-export the DaemonGuard from common module for use in this file
use common::DaemonGuard;

#[cfg(debug_assertions)]
#[tokio::test]
async fn blocked_exec_stream_stdin_does_not_prevent_shutdown() {
    assert_blocked_exec_stream_shutdown(false).await;
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn blocked_exec_stream_eof_does_not_prevent_shutdown() {
    assert_blocked_exec_stream_shutdown(true).await;
}

#[cfg(debug_assertions)]
async fn assert_blocked_exec_stream_shutdown(eof: bool) {
    struct StreamCleanup(i32);
    impl Drop for StreamCleanup {
        fn drop(&mut self) {
            // Fallback owns only the process group identified by this fixture.
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(self.0),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
    let dir = temp_data_dir();
    let data = dir.path();
    let socket = data.join("intentd.sock");
    let log_path = data.join("daemon.log");
    let pid_path = data.join("stream.pid");
    common::enable_ws_api(data);
    let mut command = common::serve_command();
    command
        .env("INTENTD_DATA_DIR", data)
        .env("INTENTD_WORKSPACES_DIR", data.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_SECRETS_FILE", data.join("secrets.json"))
        .env("INTENTD_TEST_EXEC_STREAM_WRITE_PENDING", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&log_path).unwrap()));
    command.process_group(0);
    let mut daemon = DaemonGuard::new(command.spawn().unwrap(), data.to_path_buf(), false);
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let stream = wss_rpc(
        &mut rpc,
        1,
        "host.execStream",
        json!({
            // timing-guard: keep the non-reading child alive beyond shutdown's assertion budget; shutdown must reap it.
            "command":"sh", "args":["-c", "echo $$ > \"$1\"; exec sleep 600", "sh", pid_path]
        }),
    )
    .await;
    let id = stream["requestId"].as_str().unwrap();
    let pid: i32 = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = text.trim().parse() {
                    break pid;
                }
            }
            // timing-guard: wait for this child's observable PID publication.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let _stream_cleanup = StreamCleanup(pid);
    let duplicate_path = data.join("duplicate-stream-started");
    rpc.send(Message::Text(
        json!({"jsonrpc":"2.0", "id":100,
        "method":"host.execStream", "params":{
            "requestId":id, "command":"sh",
            "args":["-c", "echo started > \"$1\"", "sh", duplicate_path]
        }})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Message::Text(text) = rpc.next().await.unwrap().unwrap() {
                let frame: Value = serde_json::from_str(&text).unwrap();
                if frame["id"] == 100 {
                    assert_eq!(frame["jsonrpc"], "2.0");
                    assert_eq!(frame["error"]["code"], -32602);
                    assert!(frame["error"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("already active"));
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    for request in 2..50 {
        rpc.send(Message::Text(
            json!({"jsonrpc":"2.0", "id":request,
            "method":"host.execStream.write", "params":{"requestId":id,"stdin":"x".repeat(65536)}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    }
    if eof {
        rpc.send(Message::Text(
            json!({"jsonrpc":"2.0", "id":50,
            "method":"host.execStream.write", "params":{"requestId":id,"eof":true}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    }
    let pending_message = if eof {
        "host.execStream stdin EOF send pending"
    } else {
        "host.execStream stdin send pending"
    };
    timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read_to_string(&log_path)
                .unwrap()
                .contains(pending_message)
            {
                break;
            }
            // timing-guard: await the actual bounded stdin send returning Pending in the admitted RPC.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        uds_rpc(&socket, 60, "system.shutdown", json!({})).await["result"]["ok"],
        true
    );
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = daemon.child_mut().try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            // timing-guard: poll the real daemon exit after shutdown was acknowledged.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("request drain waited for stdin before cancelling its non-reading child");
    assert_eq!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH)
    );
    assert!(!duplicate_path.exists(), "rejected duplicate executed");
    let store = intent_store::Store::open(&data.join("intentd.db"))
        .await
        .unwrap();
    let events = store
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    let exits: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "host:exec:exit" && event.data["requestId"] == id)
        .collect();
    assert_eq!(exits.len(), 1);
    assert_eq!(exits[0].data["cancelled"], true);
    store.close().await;
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn cancelled_settings_hook_joins_before_main_listener_teardown() {
    use tokio::io::AsyncReadExt;
    let dir = temp_data_dir();
    let data = dir.path();
    let socket = data.join("intentd.sock");
    let response_path = data.join("response-gate.sock");
    let start_path = data.join("start-gate.sock");
    let responses = tokio::net::UnixListener::bind(&response_path).unwrap();
    let starts = tokio::net::UnixListener::bind(&start_path).unwrap();
    let log_path = data.join("daemon.log");
    let mut command = common::serve_command();
    command
        .env("INTENTD_DATA_DIR", data)
        .env("INTENTD_WORKSPACES_DIR", data.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TEST_SETTINGS_RESPONSE_GATE", response_path)
        .env("INTENTD_TEST_SETTINGS_WS_START_GATE", start_path)
        .env("INTENTD_SECRETS_FILE", data.join("secrets.json"))
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&log_path).unwrap()));
    command.process_group(0);
    let mut daemon = DaemonGuard::new(command.spawn().unwrap(), data.to_path_buf(), false);
    let (mut response, _) = timeout(common::daemon_startup_timeout(), responses.accept())
        .await
        .unwrap()
        .unwrap();
    let (mut start, _) = timeout(common::daemon_startup_timeout(), starts.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(await_uds(&socket).await);
    response.write_u8(1).await.unwrap();
    assert_eq!(
        response.read_u8().await.unwrap(),
        1,
        "response future must be cancelled before shutdown"
    );
    assert_eq!(
        uds_rpc(&socket, 1, "system.shutdown", json!({})).await["result"]["ok"],
        true
    );
    timeout(Duration::from_secs(10), async {
        loop {
            let log = std::fs::read_to_string(&log_path).unwrap();
            assert!(
                !log.contains("phase=\"wss_stop\""),
                "main stopped WSS before joining the cancelled settings owner: {log}"
            );
            if log.contains("phase=\"request_drain\" state=\"completed\"") {
                break;
            }
            assert!(daemon.child_mut().try_wait().unwrap().is_none());
            // timing-guard: poll the observable request-drain log while the startup barrier remains held.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // The held hook has not even captured stop_generation. It could start a
    // fresh listener after final stop if main moved its settings join later.
    assert!(daemon.child_mut().try_wait().unwrap().is_none());
    start.write_u8(1).await.unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = daemon.child_mut().try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            // timing-guard: poll std::process::Child::try_wait without blocking the async test runtime.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let log = std::fs::read_to_string(&log_path).unwrap();
    let drained = log
        .find("phase=\"request_drain\" state=\"completed\"")
        .unwrap();
    let listening = log.find("intentd WSS listening").unwrap();
    let stopped = log.find("phase=\"wss_stop\" state=\"completed\"").unwrap();
    let closed = log
        .find("phase=\"store_close\" state=\"completed\"")
        .unwrap();
    assert!(
        drained < listening && listening < stopped && stopped < closed,
        "{log}"
    );
    let store = intent_store::Store::open(&data.join("intentd.db"))
        .await
        .unwrap();
    assert!(store
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(|event| event.event_type == "settings:changed"
            && event.data["changes"]
                .as_array()
                .is_some_and(|changes| changes
                    .iter()
                    .any(|change| change["path"] == "server.wsApi.enabled"
                        && change["value"] == true))));
    store.close().await;
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn cancelled_mcp_enable_joins_before_main_hub_teardown() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    let dir = temp_data_dir();
    let data = dir.path();
    let socket = data.join("intentd.sock");
    let response_path = data.join("response-gate.sock");
    let start_path = data.join("mcp-start-gate.sock");
    let responses = tokio::net::UnixListener::bind(&response_path).unwrap();
    let starts = tokio::net::UnixListener::bind(&start_path).unwrap();
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    let wrapper = data.join("held-mcp.mjs");
    std::fs::write(
        &wrapper,
        format!(
            r"
import net from 'node:net';
const gate = net.createConnection(process.env.MOCK_MCP_START_GATE);
gate.on('connect', () => gate.write(JSON.stringify([process.pid, process.env.INTENTD_SECRETS_FILE]) + '\n'));
await new Promise(resolve => gate.once('data', resolve));
gate.end();
await import({});
",
            serde_json::to_string(&format!("file://{fixture}")).unwrap()
        ),
    )
    .unwrap();
    let config = json!({"id":"shutdown-held","transport":"stdio","command":"node","args":[wrapper],"env":{"MOCK_MCP_START_GATE":start_path,"INTENTD_SECRETS_FILE":data.join("secrets.json")},"enabled":false});
    let log_path = data.join("daemon.log");
    let mut command = common::serve_command();
    command
        .env("INTENTD_DATA_DIR", data)
        .env("INTENTD_SECRETS_FILE", data.join("secrets.json"))
        .env("INTENTD_WORKSPACES_DIR", data.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TEST_SETTINGS_RESPONSE_GATE", response_path)
        .env("INTENTD_TEST_MCP_ENABLE_CONFIG", config.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::from(std::fs::File::create(&log_path).unwrap()));
    command.process_group(0);
    let mut daemon = DaemonGuard::new(command.spawn().unwrap(), data.to_path_buf(), false);
    let (mut response, _) = timeout(common::daemon_startup_timeout(), responses.accept())
        .await
        .unwrap()
        .unwrap();
    let (start, _) = timeout(common::daemon_startup_timeout(), starts.accept())
        .await
        .unwrap()
        .unwrap();
    let mut start = BufReader::new(start);
    let mut pid = String::new();
    timeout(Duration::from_secs(5), start.read_line(&mut pid))
        .await
        .unwrap()
        .unwrap();
    let child_identity: Value = serde_json::from_str(pid.trim()).unwrap();
    assert_eq!(
        child_identity[1],
        data.join("secrets.json").to_str().unwrap()
    );
    let pid = i32::try_from(child_identity[0].as_i64().unwrap()).unwrap();
    assert!(await_uds(&socket).await);
    response.write_u8(1).await.unwrap();
    assert_eq!(response.read_u8().await.unwrap(), 1);
    assert_eq!(
        uds_rpc(&socket, 1, "system.shutdown", json!({})).await["result"]["ok"],
        true
    );
    timeout(Duration::from_secs(5), async {
        loop {
            let log = std::fs::read_to_string(&log_path).unwrap();
            assert!(
                !log.contains("phase=\"mcp_shutdown\""),
                "MCP stop passed held enable: {log}"
            );
            if log.contains("phase=\"request_drain\" state=\"completed\"") {
                break;
            }
            assert!(daemon.child_mut().try_wait().unwrap().is_none());
            // timing-guard: poll the observable request-drain log while the startup barrier remains held.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Child exists but is not registered in the hub until initialize completes.
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok());
    start.get_mut().write_u8(1).await.unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = daemon.child_mut().try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            // timing-guard: poll std::process::Child::try_wait without blocking the async test runtime.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH),
        "MCP child escaped final hub reap"
    );
    let log = std::fs::read_to_string(&log_path).unwrap();
    let stopped = log
        .find("phase=\"mcp_shutdown\" state=\"completed\"")
        .unwrap();
    let closed = log
        .find("phase=\"store_close\" state=\"completed\"")
        .unwrap();
    assert!(stopped < closed, "{log}");
    let store = intent_store::Store::open(&data.join("intentd.db"))
        .await
        .unwrap();
    let events = store
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "mcp.servers:status-changed"
                && event.data["serverId"] == "shutdown-held"
                && event.data["status"]["state"] == "running"),
        "{events:?}"
    );
    store.close().await;
}

#[tokio::test]
async fn graceful_shutdown_captures_interrupted_agents() {
    let Some(script) = gate("graceful_shutdown_captures_interrupted_agents") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let listen = "both";
    let socket = data_dir.join("intentd.sock");
    let ws_id = "ws-graceful-test";

    // Pre-seed workspace so agent.create RPC can succeed.
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

    // Phase 1: Boot daemon with mock ACP provider.
    // blockUntilCancel makes the mock stream a chunk then park the prompt unresolved,
    // keeping the agent in the busy set until session/cancel (which shutdown will send).
    let behavior = json!({
        "blockUntilCancel": true
    })
    .to_string();
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    // Pin resumeInterruptedOnStart=off: this suite asserts the captured row
    // is still pending after restart, but `auto` resumes on headless hosts.
    common::disable_resume_on_start(&data_dir);
    let mut cmd1 = common::serve_command();
    cmd1.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("MOCK_AGENT_SCRIPT_PATH", &script)
        .env("MOCK_AGENT_BEHAVIOR", &behavior)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd1.process_group(0);
    let child = cmd1.spawn().expect("spawn intentd serve");
    let mut daemon = DaemonGuard::new(child, data_dir.clone(), false);
    if !await_uds(&socket).await {
        let log_path = data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("Daemon log:\n{log}");
        }
        panic!("daemon did not start");
    }

    // Fetch fingerprint and actual bound port for TLS cert pinning.
    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");

    // Open event subscriber BEFORE creating the agent so we miss no events.
    let cfg = client_config(&fp);
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "script:state"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    // RPC conn — create agent and send a message to make it in-flight (Active).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Graceful Agent", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "start" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the agent to be observably in-flight: blockUntilCancel streams a chunk
    // then parks, so seeing agent:stream:activity means the agent is in the busy set.
    let mut saw_chunk = false;
    for _ in 0..20 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:activity" {
            saw_chunk = true;
            break;
        }
    }
    assert!(
        saw_chunk,
        "agent did not stream chunk (mid-turn capture relies on this)"
    );

    // Keep an idle WSS peer connected and a no-timeout command waiting for
    // stdin. Neither may prevent shutdown; the command's terminal event must
    // become durable before the store closes.
    let _idle_peer = connect_ws(port, cfg.clone()).await;
    let stream = wss_rpc(
        &mut rpc,
        14,
        "host.execStream",
        json!({"workspaceId": ws_id, "command": "cat"}),
    )
    .await;
    let stream_id = stream["requestId"]
        .as_str()
        .expect("stream request id")
        .to_string();

    let script_def = wss_rpc(
        &mut rpc,
        15,
        "script.create",
        json!({
            "workspaceId": ws_id, "name": "Shutdown held command", "mode": "command",
            "purpose": "oneOff", "command": "cat"
        }),
    )
    .await;
    let script_id = script_def["id"].as_str().unwrap().to_string();
    // Send without awaiting the response: this request stays admitted while
    // its PTY waits on stdin, with no timeout to release request_drain.
    rpc.send(Message::Text(
        json!({"jsonrpc":"2.0","id":16,"method":"script.run",
        "params":{"workspaceId":ws_id,"scriptId":script_id}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    timeout(Duration::from_secs(20), async {
        loop {
            let frame = wss_event(&mut sub, 20).await;
            let event = &frame["params"]["event"];
            if event["type"] == "script:state"
                && event["data"]["scriptId"] == script_id
                && event["data"]["status"] == "running"
            {
                break;
            }
        }
    })
    .await
    .expect("script.run must be admitted and running before shutdown");

    // Now trigger graceful shutdown via system.shutdown RPC over UDS (system.*
    // is UDS-only; see PROTOCOL §5.7).
    let shutdown_result = uds_rpc(&socket, 13, "system.shutdown", json!({})).await;
    assert_eq!(shutdown_result["result"].get("ok"), Some(&json!(true)));
    assert_eq!(
        shutdown_result["result"].get("stopping"),
        Some(&json!(true))
    );

    // Wait for daemon to exit gracefully (up to 10 seconds).
    // daemon.child_mut().wait() is blocking, so we poll for process death.
    let exit_ok = timeout(Duration::from_secs(10), async {
        loop {
            match daemon.child_mut().try_wait() {
                Ok(Some(status)) => return status.success(),
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("failed to wait for daemon: {e}"),
            }
        }
    })
    .await
    .expect("daemon did not exit within timeout");
    // Graceful shutdown should exit 0.
    assert!(exit_ok, "daemon exited non-zero");
    // Explicitly drop the first Daemon so its Drop guard doesn't kill data_dir cleanup.
    std::mem::drop(daemon);

    let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap();
    assert!(
        !log.contains("attempted to acquire a connection on a closed pool"),
        "late store writer: {log}"
    );
    {
        let store = intent_store::Store::open(&data_dir.join("intentd.db"))
            .await
            .unwrap();
        let rows = store
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(intent_core::WorkspaceId::from(ws_id)),
                ..Default::default()
            })
            .await
            .unwrap();
        let exits = rows
            .iter()
            .filter(|event| {
                event.event_type == "host:exec:exit" && event.data["requestId"] == stream_id
            })
            .collect::<Vec<_>>();
        assert_eq!(
            exits.len(),
            1,
            "stream terminal event must survive shutdown"
        );
        assert_eq!(exits[0].data["cancelled"], true);
        assert!(
            rows.iter().any(|event| event.event_type == "script:state"
                && event.data["scriptId"] == script_id
                && event.data["status"] == "exited"),
            "terminal script state survives reopen"
        );
        let definition = store
            .get_script_in_workspace(&intent_core::WorkspaceId::from(ws_id), &script_id)
            .await
            .unwrap()
            .unwrap();
        assert!(definition.last_run.is_none());
        assert!(
            store
                .pending_script_runs()
                .await
                .unwrap()
                .iter()
                .any(|(workspace, id, _, started)| workspace.as_str() == ws_id
                    && id == &script_id
                    && started.is_some()),
            "running command retains its durable admission for restart recovery"
        );
        store.close().await;
    }

    // Phase 2: Restart daemon — should list the interrupted agent.
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    let mut cmd2 = common::serve_command();
    cmd2.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon2.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd2.process_group(0);
    let child2 = cmd2.spawn().expect("spawn intentd serve 2");
    let _daemon2 = DaemonGuard::process_only(child2);
    assert!(await_uds(&socket).await, "daemon did not restart");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint 2")
        .to_string();
    let port2 = u16::try_from(status["result"]["port"].as_u64().expect("port 2"))
        .expect("value fits in u16");
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port2, cfg).await;

    let scripts = wss_rpc(&mut ws, 17, "script.list", json!({"workspaceId":ws_id})).await;
    let recovered = scripts["scripts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|script| script["id"] == script_id)
        .expect("interrupted script remains in history");
    assert_eq!(recovered["lastRun"]["outcome"], "interrupted");

    // Phase 3: Call agent.listInterrupted over WSS.
    let result = wss_rpc(&mut ws, 4, "agent.listInterrupted", json!({})).await;

    // Verify the response shape.
    let agents = result["agents"].as_array().expect("agents array");
    assert_eq!(
        agents.len(),
        1,
        "expected 1 interrupted agent after graceful shutdown"
    );
    let interrupted = &agents[0];
    assert_eq!(interrupted["agentId"].as_str(), Some(agent_id.as_str()));
    assert_eq!(interrupted["workspaceId"].as_str(), Some(ws_id));
    assert_eq!(interrupted["agentName"].as_str(), Some("Graceful Agent"));
    assert_eq!(
        interrupted["prevStatus"].as_str(),
        Some("active"),
        "graceful shutdown should capture Active status before settling to RuntimeIdle"
    );
    assert!(interrupted["interruptedAt"].is_string());

    // Daemon Drop guard will kill the process and clean up data_dir.
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-INTERRUPTED".to_string(),
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
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    }
}
