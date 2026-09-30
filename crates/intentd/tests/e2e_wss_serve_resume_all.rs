//! WSS end-to-end for `intentd serve --resume-all` (headless auto-resume).
//!
//! Boots daemon1 with a working mock-ACP agent, interrupts it (kill daemon),
//! then boots daemon2 with `--resume-all` on the same data dir. Verifies that
//! the agent resumes WITHOUT any `agent.resolveInterrupted` RPC: continuation
//! turn runs, agent completes, observable over WSS.
//!
//! Coverage:
//! - `--resume-all` auto-resumes all pending interrupted agents at startup
//! - Resumed agents complete their work (observable via WSS events)
//! - Listeners serve while recovery runs: reserved automatic candidates stay
//!   hidden from `agent.listInterrupted` (no client-visible dialog blip)
//! - No `agent.resolveInterrupted` RPC required
//! - `agents.resumeInterruptedOnStart=on` (written over WSS via
//!   `settings.update`) runs the same sweep WITHOUT `--resume-all`, even when
//!   a display is present

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Short base under /tmp (UDS `SUN_LEN` cap); the returned guard removes the
/// dir on drop — hold it for the full test (`INTENTD_TEST_KEEP_TMP` keeps it).
fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wra-")
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

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("safe defaults")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider: provider.clone(),
        }))
        .with_no_client_auth()
}

async fn connect_ws(
    port: u16,
    cfg: ClientConfig,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, Arc::new(cfg), &url).await
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
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

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)], resume_all: bool) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    if resume_all {
        cmd.arg("--resume-all");
    }
    if listen == "insecure" {
        cmd.arg("--insecure");
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Spawn in its own process group (pgid == child pid) so killing the daemon on
    // test panic/failure also kills spawned Node.js ACP mock providers via killpg.
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.spawn().expect("spawn intentd serve")
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "RESUME-ALL-E2E".to_string(),
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
        diff_summary: None,
    }
}

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill the whole process group FIRST (daemon + any Node.js ACP provider
        // children) BEFORE wait(), so children are reaped before they get reparented.
        // The daemon was spawned with process_group(0), making it the group leader.
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        // On test panic, print data-dir path + daemon log tail for diagnosability
        if std::thread::panicking() {
            eprintln!("\n=== DAEMON CLEANUP (test panicked) ===");
            eprintln!("Data dir: {}", self.data_dir.display());
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                let lines: Vec<_> = log.lines().rev().take(30).collect();
                eprintln!("Last 30 lines of daemon.log:");
                for line in lines.iter().rev() {
                    eprintln!("  {line}");
                }
            }
        }
    }
}

fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

#[tokio::test]
async fn serve_resume_all_auto_resumes_interrupted_agents() {
    let Some(script) = gate("serve_resume_all_auto_resumes_interrupted_agents") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();

    // Simple mock behavior: just respond with a message
    let behavior = json!({
        "response": "Agent resumed and completed!"
    })
    .to_string();

    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];

    // Phase 1: Boot daemon1, create workspace and agent, then interrupt it
    eprintln!("Phase 1: Boot daemon1 and create interrupted agent");
    let child1 = spawn_serve(&data_dir, "both", &env, false);
    let daemon1 = Daemon {
        child: child1,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon1 did not start");

    // Seed workspace and create an agent session
    let ws_id = {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace_seed(&ws))
            .await
            .expect("insert ws");
        ws.0
    };

    // Create the agent via RPC
    let create_result = uds_rpc(
        &socket,
        1,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Test Agent",
            "model": "default", "provider": "mock"
        }),
    )
    .await;
    let created_agent_id = create_result["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Send a message to the agent to make it active
    let _ = uds_rpc(
        &socket,
        2,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": created_agent_id,
            "content": "Start working"
        }),
    )
    .await;

    // Give it a moment to start processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Manually insert an interrupted_agent row (simulating daemon crash)
    {
        use intent_core::{now_iso, AgentId, WorkspaceId};
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        store
            .insert_interrupted_agent(
                &AgentId(created_agent_id.clone()),
                &WorkspaceId(ws_id.clone()),
                "active",
                &now_iso(),
            )
            .await
            .expect("insert interrupted agent");
    }

    eprintln!("Killing daemon1 to simulate interruption");
    drop(daemon1); // Kill daemon1

    // Phase 2: Boot daemon2 with --resume-all and subscribe to agent events
    eprintln!("Phase 2: Boot daemon2 with --resume-all");
    tokio::time::sleep(Duration::from_secs(1)).await; // Let daemon1 fully die

    let child2 = spawn_serve(&data_dir, "both", &env, true);
    let _daemon2 = Daemon {
        child: child2,
        data_dir: data_dir.clone(),
    };

    assert!(await_uds(&socket).await, "daemon2 did not start");

    // Get system status to retrieve port and fingerprint for WSS
    let status = common::await_wss_status(&socket).await;
    let actual_port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    // Automatic candidates are reserved before listeners start, so the first
    // RPC sees no manual-recovery candidates even if the sweep is unfinished.
    eprintln!("Phase 3: Assert listInterrupted is empty on first connect");
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(actual_port, cfg).await;
    let list_result = wss_rpc(&mut ws, 1, "agent.listInterrupted", json!({})).await;
    let agents = list_result["agents"].as_array().expect("agents array");
    assert!(
        agents.is_empty(),
        "expected agent.listInterrupted to be empty on first connect after a \
         resuming start, got {agents:?}"
    );
    eprintln!("✓ Interrupted agents list is empty on first connect");

    wait_for_sweep(&data_dir).await;

    // Phase 4: Poll agent status to confirm turn completion (agent reached idle)
    eprintln!("Phase 4: Poll agent status to confirm turn completion");
    // Poll agent.get to wait for the agent to complete its turn and reach idle
    eprintln!("Polling agent.get until agent is idle (bounded to 30s)...");
    let mut agent_is_idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let agent_result = wss_rpc(
            &mut ws,
            2,
            "agent.get",
            json!({
                "workspaceId": ws_id,
                "agentId": created_agent_id
            }),
        )
        .await;

        let is_active = agent_result["agent"]["isActive"].as_bool().unwrap_or(false);
        let status = agent_result["agent"]["status"].as_str().unwrap_or("");

        eprintln!("Agent status: {status}, isActive: {is_active}");

        // Agent has completed its turn when it's not active (isActive=false)
        // The status field uses lowercase: "idle", "complete", etc.
        if !is_active {
            eprintln!("✓ Agent reached idle state after resume (isActive=false)");
            agent_is_idle = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        agent_is_idle,
        "Expected agent {created_agent_id} to complete turn and reach idle, but timed out"
    );

    eprintln!("SUCCESS: --resume-all auto-resumed agent AND agent completed its turn");
}

/// `agents.resumeInterruptedOnStart=on` gates the startup sweep without
/// `--resume-all`: the setting is written over the real WSS transport
/// (`settings.update`), the daemon is killed with an interrupted agent
/// pending, and the restarted daemon — no `--resume-all` flag, DISPLAY set so
/// `auto` would NOT resume — sweeps and resumes the agent anyway.
#[tokio::test]
async fn setting_on_resumes_without_resume_all_flag() {
    let Some(script) = gate("setting_on_resumes_without_resume_all_flag") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();

    let behavior = json!({
        "response": "Agent resumed and completed!"
    })
    .to_string();

    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];

    // Phase 1: Boot daemon1, create workspace and agent, then interrupt it
    eprintln!("Phase 1: Boot daemon1 and create interrupted agent");
    let child1 = spawn_serve(&data_dir, "both", &env, false);
    let daemon1 = Daemon {
        child: child1,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon1 did not start");

    let ws_id = {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace_seed(&ws))
            .await
            .expect("insert ws");
        ws.0
    };

    let create_result = uds_rpc(
        &socket,
        1,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Test Agent",
            "model": "default", "provider": "mock"
        }),
    )
    .await;
    let created_agent_id = create_result["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let _ = uds_rpc(
        &socket,
        2,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": created_agent_id,
            "content": "Start working"
        }),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Flip the setting to `on` over the real WSS transport — this is the wire
    // path production clients use, and it persists to config.toml for daemon2.
    eprintln!("Setting agents.resumeInterruptedOnStart=on over WSS");
    {
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().expect("port"))
            .expect("value fits in u16");
        let fingerprint = status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint")
            .to_string();
        let cfg = client_config(&fingerprint);
        let mut ws = connect_ws(port, cfg).await;
        let result = wss_rpc(
            &mut ws,
            1,
            "settings.update",
            json!({ "changes": [
                { "path": "agents.resumeInterruptedOnStart", "value": "on" }
            ] }),
        )
        .await;
        let applied = result["applied"].as_array().expect("applied array");
        assert_eq!(applied.len(), 1, "{result}");
        assert_eq!(applied[0]["path"], json!("agents.resumeInterruptedOnStart"));
        assert_eq!(applied[0]["value"], json!("on"));
    }

    // Manually insert an interrupted_agent row (simulating daemon crash)
    {
        use intent_core::{now_iso, AgentId, WorkspaceId};
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        store
            .insert_interrupted_agent(
                &AgentId(created_agent_id.clone()),
                &WorkspaceId(ws_id.clone()),
                "active",
                &now_iso(),
            )
            .await
            .expect("insert interrupted agent");
    }

    eprintln!("Killing daemon1 to simulate interruption");
    drop(daemon1);

    // Phase 2: Boot daemon2 WITHOUT --resume-all. DISPLAY is set so
    // `detect_has_display()` is true: `auto` would skip the sweep, proving a
    // resume here is the `on` setting and not the headless heuristic.
    eprintln!("Phase 2: Boot daemon2 without --resume-all (setting=on, DISPLAY set)");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let env2: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("DISPLAY", ":99"),
    ];
    let child2 = spawn_serve(&data_dir, "both", &env2, false);
    let _daemon2 = Daemon {
        child: child2,
        data_dir: data_dir.clone(),
    };

    assert!(await_uds(&socket).await, "daemon2 did not start");

    let status = common::await_wss_status(&socket).await;
    let actual_port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    // Automatic candidates are reserved before listeners start, so the first
    // RPC sees no manual-recovery candidates even if the sweep is unfinished.
    eprintln!("Phase 3: Assert listInterrupted is empty on first connect");
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(actual_port, cfg).await;
    let list_result = wss_rpc(&mut ws, 1, "agent.listInterrupted", json!({})).await;
    let agents = list_result["agents"].as_array().expect("agents array");
    assert!(
        agents.is_empty(),
        "expected agent.listInterrupted to be empty on first connect after a \
         resuming start, got {agents:?}"
    );
    eprintln!("✓ Interrupted agents list is empty on first connect");

    wait_for_sweep(&data_dir).await;

    // Phase 4: Poll agent status to confirm turn completion (agent reached idle)
    eprintln!("Phase 4: Poll agent status to confirm turn completion");
    eprintln!("Polling agent.get until agent is idle (bounded to 30s)...");
    let mut agent_is_idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let agent_result = wss_rpc(
            &mut ws,
            2,
            "agent.get",
            json!({
                "workspaceId": ws_id,
                "agentId": created_agent_id
            }),
        )
        .await;

        let is_active = agent_result["agent"]["isActive"].as_bool().unwrap_or(false);
        let status = agent_result["agent"]["status"].as_str().unwrap_or("");
        eprintln!("Agent status: {status}, isActive: {is_active}");
        if !is_active {
            eprintln!("✓ Agent reached idle state after resume (isActive=false)");
            agent_is_idle = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        agent_is_idle,
        "Expected agent {created_agent_id} to complete turn and reach idle, but timed out"
    );

    eprintln!("SUCCESS: setting=on auto-resumed agent without --resume-all");
}

struct RecoveryFixture {
    root: tempfile::TempDir,
    store: intent_store::Store,
    agents: Vec<intent_core::AgentId>,
    script: String,
}

impl RecoveryFixture {
    async fn new() -> Self {
        let script = gate("startup recovery").unwrap();
        let root = temp_data_dir();
        let data_dir = root.path();
        let socket = data_dir.join("intentd.sock");
        let env = [
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ];
        let daemon = Daemon {
            child: spawn_serve(data_dir, "both", &env, false),
            data_dir: data_dir.to_owned(),
        };
        assert!(await_uds(&socket).await);
        let store = intent_store::Store::open(&data_dir.join("intentd.db"))
            .await
            .unwrap();
        let workspace = intent_core::WorkspaceId::new();
        store
            .insert_workspace(&workspace_seed(&workspace))
            .await
            .unwrap();
        let mut agents = Vec::new();
        for name in ["First", "Second"] {
            let created = uds_rpc(&socket, 1, "agent.create", json!({"workspaceId": workspace, "name": name, "provider": "mock", "model": "default"})).await;
            agents.push(intent_core::AgentId::from(
                created["result"]["agent"]["id"].as_str().unwrap(),
            ));
        }
        let setting = uds_rpc(
            &socket,
            2,
            "settings.update",
            json!({"changes": [{"path": "agents.resumeInterruptedOnStart", "value": "off"}]}),
        )
        .await;
        assert!(setting["error"].is_null(), "{setting}");
        drop(daemon);
        for agent in &agents {
            store
                .insert_interrupted_agent(agent, &workspace, "active", &intent_core::now_iso())
                .await
                .unwrap();
        }
        Self {
            root,
            store,
            agents,
            script,
        }
    }

    fn start(&self, resume: bool) -> (Daemon, tokio::net::UnixListener) {
        self.start_mode(resume, "both")
    }

    fn start_mode(&self, resume: bool, mode: &str) -> (Daemon, tokio::net::UnixListener) {
        let data_dir = self.root.path();
        let gate_path = data_dir.join("recovery-gate.sock");
        let gate = tokio::net::UnixListener::bind(&gate_path).unwrap();
        let env = [
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", self.script.as_str()),
            (
                "INTENTD_TEST_STARTUP_RESUME_GATE",
                gate_path.to_str().unwrap(),
            ),
        ];
        (
            Daemon {
                child: spawn_serve(data_dir, mode, &env, resume),
                data_dir: data_dir.to_owned(),
            },
            gate,
        )
    }

    async fn wss(&self) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
        let socket = self.root.path().join("intentd.sock");
        assert!(await_uds(&socket).await);
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        connect_ws(
            port,
            client_config(status["result"]["fingerprint"].as_str().unwrap()),
        )
        .await
    }
}

async fn wait_for_sweep(data_dir: &Path) {
    timeout(common::rpc_read_timeout(), async {
        loop {
            let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap();
            if log.contains("resume-on-start: auto-resume sweep complete") {
                return;
            }
            // timing-guard: poll an observable sweep-completion barrier
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sweep completes after release");
}

/// The gate holds the actual startup sweep, rather than a provider turn (which
/// already runs in the background). Neither transport may depend on its release.
#[tokio::test]
async fn startup_recovery_serves_rpcs_while_sweep_is_held() {
    let fixture = RecoveryFixture::new().await;
    let (_daemon, gate) = fixture.start(true);
    let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
        .await
        .expect("sweep reached gate")
        .unwrap();
    timeout(Duration::from_secs(5), async {
        let mut ws = fixture.wss().await;
        let pending = wss_rpc(&mut ws, 1, "agent.listInterrupted", json!({})).await;
        assert_eq!(
            pending["agents"],
            json!([]),
            "automatic candidates must not flash in the manual recovery dialog"
        );
        let workspaces = wss_rpc(&mut ws, 2, "workspace.list", json!({})).await;
        assert!(workspaces.is_object());
    })
    .await
    .expect("UDS and authenticated WSS RPCs must finish before releasing recovery");
    assert_eq!(
        fixture.store.list_interrupted_agents().await.unwrap().len(),
        2,
        "the sweep is still held"
    );
    release.write_u8(1).await.unwrap();
    wait_for_sweep(fixture.root.path()).await;
    assert!(fixture
        .store
        .list_interrupted_agents()
        .await
        .unwrap()
        .is_empty());
    fixture.store.close().await;
}

#[tokio::test]
async fn startup_recovery_disabled_keeps_candidates_visible() {
    let fixture = RecoveryFixture::new().await;
    let (_daemon, _gate) = fixture.start(false);
    let mut ws = fixture.wss().await;
    let pending = wss_rpc(&mut ws, 1, "agent.listInterrupted", json!({})).await;
    assert_eq!(pending["agents"].as_array().unwrap().len(), 2);
    let log = std::fs::read_to_string(fixture.root.path().join("daemon.log")).unwrap();
    assert!(!log.contains("resume-on-start: resuming interrupted agents"));
    fixture.store.close().await;
}

#[tokio::test]
async fn startup_recovery_manual_resolution_wins_before_automatic_claim() {
    let fixture = RecoveryFixture::new().await;
    let (_daemon, gate) = fixture.start(true);
    let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = fixture.wss().await;
    let result = wss_rpc(
        &mut ws,
        1,
        "agent.resolveInterrupted",
        json!({"resume": [fixture.agents[0]], "abandon": [fixture.agents[1]]}),
    )
    .await;
    assert_eq!(result["resumed"], json!([fixture.agents[0]]));
    assert_eq!(result["abandoned"], json!([fixture.agents[1]]));
    release.write_u8(1).await.unwrap();
    wait_for_sweep(fixture.root.path()).await;
    for (i, id) in fixture.agents.iter().enumerate() {
        let session = fixture.store.get_agent_session(id).await.unwrap();
        assert_eq!(
            session.messages.iter().filter(|m| m.role == "user").count(),
            usize::from(i == 0),
            "automatic recovery cannot duplicate or undo manual resolution"
        );
    }
    fixture.store.close().await;
}

#[tokio::test]
async fn startup_recovery_shutdown_releases_unclaimed_candidates() {
    let fixture = RecoveryFixture::new().await;
    let (mut daemon, gate) = fixture.start(true);
    let (_release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = fixture.wss().await;
    assert_eq!(
        wss_rpc(&mut ws, 1, "agent.listInterrupted", json!({})).await["agents"],
        json!([])
    );
    let stopped = uds_rpc(
        &fixture.root.path().join("intentd.sock"),
        2,
        "system.shutdown",
        json!({}),
    )
    .await;
    assert!(stopped["error"].is_null(), "{stopped}");
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if let Some(status) = daemon.child.try_wait().unwrap() {
                assert!(status.success(), "shutdown failed: {status}");
                break;
            }
            // timing-guard: wait for graceful daemon exit while recovery stays held
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("shutdown does not wait for an unclaimed resume");
    assert_eq!(
        fixture.store.list_interrupted_agents().await.unwrap().len(),
        2
    );
    for agent in &fixture.agents {
        assert!(fixture
            .store
            .get_agent_session(agent)
            .await
            .unwrap()
            .messages
            .is_empty());
    }
    drop(daemon);
    drop(gate);
    std::fs::remove_file(fixture.root.path().join("recovery-gate.sock")).unwrap();
    let (_restarted, _gate) = fixture.start(false);
    let mut reconnected = fixture.wss().await;
    let pending = wss_rpc(&mut reconnected, 3, "agent.listInterrupted", json!({})).await;
    assert_eq!(
        pending["agents"].as_array().unwrap().len(),
        2,
        "unclaimed work survives a restart"
    );
    fixture.store.close().await;
}

#[tokio::test]
async fn startup_recovery_insecure_ws_serves_while_sweep_is_held() {
    let fixture = RecoveryFixture::new().await;
    let (_daemon, gate) = fixture.start_mode(true, "insecure");
    let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), async {
        let socket = fixture.root.path().join("intentd.sock");
        assert!(await_uds(&socket).await);
        let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
        let port = status["result"]["port"].as_u64().unwrap();
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws"))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0", "id": 2, "method":"agent.listInterrupted", "params":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let frame = ws.next().await.unwrap().unwrap();
        let value: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], 2);
        assert_eq!(value["result"]["agents"], json!([]));
    })
    .await
    .expect("insecure WS and UDS ready before recovery release");
    release.write_u8(1).await.unwrap();
    wait_for_sweep(fixture.root.path()).await;
    fixture.store.close().await;
}

#[tokio::test]
async fn startup_recovery_failure_can_be_retried_on_the_same_connection() {
    let fixture = RecoveryFixture::new().await;
    let (_daemon, gate) = fixture.start(true);
    let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = fixture.wss().await;
    let id = &fixture.agents[0];
    wss_rpc(&mut ws, 1, "agent.retire", json!({"agentId": id})).await;
    assert_eq!(
        wss_rpc(&mut ws, 10, "agent.listInterrupted", json!({})).await["agents"],
        json!([])
    );
    let workspace = fixture
        .store
        .get_agent_session(id)
        .await
        .unwrap()
        .workspace_id;
    wss_rpc(
        &mut ws,
        11,
        "events.subscribe",
        json!({"workspaceId": workspace, "eventTypes": ["agent:updated"]}),
    )
    .await;
    release.write_u8(1).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let frame = ws.next().await.unwrap().unwrap();
            if let Message::Text(text) = frame {
                let frame: Value = serde_json::from_str(&text).unwrap();
                let event = &frame["params"]["event"];
                if event["type"] == "agent:updated"
                    && event["data"]["startupRecoveryFailed"] == true
                {
                    assert_eq!(event["data"]["agentId"], json!(id));
                    assert_eq!(event["workspaceId"], json!(workspace));
                    break;
                }
            }
        }
    })
    .await
    .expect("failed recovery must notify the already connected client");
    let pending = wss_rpc(&mut ws, 2, "agent.listInterrupted", json!({})).await;
    assert_eq!(pending["agents"].as_array().unwrap().len(), 1);
    assert_eq!(pending["agents"][0]["agentId"], json!(id));
    wss_rpc(&mut ws, 3, "agent.restore", json!({"agentId": id})).await;
    let retried = wss_rpc(
        &mut ws,
        4,
        "agent.resolveInterrupted",
        json!({"resume": [id]}),
    )
    .await;
    assert_eq!(retried["resumed"], json!([id]));
    assert_eq!(
        wss_rpc(&mut ws, 5, "agent.listInterrupted", json!({})).await["agents"],
        json!([])
    );
    fixture.store.close().await;
}

/// Persisted watch/group bookkeeping is visible while completion delivery is
/// held. The real authenticated WSS transport must answer before release.
#[tokio::test]
async fn startup_completion_recovery_serves_wss_before_waking_parents() {
    for (grouped, stop_while_held) in [(false, false), (true, false), (true, true)] {
        let fixture = RecoveryFixture::new().await;
        let parent = &fixture.agents[0];
        let child = &fixture.agents[1];
        let mut session = fixture
            .store
            .get_agent_session_summary(child)
            .await
            .unwrap();
        let workspace = session.workspace_id.clone();
        session.status = intent_core::AgentStatus::Completed;
        session.completion_report = Some("finished during downtime".into());
        session.completion_report_timestamp = Some(intent_core::now_iso());
        fixture
            .store
            .update_agent_session(&workspace, &session)
            .await
            .unwrap();
        fixture
            .store
            .set_interrupted_resolution(child, "abandoned", &intent_core::now_iso())
            .await
            .unwrap();
        let group_id = grouped.then(|| "startup-group".to_string());
        if let Some(id) = &group_id {
            fixture
                .store
                .upsert_delegation_group(&intent_store::PersistedDelegationGroup {
                    group_id: id.clone(),
                    workspace_id: workspace.clone(),
                    parent_agent_id: parent.clone(),
                    await_mode: "after_all".into(),
                    expected_agent_ids: vec![child.clone()],
                    completed_agent_ids: vec![],
                    deleted_agent_ids: vec![],
                    sealed: true,
                    delivered: false,
                    event_summaries: vec![],
                    raw_events_json: vec![],
                    created_at: intent_core::now_iso(),
                    updated_at: intent_core::now_iso(),
                })
                .await
                .unwrap();
        }
        fixture
            .store
            .upsert_completion_watch(&intent_store::PersistedCompletionWatch {
                id: "startup-watch".into(),
                parent_workspace_id: workspace.clone(),
                child_workspace_id: workspace.clone(),
                parent_agent_id: parent.clone(),
                parent_agent_name: "First".into(),
                child_agent_id: child.clone(),
                group_id,
                report_delivered: false,
                wake_on_attention: false,
                completion_only: false,
                created_at: intent_core::now_iso(),
            })
            .await
            .unwrap();
        let gate_path = fixture.root.path().join("completion-gate.sock");
        let gate = tokio::net::UnixListener::bind(&gate_path).unwrap();
        let provider_log = fixture.root.path().join("provider-sessions.jsonl");
        let env = [
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", fixture.script.as_str()),
            ("MOCK_AGENT_SESSION_LOG", provider_log.to_str().unwrap()),
            (
                "INTENTD_TEST_STARTUP_COMPLETION_GATE",
                gate_path.to_str().unwrap(),
            ),
        ];
        let mut daemon = Daemon {
            child: spawn_serve(fixture.root.path(), "both", &env, true),
            data_dir: fixture.root.path().to_owned(),
        };
        let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
            .await
            .expect("completion recovery reached gate")
            .unwrap();
        let mut ws = fixture.wss().await;
        let status = wss_rpc(&mut ws, 10, "system.status", json!({})).await;
        assert!(status.is_object(), "{status}");
        let pending = wss_rpc(&mut ws, 11, "agent.listInterrupted", json!({})).await;
        assert_eq!(
            pending["agents"],
            json!([]),
            "reservations precede early completion wakes"
        );
        let subscriptions = wss_rpc(
            &mut ws,
            12,
            "agent.getSubscriptions",
            json!({"workspaceId": workspace, "agentId": parent}),
        )
        .await;
        assert_eq!(
            subscriptions["subscriptions"].as_array().unwrap().len(),
            1,
            "watch registry ready before reconciliation"
        );
        assert_eq!(
            subscriptions["delegationGroups"].as_array().unwrap().len(),
            usize::from(grouped),
            "group registry ready before reconciliation"
        );
        assert!(
            !provider_log.exists(),
            "no recovery provider may start before release"
        );
        assert_eq!(
            fixture.store.list_completion_watches().await.unwrap().len(),
            1
        );
        if stop_while_held {
            let stopped = uds_rpc(
                &fixture.root.path().join("intentd.sock"),
                13,
                "system.shutdown",
                json!({}),
            )
            .await;
            assert!(stopped["error"].is_null(), "{stopped}");
            timeout(common::daemon_startup_timeout(), async {
                loop {
                    if let Some(status) = daemon.child.try_wait().unwrap() {
                        assert!(status.success(), "{status}");
                        break;
                    }
                    // timing-guard: observe graceful shutdown while completion recovery is held
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("shutdown cancels unadmitted completion recovery");
            assert_eq!(
                fixture.store.list_completion_watches().await.unwrap().len(),
                1
            );
            assert_eq!(
                fixture
                    .store
                    .list_undelivered_groups(&workspace)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                fixture.store.list_interrupted_agents().await.unwrap().len(),
                1
            );
            assert!(!provider_log.exists());
            continue;
        }
        release.write_u8(1).await.unwrap();
        wait_for_sweep(fixture.root.path()).await;
        timeout(common::rpc_read_timeout(), async {
            loop {
                if fixture
                    .store
                    .list_completion_watches()
                    .await
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                // timing-guard: wait for durable completion-watch retirement
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("completion delivered after release");
        let messages = fixture
            .store
            .get_agent_session(parent)
            .await
            .unwrap()
            .messages;
        let wakes = messages
            .iter()
            .filter(|m| {
                m.metadata
                    .as_ref()
                    .is_some_and(|v| v["type"] == "event_notification")
            })
            .count();
        assert_eq!(
            wakes, 1,
            "exactly one recovery wake, grouped={grouped}: {messages:?}"
        );
        assert!(fixture
            .store
            .list_undelivered_groups(&workspace)
            .await
            .unwrap()
            .is_empty());
    }
}
