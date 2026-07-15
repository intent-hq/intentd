//! WSS end-to-end for delegation-group persistence and aggregated wake across restart.
//!
//! Creates a parent that delegates two children with `waitMode: 'after_all'`, allows
//! child1 to complete, kills the daemon mid-flight, restarts, resumes child2, allows
//! child2 to complete post-restart, and verifies the parent receives exactly ONE
//! aggregated wake over WSS with both children's summaries.
//!
//! Coverage:
//! - Delegation groups persist to SQLite (write-through)
//! - Groups rehydrate on `agent.resolveInterrupted` with sealed=true
//! - Pre-restart completions survive restart
//! - Aggregated wake fires exactly once with both summaries
//! - Wake observable via WSS (stream lifecycle keyed by parent)
//! - Group row deleted after delivery

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
const CHILD1_REPORT: &str = "CHILD1_DONE_PRE_RESTART";
const CHILD2_REPORT: &str = "CHILD2_DONE_POST_RESTART";

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
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
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
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
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
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
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    let tls = TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string()))
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
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

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
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-delgrp-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
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

async fn seed_workspace_only(data_dir: &Path) -> String {
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
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "DELGRP-E2E".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
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
    }
}

fn boot_daemon(data_dir: &PathBuf, port: u16, env: &[(&str, &str)]) -> std::process::Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg("both")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", &port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

#[tokio::test]
async fn delegation_group_persists_across_restart() {
    let Some(script) = gate("WSS delegation-group persist E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    eprintln!("[TEST] data_dir: {}", data_dir.display());
    let ws_id = seed_workspace_only(&data_dir).await;
    eprintln!("[TEST] seeded workspace: {}", ws_id);
    let port = free_port();
    let socket = data_dir.join("intentd.sock");
    eprintln!("[TEST] ready to boot daemon on port {}", port);

    // Mock ACP behavior: children report after delay (to ensure parent seals group first)
    let report1_js = format!(
        "return await ws.agent.reportToParent({});",
        json!(CHILD1_REPORT)
    );
    let report2_js = format!(
        "return await ws.agent.reportToParent({});",
        json!(CHILD2_REPORT)
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": "CHILD1",
                "delayMs": 8000,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report1_js, "summary": "child1 report" }
                },
                "response": "child1 done"
            },
            {
                "ifPromptContains": "CHILD2",
                "delayMs": 8000,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report2_js, "summary": "child2 report" }
                },
                "response": "child2 done"
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent ack"
            },
            {
                "ifPromptContains": "PARENT_GO",
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": {
                            "code": "return await ws.agent.delegate({ agentInstructions: 'CHILD1', waitMode: 'after_all', model: 'mock:default' });",
                            "summary": "delegate child1"
                        }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": {
                            "code": "return await ws.agent.delegate({ agentInstructions: 'CHILD2', waitMode: 'after_all', model: 'mock:default' });",
                            "summary": "delegate child2"
                        }
                    }
                ],
                "response": "delegated both"
            }
        ]
    }).to_string();

    let env = [
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];

    // Phase 1: Boot, create parent, delegate both children
    let mut daemon = boot_daemon(&data_dir, port, &env);
    assert!(await_uds(&socket).await, "daemon start");

    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let cfg = client_config(&fp);

    // Subscribe BEFORE creating the parent so we don't miss events
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_string();

    wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &parent_id, "content": "PARENT_GO" }),
    )
    .await;

    // Wait for parent to idle after delegating
    let mut parent_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev["type"] == "agent:idle" && ev_agent == parent_id {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent went idle after delegating");

    // Get child IDs from parent's waitingForAgentIds (more reliable than event capture)
    let lite = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": &parent_id })).await;
    let waiting = lite["agent"]["waitingForAgentIds"]
        .as_array()
        .expect("waitingForAgentIds");
    assert_eq!(waiting.len(), 2, "parent waiting for 2 children");
    let child1_id = waiting[0].as_str().unwrap().to_string();
    let child2_id = waiting[1].as_str().unwrap().to_string();

    // Wait for child1 to complete (idle)
    let mut child1_idle = false;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:idle"
            && frame["params"]["event"]["data"]["agentId"] == child1_id
        {
            child1_idle = true;
            break;
        }
    }
    assert!(child1_idle, "child1 went idle");

    // Kill daemon before child2 completes
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("wait daemon");
    drop(sub);
    drop(rpc);

    // Insert interrupted_agent row for child2
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    {
        use intent_core::{now_iso, AgentId, WorkspaceId};
        store
            .insert_interrupted_agent(
                &AgentId(child2_id.clone()),
                &WorkspaceId(ws_id.to_string()),
                "active",
                &now_iso(),
            )
            .await
            .expect("insert interrupted child2");
    }

    // Restart daemon
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut daemon2 = boot_daemon(&data_dir, port, &env);
    assert!(await_uds(&socket).await, "daemon restart");

    // Reconnect WSS
    let status2 = uds_rpc(&socket, 20, "system.status", json!({})).await;
    let fp2 = status2["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let cfg2 = client_config(&fp2);

    let mut sub2 = connect_ws(port, cfg2.clone()).await;
    wss_rpc(
        &mut sub2,
        21,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;

    let mut rpc2 = connect_ws(port, cfg2).await;

    // Resume child2
    wss_rpc(
        &mut rpc2,
        30,
        "agent.resolveInterrupted",
        json!({ "resume": [child2_id.clone()] }),
    )
    .await;

    // Wait for child2 to complete
    let mut child2_idle = false;
    for _ in 0..100 {
        let frame = wss_event(&mut sub2, 30).await;
        if frame["params"]["event"]["type"] == "agent:idle"
            && frame["params"]["event"]["data"]["agentId"] == child2_id
        {
            child2_idle = true;
            break;
        }
    }
    assert!(child2_idle, "child2 went idle post-restart");

    // Observe the aggregated wake: parent receives stream events
    let mut wake_chunks = 0u32;
    let mut wake_ends = 0u32;
    let mut parent_idle_again = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub2, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev_agent != parent_id {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:stream:chunk") => wake_chunks += 1,
            Some("agent:stream:end") => wake_ends += 1,
            Some("agent:idle") => parent_idle_again = true,
            _ => {}
        }
        if parent_idle_again && wake_ends >= 1 {
            break;
        }
    }
    assert!(wake_chunks >= 1, "wake turn streamed ≥1 chunk");
    assert_eq!(wake_ends, 1, "exactly one wake stream:end");
    assert!(parent_idle_again, "parent idled after wake");

    // Verify parent transcript has ONE wake with both reports
    let conv = wss_rpc(
        &mut rpc2,
        40,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");

    // Debug: Count and show wake messages
    let texts: Vec<String> = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    let wakes: Vec<&String> = texts
        .iter()
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect();

    assert_eq!(wakes.len(), 1, "exactly one wake message");
    let wake = wakes[0];
    assert!(
        wake.contains("All 2 delegated child agent(s) settled"),
        "wake header"
    );
    assert!(wake.contains(CHILD1_REPORT), "wake has child1 report");
    assert!(wake.contains(CHILD2_REPORT), "wake has child2 report");

    // Verify delegation_group row deleted after delivery
    let final_groups = store
        .list_undelivered_groups(&intent_core::WorkspaceId(ws_id.to_string()))
        .await
        .expect("list groups after delivery");
    assert_eq!(final_groups.len(), 0, "group deleted after delivery");

    daemon2.kill().expect("kill daemon2");
    daemon2.wait().expect("wait daemon2");
}
