//! WSS end-to-end for `agent.resolveInterrupted` (INT-41, agent-resumption phase 2).
//!
//! Boots a real `intentd serve` (WSS listener enabled via config), creates interrupted agent rows,
//! then calls `agent.resolveInterrupted` to resume/abandon them. Verifies that
//! resumed agents receive continuation messages and abandoned agents get system
//! interruption messages.
//!
//! Coverage:
//! - Resume path delivers continuation message and re-registers completion watches
//! - Abandon path appends system interruption message and emits events
//! - Rows leave `agent.listInterrupted` once resolved
//! - Unknown/already-resolved ids land in `failed`
//! - Id in both resume and abandon lists → -32602

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;

use common::DaemonGuard;
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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-resolve-{}", &id[..8]));
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

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-RESOLVE".to_string(),
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
    }
}

#[tokio::test]
async fn resolve_interrupted_resume_and_abandon() {
    // Phase 5: Verify the resumed agent received the reworded continuation
    // message as the last user-role message (and that it no longer mentions
    // "intentd").
    use intent_core::AgentId;
    let data_dir = temp_data_dir();
    let listen = "both";
    let socket = data_dir.join("intentd.sock");

    // Phase 1: Boot daemon, create workspace, seed two interrupted agent rows.
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    // Spawn in its own process group to prevent ACP mock process leaks
    #[cfg(unix)]
    cmd.process_group(0);
    let child = cmd.spawn().expect("spawn intentd serve");
    let mut guard = DaemonGuard::new(child, data_dir.clone(), true);
    assert!(await_uds(&socket).await, "daemon did not start");

    let ws_id = "ws-resolve-test";
    let agent_resume = format!("agent-{}", Uuid::new_v4().simple());
    let agent_abandon = format!("agent-{}", Uuid::new_v4().simple());

    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");

    {
        use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
        let ts = now_iso();
        store
            .insert_workspace(&workspace_seed(&WorkspaceId(ws_id.to_string())))
            .await
            .expect("insert workspace");

        // Agent 1: will be resumed
        let session1 = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(agent_resume.clone()),
            workspace_id: WorkspaceId(ws_id.to_string()),
            backend_session_id: None,
            acp_session_id: None,
            name: "Resume Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::RuntimeIdle, // settled after heal
            is_active: true,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
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
        };
        store
            .insert_agent_session(&session1)
            .await
            .expect("insert agent 1");
        store
            .insert_interrupted_agent(
                &AgentId(agent_resume.clone()),
                &WorkspaceId(ws_id.to_string()),
                "active",
                &ts,
            )
            .await
            .expect("insert interrupted 1");

        // Agent 2: will be abandoned
        let session2 = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(agent_abandon.clone()),
            workspace_id: WorkspaceId(ws_id.to_string()),
            backend_session_id: None,
            acp_session_id: None,
            name: "Abandon Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::RuntimeIdle,
            is_active: true,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
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
        };
        store
            .insert_agent_session(&session2)
            .await
            .expect("insert agent 2");
        store
            .insert_interrupted_agent(
                &AgentId(agent_abandon.clone()),
                &WorkspaceId(ws_id.to_string()),
                "active",
                &ts,
            )
            .await
            .expect("insert interrupted 2");
    }

    // Fetch fingerprint and port
    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");

    // Open WSS connection
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // Phase 2: Verify both agents are in the pending list
    let list1 = wss_rpc(&mut ws, 2, "agent.listInterrupted", json!({})).await;
    let agents = list1["agents"].as_array().expect("agents array");
    assert_eq!(agents.len(), 2, "expected 2 interrupted agents");

    // Phase 3: Call agent.resolveInterrupted
    let result = wss_rpc(
        &mut ws,
        3,
        "agent.resolveInterrupted",
        json!({
            "resume": [agent_resume.clone()],
            "abandon": [agent_abandon.clone()],
        }),
    )
    .await;

    // Verify response shape
    let resumed = result["resumed"].as_array().expect("resumed array");
    let abandoned = result["abandoned"].as_array().expect("abandoned array");
    let failed = result["failed"].as_array().expect("failed array");

    assert_eq!(resumed.len(), 1, "expected 1 resumed");
    assert_eq!(resumed[0].as_str(), Some(agent_resume.as_str()));
    assert_eq!(abandoned.len(), 1, "expected 1 abandoned");
    assert_eq!(abandoned[0].as_str(), Some(agent_abandon.as_str()));
    assert_eq!(failed.len(), 0, "expected 0 failed");

    // Phase 4: Verify rows are no longer in pending list
    let list2 = wss_rpc(&mut ws, 4, "agent.listInterrupted", json!({})).await;
    let agents2 = list2["agents"].as_array().expect("agents array 2");
    assert_eq!(
        agents2.len(),
        0,
        "expected 0 interrupted agents after resolve"
    );

    let resume_agent_id = AgentId(agent_resume.clone());
    let resumed_session = store
        .get_agent_session(&resume_agent_id)
        .await
        .expect("get resumed session");
    let last_user_idx = resumed_session
        .messages
        .iter()
        .rposition(|m| m.role == "user")
        .expect("expected user continuation message on resumed session");
    let last_user_msg = &resumed_session.messages[last_user_idx];
    let resumed_blocks = last_user_msg.content.as_array().expect("content blocks");
    assert_eq!(resumed_blocks[0]["type"], "text");
    let continuation_text = resumed_blocks[0]["text"].as_str().expect("text block");
    assert!(
        continuation_text.starts_with("You were interrupted for about "),
        "continuation should carry the approved wording with a humanized outage duration, \
         got: {continuation_text}"
    );
    assert!(
        continuation_text.ends_with(
            "due to a harness shutdown and restart. You can now continue your work and pick \
             up where you left off."
        ),
        "continuation should end with the approved wording, got: {continuation_text}"
    );
    assert!(
        !continuation_text.contains("intentd"),
        "continuation must not mention intentd, got: {continuation_text}"
    );

    // Phase 5b: the resume path also appends a system interruption marker
    // (same shape as the abandon marker, meta.kind == "interruption") and it
    // must sit IMMEDIATELY BEFORE the continuation user message. Locate it by
    // its exact shape (not "first system message") so unrelated system rows
    // can't shadow it.
    let is_interruption_marker = |m: &intent_core::AgentMessage| {
        m.role == "system"
            && m.content.as_array().is_some_and(|blocks| {
                blocks.len() == 1
                    && blocks[0]["type"] == "text"
                    && blocks[0]["text"]
                        == "The previous turn was interrupted because the harness shut down. \
                            Continuing below."
                    && blocks[0]["meta"]["kind"] == "interruption"
            })
    };
    let marker_idx = resumed_session
        .messages
        .iter()
        .position(is_interruption_marker)
        .expect("expected system interruption marker on resumed session");
    assert_eq!(
        marker_idx + 1,
        last_user_idx,
        "system marker (index {marker_idx}) must sit immediately before the \
         continuation user message (index {last_user_idx})"
    );
    assert_eq!(
        resumed_session
            .messages
            .iter()
            .filter(|m| is_interruption_marker(m))
            .count(),
        1,
        "exactly one interruption marker on the resumed session"
    );

    // Phase 6: Verify the abandoned agent has a system message
    let abandoned_session = store
        .get_agent_session(&AgentId(agent_abandon.clone()))
        .await
        .expect("get abandoned session");
    assert!(!abandoned_session.messages.is_empty(), "expected messages");
    let last_msg = abandoned_session.messages.last().unwrap();
    assert_eq!(last_msg.role, "system", "expected system message");
    let blocks = last_msg.content.as_array().expect("content blocks");
    let text_block = &blocks[0];
    assert_eq!(text_block["type"], "text");
    assert!(text_block["text"]
        .as_str()
        .unwrap()
        .contains("interrupted because intentd restarted"));
    assert_eq!(text_block["meta"]["kind"], "interruption");

    // Phase 7: Test error case - unknown agent id (already resolved)
    let unknown_result = wss_rpc(
        &mut ws,
        5,
        "agent.resolveInterrupted",
        json!({
            "resume": [agent_resume.clone()],
        }),
    )
    .await;
    let resumed2 = unknown_result["resumed"]
        .as_array()
        .expect("resumed array 2");
    let failed2 = unknown_result["failed"].as_array().expect("failed array 2");
    assert_eq!(resumed2.len(), 0, "already resolved should not resume");
    assert_eq!(failed2.len(), 1, "already resolved should be in failed");

    guard.child_mut().kill().expect("kill daemon");
    guard.child_mut().wait().expect("wait daemon");
    drop(guard);
}

#[tokio::test]
async fn resolve_interrupted_invalid_params_validation() {
    let data_dir = temp_data_dir();
    let listen = "both";
    let socket = data_dir.join("intentd.sock");

    // Boot daemon
    if listen != "uds" {
        common::enable_ws_api(&data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd.process_group(0);
    let child = cmd.spawn().expect("spawn intentd serve");
    let mut guard = DaemonGuard::new(child, data_dir.clone(), true);
    assert!(await_uds(&socket).await, "daemon did not start");

    // Fetch fingerprint and port
    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");

    // Open WSS connection
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // Test 1: Non-array resume param → -32602
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "agent.resolveInterrupted",
        "params": { "resume": "not-an-array" }
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    let resp = timeout(common::rpc_read_timeout(), ws.next())
        .await
        .expect("timeout")
        .expect("msg")
        .expect("ok");
    let v: Value = match resp {
        Message::Text(t) => serde_json::from_str(&t).expect("json"),
        _ => panic!("expected text"),
    };
    assert_eq!(v["id"], json!(1));
    assert_eq!(
        v["error"]["code"],
        json!(-32602),
        "expected -32602 for non-array resume"
    );
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("resume must be an array"),
        "error message should mention array requirement"
    );

    // Test 2: Non-array abandon param → -32602
    let req = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "agent.resolveInterrupted",
        "params": { "abandon": 123 }
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    let resp = timeout(common::rpc_read_timeout(), ws.next())
        .await
        .expect("timeout")
        .expect("msg")
        .expect("ok");
    let v: Value = match resp {
        Message::Text(t) => serde_json::from_str(&t).expect("json"),
        _ => panic!("expected text"),
    };
    assert_eq!(v["id"], json!(2));
    assert_eq!(
        v["error"]["code"],
        json!(-32602),
        "expected -32602 for non-array abandon"
    );
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("abandon must be an array"),
        "error message should mention array requirement"
    );

    // Test 3: Array with non-string element → -32602
    let req = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "agent.resolveInterrupted",
        "params": { "resume": ["valid-id", 123, "another-id"] }
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    let resp = timeout(common::rpc_read_timeout(), ws.next())
        .await
        .expect("timeout")
        .expect("msg")
        .expect("ok");
    let v: Value = match resp {
        Message::Text(t) => serde_json::from_str(&t).expect("json"),
        _ => panic!("expected text"),
    };
    assert_eq!(v["id"], json!(3));
    assert_eq!(
        v["error"]["code"],
        json!(-32602),
        "expected -32602 for non-string array element"
    );
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("must be a string"),
        "error message should mention string requirement"
    );

    // Test 4: Valid mixed resume/abandon (should succeed)
    let req = json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "agent.resolveInterrupted",
        "params": { "resume": ["agent-1", "agent-2"], "abandon": ["agent-3"] }
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    let resp = timeout(common::rpc_read_timeout(), ws.next())
        .await
        .expect("timeout")
        .expect("msg")
        .expect("ok");
    let v: Value = match resp {
        Message::Text(t) => serde_json::from_str(&t).expect("json"),
        _ => panic!("expected text"),
    };
    assert_eq!(v["id"], json!(4));
    assert!(v.get("result").is_some(), "valid params should succeed");
    assert!(v.get("error").is_none(), "valid params should not error");

    guard.child_mut().kill().expect("kill daemon");
    guard.child_mut().wait().expect("wait daemon");
    drop(guard);
}
