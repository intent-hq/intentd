//! Real authenticated TLS/WSS script monitor controls and lifecycle events.
//! Registration uses the service endpoint called by the authenticated MCP helper.
#![cfg(unix)]
mod common;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Result as CoreResult, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex)
/// and otherwise validates the handshake signature with the ring provider.
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
        let digest = Sha256::digest(end_entity.as_ref());
        let hex: Vec<String> = digest.iter().map(|b| format!("{b:02X}")).collect();
        if hex.join(":").eq_ignore_ascii_case(&self.fingerprint) {
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

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    services: Arc<Services>,
    store: Store,
    ws_id: WorkspaceId,
    agent_id: AgentId,
    _dir: tempfile::TempDir,
}
fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Script monitor".into(),
        branch: "feature".into(),
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
        repository_owner: Some("o".into()),
        repository_name: Some("r".into()),
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

fn agent_session(ws: &WorkspaceId, id: &str) -> AgentSession {
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Owner".into(),
        name_explicitly_set: true,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Active,
        is_active: false,
        messages: vec![],
        stats: None,
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
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
    }
}

async fn boot() -> Fixture {
    let dir_guard = common::test_tempdir("intentd-script-monitor-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_id))
        .await
        .expect("seed workspace");
    let agent_id = AgentId::from("agent-prmon-e2e");
    store
        .insert_agent_session(&agent_session(&ws_id, agent_id.as_str()))
        .await
        .expect("seed agent");

    let services = Arc::new(
        Services::new(store.clone())
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone()),
    );
    let api: Arc<dyn WorkspaceApi> = services.clone();
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        services,
        store,
        ws_id,
        agent_id,
        _dir: dir_guard,
    }
}

async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response envelope so callers
/// can assert both `result` and `error` shapes.
async fn wss_call(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_call(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Wait for the next `events.event` notification whose `type` matches.
async fn next_event(ws: &mut TlsWs, event_type: &str) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
}

#[tokio::test]
async fn script_monitor_wss_ownership_cancel_run_events_and_isolation() {
    let f = boot().await;
    let mut client = connect(f.port, f.cfg.clone()).await;
    let mut events = connect(f.port, f.cfg.clone()).await;
    let hello = wss_rpc(
        &mut client,
        50,
        "client.hello",
        json!({"clientId":"script-monitor-e2e","clientType":"web"}),
    )
    .await;
    assert_eq!(hello["server"]["capabilities"]["scriptMonitors"], 1);
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({"workspaceId":f.ws_id,"eventTypes":["scriptMonitor:*"]}),
    )
    .await;
    let script=wss_rpc(&mut client,2,"script.create",json!({"workspaceId":f.ws_id,"name":"Controlled command","command":"read value","mode":"command","purpose":"saved"})).await;
    let id = script["id"].as_str().unwrap().to_owned();
    let started = wss_rpc(
        &mut client,
        3,
        "script.start",
        json!({"workspaceId":f.ws_id,"scriptId":id}),
    )
    .await;
    assert!(started["runId"].is_string(), "{started}");
    let first = f
        .services
        .script_monitor(
            f.ws_id.clone(),
            f.agent_id.clone(),
            id.clone(),
            json!({"ttlMs":60000,"runId":started["runId"]}),
        )
        .await
        .unwrap();
    let monitor = first["monitor"].clone();
    assert_eq!(
        next_event(&mut events, "scriptMonitor:registered").await["data"]["monitor"],
        monitor
    );
    let retry = f
        .services
        .script_monitor(
            f.ws_id.clone(),
            f.agent_id.clone(),
            id.clone(),
            json!({"ttlMs":1000,"lineCount":1}),
        )
        .await
        .unwrap();
    assert_eq!(retry, first);
    let other = AgentId::from("agent-other-monitor");
    f.store
        .insert_agent_session(&agent_session(&f.ws_id, other.as_str()))
        .await
        .unwrap();
    let refused = f
        .services
        .script_monitor(f.ws_id.clone(), other, id.clone(), json!({"ttlMs":60000}))
        .await
        .unwrap();
    assert_eq!(refused["reason"], "already-monitored");
    assert_eq!(refused["ownerAgentId"], json!(f.agent_id));
    let list = wss_rpc(
        &mut client,
        4,
        "scriptMonitor.list",
        json!({"workspaceId":f.ws_id}),
    )
    .await;
    assert_eq!(list["monitors"], json!([monitor]));
    let foreign = WorkspaceId::new();
    f.store
        .insert_workspace(&workspace(&foreign))
        .await
        .unwrap();
    let foreign_agent = AgentId::from("agent-foreign-monitor");
    f.store
        .insert_agent_session(&agent_session(&foreign, foreign_agent.as_str()))
        .await
        .unwrap();
    let foreign_script=wss_rpc(&mut client,40,"script.create",json!({"workspaceId":foreign,"name":"Other command","command":"read value","mode":"command","purpose":"saved"})).await;
    let foreign_id = foreign_script["id"].as_str().unwrap().to_owned();
    wss_rpc(
        &mut client,
        41,
        "script.start",
        json!({"workspaceId":foreign,"scriptId":foreign_id}),
    )
    .await;
    let foreign_watch = f
        .services
        .script_monitor(
            foreign.clone(),
            foreign_agent.clone(),
            foreign_id.clone(),
            json!({"ttlMs":60000}),
        )
        .await
        .unwrap();
    assert_eq!(
        wss_rpc(
            &mut client,
            42,
            "scriptMonitor.list",
            json!({"workspaceId":foreign})
        )
        .await["monitors"],
        json!([foreign_watch["monitor"].clone()])
    );
    let error = wss_call(
        &mut client,
        5,
        "scriptMonitor.cancelRun",
        json!({"workspaceId":foreign,"monitorId":monitor["monitorId"]}),
    )
    .await;
    assert_eq!(error["error"]["code"], -32602);
    let cancelled = wss_rpc(
        &mut client,
        6,
        "scriptMonitor.cancelRun",
        json!({"workspaceId":f.ws_id,"monitorId":monitor["monitorId"]}),
    )
    .await;
    assert_eq!(cancelled["runStopped"], true);
    assert_eq!(cancelled["monitor"]["state"], "completed");
    assert_eq!(cancelled["monitor"]["result"]["outcome"], "cancelled");
    assert!(cancelled["monitor"]["result"].get("runId").is_none());
    let completed = next_event(&mut events, "scriptMonitor:completed").await;
    assert_eq!(completed["data"]["monitor"], cancelled["monitor"]);
    assert_eq!(
        wss_rpc(
            &mut client,
            43,
            "scriptMonitor.list",
            json!({"workspaceId":foreign})
        )
        .await["monitors"][0]["state"],
        "active"
    );
    assert!(f
        .store
        .get_agent_message_by_id_with_pruned(
            &foreign_agent,
            &format!("script-monitor:{}", monitor["monitorId"].as_str().unwrap())
        )
        .await
        .unwrap()
        .is_none());
    wss_rpc(
        &mut client,
        44,
        "scriptMonitor.cancel",
        json!({"workspaceId":foreign,"monitorId":foreign_watch["monitor"]["monitorId"]}),
    )
    .await;
    wss_rpc(
        &mut client,
        45,
        "script.stop",
        json!({"workspaceId":foreign,"scriptId":foreign_id}),
    )
    .await;
    let repeat = wss_rpc(
        &mut client,
        7,
        "scriptMonitor.cancelRun",
        json!({"workspaceId":f.ws_id,"monitorId":monitor["monitorId"]}),
    )
    .await;
    assert_eq!(repeat["runStopped"], false);
    let fresh = wss_rpc(
        &mut client,
        8,
        "script.restart",
        json!({"workspaceId":f.ws_id,"scriptId":id}),
    )
    .await;
    assert_ne!(fresh["runId"], started["runId"]);
    let rearmed = f
        .services
        .script_monitor(
            f.ws_id.clone(),
            f.agent_id.clone(),
            id.clone(),
            json!({"ttlMs":60000}),
        )
        .await
        .unwrap();
    let silent = wss_rpc(
        &mut client,
        9,
        "scriptMonitor.cancel",
        json!({"workspaceId":f.ws_id,"monitorId":rearmed["monitor"]["monitorId"]}),
    )
    .await;
    assert_eq!(silent["monitor"]["state"], "cancelled");
    assert_eq!(silent["monitor"]["reason"], "unmonitored");
    assert_eq!(
        next_event(&mut events, "scriptMonitor:cancelled").await["data"]["monitor"],
        silent["monitor"]
    );
    wss_rpc(
        &mut client,
        10,
        "script.stop",
        json!({"workspaceId":f.ws_id,"scriptId":id}),
    )
    .await;
}

#[tokio::test]
async fn script_monitor_wss_lifecycle_suppresses_recoverable_wakes_and_active_rows() {
    for (method, undo, reason) in [
        ("agent.retire", Some("agent.restore"), "owner-retired"),
        ("agent.delete", None, "owner-deleted"),
        (
            "workspace.archive",
            Some("workspace.unarchive"),
            "workspace-archived",
        ),
    ] {
        let f = boot().await;
        let mut client = connect(f.port, f.cfg.clone()).await;
        let mut events = connect(f.port, f.cfg.clone()).await;
        wss_rpc(
            &mut events,
            1,
            "events.subscribe",
            json!({"workspaceId":f.ws_id,"eventTypes":["scriptMonitor:*"]}),
        )
        .await;
        let script = wss_rpc(&mut client, 2, "script.create", json!({"workspaceId":f.ws_id,"name":"Lifecycle command","command":"read value","mode":"command","purpose":"saved"})).await;
        let script_id = script["id"].as_str().unwrap().to_owned();
        let run = wss_rpc(
            &mut client,
            3,
            "script.start",
            json!({"workspaceId":f.ws_id,"scriptId":script_id}),
        )
        .await;
        // Persist the exact recovery boundary before hydration: one committed
        // trigger with an undelivered outbox entry and a rearmed active watch.
        // There is deliberately no background dispatcher in this fixture yet.
        let mut pending = intent_core::ScriptMonitor {
            monitor_id: "pending-wake".into(),
            workspace_id: f.ws_id.clone(),
            agent_id: f.agent_id.clone(),
            script_id: script_id.clone(),
            run_id: run["runId"].as_str().unwrap().into(),
            script_name: "Lifecycle command".into(),
            mode: intent_core::ScriptMode::Command,
            state: "active".into(),
            created_at: now_iso(),
            expires_at: (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            output_pattern: None,
            line_count: Some(1),
            settled_at: None,
            reason: None,
            result: None,
            trigger: None,
        };
        f.store.insert_script_monitor(&pending).await.unwrap();
        pending.state = "triggered".into();
        pending.reason = Some("line-count".into());
        pending.settled_at = Some(now_iso());
        pending.trigger = Some(intent_core::ScriptMonitorTrigger {
            observed_line_count: 1,
            matched_line: None,
        });
        f.store.settle_script_monitor(&pending).await.unwrap();
        let mut active = pending.clone();
        active.monitor_id = "active-watch".into();
        active.state = "active".into();
        active.reason = None;
        active.settled_at = None;
        active.trigger = None;
        f.store.insert_script_monitor(&active).await.unwrap();
        f.store
            .set_agent_session_status(
                &f.ws_id,
                &f.agent_id,
                AgentStatus::RuntimeIdle,
                false,
                &now_iso(),
                None,
            )
            .await
            .unwrap();
        wss_rpc(
            &mut client,
            4,
            method,
            json!({"workspaceId":f.ws_id,"agentId":f.agent_id}),
        )
        .await;
        let event = next_event(&mut events, "scriptMonitor:cancelled").await;
        assert_eq!(event["data"]["monitor"]["reason"], reason);
        assert_eq!(event["data"]["monitor"]["monitorId"], "active-watch");
        let rows = wss_rpc(
            &mut client,
            5,
            "scriptMonitor.list",
            json!({"workspaceId":f.ws_id}),
        )
        .await;
        assert_eq!(rows["monitors"].as_array().unwrap().len(), 2);
        assert_eq!(
            f.store
                .script_monitor(&f.ws_id, "pending-wake")
                .await
                .unwrap()
                .state,
            "triggered"
        );
        assert!(!f
            .store
            .script_monitor_wake_allowed("pending-wake")
            .await
            .unwrap());
        assert!(!f
            .store
            .script_monitor_wake_pending("active-watch")
            .await
            .unwrap());
        if let Some(undo) = undo {
            wss_rpc(
                &mut client,
                6,
                undo,
                json!({"workspaceId":f.ws_id,"agentId":f.agent_id}),
            )
            .await;
            assert!(!f
                .store
                .script_monitor_wake_allowed("pending-wake")
                .await
                .unwrap());
            assert_eq!(
                f.store
                    .script_monitor(&f.ws_id, "active-watch")
                    .await
                    .unwrap()
                    .state,
                "cancelled"
            );
        }
        assert!(f
            .store
            .get_agent_message_by_id_with_pruned(&f.agent_id, "script-monitor:pending-wake")
            .await
            .unwrap()
            .is_none());
        wss_rpc(
            &mut client,
            7,
            "script.stop",
            json!({"workspaceId":f.ws_id,"scriptId":script_id}),
        )
        .await;
    }
}
