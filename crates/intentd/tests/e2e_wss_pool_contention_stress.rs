//! Stress test: concurrent writes must not starve reads (pool-contention fix).
//!
//! Drives 30+ concurrent note writes over WSS and asserts that a lightweight
//! read RPC (`workspace.list`) issued mid-load responds within a small bound
//! (see `contention_budget`), proving the single-writer/read pool split
//! (fix/sqlite-pool-contention) prevents pool exhaustion and
//! `database is locked` errors.

mod common;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use intent_core::{now_iso, AgentId, Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

#[derive(Default)]
struct MemTokenStore(std::sync::Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
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

/// Create a temp dir with a recognizable prefix under the system temp root.
/// The returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test tempdir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
}

async fn make_services() -> (Arc<dyn WorkspaceApi>, EventBus, Store, tempfile::TempDir) {
    let dir = test_tempdir("intentd-wss-stress-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let services = Services::new(store.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    (api, bus, store, dir)
}

struct Server {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    store: Store,
    _dir: tempfile::TempDir,
}

async fn start() -> Server {
    let (api, bus, store, dir) = make_services().await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws =
        WsApiServer::new(api.clone(), bus.clone(), &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Server {
        ws,
        port,
        cfg,
        store,
        _dir: dir,
    }
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Latency budget for a lightweight read issued mid-storm.
///
/// `baseline` is the measured latency of the same lightweight RPC on this
/// host just before the storm starts. The CI runner may be co-tenant with up
/// to 7 other heavy jobs on one box (monorepo#1239), where scheduler + TLS
/// contention alone pushes an unloaded lightweight WSS call into multi-second
/// territory, so the budget scales with the observed per-call cost instead of
/// assuming an exclusive machine. A genuine pool-starvation regression still
/// trips the bound: it couples the read's latency to the whole storm's
/// duration, far beyond 4x an unloaded call on the same host.
fn contention_budget(floor: Duration, baseline: Duration) -> Duration {
    floor.max(baseline * 4)
}

/// 30 concurrent note writes + read RPC mid-load must respond within the
/// co-tenancy-calibrated `contention_budget`. Proves the single-writer/read
/// pool split prevents pool exhaustion and `database is locked` errors under
/// heavy concurrent write load.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_do_not_starve_reads() {
    let srv = start().await;

    // Create a workspace + 30 notes.
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Stress WS"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut note_ids = Vec::new();
    for i in 0..30 {
        let resp = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"note.create","params":{{"workspaceId":"{}","title":"Note {}","content":"initial"}}}}"#,
                i + 2,
                ws_id,
                i
            ),
        )
        .await;
        let note_id = resp["result"]["note"]["id"]
            .as_str()
            .expect("note id")
            .to_string();
        note_ids.push(note_id);
    }

    // Baseline: the same lightweight read with zero concurrent load, to
    // calibrate the starvation bound to this host's current per-call cost.
    let baseline_start = Instant::now();
    let baseline_resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":998,"method":"workspace.list"}"#,
    )
    .await;
    let baseline = baseline_start.elapsed();
    assert!(
        baseline_resp.get("result").is_some(),
        "baseline workspace.list must succeed: {baseline_resp}"
    );

    // Spawn 30 concurrent note write tasks.
    let mut write_tasks = Vec::new();
    for (i, note_id) in note_ids.iter().enumerate() {
        let port = srv.port;
        let cfg = srv.cfg.clone();
        let ws_id = ws_id.clone();
        let note_id = note_id.clone();
        let handle = tokio::spawn(async move {
            let frame = format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"note.setContent","params":{{"workspaceId":"{}","noteId":"{}","content":"edit {i}","confirmReplacement":true}}}}"#,
                100 + i,
                ws_id,
                note_id,
            );
            wss_call(port, cfg, &frame).await
        });
        write_tasks.push(handle);
    }

    // Issue a lightweight read RPC mid-load (workspace.list).
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = Instant::now();
    let list_resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":999,"method":"workspace.list"}"#,
    )
    .await;
    let elapsed = start.elapsed();

    // Assert: the mid-load read is not latency-coupled to the write storm.
    // A pool-exhaustion regression makes the read wait out the writers, far
    // beyond 4x the unloaded baseline on the same host.
    let budget = contention_budget(Duration::from_secs(2), baseline);
    assert!(
        elapsed < budget,
        "workspace.list took {elapsed:?} (budget {budget:?}, unloaded baseline {baseline:?}) — read pool is blocked by writers"
    );
    assert_eq!(list_resp["id"], 999);
    assert!(
        list_resp.get("result").is_some(),
        "workspace.list must succeed: {list_resp}"
    );
    assert!(
        list_resp["result"]["workspaces"].is_array(),
        "workspace.list must return workspaces array: {list_resp}"
    );

    // Assert: no write failures.
    for (i, task) in write_tasks.into_iter().enumerate() {
        let resp = task.await.expect("write task panicked");
        assert!(
            resp.get("result").is_some() && resp.get("error").is_none(),
            "note write {i} failed: {resp}"
        );
    }

    srv.ws.stop().await;
}

/// monorepo#958 scaling regression: with 100+ agents carrying multi-KB
/// transcripts, concurrent `agent.list` lifecycle refreshes stay fast and do
/// NOT starve unrelated lightweight reads (`note.list`). Before the bounded
/// projections, each `agent.list` hydrated + decoded every transcript
/// (~6 MB of content JSON here), saturating the read pool; the issue's live
/// evidence was note refreshes pool-timing-out behind `agent.list`.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_agent_list_with_many_agents_does_not_starve_reads() {
    use intent_core::{AgentSession, AgentStatus, WorkspaceId};
    use intent_store::ReplaceMessage;

    let srv = start().await;

    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Scale WS"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Seed 100 agents × 60 messages with ~1 KB bodies (one write txn per
    // agent). A full-hydration `agent.list` would fetch + decode ~6 MB of
    // content JSON per call; the bounded projection touches ≤2 rows per agent.
    let kb_filler = "x".repeat(1024);
    for a in 0..100 {
        let ts = now_iso();
        let session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(format!("agent-{}", uuid::Uuid::new_v4())),
            workspace_id: WorkspaceId(ws_id.clone()),
            backend_session_id: None,
            acp_session_id: None,
            name: format!("Scale {a}"),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Completed,
            is_active: false,
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
        let contents: Vec<serde_json::Value> = (0..60)
            .map(|m| {
                let role_text = if m % 2 == 0 { "ask" } else { "reply" };
                serde_json::json!([{ "type": "text", "text": format!("{role_text} {m} {kb_filler}") }])
            })
            .collect();
        let messages: Vec<ReplaceMessage<'_>> = contents
            .iter()
            .enumerate()
            .map(|(m, content)| ReplaceMessage {
                role: if m % 2 == 0 { "user" } else { "assistant" },
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        srv.store
            .insert_agent_session_with_messages(&session, &messages)
            .await
            .expect("seed agent with transcript");
    }

    // Baseline: the same lightweight read with zero concurrent load, to
    // calibrate the starvation bound to this host's current per-call cost.
    let baseline_start = Instant::now();
    let baseline_resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":998,"method":"note.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let baseline = baseline_start.elapsed();
    assert!(
        baseline_resp["result"]["notes"].is_array(),
        "baseline note.list must succeed: {baseline_resp}"
    );

    // 8 concurrent agent.list refreshes (the FE lifecycle-refresh pattern).
    let mut list_tasks = Vec::new();
    for i in 0..8 {
        let port = srv.port;
        let cfg = srv.cfg.clone();
        let ws = ws_id.clone();
        list_tasks.push(tokio::spawn(async move {
            let start = Instant::now();
            let frame = format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"agent.list","params":{{"workspaceId":"{}"}}}}"#,
                100 + i,
                ws,
            );
            let resp = wss_call(port, cfg, &frame).await;
            (resp, start.elapsed())
        }));
    }

    // Mid-load, the unrelated lightweight read must respond promptly.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let start = Instant::now();
    let notes = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":999,"method":"note.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let note_elapsed = start.elapsed();
    assert!(
        notes["result"]["notes"].is_array(),
        "note.list must succeed mid-load: {notes}"
    );
    let note_budget = contention_budget(Duration::from_secs(2), baseline);
    assert!(
        note_elapsed < note_budget,
        "note.list took {note_elapsed:?} (budget {note_budget:?}, unloaded baseline {baseline:?}) — starved behind agent.list transcript reads"
    );

    // Every refresh returns the full bounded projection quickly.
    for task in list_tasks {
        let (resp, elapsed) = task.await.expect("agent.list task panicked");
        let agents = resp["result"]["agents"]
            .as_array()
            .unwrap_or_else(|| panic!("agent.list must succeed: {resp}"));
        assert_eq!(agents.len(), 100);
        for lite in agents {
            assert_eq!(lite["messageCount"], 60);
            assert!(
                lite["lastAgentResponse"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("reply 59")),
                "projection carries the newest assistant row: {lite}"
            );
        }
        let list_budget = contention_budget(Duration::from_secs(5), baseline);
        assert!(
            elapsed < list_budget,
            "agent.list took {elapsed:?} (budget {list_budget:?}, unloaded baseline {baseline:?}) with 100 seeded agents — transcript hydration is back"
        );
    }

    srv.ws.stop().await;
}
