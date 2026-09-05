//! WSS end-to-end for the emit-path PR merge (docs/protocol/methods/
//! workspace.md, docs/protocol/06-events.md): `workspace.list` rows and the
//! `workspace.subscribe` seq-0 snapshot fold externally known PRs — git-root
//! discoveries (`workspace_git_root.pull_requests`) and agent PR monitors
//! (active + completed; cancelled excluded) — into `pullRequests`, deduped
//! by URL with workspace > git-root > monitor priority. Rows with nothing
//! to merge keep the field omitted (never `[]`), archived workspaces merge
//! only when the call includes them, and nothing is persisted back. Drives
//! a real [`WsApiServer`] over TLS with bearer-token auth and a pinned
//! self-signed fingerprint (the production transport path).

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, PrMonitor, PrMonitorId, PrMonitorState,
    PullRequestInfo, PullRequestStatus, Result as CoreResult, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource,
    WorkspaceId, WorkspaceStatus,
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

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

fn workspace(id: &WorkspaceId, title: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: title.into(),
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
        setup_result: None,
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
        disk_usage: None,
        pending_delete_at: None,
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
        name: "Monitor Owner".into(),
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
    }
}

fn pr_info(number: u64, url: &str, title: &str) -> PullRequestInfo {
    PullRequestInfo {
        id: number.to_string(),
        number,
        url: url.to_string(),
        title: title.to_string(),
        status: PullRequestStatus::Open,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        base_ref: None,
        head_ref: None,
        head_sha: None,
        author: None,
        mergeable: None,
        mergeable_state: None,
        is_draft: None,
    }
}

fn git_root(ws: &WorkspaceId, path: &str, prs: Vec<PullRequestInfo>) -> WorkspaceGitRoot {
    let ts = now_iso();
    WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: ws.clone(),
        path: path.to_string(),
        source: WorkspaceGitRootSource::Agent,
        repo_owner: Some("o".into()),
        repo_name: Some("r".into()),
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: Some(prs),
        created_at: ts.clone(),
        updated_at: ts,
    }
}

fn monitor(
    ws: &WorkspaceId,
    owner: &str,
    name: &str,
    number: i64,
    state: PrMonitorState,
    snapshot: Option<String>,
) -> PrMonitor {
    PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: ws.clone(),
        agent_id: AgentId::from("agent-merge-e2e"),
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: number,
        state,
        last_snapshot: snapshot,
        baseline_snapshot: None,
        pending_changes: vec![],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: "2026-01-02T00:00:00Z".into(),
        updated_at: "2026-01-02T00:00:01Z".into(),
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_merge: WorkspaceId,
    ws_plain: WorkspaceId,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over a store seeded with:
/// `ws_merge` — NULL stored `pullRequests`, one git-root PR, one
/// snapshot-backed active monitor, one snapshotless completed monitor, one
/// cancelled monitor (excluded), and a snapshotless monitor duplicating the
/// git-root PR's URL (deduped) — and `ws_plain`, a workspace with a git root
/// whose `pull_requests` list is empty (nothing to merge; field must stay
/// omitted on the wire).
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-merged-prs-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_merge = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_merge, "Merge sources"))
        .await
        .expect("seed ws_merge");
    // Monitor rows FK onto agent_session.
    store
        .insert_agent_session(&agent_session(&ws_merge, "agent-merge-e2e"))
        .await
        .expect("seed agent");
    store
        .upsert_workspace_git_root(&git_root(
            &ws_merge,
            "/tmp/root-a",
            vec![pr_info(1, "https://github.com/o/r/pull/1", "Root PR")],
        ))
        .await
        .expect("ws_merge root");
    let snapshot = serde_json::json!({
        "title": "Monitored PR",
        "url": "https://github.com/o/r/pull/2",
        "headSha": "abc123",
        "conversationCount": 0,
        "reviewCommentCount": 0,
        "requirements": {
            "state": "open",
            "isDraft": false,
            "hasConflicts": false,
            "isBehind": false,
            "mergeable": true,
            "checks": {
                "total": 0, "passed": 0, "failed": 0, "pending": 0,
                "items": [], "failingRequired": [], "pendingRequired": [],
                "requiredKnown": true
            },
            "approvals": { "decision": "none", "have": 0, "changesRequested": 0 },
            "threads": { "unresolved": 0 },
            "rulesKnown": false
        }
    })
    .to_string();
    for m in [
        monitor(
            &ws_merge,
            "o",
            "r",
            2,
            PrMonitorState::Active,
            Some(snapshot),
        ),
        monitor(&ws_merge, "o2", "r2", 7, PrMonitorState::Completed, None),
        monitor(&ws_merge, "o", "r", 9, PrMonitorState::Cancelled, None),
        monitor(&ws_merge, "o", "r", 1, PrMonitorState::Active, None),
    ] {
        store.insert_pr_monitor(&m).await.expect("seed monitor");
    }

    // Nothing-to-merge row: an empty git-root list must not materialize `[]`.
    let ws_plain = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_plain, "Plain"))
        .await
        .expect("seed ws_plain");
    store
        .upsert_workspace_git_root(&git_root(&ws_plain, "/tmp/root-b", vec![]))
        .await
        .expect("ws_plain root");

    let services = Arc::new(
        Services::new(store)
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
        ws_merge,
        ws_plain,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return `result` (asserting no `error`).
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let v = timeout(common::rpc_read_timeout(), async {
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
    .expect("response timeout");
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Read frames until the `subscription.push` notification arrives.
async fn next_subscription_push(ws: &mut TlsWs) -> Value {
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("method") == Some(&json!("subscription.push")) {
                        return v["params"].clone();
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
    .expect("subscription.push timeout")
}

/// Shared assertions on a list-shaped row set (`workspace.list` result rows
/// or the seq-0 snapshot entries): the merged `pullRequests` array carries
/// git-root first then monitor-derived entries, deduped by URL with
/// cancelled monitors excluded, and the nothing-to-merge row keeps the
/// field omitted (never `[]`).
fn assert_merged_rows(rows: &[Value], ws_merge: &WorkspaceId, ws_plain: &WorkspaceId, path: &str) {
    let merged = rows
        .iter()
        .find(|r| r["id"] == json!(ws_merge.as_str()))
        .expect("merge row");
    let prs = merged["pullRequests"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: merged pullRequests is an array: {merged}"));
    let urls: Vec<_> = prs.iter().map(|p| p["url"].as_str().unwrap()).collect();
    assert_eq!(
        urls,
        [
            "https://github.com/o/r/pull/1",
            "https://github.com/o/r/pull/2",
            "https://github.com/o2/r2/pull/7",
        ],
        "{path}: git-root first, then monitor-derived; cancelled + dup excluded"
    );
    // Git-root entry beat the snapshotless duplicate monitor on PR 1.
    assert_eq!(prs[0]["title"], json!("Root PR"), "{path}");
    assert_eq!(prs[0]["status"], json!("Open"), "{path}");
    // Snapshot-backed monitor entry: fields synthesized off the snapshot.
    assert_eq!(prs[1]["title"], json!("Monitored PR"), "{path}");
    assert_eq!(prs[1]["number"], json!(2), "{path}");
    assert_eq!(prs[1]["headSha"], json!("abc123"), "{path}");
    assert_eq!(prs[1]["isDraft"], json!(false), "{path}");
    // Snapshotless completed monitor: synthesized identity; terminal
    // without a verdict reads closed, never merged.
    assert_eq!(prs[2]["title"], json!("o2/r2#7"), "{path}");
    assert_eq!(prs[2]["status"], json!("Closed"), "{path}");

    let plain = rows
        .iter()
        .find(|r| r["id"] == json!(ws_plain.as_str()))
        .expect("plain row");
    assert!(
        plain.get("pullRequests").is_none(),
        "{path}: empty git-root list keeps pullRequests omitted, never []: {plain}"
    );
}

/// `workspace.list` rows over WSS carry the emit-path merged `pullRequests`
/// (git-root + monitor sources, URL-deduped, cancelled excluded), while the
/// nothing-to-merge row omits the field entirely.
#[tokio::test]
async fn workspace_list_merges_external_prs_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    let listed = wss_rpc(&mut rpc, 1, "workspace.list", json!({})).await;
    let rows = listed["workspaces"].as_array().expect("workspaces array");
    assert_merged_rows(rows, &fx.ws_merge, &fx.ws_plain, "workspace.list");
}

/// The `workspace.subscribe` seq-0 snapshot rides the same lite list path
/// and must carry the identical merged `pullRequests` a `workspace.list`
/// would (docs/protocol/06-events.md).
#[tokio::test]
async fn workspace_subscribe_snapshot_merges_external_prs_over_wss() {
    let fx = boot().await;
    let mut sub = connect(fx.port, fx.cfg.clone()).await;

    let sub_res = wss_rpc(&mut sub, 1, "workspace.subscribe", json!({})).await;
    let sub_id = sub_res["subscriptionId"].as_str().expect("subscriptionId");

    let push = next_subscription_push(&mut sub).await;
    assert_eq!(push["subscriptionId"], json!(sub_id), "push: {push}");
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    assert_eq!(push["seq"], json!(0), "push: {push}");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    assert_merged_rows(snap, &fx.ws_merge, &fx.ws_plain, "subscribe seq-0");
}
