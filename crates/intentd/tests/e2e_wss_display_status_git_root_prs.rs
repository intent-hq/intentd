//! WSS end-to-end for the git-root PR fold of the derived
//! `Workspace.displayStatus` (docs/protocol/methods/workspace.md §5.1 step 4,
//! docs/protocol/06-events.md `workspace:displayStatus-changed`): PRs
//! persisted on a workspace's secondary git roots
//! (`workspace_git_root.pull_requests`) are a same-rung input to the PR
//! stages on every read surface — `workspace.list`, the `workspace.subscribe`
//! seq-0 snapshot, `workspace.get` — and on the transition recompute. A
//! merged git-root PR reads `pr_merged` once the tasks are complete; an open
//! one holds `pr_open` even with every task complete. Drives a real
//! [`WsApiServer`] over TLS with bearer-token auth and a pinned self-signed
//! fingerprint (the production transport path).

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, Result as CoreResult, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceGitRootSource, WorkspaceId, WorkspaceStatus,
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

/// A workspace with no PR linkage and no repo info of its own: the only PR
/// signal it can carry comes from its registered git roots.
fn workspace(id: &WorkspaceId, title: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: title.into(),
        branch: String::new(),
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
    }
}

fn pr_info(number: u64, status: PullRequestStatus) -> PullRequestInfo {
    PullRequestInfo {
        id: number.to_string(),
        number,
        url: format!("https://github.com/o/r/pull/{number}"),
        title: format!("Root PR {number}"),
        status,
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

/// The same store seed the other git-root e2e tests use in place of
/// `ws.git.registerRoot` (which needs a real repo on disk).
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

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    /// Git root carries one merged PR; no workspace-level PR linkage.
    ws_merged: WorkspaceId,
    /// Git root carries one open PR; no workspace-level PR linkage.
    ws_open: WorkspaceId,
    _dir: tempfile::TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over two seeded workspaces whose
/// only PR signal lives on a secondary git root.
async fn boot() -> Fixture {
    let dir_guard = common::test_tempdir("intentd-display-status-git-root-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_merged = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_merged, "Merged root PR"))
        .await
        .expect("seed ws_merged");
    store
        .upsert_workspace_git_root(&git_root(
            &ws_merged,
            "/tmp/root-merged",
            vec![pr_info(1, PullRequestStatus::Merged)],
        ))
        .await
        .expect("ws_merged root");

    let ws_open = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_open, "Open root PR"))
        .await
        .expect("seed ws_open");
    store
        .upsert_workspace_git_root(&git_root(
            &ws_open,
            "/tmp/root-open",
            vec![pr_info(2, PullRequestStatus::Open)],
        ))
        .await
        .expect("ws_open root");

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
        ws_merged,
        ws_open,
        _dir: dir_guard,
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
    assert_eq!(v["jsonrpc"], json!("2.0"), "envelope: {v}");
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

/// Create a spec-child task note over the wire in the given status.
async fn seed_task(rpc: &mut TlsWs, id: i64, ws: &WorkspaceId, status: &str) -> String {
    let created = wss_rpc(
        rpc,
        id,
        "note.create",
        json!({ "workspaceId": ws.as_str(), "title": "Task", "parentId": "spec" }),
    )
    .await;
    let note_id = created["note"]["id"].as_str().expect("note id").to_string();
    let marked = wss_rpc(
        rpc,
        id + 1,
        "task.markAsTask",
        json!({ "workspaceId": ws.as_str(), "noteId": note_id, "status": status }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");
    note_id
}

/// `workspace.get` → `workspace.displayStatus`.
async fn get_status(rpc: &mut TlsWs, id: i64, ws: &WorkspaceId) -> Value {
    let got = wss_rpc(
        rpc,
        id,
        "workspace.get",
        json!({ "workspaceId": ws.as_str() }),
    )
    .await;
    got["workspace"]["displayStatus"].clone()
}

/// `displayStatus` of one row in a list-shaped row set (`workspace.list`
/// rows or the seq-0 snapshot entries).
fn row_status<'a>(rows: &'a [Value], ws: &WorkspaceId, path: &str) -> &'a Value {
    let row = rows
        .iter()
        .find(|r| r["id"] == json!(ws.as_str()))
        .unwrap_or_else(|| panic!("{path}: workspace {} listed", ws.as_str()));
    &row["displayStatus"]
}

/// The three read surfaces — `workspace.list`, the `workspace.subscribe`
/// seq-0 snapshot, and `workspace.get` — agree on the git-root-derived
/// rollup, and the transition recompute derives the same value: a merged
/// git-root PR reads `pr_merged` once the workspace's tasks are complete
/// (`idle` while one is still in progress — open tasks precede the merged
/// check), while an open git-root PR reads `pr_open` even with every task
/// complete.
#[tokio::test]
async fn git_root_pr_status_agrees_across_read_surfaces_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // ws_open: the only task is already complete; ws_merged: in progress.
    seed_task(&mut rpc, 1, &fx.ws_open, "complete").await;
    let merged_task = seed_task(&mut rpc, 3, &fx.ws_merged, "in_progress").await;

    // Baseline reads (also seed the transition baseline).
    assert_eq!(get_status(&mut rpc, 5, &fx.ws_merged).await, "idle");
    assert_eq!(get_status(&mut rpc, 6, &fx.ws_open).await, "pr_open");
    let listed = wss_rpc(&mut rpc, 7, "workspace.list", json!({})).await;
    let rows = listed["workspaces"].as_array().expect("workspaces array");
    assert_eq!(row_status(rows, &fx.ws_merged, "workspace.list"), "idle");
    assert_eq!(row_status(rows, &fx.ws_open, "workspace.list"), "pr_open");

    // Complete the remaining task: the recompute folds the merged git-root
    // PR and pushes the transition.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_merged.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");
    let updated = wss_rpc(
        &mut rpc,
        8,
        "task.updateNoteStatus",
        json!({ "workspaceId": fx.ws_merged.as_str(), "noteId": merged_task, "status": "complete" }),
    )
    .await;
    assert_eq!(updated["ok"], true, "updateNoteStatus ok: {updated}");
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["workspaceId"], fx.ws_merged.as_str());
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_merged.as_str(), "displayStatus": "pr_merged" })
    );

    // workspace.get
    assert_eq!(get_status(&mut rpc, 11, &fx.ws_merged).await, "pr_merged");
    assert_eq!(get_status(&mut rpc, 12, &fx.ws_open).await, "pr_open");

    // workspace.list
    let listed = wss_rpc(&mut rpc, 13, "workspace.list", json!({})).await;
    let rows = listed["workspaces"].as_array().expect("workspaces array");
    assert_eq!(
        row_status(rows, &fx.ws_merged, "workspace.list"),
        "pr_merged"
    );
    assert_eq!(row_status(rows, &fx.ws_open, "workspace.list"), "pr_open");

    // workspace.subscribe seq-0 snapshot (the lite list path).
    let mut snap_conn = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(&mut snap_conn, 20, "workspace.subscribe", json!({})).await;
    let sub_id = sub_res["subscriptionId"].as_str().expect("subscriptionId");
    let push = next_subscription_push(&mut snap_conn).await;
    assert_eq!(push["subscriptionId"], json!(sub_id), "push: {push}");
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    assert_eq!(push["seq"], json!(0), "push: {push}");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    assert_eq!(
        row_status(snap, &fx.ws_merged, "subscribe seq-0"),
        "pr_merged"
    );
    assert_eq!(row_status(snap, &fx.ws_open, "subscribe seq-0"), "pr_open");
}
