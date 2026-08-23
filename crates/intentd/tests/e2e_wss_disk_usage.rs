//! WSS end-to-end for the on-demand `workspace.diskUsage` method (PROTOCOL
//! §5.1, monorepo#1396): the first call answers `{ refreshing: true }` with
//! `diskUsage` omitted (the walk runs detached and backfills), a follow-up
//! poll observes the computed `{ bytes, fileCount, computedAt, breakdown }`
//! payload and a settled fresh entry reads `refreshing: false`; rows without
//! a daemon-managed directory (skip-isolation) answer `{ refreshing: false }`
//! without the field; an unknown id is the standard not-found error; and
//! `workspace.list` / `workspace.get` rows never carry `diskUsage` (the
//! aggregate left the hot read path). Drives a real [`WsApiServer`] over TLS
//! with bearer-token auth and a pinned self-signed fingerprint (the
//! production transport path).

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, Result as CoreResult, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
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
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_id: WorkspaceId,
    skip_ws_id: WorkspaceId,
    _dir: TempDir,
}

/// Seed a minimal workspace row. `worktree_path` decides whether the row has
/// a daemon-managed directory; `skip_worktree` marks the direct-mode row.
fn seed_workspace(title: &str, worktree_path: Option<String>, skip_worktree: bool) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: WorkspaceId::new(),
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
        worktree_path,
        scope: None,
        skip_worktree,
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
        display_status: None,
        waiting: false,
        token_usage: None,
        cow_supported: None,
        checkout_mode: None,
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Boot a TLS + bearer-auth WSS listener over a hermetic workspaces root
/// seeded with two rows: one with a daemon-managed directory
/// (`<root>/<id>/repo` checkout containing a file of known size) and one
/// direct-mode (`skipWorktree: true`, no directory at all).
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-disk-usage-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    // Managed row: <root>/<id>/repo with real content to be walked.
    let mut managed = seed_workspace("Disk usage", None, false);
    let ws_dir = workspaces_root.join(managed.id.as_str());
    let checkout = ws_dir.join("repo");
    std::fs::create_dir_all(&checkout).expect("mkdir checkout");
    std::fs::write(checkout.join("data.bin"), vec![0xAB; 8192]).expect("seed file");
    managed.worktree_path = Some(checkout.to_string_lossy().into_owned());
    store
        .insert_workspace(&managed)
        .await
        .expect("seed managed");

    // Direct-mode row: no daemon-managed directory ⇒ diskUsage never appears.
    let skip = seed_workspace("Direct mode", None, true);
    store.insert_workspace(&skip).await.expect("seed skip");

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
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        ws_id: managed.id,
        skip_ws_id: skip.id,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response envelope
/// (`id` / `jsonrpc` / `result` or `error`).
async fn wss_rpc_envelope(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Assert the `diskUsage` payload shape: physical bytes cover the seeded
/// 8 KiB file, `fileCount` counts it, `computedAt` is present, and the
/// breakdown names the `repo` top-level directory.
fn assert_disk_usage_shape(du: &Value) {
    let bytes = du["bytes"].as_u64().expect("bytes is u64");
    assert!(bytes >= 8192, "allocated bytes cover the seeded file: {du}");
    assert_eq!(du["fileCount"], json!(1), "one seeded file: {du}");
    assert!(
        du["computedAt"].as_str().is_some_and(|s| !s.is_empty()),
        "computedAt present: {du}"
    );
    let breakdown = du["breakdown"].as_array().expect("breakdown array");
    assert_eq!(breakdown.len(), 1, "single top-level entry: {du}");
    assert_eq!(breakdown[0]["name"], json!("repo"));
    assert_eq!(breakdown[0]["bytes"], json!(bytes));
    assert_eq!(breakdown[0]["fileCount"], json!(1));
}

/// `workspace.diskUsage` serves the managed row on demand: the first call
/// answers `{ refreshing: true }` with `diskUsage` omitted (never `null`), a
/// bounded poll then observes the computed payload, and a settled fresh
/// entry reads `refreshing: false`. The direct-mode row answers
/// `{ refreshing: false }` without the field, and an unknown id is the
/// standard not-found error envelope.
#[tokio::test]
async fn disk_usage_method_serves_on_demand_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // First call: walk armed in the background, field omitted — never null.
    let first = wss_rpc(
        &mut rpc,
        1,
        "workspace.diskUsage",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(first["refreshing"], json!(true), "first call arms: {first}");
    assert!(
        first.get("diskUsage").is_none(),
        "first call omits diskUsage: {first}"
    );

    // Poll until the backfill lands with a settled `refreshing: false`
    // (bounded). The walk is fast; the fresh TTL (~60s) far exceeds the
    // poll budget, so a computed entry must eventually read settled.
    let mut settled = None;
    for attempt in 0..100i64 {
        let got = wss_rpc(
            &mut rpc,
            10 + attempt,
            "workspace.diskUsage",
            json!({ "workspaceId": fx.ws_id.as_str() }),
        )
        .await;
        if got.get("diskUsage").is_some() && got["refreshing"] == json!(false) {
            settled = Some(got);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = settled.expect("diskUsage backfilled + settled within the poll budget");
    assert_disk_usage_shape(&got["diskUsage"]);

    // Direct-mode row (no daemon-managed dir): no walk, nothing to refresh.
    let skip = wss_rpc(
        &mut rpc,
        200,
        "workspace.diskUsage",
        json!({ "workspaceId": fx.skip_ws_id.as_str() }),
    )
    .await;
    assert_eq!(skip["refreshing"], json!(false), "direct-mode row: {skip}");
    assert!(
        skip.get("diskUsage").is_none(),
        "direct-mode row never grows diskUsage: {skip}"
    );

    // Unknown workspaceId: standard not-found error envelope.
    let missing = wss_rpc_envelope(
        &mut rpc,
        300,
        "workspace.diskUsage",
        json!({ "workspaceId": "ws_does_not_exist" }),
    )
    .await;
    assert_eq!(missing["jsonrpc"], json!("2.0"));
    assert_eq!(missing["error"]["code"], json!(-32602), "{missing}");
    assert_eq!(missing["error"]["message"], json!("Workspace not found"));
}

/// The aggregate left the hot read path: `workspace.get` and `workspace.list`
/// rows never carry `diskUsage`, even after an on-demand call populated the
/// cache for the same workspace.
#[tokio::test]
async fn disk_usage_never_appears_on_list_or_get_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // Populate the cache via the on-demand method (bounded poll).
    let mut populated = false;
    for attempt in 0..100i64 {
        let got = wss_rpc(
            &mut rpc,
            1 + attempt,
            "workspace.diskUsage",
            json!({ "workspaceId": fx.ws_id.as_str() }),
        )
        .await;
        if got.get("diskUsage").is_some() {
            populated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(populated, "cache populated within the poll budget");

    // Even with a warm cache the list/get rows omit the field.
    let got = wss_rpc(
        &mut rpc,
        200,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(
        got["workspace"].get("diskUsage").is_none(),
        "workspace.get row omits diskUsage: {got}"
    );

    let listed = wss_rpc(&mut rpc, 201, "workspace.list", json!({})).await;
    let rows = listed["workspaces"].as_array().expect("workspaces array");
    for row in rows {
        assert!(
            row.get("diskUsage").is_none(),
            "workspace.list row omits diskUsage: {row}"
        );
    }
}
