//! Workspace-draft lifecycle over the production JSON-RPC WebSocket router.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once};

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    Result as CoreResult, SetupResult, SetupResultState, WorkspaceApi, WorkspaceDraftId,
    WorkspaceId,
};
use intent_services::{EventBus, Services, WorkspaceDraftPromotionFailpoint};
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;

type Ws = common::TlsWs;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

fn use_short_cache_clone_timeout() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| std::env::set_var("INTENTD_CACHE_CLONE_TIMEOUT_SECS", "1"));
}

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
        let fingerprint = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fingerprint == self.fingerprint {
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
    Arc::new(
        ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
                fingerprint: fingerprint.to_string(),
                provider,
            }))
            .with_no_client_auth(),
    )
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "intentd-workspace-draft-e2e-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn boot(root: &Path) -> (WsApiServer, u16, Arc<ClientConfig>) {
    boot_with_failpoint(root, None).await
}

async fn boot_with_failpoint(
    root: &Path,
    failpoint: Option<WorkspaceDraftPromotionFailpoint>,
) -> (WsApiServer, u16, Arc<ClientConfig>) {
    let store = Store::open(&root.join("intentd.db")).await.expect("store");
    store
        .reconcile_interrupted_setup_results()
        .await
        .expect("reconcile interrupted setup results");
    let bus = EventBus::new(store.clone());
    let mut services = Services::new(store)
        .with_workspaces_root(root.join("workspaces"))
        .with_event_bus(bus.clone());
    if let Some(failpoint) = failpoint {
        services = services.with_workspace_draft_promotion_failpoint(failpoint);
    }
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(root).expect("certificate");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let server = WsApiServer::new(
        api,
        bus,
        &tls,
        &token_store,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        None,
    )
    .expect("server");
    let config = client_config(&tls.fingerprint256);
    let port = server.start().await.expect("start");
    (server, port, config)
}

#[tokio::test]
async fn promotion_restart_after_workspace_insert_recovers_agent_and_turn() {
    let root = TempDir::new();
    let repo = make_repo(&root.0);
    let fail_draft = Arc::new(Mutex::new(None::<String>));
    let fail_draft_probe = fail_draft.clone();
    let failpoint: WorkspaceDraftPromotionFailpoint = Arc::new(move |draft_id| {
        fail_draft_probe.lock().unwrap().as_deref() == Some(draft_id.as_str())
    });
    let (server, port, config) = boot_with_failpoint(&root.0, Some(failpoint)).await;
    let mut ws = connect(port, config).await;
    let draft = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({
            "intentText":"crash-safe",
            "source":{"kind":"local","path":repo,"branch":"main","isolation":"in-place"}
        }),
    )
    .await;
    let draft_id = draft["id"].as_str().unwrap().to_string();
    *fail_draft.lock().unwrap() = Some(draft_id.clone());
    let interrupted = rpc_raw(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({
            "id":draft_id,
            "expectedRevision":0,
            "initialAgent":{"name":"Coordinator","provider":"codex","prompt":"first turn"}
        }),
    )
    .await;
    assert_eq!(interrupted["error"]["code"], -32603);
    let interrupted_draft = rpc(&mut ws, 3, "workspaceDraft.get", json!({"id":draft_id})).await;
    assert_eq!(interrupted_draft["phase"], "failed");
    let operation_key = interrupted_draft["operationKey"].as_str().unwrap();
    let workspace_id = interrupted_draft["promotedWorkspaceId"]
        .as_str()
        .expect("workspace mapping committed before interruption")
        .to_string();
    let workspace_id_typed = WorkspaceId::from(workspace_id.as_str());
    let probe_store = Store::open(&root.0.join("intentd.db"))
        .await
        .expect("probe store");
    probe_store
        .get_workspace(&workspace_id_typed)
        .await
        .expect("workspace row committed before interruption");
    assert!(
        probe_store
            .list_agent_session_summaries(&workspace_id_typed)
            .await
            .expect("probe agents")
            .is_empty(),
        "failpoint must interrupt before initial-agent creation"
    );
    assert_eq!(
        probe_store
            .get_idempotent("", operation_key)
            .await
            .expect("read idempotency row"),
        None,
        "interruption must occur before the workspace.create result is cached"
    );
    drop(ws);
    drop(server);

    let (_restarted, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let recovered = rpc(
        &mut ws,
        4,
        "workspaceDraft.promote",
        json!({
            "id":draft_id,
            "expectedRevision":0,
            "initialAgent":{"name":"Coordinator","provider":"codex","prompt":"first turn"}
        }),
    )
    .await;
    assert_eq!(recovered["workspace"]["id"], workspace_id);
    assert_eq!(recovered["draft"]["phase"], "promoted");
    let agent_id = recovered["initialAgent"]["id"]
        .as_str()
        .expect("original initial agent recovered")
        .to_string();

    let workspaces = rpc(&mut ws, 5, "workspace.list", json!({})).await;
    assert_eq!(workspaces["workspaces"].as_array().unwrap().len(), 1);
    let agents = rpc(
        &mut ws,
        6,
        "agent.list",
        json!({"workspaceId":workspace_id}),
    )
    .await;
    assert_eq!(agents["agents"].as_array().unwrap().len(), 1);
    assert_eq!(agents["agents"][0]["id"], agent_id);
    let conversation = rpc(
        &mut ws,
        7,
        "agent.getConversation",
        json!({"workspaceId":workspace_id,"agentId":agent_id}),
    )
    .await;
    let first_turns = conversation["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .count();
    assert_eq!(first_turns, 1, "restart must not duplicate the first turn");
}

async fn connect(port: u16, config: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, config, &url).await
}

async fn rpc_raw(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["id"] == id {
                        return value;
                    }
                }
                Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                _ => {}
            }
        }
    })
    .await
    .expect("RPC response")
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let response = rpc_raw(ws, id, method, params).await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

async fn send_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) {
    let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
}

async fn wait_for_setup_state(ws: &mut Ws, workspace_id: &str, state: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let workspace = rpc(ws, 91, "workspace.get", json!({"workspaceId":workspace_id})).await
            ["workspace"]
            .clone();
        if workspace["setupResult"]["state"] == state {
            return workspace;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "setupResult: {workspace}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_failed_draft_without_workspace(
    ws: &mut Ws,
    draft_id: &str,
    expected_error_fragment: &str,
) {
    let retained = rpc(ws, 92, "workspaceDraft.get", json!({"id":draft_id})).await;
    assert_eq!(retained["phase"], "failed");
    assert!(
        retained["lastError"]
            .as_str()
            .is_some_and(|error| error.to_lowercase().contains(expected_error_fragment)),
        "retained draft error: {retained}"
    );
    let workspaces = rpc(ws, 93, "workspace.list", json!({})).await;
    assert!(workspaces["workspaces"].as_array().unwrap().is_empty());
}

async fn spawn_http_error_server(status: &'static str, delay: Duration) -> (u16, Arc<Notify>) {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind HTTP fixture");
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(Notify::new());
    let accepted_task = accepted.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            accepted_task.notify_one();
            tokio::spawn(async move {
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await;
                sleep(delay).await;
                let response =
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (port, accepted)
}

fn make_repo(root: &Path) -> PathBuf {
    make_repo_named(root, "repo")
}

fn make_repo_named(root: &Path, name: &str) -> PathBuf {
    let repo = root.join(name);
    std::fs::create_dir_all(&repo).unwrap();
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Test"],
    ] {
        assert!(std::process::Command::new("git")
            .args(args)
            .current_dir(&repo)
            .status()
            .unwrap()
            .success());
    }
    std::fs::write(repo.join("README.md"), "test\n").unwrap();
    assert!(std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo)
        .status()
        .unwrap()
        .success());
    assert!(std::process::Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(&repo)
        .status()
        .unwrap()
        .success());
    repo
}

async fn assert_single_workspace_agent_and_turn(ws: &mut Ws, workspace_id: &str, agent_id: &str) {
    let workspaces = rpc(ws, 94, "workspace.list", json!({})).await;
    assert_eq!(workspaces["workspaces"].as_array().unwrap().len(), 1);
    let agents = rpc(ws, 95, "agent.list", json!({"workspaceId":workspace_id})).await;
    assert_eq!(agents["agents"].as_array().unwrap().len(), 1);
    assert_eq!(agents["agents"][0]["id"], agent_id);
    let conversation = rpc(
        ws,
        96,
        "agent.getConversation",
        json!({"workspaceId":workspace_id,"agentId":agent_id}),
    )
    .await;
    assert_eq!(
        conversation["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "user")
            .count(),
        1,
        "promotion retry must not duplicate the initial turn"
    );
}

#[tokio::test]
async fn draft_crud_promote_replay_events_and_setup_result() {
    let root = TempDir::new();
    let repo = make_repo(&root.0);
    let (_server, port, config) = boot(&root.0).await;
    let mut sub = connect(port, config.clone()).await;
    rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"eventTypes":["workspace-draft:updated","workspace-draft:promoted","workspace-draft:deleted"]}),
    )
    .await;
    let mut ws = connect(port, config).await;
    let created = rpc(
        &mut ws,
        2,
        "workspaceDraft.create",
        json!({
            "intentText":"Build it",
            "source":{"kind":"local","path":repo,"branch":"main","isolation":"worktree"},
            "config":{"setupScript":"exit 0"}
        }),
    )
    .await;
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["revision"], 0);

    let updated = rpc(
        &mut ws,
        3,
        "workspaceDraft.update",
        json!({"id":id,"expectedRevision":0,"patch":{"title":"Untitled"}}),
    )
    .await;
    assert_eq!(updated["revision"], 1);
    let conflict = rpc_raw(
        &mut ws,
        4,
        "workspaceDraft.update",
        json!({"id":id,"expectedRevision":0,"patch":{"intentText":"stale"}}),
    )
    .await;
    assert_eq!(conflict["error"]["code"], -32009);
    assert_eq!(conflict["error"]["data"]["current"]["revision"], 1);

    let promoted = rpc(
        &mut ws,
        5,
        "workspaceDraft.promote",
        json!({"id":id,"expectedRevision":1,"initialAgent":{"name":"Coordinator","provider":"codex"}}),
    )
    .await;
    let workspace_id = promoted["workspace"]["id"].as_str().unwrap();
    assert!(promoted["initialAgent"]["id"].is_string());
    assert_eq!(promoted["initialAgent"]["status"], "Idle");
    let replay = rpc(
        &mut ws,
        6,
        "workspaceDraft.promote",
        json!({"id":id,"expectedRevision":1}),
    )
    .await;
    assert_eq!(replay["workspace"]["id"], workspace_id);
    assert_eq!(replay["initialAgent"]["id"], promoted["initialAgent"]["id"]);

    let delivered = rpc(
        &mut ws,
        7,
        "workspaceDraft.markDelivery",
        json!({"id":id,"delivery":{"state":"sent","messageId":"message-1"}}),
    )
    .await;
    assert_eq!(delivered["delivery"]["state"], "sent");
    assert_eq!(delivered["delivery"]["messageId"], "message-1");
    assert_eq!(
        rpc(&mut ws, 8, "workspaceDraft.list", json!({})).await,
        json!([])
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let fetched = rpc(
            &mut ws,
            9,
            "workspace.get",
            json!({"workspaceId":workspace_id}),
        )
        .await;
        if fetched["workspace"]["setupResult"]["state"] == "succeeded" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "setupResult: {fetched}"
        );
        sleep(Duration::from_millis(50)).await;
    }
    let listed = rpc(&mut ws, 10, "workspace.list", json!({})).await;
    let workspace = listed["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|workspace| workspace["id"] == workspace_id)
        .unwrap();
    assert_eq!(workspace["setupResult"]["state"], "succeeded");

    assert_eq!(
        rpc(&mut ws, 11, "workspaceDraft.delete", json!({"id":id})).await,
        json!({"deleted":true})
    );
    assert_eq!(
        rpc(&mut ws, 12, "workspaceDraft.delete", json!({"id":id})).await,
        json!({"deleted":false})
    );

    let mut saw_updated = false;
    let mut saw_delivery = false;
    let mut saw_promoted = false;
    let mut saw_deleted = false;
    let event_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < event_deadline
        && !(saw_updated && saw_delivery && saw_promoted && saw_deleted)
    {
        let Ok(Some(Ok(Message::Text(text)))) =
            timeout(Duration::from_millis(250), sub.next()).await
        else {
            continue;
        };
        let event: Value = serde_json::from_str(&text).unwrap();
        let event = &event["params"]["event"];
        assert_eq!(event["workspaceId"], "");
        match event["type"].as_str() {
            Some("workspace-draft:updated") => {
                assert_eq!(event["data"]["draft"]["id"], id);
                saw_updated = true;
                saw_delivery |= event["data"]["draft"]["delivery"]["state"] == "sent";
            }
            Some("workspace-draft:promoted") => {
                assert_eq!(event["data"]["draftId"], id);
                assert_eq!(event["data"]["workspaceId"], workspace_id);
                assert_eq!(
                    event["data"]["initialAgentId"],
                    promoted["initialAgent"]["id"]
                );
                saw_promoted = true;
            }
            Some("workspace-draft:deleted") => {
                assert_eq!(event["data"]["draftId"], id);
                saw_deleted = true;
            }
            _ => {}
        }
    }
    assert!(saw_updated && saw_delivery && saw_promoted && saw_deleted);
}

#[tokio::test]
async fn failed_promotion_retains_and_restart_restores_draft() {
    let root = TempDir::new();
    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let draft = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({"intentText":"retain","source":{"kind":"newFolder","parentPath":"/dev/null","name":"nope"}}),
    )
    .await;
    let id = draft["id"].as_str().unwrap().to_string();
    let failed = rpc_raw(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({"id":id,"expectedRevision":0}),
    )
    .await;
    assert!(failed["error"]["code"].is_number(), "{failed}");
    let retained = rpc(&mut ws, 3, "workspaceDraft.get", json!({"id":id})).await;
    assert_eq!(retained["phase"], "failed");
    assert!(retained["lastError"].is_string());
    drop(ws);
    drop(server);

    let (_restarted, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let restored = rpc(&mut ws, 4, "workspaceDraft.get", json!({"id":id})).await;
    assert_eq!(restored["phase"], "failed");
    assert_eq!(restored["intentText"], "retain");
}

#[tokio::test]
async fn new_folder_promotion_initializes_main_and_rejects_non_empty_target() {
    let root = TempDir::new();
    let (_server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;

    let fresh_name = "fresh-project";
    let fresh_path = root.0.join(fresh_name);
    let fresh = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({
            "intentText":"start fresh",
            "source":{"kind":"newFolder","parentPath":root.0,"name":fresh_name}
        }),
    )
    .await;
    let promoted = rpc(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({"id":fresh["id"],"expectedRevision":0}),
    )
    .await;
    assert_eq!(
        promoted["workspace"]["repositoryPath"],
        fresh_path.to_string_lossy().as_ref()
    );
    assert_eq!(promoted["workspace"]["skipWorktree"], true);
    let head = std::process::Command::new("git")
        .args(["symbolic-ref", "HEAD"])
        .current_dir(&fresh_path)
        .output()
        .unwrap();
    assert!(head.status.success());
    assert_eq!(
        String::from_utf8_lossy(&head.stdout).trim(),
        "refs/heads/main"
    );

    let occupied_name = "occupied-project";
    let occupied_path = root.0.join(occupied_name);
    std::fs::create_dir_all(&occupied_path).unwrap();
    std::fs::write(occupied_path.join("keep.txt"), "keep\n").unwrap();
    let occupied = rpc(
        &mut ws,
        3,
        "workspaceDraft.create",
        json!({
            "intentText":"retain me",
            "source":{"kind":"newFolder","parentPath":root.0,"name":occupied_name}
        }),
    )
    .await;
    let rejected = rpc_raw(
        &mut ws,
        4,
        "workspaceDraft.promote",
        json!({"id":occupied["id"],"expectedRevision":0}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32602);
    let retained = rpc(
        &mut ws,
        5,
        "workspaceDraft.get",
        json!({"id":occupied["id"]}),
    )
    .await;
    assert_eq!(retained["phase"], "failed");
    assert_eq!(retained["intentText"], "retain me");
    assert_eq!(
        retained["lastError"],
        format!(
            "invalid params: new project directory already exists and is not empty: {}",
            occupied_path.display()
        )
    );
    assert!(!occupied_path.join(".git").exists());
    assert_eq!(
        std::fs::read_to_string(occupied_path.join("keep.txt")).unwrap(),
        "keep\n"
    );
}

#[tokio::test]
async fn restart_restores_acknowledged_draft_boundaries_and_lost_promote_ack() {
    let root = TempDir::new();
    let repo = make_repo(&root.0);
    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let created = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({"ownerClientId":"restart-client"}),
    )
    .await;
    let draft_id = created["id"].as_str().unwrap().to_string();
    let operation_key = created["operationKey"].clone();
    assert_eq!(created["phase"], "editing");
    drop(ws);
    drop(server);

    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let restored_created = rpc(&mut ws, 2, "workspaceDraft.get", json!({"id":draft_id})).await;
    assert_eq!(restored_created["operationKey"], operation_key);
    assert_eq!(restored_created["intentText"], "");
    let edited = rpc(
        &mut ws,
        3,
        "workspaceDraft.update",
        json!({"id":draft_id,"expectedRevision":0,"patch":{"intentText":"acknowledged"}}),
    )
    .await;
    assert_eq!(edited["revision"], 1);
    let unsent_client_value = "not sent before the debounce fired";
    drop(ws);
    drop(server);

    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let restored_edit = rpc(&mut ws, 4, "workspaceDraft.get", json!({"id":draft_id})).await;
    assert_eq!(restored_edit["intentText"], "acknowledged");
    assert_ne!(restored_edit["intentText"], unsent_client_value);
    let source_selected = rpc(
        &mut ws,
        5,
        "workspaceDraft.update",
        json!({
            "id":draft_id,
            "expectedRevision":1,
            "patch":{"source":{"kind":"local","path":repo,"branch":"main","isolation":"in-place"}}
        }),
    )
    .await;
    assert_eq!(source_selected["revision"], 2);
    drop(ws);
    drop(server);

    let probe_store = Store::open(&root.0.join("intentd.db")).await.unwrap();
    let promoting = probe_store
        .set_workspace_draft_phase(
            &WorkspaceDraftId::from(draft_id.as_str()),
            intent_core::DraftPhase::Promoting,
            None,
        )
        .await
        .unwrap();
    assert_eq!(promoting.revision, 3);
    probe_store.close().await;
    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config.clone()).await;
    let restored_promoting = rpc(&mut ws, 6, "workspaceDraft.get", json!({"id":draft_id})).await;
    assert_eq!(restored_promoting["phase"], "promoting");

    send_rpc(
        &mut ws,
        7,
        "workspaceDraft.promote",
        json!({
            "id":draft_id,
            "expectedRevision":2,
            "initialAgent":{"name":"Coordinator","provider":"codex","prompt":"first turn"}
        }),
    )
    .await;
    let completion_store = Store::open(&root.0.join("intentd.db")).await.unwrap();
    let promoted = timeout(Duration::from_secs(10), async {
        loop {
            let draft = completion_store
                .get_workspace_draft(&WorkspaceDraftId::from(draft_id.as_str()))
                .await
                .unwrap();
            if draft.phase == intent_core::DraftPhase::Promoted {
                break draft;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("promotion completed before dropped ACK");
    let workspace_id = promoted.promoted_workspace_id.unwrap().0;
    let agent_id = promoted.initial_agent_id.unwrap().0;
    drop(ws);
    drop(server);
    completion_store.close().await;

    let (_server, port, config) = boot(&root.0).await;
    let mut fresh_ws = connect(port, config).await;
    let replay = rpc(
        &mut fresh_ws,
        8,
        "workspaceDraft.promote",
        json!({"id":draft_id,"expectedRevision":2}),
    )
    .await;
    assert_eq!(replay["workspace"]["id"], workspace_id);
    assert_eq!(replay["initialAgent"]["id"], agent_id);
    assert_eq!(replay["draft"]["operationKey"], operation_key);
    assert_single_workspace_agent_and_turn(&mut fresh_ws, &workspace_id, &agent_id).await;
}

#[tokio::test]
async fn concurrent_clients_surface_revision_conflict_and_share_one_promotion() {
    let root = TempDir::new();
    let repo = make_repo(&root.0);
    let (_server, port, config) = boot(&root.0).await;
    let mut first = connect(port, config.clone()).await;
    let mut second = connect(port, config).await;
    let created = rpc(
        &mut first,
        1,
        "workspaceDraft.create",
        json!({
            "intentText":"original",
            "source":{"kind":"local","path":repo,"branch":"main","isolation":"in-place"}
        }),
    )
    .await;
    let draft_id = created["id"].as_str().unwrap().to_string();
    let (left, right) = tokio::join!(
        rpc_raw(
            &mut first,
            2,
            "workspaceDraft.update",
            json!({"id":draft_id,"expectedRevision":0,"patch":{"title":"left"}}),
        ),
        rpc_raw(
            &mut second,
            3,
            "workspaceDraft.update",
            json!({"id":draft_id,"expectedRevision":0,"patch":{"title":"right"}}),
        )
    );
    let responses = [&left, &right];
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.get("result").is_some())
            .count(),
        1
    );
    let conflict = responses
        .iter()
        .find(|response| response.get("error").is_some())
        .unwrap();
    assert_eq!(conflict["error"]["code"], -32009);
    assert_eq!(conflict["error"]["data"]["current"]["revision"], 1);

    let (first_promotion, second_promotion) = tokio::join!(
        rpc(
            &mut first,
            4,
            "workspaceDraft.promote",
            json!({
                "id":draft_id,"expectedRevision":1,
                "initialAgent":{"name":"Coordinator","provider":"codex","prompt":"one turn"}
            }),
        ),
        rpc(
            &mut second,
            5,
            "workspaceDraft.promote",
            json!({
                "id":draft_id,"expectedRevision":1,
                "initialAgent":{"name":"Coordinator","provider":"codex","prompt":"one turn"}
            }),
        )
    );
    assert_eq!(
        first_promotion["workspace"]["id"],
        second_promotion["workspace"]["id"]
    );
    assert_eq!(
        first_promotion["initialAgent"]["id"],
        second_promotion["initialAgent"]["id"]
    );
    assert_single_workspace_agent_and_turn(
        &mut first,
        first_promotion["workspace"]["id"].as_str().unwrap(),
        first_promotion["initialAgent"]["id"].as_str().unwrap(),
    )
    .await;
}

#[tokio::test]
async fn setup_result_covers_absent_running_failures_and_listener_reconnect() {
    let root = TempDir::new();
    let absent_repo = make_repo_named(&root.0, "absent-repo");
    let failing_repo = make_repo_named(&root.0, "failing-repo");
    let prespawn_repo = make_repo_named(&root.0, "prespawn-repo");
    let missing_interpreter_repo = make_repo_named(&root.0, "missing-interpreter-repo");
    std::fs::write(prespawn_repo.join(".intent"), "block directory creation\n").unwrap();
    assert!(std::process::Command::new("git")
        .args(["add", ".intent"])
        .current_dir(&prespawn_repo)
        .status()
        .unwrap()
        .success());
    assert!(std::process::Command::new("git")
        .args(["commit", "-m", "block setup directory"])
        .current_dir(&prespawn_repo)
        .status()
        .unwrap()
        .success());

    let (_server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config.clone()).await;
    let absent = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({"source":{"kind":"local","path":absent_repo,"branch":"main","isolation":"worktree"}}),
    )
    .await;
    let absent_promotion = rpc(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({"id":absent["id"],"expectedRevision":0}),
    )
    .await;
    let absent_workspace = rpc(
        &mut ws,
        3,
        "workspace.get",
        json!({"workspaceId":absent_promotion["workspace"]["id"]}),
    )
    .await;
    assert!(absent_workspace["workspace"].get("setupResult").is_none());

    let failing = rpc(
        &mut ws,
        4,
        "workspaceDraft.create",
        json!({
            "source":{"kind":"local","path":failing_repo,"branch":"main","isolation":"worktree"},
            "config":{"setupScript":"sleep 1; exit 23"}
        }),
    )
    .await;
    let failing_promotion = rpc(
        &mut ws,
        5,
        "workspaceDraft.promote",
        json!({"id":failing["id"],"expectedRevision":0}),
    )
    .await;
    let failing_workspace_id = failing_promotion["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let running = wait_for_setup_state(&mut ws, &failing_workspace_id, "running").await;
    assert!(running["setupResult"]["startedAt"].is_string());
    assert!(running["setupResult"].get("finishedAt").is_none());
    assert!(running["setupResult"].get("exitCode").is_none());
    assert!(running["setupResult"].get("error").is_none());
    drop(ws);
    let mut reconnected = connect(port, config).await;
    let failed = wait_for_setup_state(&mut reconnected, &failing_workspace_id, "failed").await;
    assert_eq!(failed["setupResult"]["exitCode"], 23);
    assert_eq!(
        failed["setupResult"]["error"],
        "setup script exited with code 23"
    );

    let prespawn = rpc(
        &mut reconnected,
        6,
        "workspaceDraft.create",
        json!({
            "source":{"kind":"local","path":prespawn_repo,"branch":"main","isolation":"worktree"},
            "config":{"setupScript":"echo should-not-run"}
        }),
    )
    .await;
    let prespawn_promotion = rpc(
        &mut reconnected,
        7,
        "workspaceDraft.promote",
        json!({"id":prespawn["id"],"expectedRevision":0}),
    )
    .await;
    let prespawn_failed = wait_for_setup_state(
        &mut reconnected,
        prespawn_promotion["workspace"]["id"].as_str().unwrap(),
        "failed",
    )
    .await;
    assert!(prespawn_failed["setupResult"].get("exitCode").is_none());
    assert_eq!(
        prespawn_failed["setupResult"]["error"],
        ".intent is not a directory"
    );

    let missing_interpreter = rpc(
        &mut reconnected,
        8,
        "workspaceDraft.create",
        json!({
            "source":{"kind":"local","path":missing_interpreter_repo,"branch":"main","isolation":"worktree"},
            "config":{"setupScript":"exec /definitely-missing/intent-setup-interpreter"}
        }),
    )
    .await;
    let missing_interpreter_promotion = rpc(
        &mut reconnected,
        9,
        "workspaceDraft.promote",
        json!({"id":missing_interpreter["id"],"expectedRevision":0}),
    )
    .await;
    let spawn_failed = wait_for_setup_state(
        &mut reconnected,
        missing_interpreter_promotion["workspace"]["id"]
            .as_str()
            .unwrap(),
        "failed",
    )
    .await;
    assert_eq!(spawn_failed["setupResult"]["exitCode"], 127);
    assert_eq!(
        spawn_failed["setupResult"]["error"],
        "setup script exited with code 127"
    );
}

#[tokio::test]
async fn restart_reconciles_unfinished_setup_result_to_unknown() {
    let root = TempDir::new();
    let repo = make_repo(&root.0);
    let (server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let draft = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({"source":{"kind":"local","path":repo,"branch":"main","isolation":"in-place"}}),
    )
    .await;
    let promotion = rpc(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({"id":draft["id"],"expectedRevision":0}),
    )
    .await;
    let workspace_id = promotion["workspace"]["id"].as_str().unwrap().to_string();
    let store = Store::open(&root.0.join("intentd.db")).await.unwrap();
    store
        .update_workspace_setup_result(
            &WorkspaceId::from(workspace_id.as_str()),
            &SetupResult {
                state: SetupResultState::Running,
                started_at: Some("2026-09-05T00:00:00Z".into()),
                ..SetupResult::default()
            },
        )
        .await
        .unwrap();
    store.close().await;
    drop(ws);
    drop(server);

    let (_restarted, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;
    let restored = rpc(
        &mut ws,
        3,
        "workspace.get",
        json!({"workspaceId":workspace_id}),
    )
    .await;
    assert_eq!(restored["workspace"]["setupResult"]["state"], "unknown");
    assert_eq!(
        restored["workspace"]["setupResult"]["startedAt"],
        "2026-09-05T00:00:00Z"
    );
}

#[tokio::test]
async fn github_clone_failures_retain_drafts_without_orphan_workspaces() {
    use_short_cache_clone_timeout();
    let root = TempDir::new();
    let (_server, port, config) = boot(&root.0).await;
    let mut ws = connect(port, config).await;

    let (forbidden_port, _) =
        spawn_http_error_server("403 Forbidden", Duration::from_millis(0)).await;
    let forbidden = rpc(
        &mut ws,
        1,
        "workspaceDraft.create",
        json!({"source":{
            "kind":"github","url":format!("http://127.0.0.1:{forbidden_port}/private/repo.git"),
            "owner":"private","name":"repo"
        }}),
    )
    .await;
    let forbidden_response = rpc_raw(
        &mut ws,
        2,
        "workspaceDraft.promote",
        json!({"id":forbidden["id"],"expectedRevision":0}),
    )
    .await;
    assert!(forbidden_response.get("error").is_some());
    assert_failed_draft_without_workspace(&mut ws, forbidden["id"].as_str().unwrap(), "403").await;

    let (timeout_port, _) = spawn_http_error_server("200 OK", Duration::from_secs(5)).await;
    let stalled = rpc(
        &mut ws,
        3,
        "workspaceDraft.create",
        json!({"source":{
            "kind":"github","url":format!("http://127.0.0.1:{timeout_port}/slow/repo.git"),
            "owner":"slow","name":"repo"
        }}),
    )
    .await;
    let timeout_response = rpc_raw(
        &mut ws,
        4,
        "workspaceDraft.promote",
        json!({"id":stalled["id"],"expectedRevision":0}),
    )
    .await;
    assert!(timeout_response.get("error").is_some());
    assert_failed_draft_without_workspace(&mut ws, stalled["id"].as_str().unwrap(), "timed out")
        .await;

    let occupied_target = root.0.join("workspaces/clones/occupied");
    std::fs::create_dir_all(&occupied_target).unwrap();
    std::fs::write(occupied_target.join("keep"), "keep").unwrap();
    let occupied = rpc(
        &mut ws,
        5,
        "workspaceDraft.create",
        json!({"source":{
            "kind":"github","url":"occupied","owner":"local","name":"occupied"
        }}),
    )
    .await;
    let occupied_response = rpc_raw(
        &mut ws,
        6,
        "workspaceDraft.promote",
        json!({"id":occupied["id"],"expectedRevision":0}),
    )
    .await;
    assert_eq!(occupied_response["error"]["code"], -32602);
    assert_eq!(
        occupied_response["error"]["data"]["code"],
        "destination-exists-non-empty"
    );
    assert_failed_draft_without_workspace(&mut ws, occupied["id"].as_str().unwrap(), "not empty")
        .await;

    let permission_source = make_repo_named(&root.0, "permission-source");
    let cache_owner = root
        .0
        .join("workspaces/.repo-cache")
        .join(root.0.file_name().unwrap());
    std::fs::create_dir_all(&cache_owner).unwrap();
    std::fs::set_permissions(&cache_owner, std::fs::Permissions::from_mode(0o500)).unwrap();
    let denied = rpc(
        &mut ws,
        7,
        "workspaceDraft.create",
        json!({"source":{
            "kind":"github","url":format!("file://{}", permission_source.display()),
            "owner":"local","name":"denied"
        }}),
    )
    .await;
    let denied_response = rpc_raw(
        &mut ws,
        8,
        "workspaceDraft.promote",
        json!({"id":denied["id"],"expectedRevision":0}),
    )
    .await;
    std::fs::set_permissions(&cache_owner, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(denied_response.get("error").is_some());
    assert_failed_draft_without_workspace(
        &mut ws,
        denied["id"].as_str().unwrap(),
        "permission denied",
    )
    .await;
}

#[tokio::test]
async fn late_clone_reply_cannot_mutate_a_switched_backend_draft() {
    use_short_cache_clone_timeout();
    let old_root = TempDir::new();
    let new_root = TempDir::new();
    let (http_port, accepted) = spawn_http_error_server("200 OK", Duration::from_secs(5)).await;
    let (_old_server, old_port, old_config) = boot(&old_root.0).await;
    let mut old_ws = connect(old_port, old_config).await;
    let old_draft = rpc(
        &mut old_ws,
        1,
        "workspaceDraft.create",
        json!({"ownerClientId":"same-client","source":{
            "kind":"github","url":format!("http://127.0.0.1:{http_port}/late/repo.git"),
            "owner":"late","name":"repo"
        }}),
    )
    .await;
    let old_id = old_draft["id"].as_str().unwrap().to_string();
    let old_promotion = tokio::spawn(async move {
        rpc_raw(
            &mut old_ws,
            2,
            "workspaceDraft.promote",
            json!({"id":old_id,"expectedRevision":0}),
        )
        .await
    });
    timeout(Duration::from_secs(5), accepted.notified())
        .await
        .expect("old backend clone reached fixture");

    let (_new_server, new_port, new_config) = boot(&new_root.0).await;
    let mut new_ws = connect(new_port, new_config).await;
    let new_draft = rpc(
        &mut new_ws,
        3,
        "workspaceDraft.create",
        json!({"ownerClientId":"same-client","intentText":"new backend truth"}),
    )
    .await;
    let old_result = old_promotion.await.unwrap();
    assert!(old_result.get("error").is_some());
    let still_new = rpc(
        &mut new_ws,
        4,
        "workspaceDraft.get",
        json!({"id":new_draft["id"]}),
    )
    .await;
    assert_eq!(still_new["phase"], "editing");
    assert_eq!(still_new["intentText"], "new backend truth");
    assert_eq!(still_new["revision"], 0);
    assert!(
        rpc(&mut new_ws, 5, "workspace.list", json!({})).await["workspaces"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
