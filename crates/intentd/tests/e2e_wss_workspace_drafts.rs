//! Workspace-draft lifecycle over the production JSON-RPC WebSocket router.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
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
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;

type Ws = common::TlsWs;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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
async fn promotion_restart_after_create_recovers_one_workspace_agent_and_turn() {
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
    let probe_store = Store::open(&root.0.join("intentd.db"))
        .await
        .expect("probe store");
    assert_eq!(
        probe_store
            .get_idempotent("", operation_key)
            .await
            .expect("read idempotency row"),
        None,
        "interruption must occur before the workspace.create result is cached"
    );
    let workspace_id = interrupted_draft["promotedWorkspaceId"]
        .as_str()
        .expect("workspace mapping committed before interruption")
        .to_string();
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

fn make_repo(root: &Path) -> PathBuf {
    let repo = root.join("repo");
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
