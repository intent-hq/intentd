//! Original-socket selection intent through actual cold UDS/authenticated WSS.
//! Disposable databases, empty roots and credentials; no Git or provider action.
#![cfg(unix)]
mod common;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, Principal, PrincipalId, WorkspaceGitRootId, WorkspaceId, WorkspaceRole,
};
use intent_store::Store;
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
const TOKEN: &str = "cececececececececececececececececececececececececececececececece";
const GUEST: &str = "abababababababababababababababababababababababababababababababab";
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

struct Harness {
    child: GuardedChild,
    dir: tempfile::TempDir,
    root: PathBuf,
    workspace: WorkspaceId,
    registered: WorkspaceGitRootId,
    guest: PrincipalId,
    port: u16,
    cfg: Arc<ClientConfig>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Harness {
    async fn boot() -> Self {
        Self::boot_with_ids(None).await
    }
    async fn boot_with_ids(ids: Option<(WorkspaceId, WorkspaceGitRootId)>) -> Self {
        let dir = common::test_tempdir_in("/tmp", "itd-selection-wire-");
        let root = dir.path().join("repo");
        std::fs::create_dir(&root).unwrap();
        let registered_path = dir.path().join("secondary");
        std::fs::create_dir(&registered_path).unwrap();
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        let mut ws = intent_core::chief_workspace();
        ws.id = ids
            .as_ref()
            .map_or_else(WorkspaceId::new, |ids| ids.0.clone());
        ws.branch = "main".into();
        ws.path = Some(dir.path().join("workspace").to_string_lossy().into_owned());
        ws.repository_path = Some(root.to_string_lossy().into_owned());
        store.insert_workspace(&ws).await.unwrap();
        let registered = ids.map_or_else(WorkspaceGitRootId::new, |ids| ids.1);
        let row=serde_json::from_value(json!({"id":registered,"workspaceId":ws.id,"path":registered_path,"source":"auto","registeredByAgentIds":[],"createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
        store.upsert_workspace_git_root(&row).await.unwrap();
        let guest = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: Some(4242),
            login: Some("guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&guest).await.unwrap();
        let hash = Sha256::digest(GUEST.as_bytes()).iter().fold(
            String::with_capacity(64),
            |mut hash, byte| {
                use std::fmt::Write as _;
                write!(hash, "{byte:02x}").unwrap();
                hash
            },
        );
        store
            .insert_principal_credential(&guest.id, &hash)
            .await
            .unwrap();
        store
            .add_workspace_member(&ws.id, &guest.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        drop(store);
        std::fs::write(dir.path().join("config.toml"),"[providers]\nenabled = {}\n[mcp]\nenableUserServers = false\n[agents]\nresumeInterruptedOnStart = \"off\"\n").unwrap();
        common::enable_ws_api(dir.path());
        let bins = dir.path().join("bin");
        std::fs::create_dir(&bins).unwrap();
        std::fs::create_dir(dir.path().join("specialists")).unwrap();
        let workspaces = dir.path().join("workspaces");
        std::fs::create_dir(&workspaces).unwrap();
        let log = std::fs::File::create(dir.path().join("daemon.log")).unwrap();
        let mut cmd = common::serve_command();
        common::hermetic_github_identity(&mut cmd, dir.path());
        cmd.env("INTENTD_DATA_DIR", dir.path())
            .env("INTENTD_WORKSPACES_DIR", workspaces)
            .env("INTENTD_SECRETS_FILE", dir.path().join("secrets.json"))
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .env("INTENTD_SPECIALISTS_DIR", dir.path().join("specialists"))
            .env(
                "RUST_LOG",
                "warn,intentd=info,intent_services::repository_native_wire=debug",
            )
            .env_remove("GITLAB_TOKEN")
            .env("PATH", bins)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        let child = GuardedChild::spawn(&mut cmd).unwrap();
        let socket = dir.path().join("intentd.sock");
        timeout(common::daemon_startup_timeout(), async {
            loop {
                if UnixStream::connect(&socket).await.is_ok() {
                    break;
                } // timing-guard: socket readiness poll
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        Self {
            child,
            dir,
            root,
            workspace: ws.id,
            registered,
            guest: guest.id,
            port,
            cfg,
        }
    }
    async fn uds(&self) -> Client {
        Client {
            socket: Socket::Uds(BufReader::new(
                UnixStream::connect(self.dir.path().join("intentd.sock"))
                    .await
                    .unwrap(),
            )),
            id: 0,
            notices: Vec::new(),
        }
    }
    async fn wss(&self, token: &str) -> Client {
        let url = format!("wss://localhost:{}/ws?token={token}", self.port);
        Client {
            socket: Socket::Ws(Box::new(
                common::wss_connect_with_retry(self.port, self.cfg.clone(), &url).await,
            )),
            id: 0,
            notices: Vec::new(),
        }
    }
    fn query(&self) -> Value {
        json!({"workspaceId":self.workspace})
    }
}
enum Socket {
    Uds(BufReader<UnixStream>),
    Ws(Box<common::TlsWs>),
}
struct Client {
    socket: Socket,
    id: u64,
    notices: Vec<Value>,
}
impl Client {
    async fn next(&mut self) -> Value {
        timeout(common::rpc_read_timeout(), async {
            loop {
                match &mut self.socket {
                    Socket::Uds(stream) => {
                        let mut line = String::new();
                        assert!(stream.read_line(&mut line).await.unwrap() > 0);
                        return serde_json::from_str(&line).unwrap();
                    }
                    Socket::Ws(ws) => match ws.next().await {
                        Some(Ok(Message::Text(text))) => {
                            return serde_json::from_str(&text).unwrap()
                        }
                        Some(Ok(Message::Ping(p))) => {
                            ws.send(Message::Pong(p)).await.unwrap();
                        }
                        Some(Ok(_)) => {}
                        x => panic!("socket closed {x:?}"),
                    },
                }
            }
        })
        .await
        .unwrap()
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let text =
            json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params}).to_string();
        match &mut self.socket {
            Socket::Uds(stream) => {
                stream
                    .get_mut()
                    .write_all(format!("{text}\n").as_bytes())
                    .await
                    .unwrap();
            }
            Socket::Ws(ws) => ws.send(Message::Text(text.into())).await.unwrap(),
        }
        loop {
            let frame = self.next().await;
            if frame["id"] == self.id {
                return frame;
            }
            self.notices.push(frame);
        }
    }
    async fn capture(&mut self, query: Value) -> Value {
        let r = self
            .rpc("workspace.repositorySelection.capture", query)
            .await;
        assert!(r.get("error").is_none(), "{r}");
        r["result"].clone()
    }
    async fn save(&mut self, query: Value, capture: &Value, choice: Value) -> Value {
        let mut q = query;
        q["selectionId"] = capture["selectionId"].clone();
        q["choice"] = choice;
        self.rpc("workspace.repositorySelection.save", q).await
    }
    async fn reconcile(&mut self, query: Value, capture: &Value) -> Value {
        let mut q = query;
        q["selectionId"] = capture["selectionId"].clone();
        self.rpc("workspace.repositorySelection.reconcile", q).await
    }
    async fn retired(&mut self, id: &Value) -> Value {
        loop {
            if let Some(pos) = self.notices.iter().position(|v| {
                v["method"] == "workspace.repositorySelection.retired"
                    && (v["params"]["allRetired"] == true
                        || v["params"]["selectionIds"]
                            .as_array()
                            .is_some_and(|ids| ids.contains(id)))
            }) {
                return self.notices.remove(pos);
            }
            let next = self.next().await;
            self.notices.push(next);
        }
    }
}
fn success(value: &Value) -> &Value {
    assert!(value.get("error").is_none(), "{value}");
    &value["result"]
}
fn refused(value: &Value) {
    assert_eq!(value["error"]["code"], -32003, "{value}");
}

#[tokio::test]
async fn native_selection_cold_uds_wss_manager_permission_cas_and_receipts() {
    let h = Harness::boot().await;
    let mut local = h.uds().await;
    let hello = local
        .rpc("client.hello", json!({"clientId":"selection-uds"}))
        .await;
    assert_eq!(success(&hello)["server"]["protocolVersion"], "10.11");
    assert_eq!(
        success(&hello)["server"]["capabilities"]["repositorySelection"],
        1
    );
    assert_eq!(
        success(&hello)["server"]["capabilities"]["repositoryContext"],
        1
    );
    let edit = local.capture(h.query()).await;
    assert_eq!(edit["snapshot"]["selection"]["kind"], "neverSaved");
    assert!(edit["snapshot"]["selectionRevision"].is_string());
    let mut owner = h.wss(TOKEN).await;
    refused(
        &owner
            .save(h.query(), &edit, json!({"mode":"automatic"}))
            .await,
    );
    let stale = owner.capture(h.query()).await;
    let first = local
        .save(
            h.query(),
            &edit,
            json!({"mode":"explicit-remote","remoteName":"unobserved-local-intent"}),
        )
        .await;
    assert_eq!(
        success(&first)["attempt"]["receipt"]["persistence"]["kind"],
        "committed"
    );
    assert_eq!(
        success(&first)["attempt"]["receipt"]["result"]["kind"],
        "applied"
    );
    let again = local
        .save(
            h.query(),
            &edit,
            json!({"mode":"explicit-remote","remoteName":"unobserved-local-intent"}),
        )
        .await;
    assert_eq!(success(&again), success(&first));
    let conflict = owner
        .save(h.query(), &stale, json!({"mode":"automatic"}))
        .await;
    assert_eq!(
        success(&conflict)["attempt"]["receipt"]["result"]["kind"],
        "conflict"
    );
    assert_eq!(
        success(&conflict)["attempt"]["receipt"]["persistence"]["kind"],
        "noEffect"
    );
    let changed = local
        .save(h.query(), &edit, json!({"mode":"automatic"}))
        .await;
    assert_eq!(changed["error"]["code"], -32602);
    let exact_q = json!({"workspaceId":h.workspace,"gitRootId":h.registered});
    let exact = owner.capture(exact_q.clone()).await;
    refused(
        &owner
            .save(h.query(), &exact, json!({"mode":"automatic"}))
            .await,
    );
    assert_eq!(
        success(
            &owner
                .save(exact_q.clone(), &exact, json!({"mode":"automatic"}))
                .await
        )["attempt"]["receipt"]["result"]["kind"],
        "applied"
    );
    let mut collaborator = h.wss(GUEST).await;
    refused(
        &collaborator
            .rpc("workspace.repositorySelection.capture", h.query())
            .await,
    );
    // The existing ordinary read remains usable by this actual collaborator.
    success(
        &collaborator
            .rpc("workspace.repositoryContext.capture", h.query())
            .await,
    );
    let malformed = local
        .rpc(
            "workspace.repositorySelection.capture",
            json!({"workspaceId":h.workspace,"principalId":h.guest}),
        )
        .await;
    assert_eq!(malformed["error"]["code"], -32602);
    let reset = local.capture(h.query()).await;
    let q = json!({"workspaceId":h.workspace,"selectionId":reset["selectionId"]});
    let result = local
        .rpc("workspace.repositorySelection.reset", q.clone())
        .await;
    assert_eq!(
        success(&result)["attempt"]["receipt"]["result"]["snapshot"]["selection"]["kind"],
        "reset"
    );
    assert_eq!(
        success(&local.reconcile(h.query(), &edit).await),
        success(&first)
    );
    for _ in 0..2 {
        assert_eq!(
            success(
                &local
                    .rpc("workspace.repositorySelection.release", q.clone())
                    .await
            )["released"],
            true
        );
    }
    let event = local.retired(&edit["selectionId"]).await;
    assert!(event["params"]["sequence"].is_string());
    assert!(event["params"].get("root").is_none());
    let mut replacement = h.uds().await;
    refused(&replacement.reconcile(h.query(), &edit).await);
    assert_eq!(std::fs::read_dir(&h.root).unwrap().count(), 0);
    assert_eq!(
        std::fs::read_dir(h.dir.path().join("secondary"))
            .unwrap()
            .count(),
        0
    );
}
#[tokio::test]
async fn native_selection_two_real_hosts_equal_ids_and_socket_loss_never_replay() {
    let a = Harness::boot().await;
    let b = Harness::boot_with_ids(Some((a.workspace.clone(), a.registered.clone()))).await;
    let mut sa = a.uds().await;
    let mut sb = b.wss(TOKEN).await;
    let original = sa.capture(a.query()).await;
    let result = sa
        .save(a.query(), &original, json!({"mode":"automatic"}))
        .await;
    assert_eq!(
        success(&result)["attempt"]["receipt"]["persistence"]["kind"],
        "committed"
    );
    refused(
        &sb.save(b.query(), &original, json!({"mode":"automatic"}))
            .await,
    );
    refused(&sb.reconcile(b.query(), &original).await);
    let untouched = sb.capture(b.query()).await;
    assert_eq!(untouched["snapshot"]["selection"]["kind"], "neverSaved");
    drop(sa);
    let mut fresh = a.uds().await;
    refused(&fresh.reconcile(a.query(), &original).await);
    let observation = fresh.capture(a.query()).await;
    assert_eq!(observation["snapshot"]["selection"]["kind"], "saved");
    assert_ne!(observation["selectionId"], original["selectionId"]);
    // Current saved value is not a recovered original operation receipt.
    assert!(observation.get("attempt").is_none());
}
