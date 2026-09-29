//! Cold ordinary repository-context reads over actual UDS and authenticated WSS.
//! All stores, Git roots, credentials and TLS endpoints belong to this fixture.
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
use std::path::{Path, PathBuf};
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
fn git(root: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
fn repository(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    git(path, &["init", "--initial-branch=main"]);
    git(
        path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
}
impl Harness {
    async fn boot() -> Self {
        Self::boot_with_ids(None).await
    }
    async fn boot_with_ids(ids: Option<(WorkspaceId, WorkspaceGitRootId)>) -> Self {
        let dir = common::test_tempdir_in("/tmp", "itd-repository-wire-");
        let root = dir.path().join("repo");
        repository(&root);
        let registered_path = dir.path().join("secondary");
        repository(&registered_path);
        git(
            &registered_path,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/fixture/registered.git",
            ],
        );
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
        std::os::unix::fs::symlink("/usr/bin/git", bins.join("git")).unwrap();
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
        let harness = Self {
            child,
            dir,
            root,
            workspace: ws.id,
            registered,
            guest: guest.id,
            port,
            cfg,
        };
        // The watcher's initial catch-up legitimately replaces even an equal
        // settings snapshot. Observe one real reload before positive captures,
        // then record a normal self-write so later coalesced file events skip it.
        timeout(common::daemon_startup_timeout(), async {
            loop {
                let log = std::fs::read_to_string(harness.dir.path().join("daemon.log")).unwrap();
                if log.contains("config.toml live-reload watcher ready") {
                    break;
                }
                // timing-guard: actual watcher registration readiness
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        let config = harness.dir.path().join("config.toml");
        let text = std::fs::read_to_string(&config).unwrap() + "\n[git]\nautoCommit = false\n";
        let next = harness.dir.path().join("config-next.toml");
        std::fs::write(&next, text).unwrap();
        std::fs::rename(&next, &config).unwrap();
        let mut control = harness.uds().await;
        timeout(common::daemon_startup_timeout(), async {
            loop {
                let value = control
                    .rpc("settings.get", json!({"path":"git.autoCommit"}))
                    .await;
                if success(&value)["value"] == false {
                    break;
                }
                // timing-guard: wait for the real file reload publication
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        success(
            &control
                .rpc(
                    "settings.update",
                    json!({"changes":[{"path":"git.autoCommit","value":false}]}),
                )
                .await,
        );
        harness
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
        let r = self.rpc("workspace.repositoryContext.capture", query).await;
        assert!(r.get("error").is_none(), "{r}");
        r["result"].clone()
    }
    async fn read(&mut self, query: Value, capture: &Value) -> Value {
        let mut q = query;
        q["repositoryLifetimeId"] = capture["lifetimeId"].clone();
        self.rpc("workspace.repositoryContext", q).await
    }
    async fn retired(&mut self, id: &Value) -> Value {
        loop {
            if let Some(pos) = self.notices.iter().position(|v| {
                v["method"] == "workspace.repositoryContext.retired"
                    && (v["params"]["allRetired"] == true
                        || v["params"]["lifetimeIds"]
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
async fn native_wire_cold_daemon_uds_wss_original_context_and_permissions() {
    let h = Harness::boot().await;
    let mut uds = h.uds().await;
    let hello = uds
        .rpc("client.hello", json!({"clientId":"repository-uds"}))
        .await;
    assert_eq!(
        success(&hello)["server"]["capabilities"]["repositoryContext"],
        1
    );
    assert_eq!(success(&hello)["server"]["protocolVersion"], "10.10");
    let c = uds.capture(h.query()).await;
    assert_eq!(c["coverage"]["kind"], "workspaceInventory");
    assert!(c["scope"]["authorityGeneration"].is_string());
    assert!(c["retirementSequence"].is_string());
    let first = uds.read(h.query(), &c).await;
    assert_eq!(success(&first)["roots"].as_array().unwrap().len(), 2);
    assert_eq!(success(&first)["scope"], c["scope"]);
    assert!(success(&first)["roots"][0]["remotes"]
        .as_array()
        .unwrap()
        .is_empty());
    let mut owner = h.wss(TOKEN).await;
    refused(&owner.read(h.query(), &c).await);
    let q = json!({"workspaceId":h.workspace,"gitRootId":h.registered});
    let exact = owner.capture(q.clone()).await;
    assert_eq!(exact["coverage"]["kind"], "registeredRoot");
    let one = owner.read(q.clone(), &exact).await;
    assert_eq!(success(&one)["roots"].as_array().unwrap().len(), 1);
    refused(&owner.read(h.query(), &exact).await);
    let mut wrong_release = h.query();
    wrong_release["repositoryLifetimeId"] = exact["lifetimeId"].clone();
    refused(
        &owner
            .rpc("workspace.repositoryContext.release", wrong_release)
            .await,
    );
    let mut exact_release = q.clone();
    exact_release["repositoryLifetimeId"] = exact["lifetimeId"].clone();
    for _ in 0..2 {
        assert_eq!(
            success(
                &owner
                    .rpc("workspace.repositoryContext.release", exact_release.clone())
                    .await
            )["released"],
            true
        );
    }
    let retirement = owner.retired(&exact["lifetimeId"]).await;
    assert_eq!(
        retirement["params"]["lifetimeIds"],
        json!([exact["lifetimeId"]])
    );
    refused(&owner.read(q.clone(), &exact).await);
    let mut guest = h.wss(GUEST).await;
    let gc = guest.capture(h.query()).await;
    let gr = guest.read(h.query(), &gc).await;
    let guest_targets = success(&gr)["roots"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|r| r["targets"].as_array().unwrap())
        .collect::<Vec<_>>();
    assert!(!guest_targets.is_empty());
    assert!(guest_targets.iter().all(|t| t["availability"] == "unknown"
        && t["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["state"] == "unknown")));
    assert!(success(&gr)["roots"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|r| r["targets"].as_array().unwrap())
        .all(|t| t.get("connection").is_none()));
    let invalid = guest
        .rpc(
            "workspace.repositoryContext.capture",
            json!({"workspaceId":h.workspace,"principalId":h.guest}),
        )
        .await;
    assert_eq!(invalid["error"]["code"], -32602);
    let remove = uds
        .rpc(
            "workspace.members.remove",
            json!({"workspaceId":h.workspace,"principalId":h.guest}),
        )
        .await;
    success(&remove);
    refused(&guest.read(h.query(), &gc).await);
    let notice = guest.retired(&gc["lifetimeId"]).await;
    assert!(notice["params"]["sequence"].is_string());
    assert!(notice["params"].get("workspaceId").is_none());
    let readd = uds
        .rpc(
            "workspace.members.add",
            json!({"workspaceId":h.workspace,"principalId":h.guest}),
        )
        .await;
    success(&readd);
    refused(&guest.read(h.query(), &gc).await);
    let new_guest = guest.capture(h.query()).await;
    success(&guest.read(h.query(), &new_guest).await);
    let before_settings = uds.capture(h.query()).await;
    success(&uds.read(h.query(), &before_settings).await);
    success(
        &uds.rpc(
            "settings.update",
            json!({"changes":[{"path":"git.autoCommit","value":true}]}),
        )
        .await,
    );
    refused(&uds.read(h.query(), &before_settings).await);
    let retired = uds.retired(&before_settings["lifetimeId"]).await;
    assert!(retired["params"]["lifetimeIds"].as_array().is_some());
    let original = uds.capture(h.query()).await;
    success(&uds.read(h.query(), &original).await);
    git(
        &h.root,
        &["remote", "add", "origin", "https://github.com/team/one.git"],
    );
    git(
        &h.root,
        &["remote", "add", "second", "https://github.com/team/two.git"],
    );
    refused(&uds.read(h.query(), &original).await);
    let changed = uds.capture(h.query()).await;
    let facts = uds.read(h.query(), &changed).await;
    assert_eq!(
        success(&facts)["roots"][0]["targets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let mut release = h.query();
    release["repositoryLifetimeId"] = changed["lifetimeId"].clone();
    for _ in 0..2 {
        let r = uds
            .rpc("workspace.repositoryContext.release", release.clone())
            .await;
        assert_eq!(success(&r)["released"], true);
    }
    refused(&uds.read(h.query(), &changed).await);
    let pending = uds.capture(h.query()).await;
    let scheduled = uds
        .rpc(
            "workspace.delete",
            json!({"workspaceId":h.workspace,"undoDelayMs":30_000}),
        )
        .await;
    assert_eq!(success(&scheduled)["scheduled"], true);
    refused(&uds.read(h.query(), &pending).await);
    assert_eq!(
        success(&uds.rpc("workspace.cancelDelete", h.query()).await)["cancelled"],
        true
    );
    refused(&uds.read(h.query(), &pending).await);
    let resumed = uds.capture(h.query()).await;
    success(&uds.read(h.query(), &resumed).await);
    success(&guest.rpc("workspace.get", h.query()).await);
    success(&owner.rpc("workspace.get", h.query()).await);
    success(&uds.rpc("workspace.get", h.query()).await);
    for client in [&uds, &owner, &guest] {
        assert!(
            client.notices.iter().all(|frame| frame.get("id").is_none()),
            "no second response for a consumed request"
        );
    }
}

#[tokio::test]
async fn native_wire_two_actual_hosts_with_equal_root_ids_cannot_rebind() {
    let a = Harness::boot().await;
    let b = Harness::boot_with_ids(Some((a.workspace.clone(), a.registered.clone()))).await;
    git(
        &b.root,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/host-b/repository.git",
        ],
    );
    let mut local = a.uds().await;
    let mut remote = b.wss(TOKEN).await;
    let ac = local.capture(a.query()).await;
    let bc = remote.capture(b.query()).await;
    assert_ne!(ac["scope"]["daemonId"], bc["scope"]["daemonId"]);
    let av = local.read(a.query(), &ac).await;
    let bv = remote.read(b.query(), &bc).await;
    assert!(success(&av)["roots"][0]["remotes"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        success(&bv)["roots"][0]["targets"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    refused(&local.read(a.query(), &bc).await);
    refused(&remote.read(b.query(), &ac).await);
    let mut replacement = b.wss(TOKEN).await;
    refused(&replacement.read(b.query(), &bc).await);
    success(&remote.read(b.query(), &bc).await);
}
