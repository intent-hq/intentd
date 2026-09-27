//! Actual renderer RPC -> TLS router -> accept-changes -> production GitHub adapter.
use super::*;
use intent_services::{EventBus, InMemorySecretStore, SecretStore, Services, SettingsRegistry};
use intent_transport::{AsyncTokenStore, TokenStore, WsApiServer, WsOptions};
use std::sync::atomic::{AtomicU16, Ordering};

struct OwnerToken;
impl TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some(TOKEN.into())
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

struct Forge {
    task: tokio::task::JoinHandle<()>,
    status: Arc<AtomicU16>,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    url: String,
}

impl Drop for Forge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Forge {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(401));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (state, seen) = (status.clone(), requests.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let request = String::from_utf8_lossy(&bytes);
                seen.lock().unwrap().push(request.to_string());
                let code = state.load(Ordering::SeqCst);
                let body = match code {
                    201 => json!({"number":7,"html_url":"https://github.com/fake-org/fake-repo/pull/7",
                        "title":"Member work","state":"open","head":{"ref":"feature"},"base":{"ref":"main"}}),
                    401 => json!({"message":"private-rejected https://private.invalid/secret"}),
                    403 => json!({"message":"insufficient_scope private-scope https://private.invalid/secret"}),
                    500 => json!({"message":"ordinary server failure"}),
                    _ => unreachable!(),
                }.to_string();
                let response = format!("HTTP/1.1 {code} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        Self {
            task,
            status,
            requests,
            url,
        }
    }
}

struct Host {
    dir: tempfile::TempDir,
    ws: WorkspaceId,
    member: PrincipalId,
    store: Store,
    server: WsApiServer,
    worker: tokio::task::JoinHandle<()>,
    port: u16,
    cfg: Arc<ClientConfig>,
    forge: Forge,
}

impl Host {
    async fn start(repository_token: &str) -> Self {
        let forge = Forge::start().await;
        let dir = temp_data_dir();
        let ws = WorkspaceId::new();
        let member = seed_member(dir.path(), &ws).await;
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        let mut person = store.get_principal(&member).await.unwrap();
        person.identity = Some(intent_core::PrincipalIdentity {
            provider: "gitlab".into(),
            host: "identity.invalid".into(),
            external_user_id: "42".into(),
        });
        store.upsert_principal(&person).await.unwrap();
        let mut workspace = store.get_workspace(&ws).await.unwrap();
        workspace.repository_owner = Some("fake-org".into());
        workspace.repository_name = Some("fake-repo".into());
        workspace.branch = "feature".into();
        workspace.base_ref = Some("main".into());
        store.update_workspace(&workspace).await.unwrap();
        let work = Path::new(workspace.worktree_path.as_ref().unwrap());
        let git_config = dir.path().join("empty-git-config");
        std::fs::write(&git_config, "").unwrap();
        for args in [
            vec!["init", "--initial-branch=feature"],
            vec![
                "-c",
                "user.name=Owner",
                "-c",
                "user.email=owner@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-m",
                "init",
            ],
            vec!["branch", "main"],
        ] {
            let output = Command::new("git")
                .args(args)
                .current_dir(work)
                .env_remove("GITHUB_TOKEN")
                .env_remove("GH_TOKEN")
                .env_remove("GITLAB_TOKEN")
                .env("GIT_CONFIG_GLOBAL", &git_config)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets
            .store("sourceControl.github.token", repository_token)
            .unwrap();
        secrets
            .store("collaboration.github.token", "private-identity-github")
            .unwrap();
        secrets
            .store("collaboration.gitlab.token", "private-identity-gitlab")
            .unwrap();
        let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.github.tokenSource".into(), json!("explicit")),
                ("sourceControl.github.apiBaseUrl".into(), json!(forge.url)),
                (
                    "sourceControl.github.exposeGitCredentialToChildren".into(),
                    json!(false),
                ),
            ])
            .unwrap();
        let github =
            intent_sourcecontrol::GitHubSourceControl::new(repository_token, Some(&forge.url))
                .unwrap();
        let bus = EventBus::new(store.clone());
        let services = Arc::new(
            Services::new(store.clone())
                .with_workspaces_root(dir.path().join("workspaces"))
                .with_settings_registry(registry)
                .with_secret_store(secrets)
                .with_event_bus(bus.clone())
                .with_source_control(Arc::new(github)),
        );
        let worker = services.spawn_execution_context_loop();
        let tls = intent_transport::ensure_tls_certificate(dir.path()).unwrap();
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
        let server = WsApiServer::new(
            services,
            bus,
            &tls,
            &tokens,
            WsOptions {
                base_port: 0,
                bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let port = server.start().await.unwrap();
        let cfg = client_config(&tls.fingerprint256);
        Self {
            dir,
            ws,
            member,
            store,
            server,
            worker,
            port,
            cfg,
            forge,
        }
    }

    async fn history(&self) -> Vec<intent_core::Event> {
        self.store
            .query_events(&intent_store::EventQuery {
                event_types: vec!["host:execution-context-changed".into()],
                ..Default::default()
            })
            .await
            .unwrap()
    }

    async fn stop(self) -> tempfile::TempDir {
        self.server.stop().await;
        self.worker.abort();
        let _ = self.worker.await;
        self.dir
    }
}

fn assert_safe_in_band(reply: &serde_json::Value) {
    assert_eq!(reply["jsonrpc"], "2.0");
    assert_eq!(reply["id"], 10);
    assert!(
        reply["error"].is_null(),
        "pipeline failure stays in-band: {reply}"
    );
    let result = &reply["result"];
    assert_eq!(result["success"], false, "{reply}");
    assert_eq!(result["steps"][0]["id"], "create-pr");
    assert_eq!(result["steps"][0]["status"], "failed");
    assert_eq!(result["steps"][0]["error"], result["error"]);
    let message = result["error"].as_str().unwrap();
    for semantic in ["connected host", "Git authorization", "owner", "retry"] {
        assert!(message.contains(semantic), "{reply}");
    }
    for private in [
        "private-",
        "private.invalid",
        "host-a-token",
        "host-b-token",
    ] {
        assert!(!reply.to_string().contains(private), "{reply}");
    }
    assert!(
        result["executionAuthorization"].is_null(),
        "no new wire fields"
    );
}

#[intent_test_macros::daemon_test]
async fn member_accept_changes_creation_auth_is_safe_durable_and_host_scoped_over_wss() {
    let a = Host::start("host-a-token").await;
    let b = Host::start("host-b-token").await;
    let mut client = member_connection(a.port, a.cfg.clone()).await;
    let mut observer = member_connection(a.port, a.cfg.clone()).await;
    let mut owner = connect_ws(a.port, a.cfg.clone()).await;
    wss_rpc(
        &mut observer,
        1,
        "events.subscribe",
        json!({"eventTypes":["host:execution-context-changed"]}),
    )
    .await;
    let context = wss_rpc(&mut client, 1, "host.executionContext", json!({})).await;
    assert_eq!(context["repositoryConnections"][0]["configured"], true);
    assert_eq!(context.as_object().unwrap().len(), 5);
    let params = json!({"workspaceId":a.ws, "action":"create-pr", "prTitle":"Member work", "prBody":"Description"});
    let mut event_ids = Vec::new();
    for status in [401, 403] {
        a.forge.status.store(status, Ordering::SeqCst);
        let reply =
            wss_rpc_envelope(&mut client, 10, "accept-changes.execute", params.clone()).await;
        assert_safe_in_band(&reply);
        let changed = wss_event(&mut observer, 10).await;
        let event = &changed["params"]["event"];
        assert_eq!(event["type"], "host:execution-context-changed");
        assert_eq!(
            event["data"], context,
            "configured flags need not change on rejection"
        );
        let id = event["id"].as_str().unwrap().to_string();
        assert!(a
            .history()
            .await
            .iter()
            .any(|row| row.id.as_str() == id && row.data == context));
        event_ids.push(id);
        assert!(a
            .store
            .get_workspace(&a.ws)
            .await
            .unwrap()
            .pr_number
            .is_none());
        let legacy =
            wss_rpc_envelope(&mut owner, 10, "accept-changes.execute", params.clone()).await;
        let expected = match status {
            401 => "internal error: source control auth error: private-rejected https://private.invalid/secret",
            403 => "internal error: source control auth error: insufficient_scope private-scope https://private.invalid/secret",
            _ => unreachable!(),
        };
        assert_eq!(legacy["result"]["error"], expected);
        assert_eq!(legacy["result"]["steps"][0]["error"], expected);
        assert_eq!(
            wss_event(&mut observer, 10).await["params"]["event"]["data"],
            context
        );
    }
    // A server failure remains an ordinary error, with no auth invalidation.
    a.forge.status.store(500, Ordering::SeqCst);
    let before = a.history().await.len();
    let legacy = wss_rpc_envelope(&mut owner, 10, "accept-changes.execute", params.clone()).await;
    let ordinary =
        wss_rpc_envelope(&mut client, 10, "accept-changes.execute", params.clone()).await;
    assert_eq!(ordinary, legacy);
    assert_eq!(
        ordinary["result"]["error"],
        "internal error: source control api error: 500: ordinary server failure"
    );
    assert!(try_wss_event(&mut observer, Duration::from_millis(50))
        .await
        .is_none());
    assert_eq!(a.history().await.len(), before);
    assert!(b.history().await.is_empty());
    assert!(b.forge.requests.lock().unwrap().is_empty());
    assert!(b
        .store
        .get_workspace(&b.ws)
        .await
        .unwrap()
        .pr_number
        .is_none());
    // The same member socket can recover after the host credential works.
    a.forge.status.store(201, Ordering::SeqCst);
    let created = wss_rpc(&mut client, 10, "accept-changes.execute", params).await;
    assert_eq!(created["success"], true, "{created}");
    assert_eq!(created["result"]["prNumber"], 7);
    let status = wss_rpc(
        &mut client,
        11,
        "accept-changes.getStatus",
        json!({"workspaceId":a.ws}),
    )
    .await;
    assert_eq!(status["existingPR"]["number"], 7);
    assert_eq!(
        a.store.get_workspace(&a.ws).await.unwrap().pr_number,
        Some(7)
    );
    // Host B uses its own configured repository token, despite the same member token.
    b.forge.status.store(201, Ordering::SeqCst);
    let mut second = member_connection(b.port, b.cfg.clone()).await;
    assert_eq!(
        wss_rpc(
            &mut second,
            10,
            "accept-changes.execute",
            json!({"workspaceId":b.ws,"action":"create-pr"})
        )
        .await["success"],
        true
    );
    for (host, token) in [(&a, "host-a-token"), (&b, "host-b-token")] {
        let requests = host.forge.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            assert!(
                request.starts_with("POST /repos/fake-org/fake-repo/pulls "),
                "{request}"
            );
            let header = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                .unwrap();
            assert!(header.contains(token), "{header}");
            assert!(!request.contains("identity-"), "{request}");
        }
    }
    drop((client, observer, owner, second));
    let dir = a.stop().await;
    let _b = b.stop().await;
    let reopened = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let events = reopened
        .query_events(&intent_store::EventQuery {
            event_types: vec!["host:execution-context-changed".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    for id in event_ids {
        assert!(events
            .iter()
            .any(|event| event.id.as_str() == id && event.data == context));
    }
}

#[intent_test_macros::daemon_test]
async fn member_accept_changes_creation_rechecks_workspace_authority_over_wss() {
    let host = Host::start("host-a-token").await;
    let mut client = member_connection(host.port, host.cfg.clone()).await;
    for target in [WorkspaceId::new(), WorkspaceId::chief()] {
        let refused = wss_rpc_envelope(
            &mut client,
            10,
            "accept-changes.execute",
            json!({"workspaceId":target,"action":"create-pr"}),
        )
        .await;
        assert_eq!(refused["error"]["data"]["code"], "not-found", "{refused}");
    }
    // Keep the admitted socket/credential; the durable role no longer grants
    // workspace access, and this person has no guest share to fall back to.
    host.store.remove_host_member(&host.member).await.unwrap();
    let refused = wss_rpc_envelope(
        &mut client,
        10,
        "accept-changes.execute",
        json!({"workspaceId":host.ws,"action":"create-pr"}),
    )
    .await;
    assert_eq!(
        refused["error"],
        json!({"code":-32003,"message":"Forbidden"})
    );
    assert!(host.forge.requests.lock().unwrap().is_empty());
    assert!(host.history().await.is_empty());
    drop(client);
    host.stop().await;
}
