//! R11: a challenge/proof started in the real postcommit publication window
//! rejoins with fresh scope and survives the old member's live invalidation.
use super::*;

struct PublicationRelease(Arc<(Mutex<bool>, std::sync::Condvar)>);
impl Drop for PublicationRelease {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
}

async fn pause_publication_read(
    store: &Store,
) -> (tokio::sync::oneshot::Receiver<()>, PublicationRelease) {
    let (entered, reached) = tokio::sync::oneshot::channel();
    let entered = Arc::new(Mutex::new(Some(entered)));
    let state = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let mut connections = Vec::new();
    for _ in 0..store.read_pool().options().get_max_connections() {
        connections.push(store.read_pool().acquire().await.unwrap());
    }
    for connection in &mut connections {
        let state = state.clone();
        let entered = entered.clone();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                let sender = entered.lock().unwrap().take();
                if let Some(sender) = sender {
                    let _ = sender.send(());
                    let mut released = state.0.lock().unwrap();
                    while !*released {
                        released = state.1.wait(released).unwrap();
                    }
                }
                true
            });
    }
    drop(connections);
    (reached, PublicationRelease(state))
}

struct ProofServer {
    task: tokio::task::JoinHandle<()>,
    url: String,
    nonce: Arc<Mutex<String>>,
    user_read: tokio::sync::mpsc::UnboundedReceiver<()>,
}
impl Drop for ProofServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ProofServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let nonce = Arc::new(Mutex::new(String::new()));
        let proof = nonce.clone();
        let (seen, user_read) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    if stream.read_exact(&mut byte).await.is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap_or("");
                let body = if path.starts_with("/gists/") {
                    json!({"owner":{"login":"guest"},"created_at":chrono::Utc::now().to_rfc3339(),
                        "files":{"intent-join-proof.txt":{"content":proof.lock().unwrap().clone(),"truncated":false}}})
                } else if path == "/users/guest" {
                    let _ = seen.send(());
                    json!({"id":4242,"login":"guest","name":"Guest User","avatar_url":null,"html_url":"https://github.com/guest"})
                } else {
                    json!({"id":9000,"login":"owner","name":"Owner","avatar_url":null,"html_url":"https://github.com/owner"})
                }.to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            task,
            url,
            nonce,
            user_read,
        }
    }
}

#[tokio::test]
async fn member_removal_postcommit_owner_rejoin_survives() {
    postcommit_rejoin(false).await;
}

#[tokio::test]
async fn member_removal_postcommit_self_rejoin_survives() {
    postcommit_rejoin(true).await;
}

async fn postcommit_rejoin(self_revoke: bool) {
    use intent_core::{with_caller, Caller, PrincipalIdentity};
    use intent_transport::tunnel::Frame;
    for host_rejoin in [true, false] {
        let scope = if host_rejoin { "host" } else { "workspace" };
        let role = if host_rejoin { "member" } else { "guest" };
        let mut forge = ProofServer::start().await;
        let dir = test_tempdir("intentd-removal-rejoin-");
        let db = dir.path().join("intentd.db");
        let store = Store::open(&db).await.unwrap();
        // Separate connections to the SAME database let the test hold the actual
        // publisher's attribution read without holding admission's SQLite lock.
        let publication_store = Store::open(&db).await.unwrap();
        let bus = EventBus::new(publication_store.clone());
        let registry = Arc::new(
            intent_services::SettingsRegistry::load(dir.path().join("config.toml")).unwrap(),
        );
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_settings_registry(registry.clone())
            .with_gitlab_secret_store(intent_core::FileSecretStore::with_path(
                dir.path().join("secrets.json"),
            ))
            .with_source_control(Arc::new(
                intent_sourcecontrol::github::GitHubSourceControl::new(
                    "owned-fixture",
                    Some(&forge.url),
                )
                .unwrap(),
            ));
        let (srv, _) =
            start_pairing_services(Arc::new(services), bus, store, registry, dir, None).await;
        let primary = srv.store.get_primary_principal().await.unwrap();
        let mut admin = reconnect(&srv, &primary, TOKEN).await;
        let old_token = "old-member-r11";
        let mut old = Guest::connect(&srv, old_token).await;
        old.principal.identity = Some(PrincipalIdentity::github(4242));
        srv.store.upsert_principal(&old.principal).await.unwrap();
        sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
            .bind(&old.principal.id.0)
            .bind(now_iso())
            .execute(srv.store.write_pool())
            .await
            .unwrap();
        let workspace = WorkspaceId::new();
        let other_workspace = WorkspaceId::new();
        for id in [&workspace, &other_workspace] {
            srv.store
                .insert_workspace(&fixture_workspace(id))
                .await
                .unwrap();
        }
        let invite = if host_rejoin {
            with_caller(
                Caller::Daemon,
                srv.api.host_invite_create(intent_core::InvitePin {
                    login: "guest".into(),
                    provider: Some("github".into()),
                    host: None,
                }),
            )
            .await
            .unwrap()
        } else {
            with_caller(
                Caller::Daemon,
                srv.api
                    .workspace_invite_create(workspace.clone(), None, None),
            )
            .await
            .unwrap()
        };
        while forge.user_read.try_recv().is_ok() {}
        let mut unaffected = Guest::connect(&srv, "unaffected-r11").await;
        srv.store
            .add_workspace_member(
                &workspace,
                &unaffected.principal.id,
                intent_core::WorkspaceRole::Collaborator,
            )
            .await
            .unwrap();
        old.call("client.hello", json!({"clientId":"old-r11"}))
            .await;
        old.call(
            "events.subscribe",
            json!({"eventTypes":["host:members-changed"]}),
        )
        .await;
        let mut old_tunnel = removal_tunnel(&srv, old_token).await;
        let mut command = if self_revoke {
            reconnect(&srv, &old.principal, old_token).await
        } else {
            reconnect(&srv, &primary, TOKEN).await
        };
        let target = old.principal.id.clone();
        let (publication_reached, release_publication) =
            pause_publication_read(&publication_store).await;
        let removal = tokio::spawn(async move {
            let response = if self_revoke {
                command.call("principal.revokeSelf", json!({})).await
            } else {
                command
                    .call("host.members.remove", json!({"principalId":target}))
                    .await
            };
            (response, command)
        });
        tokio::time::timeout(Duration::from_secs(5), publication_reached)
            .await
            .unwrap()
            .unwrap();
        assert!(srv
            .store
            .resolve_active_principal_credential(&sha256_hex(old_token.as_bytes()))
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            srv.store.get_host_role(&old.principal.id).await.unwrap(),
            intent_core::HostRole::Guest
        );
        let id = invite["invite"]["id"].as_str().unwrap().to_owned();
        let secret = invite["secret"].as_str().unwrap().to_owned();
        let url = format!("wss://localhost:{}/invite", srv.port);
        let socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
        let mut join = Guest {
            principal: old.principal.clone(),
            ws: socket,
            next_id: 1,
        };
        let challenge = join
            .call(
                "invite.challenge",
                json!({"inviteId":id,"secret":secret,"scope":scope}),
            )
            .await;
        let nonce = challenge["result"]["nonce"].as_str().unwrap().to_owned();
        forge.nonce.lock().unwrap().clone_from(&nonce);
        let prove = tokio::spawn(async move {
            join.call("invite.prove", json!({"inviteId":id,"secret":secret,"nonce":nonce,"scope":scope,"provider":"github","gistId":"r11","login":"guest"})).await
        });
        // The fresh challenge and real proof are in flight while the old
        // operation is durably committed but its actual publication is held.
        tokio::time::timeout(Duration::from_secs(5), forge.user_read.recv())
            .await
            .unwrap()
            .unwrap();
        drop(release_publication);
        let (removed, mut command) = removal.await.unwrap();
        assert!(removed.get("error").is_none());
        let fresh = prove.await.unwrap();
        assert!(
            fresh.get("error").is_none(),
            "fresh proof must be authorized"
        );
        assert_eq!(fresh["result"]["principalId"], old.principal.id.0);
        assert_eq!(fresh["result"]["hostRole"], role);
        let fresh_token = fresh["result"]["token"].as_str().unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), old.ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(frame) = frame else {
            panic!("old removal control precedes close")
        };
        let control: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(control["params"]["event"]["data"]["action"], "removed");
        assert_closed(&mut old.ws).await;
        assert_closed(&mut old_tunnel).await;
        if self_revoke {
            assert_closed(&mut command.ws).await;
        }
        let mut fresh_device = reconnect(&srv, &old.principal, fresh_token).await;
        assert_shared_capabilities(
            &fresh_device
                .call("client.hello", json!({"clientId":"fresh-r11"}))
                .await,
        );
        fresh_device
            .call(
                "events.subscribe",
                json!({"eventTypes":["host:members-changed"]}),
            )
            .await;
        assert_pairing(
            &fresh_device.call("pairing.getSelfInfo", json!({})).await,
            fresh_token,
            &old.principal,
            role,
        );
        assert!(fresh_device
            .call("workspace.get", json!({"workspaceId":workspace}))
            .await
            .get("error")
            .is_none());
        assert_eq!(
            fresh_device
                .call("workspace.get", json!({"workspaceId":other_workspace}))
                .await
                .get("error")
                .is_none(),
            host_rejoin
        );
        let online = admin
            .call("presence.snapshot", json!({"workspaceId":workspace}))
            .await;
        assert!(online["result"]["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["principalId"] == old.principal.id.0));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        if host_rejoin {
            let mut fresh_tunnel = removal_tunnel(&srv, fresh_token).await;
            removal_send(&mut fresh_tunnel, Frame::Open { stream_id: 1, port }).await;
            assert_eq!(
                removal_frame(&mut fresh_tunnel).await,
                Frame::OpenOk { stream_id: 1 }
            );
            let (mut peer, _) = listener.accept().await.unwrap();
            removal_roundtrip(&mut fresh_tunnel, &mut peer).await;
            let forwarded = fresh_device
                .call("forward.create", json!({"remotePort":port}))
                .await;
            let local = u16::try_from(forwarded["result"]["localPort"].as_u64().unwrap()).unwrap();
            let mut downstream = TcpStream::connect(("127.0.0.1", local)).await.unwrap();
            let (mut upstream, _) = listener.accept().await.unwrap();
            downstream.write_all(b"fresh").await.unwrap();
            let mut bytes = [0; 5];
            upstream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"fresh");
        } else {
            assert_eq!(
                status_code(
                    &https_request(
                        srv.port,
                        srv.cfg.clone(),
                        &upgrade_req("/tunnel", None, Some(fresh_token))
                    )
                    .await
                ),
                403
            );
            assert!(fresh_device
                .call("forward.create", json!({"remotePort":port}))
                .await
                .get("error")
                .is_some());
        }
        assert_eq!(
            unaffected.call("principal.me", json!({})).await["result"]["id"],
            unaffected.principal.id.0
        );
        for path in ["/ws", "/tunnel"] {
            assert_eq!(
                status_code(
                    &https_request(
                        srv.port,
                        srv.cfg.clone(),
                        &upgrade_req(path, None, Some(old_token))
                    )
                    .await
                ),
                401
            );
        }
        srv.ws.stop().await;
    }
}
