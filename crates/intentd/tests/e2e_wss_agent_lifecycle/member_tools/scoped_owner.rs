//! Retained workspace ownership does not grant host or member-only transport access.
use super::*;
use intent_core::WorkspaceRole;

async fn person_connection(
    port: u16,
    cfg: Arc<ClientConfig>,
    token: &str,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={token}"),
    )
    .await
}

#[tokio::test]
async fn scoped_owner_permission_recovery_keeps_transport_denials_and_revocation() {
    permission_recovery(false).await;
}

#[tokio::test]
async fn resource_context_permission_snapshot_response_and_role_checks() {
    permission_recovery(true).await;
}

async fn permission_recovery(routed: bool) {
    let script = gate("scoped owner permission recovery").expect("mock ACP prerequisite");
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    let person = seed_member(dir.path(), &ws).await;
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    store.remove_host_member(&person).await.unwrap();
    let primary = store.get_primary_principal().await.unwrap();
    store
        .set_workspace_member_role(&ws, &primary.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    store
        .add_workspace_member(&ws, &person, WorkspaceRole::Owner)
        .await
        .unwrap();
    let token = "a2".repeat(32);
    store
        .insert_principal_credential(&person, &intent_transport::hash_token(&token))
        .await
        .unwrap();
    let mut collaborator = store.get_principal(&person).await.unwrap();
    collaborator.id = PrincipalId::new();
    store.upsert_principal(&collaborator).await.unwrap();
    let collaborator_token = "a3".repeat(32);
    store
        .insert_principal_credential(
            &collaborator.id,
            &intent_transport::hash_token(&collaborator_token),
        )
        .await
        .unwrap();
    store
        .add_workspace_member(&ws, &collaborator.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let other = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&other))
        .await
        .unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\nexposeGitCredentialToChildren = false\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("secrets.json"), "{}").unwrap();
    let gh = dir.path().join("empty-gh");
    std::fs::create_dir(&gh).unwrap();
    let behavior = json!({"clientCalls":[{"method":"session/request_permission","params":{
        "sessionId":"mock-session","toolCall":{"toolCallId":"write","title":"Scoped approval"},
        "options":[{"optionId":"allow_once","name":"Allow","kind":"allow_once"}]},
        "assertResult":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}],
        "response":"Scoped owner approved work."})
    .to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("INTENTD_PERMISSION_POLICY", "interactive"),
        ("GH_CONFIG_DIR", gh.to_str().unwrap()),
        ("GITHUB_TOKEN", ""),
        ("GH_TOKEN", ""),
        ("GITLAB_TOKEN", ""),
    ];
    let _daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut guest = person_connection(port, cfg.clone(), &token).await;
    let me = wss_rpc(&mut guest, 1, "principal.me", json!({})).await;
    assert_eq!(me["id"], person.0);
    assert_eq!(me["hostRole"], "guest");
    assert_eq!(me["isAdministrator"], false);
    let row = wss_rpc(&mut guest, 2, "workspace.get", json!({"workspaceId":ws})).await;
    assert_eq!(row["workspace"]["canManage"], true, "{row}");
    assert_eq!(row["workspace"]["myRole"], "owner");
    assert_eq!(row["workspace"]["ownerPrincipalId"], person.0);
    let agent = wss_rpc(
        &mut owner,
        1,
        "agent.create",
        json!({"workspaceId":ws,"provider":"mock","model":"default"}),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap();
    let hidden_agent = wss_rpc(
        &mut owner,
        2,
        "agent.create",
        json!({"workspaceId":other,"provider":"mock","model":"default"}),
    )
    .await;
    let hidden_id = hidden_agent["agent"]["id"].as_str().unwrap();
    let mut owner_events = connect_ws(port, cfg.clone()).await;
    wss_rpc(&mut owner_events, 1, "events.subscribe", json!({"eventTypes":["agent:permission:request","agent:permission:resolved"],"workspaceId":ws})).await;
    let mut guest_events = person_connection(port, cfg.clone(), &token).await;
    wss_rpc(&mut guest_events, 1, "events.subscribe", json!({"eventTypes":["agent:permission:request","agent:permission:resolved","workspace:updated"],"workspaceId":ws})).await;
    wss_rpc(
        &mut guest,
        3,
        "agent.sendMessage",
        json!({"agentId":agent_id,"workspaceId":ws,"content":"request approval"}),
    )
    .await;
    let requested = wss_event(&mut owner_events, 30).await;
    assert_eq!(
        requested["params"]["event"]["type"],
        "agent:permission:request"
    );
    let request_id = requested["params"]["event"]["data"]["requestId"]
        .as_str()
        .unwrap();
    for mut filter in [json!({}), json!({"agentId":agent_id})] {
        if routed {
            filter["workspaceId"] = json!(other);
        }
        let pending = wss_rpc(&mut guest, 4, "agent.pendingPermissions", filter).await;
        assert_eq!(pending["requests"].as_array().unwrap().len(), 1);
        assert_eq!(pending["requests"][0]["requestId"], request_id);
    }
    let hidden = wss_rpc_envelope(
        &mut guest,
        5,
        "agent.pendingPermissions",
        json!({"agentId":hidden_id}),
    )
    .await;
    assert_eq!(hidden["error"]["data"]["code"], "not-found");
    let mut ordinary = person_connection(port, cfg.clone(), &collaborator_token).await;
    let pending = wss_rpc(&mut ordinary, 1, "agent.pendingPermissions", json!({})).await;
    assert_eq!(pending["requests"], json!([]));
    let mut answer =
        json!({"requestId":request_id,"outcome":{"outcome":"selected","optionId":"allow_once"}});
    if routed {
        answer["workspaceId"] = json!(other);
    }
    let refused =
        wss_rpc_envelope(&mut ordinary, 2, "agent.respondPermission", answer.clone()).await;
    assert_eq!(refused["error"]["code"], -32003, "{refused}");
    let refused = wss_rpc_envelope(&mut guest, 6, "script.list", json!({"workspaceId":ws})).await;
    assert_eq!(refused["error"]["code"], -32003, "{refused}");

    store
        .set_workspace_member_role(&ws, &person, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let row = wss_rpc(&mut guest, 7, "workspace.get", json!({"workspaceId":ws})).await;
    assert_eq!(row["workspace"]["canManage"], false);
    assert_eq!(row["workspace"]["myRole"], "collaborator");
    assert_eq!(
        wss_rpc(&mut guest, 8, "agent.pendingPermissions", json!({})).await["requests"],
        json!([])
    );
    let refused = wss_rpc_envelope(&mut guest, 9, "agent.respondPermission", answer.clone()).await;
    assert_eq!(refused["error"]["code"], -32003);
    store
        .set_workspace_member_role(&ws, &person, WorkspaceRole::Owner)
        .await
        .unwrap();
    assert_eq!(
        wss_rpc(&mut guest, 10, "agent.respondPermission", answer).await["resolved"],
        true
    );
    let resolved = wss_event(&mut owner_events, 15).await;
    assert_eq!(
        resolved["params"]["event"]["type"],
        "agent:permission:resolved"
    );
    assert_eq!(
        wss_rpc(&mut guest, 11, "agent.pendingPermissions", json!({})).await["requests"],
        json!([])
    );

    // A visible lifecycle event is an ordering barrier for both refused prompt events.
    wss_rpc(
        &mut owner,
        3,
        "workspace.update",
        json!({"workspaceId":ws,"title":"Permission event barrier"}),
    )
    .await;
    timeout(Duration::from_secs(15), async {
        loop {
            let event = wss_event(&mut guest_events, 15).await;
            assert_eq!(
                event["params"]["event"]["type"], "workspace:updated",
                "guest received a member-only event: {event}"
            );
            if event.to_string().contains("Permission event barrier") {
                break;
            }
        }
    })
    .await
    .expect("visible lifecycle barrier");
    for kind in ["agent:permission:request", "agent:permission:resolved"] {
        let events = wss_rpc(
            &mut guest,
            12,
            "event.query",
            json!({"workspaceId":ws,"eventType":kind}),
        )
        .await;
        assert_eq!(events, json!([]));
        let events = wss_rpc(
            &mut owner,
            4,
            "event.query",
            json!({"workspaceId":ws,"eventType":kind}),
        )
        .await;
        assert!(
            !events.as_array().unwrap().is_empty(),
            "owner durable control for {kind}"
        );
    }
    wss_rpc(&mut guest, 13, "principal.revokeSelf", json!({})).await;
    timeout(Duration::from_secs(15), async {
        loop {
            match guest_events.next().await {
                None | Some(Ok(Message::Close(_)) | Err(_)) => break,
                Some(Ok(Message::Ping(p))) => {
                    let _ = guest_events.send(Message::Pong(p)).await;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("revocation closes the already-admitted guest socket");
    let tls = tls_connect(port, cfg).await;
    let replay =
        tokio_tungstenite::client_async(format!("wss://localhost:{port}/ws?token={token}"), tls)
            .await;
    assert!(
        matches!(replay, Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status().as_u16() == 401)
    );
    assert_eq!(
        store.get_workspace_member_role(&ws, &person).await.unwrap(),
        Some(WorkspaceRole::Owner),
        "bearer revocation does not manufacture a workspace demotion"
    );
}
