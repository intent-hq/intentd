use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

async fn workspace_push(guest: &mut Guest) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match guest.ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    if frame["method"] == "subscription.push" {
                        return frame["params"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => guest.ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("expected workspace push, got {other:?}"),
            }
        }
    })
    .await
    .expect("workspace push within deadline")
}

#[tokio::test]
async fn scoped_owner_projection_over_wss_tracks_workspace_snapshots_and_role_loss() {
    let srv = start(WsOptions::default()).await;
    let primary = srv.store.get_primary_principal().await.unwrap();
    let owned = WorkspaceId::new();
    let other = WorkspaceId::new();
    for id in [&owned, &other] {
        srv.store
            .insert_workspace(&fixture_workspace(id))
            .await
            .unwrap();
    }
    let token = "ba".repeat(32);
    let mut guest = Guest::connect(&srv, &token).await;
    srv.store
        .set_workspace_member_role(&owned, &primary.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    srv.store
        .add_workspace_member(&owned, &guest.principal.id, WorkspaceRole::Owner)
        .await
        .unwrap();
    let me = guest.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["hostRole"], "guest");
    assert_eq!(me["result"]["isAdministrator"], false);
    let guest_id = guest.principal.id.clone();
    let assert_owner = |row: &Value| {
        assert_eq!(row["id"], owned.0, "{row}");
        assert_eq!(row["myRole"], "owner", "{row}");
        assert_eq!(row["ownerPrincipalId"], guest_id.0, "{row}");
        assert_eq!(row["canManage"], true, "{row}");
    };
    let got = guest
        .call("workspace.get", json!({"workspaceId":owned}))
        .await;
    assert_owner(&got["result"]["workspace"]);
    let listed = guest.call("workspace.list", json!({})).await;
    let rows = listed["result"]["workspaces"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_owner(&rows[0]);

    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    let mut subscriber = Guest {
        principal: guest.principal.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 0,
    };
    assert!(subscriber
        .call("workspace.subscribe", json!({}))
        .await
        .get("error")
        .is_none());
    let snapshot = workspace_push(&mut subscriber).await;
    assert_eq!(snapshot["kind"], "snapshot");
    assert_eq!(snapshot["seq"], 0);
    assert_owner(&snapshot["snapshot"][0]);
    let updated = guest
        .call(
            "workspace.update",
            json!({"workspaceId":owned,"title":"Scoped owner update"}),
        )
        .await;
    assert_owner(&updated["result"]["workspace"]);
    let delta = workspace_push(&mut subscriber).await;
    assert_eq!(delta["kind"], "delta");
    assert_owner(&delta["delta"]["updated"][0]);

    for (method, params) in [
        ("script.list", json!({"workspaceId":owned})),
        ("workspace.create", json!({"title":"Forbidden creation"})),
        ("settings.list", json!({})),
    ] {
        let refused = guest.call(method, params).await;
        assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
    }
    assert_eq!(
        guest
            .call("workspace.get", json!({"workspaceId":other}))
            .await["error"]["data"]["code"],
        "not-found"
    );
    srv.store
        .set_workspace_member_role(&owned, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let updated = guest
        .call(
            "workspace.update",
            json!({"workspaceId":owned,"title":"Demoted owner update"}),
        )
        .await;
    assert_eq!(updated["result"]["workspace"]["canManage"], false);
    assert_eq!(updated["result"]["workspace"]["myRole"], "collaborator");
    let delta = workspace_push(&mut subscriber).await;
    assert_eq!(delta["delta"]["updated"][0]["canManage"], false);
    assert_eq!(delta["delta"]["updated"][0]["myRole"], "collaborator");
    let refused = guest
        .call(
            "workspace.update",
            json!({"workspaceId":owned,"defaultModel":"no-longer-owner"}),
        )
        .await;
    assert_eq!(refused["error"]["code"], -32003, "{refused}");
    let removed = intent_core::with_caller(
        intent_core::Caller::Daemon,
        srv.api
            .workspace_members_remove(owned.clone(), guest.principal.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(removed["removed"], true);
    let delta = workspace_push(&mut subscriber).await;
    assert_eq!(delta["delta"]["removedIds"], json!([owned]));
    assert_eq!(
        guest
            .call("workspace.get", json!({"workspaceId":owned}))
            .await["error"]["data"]["code"],
        "not-found"
    );
    assert!(
        guest.call("workspace.list", json!({})).await["result"]["workspaces"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(subscriber);
    drop(guest);
    srv.ws.stop().await;
}

/// Narrow role/admission proof; full member method/event/reverse/tunnel
/// capabilities are exercised by the subsequent transport implementation.
#[tokio::test]
async fn host_roles_follow_durable_authority_over_wss_and_reconnect() {
    let srv = start(WsOptions::default()).await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    assert!(owner.identity_key().is_none());
    let token = "cd".repeat(32);
    let mut member = Guest::connect(&srv, &token).await;
    let guest = member.call("principal.me", json!({})).await;
    assert_eq!(guest["result"]["hostRole"], "guest");
    assert_eq!(guest["result"]["hostMembershipRevision"], 0);
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    // The already-admitted guest caller must use the new durable grant.
    let me = member.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["id"], member.principal.id.0);
    assert_eq!(me["result"]["hostRole"], "member");
    assert_eq!(me["result"]["isAdministrator"], false);
    assert_eq!(me["result"]["hostMembershipRevision"], 1);
    for (id, retained) in [("old-workspace", true), ("future-workspace", false)] {
        let ws = WorkspaceId::from(id);
        srv.store
            .insert_workspace(&fixture_workspace(&ws))
            .await
            .unwrap();
        if retained {
            srv.store
                .add_workspace_member(&ws, &member.principal.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let got = member
            .call("workspace.get", json!({"workspaceId": id}))
            .await;
        let row = &got["result"]["workspace"];
        assert_eq!(row["ownerPrincipalId"], owner.id.0, "{got}");
        assert_eq!(row["myRole"], "collaborator", "{got}");
        assert_eq!(row["canManage"], true, "{got}");
    }
    let listed = member.call("workspace.list", json!({})).await;
    assert_eq!(listed["result"]["workspaces"].as_array().unwrap().len(), 2);
    let refused = member.call("settings.list", json!({})).await;
    assert_eq!(refused["error"]["code"], -32003);
    // Reconnect resolves the member role at credential admission too.
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    member.ws = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    // Storage removal does not broadcast; this intentionally proves that
    // service gates revalidate even before the later socket-close integration.
    let me = member.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["hostRole"], "guest");
    assert_eq!(me["result"]["hostMembershipRevision"], 2);
    let denied = member
        .call("workspace.get", json!({"workspaceId":"old-workspace"}))
        .await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    let denied = https_request(
        srv.port,
        srv.cfg.clone(),
        &upgrade_req("/ws", None, Some(&token)),
    )
    .await;
    assert_eq!(status_code(&denied), 401);
    drop(member);
    srv.ws.stop().await;
}

#[tokio::test]
async fn owner_device_credential_keeps_owner_identity_and_administration() {
    let srv = start(WsOptions::default()).await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let token = "de".repeat(32);
    srv.store
        .insert_principal_credential(&owner.id, &sha256_hex(token.as_bytes()))
        .await
        .unwrap();
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    let mut device = Guest {
        principal: owner.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 0,
    };
    let me = device.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["id"], owner.id.0);
    assert_eq!(me["result"]["hostRole"], "owner");
    assert_eq!(me["result"]["isAdministrator"], true);
    assert!(device
        .call("settings.list", json!({}))
        .await
        .get("error")
        .is_none());
    drop(device);
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_workspace_tools_and_safe_context_over_wss() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("sourceControl.github.tokenSource", json!("explicit"));
    srv.set_setting(
        "sourceControl.github.exposeGitCredentialToChildren",
        json!(false),
    );
    srv.set_setting("model.defaultProvider", json!("codex"));
    let mut member = Guest::connect(&srv, &"fe".repeat(32)).await;
    assert_eq!(
        member.call("host.executionContext", json!({})).await["error"]["code"],
        -32003
    );
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(member.principal.id.as_str())
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let agent = intent_core::with_caller(
        intent_core::Caller::Daemon,
        srv.api.agent_create(
            ws.clone(),
            None,
            Some("gpt-test".into()),
            None,
            None,
            None,
            intent_core::AgentCreateExtra {
                provider: Some("codex".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    let agent_id = agent["agent"]["id"].as_str().unwrap();
    let context = member.call("host.executionContext", json!({})).await;
    assert_eq!(context["result"]["defaultProviderId"], "codex");
    assert_eq!(
        context["result"]["gitCredentialPolicy"]["managedHelperEnabled"],
        false
    );
    assert_eq!(
        context["result"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "defaultModelId",
            "defaultProviderId",
            "enabledProviderIds",
            "gitCredentialPolicy",
            "repositoryConnections"
        ]
        .into_iter()
        .collect()
    );
    for (method, params) in [
        ("terminal.list", json!({"workspaceId":ws})),
        ("script.list", json!({"workspaceId":ws})),
        ("agent.pendingPermissions", json!({"agentId":agent_id})),
        ("agent.delete", json!({"agentId":agent_id})),
        ("repo.list", json!({})),
    ] {
        let reply = member.call(method, params).await;
        assert!(reply.get("error").is_none(), "{method}: {reply}");
    }
    let missing_script = member
        .call(
            "script.create",
            json!({"workspaceId":WorkspaceId::new(),"name":"Missing workspace","command":"true","mode":"command"}),
        )
        .await;
    assert_eq!(
        missing_script["error"]["data"]["code"], "not-found",
        "{missing_script}"
    );
    // Legacy owner entries can share a runtime id while only the newest
    // workspace owns the durable row. A member must not delete that row.
    for scope in [ws.clone(), WorkspaceId::chief()] {
        intent_core::with_caller(
            intent_core::Caller::Daemon,
            srv.api.script_create(
                scope,
                intent_core::ScriptCreateParams {
                    name: "Shared id".into(),
                    command: "echo protected".into(),
                    script_id: Some("protected-script".into()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
    }
    let refused = member
        .call(
            "script.remove",
            json!({
                "workspaceId":ws,"scriptId":"protected-script"
            }),
        )
        .await;
    assert_eq!(refused["error"]["data"]["code"], "not-found", "{refused}");
    assert_eq!(
        srv.store
            .script_workspace("protected-script")
            .await
            .unwrap(),
        Some(WorkspaceId::chief())
    );
    let valid = member
        .call(
            "script.create",
            json!({
                "workspaceId":ws,"name":"Valid removal","command":"true","mode":"command"
            }),
        )
        .await;
    let valid_id = valid["result"]["id"].as_str().unwrap();
    let removed = member
        .call(
            "script.remove",
            json!({"workspaceId":ws,"scriptId":valid_id}),
        )
        .await;
    assert_eq!(removed["result"]["ok"], true, "{removed}");
    assert!(srv
        .store
        .script_workspace(valid_id)
        .await
        .unwrap()
        .is_none());
    #[cfg(unix)]
    {
        let missing = WorkspaceId::new();
        let reply = member
            .call(
                "terminal.create",
                json!({"workspaceId":missing,"command":"/bin/cat"}),
            )
            .await;
        // The regression must not leave a process behind if creation succeeds.
        if let Some(id) = reply["result"]["terminalId"].as_str() {
            intent_core::with_caller(
                intent_core::Caller::Daemon,
                srv.api.terminal_kill(id.into()),
            )
            .await
            .unwrap();
        }
        assert_eq!(reply["error"]["data"]["code"], "not-found", "{reply}");
    }
    for (method, params) in [
        ("settings.list", json!({})),
        ("repo.remove", json!({"path":"/tmp/foreign"})),
        (
            "agent.replaceMessages",
            json!({"agentId":agent_id,"messages":[]}),
        ),
        (
            "system.gitCredential",
            json!({"protocol":"https","host":"github.com"}),
        ),
        (
            "mcp.servers.toggle",
            json!({"serverId":"missing","enabled":true}),
        ),
    ] {
        assert_eq!(
            member.call(method, params).await["error"]["code"],
            -32003,
            "{method}"
        );
    }
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    assert_eq!(
        member.call("host.executionContext", json!({})).await["error"]["code"],
        -32003
    );
    assert_eq!(
        member
            .call("terminal.list", json!({"workspaceId":ws}))
            .await["error"]["code"],
        -32003
    );
    drop(member);
    srv.ws.stop().await;
}

/// Host membership grants ordinary-workspace `CoW` operations, not access to
/// the chief workspace; a caller-supplied workspace cannot hide an agent's scope.
#[tokio::test]
async fn sandbox_cow_authorization_preserves_owner_member_and_guest_boundaries() {
    use intent_core::AgentId;
    use intent_store::{Sandbox, SandboxStatus};

    let srv = start(WsOptions::default()).await;
    let ordinary = WorkspaceId::from("cow-members");
    let chief = WorkspaceId::chief();
    let ordinary_agent = AgentId::from("ordinary-agent");
    let chief_agent = AgentId::from("chief-agent");
    srv.store
        .insert_workspace(&fixture_workspace(&ordinary))
        .await
        .unwrap();
    srv.store
        .get_workspace(&chief)
        .await
        .expect("startup seeds the chief workspace");
    for (workspace, agent) in [(&ordinary, &ordinary_agent), (&chief, &chief_agent)] {
        sqlx::query(
            "INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
             VALUES (?1, ?2, ?1, 'idle', 't0', 't0')",
        )
        .bind(&agent.0)
        .bind(&workspace.0)
        .execute(srv.store.write_pool())
        .await
        .unwrap();
        let path = srv.dir.path().join(agent.as_str());
        std::fs::create_dir(&path).unwrap();
        srv.store
            .insert_sandbox(&Sandbox {
                id: format!("sandbox-{}", agent.0),
                workspace_id: workspace.clone(),
                agent_id: agent.clone(),
                path: path.to_string_lossy().into_owned(),
                branch: format!("sb/{}", agent.0),
                base_commit_sha: "base".into(),
                snapshot_commit_sha: None,
                last_merged_commit_sha: None,
                // The real merge acknowledgement exercises authorization without
                // launching a merge worker or requiring a platform-specific clone.
                status: SandboxStatus::Merging,
                retry_count: 0,
                merge_on_turn_end: true,
                conflicting_paths: vec![],
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .await
            .unwrap();
        let owner = wss_call(
            srv.port,
            srv.cfg.clone(),
            &json!({
                "jsonrpc":"2.0", "id":1, "method":"sandbox.cow.merge",
                "params":{"workspaceId":workspace,"agentId":agent},
            })
            .to_string(),
        )
        .await;
        assert_eq!(owner["result"]["status"], "in_progress", "{owner}");
    }

    let mut member = Guest::connect(&srv, &"d8".repeat(32)).await;
    srv.store
        .add_workspace_member(&ordinary, &member.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let params = json!({"workspaceId":ordinary,"agentId":ordinary_agent});
    for method in ["sandbox.cow.merge", "sandbox.cow.discard"] {
        let refused = member.call(method, params.clone()).await;
        assert_eq!(
            refused["error"]["code"], -32003,
            "guest {method}: {refused}"
        );
        assert!(refused.get("result").is_none(), "{refused}");
    }
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace_member WHERE workspace_id=? AND principal_id=?")
        .bind(&ordinary.0)
        .bind(&member.principal.id.0)
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    let inherited = member
        .call("workspace.get", json!({"workspaceId":ordinary}))
        .await;
    assert_eq!(
        inherited["result"]["workspace"]["canManage"], true,
        "{inherited}"
    );
    let allowed = member.call("sandbox.cow.merge", params.clone()).await;
    assert_eq!(allowed["result"]["status"], "in_progress", "{allowed}");

    // Both the real and a forged ordinary workspace id must be refused for
    // the chief agent. Authorization resolves the agent's stored workspace.
    for workspace in [&chief, &ordinary] {
        for method in ["sandbox.cow.merge", "sandbox.cow.discard"] {
            let refused = member
                .call(
                    method,
                    json!({"workspaceId":workspace,"agentId":chief_agent}),
                )
                .await;
            assert_eq!(
                refused["error"]["data"]["code"], "not-found",
                "{method}: {refused}"
            );
            assert!(refused.get("result").is_none(), "{refused}");
        }
    }
    let kept = srv
        .store
        .get_sandbox(&chief, &chief_agent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.status, SandboxStatus::Merging);
    assert!(Path::new(&kept.path).is_dir());

    let discarded = member.call("sandbox.cow.discard", params).await;
    assert_eq!(discarded["result"]["ok"], true, "{discarded}");
    assert!(srv
        .store
        .get_sandbox(&ordinary, &ordinary_agent)
        .await
        .unwrap()
        .is_none());
    assert!(!srv.dir.path().join(ordinary_agent.as_str()).exists());
    assert!(srv
        .store
        .get_sandbox(&chief, &chief_agent)
        .await
        .unwrap()
        .is_some());

    let owner = wss_call(
        srv.port,
        srv.cfg.clone(),
        &json!({
            "jsonrpc":"2.0", "id":2, "method":"sandbox.cow.discard",
            "params":{"workspaceId":chief,"agentId":chief_agent},
        })
        .to_string(),
    )
    .await;
    assert_eq!(owner["result"]["ok"], true, "{owner}");
    assert!(srv
        .store
        .get_sandbox(&chief, &chief_agent)
        .await
        .unwrap()
        .is_none());
    drop(member);
    srv.ws.stop().await;
}
