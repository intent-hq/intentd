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

#[tokio::test]
async fn shared_global_rules_over_wss_preserve_owner_writes_and_workspace_boundaries() {
    let srv = start(WsOptions::default()).await;
    let shared = WorkspaceId::new();
    let hidden = WorkspaceId::new();
    for id in [&shared, &hidden] {
        srv.store
            .insert_workspace(&fixture_workspace(id))
            .await
            .unwrap();
    }
    let guest_token = "d8".repeat(32);
    let mut guest = Guest::connect(&srv, &guest_token).await;
    let mut member = Guest::connect(&srv, &"e8".repeat(32)).await;
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    srv.store
        .add_workspace_member(&shared, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let params = json!({"workspaceId":"global","ruleType":"base-system-prompt"});
    let updated = wss_call(srv.port, srv.cfg.clone(), &json!({"jsonrpc":"2.0","id":1,"method":"rules.update","params":{"workspaceId":"global","ruleType":"base-system-prompt","content":"Shared host instructions","enabled":true}}).to_string()).await;
    assert!(updated.get("error").is_none(), "{updated}");
    for reader in [&mut guest, &mut member] {
        let got = reader.call("rules.get", params.clone()).await;
        assert_eq!(
            got["result"]["content"], "Shared host instructions",
            "{got}"
        );
        assert_eq!(got["result"]["enabled"], true);
        assert!(got["result"]["updatedAt"].is_i64());
        assert!(reader
            .call("specialist.list", json!({}))
            .await
            .get("error")
            .is_none());
        for (method, args) in [
            (
                "rules.update",
                json!({"workspaceId":shared,"ruleType":"base-system-prompt","content":"forbidden"}),
            ),
            (
                "rules.update",
                json!({"workspaceId":"global","ruleType":"base-system-prompt","content":"forbidden"}),
            ),
            ("specialist.create", json!({"id":"forbidden","spec":{}})),
            ("specialist.edit", json!({"id":"implementor","spec":{}})),
            ("specialist.delete", json!({"id":"implementor"})),
            ("settings.list", json!({})),
        ] {
            let denied = reader.call(method, args).await;
            assert_eq!(denied["error"]["code"], -32003, "{method}: {denied}");
        }
        for ws in [json!("global"), json!(WorkspaceId::chief())] {
            assert!(reader
                .call(
                    "rules.get",
                    json!({"workspaceId":ws,"ruleType":"workspace"})
                )
                .await
                .get("error")
                .is_some());
        }
    }
    assert!(guest
        .call("rules.list", json!({"workspaceId":shared}))
        .await
        .get("error")
        .is_some());
    assert!(guest
        .call(
            "rules.get",
            json!({"workspaceId":hidden,"ruleType":"base-system-prompt"})
        )
        .await
        .get("error")
        .is_some());
    srv.store
        .remove_workspace_member(&shared, &guest.principal.id)
        .await
        .unwrap();
    assert!(
        guest
            .call("rules.get", params.clone())
            .await
            .get("error")
            .is_some(),
        "removed guest"
    );
    srv.store
        .add_workspace_member(&shared, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    assert!(guest
        .call("rules.get", params.clone())
        .await
        .get("error")
        .is_none());
    assert!(guest
        .call("principal.revokeSelf", json!({}))
        .await
        .get("error")
        .is_none());
    assert_eq!(
        status_code(
            &https_request(
                srv.port,
                srv.cfg.clone(),
                &upgrade_req("/ws", None, Some(&guest_token))
            )
            .await
        ),
        401,
        "revoked token cannot reconnect to read instructions"
    );
    sqlx::query("DELETE FROM host_member WHERE principal_id = ?")
        .bind(&member.principal.id.0)
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert!(
        member
            .call("rules.get", params)
            .await
            .get("error")
            .is_some(),
        "removed member"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn shared_specialist_project_paths_over_wss_require_guest_membership() {
    let tree = common::test_tempdir("shared-specialist-wss");
    let allowed = tree.path().join("allowed");
    let hidden = tree.path().join("hidden");
    for dir in [&allowed, &hidden] {
        let specialists = dir.join(".intent/specialists");
        std::fs::create_dir_all(&specialists).unwrap();
        std::fs::write(
            specialists.join("project-only.md"),
            "---\nname: Project only\ndescription: Project definition\n---\nProject instructions",
        )
        .unwrap();
    }
    let srv = start(WsOptions::default()).await;
    let shared = WorkspaceId::new();
    let mut row = fixture_workspace(&shared);
    row.worktree_path = Some(allowed.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let mut guest = Guest::connect(&srv, &"f8".repeat(32)).await;
    srv.store
        .add_workspace_member(&shared, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let got = guest
        .call(
            "specialist.get",
            json!({"id":"project-only","workspacePath":allowed}),
        )
        .await;
    assert_eq!(got["result"]["specialist"]["name"], "Project only", "{got}");
    let listed = guest
        .call(
            "specialist.list",
            json!({"workspaceId":shared,"includeProject":true}),
        )
        .await;
    assert!(
        listed["result"]["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["id"] == "project-only"),
        "{listed}"
    );
    let denied = guest
        .call(
            "specialist.get",
            json!({"id":"project-only","workspacePath":hidden}),
        )
        .await;
    assert_eq!(denied["error"]["code"], -32003, "{denied}");
    let global = guest
        .call("specialist.get", json!({"id":"implementor"}))
        .await;
    assert_eq!(
        global["result"]["specialist"]["id"], "implementor",
        "{global}"
    );
    srv.store
        .remove_workspace_member(&shared, &guest.principal.id)
        .await
        .unwrap();
    let denied = guest
        .call(
            "specialist.get",
            json!({"id":"project-only","workspacePath":allowed}),
        )
        .await;
    assert_eq!(denied["error"]["code"], -32003, "{denied}");
    srv.ws.stop().await;
}
