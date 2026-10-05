//! Repository-free skill discovery through authenticated, fingerprint-pinned TLS WSS.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

#[intent_test_macros::daemon_test]
async fn wss_skill_list_repo_less_preserves_authorization_and_does_not_provision() {
    let srv = start(WsOptions::default()).await;
    let mut owner = Guest {
        principal: srv.store.get_primary_principal().await.unwrap(),
        ws: connect_ws(srv.port, srv.cfg.clone()).await,
        next_id: 0,
    };
    let created = owner
        .call(
            "workspace.create",
            json!({"title":"Skills without a repository"}),
        )
        .await;
    let id = WorkspaceId::from(
        created["result"]["workspace"]["id"]
            .as_str()
            .expect("workspace id"),
    );
    let before = serde_json::to_value(srv.store.get_workspace(&id).await.unwrap()).unwrap();
    let root = srv.dir.path().join("workspaces");
    let entries_before = std::fs::read_dir(&root).unwrap().count();
    let listed = owner.call("skill.list", json!({"workspaceId":id})).await;
    let skills = listed["result"]
        .as_array()
        .unwrap_or_else(|| panic!("skill.list must succeed: {listed}"));
    assert!(
        skills.iter().all(|skill| skill["scope"] == "user"),
        "{listed}"
    );
    assert!(skills
        .windows(2)
        .all(|pair| pair[0]["name"].as_str() <= pair[1]["name"].as_str()));
    for skill in skills {
        assert!(skill["name"].is_string());
        assert!(skill["description"].is_string());
        assert!(skill["location"].is_string());
    }
    assert_eq!(
        serde_json::to_value(srv.store.get_workspace(&id).await.unwrap()).unwrap(),
        before
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), entries_before);
    assert!(before["worktreePath"].is_null());
    assert!(before["repositoryPath"].is_null());

    let missing = owner
        .call(
            "skill.list",
            json!({"workspaceId":"missing-skills-workspace"}),
        )
        .await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");
    let mut guest = Guest::connect(&srv, &"bd".repeat(32)).await;
    let denied = guest.call("skill.list", json!({"workspaceId":id})).await;
    assert_eq!(denied["error"]["code"], -32602, "{denied}");
    srv.store
        .add_workspace_member(&id, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let allowed = guest.call("skill.list", json!({"workspaceId":id})).await;
    assert_eq!(allowed["result"], listed["result"], "{allowed}");
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn wss_skill_list_configured_keeps_project_precedence() {
    let srv = start(WsOptions::default()).await;
    let project = srv.dir.path().join("project");
    for (tier, description) in [(".agents", "base"), (".intent", "override")] {
        let dir = project
            .join(tier)
            .join("skills/intent-5862-project-fixture");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!(
                "---\nname: intent-5862-project-fixture\ndescription: {description}\n---\nBody\n"
            ),
        )
        .unwrap();
    }
    let id = WorkspaceId::new();
    let mut row = fixture_workspace(&id);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let listed = wss_call(
        srv.port,
        srv.cfg.clone(),
        &json!({
            "jsonrpc":"2.0", "id":1, "method":"skill.list", "params":{"workspaceId":id}
        })
        .to_string(),
    )
    .await;
    assert_eq!(listed["jsonrpc"], "2.0");
    assert_eq!(listed["id"], 1);
    assert!(listed.get("error").is_none(), "{listed}");
    let skills = listed["result"].as_array().unwrap();
    let matching: Vec<_> = skills
        .iter()
        .filter(|s| s["name"] == "intent-5862-project-fixture")
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0]["scope"], "project");
    assert_eq!(matching[0]["description"], "override");
    assert!(skills
        .windows(2)
        .all(|pair| pair[0]["name"].as_str() <= pair[1]["name"].as_str()));
    srv.ws.stop().await;
}
