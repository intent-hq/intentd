use super::*;
use std::os::unix::fs::symlink;

fn write_agent(path: &Path, name: &str, extra: &str, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        format!(
            "---\nname: {name}\ndescription: >-\n  Reviews code\n  carefully\n{extra}---\n{body}\n"
        ),
    )
    .unwrap();
}

fn definition<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == id)
        .unwrap_or_else(|| panic!("missing {id}: {list}"))
}

#[tokio::test]
async fn claude_agents_discovery_follows_links_and_preserves_precedence() {
    let dir = scratch_dir("claude-import");
    let home = dir.path().join("home");
    let squad = dir.path().join("squad");
    std::fs::create_dir_all(home.join(".claude")).unwrap();
    write_agent(
        &squad.join("nested/filename.md"),
        "review-helper",
        "model: sonnet\nskills: [code-review]\n",
        "Review the patch.",
    );
    write_agent(
        &squad.join("inherited.md"),
        "inherited",
        "model: inherit\n",
        "Use the default model.",
    );
    write_agent(
        &squad.join("developer.md"),
        "developer",
        "",
        "Must not replace the Intent developer.",
    );
    write_agent(
        &dir.path().join("linked.md"),
        "file-linked",
        "model: claude-custom-model\n",
        "Linked file prompt.",
    );
    symlink(dir.path().join("linked.md"), squad.join("file-linked.md")).unwrap();
    symlink(&squad, squad.join("cycle")).unwrap();
    symlink(dir.path().join("missing"), squad.join("broken.md")).unwrap();
    std::fs::write(squad.join("invalid.md"), "---\nname: [\n---\nInvalid").unwrap();
    symlink(&squad, home.join(".claude/agents")).unwrap();

    let project = dir.path().join("project");
    write_agent(
        &project.join(".claude/agents/review-helper.md"),
        "review-helper",
        "model: opus\n",
        "Project prompt.",
    );
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let list = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    let imported = definition(&list, "review-helper");
    assert_eq!(imported["name"], "review-helper");
    assert_eq!(imported["description"], "Reviews code carefully");
    assert_eq!(imported["codingAgent"], "claude-code");
    assert_eq!(imported["model"], "sonnet");
    assert_eq!(imported["importedFrom"], "claude-code");
    assert_eq!(imported["source"], "user");
    assert!(imported["prompt"].as_str().unwrap().contains("code-review"));
    assert!(imported["prompt"]
        .as_str()
        .unwrap()
        .contains("Review the patch."));
    assert!(imported.get("unsupportedFields").is_none());
    assert!(definition(&list, "inherited").get("model").is_none());
    assert_eq!(
        definition(&list, "file-linked")["model"],
        "claude-custom-model"
    );
    assert!(definition(&list, "developer").get("importedFrom").is_none());
    assert_eq!(
        list["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["id"] == "review-helper")
            .count(),
        1
    );

    let got = wss_rpc(
        &mut client,
        2,
        "specialist.get",
        json!({"id":"review-helper", "workspacePath":project}),
    )
    .await;
    assert_eq!(got["specialist"]["prompt"], "Project prompt.");
    assert_eq!(got["specialist"]["source"], "project");
    write_agent(
        &home.join(".intent/specialists/review-helper.md"),
        "Intent override",
        "",
        "Intent prompt.",
    );
    let got = wss_rpc(
        &mut client,
        3,
        "specialist.get",
        json!({"id":"review-helper", "workspacePath":project}),
    )
    .await;
    assert_eq!(got["specialist"]["prompt"], "Intent prompt.");
    assert!(got["specialist"].get("importedFrom").is_none());
    assert!(
        got["specialist"].get("codingAgent").is_none(),
        "imports must not leak provider pins into Intent overrides"
    );
    wss_rpc(
        &mut client,
        4,
        "settings.update",
        json!({"changes":[{"path":"providers.paths", "value":{"auggie":"/bin/sh"}}]}),
    )
    .await;
    let workspace = wss_rpc(
        &mut client,
        5,
        "workspace.create",
        json!({"title":"Imported agent creation"}),
    )
    .await;
    let created = wss_rpc(&mut client, 6, "agent.create", json!({"workspaceId":workspace["workspace"]["id"], "specialistId":"inherited", "provider":"auggie"})).await;
    assert_eq!(created["agent"]["name"], "inherited");
    assert_eq!(created["agent"]["metadata"]["specialist"], "inherited");
    assert_eq!(created["agent"]["provider"], "auggie");
    std::fs::write(
        dir.path().join("claude-discovery-result.json"),
        serde_json::to_vec_pretty(&list).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_are_read_only_and_unsupported_controls_block_creation() {
    let dir = scratch_dir("claude-readonly");
    let home = dir.path().join("home");
    let original = home.join(".claude/agents/restricted.md");
    write_agent(
        &original,
        "restricted",
        "tools: Read, Grep\npermissionMode: plan\nhooks: {}\n",
        "Read only.",
    );
    let before = std::fs::read(&original).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let list = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    assert_eq!(
        definition(&list, "restricted")["unsupportedFields"],
        json!(["hooks", "permissionMode", "tools"])
    );
    let workspace = wss_rpc(
        &mut client,
        2,
        "workspace.create",
        json!({"title":"Imported agents"}),
    )
    .await;
    let denied = wss_reply(&mut client, 3, "agent.create", json!({"workspaceId":workspace["workspace"]["id"], "specialistId":"restricted", "name":"Restricted", "provider":"mock"})).await;
    assert_eq!(denied["error"]["code"], -32602, "{denied}");
    assert!(
        denied["error"]["message"]
            .as_str()
            .unwrap()
            .contains("permissionMode"),
        "{denied}"
    );
    let agents = wss_rpc(
        &mut client,
        30,
        "agent.list",
        json!({"workspaceId":workspace["workspace"]["id"]}),
    )
    .await;
    assert!(
        agents["agents"].as_array().unwrap().is_empty(),
        "rejected imports must not persist sessions: {agents}"
    );
    for (id, method) in [(4, "specialist.edit"), (5, "specialist.delete")] {
        let denied = wss_reply(&mut client, id, method, json!({"id":"restricted", "scope":"user", "spec":{"name":"Changed", "behaviorPrompt":"Changed"}})).await;
        assert_eq!(denied["error"]["code"], -32602, "{denied}");
        assert!(
            denied["error"]["message"]
                .as_str()
                .unwrap()
                .contains("read-only"),
            "{denied}"
        );
        assert_eq!(std::fs::read(&original).unwrap(), before);
    }
    let created = wss_rpc(&mut client, 6, "specialist.create", json!({"id":"restricted", "scope":"user", "spec":{"name":"Intent version", "behaviorPrompt":"Explicit Intent definition."}})).await;
    assert!(created["specialist"].get("importedFrom").is_none());
    assert!(created["specialist"].get("unsupportedFields").is_none());
    assert_eq!(std::fs::read(&original).unwrap(), before);
    wss_rpc(
        &mut client,
        7,
        "specialist.delete",
        json!({"id":"restricted", "scope":"user"}),
    )
    .await;
    let restored = wss_rpc(&mut client, 8, "specialist.get", json!({"id":"restricted"})).await;
    assert_eq!(restored["specialist"]["importedFrom"], "claude-code");
    std::fs::write(
        dir.path().join("claude-readonly-result.json"),
        serde_json::to_vec_pretty(&restored).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_external_target_edits_and_link_retargets_emit_changes() {
    let dir = scratch_dir("claude-watch");
    let home = dir.path().join("home");
    let squad = dir.path().join("squad");
    let replacement = dir.path().join("replacement");
    std::fs::create_dir_all(home.join(".claude")).unwrap();
    write_agent(
        &squad.join("reviewer.md"),
        "reviewer",
        "",
        "Initial prompt.",
    );
    write_agent(
        &replacement.join("reviewer.md"),
        "reviewer",
        "",
        "Retargeted prompt.",
    );
    symlink(&squad, home.join(".claude/agents")).unwrap();
    std::fs::create_dir_all(dir.path().join("checkout")).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut client,
        1,
        "workspace.create",
        json!({"title":"Claude watcher", "skipWorktree":true, "path":dir.path().join("checkout")}),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].clone();
    let mut subscription = connect_ws(port, cfg).await;
    wss_rpc(
        &mut subscription,
        1,
        "events.subscribe",
        json!({"eventTypes":["specialists:changed"], "workspaceId":workspace_id}),
    )
    .await;
    let mut evidence = Vec::new();
    for (id, expected) in [(2, "Edited prompt."), (3, "Retargeted prompt.")] {
        if id == 2 {
            write_agent(&squad.join("reviewer.md"), "reviewer", "", expected);
        } else {
            let staged = home.join(".claude/agents-next");
            symlink(&replacement, &staged).unwrap();
            std::fs::rename(staged, home.join(".claude/agents")).unwrap();
        }
        let event = next_event(&mut subscription, &["specialists:changed"], 20).await;
        assert_eq!(event["data"], json!({"workspaceId":workspace_id}));
        let current = wss_rpc(&mut client, id, "specialist.get", json!({"id":"reviewer"})).await;
        assert_eq!(current["specialist"]["prompt"], expected);
        evidence.push(json!({"event":event, "specialist":current}));
    }
    std::fs::write(
        dir.path().join("claude-watch-result.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}
