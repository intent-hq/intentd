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

    let nested = squad.join("a-depth/a/b/c/d/e/f");
    std::fs::create_dir_all(&nested).unwrap();
    let shared = dir.path().join("shared-depth");
    write_agent(
        &shared.join("nested/reachable.md"),
        "shallow-reachable",
        "",
        "Reached through the shallow alias.",
    );
    symlink(&shared, nested.join("deep")).unwrap();
    symlink(&shared, squad.join("z-shallow")).unwrap();

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
        definition(&list, "shallow-reachable")["prompt"],
        "Reached through the shallow alias."
    );
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

#[tokio::test]
async fn claude_agents_report_exclusions_and_clear_repaired_diagnostics() {
    let dir = scratch_dir("claude-diagnostics");
    let home = dir.path().join("home");
    let root = home.join(".claude/agents");
    write_agent(&root.join("a.md"), "collision", "", "Winner.");
    write_agent(&root.join("b.md"), "collision", "", "Shadowed.");
    write_agent(
        &root.join("developer.md"),
        "developer",
        "",
        "Shadowed built-in.",
    );
    std::fs::write(root.join("invalid.md"), "---\nname: [\n---\n").unwrap();
    std::fs::write(root.join("binary.md"), [0xff, 0xfe]).unwrap();
    std::fs::File::create(root.join("large.md"))
        .unwrap()
        .set_len(1_048_577)
        .unwrap();
    symlink(root.join("missing-target.md"), root.join("broken.md")).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg.clone()).await;
    let before = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    assert_eq!(definition(&before, "collision")["prompt"], "Winner.");
    let diagnostics = before["importDiagnostics"].as_array().unwrap();
    for (file, code) in [
        ("b.md", "shadowed"),
        ("developer.md", "shadowed"),
        ("invalid.md", "invalid"),
        ("binary.md", "unreadable"),
        ("large.md", "too-large"),
        ("broken.md", "broken-link"),
    ] {
        assert!(
            diagnostics
                .iter()
                .any(|d| d["path"] == root.join(file).to_string_lossy().as_ref()
                    && d["code"] == code
                    && d["source"] == "user"),
            "{file}: {before}"
        );
    }
    let duplicate = diagnostics
        .iter()
        .find(|d| d["path"] == root.join("b.md").to_string_lossy().as_ref())
        .unwrap();
    assert_eq!(duplicate["specialistId"], "collision");
    assert_eq!(
        duplicate["winnerPath"],
        root.join("a.md").to_string_lossy().as_ref()
    );
    let workspace = wss_rpc(
        &mut client,
        3,
        "workspace.create",
        json!({"title":"Import diagnostic repair"}),
    )
    .await;
    let mut subscription = connect_ws(port, cfg).await;
    wss_rpc(
        &mut subscription,
        1,
        "events.subscribe",
        json!({"eventTypes":["specialists:changed"],"workspaceId":workspace["workspace"]["id"]}),
    )
    .await;
    write_agent(&root.join("invalid.md"), "repaired", "", "Repaired.");
    let event = next_event(&mut subscription, &["specialists:changed"], 20).await;
    let after = wss_rpc(&mut client, 2, "specialist.list", json!({})).await;
    assert_eq!(definition(&after, "repaired")["prompt"], "Repaired.");
    assert!(!after["importDiagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["path"] == root.join("invalid.md").to_string_lossy().as_ref()));
    std::fs::write(
        dir.path().join("claude-diagnostics-evidence.json"),
        serde_json::to_vec_pretty(&json!({"before":before,"event":event,"after":after})).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_missing_skills_block_creation_and_recover_in_project_scope() {
    let dir = scratch_dir("claude-dependencies");
    let home = dir.path().join("home");
    write_agent(
        &home.join(".claude/agents/reviewer.md"),
        "reviewer",
        "skills: [code-review, missing-kit]\n",
        "Review.",
    );
    let installed = home.join(".claude/skills/review/SKILL.md");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    std::fs::write(
        &installed,
        "---\nname: code-review\ndescription: Reviews code\n---\nUse the kit.",
    )
    .unwrap();
    let checkout = dir.path().join("checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let before = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    assert_eq!(
        definition(&before, "reviewer")["requiredSkills"],
        json!(["code-review", "missing-kit"])
    );
    assert_eq!(
        definition(&before, "reviewer")["missingSkills"],
        json!(["missing-kit"])
    );
    let workspace = wss_rpc(
        &mut client,
        2,
        "workspace.create",
        json!({"title":"Required skill repair", "skipWorktree":true,"path":checkout}),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].clone();
    let denied = wss_reply(
        &mut client,
        3,
        "agent.create",
        json!({"workspaceId":workspace_id,"specialistId":"reviewer","provider":"mock"}),
    )
    .await;
    assert_eq!(denied["error"]["code"], -32602, "{denied}");
    assert!(
        denied["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing-kit"),
        "{denied}"
    );
    let sessions = wss_rpc(
        &mut client,
        4,
        "agent.list",
        json!({"workspaceId":workspace_id}),
    )
    .await;
    assert!(sessions["agents"].as_array().unwrap().is_empty());
    let repaired = checkout.join(".intent/skills/kit/SKILL.md");
    std::fs::create_dir_all(repaired.parent().unwrap()).unwrap();
    std::fs::write(
        &repaired,
        "---\nname: missing-kit\ndescription: Installed project kit\n---\nFollow this kit.",
    )
    .unwrap();
    let after = wss_rpc(
        &mut client,
        5,
        "specialist.get",
        json!({"id":"reviewer","workspacePath":checkout}),
    )
    .await;
    assert!(
        after["specialist"].get("missingSkills").is_none(),
        "{after}"
    );
    let global = wss_rpc(&mut client, 6, "specialist.list", json!({})).await;
    assert_eq!(
        definition(&global, "reviewer")["missingSkills"],
        json!(["missing-kit"])
    );
    wss_rpc(
        &mut client,
        7,
        "settings.update",
        json!({"changes":[{"path":"providers.paths", "value":{"auggie":"/bin/sh"}}]}),
    )
    .await;
    let created = wss_rpc(
        &mut client,
        8,
        "agent.create",
        json!({"workspaceId":workspace_id,"specialistId":"reviewer","provider":"auggie"}),
    )
    .await;
    assert_eq!(created["agent"]["metadata"]["specialist"], "reviewer");
    std::fs::remove_file(&installed).unwrap();
    let removed = wss_rpc(
        &mut client,
        9,
        "specialist.get",
        json!({"id":"reviewer","workspacePath":checkout}),
    )
    .await;
    assert_eq!(
        removed["specialist"]["missingSkills"],
        json!(["code-review"])
    );
    std::fs::write(
        dir.path().join("claude-dependency-evidence.json"),
        serde_json::to_vec_pretty(
            &json!({"before":before,"denied":denied,"after":after,"removed":removed}),
        )
        .unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_limits_are_explicit_and_deterministic() {
    let dir = scratch_dir("claude-limits");
    let home = dir.path().join("home");
    let root = home.join(".claude/agents");
    write_agent(&root.join("a.md"), "kept", "", "Keep the valid prefix.");
    let crowded = root.join("crowded");
    std::fs::create_dir_all(&crowded).unwrap();
    for index in 0..4097 {
        std::fs::write(crowded.join(format!("{index}.txt")), "").unwrap();
    }
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let first = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    let second = wss_rpc(&mut client, 2, "specialist.list", json!({})).await;
    assert_eq!(first, second);
    assert_eq!(
        definition(&first, "kept")["prompt"],
        "Keep the valid prefix."
    );
    assert!(
        first["importDiagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "scan-limit"
                && d["path"] == crowded.to_string_lossy().as_ref()
                && d["isDirectory"] == true),
        "{first}"
    );
    std::fs::remove_dir_all(&crowded).unwrap();
    for index in 0..130 {
        std::fs::write(
            root.join(format!("invalid-{index}.md")),
            "Invalid frontmatter",
        )
        .unwrap();
    }
    let capped = wss_rpc(&mut client, 3, "specialist.list", json!({})).await;
    assert_eq!(capped["importDiagnostics"].as_array().unwrap().len(), 128);
    assert_eq!(capped["importDiagnostics"][127]["code"], "scan-limit");
    std::fs::remove_dir_all(&root).unwrap();
    let payloads = dir.path().join("linked-payloads");
    std::fs::create_dir_all(&payloads).unwrap();
    let invalid_bytes = vec![0xff; 1_048_576];
    for index in 0..32 {
        std::fs::write(payloads.join(format!("{index:02}.md")), &invalid_bytes).unwrap();
    }
    write_agent(
        &root.join("z-valid.md"),
        "after-budget",
        "",
        "Available after repair.",
    );
    symlink(&payloads, root.join("payloads")).unwrap();
    let exhausted = wss_rpc(&mut client, 4, "specialist.list", json!({})).await;
    assert!(!exhausted["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["id"] == "after-budget"));
    assert!(
        exhausted["importDiagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "scan-limit"
                && d["path"] == root.join("z-valid.md").to_string_lossy().as_ref()
                && d["isDirectory"] == false),
        "{exhausted}"
    );
    std::fs::remove_file(root.join("payloads")).unwrap();
    std::fs::remove_dir_all(&payloads).unwrap();
    let restored = wss_rpc(&mut client, 5, "specialist.list", json!({})).await;
    assert_eq!(
        definition(&restored, "after-budget")["prompt"],
        "Available after repair."
    );
    std::fs::write(
        dir.path().join("claude-limit-evidence.json"),
        serde_json::to_vec_pretty(
            &json!({"entries":first,"diagnostics":capped,"bytes":exhausted,"restored":restored}),
        )
        .unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_custom_config_root_discovers_and_watches_agents_and_skills() {
    let dir = scratch_dir("claude-config");
    let home = dir.path().join("home");
    let config = dir.path().join("custom-config");
    let target = dir.path().join("shared-config");
    write_agent(
        &home.join(".claude/agents/default.md"),
        "not-from-config",
        "",
        "Default root.",
    );
    write_agent(
        &target.join("agents/custom.md"),
        "configured",
        "skills: [config-kit]\n",
        "Custom root.",
    );
    std::fs::create_dir_all(target.join("skills/kit")).unwrap();
    std::fs::write(
        target.join("skills/kit/SKILL.md"),
        "---\nname: config-kit\ndescription: Configuration kit\n---\nFollow.",
    )
    .unwrap();
    symlink(&target, &config).unwrap();
    let child = common::DaemonGuard::process_only(spawn_serve_with_claude_config(
        dir.path(),
        &home,
        Some(&config),
    ));
    let (daemon, port, cfg) = await_boot(dir.path(), child).await;
    let mut client = connect_ws(port, cfg.clone()).await;
    let first = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    assert!(
        definition(&first, "configured")
            .get("missingSkills")
            .is_none(),
        "{first}"
    );
    assert!(!first["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["id"] == "not-from-config"));
    let workspace = wss_rpc(
        &mut client,
        2,
        "workspace.create",
        json!({"title":"Custom config"}),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].clone();
    let skills = wss_rpc(
        &mut client,
        3,
        "skill.list",
        json!({"workspaceId":workspace_id}),
    )
    .await;
    assert!(
        skills
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "config-kit"),
        "{skills}"
    );
    let mut subscription = connect_ws(port, cfg).await;
    wss_rpc(
        &mut subscription,
        1,
        "events.subscribe",
        json!({"eventTypes":["specialists:changed"],"workspaceId":workspace_id}),
    )
    .await;
    write_agent(
        &target.join("agents/custom.md"),
        "configured",
        "skills: [config-kit]\n",
        "Updated custom root.",
    );
    let event = next_event(&mut subscription, &["specialists:changed"], 20).await;
    let after = wss_rpc(&mut client, 4, "specialist.get", json!({"id":"configured"})).await;
    assert!(after["specialist"]["prompt"]
        .as_str()
        .unwrap()
        .contains("Updated custom root."));
    std::fs::write(
        dir.path().join("claude-config-evidence.json"),
        serde_json::to_vec_pretty(
            &json!({"before":first,"skills":skills,"event":event,"after":after}),
        )
        .unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}
