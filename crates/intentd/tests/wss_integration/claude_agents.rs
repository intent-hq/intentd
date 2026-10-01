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
    let checkout = dir.path().join("checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let workspace = wss_rpc(
        &mut client,
        3,
        "workspace.create",
        json!({"title":"Import diagnostic repair","skipIsolation":true,"worktreePath":checkout}),
    )
    .await;
    assert_eq!(
        workspace["workspace"]["worktreePath"],
        checkout.to_string_lossy().as_ref()
    );
    stop(daemon, &dir.path().join("intentd.sock")).await;
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg.clone()).await;
    let mut subscription = connect_ws(port, cfg).await;
    wss_rpc(
        &mut subscription,
        1,
        "events.subscribe",
        json!({"eventTypes":["specialists:changed"],"workspaceId":workspace["workspace"]["id"]}),
    )
    .await;
    let mut watch_ready = None;
    for attempt in 0..20 {
        write_agent(
            &root.join("a.md"),
            "collision",
            "",
            &format!("Watcher readiness probe {attempt}."),
        );
        if let Ok(event) = tokio::time::timeout(
            common::test_timeout(Duration::from_millis(750)),
            next_event(&mut subscription, &["specialists:changed"], 20),
        )
        .await
        {
            watch_ready = Some(event);
            break;
        }
    }
    let watch_ready = watch_ready.expect("specialist watch must observe a readiness probe");
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
        serde_json::to_vec_pretty(
            &json!({"before":before,"watchReady":watch_ready,"event":event,"after":after}),
        )
        .unwrap(),
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
        json!({"title":"Required skill repair", "skipIsolation":true,"worktreePath":checkout}),
    )
    .await;
    assert_eq!(
        workspace["workspace"]["worktreePath"],
        checkout.to_string_lossy().as_ref()
    );
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
    let checkout = dir.path().join("checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let workspace = wss_rpc(
        &mut client,
        2,
        "workspace.create",
        json!({"title":"Custom config","skipIsolation":true,"worktreePath":checkout}),
    )
    .await;
    assert_eq!(
        workspace["workspace"]["worktreePath"],
        checkout.to_string_lossy().as_ref()
    );
    stop(daemon, &dir.path().join("intentd.sock")).await;
    let child = common::DaemonGuard::process_only(spawn_serve_with_claude_config(
        dir.path(),
        &home,
        Some(&config),
    ));
    let (daemon, port, cfg) = await_boot(dir.path(), child).await;
    let mut client = connect_ws(port, cfg.clone()).await;
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

#[tokio::test]
async fn claude_agents_preview_requests_stay_fresh_and_workspace_scoped() {
    let dir = scratch_dir("claude-preview-snapshot");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let other = dir.path().join("other-project");
    let user_file = home.join(".claude/agents/reviewer.md");
    let project_file = project.join(".claude/agents/reviewer.md");
    write_agent(&user_file, "reviewer", "model: sonnet\n", "User prompt.");
    write_agent(
        &project_file,
        "reviewer",
        "model: opus\n",
        "Project prompt.",
    );
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let mut evidence = Vec::new();
    let mut request_id = 1;
    for (path, model, prompt) in [
        (&project, "opus", "Project prompt."),
        (&other, "sonnet", "User prompt."),
        (&project, "haiku", "Revised project prompt."),
    ] {
        if model == "haiku" {
            write_agent(&project_file, "reviewer", "model: haiku\n", prompt);
        }
        for method in ["specialist.list", "specialist.get"] {
            let mut params = json!({"workspacePath":path});
            if method == "specialist.get" {
                params["id"] = json!("reviewer");
            }
            let result = wss_rpc(&mut client, request_id, method, params).await;
            request_id += 1;
            let row = if method == "specialist.get" {
                &result["specialist"]
            } else {
                definition(&result, "reviewer")
            };
            let (expected_model, expected_prompt) = if method == "specialist.list" {
                ("sonnet", "User prompt.")
            } else {
                (model, prompt)
            };
            assert_eq!(row["model"], expected_model);
            assert_eq!(row["resolvedModel"], expected_model);
            assert_eq!(row["resolvedProvider"], "claude-code");
            assert_eq!(row["prompt"], expected_prompt);
            if method == "specialist.list" {
                assert!(result.get("importDiagnostics").is_none());
            }
            evidence.push(json!({"method":method,"workspacePath":path,"result":result}));
        }
    }
    std::fs::write(
        dir.path().join("claude-preview-snapshot-evidence.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_project_catalog_requires_explicit_stored_workspace_scope() {
    let dir = scratch_dir("claude-project-catalog");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let other = dir.path().join("other");
    let project_file = project.join(".claude/agents/reviewer.md");
    write_agent(
        &home.join(".claude/agents/reviewer.md"),
        "reviewer",
        "skills: [project-kit]\n",
        "User prompt.",
    );
    write_agent(
        &project_file,
        "reviewer",
        "skills: [project-kit]\n",
        "Project prompt.",
    );
    std::fs::create_dir_all(project.join(".claude/skills/project-kit")).unwrap();
    std::fs::write(
        project.join(".claude/skills/project-kit/SKILL.md"),
        "---\nname: project-kit\ndescription: Project kit\n---\nProject instructions.",
    )
    .unwrap();
    std::fs::create_dir_all(&other).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let mut workspace_ids = Vec::new();
    for (id, path) in [(1, &project), (2, &other)] {
        let workspace = wss_rpc(
            &mut client,
            id,
            "workspace.create",
            json!({"title":"Project catalog","skipIsolation":true,"worktreePath":path}),
        )
        .await;
        assert_eq!(
            workspace["workspace"]["worktreePath"],
            path.to_string_lossy().as_ref()
        );
        workspace_ids.push(workspace["workspace"]["id"].clone());
    }
    let mut evidence = Vec::new();
    let mut request_id = 3;
    for params in [
        json!({}),
        json!({"workspaceId":workspace_ids[0],"workspacePath":project}),
        json!({"workspaceId":workspace_ids[0],"includeProject":false}),
        json!({"workspaceId":workspace_ids[0],"includeProject":null}),
    ] {
        let result = wss_rpc(&mut client, request_id, "specialist.list", params.clone()).await;
        request_id += 1;
        let row = definition(&result, "reviewer");
        assert_eq!(row["source"], "user");
        assert_eq!(row["missingSkills"], json!(["project-kit"]));
        evidence.push(json!({"params":params,"result":result}));
    }
    for (workspace_id, spoofed_path, expected_source) in [
        (&workspace_ids[0], &other, "project"),
        (&workspace_ids[1], &project, "user"),
    ] {
        let params =
            json!({"workspaceId":workspace_id,"workspacePath":spoofed_path,"includeProject":true});
        let result = wss_rpc(&mut client, request_id, "specialist.list", params.clone()).await;
        request_id += 1;
        let row = definition(&result, "reviewer");
        assert_eq!(row["source"], expected_source);
        assert_eq!(
            row.get("missingSkills").is_none(),
            expected_source == "project"
        );
        if expected_source == "project" {
            assert_eq!(row["path"], project_file.to_string_lossy().as_ref());
            assert!(result["importDiagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["code"] == "shadowed"));
        }
        evidence.push(json!({"params":params,"result":result}));
    }
    for params in [
        json!({"includeProject":true}),
        json!({"workspaceId":workspace_ids[0],"includeProject":"true"}),
        json!({"workspaceId":workspace_ids[0],"includeProject":1}),
        json!({"workspaceId":"unknown-workspace","includeProject":true}),
    ] {
        let reply = wss_reply(&mut client, request_id, "specialist.list", params.clone()).await;
        request_id += 1;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
        evidence.push(json!({"params":params,"reply":reply}));
    }
    write_agent(&project_file, "reviewer", "", "Updated project prompt.");
    let updated = wss_rpc(
        &mut client,
        request_id,
        "specialist.list",
        json!({"workspaceId":workspace_ids[0],"includeProject":true}),
    )
    .await;
    request_id += 1;
    assert_eq!(
        definition(&updated, "reviewer")["prompt"],
        "Updated project prompt."
    );
    std::fs::remove_file(&project_file).unwrap();
    let removed = wss_rpc(
        &mut client,
        request_id,
        "specialist.list",
        json!({"workspaceId":workspace_ids[0],"includeProject":true}),
    )
    .await;
    assert_eq!(definition(&removed, "reviewer")["source"], "user");
    assert!(definition(&removed, "reviewer")
        .get("missingSkills")
        .is_none());
    assert!(removed.get("importDiagnostics").is_none());
    evidence.push(json!({"updated":updated,"removed":removed}));
    std::fs::write(
        dir.path().join("claude-project-catalog-evidence.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn claude_agents_preserve_native_aliases_in_catalogs_and_creation() {
    let dir = scratch_dir("claude-native-alias");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    for id in ["coordinator", "review-alias", "native-review"] {
        write_agent(
            &home.join(format!(".claude/agents/{id}.md")),
            id,
            "",
            "Imported prompt.",
        );
    }
    write_agent(
        &home.join(".intent/specialists/native-review.md"),
        "Native reviewer",
        "aliases: [\"review-alias\"]\n",
        "Native prompt.",
    );
    std::fs::create_dir_all(&project).unwrap();
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let list = wss_rpc(&mut client, 1, "specialist.list", json!({})).await;
    for id in ["coordinator", "review-alias"] {
        assert!(!list["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == id));
        assert!(list["importDiagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "shadowed" && d["specialistId"] == id));
    }
    assert!(definition(&list, "native-review")
        .get("importedFrom")
        .is_none());
    let workspace = wss_rpc(
        &mut client,
        2,
        "workspace.create",
        json!({"title":"Native aliases","skipIsolation":true,"worktreePath":project}),
    )
    .await;
    wss_rpc(
        &mut client,
        3,
        "settings.update",
        json!({"changes":[{"path":"providers.paths","value":{"auggie":"/bin/sh"}}]}),
    )
    .await;
    let mut evidence = vec![json!({"catalog":list})];
    let mut request_id = 4;
    for (alias, canonical) in [
        ("coordinator", "spec-writer"),
        ("review-alias", "native-review"),
    ] {
        let got = wss_rpc(
            &mut client,
            request_id,
            "specialist.get",
            json!({"id":alias}),
        )
        .await;
        request_id += 1;
        assert_eq!(got["specialist"]["id"], canonical);
        let created = wss_rpc(&mut client, request_id, "agent.create", json!({"workspaceId":workspace["workspace"]["id"],"specialistId":alias,"provider":"auggie"})).await;
        request_id += 1;
        assert_eq!(created["agent"]["metadata"]["specialist"], canonical);
        evidence.push(json!({"alias":alias,"got":got,"created":created}));
    }
    write_agent(
        &project.join(".intent/specialists/native-review.md"),
        "Project reviewer",
        "aliases: []\n",
        "Project prompt.",
    );
    let released = wss_rpc(
        &mut client,
        request_id,
        "specialist.get",
        json!({"id":"review-alias","workspacePath":project}),
    )
    .await;
    request_id += 1;
    assert_eq!(released["specialist"]["id"], "review-alias");
    assert_eq!(released["specialist"]["importedFrom"], "claude-code");
    write_agent(
        &project.join(".intent/specialists/review-alias.md"),
        "Explicit native",
        "",
        "Explicit native prompt.",
    );
    let explicit = wss_rpc(
        &mut client,
        request_id,
        "specialist.get",
        json!({"id":"review-alias","workspacePath":project}),
    )
    .await;
    assert_eq!(explicit["specialist"]["id"], "review-alias");
    assert!(explicit["specialist"].get("importedFrom").is_none());
    evidence.push(json!({"releasedAlias":released,"explicitNative":explicit}));
    std::fs::write(
        dir.path().join("claude-native-alias-evidence.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}

#[tokio::test]
async fn project_linked_agent_and_skill_targets_emit_live_updates() {
    let dir = scratch_dir("project-link-targets");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    std::fs::create_dir_all(&home).unwrap();
    let cases = [
        (
            "agent-ancestor",
            true,
            ".claude/agents/ancestor.md",
            ".claude/agent-ancestor.txt",
            "../agent-ancestor.txt",
        ),
        (
            "agent-inside",
            true,
            ".claude/agents/inside.md",
            ".claude/agents/agent-inside.txt",
            "agent-inside.txt",
        ),
        (
            "skill-ancestor",
            false,
            ".claude/skills/ancestor/SKILL.md",
            ".claude/skill-ancestor.txt",
            "../../skill-ancestor.txt",
        ),
        (
            "skill-inside",
            false,
            ".claude/skills/inside/SKILL.md",
            ".claude/skills/skill-inside.txt",
            "../skill-inside.txt",
        ),
    ];
    let write = |path: &Path, name: &str, value: &str| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            format!("---\nname: {name}\ndescription: {value}\n---\n{value}\n"),
        )
        .unwrap();
    };
    for (name, _, alias, target, relative) in cases {
        write(&project.join(target), name, "Initial");
        let alias = project.join(alias);
        std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
        symlink(relative, alias).unwrap();
    }
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg).await;
    let workspace = wss_rpc(
        &mut client,
        1,
        "workspace.create",
        json!({
            "title":"Project linked targets", "skipIsolation":true, "worktreePath":project
        }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].clone();
    stop(daemon, &dir.path().join("intentd.sock")).await;
    let (daemon, port, cfg) = boot(dir.path(), &home).await;
    let mut client = connect_ws(port, cfg.clone()).await;
    let mut subscription = connect_ws(port, cfg).await;
    wss_rpc(
        &mut subscription,
        1,
        "events.subscribe",
        json!({
            "eventTypes":["specialists:changed","skills:changed"], "workspaceId":workspace_id
        }),
    )
    .await;
    let mut ready = None;
    for attempt in 0..20 {
        write(
            &project.join(".intent/specialists/watch-probe.md"),
            "watch-probe",
            &format!("Ready {attempt}"),
        );
        if let Ok(event) = tokio::time::timeout(
            common::test_timeout(Duration::from_millis(750)),
            next_event(&mut subscription, &["specialists:changed"], 20),
        )
        .await
        {
            ready = Some(event);
            break;
        }
    }
    let mut evidence = vec![json!({"ready":ready.expect("project watcher readiness")})];
    let mut request_id = 2;
    for (name, agent, alias, target, _) in cases {
        let alias = project.join(alias);
        let mut target = project.join(target);
        let bridge = project.join(format!(".claude/{name}-bridge.txt"));
        for step in [
            "edit-one",
            "edit-two",
            "replace",
            "delete",
            "recreate",
            "retarget",
            "edit-retargeted",
            "retarget-intermediate",
            "edit-final",
        ] {
            let value = format!("{name} {step}");
            match step {
                "replace" => {
                    let staging = target.with_extension("tmp");
                    write(&staging, name, &value);
                    std::fs::rename(staging, &target).unwrap();
                }
                "delete" => std::fs::remove_file(&target).unwrap(),
                "retarget" => {
                    target = project.join(format!(".claude/{name}-retargeted.txt"));
                    write(&target, name, &value);
                    symlink(&target, &bridge).unwrap();
                    let staging = alias.with_extension("next");
                    symlink(&bridge, &staging).unwrap();
                    std::fs::rename(staging, &alias).unwrap();
                }
                "retarget-intermediate" => {
                    target = project.join(format!(".claude/{name}-final.txt"));
                    write(&target, name, &value);
                    let staging = bridge.with_extension("next");
                    symlink(&target, &staging).unwrap();
                    std::fs::rename(staging, &bridge).unwrap();
                }
                _ => write(&target, name, &value),
            }
            let event_type = if agent {
                "specialists:changed"
            } else {
                "skills:changed"
            };
            let event = next_event(&mut subscription, &[event_type], 20).await;
            assert_eq!(event["workspaceId"], workspace_id, "{name} {step}");
            let (method, params) = if agent {
                (
                    "specialist.list",
                    json!({"workspaceId":workspace_id,"includeProject":true}),
                )
            } else {
                ("skill.list", json!({"workspaceId":workspace_id}))
            };
            let catalog = wss_rpc(&mut client, request_id, method, params).await;
            request_id += 1;
            let rows = if agent {
                &catalog["specialists"]
            } else {
                &catalog
            };
            let row = rows
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row[if agent { "id" } else { "name" }] == name);
            if step == "delete" {
                assert!(row.is_none(), "{name} {step}: {catalog}");
            } else {
                assert_eq!(
                    row.expect("linked definition")[if agent { "prompt" } else { "description" }],
                    value,
                    "{name} {step}"
                );
            }
            evidence.push(
                json!({"name":name,"step":step,"target":target,"event":event,"catalog":catalog}),
            );
        }
    }
    std::fs::write(
        dir.path().join("project-linked-targets-evidence.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    stop(daemon, &dir.path().join("intentd.sock")).await;
}
