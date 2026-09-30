//! Linked skills through the real authenticated, fingerprint-pinned WSS transport.
use super::*;
use serde_json::json;
use std::os::unix::fs::symlink;
use std::path::Path;

fn write_skill(path: &Path, name: &str, description: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        format!("---\nname: {name}\ndescription: {description}\n---\nBody\n"),
    )
    .unwrap();
}

async fn list(srv: &Server, id: &WorkspaceId) -> Value {
    let reply = wss_call(
        srv.port,
        srv.cfg.clone(),
        &json!({
            "jsonrpc":"2.0", "id":71, "method":"skill.list", "params":{"workspaceId":id}
        })
        .to_string(),
    )
    .await;
    assert_eq!(reply["jsonrpc"], "2.0");
    assert_eq!(reply["id"], 71);
    assert!(reply.get("error").is_none(), "{reply}");
    assert!(reply["result"].is_array(), "{reply}");
    reply["result"].clone()
}

fn named<'a>(skills: &'a Value, name: &str) -> &'a Value {
    let matches: Vec<_> = skills
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["name"] == name)
        .collect();
    assert_eq!(matches.len(), 1, "{skills}");
    matches[0]
}

#[intent_test_macros::daemon_test]
async fn wss_linked_skills_external_edits_additions_deletions_and_root_retarget_emit() {
    use intent_services::events::{GitStatusRefresher, WatcherRegistry};
    let srv = start(WsOptions::default()).await;
    let project = srv.dir.path().join("watched-project");
    let root = project.join(".claude/skills");
    let target = srv.dir.path().join("watched-target");
    std::fs::create_dir_all(root.parent().unwrap()).unwrap();
    write_skill(&target.join("one/SKILL.md"), "watched-linked", "initial");
    symlink(&target, &root).unwrap();
    let id = WorkspaceId::new();
    let mut row = fixture_workspace(&id);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    ws.send(Message::text(json!({"jsonrpc":"2.0","id":72,"method":"events.subscribe","params":{"eventTypes":["skills:changed"],"workspaceId":id}}).to_string())).await.unwrap();
    let sub = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let reply: Value = serde_json::from_str(&text).unwrap();
                    if reply["id"] == 72 {
                        break reply;
                    }
                }
                Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("subscription stream closed: {other:?}"),
            }
        }
    })
    .await
    .expect("subscription reply");
    assert!(sub["result"]["subscriptionId"].is_string(), "{sub}");
    let cache = Services::new(srv.store.clone()).git_status_cache();
    let refresher = Arc::new(GitStatusRefresher::start(
        srv.bus.clone(),
        srv.api.clone(),
        cache,
    ));
    let registry = WatcherRegistry::start(srv.bus.clone(), srv.api.clone(), refresher).await;
    let mut evidence = Vec::new();
    // Each step starts only after the preceding observable event and list reply.
    for step in 0..8 {
        match step {
            0 => {}
            1 => {
                let file = target.join("one/SKILL.md");
                let before = std::fs::metadata(&file).unwrap();
                write_skill(&file, "watched-linked", "changed");
                std::fs::File::options()
                    .write(true)
                    .open(&file)
                    .unwrap()
                    .set_times(std::fs::FileTimes::new().set_modified(before.modified().unwrap()))
                    .unwrap();
                let after = std::fs::metadata(&file).unwrap();
                assert_eq!(after.len(), before.len());
                assert_eq!(after.modified().unwrap(), before.modified().unwrap());
            }
            2 => write_skill(&target.join("two/SKILL.md"), "watched-added", "addition"),
            3 => {
                std::fs::remove_file(target.join("two/SKILL.md")).unwrap();
            }
            4 => {
                let replacement = srv.dir.path().join("watched-replacement");
                write_skill(
                    &replacement.join("one/SKILL.md"),
                    "watched-linked",
                    "new root",
                );
                std::fs::remove_file(&root).unwrap();
                symlink(&replacement, &root).unwrap();
            }
            5 => {
                let definition = srv.dir.path().join("external-definition.txt");
                write_skill(&definition, "watched-file", "linked file");
                std::fs::create_dir_all(root.join("file")).unwrap();
                symlink(&definition, root.join("file/SKILL.md")).unwrap();
            }
            6 => write_skill(
                &srv.dir.path().join("external-definition.txt"),
                "watched-file",
                "edited arbitrary target filename",
            ),
            _ => {
                std::fs::remove_file(srv.dir.path().join("external-definition.txt")).unwrap();
            }
        }
        let event = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if value["method"] == "events.event"
                            && value["params"]["event"]["type"] == "skills:changed"
                        {
                            return value;
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        ws.send(Message::Pong(p)).await.unwrap();
                    }
                    Some(Ok(_)) => {}
                    other => panic!("event stream closed: {other:?}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no skills:changed event at step {step}"));
        assert_eq!(event["jsonrpc"], "2.0");
        assert_eq!(event["params"]["event"]["workspaceId"], id.as_str());
        assert_eq!(event["params"]["event"]["data"]["workspaceId"], id.as_str());
        let skills = list(&srv, &id).await;
        let description = match step {
            0 => "initial",
            1..=3 => "changed",
            _ => "new root",
        };
        assert_eq!(
            named(&skills, "watched-linked")["description"],
            description,
            "step {step}"
        );
        assert_eq!(
            skills
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["name"] == "watched-added"),
            step == 2
        );
        if step == 5 || step == 6 {
            let description = if step == 5 {
                "linked file"
            } else {
                "edited arbitrary target filename"
            };
            assert_eq!(named(&skills, "watched-file")["description"], description);
        } else {
            assert!(!skills
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["name"] == "watched-file"));
        }
        evidence.push(json!({"step":step,"event":event,"skills":skills}));
    }
    std::fs::write(
        srv.dir.path().join("linked-skills-events.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    eprintln!(
        "linked skills event evidence: {}",
        srv.dir.path().join("linked-skills-events.json").display()
    );
    drop(registry);
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn wss_linked_skills_roots_children_files_cycles_precedence_and_retarget() {
    let srv = start(WsOptions::default()).await;
    let project = srv.dir.path().join("linked-project");
    let external = srv.dir.path().join("external");
    let root = project.join(".claude/skills");
    let high = project.join(".intent/skills");
    std::fs::create_dir_all(root.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&external).unwrap();
    symlink(&external, &root).unwrap();
    let target = srv.dir.path().join("target");
    write_skill(&target.join("SKILL.md"), "linked-fixture", "first");
    symlink(&target, external.join("a-link")).unwrap();
    symlink(&target, external.join("b-alias")).unwrap();
    symlink(&external, external.join("cycle")).unwrap();
    symlink(external.join("missing"), external.join("broken")).unwrap();
    let file = srv.dir.path().join("definition.md");
    write_skill(&file, "linked-file", "file target");
    std::fs::create_dir_all(external.join("file")).unwrap();
    symlink(&file, external.join("file/SKILL.md")).unwrap();
    // The same physical target in a higher tier must retain project precedence
    // and display the higher tier's useful alias, without duplicate discovery.
    std::fs::create_dir_all(&high).unwrap();
    symlink(&target, high.join("preferred")).unwrap();
    write_skill(
        &external.join("deep/a/b/c/d/SKILL.md"),
        "too-deep",
        "beyond existing depth limit",
    );
    let id = WorkspaceId::new();
    let mut row = fixture_workspace(&id);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let first = list(&srv, &id).await;
    assert!(!first
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["name"] == "too-deep"));
    assert_eq!(
        named(&first, "linked-fixture")["location"],
        high.join("preferred/SKILL.md").to_string_lossy().as_ref()
    );
    assert_eq!(named(&first, "linked-file")["description"], "file target");
    assert_eq!(named(&first, "linked-file")["scope"], "project");
    write_skill(&file, "linked-file", "updated target description");
    assert_eq!(
        named(&list(&srv, &id).await, "linked-file")["description"],
        "updated target description"
    );
    let replacement = srv.dir.path().join("replacement");
    write_skill(
        &replacement.join("SKILL.md"),
        "linked-fixture",
        "retargeted",
    );
    std::fs::remove_file(high.join("preferred")).unwrap();
    symlink(&replacement, high.join("preferred")).unwrap();
    let after = list(&srv, &id).await;
    assert_eq!(named(&after, "linked-fixture")["description"], "retargeted");
    std::fs::remove_file(&file).unwrap();
    assert!(!list(&srv, &id)
        .await
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["name"] == "linked-file"));
    write_skill(&file, "linked-file", "repaired");
    let repaired = list(&srv, &id).await;
    assert_eq!(named(&repaired, "linked-file")["description"], "repaired");
    std::fs::write(
        srv.dir.path().join("linked-skills-evidence.json"),
        serde_json::to_vec_pretty(&json!({"before":first,"retargeted":after,"repaired":repaired}))
            .unwrap(),
    )
    .unwrap();
    eprintln!(
        "linked skills discovery evidence: {}",
        srv.dir.path().join("linked-skills-evidence.json").display()
    );
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn wss_linked_skills_companion_base_and_shallower_alias() {
    let srv = start(WsOptions::default()).await;
    let project = srv.dir.path().join("resource-project");
    let kit = project.join("shared-kit");
    let alias = project.join(".claude/skills/file-only/SKILL.md");
    write_skill(
        &kit.join("guide.md"),
        "resource-fixture",
        "shared resources",
    );
    std::fs::create_dir_all(kit.join("scripts")).unwrap();
    std::fs::write(kit.join("scripts/check.sh"), "echo companion-found\n").unwrap();
    std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
    symlink(kit.join("guide.md"), &alias).unwrap();
    write_skill(
        &project.join(".intent/skills/plain/SKILL.md"),
        "plain-fixture",
        "ordinary",
    );
    let tree = project.join("tree");
    write_skill(
        &tree.join("child/SKILL.md"),
        "depth-fixture",
        "reachable via shallow alias",
    );
    let deep = project.join(".intent/skills/a/b/c/linked");
    std::fs::create_dir_all(deep.parent().unwrap()).unwrap();
    symlink(&tree, &deep).unwrap();
    let shallow = project.join(".claude/skills/shallow");
    symlink(&tree, &shallow).unwrap();
    symlink(&tree, tree.join("loop")).unwrap();
    let id = WorkspaceId::new();
    let mut row = fixture_workspace(&id);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let skills = list(&srv, &id).await;
    let linked = named(&skills, "resource-fixture");
    assert_eq!(linked["location"], alias.to_string_lossy().as_ref());
    assert_eq!(
        linked["resourceDirectory"],
        kit.canonicalize().unwrap().to_string_lossy().as_ref()
    );
    assert!(named(&skills, "plain-fixture")
        .get("resourceDirectory")
        .is_none());
    assert_eq!(
        named(&skills, "depth-fixture")["location"],
        shallow.join("child/SKILL.md").to_string_lossy().as_ref()
    );
    let resource =
        Path::new(linked["resourceDirectory"].as_str().unwrap()).join("scripts/check.sh");
    let read = wss_call(srv.port, srv.cfg.clone(), &json!({"jsonrpc":"2.0","id":73,"method":"file.read","params":{"workspaceId":id,"path":resource}}).to_string()).await;
    assert_eq!(read["jsonrpc"], "2.0");
    assert_eq!(read["id"], 73);
    assert_eq!(read["result"], "echo companion-found\n", "{read}");
    let before = std::fs::metadata(kit.join("guide.md")).unwrap();
    write_skill(
        &kit.join("guide.md"),
        "resource-fixture",
        "edited resources",
    );
    std::fs::File::options()
        .write(true)
        .open(kit.join("guide.md"))
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(before.modified().unwrap()))
        .unwrap();
    assert_eq!(
        std::fs::metadata(kit.join("guide.md")).unwrap().len(),
        before.len()
    );
    let updated = list(&srv, &id).await;
    assert_eq!(
        named(&updated, "resource-fixture")["description"],
        "edited resources"
    );
    let evidence = srv.dir.path().join("linked-skills-resources.json");
    std::fs::write(
        &evidence,
        serde_json::to_vec_pretty(&json!({"skills":skills,"companionRead":read,"updated":updated}))
            .unwrap(),
    )
    .unwrap();
    eprintln!("linked skills resource evidence: {}", evidence.display());
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn wss_linked_skills_byte_budget_counts_invalid_utf8_and_recovers() {
    let srv = start(WsOptions::default()).await;
    let project = srv.dir.path().join("bounded-project");
    let root = project.join(".claude/skills");
    let target = srv.dir.path().join("bounded-target");
    let payloads = target.join("payloads");
    std::fs::create_dir_all(root.parent().unwrap()).unwrap();
    symlink(&target, &root).unwrap();
    let oversized = target.join("oversized/SKILL.md");
    write_skill(&oversized, "too-large", "Exceeds the per-file limit");
    std::fs::File::options()
        .write(true)
        .open(&oversized)
        .unwrap()
        .set_len(1_048_577)
        .unwrap();
    let invalid_bytes = vec![0xff; 1_048_576];
    for index in 0..32 {
        let file = payloads.join(format!("{index:02}/SKILL.md"));
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, &invalid_bytes).unwrap();
    }
    write_skill(
        &target.join("z-valid/SKILL.md"),
        "after-budget",
        "Available after repair",
    );
    let id = WorkspaceId::new();
    let mut row = fixture_workspace(&id);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&row).await.unwrap();
    let exhausted = list(&srv, &id).await;
    assert!(
        !exhausted
            .as_array()
            .unwrap()
            .iter()
            .any(|s| matches!(s["name"].as_str(), Some("after-budget" | "too-large"))),
        "{exhausted}"
    );
    std::fs::remove_dir_all(&payloads).unwrap();
    let restored = list(&srv, &id).await;
    assert_eq!(
        named(&restored, "after-budget")["description"],
        "Available after repair"
    );
    assert!(!restored
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["name"] == "too-large"));
    let evidence = srv.dir.path().join("linked-skills-limits.json");
    std::fs::write(
        &evidence,
        serde_json::to_vec_pretty(&json!({"exhausted":exhausted,"restored":restored})).unwrap(),
    )
    .unwrap();
    eprintln!("linked skills byte-limit evidence: {}", evidence.display());
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn wss_linked_skills_custom_config_directory_and_empty_fallback() {
    use std::process::Stdio;
    let temp = common::test_tempdir("linked-skills-config-");
    let mut evidence = Vec::new();
    for mode in ["linked", "missing", "empty"] {
        let data = temp.path().join(mode);
        let home = data.join("home");
        let config = data.join("config");
        let target = data.join("config-target");
        let project = data.join("project");
        std::fs::create_dir_all(&project).unwrap();
        write_skill(
            &home.join(".claude/skills/default/SKILL.md"),
            "default-config-fixture",
            "default root",
        );
        write_skill(
            &project.join(".claude/skills/project/SKILL.md"),
            "project-config-fixture",
            "project unchanged",
        );
        if mode == "linked" {
            write_skill(
                &target.join("skills/custom/SKILL.md"),
                "custom-config-fixture",
                "custom root",
            );
            symlink(&target, &config).unwrap();
        }
        common::enable_ws_api(&data);
        let log_path = data.join("daemon.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut command = common::serve_command();
        common::hermetic_github_identity(&mut command, &data);
        command
            .env("HOME", &home)
            .env(
                "CLAUDE_CONFIG_DIR",
                if mode == "empty" {
                    Path::new("")
                } else {
                    &config
                },
            )
            .env("INTENTD_DATA_DIR", &data)
            .env("INTENTD_WORKSPACES_DIR", data.join("workspaces"))
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .env("INTENTD_SECRETS_FILE", data.join("secrets.json"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        std::fs::create_dir_all(data.join("workspaces")).unwrap();
        let mut daemon = common::DaemonGuard::process_only(command.spawn().unwrap());
        let socket = data.join("intentd.sock");
        common::await_daemon_listening(daemon.child_mut(), &socket, &log_path).await;
        let status = common::await_wss_status_logged(&socket, &log_path).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        let created = wss_call(port, cfg.clone(), &json!({"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Config skills","skipIsolation":true,"worktreePath":project}}).to_string()).await;
        assert_eq!(
            created["result"]["workspace"]["worktreePath"],
            project.to_string_lossy().as_ref()
        );
        let id = created["result"]["workspace"]["id"].as_str().unwrap();
        let listed = wss_call(
            port,
            cfg.clone(),
            &json!({"jsonrpc":"2.0","id":2,"method":"skill.list","params":{"workspaceId":id}})
                .to_string(),
        )
        .await;
        assert_eq!(listed["jsonrpc"], "2.0");
        assert_eq!(listed["id"], 2);
        let skills = listed["result"].as_array().unwrap();
        assert_eq!(
            skills.iter().any(|s| s["name"] == "default-config-fixture"),
            mode == "empty"
        );
        assert_eq!(
            skills.iter().any(|s| s["name"] == "custom-config-fixture"),
            mode == "linked"
        );
        assert_eq!(
            named(&listed["result"], "project-config-fixture")["scope"],
            "project"
        );
        let mut changed = Value::Null;
        if mode != "empty" {
            let mut events = connect_ws(port, cfg.clone()).await;
            events.send(Message::text(json!({"jsonrpc":"2.0","id":3,"method":"events.subscribe","params":{"workspaceId":id,"eventTypes":["skills:changed"]}}).to_string())).await.unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match events.next().await {
                        Some(Ok(Message::Text(text))) => {
                            let reply: Value = serde_json::from_str(&text).unwrap();
                            if reply["id"] == 3 {
                                assert!(reply["result"]["subscriptionId"].is_string(), "{reply}");
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(p))) => events.send(Message::Pong(p)).await.unwrap(),
                        Some(Ok(_)) => {}
                        other => panic!("config subscription closed: {other:?}"),
                    }
                }
            })
            .await
            .expect("config skills subscription");
            write_skill(
                &config.join("skills/custom/SKILL.md"),
                "custom-config-fixture",
                "live update",
            );
            changed = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    match events.next().await {
                        Some(Ok(Message::Text(text))) => {
                            let event: Value = serde_json::from_str(&text).unwrap();
                            if event["method"] == "events.event" && event["params"]["event"]["type"] == "skills:changed" {
                                let reply = wss_call(port, cfg.clone(), &json!({"jsonrpc":"2.0","id":4,"method":"skill.list","params":{"workspaceId":id}}).to_string()).await;
                                if reply["result"].as_array().unwrap().iter().any(|s| s["name"] == "custom-config-fixture" && s["description"] == "live update") {
                                    break json!({"event":event,"listed":reply});
                                }
                            }
                        }
                        Some(Ok(Message::Ping(p))) => events.send(Message::Pong(p)).await.unwrap(),
                        Some(Ok(_)) => {},
                        other => panic!("config event stream closed: {other:?}"),
                    }
                }
            }).await.expect("live custom config skills update");
        }
        evidence.push(json!({"mode":mode,"listed":listed,"changed":changed}));
        drop(daemon);
    }
    let artifact = temp.path().join("config-directory-evidence.json");
    std::fs::write(&artifact, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    eprintln!(
        "linked skills configuration evidence: {}",
        artifact.display()
    );
}
