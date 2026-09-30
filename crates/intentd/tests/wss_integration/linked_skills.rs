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
            1 => write_skill(
                &target.join("one/SKILL.md"),
                "watched-linked",
                "external metadata edit",
            ),
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
            1..=3 => "external metadata edit",
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
