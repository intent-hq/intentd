//! Real WSS ingress and production manager delivery to a deterministic Codex ACP child.
use super::*;

type Socket = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

fn write_skill(root: &Path, name: &str, description: &str) -> PathBuf {
    let path = root.join(".agents/skills").join(name).join("SKILL.md");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!("---\nname: {name}\ndescription: {description}\n---\nInstructions.\n"),
    )
    .unwrap();
    path
}

fn log_entries(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn send_turn(
    rpc: &mut Socket,
    sub: &mut Socket,
    workspace: &str,
    agent: &str,
    turn: usize,
    log: &Path,
) -> Value {
    let sent = wss_rpc(
        rpc,
        100 + i64::try_from(turn).unwrap(),
        "agent.sendMessage",
        json!({
            "workspaceId": workspace, "agentId": agent, "content": format!("catalog turn {turn}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "{sent}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut ended = false;
    let mut idle = false;
    while !(ended && idle) {
        let frame = wss_event_opt_until(sub, deadline)
            .await
            .expect("catalog turn finished");
        let event = &frame["params"]["event"];
        if event["data"]["agentId"] != agent {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("catalog turn failed: {event}"),
            Some("agent:stream:end") => ended = true,
            Some("agent:status-changed") if event["data"]["status"] == "idle" => idle = true,
            _ => {}
        }
    }
    let entries = log_entries(log);
    assert_eq!(entries.len(), turn + 1, "one ACP prompt per normal turn");
    let entry = entries.last().unwrap().clone();
    assert!(entry["text"]
        .as_str()
        .unwrap()
        .ends_with(&format!("catalog turn {turn}")));
    entry
}

async fn assert_list(rpc: &mut Socket, workspace: &str, expected: &[(&str, &str, &Path, &str)]) {
    let envelope = wss_rpc_envelope(rpc, 30, "skill.list", json!({"workspaceId":workspace})).await;
    assert_eq!(envelope["id"], 30);
    assert_eq!(envelope["jsonrpc"], "2.0");
    assert!(envelope.get("error").is_none(), "{envelope}");
    let skills = envelope["result"]
        .as_array()
        .expect("skill.list result array");
    assert_eq!(skills.len(), expected.len(), "{skills:?}");
    for (name, description, path, scope) in expected {
        let skill = skills
            .iter()
            .find(|skill| skill["name"] == *name)
            .expect("expected skill");
        assert_eq!(skill["description"], *description);
        assert_eq!(skill["location"], path.to_str().unwrap());
        assert_eq!(skill["scope"], *scope);
        assert!(
            std::fs::read_to_string(skill["location"].as_str().unwrap()).is_ok(),
            "original path remains readable"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn codex_personal_catalog_assistant_fresh_resume_and_refresh_over_wss() {
    catalog_lifecycle(false).await;
}

#[intent_test_macros::daemon_test]
async fn codex_personal_and_project_catalog_fresh_resume_and_refresh_over_wss() {
    catalog_lifecycle(true).await;
}

async fn catalog_lifecycle(with_repository: bool) {
    let Some(script) = gate("WSS Codex personal skill catalog") else {
        return;
    };
    let dir = temp_data_dir();
    let data = dir.path();
    let home = data.join("personal-home");
    let first = write_skill(&home, "find-skills", "Find personal skills");
    let second = write_skill(&home, "ios-device-build", "Build on an iOS device");
    let project = data.join("project");
    let project_skill =
        with_repository.then(|| write_skill(&project, "project-review", "Review this repository"));
    let workspace = if with_repository {
        let id = seed_workspace_only(data).await;
        let store = intent_store::Store::open(&data.join("intentd.db"))
            .await
            .unwrap();
        let mut ws = store
            .get_workspace(&intent_core::WorkspaceId::from(id.as_str()))
            .await
            .unwrap();
        ws.path = Some(project.to_string_lossy().into_owned());
        ws.worktree_path = ws.path.clone();
        store.update_workspace(&ws).await.unwrap();
        store.close().await;
        id
    } else {
        intent_core::CHIEF_WORKSPACE_ID.to_string()
    };
    let toolchain = common::codex_npx::install(data, &script);
    let prompt_log = data.join("catalog-prompts.jsonl");
    let session_log = data.join("catalog-sessions.jsonl");
    let behavior =
        json!({"response":"catalog turn complete", "advertiseLoadSession":true}).to_string();
    let mut env: Vec<(&str, &str)> = toolchain
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    env.extend([
        ("HOME", home.to_str().unwrap()),
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log.to_str().unwrap()),
        ("MOCK_AGENT_SESSION_LOG", session_log.to_str().unwrap()),
    ]);
    let mut agent = String::new();
    let mut first_session = Value::Null;
    for phase in 0..3 {
        if phase == 1 {
            write_skill(&home, "find-skills", "Changed while daemon stopped");
        }
        let daemon = Daemon {
            child: spawn_serve(data, "both", &env),
        };
        let socket = data.join("intentd.sock");
        assert!(await_uds(&socket).await);
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        let mut rpc = connect_ws(port, cfg.clone()).await;
        let mut sub = connect_ws(port, cfg).await;
        let subscribed = wss_rpc(
            &mut sub,
            1,
            "events.subscribe",
            json!({"eventTypes":["agent:*"],"workspaceId":workspace}),
        )
        .await;
        assert!(subscribed["subscriptionId"].is_string());
        if phase == 0 {
            let mut expected = vec![
                (
                    "find-skills",
                    "Find personal skills",
                    first.as_path(),
                    "user",
                ),
                (
                    "ios-device-build",
                    "Build on an iOS device",
                    second.as_path(),
                    "user",
                ),
            ];
            if let Some(path) = project_skill.as_deref() {
                expected.push(("project-review", "Review this repository", path, "project"));
            }
            assert_list(&mut rpc, &workspace, &expected).await;
            let created = wss_rpc(
                &mut rpc,
                2,
                "agent.create",
                json!({"workspaceId":workspace,"name":"Skill catalog delivery","provider":"codex"}),
            )
            .await;
            agent = created["agent"]["id"]
                .as_str()
                .expect("created agent")
                .to_string();
        }
        let turns = match phase {
            0 => 0..3,
            1 => 3..9,
            _ => 9..10,
        };
        for turn in turns {
            match turn {
                2 => {
                    write_skill(&home, "find-skills", "Changed during active conversation");
                }
                5 => {
                    std::fs::remove_file(&first).unwrap();
                    std::fs::remove_file(&second).unwrap();
                    if let Some(path) = &project_skill {
                        std::fs::remove_file(path).unwrap();
                    }
                    assert_list(&mut rpc, &workspace, &[]).await;
                }
                7 => {
                    write_skill(&home, "find-skills", "Restored without restarting");
                }
                _ => {}
            }
            let entry = send_turn(&mut rpc, &mut sub, &workspace, &agent, turn, &prompt_log).await;
            let text = entry["text"].as_str().unwrap();
            assert_eq!(entry["loaded"], phase > 0, "{entry}");
            if turn == 0 {
                first_session = entry["sessionId"].clone();
                assert_eq!(text.matches("<available_skills>").count(), 1);
                assert!(text.contains("<name>find-skills</name>"));
                assert!(text.contains("<description>Find personal skills</description>"));
                assert!(text.contains(first.to_str().unwrap()));
                assert!(text.contains("<name>ios-device-build</name>"));
                assert!(text.contains(second.to_str().unwrap()));
                assert!(!text.contains("Intent skill catalog update:"));
            } else {
                assert_eq!(
                    entry["sessionId"], first_session,
                    "retained ACP conversation"
                );
                assert!(
                    !text.contains("<supervisor>"),
                    "no replay of retained history: {text}"
                );
                if matches!(turn, 2 | 3 | 5 | 7) {
                    assert!(
                        text.starts_with("<system>\nIntent skill catalog update:"),
                        "only catalog instructions are refreshed: {text}"
                    );
                    assert_eq!(text.matches("<available_skills>").count(), 1);
                    assert!(text.contains("Keep provider-bundled system skills"));
                    match turn {
                        2 => assert!(text.contains("Changed during active conversation")),
                        3 => {
                            assert!(text.contains("Changed while daemon stopped"));
                            assert!(!text.contains("Changed during active conversation"));
                        }
                        5 => {
                            assert!(text.contains("<available_skills>\n</available_skills>"));
                            assert!(!text.contains("<skill>"));
                        }
                        7 => {
                            assert!(text.contains("Restored without restarting"));
                            assert!(text.contains(first.to_str().unwrap()));
                            assert!(!text.contains("ios-device-build"));
                        }
                        _ => unreachable!(),
                    }
                } else {
                    assert!(
                        !text.contains("<available_skills>"),
                        "unchanged catalog must not repeat: {text}"
                    );
                }
            }
            if matches!(turn, 0 | 2 | 3) {
                assert_eq!(
                    text.contains("<name>project-review</name>"),
                    with_repository
                );
                if let Some(path) = &project_skill {
                    assert!(text.contains(path.to_str().unwrap()));
                }
            }
        }
        let sessions = log_entries(&session_log);
        assert_eq!(
            sessions.len(),
            phase + 1,
            "one establishment per daemon lifetime"
        );
        assert_eq!(
            sessions[phase]["method"],
            if phase == 0 {
                "session/new"
            } else {
                "session/load"
            }
        );
        if phase == 2 {
            let conversation = wss_rpc(
                &mut rpc,
                40,
                "agent.getConversation",
                json!({"workspaceId":workspace,"agentId":agent}),
            )
            .await;
            let messages = conversation["messages"]
                .as_array()
                .expect("conversation messages");
            for turn in 0..10 {
                assert!(
                    messages.iter().any(|message| message["role"] == "user"
                        && message
                            .to_string()
                            .contains(&format!("catalog turn {turn}"))),
                    "retained user history: {conversation}"
                );
            }
            if !with_repository {
                let ws = wss_rpc(
                    &mut rpc,
                    41,
                    "workspace.get",
                    json!({"workspaceId":workspace}),
                )
                .await;
                assert!(
                    ws["workspace"]["worktreePath"].is_null(),
                    "Assistant needs no checkout: {ws}"
                );
                assert!(ws["workspace"]["repositoryPath"].is_null());
                assert_eq!(
                    std::fs::read_dir(data.join("workspaces")).unwrap().count(),
                    0
                );
            }
        }
        drop(rpc);
        drop(sub);
        drop(daemon);
    }
}
