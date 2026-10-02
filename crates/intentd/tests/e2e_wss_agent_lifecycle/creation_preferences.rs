//! Manual specialist preferences over authenticated, pinned WSS, including restart.
use super::*;

async fn rpc_envelope<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    loop {
        match timeout(Duration::from_secs(15), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == id {
                    assert_eq!(value["jsonrpc"], "2.0");
                    return value;
                }
            }
            Message::Ping(data) => ws.send(Message::Pong(data)).await.unwrap(),
            _ => {}
        }
    }
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_survive_restart_and_rejected_requests_over_wss() {
    let data = temp_data_dir();
    let first = seed_workspace_only(data.path()).await;
    let second = seed_workspace_only(data.path()).await;
    let env = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let mut daemon = Some(Daemon {
        child: spawn_serve(data.path(), "both", &env),
    });
    let socket = data.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let read = rpc_envelope(
        &mut rpc,
        1,
        "agent.getCreationPreferences",
        json!({"workspaceId":first}),
    )
    .await;
    assert_eq!(read, json!({"jsonrpc":"2.0","id":1,"result":{}}));
    let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":first,"specialistId":"coordinator","rememberSpecialist":true,"provider":"mock","model":"default"})).await;
    let id = created["agent"]["id"].as_str().unwrap();
    assert_eq!(
        wss_rpc(
            &mut rpc,
            3,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":"spec-writer"})
    );
    assert_eq!(
        wss_rpc(
            &mut rpc,
            4,
            "agent.getCreationPreferences",
            json!({"workspaceId":second})
        )
        .await,
        json!({})
    );
    for params in [
        json!({"workspaceId":first,"specialistId":"nonexistent","rememberSpecialist":true}),
        json!({"workspaceId":first,"rememberSpecialist":"yes"}),
    ] {
        let bad = rpc_envelope(&mut rpc, 5, "agent.create", params).await;
        assert_eq!(bad["error"]["code"], -32602);
    }
    let bad = rpc_envelope(&mut rpc, 6, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":"nonexistent","rememberSpecialist":true}})).await;
    assert_eq!(bad["error"]["code"], -32602);
    assert_eq!(
        wss_rpc(
            &mut rpc,
            7,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":"spec-writer"})
    );
    let updated = wss_rpc(&mut rpc, 8, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":"implementor","rememberSpecialist":true}})).await;
    assert_eq!(updated["success"], true);
    assert_eq!(updated["agent"]["name"], "Implementor");
    assert_eq!(updated["agent"]["nameExplicitlySet"], false);
    wss_rpc(
        &mut rpc,
        9,
        "agent.create",
        json!({"workspaceId":second,"rememberSpecialist":true,"provider":"mock","model":"default"}),
    )
    .await;
    wss_rpc(&mut rpc, 10, "agent.create", json!({"workspaceId":first,"rememberSpecialist":true,"isBackground":true,"provider":"mock","model":"default"})).await;
    drop(rpc);
    drop(daemon.take());
    daemon = Some(Daemon {
        child: spawn_serve(data.path(), "both", &env),
    });
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    assert_eq!(
        rpc_envelope(
            &mut rpc,
            11,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"jsonrpc":"2.0","id":11,"result":{"specialistId":"implementor"}})
    );
    assert_eq!(
        wss_rpc(
            &mut rpc,
            12,
            "agent.getCreationPreferences",
            json!({"workspaceId":second})
        )
        .await,
        json!({"specialistId":null})
    );
    wss_rpc(&mut rpc, 13, "agent.update", json!({"agentId":id,"workspaceId":first,"changes":{"specialist":null,"rememberSpecialist":true}})).await;
    assert_eq!(
        wss_rpc(
            &mut rpc,
            14,
            "agent.getCreationPreferences",
            json!({"workspaceId":first})
        )
        .await,
        json!({"specialistId":null})
    );
    drop(daemon);
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_workspace_initial_agent_ui_shape_over_wss() {
    let Some(script) = gate("initial-agent preferences and naming E2E") else {
        return;
    };
    let data = temp_data_dir();
    let prompt_log = data.path().join("initial-agent-prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({"response": "Initial agent response"}).to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log_str.as_str()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(data.path(), "both", &env),
    };
    let socket = data.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let mut sub = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let subscribed = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"eventTypes":["agent:*"]}),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string());
    for (name, explicit, remember, specialist) in [
        (Some("Implementor"), Some(false), true, Some("implementor")),
        (Some("My initial task"), None, true, Some("implementor")),
        (Some("Legacy custom"), None, false, Some("implementor")),
        (Some("Agent"), Some(false), true, None),
        (None, Some(false), true, None),
    ] {
        let label = name.unwrap_or("Nameless General");
        let mut initial = json!({"provider":"mock","model":"default","rememberSpecialist":remember,"prompt":format!("Initial naming case {label}: fix sidebar selection")});
        if let Some(name) = name {
            initial["name"] = json!(name);
        }
        if let Some(explicit) = explicit {
            initial["nameExplicitlySet"] = json!(explicit);
        }
        if let Some(specialist) = specialist {
            initial["specialist"] = json!(specialist);
        }
        let created = rpc_envelope(
            &mut rpc,
            20,
            "workspace.create",
            json!({"title":"Initial preference","initialAgent":initial}),
        )
        .await;
        assert!(created.get("error").is_none(), "{created}");
        let generated_name = created["result"]["initialAgent"]["name"].as_str().unwrap();
        if let Some(name) = name {
            assert_eq!(generated_name, name);
        } else {
            assert!(generated_name.starts_with("Agent "));
        }
        assert_eq!(
            created["result"]["initialAgent"]["nameExplicitlySet"],
            explicit.unwrap_or(true)
        );
        let ws = &created["result"]["workspace"]["id"];
        assert!(ws.is_string(), "{created}");
        let preferences = wss_rpc(
            &mut rpc,
            21,
            "agent.getCreationPreferences",
            json!({"workspaceId":ws}),
        )
        .await;
        assert_eq!(
            preferences,
            if remember {
                json!({"specialistId":specialist})
            } else {
                json!({})
            }
        );
        let agent_id = created["result"]["initialAgent"]["id"].as_str().unwrap();
        for turn in 0..2 {
            let content = if turn == 0 {
                format!("Initial naming case {label}: fix sidebar selection")
            } else {
                format!("Follow-up naming case {label}: check sidebar selection")
            };
            if turn > 0 {
                let sent = wss_rpc(
                    &mut rpc,
                    25,
                    "agent.sendMessage",
                    json!({
                        "workspaceId":ws,"agentId":agent_id,"content":content
                    }),
                )
                .await;
                assert_eq!(sent["success"], true);
            }
            timeout(Duration::from_secs(30), async {
                loop {
                    let frame = wss_event(&mut sub, 30).await;
                    let event = &frame["params"]["event"];
                    if event["type"] == "agent:status-changed"
                        && event["data"]["agentId"] == agent_id
                        && event["data"]["status"] == "idle"
                    {
                        break;
                    }
                }
            })
            .await
            .expect("initial agent turn settled");
            let log = std::fs::read_to_string(&prompt_log).expect("provider prompt log");
            let prompts: Vec<Value> = log
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let text = prompts
                .iter()
                .rev()
                .filter_map(|prompt| prompt["text"].as_str())
                .find(|text| text.contains(&content))
                .expect("initial message reached provider");
            assert_eq!(
                text.contains("This agent still has a generated name"),
                explicit == Some(false) && turn == 0,
                "case {label} turn {turn}: {text}"
            );
            assert!(!text.contains("This workspace needs a title"));
        }
        let got = wss_rpc(&mut rpc, 26, "agent.get", json!({"agentId":agent_id})).await;
        assert_eq!(
            got["agent"]["name"], generated_name,
            "naming hints do not mutate names"
        );
        if name.is_none() {
            let updated = wss_rpc(
                &mut rpc,
                27,
                "agent.update",
                json!({
                    "workspaceId":ws,"agentId":agent_id,
                    "changes":{"specialist":"implementor","rememberSpecialist":true}
                }),
            )
            .await;
            assert_eq!(
                updated["agent"]["name"], "Implementor",
                "welcome selection recognizes the daemon-generated General name"
            );
            assert_eq!(
                wss_rpc(
                    &mut rpc,
                    28,
                    "agent.getCreationPreferences",
                    json!({"workspaceId":ws})
                )
                .await,
                json!({"specialistId":"implementor"})
            );
        }
    }
    let before = wss_rpc(&mut rpc, 22, "workspace.list", json!({})).await;
    for initial in [
        json!({"name":"Invalid specialist","specialist":"missing-specialist","rememberSpecialist":true,"nameExplicitlySet":false}),
        json!({"name":"Bad memory flag","rememberSpecialist":"true"}),
        json!({"name":"Bad name flag","nameExplicitlySet":"false"}),
    ] {
        let failed = rpc_envelope(
            &mut rpc,
            23,
            "workspace.create",
            json!({"title":"Rejected preference","initialAgent":initial}),
        )
        .await;
        assert_eq!(failed["error"]["code"], -32602, "{failed}");
    }
    let after = wss_rpc(&mut rpc, 24, "workspace.list", json!({})).await;
    assert_eq!(
        before["workspaces"].as_array().unwrap().len(),
        after["workspaces"].as_array().unwrap().len(),
        "rejected initial-agent plans must leave no workspace"
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_welcome_instructions_reach_first_turn_over_wss() {
    let Some(script) = gate("initial-agent preferences and naming E2E") else {
        return;
    };
    let data = temp_data_dir();
    let prompt_log = data.path().join("initial-agent-prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({"response": "Initial agent response"}).to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log_str.as_str()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(data.path(), "both", &env),
    };
    let socket = data.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let mut sub = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let subscribed = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"eventTypes":["agent:*"]}),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string());

    for custom in [false, true] {
        let created = wss_rpc(
            &mut rpc,
            40,
            "workspace.create",
            json!({"title":"Welcome instructions"}),
        )
        .await;
        let ws = &created["workspace"]["id"];
        let mut params = json!({"workspaceId":ws,"provider":"mock","model":"default"});
        if custom {
            params["name"] = json!("Custom task name");
            params["metadata"] = json!({"behaviorPrompt":"EXPLICIT_WELCOME_BEHAVIOR_OVERRIDE"});
        }
        let created = wss_rpc(&mut rpc, 41, "agent.create", params).await;
        let id = &created["agent"]["id"];
        let updated = wss_rpc(&mut rpc, 42, "agent.update", json!({"workspaceId":ws,"agentId":id,"changes":{"specialist":"implementor","rememberSpecialist":true}})).await;
        assert_eq!(
            updated["agent"]["name"],
            if custom {
                "Custom task name"
            } else {
                "Implementor"
            }
        );
        let content = format!("Welcome instruction first turn custom={custom}");
        let sent = wss_rpc(
            &mut rpc,
            43,
            "agent.sendMessage",
            json!({"workspaceId":ws,"agentId":id,"content":content}),
        )
        .await;
        assert_eq!(sent["success"], true);
        timeout(Duration::from_secs(30), async {
            loop {
                let frame = wss_event(&mut sub, 30).await;
                let event = &frame["params"]["event"];
                if event["type"] == "agent:status-changed"
                    && event["data"]["agentId"] == *id
                    && event["data"]["status"] == "idle"
                {
                    break;
                }
            }
        })
        .await
        .expect("welcome first turn settled");
        let log = std::fs::read_to_string(&prompt_log).unwrap();
        let prompts: Vec<Value> = log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let text = prompts
            .iter()
            .rev()
            .filter_map(|p| p["text"].as_str())
            .find(|text| text.contains(&content))
            .unwrap();
        assert!(
            text.contains("Stay within task scope. No refactors, no scope creep."),
            "missing specialist reminder: {text}"
        );
        assert_eq!(
            text.contains("Implement your assigned task"),
            !custom,
            "specialist body precedence: {text}"
        );
        assert_eq!(
            text.contains("EXPLICIT_WELCOME_BEHAVIOR_OVERRIDE"),
            custom,
            "explicit body precedence: {text}"
        );
        assert_eq!(
            text.contains("This agent still has a generated name"),
            !custom
        );
    }
}
