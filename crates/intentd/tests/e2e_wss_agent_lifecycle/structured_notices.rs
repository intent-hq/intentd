use super::*;

const WARNING: &str = "State database unavailable; continuing without stored state.";
const DETAIL: &str = "Structured notice detail only";
const REPLY: &str = "Warning: State database unavailable; continuing without stored state.\n\n";

#[intent_test_macros::daemon_test]
async fn structured_notices_preserve_genuine_assistant_text_over_wss() {
    exercise_notices(false).await;
}

#[intent_test_macros::daemon_test]
async fn structured_notices_preserve_fatal_prompt_failure_over_wss() {
    exercise_notices(true).await;
}

async fn exercise_notices(fail: bool) {
    let Some(script) = gate("WSS structured notices") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(data_dir.path()).await;
    let mut behavior = json!({
        "notices": [
            {"severity": "warning", "title": WARNING, "description": DETAIL},
            {"severity": "info", "title": "Provider information"},
            {"severity": "error", "title": "Advisory error, not a failed turn"}
        ],
        "response": REPLY
    });
    if fail {
        behavior["promptRpcError"] = json!({"code": -32603, "message": "Fatal provider failure"});
    }
    let behavior = behavior.to_string();
    let _daemon = Daemon {
        child: spawn_serve(
            data_dir.path(),
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
            ],
        ),
    };
    let socket = data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "workspaceId": ws_id, "eventTypes": ["agent:*", "chat:stream:delta"]
        }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Notice test", "model": "default", "provider": "mock"
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap();
    let sent = wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id, "agentId": agent_id, "content": "Reply verbatim"
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    let events = timeout(Duration::from_secs(40), async {
        let mut events = Vec::new();
        loop {
            let frame = wss_event(&mut sub, 40).await;
            assert_eq!(frame["jsonrpc"], "2.0");
            let event = frame["params"]["event"].clone();
            if event["data"]["agentId"] != agent_id {
                continue;
            }
            let terminal = if fail { "agent:failed" } else { "agent:idle" };
            let done = event["type"] == terminal;
            if !fail {
                assert_ne!(
                    event["type"], "agent:failed",
                    "advisory error cannot fail a turn: {event}"
                );
            }
            events.push(event);
            if done {
                return events;
            }
        }
    })
    .await
    .expect("turn terminated");
    let deltas: String = events
        .iter()
        .filter(|e| e["type"] == "chat:stream:delta")
        .filter_map(|e| e["data"]["content"].as_str())
        .collect();
    assert_eq!(
        deltas,
        if fail { "" } else { REPLY },
        "only genuine assistant text streams"
    );
    assert!(!serde_json::to_string(&events).unwrap().contains(DETAIL));
    assert!(events.iter().any(|e| e["type"] == "agent:stream:end"));
    if fail {
        assert!(events.last().unwrap()["data"]["error"]
            .as_str()
            .unwrap()
            .contains("Fatal provider failure"));
    }
    let convo = wss_rpc(
        &mut rpc,
        4,
        "agent.getConversation",
        json!({
            "workspaceId": ws_id, "agentId": agent_id
        }),
    )
    .await;
    let assistant_text: String = convo["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .flat_map(|m| m["contentBlocks"].as_array().unwrap())
        .filter_map(|b| b["text"].as_str())
        .collect();
    assert_eq!(assistant_text, if fail { "" } else { REPLY });
    let got = wss_rpc(
        &mut rpc,
        5,
        "agent.get",
        json!({
            "workspaceId": ws_id, "agentId": agent_id
        }),
    )
    .await;
    let preview = got["agent"]["lastAgentResponse"].as_str().unwrap_or("");
    assert_eq!(preview, if fail { "" } else { REPLY.trim() });
    if fail {
        assert_eq!(got["agent"]["status"], "error");
    }

    // Diagnostics retain severity, detail, and both daemon/ACP attribution.
    let log = std::fs::read_to_string(data_dir.path().join("daemon.log")).unwrap();
    let line = log
        .lines()
        .find(|line| line.contains(DETAIL))
        .expect("notice diagnostic logged");
    for expected in [
        "WARN",
        "warning",
        WARNING,
        agent_id,
        "mock-session-1",
        &ws_id,
    ] {
        assert!(line.contains(expected), "missing {expected:?} in {line}");
    }
}
