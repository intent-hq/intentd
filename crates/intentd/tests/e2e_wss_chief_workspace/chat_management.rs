//! Assistant chat management through the real ACP → MCP bridge, with state
//! and protocol events observed over authenticated, pinned-TLS connections.

use super::{
    await_uds, client_config, common, connect_ws, disable_toon_output, gate, poll_conversation,
    seed_target, spawn_serve, temp_data_dir, tool_result_jsons, wss_event, wss_rpc_envelope,
    Daemon, CHIEF_WORKSPACE_ID, TOKEN,
};
use serde_json::{json, Value};
use std::path::Path;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn start_bridge(data_dir: &Path, script: &str, code: &str) -> (Daemon, Ws, Ws) {
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "CHAT_MANAGEMENT",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "Exercise Assistant chat management" },
            },
            "response": "Assistant chat management checked",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
    let daemon = Daemon {
        child: spawn_serve(
            data_dir,
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("MOCK_AGENT_SCRIPT_PATH", script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
            ],
        ),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("WSS port")).expect("port fits u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    let ws = connect_ws(port, cfg.clone()).await;
    let events = connect_ws(port, cfg).await;
    (daemon, ws, events)
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let response = wss_rpc_envelope(ws, id, method, params).await;
    assert_eq!(response["jsonrpc"], "2.0", "{response}");
    assert_eq!(response["id"], id, "{response}");
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

async fn create_chat(ws: &mut Ws, id: i64, workspace_id: &str, name: &str) -> String {
    let created = rpc(
        ws,
        id,
        "agent.create",
        json!({
            "workspaceId": workspace_id, "name": name,
            "model": "default", "provider": "mock",
        }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

async fn run_bridge(ws: &mut Ws, workspace_id: &str, agent_id: &str) -> Value {
    let sent = rpc(
        ws,
        50,
        "agent.sendMessage",
        json!({
            "workspaceId": workspace_id, "agentId": agent_id,
            "content": "CHAT_MANAGEMENT",
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    // Conversation reads are slim by default. Hydrate the persisted result
    // instead of trying to parse a truncated tool-output preview as JSON.
    let (message_id, block_id) = poll_conversation(
        ws,
        100,
        agent_id,
        "Assistant chat management tool result",
        |messages| {
            for message in messages.as_array()? {
                for block in message["contentBlocks"].as_array()? {
                    if block["type"] == "tool_result" {
                        return Some((
                            message["id"].as_str()?.to_string(),
                            block["id"].as_str()?.to_string(),
                        ));
                    }
                }
            }
            None
        },
    )
    .await;
    let full = rpc(
        ws,
        200,
        "agent.getMessageBlock",
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": block_id }),
    )
    .await;
    tool_result_jsons(&json!([{ "contentBlocks": [full["block"]] }]))
        .into_iter()
        .find(|result| result["chatManagement"] == true)
        .unwrap_or_else(|| panic!("chat management failed: {full}"))
}

/// §6.3 notification envelope; §6.5 payloads are asserted by each caller.
async fn next_chat_event(events: &mut Ws, subscription: &Value, target: &str) -> Value {
    loop {
        let frame = wss_event(events, 10).await;
        assert_eq!(frame["jsonrpc"], "2.0", "{frame}");
        assert_eq!(frame["method"], "events.event", "{frame}");
        assert!(frame.get("id").is_none(), "notification has an id: {frame}");
        assert_eq!(
            frame["params"]["subscriptionId"],
            subscription["subscriptionId"]
        );
        let event = frame["params"]["event"].clone();
        assert_eq!(event["workspaceId"], CHIEF_WORKSPACE_ID, "{event}");
        assert!(event["id"].is_string(), "{event}");
        assert!(event["timestamp"].is_string(), "{event}");
        assert!(event["actor"].is_object(), "{event}");
        if event["data"]["agentId"] == target {
            return event;
        }
    }
}

#[tokio::test]
async fn assistant_chat_management_roundtrip_over_wss() {
    let Some(script) = gate("WSS Assistant chat management") else {
        return;
    };
    let data_dir = temp_data_dir();
    let code = r"
        const first = await ws.app.chats.list({ limit: 1 });
        const second = await ws.app.chats.list({ limit: 1, cursor: first.nextCursor });
        const target = [...first.chats, ...second.chats].find(c => c.agentName === 'Other Assistant chat');
        const conversation = await ws.app.chats.readConversation(target.agentId, { lastN: 1 });
        const older = await ws.app.chats.readConversation(target.agentId, { startTurn: 1, endTurn: 1 });
        const message = conversation.messages[0];
        const full = await ws.app.chats.getMessageBlock(target.agentId, message.id, message.contentBlocks[0].id);
        const renamed = await ws.app.chats.rename(target.agentId, '  Renamed Assistant chat  ');
        const beforeDelete = Date.now();
        const scheduled = await ws.app.chats.delete(target.agentId);
        const afterDelete = Date.now();
        const pending = (await ws.app.chats.list()).chats.find(c => c.agentId === target.agentId).pendingDeleteAt;
        const cancelled = await ws.app.chats.cancelDelete(target.agentId);
        const cancelledAgain = await ws.app.chats.cancelDelete(target.agentId);
        const after = (await ws.app.chats.list()).chats.find(c => c.agentId === target.agentId);
        return { chatManagement: true, first, second, conversation, older, full,
            renamed, beforeDelete, scheduled, afterDelete, pending, cancelled, cancelledAgain, after };
    ";
    let (_daemon, mut ws, mut events) = start_bridge(data_dir.path(), &script, code).await;
    let target = create_chat(&mut ws, 1, CHIEF_WORKSPACE_ID, "Other Assistant chat").await;
    let caller = create_chat(&mut ws, 2, CHIEF_WORKSPACE_ID, "Managing Assistant").await;
    let (_, ordinary) = seed_target(&mut ws, 3, "Ordinary workspace", "Workspace chat").await;
    rpc(
        &mut ws,
        5,
        "agent.update",
        json!({ "agentId": target, "changes": { "notificationsMuted": true } }),
    )
    .await;
    for (id, text) in [
        (6, "Earlier Assistant message"),
        (7, "Latest Assistant message"),
    ] {
        let appended = rpc(
            &mut ws,
            id,
            "agent.appendMessage",
            json!({
                "agentId": target, "workspaceId": CHIEF_WORKSPACE_ID, "role": "user",
                "contentBlocks": [{ "type": "text", "text": text }],
                "metadata": { "nested": { "notificationsMuted": true, "fixture": "preserved" } },
            }),
        )
        .await;
        assert_eq!(appended["success"], true);
    }
    let subscription = rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "eventTypes": ["agent:renamed", "agent:delete-scheduled", "agent:delete-cancelled"],
        }),
    )
    .await;
    assert!(subscription["subscriptionId"].is_string());
    let result = run_bridge(&mut ws, CHIEF_WORKSPACE_ID, &caller).await;
    assert!(
        !result.to_string().contains("notificationsMuted"),
        "hidden field leaked: {result}"
    );
    for page in [&result["first"], &result["second"]] {
        assert_eq!(page["total"], 2, "{page}");
        assert_eq!(page["returned"], 1, "{page}");
        assert_eq!(page["chats"].as_array().unwrap().len(), 1);
        assert_ne!(page["chats"][0]["agentId"], ordinary);
        assert_eq!(
            page["chats"][0]["isCurrent"],
            page["chats"][0]["agentId"] == caller
        );
    }
    assert_eq!(result["first"]["nextCursor"], 1);
    assert!(result["second"].get("nextCursor").is_none());
    let mut listed = [
        result["first"]["chats"][0]["agentId"].as_str().unwrap(),
        result["second"]["chats"][0]["agentId"].as_str().unwrap(),
    ];
    listed.sort_unstable();
    let mut expected = [caller.as_str(), target.as_str()];
    expected.sort_unstable();
    assert_eq!(listed, expected);
    for (key, turn, text) in [
        ("conversation", 2, "Latest Assistant message"),
        ("older", 1, "Earlier Assistant message"),
    ] {
        let read = &result[key];
        assert_eq!(read["workspaceId"], CHIEF_WORKSPACE_ID, "{read}");
        assert_eq!(read["agentId"], target, "{read}");
        assert_eq!(read["agentName"], "Other Assistant chat");
        assert_eq!(read["totalMessages"], 2);
        assert_eq!(read["returnedMessages"], 1);
        assert_eq!(read["startTurn"], turn);
        assert_eq!(read["endTurn"], turn);
        assert_eq!(read["includeToolCalls"], false);
        assert_eq!(read["messages"].as_array().unwrap().len(), 1);
        assert_eq!(read["messages"][0]["contentBlocks"][0]["text"], text);
        assert_eq!(
            read["messages"][0]["metadata"]["nested"]["fixture"],
            "preserved"
        );
    }
    assert_eq!(
        result["full"]["block"],
        result["conversation"]["messages"][0]["contentBlocks"][0]
    );
    assert_eq!(
        result["renamed"],
        json!({ "success": true, "name": "Renamed Assistant chat" })
    );
    assert_eq!(result["scheduled"]["success"], true);
    assert_eq!(result["scheduled"]["scheduled"], true);
    assert_eq!(result["pending"], result["scheduled"]["deleteAt"]);
    let delete_at =
        chrono::DateTime::parse_from_rfc3339(result["scheduled"]["deleteAt"].as_str().unwrap())
            .expect("ISO deletion deadline")
            .timestamp_millis();
    assert!(delete_at >= result["beforeDelete"].as_i64().unwrap() + 15_000);
    assert!(delete_at <= result["afterDelete"].as_i64().unwrap() + 15_000);
    assert_eq!(result["cancelled"], json!({ "cancelled": true }));
    assert_eq!(result["cancelledAgain"], json!({ "cancelled": false }));
    assert_eq!(result["after"]["agentName"], "Renamed Assistant chat");
    assert!(result["after"].get("pendingDeleteAt").is_none());

    let renamed = next_chat_event(&mut events, &subscription, &target).await;
    assert_eq!(renamed["type"], "agent:renamed");
    assert_eq!(
        renamed["data"],
        json!({ "agentId": target, "name": "Renamed Assistant chat" })
    );
    let scheduled = next_chat_event(&mut events, &subscription, &target).await;
    assert_eq!(scheduled["type"], "agent:delete-scheduled");
    assert_eq!(
        scheduled["data"],
        json!({
            "agentId": target, "workspaceId": CHIEF_WORKSPACE_ID,
            "deleteAt": result["scheduled"]["deleteAt"],
        })
    );
    let cancelled = next_chat_event(&mut events, &subscription, &target).await;
    assert_eq!(cancelled["type"], "agent:delete-cancelled");
    assert_eq!(
        cancelled["data"],
        json!({ "agentId": target, "workspaceId": CHIEF_WORKSPACE_ID })
    );
    let persisted = rpc(&mut ws, 201, "agent.get", json!({ "agentId": target })).await;
    assert_eq!(persisted["agent"]["name"], "Renamed Assistant chat");
    assert_eq!(persisted["agent"]["nameExplicitlySet"], true);
    assert_eq!(persisted["agent"]["notificationsMuted"], true);
    assert!(persisted["agent"].get("pendingDeleteAt").is_none());
    let transcript = rpc(
        &mut ws,
        202,
        "agent.getConversation",
        json!({ "agentId": target }),
    )
    .await;
    assert_eq!(transcript["totalMessages"], 2, "chat history survives undo");
    rpc(&mut ws, 203, "agent.stop", json!({ "agentId": caller })).await;
}

#[tokio::test]
async fn ordinary_chat_cannot_bypass_assistant_chat_management_over_wss() {
    let Some(script) = gate("WSS ordinary chat management denial") else {
        return;
    };
    let data_dir = temp_data_dir();
    let code = r"
        const namespaceMissing = typeof ws.app.chats === 'undefined';
        const target = (await ws.agent.list()).find(a => a.name === 'Protected workspace chat');
        const args = {
            workspaceId: '__chief__', agentId: target.id, name: 'Forbidden rename',
            messageId: 'unknown-message', blockId: 'unknown-block',
        };
        const denied = [];
        for (const method of ['list', 'readConversation', 'getMessageBlock', 'rename', 'delete', 'cancelDelete']) {
            try {
                const result = await host({ method: 'app.chats.' + method, args });
                denied.push({ method, allowed: true, result });
            } catch (error) {
                denied.push({ method, allowed: false, error: error.message });
            }
        }
        return { chatManagement: true, namespaceMissing, denied };
    ";
    let (_daemon, mut ws, _events) = start_bridge(data_dir.path(), &script, code).await;
    let (workspace, target) =
        seed_target(&mut ws, 1, "Ordinary workspace", "Protected workspace chat").await;
    let caller = create_chat(&mut ws, 3, &workspace, "Ordinary caller").await;
    let assistant = create_chat(&mut ws, 4, CHIEF_WORKSPACE_ID, "Protected Assistant").await;
    let before = rpc(&mut ws, 5, "agent.get", json!({ "agentId": target })).await;
    let assistant_before = rpc(&mut ws, 6, "agent.get", json!({ "agentId": assistant })).await;
    let result = run_bridge(&mut ws, &workspace, &caller).await;
    assert_eq!(result["namespaceMissing"], true, "{result}");
    let denied = result["denied"].as_array().expect("raw host results");
    assert_eq!(denied.len(), 6);
    for call in denied {
        assert_eq!(call["allowed"], false, "raw host bypass: {call}");
        assert!(
            call["error"]
                .as_str()
                .unwrap()
                .contains("only available in the Assistant workspace"),
            "{call}"
        );
    }
    let after = rpc(&mut ws, 201, "agent.get", json!({ "agentId": target })).await;
    assert_eq!(after, before, "raw host denial must not mutate target");
    let assistant_after = rpc(&mut ws, 202, "agent.get", json!({ "agentId": assistant })).await;
    assert_eq!(
        assistant_after, assistant_before,
        "Assistant chats remain unchanged"
    );
    rpc(&mut ws, 203, "agent.stop", json!({ "agentId": caller })).await;
}

#[tokio::test]
async fn assistant_chat_management_rejects_workspace_targets_and_current_chat_deletion_over_wss() {
    let Some(script) = gate("WSS Assistant chat target restrictions") else {
        return;
    };
    let data_dir = temp_data_dir();
    let code = r"
        const target = (await ws.app.agents.list()).threads.find(a => a.agentName === 'Protected workspace chat');
        const caller = (await ws.app.chats.list()).chats.find(c => c.isCurrent);
        const denied = [];
        for (const method of ['readConversation', 'getMessageBlock', 'rename', 'delete', 'cancelDelete']) {
            try {
                const result = await host({ method: 'app.chats.' + method, args: {
                    workspaceId: '__chief__', agentId: target.agentId, name: 'Forbidden rename',
                    messageId: 'unknown-message', blockId: 'unknown-block',
                } });
                denied.push({ method, allowed: true, result });
            } catch (error) {
                denied.push({ method, allowed: false, error: error.message });
            }
        }
        let selfDelete;
        try { selfDelete = { allowed: true, result: await ws.app.chats.delete(caller.agentId) }; }
        catch (error) { selfDelete = { allowed: false, error: error.message }; }
        return { chatManagement: true, denied, selfDelete };
    ";
    let (_daemon, mut ws, _events) = start_bridge(data_dir.path(), &script, code).await;
    let (_, target) =
        seed_target(&mut ws, 1, "Ordinary workspace", "Protected workspace chat").await;
    let caller = create_chat(&mut ws, 3, CHIEF_WORKSPACE_ID, "Managing Assistant").await;
    let before = rpc(&mut ws, 4, "agent.get", json!({ "agentId": target })).await;
    let result = run_bridge(&mut ws, CHIEF_WORKSPACE_ID, &caller).await;
    let denied = result["denied"]
        .as_array()
        .expect("target rejection results");
    assert_eq!(denied.len(), 5);
    for call in denied {
        assert_eq!(
            call["allowed"], false,
            "non-Assistant target accepted: {call}"
        );
        assert!(!call["error"].as_str().unwrap().is_empty(), "{call}");
    }
    assert_eq!(result["selfDelete"]["allowed"], false, "{result}");
    assert!(
        result["selfDelete"]["error"]
            .as_str()
            .unwrap()
            .contains("current"),
        "{result}"
    );
    let after = rpc(&mut ws, 201, "agent.get", json!({ "agentId": target })).await;
    assert_eq!(
        after, before,
        "rejected target operations must not mutate the session"
    );
    let current = rpc(&mut ws, 202, "agent.get", json!({ "agentId": caller })).await;
    assert!(
        current["agent"].get("pendingDeleteAt").is_none(),
        "{current}"
    );
    assert_eq!(current["agent"]["name"], "Managing Assistant");
    rpc(&mut ws, 203, "agent.stop", json!({ "agentId": caller })).await;
}
