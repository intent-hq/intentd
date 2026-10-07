//! Real model JS binding through services and TLS/WSS to a recording executor.
//! Native coordinate mapping and button events are covered by executor tests.
use super::*;

fn tool(code: &str) -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_api","arguments":{"code":code,"summary":"Verify button-free cursor movement"}}})
}

fn assert_tool_error(response: &Value, code: &str) {
    assert_eq!(response["result"]["isError"], true, "{response}");
    assert!(
        response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains(code),
        "{response}"
    );
}

#[tokio::test]
async fn wss_desktop_move_binding_preserves_action_and_refuses_invalid_or_revoked_input() {
    let (srv, services) = super::super::authenticated_devices::start_roster().await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.registry
        .apply(&[
            ("model.defaultProvider".into(), json!("auggie")),
            ("providers.paths".into(), json!({"auggie":"/bin/sh"})),
        ])
        .unwrap();
    let principal = srv.store.get_primary_principal().await.unwrap();
    let created = intent_core::with_caller(
        Caller::Wire {
            principal_id: principal.id,
            host_role: HostRole::Owner,
        },
        services.agent_create(
            ws.clone(),
            Some("Move agent".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let bridge = intent_acp::WorkspaceMcpServer::new(srv.api.clone(), ws.clone())
        .with_caller_agent_id(Some(agent.clone()));
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
    let mut socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let mut calls = vec![json!({"displayCount":1})];
    call(
        &mut socket,
        "client.hello",
        json!({"clientId":"move-desktop","capabilities":{"browserExec":true,"desktopControl":1}}),
        &mut calls,
    )
    .await;
    srv.store
        .set_workspace_browser_client(&ws, Some(&intent_core::ClientId::from("move-desktop")))
        .await
        .unwrap();
    let movement = tool("return await ws.desktop.move({x:10.5,y:20,layoutId:'l'});");
    let response = drive(&mut socket, bridge.handle_message(&movement), &mut calls)
        .await
        .unwrap();
    assert_tool_error(&response, "desktop-not-active");
    assert!(!calls
        .iter()
        .any(|p| p["operation"] == "prepareCommand" || p["operation"] == "startControl"));
    let remembered = call(
        &mut socket,
        "desktop.setPermission",
        json!({"workspaceId":ws,"agentId":agent,"computerId":"wss-physical","allowed":true}),
        &mut calls,
    )
    .await;
    assert!(remembered.get("result").is_some(), "{remembered}");
    let active = drive(
        &mut socket,
        intent_core::with_caller(
            Caller::Agent {
                agent_id: agent.clone(),
            },
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();

    // The JS bridge must reject malformed/nonfinite and button-bearing arguments
    // before issuing any command to the native endpoint.
    let before = calls
        .iter()
        .filter(|p| p["operation"] == "prepareCommand")
        .count();
    for args in [
        "{}",
        "{x:0,y:0}",
        "{x:-1,y:0,layoutId:'l'}",
        "{x:0,y:NaN,layoutId:'l'}",
        "{x:Infinity,y:0,layoutId:'l'}",
        "{x:0,y:'1',layoutId:'l'}",
        "{x:0,y:0,layoutId:'l',button:'left'}",
        "{x:0,y:0,layoutId:'l',clickCount:1}",
        "{x:0,y:0,layoutId:'l',from:{x:0,y:0}}",
        "{x:0,y:0,layoutId:'l',agentId:'other'}",
    ] {
        let request = tool(&format!("return await ws.desktop.move({args});"));
        let response = drive(&mut socket, bridge.handle_message(&request), &mut calls)
            .await
            .unwrap();
        assert_tool_error(&response, "invalid-params");
    }
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "prepareCommand")
            .count(),
        before
    );
    for (args, action) in [
        (
            "{x:10.5,y:20,layoutId:'l'}",
            json!({"kind":"move","x":10.5,"y":20,"layoutId":"l"}),
        ),
        (
            "{x:0,y:0,layoutId:'l',displayId:'d'}",
            json!({"kind":"move","x":0,"y":0,"layoutId":"l","displayId":"d"}),
        ),
    ] {
        let before = calls.len();
        let request=tool(&format!("const result=await ws.desktop.move({args}); if(JSON.stringify(result)!=='{{\"ok\":true}}') throw new Error('Unexpected move result'); return result;"));
        let response = drive(&mut socket, bridge.handle_message(&request), &mut calls)
            .await
            .unwrap();
        assert!(response.get("result").is_some(), "{response}");
        assert_ne!(response["result"]["isError"], true, "{response}");
        let commands: Vec<_> = calls[before..]
            .iter()
            .filter(|p| p["operation"] == "prepareCommand" || p["operation"] == "execute")
            .collect();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0]["action"], action);
        assert_eq!(commands[0]["sessionId"], active["sessionId"]);
        assert_eq!(commands[1]["operation"], "execute");
        assert_eq!(commands[1]["commandId"], commands[0]["commandId"]);
        assert_eq!(commands[1]["sequence"], commands[0]["sequence"]);
        assert_eq!(commands[1]["deadlineId"], "wss-ticket");
        assert!(commands[1].get("action").is_none());
    }
    // These errors originate from the recording native endpoint, proving the
    // transport/JS error contract; real bounds/topology checks live in Electron.
    let executed = calls.iter().filter(|p| p["operation"] == "execute").count();
    for (args, code) in [
        ("{x:1920,y:0,layoutId:'l'}", "invalid-params"),
        ("{x:0,y:1080,layoutId:'l'}", "invalid-params"),
        ("{x:0,y:0,layoutId:'old'}", "desktop-stale-layout"),
        (
            "{x:0,y:0,layoutId:'l',displayId:'missing'}",
            "desktop-display-unavailable",
        ),
    ] {
        let request = tool(&format!("return await ws.desktop.move({args});"));
        let response = drive(&mut socket, bridge.handle_message(&request), &mut calls)
            .await
            .unwrap();
        assert_tool_error(&response, code);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("execution: not_started"));
    }
    calls[0]["displayCount"] = 2.into();
    let response = drive(&mut socket, bridge.handle_message(&movement), &mut calls)
        .await
        .unwrap();
    assert_tool_error(&response, "desktop-display-selection-required");
    assert_eq!(
        calls.iter().filter(|p| p["operation"] == "execute").count(),
        executed
    );
    let revoked = call(
        &mut socket,
        "desktop.revoke",
        json!({"workspaceId":ws,"sessionId":active["sessionId"],"reason":"screen_locked"}),
        &mut calls,
    )
    .await;
    assert_eq!(revoked["result"]["revoked"], true);
    let prepared = calls
        .iter()
        .filter(|p| p["operation"] == "prepareCommand")
        .count();
    let response = drive(&mut socket, bridge.handle_message(&movement), &mut calls)
        .await
        .unwrap();
    assert_tool_error(&response, "desktop-not-active");
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "prepareCommand")
            .count(),
        prepared
    );
    assert_eq!(
        calls.iter().filter(|p| p["operation"] == "execute").count(),
        executed
    );
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count(),
        1,
        "move must not restart control"
    );
    srv.ws.stop().await;
}
