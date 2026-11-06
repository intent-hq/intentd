//! Window metadata and `windowId` targets through the real JS binding,
//! services and TLS/WSS reverse channel. Native target checks live in Electron.
use super::*;

fn tool(code: &str) -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_api","arguments":{"code":code,"summary":"Verify window-targeted desktop input"}}})
}

fn text(response: &Value) -> &str {
    response["result"]["content"][0]["text"].as_str().unwrap()
}

#[tokio::test]
async fn wss_window_targets_forward_unchanged_and_mismatch_reason_reaches_the_model() {
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
            Some("Window agent".into()),
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
    let mut calls = vec![json!({"displayCount":1,"screenshotExtra":{
        "frontmost":{"appName":"Slack","bundleId":"com.tinyspeck.slackmacgap","pid":522,"windowId":"win-slack"},
        "windows":[
            {"windowId":"win-slack","appName":"Slack","bundleId":"com.tinyspeck.slackmacgap","pid":522,"title":"general","x":-120,"y":50,"width":1400,"height":900,"focused":true},
            {"windowId":"win-safari","appName":"Safari","bundleId":"com.apple.Safari","pid":411,"x":200,"y":100,"width":1600,"height":1000,"focused":false}
        ]
    }})];
    call(
        &mut socket,
        "client.hello",
        json!({"clientId":"window-desktop","capabilities":{"browserExec":true,"desktopControl":1}}),
        &mut calls,
    )
    .await;
    srv.store
        .set_workspace_browser_client(&ws, Some(&intent_core::ClientId::from("window-desktop")))
        .await
        .unwrap();
    let remembered = call(
        &mut socket,
        "desktop.setPermission",
        json!({"workspaceId":ws,"agentId":agent,"computerId":"wss-physical","allowed":true}),
        &mut calls,
    )
    .await;
    assert!(remembered.get("result").is_some(), "{remembered}");
    drive(
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

    // Screenshot window metadata reaches the model unchanged.
    let want = json!({
        "frontmost": calls[0]["screenshotExtra"]["frontmost"],
        "windows": calls[0]["screenshotExtra"]["windows"],
    });
    let shot = tool(&format!("const s=await ws.desktop.screenshot(); const got=JSON.stringify({{frontmost:s.frontmost,windows:s.windows}}); if(got!=={want:?}) throw new Error('window metadata changed: '+got); return 'ok';", want = want.to_string()));
    let response = drive(&mut socket, bridge.handle_message(&shot), &mut calls)
        .await
        .unwrap();
    assert_ne!(response["result"]["isError"], true, "{response}");

    // Invalid IDs are refused before any native command.
    let prepared = calls
        .iter()
        .filter(|p| p["operation"] == "prepareCommand")
        .count();
    for code in [
        "ws.desktop.type({text:'x',windowId:''})",
        "ws.desktop.click({x:1,y:1,layoutId:'l',windowId:null})",
        "ws.desktop.keypress({key:'a',windowId:7})",
        "ws.desktop.focusWindow({})",
        "ws.desktop.focusWindow({windowId:'w',x:1})",
        "ws.desktop.screenshot({windowId:'w'})",
    ] {
        let response = drive(
            &mut socket,
            bridge.handle_message(&tool(&format!("return await {code};"))),
            &mut calls,
        )
        .await
        .unwrap();
        assert_eq!(response["result"]["isError"], true, "{code}: {response}");
        assert!(text(&response).contains("invalid-params"), "{code}");
    }
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "prepareCommand")
            .count(),
        prepared
    );

    // Targets and focusWindow forward unchanged on the existing reverse method.
    for (code, action) in [
        (
            "ws.desktop.focusWindow({windowId:'win-safari'})",
            json!({"kind":"focusWindow","windowId":"win-safari"}),
        ),
        (
            "ws.desktop.type({text:'hello',windowId:'win-safari'})",
            json!({"kind":"type","text":"hello","windowId":"win-safari"}),
        ),
        (
            "ws.desktop.drag({layoutId:'l',from:{x:300,y:200},to:{x:5,y:5},windowId:'win-safari'})",
            json!({"kind":"drag","layoutId":"l","from":{"x":300,"y":200},"to":{"x":5,"y":5},"windowId":"win-safari"}),
        ),
    ] {
        let before = calls.len();
        let response = drive(
            &mut socket,
            bridge.handle_message(&tool(&format!("return await {code};"))),
            &mut calls,
        )
        .await
        .unwrap();
        assert_ne!(response["result"]["isError"], true, "{code}: {response}");
        let prepare = calls[before..]
            .iter()
            .find(|p| p["operation"] == "prepareCommand")
            .unwrap();
        assert_eq!(prepare["action"], action);
    }

    // A native target refusal keeps its code, execution and reason for the
    // model, is never retried untargeted, and leaves control active.
    let detail = "The target window is not ready for input. Call ws.desktop.focusWindow({ windowId }) if needed, take a new screenshot to confirm the target, then retry.";
    for (execution, reason) in [("not_started", "not_frontmost"), ("partial", "not_focused")] {
        calls[0]["executeError"] = json!({"code":"desktop-target-mismatch","detail":detail,"execution":execution,"reason":reason});
        let executed = calls.iter().filter(|p| p["operation"] == "execute").count();
        let response = drive(
            &mut socket,
            bridge.handle_message(&tool(
                "return await ws.desktop.focusWindow({windowId:'win-safari'});",
            )),
            &mut calls,
        )
        .await
        .unwrap();
        assert_eq!(response["result"]["isError"], true, "{response}");
        let message = text(&response);
        assert!(message.contains("desktop-target-mismatch"), "{message}");
        assert!(message.contains(detail), "{message}");
        assert!(
            message.contains(&format!("execution: {execution}, reason: {reason}")),
            "{message}"
        );
        assert_eq!(
            calls.iter().filter(|p| p["operation"] == "execute").count(),
            executed + 1,
            "no automatic retry"
        );
        let direct = drive(
            &mut socket,
            intent_core::with_caller(
                Caller::Agent {
                    agent_id: agent.clone(),
                },
                services.desktop_agent_call(
                    ws.clone(),
                    "type".into(),
                    json!({"text":"Safari","windowId":"win-safari"}),
                ),
            ),
            &mut calls,
        )
        .await
        .unwrap_err();
        assert_eq!(direct.numeric_code(), -32602);
        assert_eq!(direct.reason.as_deref(), Some(reason));
        assert_eq!(direct.execution.as_deref(), Some(execution));
    }
    calls[0].as_object_mut().unwrap().remove("executeError");
    let response = drive(
        &mut socket,
        bridge.handle_message(&tool(
            "return await ws.desktop.type({text:'retry',windowId:'win-safari'});",
        )),
        &mut calls,
    )
    .await
    .unwrap();
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count(),
        1,
        "a target mismatch must not end or restart control"
    );
    srv.ws.stop().await;
}
