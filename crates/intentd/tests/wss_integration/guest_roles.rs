//! Persisted guest roles through the authenticated TLS router and live streams.
use super::*;
use intent_core::{AgentId, WorkspaceRole};
use serde_json::json;

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn frame(ws: &mut Ws) -> Value {
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(s))) => return serde_json::from_str(&s).unwrap(),
                Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("connection closed: {other:?}"),
            }
        }
    })
    .await
    .expect("frame deadline")
}

async fn owner(srv: &Server, method: &str, params: Value) -> Value {
    wss_call(
        srv.port,
        srv.cfg.clone(),
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
    )
    .await
}

#[intent_test_macros::daemon_test]
async fn guest_roles_wss_live_downgrade_and_execution_boundaries() {
    let srv = start(WsOptions::default()).await;
    let mut guest = Guest::connect(&srv, &"ac".repeat(32)).await;
    let created = owner(&srv, "workspace.create", json!({"title":"Guest roles"})).await;
    let ws = WorkspaceId::from(created["result"]["workspace"]["id"].as_str().unwrap());
    let primary = srv.store.get_primary_principal().await.unwrap();
    srv.store
        .add_workspace_member(&ws, &guest.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let hello = guest
        .call("client.hello", json!({"name":"Guest role test"}))
        .await;
    assert_eq!(
        hello["result"]["server"]["capabilities"]["workspaceGuestRoles"], 1,
        "{hello}"
    );
    let caps = guest.call("system.capabilities", json!({})).await;
    assert_eq!(caps["result"]["workspaceGuestRoles"], 1, "{caps}");
    for method in ["agent.create", "script.list"] {
        let refused = guest.call(method, json!({"workspaceId":ws})).await;
        assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
    }
    let params = json!({"workspaceId":ws,"principalId":guest.principal.id,"role":"manager"});
    assert_eq!(
        guest
            .call("workspace.members.setRole", params.clone())
            .await["error"]["code"],
        -32003
    );
    let subscribed = guest
        .call(
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["workspace:updated","script:*"]}),
        )
        .await;
    assert!(subscribed.get("error").is_none(), "{subscribed}");
    let changed = owner(&srv, "workspace.members.setRole", params.clone()).await;
    assert_eq!(
        changed["result"],
        json!({"workspaceId":ws,"principalId":guest.principal.id,"role":"manager","changed":true})
    );
    loop {
        let event = frame(&mut guest.ws).await;
        if event["method"] == "events.event"
            && event["params"]["event"]["data"]["changes"]["members"] == true
        {
            assert_eq!(
                event["params"]["event"]["data"],
                json!({"workspaceId":ws,"changes":{"members":true,"rolePrincipalId":guest.principal.id,"role":"manager"}})
            );
            break;
        }
    }
    assert_eq!(
        owner(&srv, "workspace.members.setRole", params).await["result"]["changed"],
        false
    );
    let view = guest.call("workspace.get", json!({"workspaceId":ws})).await;
    assert_eq!(view["result"]["workspace"]["myRole"], "manager");
    assert_eq!(view["result"]["workspace"]["canManage"], false);
    let roster = guest
        .call("workspace.members.list", json!({"workspaceId":ws}))
        .await;
    assert_eq!(roster["result"]["guestCount"], 1);
    assert_eq!(
        roster["result"]["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["principalId"] == guest.principal.id.as_str())
            .unwrap()["role"],
        "manager"
    );
    for (target, role) in [
        (&primary.id, "manager"),
        (&guest.principal.id, "owner"),
        (&guest.principal.id, "admin"),
    ] {
        let bad = owner(
            &srv,
            "workspace.members.setRole",
            json!({"workspaceId":ws,"principalId":target,"role":role}),
        )
        .await;
        assert_eq!(bad["error"]["code"], -32602, "{bad}");
    }
    let agent = guest
        .call(
            "agent.create",
            json!({"workspaceId":ws,"name":"Manager agent","provider":"mock","model":"default"}),
        )
        .await;
    let aid = agent["result"]["agent"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{agent}"))
        .to_owned();
    let model = guest
        .call(
            "agent.setModel",
            json!({"workspaceId":ws,"agentId":aid,"modelId":"role-test-model"}),
        )
        .await;
    assert!(model.get("error").is_none(), "{model}");
    let bogus = guest.call("agent.setModel",json!({"workspaceId":ws,"agentId":aid,"modelId":"role-test-model","providerId":"not-a-provider"})).await;
    assert_eq!(bogus["error"]["code"], -32602, "{bogus}");
    let effort = guest
        .call(
            "agent.update",
            json!({"workspaceId":ws,"agentId":aid,"changes":{"reasoningEffort":null}}),
        )
        .await;
    assert!(effort.get("error").is_none(), "{effort}");
    for changes in [
        json!({"systemPrompt":"override"}),
        json!({"metadata":{"permissionMode":"bypassPermissions"}}),
        json!({"provider":"arbitrary"}),
    ] {
        assert_eq!(
            guest
                .call(
                    "agent.update",
                    json!({"workspaceId":ws,"agentId":aid,"changes":changes})
                )
                .await["error"]["code"],
            -32003
        );
    }
    for extra in [
        json!({"metadata":{"permissionMode":"bypassPermissions"}}),
        json!({"workspacePath":"/"}),
        json!({"agentType":"chief"}),
    ] {
        let mut p = extra;
        p["workspaceId"] = json!(ws);
        assert_eq!(guest.call("agent.create", p).await["error"]["code"], -32003);
    }
    let saved = owner(&srv,"script.create",json!({"workspaceId":ws,"name":"saved command","command":"printf guest-role-ok","mode":"command","purpose":"saved"})).await;
    let sid = saved["result"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{saved}"))
        .to_owned();
    assert!(guest
        .call("script.list", json!({"workspaceId":ws}))
        .await
        .get("error")
        .is_none());
    for method in ["script.get", "script.status", "script.output"] {
        let response = guest
            .call(method, json!({"workspaceId":ws,"scriptId":sid}))
            .await;
        assert!(response.get("error").is_none(), "{method}: {response}");
    }
    for (method, extra) in [
        ("host.exec", json!({"command":"true"})),
        ("terminal.create", json!({})),
        (
            "terminal.write",
            json!({"terminalId":sid,"data":"whoami\n"}),
        ),
        (
            "script.create",
            json!({"name":"bad","command":"true","mode":"command"}),
        ),
        ("script.remove", json!({"scriptId":sid})),
        (
            "workspace.members.setRole",
            json!({"principalId":guest.principal.id,"role":"collaborator"}),
        ),
        ("agent.delete", json!({"agentId":aid})),
        ("agent.delegate", json!({"taskNoteId":"spec"})),
        (
            "settings.update",
            json!({"changes":{"providers.test.command":"true"}}),
        ),
    ] {
        let mut p = extra;
        p["workspaceId"] = json!(ws);
        let response = guest.call(method, p).await;
        assert_eq!(response["error"]["code"], -32003, "{method}: {response}");
    }
    for key in ["command", "args", "env", "cwd", "terminalId"] {
        let mut p = json!({"workspaceId":ws,"scriptId":sid});
        p[key] = json!("override");
        assert_eq!(
            guest.call("script.start", p.clone()).await["error"]["code"],
            -32602
        );
        assert_eq!(guest.call("script.run", p).await["error"]["code"], -32602);
    }
    let ran = guest
        .call(
            "script.run",
            json!({"workspaceId":ws,"scriptId":sid,"timeoutSeconds":5}),
        )
        .await;
    assert!(ran.get("error").is_none(), "{ran}");
    assert!(ran.to_string().contains("guest-role-ok"), "{ran}");
    let one_off = owner(&srv,"script.create",json!({"workspaceId":ws,"name":"private command","command":"true","mode":"command","purpose":"oneOff"})).await;
    let private_id = one_off["result"]["id"].as_str().unwrap();
    for method in [
        "script.get",
        "script.output",
        "script.status",
        "script.start",
        "script.stop",
        "script.restart",
        "script.run",
    ] {
        let response = guest
            .call(method, json!({"workspaceId":ws,"scriptId":private_id}))
            .await;
        assert_eq!(response["error"]["code"], -32003, "{method}: {response}");
    }
    for opts in [
        json!({"workspaceId":ws}),
        json!({"workspaceId":ws,"view":"summary","limit":1}),
    ] {
        let list = guest.call("script.list", opts).await;
        assert_eq!(
            list["result"]["scripts"].as_array().unwrap().len(),
            1,
            "{list}"
        );
        assert_eq!(list["result"]["scripts"][0]["id"], sid);
    }
    let watch_url = format!("wss://localhost:{}/ws?token={}", srv.port, "ac".repeat(32));
    let mut watcher = Guest {
        principal: guest.principal.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &watch_url).await,
        next_id: 1000,
    };
    let sub = watcher
        .call(
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["script:*"]}),
        )
        .await;
    assert!(sub.get("error").is_none(), "{sub}");
    let started = guest
        .call("script.start", json!({"workspaceId":ws,"scriptId":sid}))
        .await;
    assert_eq!(started["result"]["ok"], true, "{started}");
    // The actual PTY exit event proves Manager streaming delivery, without sleeps.
    loop {
        let v = frame(&mut watcher.ws).await;
        let e = &v["params"]["event"];
        if e["type"] == intent_core::events::SCRIPT_STATE
            && e["data"]["scriptId"] == sid
            && e["data"]["status"] == "exited"
        {
            break;
        }
    }
    let output = guest
        .call("script.output", json!({"workspaceId":ws,"scriptId":sid}))
        .await;
    assert!(output.to_string().contains("guest-role-ok"), "{output}");
    assert_eq!(
        guest
            .call("script.restart", json!({"workspaceId":ws,"scriptId":sid}))
            .await["result"]["ok"],
        true
    );
    assert_eq!(
        guest
            .call("script.stop", json!({"workspaceId":ws,"scriptId":sid}))
            .await["result"]["ok"],
        true
    );
    let other = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&other))
        .await
        .unwrap();
    let cross = guest
        .call("script.status", json!({"workspaceId":other,"scriptId":sid}))
        .await;
    assert!(cross.get("error").is_some(), "{cross}");
    let downgrade = owner(
        &srv,
        "workspace.members.setRole",
        json!({"workspaceId":ws,"principalId":guest.principal.id,"role":"collaborator"}),
    )
    .await;
    assert_eq!(downgrade["result"]["changed"], true);
    for method in [
        "agent.create",
        "script.list",
        "script.get",
        "script.start",
        "script.stop",
        "script.restart",
        "script.status",
        "script.output",
        "script.run",
    ] {
        let response = guest
            .call(method, json!({"workspaceId":ws,"scriptId":sid}))
            .await;
        assert_eq!(
            response["error"]["code"], -32003,
            "live downgrade {method}: {response}"
        );
    }
    let effort = guest
        .call(
            "agent.update",
            json!({"workspaceId":ws,"agentId":aid,"changes":{"reasoningEffort":null}}),
        )
        .await;
    assert_eq!(effort["error"]["code"], -32003, "{effort}");
    // Baseline Participant model selection remains available after the downgrade.
    let model = guest
        .call(
            "agent.setModel",
            json!({"workspaceId":ws,"agentId":aid,"modelId":"participant-model"}),
        )
        .await;
    assert!(model.get("error").is_none(), "{model}");
    assert!(srv
        .store
        .get_agent_session_summary(&AgentId::from(aid))
        .await
        .is_ok());
    drop(watcher);
    drop(guest);
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn guest_roles_saved_to_one_off_clears_cache_without_private_payload() {
    let srv = start(WsOptions::default()).await;
    let mut guest = Guest::connect(&srv, &"ad".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(&ws, &guest.principal.id, WorkspaceRole::Manager)
        .await
        .unwrap();
    let saved = owner(&srv, "script.create", json!({"workspaceId":ws,"name":"saved","command":"true","mode":"command","purpose":"saved"})).await;
    let sid = saved["result"]["id"].as_str().unwrap().to_owned();
    let sub = guest
        .call(
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["script:*","workspace:updated"]}),
        )
        .await;
    assert!(sub.get("error").is_none(), "{sub}");
    let private = owner(&srv, "script.create", json!({"workspaceId":ws,"scriptId":sid,"name":"private","command":"private-command-must-not-leak","mode":"command","purpose":"oneOff"})).await;
    assert!(private.get("error").is_none(), "{private}");
    let marker = owner(
        &srv,
        "workspace.update",
        json!({"workspaceId":ws,"changes":{"title":"transition-complete"}}),
    )
    .await;
    assert!(marker.get("error").is_none(), "{marker}");
    let mut removed = false;
    loop {
        let message = frame(&mut guest.ws).await;
        assert!(
            !message
                .to_string()
                .contains("private-command-must-not-leak"),
            "{message}"
        );
        let event = &message["params"]["event"];
        if event["type"] == intent_core::events::SCRIPT_CHANGED {
            assert_eq!(
                event["data"],
                json!({"workspaceId":ws,"scriptId":sid,"action":"guest-access-revoked"})
            );
            removed = true;
        }
        if event["type"] == intent_core::events::WORKSPACE_UPDATED {
            break;
        }
    }
    assert!(
        removed,
        "Manager must invalidate its previously saved row before the barrier"
    );
    for method in [
        "script.get",
        "script.output",
        "script.status",
        "script.start",
        "script.run",
    ] {
        let reply = guest
            .call(method, json!({"workspaceId":ws,"scriptId":sid}))
            .await;
        assert_eq!(reply["error"]["code"], -32003, "{method}: {reply}");
    }
    let list = guest.call("script.list", json!({"workspaceId":ws})).await;
    assert_eq!(list["result"]["scripts"], json!([]));
    drop(guest);
    srv.ws.stop().await;
}
