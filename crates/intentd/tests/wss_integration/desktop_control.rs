//! Real TLS/WSS desktop routing, native reverse replies, consent and no reconnect replay.
use super::*;
use intent_core::{AgentId, Caller, HostRole};
use serde_json::json;
use std::future::Future;

type Socket = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
async fn executor_reply(socket: &mut Socket, request: Value, calls: &mut Vec<Value>) {
    assert_eq!(request["method"], "desktop.control");
    let p = &request["params"];
    assert!(p["connectionEpoch"].is_string());
    assert!(p["principalId"].is_string());
    calls.push(p.clone());
    let display_count = calls.iter().find_map(|call| call["displayCount"].as_u64());
    if let Some(count) = display_count.filter(|_| {
        p["operation"] == "prepareCommand"
            && matches!(
                p["action"]["kind"].as_str(),
                Some("screenshot" | "click" | "scroll" | "drag")
            )
    }) {
        let action = &p["action"];
        let failure = if count == 0
            || action
                .get("displayId")
                .is_some_and(|id| id != "d" && !(count == 2 && id == "e"))
        {
            Some((
                "desktop-display-unavailable",
                "The requested display is unavailable.",
            ))
        } else if count > 1 && action.get("displayId").is_none() {
            Some(("desktop-display-selection-required","Multiple displays are available. Call ws.desktop.listDisplay() and ask the user which screen to use, then retry with displayId."))
        } else if action.get("layoutId").is_some_and(|layout| layout != "l") {
            Some(("desktop-stale-layout", "Display layout changed."))
        } else {
            None
        };
        if let Some((code, detail)) = failure {
            socket.send(Message::Text(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":detail,"data":{"code":code,"detail":detail,"execution":"not_started"}}}).to_string().into())).await.unwrap();
            return;
        }
    }
    let result = match p["operation"].as_str().unwrap() {
        "prepare" => {
            json!({"computerId":"wss-physical","computerName":"WSS desktop","platform":"windows"})
        }
        "startControl" => {
            json!({"ready":true,"sessionId":p["sessionId"],"computerId":"wss-physical"})
        }
        "renew" => json!({"renewed":true,"sessionId":p["sessionId"]}),
        "endControl" => json!({"ended":true,"sessionId":p["sessionId"]}),
        "prepareCommand" => {
            json!({"commandId":p["commandId"],"sequence":p["sequence"],"deadlineId":"wss-ticket","expiresInMs":10000})
        }
        "execute" => {
            let action = calls
                .iter()
                .rev()
                .find(|call| {
                    call["operation"] == "prepareCommand" && call["commandId"] == p["commandId"]
                })
                .unwrap()["action"]
                .clone();
            let result = match action["kind"].as_str().unwrap() {
                "listDisplay" => {
                    let displays:Vec<Value>=(0..display_count.unwrap_or(1)).map(|index|json!({"displayId":if index==0 {"d"} else {"e"},"width":1920,"height":1080,"originX":index*1920,"originY":0,"scaleFactor":1.0})).collect();
                    json!({"layoutId":"l","displays":displays})
                }
                "screenshot" => {
                    json!({"capturedAt":"2026-10-02T09:00:00Z","layoutId":"l","displays":[{"displayId":action.get("displayId").cloned().unwrap_or(json!("d")),"width":1920,"height":1080,"originX":0,"originY":0,"scaleFactor":1.0,"assetId":"capture","url":format!("workspace-asset://{}/capture",p["workspaceId"].as_str().unwrap()),"mimeType":"image/png"}]})
                }
                _ => json!({"ok":true}),
            };
            json!({"commandId":p["commandId"],"sequence":p["sequence"],"result":result})
        }
        other => panic!("Unexpected desktop operation: {other}"),
    };
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
}
async fn drive<F: Future>(socket: &mut Socket, future: F, calls: &mut Vec<Value>) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(Duration::from_secs(15),async {
        loop { tokio::select! {
            result=&mut future => return result,
            message=socket.next()=> match message.unwrap().unwrap() {
                Message::Text(text)=> {let v:Value=serde_json::from_str(&text).unwrap(); if v["method"]=="desktop.control" {executor_reply(socket,v,calls).await;} else { calls.push(v); } },
                Message::Ping(bytes)=> socket.send(Message::Pong(bytes)).await.unwrap(),
                _=>{}
            }
        }}
    }).await.expect("desktop service call timed out")
}
async fn call(socket: &mut Socket, method: &str, params: Value, calls: &mut Vec<Value>) -> Value {
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1001,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == "desktop.control" {
                        executor_reply(socket, v, calls).await;
                    } else if v["id"] == 1001 {
                        assert_eq!(v["jsonrpc"], "2.0");
                        return v;
                    }
                }
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                _ => {}
            }
        }
    })
    .await
    .expect("desktop WSS RPC timed out")
}
async fn drive_pair<F: Future>(
    a: &mut Socket,
    b: &mut Socket,
    future: F,
    ac: &mut Vec<Value>,
    bc: &mut Vec<Value>,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(Duration::from_secs(15),async {
        loop {tokio::select! {
            result=&mut future=>return result,
            message=a.next()=> {if let Message::Text(text)=message.unwrap().unwrap() {let v:Value=serde_json::from_str(&text).unwrap(); if v["method"]=="desktop.control" {executor_reply(a,v,ac).await;} else {ac.push(v);}}},
            message=b.next()=> {if let Message::Text(text)=message.unwrap().unwrap() {let v:Value=serde_json::from_str(&text).unwrap(); if v["method"]=="desktop.control" {executor_reply(b,v,bc).await;} else {bc.push(v);}}},
        }}
    }).await.expect("two-client desktop operation timed out")
}

#[tokio::test]
async fn simultaneous_wss_approvals_claim_one_primary_and_activate_only_that_connection() {
    let (srv, services) = super::authenticated_devices::start_roster().await;
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
            Some("Race agent".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(created["id"].as_str().unwrap());
    let caller = Caller::Agent {
        agent_id: agent.clone(),
    };
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
    let mut a = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let mut b = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let (mut ac, mut bc) = (vec![], vec![]);
    for (socket, calls, client) in [(&mut a, &mut ac, "race-a"), (&mut b, &mut bc, "race-b")] {
        assert!(call(
            socket,
            "client.hello",
            json!({"clientId":client,"capabilities":{"browserExec":true,"desktopControl":1}}),
            calls
        )
        .await
        .get("error")
        .is_none());
    }
    let pending = drive_pair(
        &mut a,
        &mut b,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut ac,
        &mut bc,
    )
    .await
    .unwrap();
    assert!(srv
        .store
        .workspace_browser_client(&ws)
        .await
        .unwrap()
        .is_none());
    let args = json!({"workspaceId":ws,"requestId":pending["requestId"],"decision":"allow_once"});
    let (ar, br) = tokio::join!(
        call(&mut a, "desktop.respondPermission", args.clone(), &mut ac),
        call(&mut b, "desktop.respondPermission", args, &mut bc)
    );
    assert_eq!(
        usize::from(ar.get("result").is_some()) + usize::from(br.get("result").is_some()),
        1
    );
    let winner = srv
        .store
        .workspace_browser_client(&ws)
        .await
        .unwrap()
        .unwrap();
    drive_pair(
        &mut a,
        &mut b,
        intent_core::with_caller(caller.clone(), async {
            loop {
                let state = services
                    .agent_snapshot(ws.clone(), agent.clone())
                    .await
                    .unwrap();
                if state["desktopControl"]["status"] == "active" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }),
        &mut ac,
        &mut bc,
    )
    .await;
    let starts = |calls: &[Value]| {
        calls
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count()
    };
    assert_eq!(starts(&ac) + starts(&bc), 1);
    assert_eq!(starts(&ac) == 1, winner.as_str() == "race-a");
    drive_pair(
        &mut a,
        &mut b,
        intent_core::with_caller(
            caller,
            services.desktop_agent_call(ws.clone(), "endControl".into(), json!({})),
        ),
        &mut ac,
        &mut bc,
    )
    .await
    .unwrap();
    assert_eq!(
        srv.store.workspace_browser_client(&ws).await.unwrap(),
        Some(winner)
    );
    a.close(None).await.unwrap();
    b.close(None).await.unwrap();
    srv.ws.stop().await;
}
#[tokio::test]
async fn desktop_wss_consent_tickets_revoke_and_no_replay() {
    let (srv, services) = super::authenticated_devices::start_roster().await;
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
    let owner = Caller::Wire {
        principal_id: principal.id.clone(),
        host_role: HostRole::Owner,
    };
    let created = intent_core::with_caller(
        owner,
        services.agent_create(
            ws.clone(),
            Some("WSS desktop agent".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(
        created["id"]
            .as_str()
            .or_else(|| created["agent"]["id"].as_str())
            .unwrap(),
    );
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
    let mut socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let mut calls = vec![];
    let hello = json!({"clientId":"desktop-primary","capabilities":{"browserExec":true,"desktopControl":1}});
    assert!(call(&mut socket, "client.hello", hello.clone(), &mut calls)
        .await
        .get("error")
        .is_none());
    let get = call(
        &mut socket,
        "desktop.getState",
        json!({"workspaceId":ws,"agentId":agent}),
        &mut calls,
    )
    .await;
    assert_eq!(
        get["result"],
        json!({"state":{"status":"inactive"},"permission":{"computerId":"wss-physical","computerName":"WSS desktop","allowed":false}})
    );
    let subscription = call(
        &mut socket,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["desktop:*"]}),
        &mut calls,
    )
    .await;
    assert!(subscription.get("error").is_none());
    assert_eq!(
        call(
            &mut socket,
            "desktop.startControl",
            json!({"workspaceId":ws}),
            &mut calls
        )
        .await["error"]["code"],
        -32601
    );
    let caller = Caller::Agent {
        agent_id: agent.clone(),
    };
    let pending = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(pending["status"], "pending_permission");
    let buffered = calls
        .iter()
        .find(|v| {
            v["method"] == "events.event"
                && v["params"]["event"]["type"] == "desktop:permission-requested"
        })
        .cloned();
    let prompt = if let Some(prompt) = buffered {
        prompt
    } else {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let message = socket.next().await.unwrap().unwrap();
                if let Message::Text(text) = message {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "desktop:permission-requested"
                    {
                        break v;
                    }
                }
            }
        })
        .await
        .unwrap()
    };
    assert_eq!(
        prompt["params"]["event"]["data"]["requestId"],
        pending["requestId"]
    );
    assert_eq!(
        prompt["params"]["event"]["data"]["options"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(!prompt.to_string().contains("stopReportToken"));

    let denied = call(
        &mut socket,
        "desktop.respondPermission",
        json!({"workspaceId":ws,"requestId":pending["requestId"],"decision":"deny"}),
        &mut calls,
    )
    .await;
    assert_eq!(
        denied["result"],
        json!({"accepted":true,"requestId":pending["requestId"]})
    );
    // Serial agent gate waits for the accepted decision's durable outcome.
    drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "endControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &mut socket,
            "desktop.setPermission",
            json!({"workspaceId":ws,"agentId":agent,"computerId":"wss-physical","allowed":true}),
            &mut calls
        )
        .await["result"]["permission"]["allowed"],
        true
    );
    srv.store
        .set_workspace_browser_client(&ws, Some(&intent_core::ClientId::from("desktop-primary")))
        .await
        .unwrap();
    let active = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(active["alreadyGranted"], false);
    drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(
                ws.clone(),
                "click".into(),
                json!({"displayId":"d","layoutId":"l","x":5,"y":6,"button":"right","clickCount":2}),
            ),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    let start = calls
        .iter()
        .find(|p| p["operation"] == "startControl")
        .unwrap()
        .clone();
    let execute = calls.iter().find(|p| p["operation"] == "execute").unwrap();
    assert_eq!(execute["deadlineId"], "wss-ticket");
    assert!(execute.get("action").is_none());
    let report = json!({"workspaceId":ws,"sessionId":active["sessionId"],"reason":"user_stop","stopReport":{"reportId":uuid::Uuid::new_v4().to_string(),"computerId":"wss-physical","connectionEpoch":start["connectionEpoch"],"stopReportToken":start["stopReportToken"]}});
    assert_eq!(
        call(&mut socket, "desktop.revoke", report.clone(), &mut calls).await["result"],
        json!({"revoked":true,"reported":true})
    );
    let before = calls.len();
    let refused = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(
                ws.clone(),
                "type".into(),
                json!({"text":"must not execute"}),
            ),
        ),
        &mut calls,
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, "desktop-not-active");
    assert_eq!(calls.len(), before);
    socket.close(None).await.unwrap();
    let mut replacement = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    call(&mut replacement, "client.hello", hello.clone(), &mut calls).await;
    assert_eq!(
        call(&mut replacement, "desktop.revoke", report, &mut calls).await["result"],
        json!({"revoked":false,"reported":false})
    );
    let state = call(
        &mut replacement,
        "desktop.getState",
        json!({"workspaceId":ws,"agentId":agent}),
        &mut calls,
    )
    .await;
    assert_eq!(state["result"]["state"], json!({"status":"inactive"}));
    assert_eq!(
        calls.iter().filter(|p| p["operation"] == "execute").count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count(),
        1
    );
    let second = drive(
        &mut replacement,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_ne!(second["sessionId"], active["sessionId"]);
    let action = intent_core::with_caller(
        caller.clone(),
        services.desktop_agent_call(
            ws.clone(),
            "type".into(),
            json!({"text":"uncertain outcome"}),
        ),
    );
    let disconnect = async {
        loop {
            let message = replacement.next().await.unwrap().unwrap();
            if let Message::Text(text) = message {
                let request: Value = serde_json::from_str(&text).unwrap();
                if request["method"] == "desktop.control" {
                    if request["params"]["operation"] == "execute" {
                        calls.push(request["params"].clone());
                        replacement.close(None).await.unwrap();
                        break;
                    }
                    executor_reply(&mut replacement, request, &mut calls).await;
                }
            }
        }
    };
    let (outcome, ()) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(action, disconnect)
    })
    .await
    .unwrap();
    let error = outcome.unwrap_err();
    assert!(
        error.code == "desktop-outcome-unknown" || error.execution.as_deref() == Some("unknown")
    );
    let mut final_socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    call(&mut final_socket, "client.hello", hello, &mut calls).await;
    let state = call(
        &mut final_socket,
        "desktop.getState",
        json!({"workspaceId":ws,"agentId":agent}),
        &mut calls,
    )
    .await;
    assert_eq!(state["result"]["state"], json!({"status":"inactive"}));
    let refused = drive(
        &mut final_socket,
        intent_core::with_caller(
            caller,
            services.desktop_agent_call(ws.clone(), "type".into(), json!({"text":"no replay"})),
        ),
        &mut calls,
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, "desktop-not-active");
    assert_eq!(
        calls.iter().filter(|p| p["operation"] == "execute").count(),
        2
    );
    assert_eq!(
        calls
            .iter()
            .filter(|p| p["operation"] == "startControl")
            .count(),
        2
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_display_selection_errors_preserve_codes_and_do_not_execute() {
    let (srv, services) = super::authenticated_devices::start_roster().await;
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
            Some("Display agent".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(created["id"].as_str().unwrap());
    let caller = Caller::Agent {
        agent_id: agent.clone(),
    };
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
    let mut socket = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let mut calls = vec![json!({"displayCount":2})];
    call(&mut socket,"client.hello",json!({"clientId":"display-desktop","capabilities":{"browserExec":true,"desktopControl":1}}),&mut calls).await;
    srv.store
        .set_workspace_browser_client(&ws, Some(&intent_core::ClientId::from("display-desktop")))
        .await
        .unwrap();
    assert!(call(
        &mut socket,
        "desktop.setPermission",
        json!({"workspaceId":ws,"agentId":agent,"computerId":"wss-physical","allowed":true}),
        &mut calls
    )
    .await
    .get("result")
    .is_some());
    drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    let listed = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "listDisplay".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(listed["displays"].as_array().unwrap().len(), 2);
    assert_eq!(listed["layoutId"], "l");
    assert_eq!(listed["displays"][0]["displayId"], "d");
    assert_eq!(listed["displays"][1]["displayId"], "e");
    assert!(listed["displays"]
        .as_array()
        .unwrap()
        .iter()
        .all(|d| d.as_object().unwrap().len() == 6));
    let before = calls
        .iter()
        .filter(|call| call["operation"] == "execute")
        .count();
    for (method, args, code) in [
        (
            "screenshot",
            json!({}),
            "desktop-display-selection-required",
        ),
        (
            "click",
            json!({"layoutId":"l","x":1,"y":1}),
            "desktop-display-selection-required",
        ),
        (
            "scroll",
            json!({"layoutId":"l","x":1,"y":1,"deltaX":0,"deltaY":1}),
            "desktop-display-selection-required",
        ),
        (
            "drag",
            json!({"layoutId":"l","from":{"x":0,"y":0},"to":{"x":1,"y":1}}),
            "desktop-display-selection-required",
        ),
        (
            "screenshot",
            json!({"displayId":"missing"}),
            "desktop-display-unavailable",
        ),
        (
            "screenshot",
            json!({"displayId":"d","layoutId":"old"}),
            "desktop-stale-layout",
        ),
    ] {
        let error = drive(
            &mut socket,
            intent_core::with_caller(
                caller.clone(),
                services.desktop_agent_call(ws.clone(), method.into(), args),
            ),
            &mut calls,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.numeric_code(), -32602);
        assert_eq!(error.execution.as_deref(), Some("not_started"));
        if code == "desktop-display-selection-required" {
            assert!(error.detail.contains("ask the user"));
        }
    }
    // Exercise the actual model-facing JS binding, service and reverse WSS
    // together: structured native errors must remain tool failures, retaining
    // the selection guidance rather than becoming successes or generic errors.
    let bridge = intent_acp::WorkspaceMcpServer::new(srv.api.clone(), ws.clone())
        .with_caller_agent_id(Some(agent.clone()));
    for (code, expected, detail) in [
        (
            "return await ws.desktop.screenshot();",
            "desktop-display-selection-required",
            "Multiple displays are available. Call ws.desktop.listDisplay() and ask the user which screen to use, then retry with displayId.",
        ),
        (
            "return await ws.desktop.screenshot({displayId:'missing'});",
            "desktop-display-unavailable",
            "The requested display is unavailable.",
        ),
        (
            "return await ws.desktop.screenshot({displayId:'d',layoutId:'old'});",
            "desktop-stale-layout",
            "Display layout changed.",
        ),
    ] {
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_api","arguments":{"code":code,"summary":"Verify desktop display selection"}}});
        let response = drive(&mut socket, bridge.handle_message(&request), &mut calls)
            .await
            .expect("MCP tool response");
        assert_eq!(response["result"]["isError"], true, "{response}");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("model-visible failure");
        assert!(text.contains(expected), "{text}");
        assert!(text.contains(detail), "{text}");
    }
    assert_eq!(
        calls
            .iter()
            .filter(|call| call["operation"] == "execute")
            .count(),
        before
    );
    let shot = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(
                ws.clone(),
                "screenshot".into(),
                json!({"displayId":"e","layoutId":"l"}),
            ),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(shot["displays"].as_array().unwrap().len(), 1);
    assert_eq!(shot["displays"][0]["displayId"], "e");
    calls[0]["displayCount"] = 1.into();
    let single = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "screenshot".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(single["displays"][0]["displayId"], "d");
    calls[0]["displayCount"] = 0.into();
    let empty = drive(
        &mut socket,
        intent_core::with_caller(
            caller.clone(),
            services.desktop_agent_call(ws.clone(), "listDisplay".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    assert_eq!(empty["displays"], json!([]));
    drive(
        &mut socket,
        intent_core::with_caller(
            caller,
            services.desktop_agent_call(ws, "endControl".into(), json!({})),
        ),
        &mut calls,
    )
    .await
    .unwrap();
    socket.close(None).await.unwrap();
    srv.ws.stop().await;
}
