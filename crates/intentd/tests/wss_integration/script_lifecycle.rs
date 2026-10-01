//! Archive/restore envelopes and invalidations through real authenticated TLS.
use super::*;
use serde_json::json;

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn frame(ws: &mut Ws) -> Value {
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(s))) => return serde_json::from_str(&s).unwrap(),
                Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("WebSocket closed: {other:?}"),
            }
        }
    })
    .await
    .expect("WSS frame deadline")
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    loop {
        let v = frame(ws).await;
        if v["id"] == id {
            assert_eq!(v["jsonrpc"], "2.0");
            return v;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn script_archive_restore_contract_over_wss() {
    let srv = start(WsOptions::default()).await;
    let mut client = connect_ws(srv.port, srv.cfg.clone()).await;
    let created = rpc(
        &mut client,
        1,
        "workspace.create",
        json!({"title":"Script history"}),
    )
    .await;
    let ws = created["result"]["workspace"]["id"].as_str().unwrap();
    let created = rpc(&mut client, 2, "script.create", json!({"workspaceId":ws,"scriptId":"retained","name":"Check","command":"true","mode":"command","purpose":"oneOff"})).await;
    assert_eq!(created["result"]["purpose"], "oneOff", "{created}");
    let mut watching = connect_ws(srv.port, srv.cfg.clone()).await;
    let sub = rpc(
        &mut watching,
        3,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["script:changed"]}),
    )
    .await;
    assert!(sub.get("error").is_none(), "{sub}");
    let archived = rpc(
        &mut client,
        4,
        "script.archive",
        json!({"workspaceId":ws,"scriptIds":["retained","absent","retained"]}),
    )
    .await;
    assert_eq!(
        archived,
        json!({"jsonrpc":"2.0","id":4,"result":{"archived":["retained"],"skipped":[{"scriptId":"absent","reason":"notFound"}]}})
    );
    loop {
        let event = frame(&mut watching).await;
        if event["method"] == "events.event" {
            assert_eq!(event["params"]["event"]["type"], "script:changed");
            assert_eq!(
                event["params"]["event"]["data"],
                json!({"scriptId":"retained","action":"updated"})
            );
            break;
        }
    }
    let active = rpc(
        &mut client,
        5,
        "script.list",
        json!({"workspaceId":ws,"archive":"active"}),
    )
    .await;
    assert_eq!(active["result"], json!({"scripts":[]}));
    let legacy = rpc(&mut client, 6, "script.list", json!({"workspaceId":ws})).await;
    assert_eq!(legacy["result"]["scripts"][0]["id"], "retained");
    assert!(legacy["result"]["scripts"][0]["archivedAt"].is_string());
    let history = rpc(
        &mut client,
        7,
        "script.list",
        json!({"workspaceId":ws,"archive":"archived"}),
    )
    .await;
    assert_eq!(history["result"], legacy["result"]);
    for (id, method, params) in [
        (8, "script.list", json!({"workspaceId":ws,"archive":null})),
        (
            9,
            "script.create",
            json!({"workspaceId":ws,"name":"bad","command":"true","mode":"command","purpose":null}),
        ),
        (
            10,
            "script.archive",
            json!({"workspaceId":ws,"scriptIds":[]}),
        ),
        (
            11,
            "script.restore",
            json!({"workspaceId":ws,"scriptIds":["retained",""]}),
        ),
    ] {
        assert_eq!(
            rpc(&mut client, id, method, params).await["error"]["code"],
            -32602
        );
    }
    let restored = rpc(
        &mut client,
        12,
        "script.restore",
        json!({"workspaceId":ws,"scriptIds":["retained","absent"]}),
    )
    .await;
    assert_eq!(
        restored,
        json!({"jsonrpc":"2.0","id":12,"result":{"restored":["retained"],"skipped":[{"scriptId":"absent","reason":"notFound"}]}})
    );
    let status = rpc(
        &mut client,
        13,
        "script.status",
        json!({"workspaceId":ws,"scriptId":"retained"}),
    )
    .await;
    assert_eq!(status["result"]["status"], "idle");
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn script_default_one_off_settlement_and_output_over_wss() {
    let srv = start(WsOptions::default()).await;
    let mut client = connect_ws(srv.port, srv.cfg.clone()).await;
    let hello = rpc(
        &mut client,
        0,
        "client.hello",
        json!({"clientId":"script-lifecycle-e2e","clientType":"web"}),
    )
    .await;
    assert_eq!(
        hello["result"]["protocolVersion"],
        intent_transport::PROTOCOL_VERSION
    );
    assert_eq!(
        hello["result"]["server"]["protocolVersion"],
        intent_transport::PROTOCOL_VERSION
    );
    assert_eq!(
        hello["result"]["server"]["capabilities"]["scriptLifecycle"], 1,
        "{hello}"
    );
    let created = rpc(
        &mut client,
        1,
        "workspace.create",
        json!({"title":"One-off results"}),
    )
    .await;
    let ws = created["result"]["workspace"]["id"].as_str().unwrap();
    let mut watching = connect_ws(srv.port, srv.cfg.clone()).await;
    rpc(
        &mut watching,
        2,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["script:state","script:changed","script:output"]}),
    )
    .await;
    for (sid, command, outcome, code) in [
        ("success", "printf retained", "succeeded", 0),
        ("failure", "exit 9", "failed", 9),
    ] {
        let def = rpc(&mut client,3,"script.create",json!({"workspaceId":ws,"scriptId":sid,"name":"check","mode":"command","command":command})).await;
        assert_eq!(def["result"]["purpose"], "oneOff");
        let run = rpc(
            &mut client,
            4,
            "script.run",
            json!({"workspaceId":ws,"scriptId":sid,"timeoutSeconds":5}),
        )
        .await;
        assert_eq!(run["result"]["exitCode"], code, "{run}");
        assert_eq!(run["result"]["timedOut"], false);
        let mut exited = false;
        loop {
            let event = frame(&mut watching).await;
            let event = &event["params"]["event"];
            if event["data"]["scriptId"] != sid {
                continue;
            }
            if event["type"] == "script:state" && event["data"]["status"] == "exited" {
                assert_eq!(event["data"]["exitCode"], code);
                exited = true;
            }
            if event["type"] == "script:changed" && event["data"]["action"] == "updated" {
                assert!(exited, "archive invalidation follows final state");
                break;
            }
        }
        let active = rpc(
            &mut client,
            5,
            "script.list",
            json!({"workspaceId":ws,"archive":"active"}),
        )
        .await;
        assert_eq!(active["result"], json!({"scripts":[]}));
        let all = rpc(&mut client, 6, "script.list", json!({"workspaceId":ws})).await;
        let row = all["result"]["scripts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == sid)
            .unwrap();
        assert!(row["archivedAt"].is_string());
        assert_eq!(row["lastRun"]["outcome"], outcome);
        assert_eq!(row["lastRun"]["exitCode"], code);
        assert!(row["lastRun"]["startedAt"].is_string() && row["lastRun"]["stoppedAt"].is_string());
        assert_eq!(
            rpc(
                &mut client,
                7,
                "script.status",
                json!({"workspaceId":ws,"scriptId":sid})
            )
            .await["result"]["status"],
            "exited"
        );
        let output = rpc(
            &mut client,
            8,
            "script.output",
            json!({"workspaceId":ws,"scriptId":sid}),
        )
        .await;
        assert!(output["result"].is_string());
        if code == 0 {
            assert!(output["result"].as_str().unwrap().contains("retained"));
        }
    }
    srv.ws.stop().await;
}

/// Drive the public MCP tools/call + JS binding and WSS router against the
/// same real services/store. Neither caller may replace omission with Saved.
#[intent_test_macros::daemon_test]
async fn script_creation_defaults_through_mcp_and_wss() {
    let srv = start(WsOptions::default()).await;
    let mut client = connect_ws(srv.port, srv.cfg.clone()).await;
    let created = rpc(
        &mut client,
        1,
        "workspace.create",
        json!({"title":"Creation defaults"}),
    )
    .await;
    let ws = created["result"]["workspace"]["id"].as_str().unwrap();
    let mcp = intent_acp::WorkspaceMcpServer::new(srv.api.clone(), WorkspaceId::from_string(ws));
    for via_mcp in [false, true] {
        for (suffix, mode, purpose, expected) in [
            ("default", "command", None, "oneOff"),
            ("saved", "command", Some("saved"), "saved"),
            ("explicit", "command", Some("oneOff"), "oneOff"),
            ("service", "service", None, "saved"),
        ] {
            let id = format!("{via_mcp}-{suffix}");
            for updating in [false, true] {
                let mut options = json!({"scriptId":id});
                if !updating {
                    if let Some(purpose) = purpose {
                        options["purpose"] = json!(purpose);
                    }
                }
                if via_mcp {
                    let code =
                        format!("return await ws.script.create('test','true','{mode}',{options});");
                    let response = mcp.handle_message(&json!({
                        "jsonrpc":"2.0", "id":2, "method":"tools/call",
                        "params":{"name":"workspace_api", "arguments":{"code":code,"summary":"Create script regression"}}
                    })).await.unwrap();
                    assert_eq!(response["result"]["isError"], false, "{response}");
                } else {
                    options["workspaceId"] = json!(ws);
                    options["name"] = json!("test");
                    options["command"] = json!("true");
                    options["mode"] = json!(mode);
                    let response = rpc(&mut client, 2, "script.create", options).await;
                    assert_eq!(response["result"]["purpose"], expected, "{response}");
                }
                let listed = rpc(&mut client, 3, "script.list", json!({"workspaceId":ws})).await;
                let row = listed["result"]["scripts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["id"] == id)
                    .unwrap();
                assert_eq!(row["purpose"], expected, "{id}, updating={updating}");
            }
        }
    }
    let invalid = rpc(&mut client, 4, "script.create", json!({"workspaceId":ws,"name":"autostart", "command":"true", "mode":"command", "autoStart":true})).await;
    assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
    let saved = rpc(&mut client, 5, "script.create", json!({"workspaceId":ws,"name":"autostart", "command":"true", "mode":"command", "autoStart":true,"purpose":"saved"})).await;
    assert_eq!(saved["result"]["purpose"], "saved", "{saved}");
    srv.ws.stop().await;
}
