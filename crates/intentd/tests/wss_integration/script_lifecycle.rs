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
