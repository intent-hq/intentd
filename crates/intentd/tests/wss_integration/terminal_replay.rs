//! Cursor fields through authenticated TLS WebSocket RPC and event delivery.
use super::*;
use base64::Engine as _;

async fn rpc(client: &mut PresenceClient, method: &str, params: Value) -> Value {
    let response = client.call(1, method, params).await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}

fn range(value: &Value, field: &str) -> (Vec<u8>, u64, u64) {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value[field].as_str().unwrap())
        .unwrap();
    let start = value["startOffset"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let end = value["endOffset"].as_str().unwrap().parse::<u64>().unwrap();
    assert_eq!(end - start, bytes.len() as u64);
    assert!(!value["daemonBootId"].as_str().unwrap().is_empty());
    (bytes, start, end)
}

#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn terminal_cursor_snapshot_event_reconnect_and_restart() {
    let srv = start(WsOptions::default()).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let mut c = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["terminal:data"]}),
    )
    .await;
    let created = rpc(
        &mut c,
        "terminal.create",
        json!({"workspaceId":ws,"command":"/bin/cat","cwd":srv.dir.path()}),
    )
    .await;
    let terminal = created["terminalId"].clone();
    let list = rpc(&mut c, "terminal.list", json!({"workspaceId":ws})).await;
    let boot = list["daemonBootId"].clone();
    let empty = rpc(
        &mut c,
        "terminal.getBuffer",
        json!({"terminalId":terminal,"maxBytes":0}),
    )
    .await;
    assert_eq!(range(&empty, "data"), (vec![], 0, 0));
    assert_eq!(empty["daemonBootId"], boot);
    // One explicit input write. Reconnect and snapshots below must never repeat it.
    rpc(&mut c,"terminal.write",json!({"terminalId":terminal,"data":base64::engine::general_purpose::STANDARD.encode(b"same\n")})).await;
    let mut output = Vec::new();
    let mut end = 0;
    // PTY echo and cat output each render the line once.
    while output.len() < b"same\r\nsame\r\n".len() {
        let event = c.event("terminal:data").await;
        let data = &event["data"];
        assert_eq!(data["terminalId"], terminal);
        assert_eq!(data["daemonBootId"], boot);
        let (bytes, start, next) = range(data, "chunk");
        assert_eq!(start, end);
        output.extend(bytes);
        end = next;
    }
    assert_eq!(output, b"same\r\nsame\r\n");
    c.close().await;
    let mut c = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    rpc(
        &mut c,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["terminal:data"]}),
    )
    .await;
    for cap in [None, Some(0), Some(3), Some(100), Some(-1)] {
        let mut params = json!({"terminalId":terminal});
        if let Some(cap) = cap {
            params["maxBytes"] = json!(cap);
        }
        let buffer = rpc(&mut c, "terminal.getBuffer", params).await;
        let (bytes, start, next) = range(&buffer, "data");
        assert_eq!(buffer["daemonBootId"], boot);
        assert_eq!(next, end);
        assert_eq!(bytes, output[usize::try_from(start).unwrap()..]);
    }
    // A second identical write must be new output at the next position.
    rpc(&mut c,"terminal.write",json!({"terminalId":terminal,"data":base64::engine::general_purpose::STANDARD.encode(b"same\n")})).await;
    let previous_end = end;
    while end < previous_end * 2 {
        let event = c.event("terminal:data").await;
        let (_, start, next) = range(&event["data"], "chunk");
        assert_eq!(start, end);
        end = next;
    }
    let buffer = rpc(&mut c, "terminal.getBuffer", json!({"terminalId":terminal})).await;
    assert_eq!(
        range(&buffer, "data").0,
        b"same\r\nsame\r\nsame\r\nsame\r\n"
    );
    rpc(&mut c, "terminal.kill", json!({"terminalId":terminal})).await;
    c.close().await;
    srv.ws.stop().await;

    let restarted = start(WsOptions::default()).await;
    restarted
        .store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let mut c = PresenceClient::open(restarted.port, restarted.cfg.clone(), TOKEN).await;
    let created = rpc(
        &mut c,
        "terminal.create",
        json!({"workspaceId":ws,"command":"/bin/cat","cwd":restarted.dir.path()}),
    )
    .await;
    assert_eq!(
        created["terminalId"], terminal,
        "IDs may repeat after restart"
    );
    let buffer = rpc(&mut c, "terminal.getBuffer", json!({"terminalId":terminal})).await;
    assert_ne!(buffer["daemonBootId"], boot);
    assert_eq!(range(&buffer, "data"), (vec![], 0, 0));
    rpc(&mut c, "terminal.kill", json!({"terminalId":terminal})).await;
    c.close().await;
    restarted.ws.stop().await;
}
