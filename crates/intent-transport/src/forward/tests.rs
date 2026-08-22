//! Unit tests for the `forward.*` port-forwarding fast-path (§5.14).

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::*;

/// Bind a loopback echo server and return its port. Each accepted connection is
/// echoed byte-for-byte, standing in for a detected remote dev server.
async fn spawn_echo() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 || sock.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

fn parsed(frame: Option<String>) -> Value {
    serde_json::from_str(&frame.expect("a response frame")).unwrap()
}

#[tokio::test]
async fn create_lists_forwards_and_round_trips_bytes() {
    let echo_port = spawn_echo().await;
    let mut reg = ForwardRegistry::default();

    let create = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "forward.create",
        "params": { "remotePort": echo_port }
    }))
    .unwrap();
    let result = parsed(handle(create, &mut reg, false).await);
    assert_eq!(result["id"], 1);
    let forward_id = result["result"]["forwardId"].as_str().unwrap().to_string();
    let local_port =
        u16::try_from(result["result"]["localPort"].as_u64().unwrap()).expect("value fits in u16");
    assert_eq!(
        u16::try_from(result["result"]["remotePort"].as_u64().unwrap()).expect("value fits in u16"),
        echo_port
    );
    assert_ne!(local_port, 0, "an ephemeral local port is bound");

    // The tunnel splices the local port to the remote echo server.
    let mut client = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    // `forward.list` reflects the active forward.
    let list = classify(&json!({ "jsonrpc": "2.0", "id": 2, "method": "forward.list" })).unwrap();
    let listed = parsed(handle(list, &mut reg, false).await);
    let forwards = listed["result"]["forwards"].as_array().unwrap();
    assert_eq!(forwards.len(), 1);
    assert_eq!(forwards[0]["forwardId"], forward_id);
    assert_eq!(
        u16::try_from(forwards[0]["localPort"].as_u64().unwrap()).expect("value fits in u16"),
        local_port
    );

    // `forward.close` tears it down; the list is then empty.
    let close = classify(&json!({
        "jsonrpc": "2.0", "id": 3, "method": "forward.close",
        "params": { "forwardId": forward_id }
    }))
    .unwrap();
    let closed = parsed(handle(close, &mut reg, false).await);
    assert_eq!(closed["result"]["ok"], true);
    let list = classify(&json!({ "jsonrpc": "2.0", "id": 4, "method": "forward.list" })).unwrap();
    let listed = parsed(handle(list, &mut reg, false).await);
    assert!(listed["result"]["forwards"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn local_create_is_a_metadata_only_no_op() {
    // On a local connection forwarding is unnecessary (§5.14): the local port
    // equals the remote port and no listener is bound.
    let mut reg = ForwardRegistry::default();
    let create = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "forward.create",
        "params": { "remotePort": 3000 }
    }))
    .unwrap();
    let result = parsed(handle(create, &mut reg, true).await);
    assert_eq!(result["result"]["localPort"].as_u64().unwrap(), 3000);
    assert_eq!(result["result"]["remotePort"].as_u64().unwrap(), 3000);
}

#[tokio::test]
async fn create_requires_remote_port_and_close_requires_id() {
    let mut reg = ForwardRegistry::default();
    let create =
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "forward.create" })).unwrap();
    let err = parsed(handle(create, &mut reg, false).await);
    assert_eq!(err["error"]["code"], -32602);
    assert_eq!(err["error"]["data"]["code"], "invalid-params");

    let close = classify(&json!({ "jsonrpc": "2.0", "id": 2, "method": "forward.close" })).unwrap();
    let err = parsed(handle(close, &mut reg, false).await);
    assert_eq!(err["error"]["code"], -32602);
    assert_eq!(err["error"]["data"]["code"], "invalid-params");
}

#[test]
fn classify_only_matches_forward_methods() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "forward.create" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "forward.list" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "forward.close" })).is_some());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.status" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "forward.list" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": [1], "method": "forward.list" })).is_none());
}

#[tokio::test]
async fn notification_create_gets_no_response() {
    let mut reg = ForwardRegistry::default();
    let req = classify(&json!({
        "jsonrpc": "2.0", "method": "forward.create", "params": { "remotePort": 3000 }
    }))
    .unwrap();
    assert!(handle(req, &mut reg, true).await.is_none());
}
