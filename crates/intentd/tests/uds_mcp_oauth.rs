//! Over-the-wire `mcp.oauth.*` slice (PROTOCOL §5.22 companion): drive
//! `mcp.oauth.list/get/set/delete` against the daemon over a temp UDS, proving
//! camelCase shapes, `-32602` on missing `serverId`, and — most importantly —
//! that a stored OAuth bag NEVER crosses the wire: every response is
//! presence-only. Uses a dummy bag literal so tests can assert its absence
//! from every response frame.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

/// Dummy bag literal: any response that leaks it fails the test.
const DUMMY: &str = "dummy-oauth-bag-marker-DO-NOT-ECHO";

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-oauth-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(w: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    w.write_all(frame.as_bytes()).await.unwrap();
    w.write_all(b"\n").await.unwrap();
    w.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn call(
    w: &mut (impl AsyncWriteExt + Unpin),
    r: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(w, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(r).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    resp
}

async fn rpc(
    w: &mut (impl AsyncWriteExt + Unpin),
    r: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let resp = call(w, r, id, method, params).await;
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

fn assert_no_dummy(v: &Value, ctx: &str) {
    assert!(
        !serde_json::to_string(v).unwrap().contains(DUMMY),
        "dummy bag literal leaked in {ctx}: {v}"
    );
}

#[tokio::test]
async fn mcp_oauth_round_trip_never_echoes_bag_and_validates_params() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Shortened prefix so the full path stays under the macOS UDS 104-char cap.
    let socket = std::env::temp_dir().join(format!("id-oa-{}.sock", Uuid::new_v4()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (rr, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rr);

    // list — empty on a fresh store.
    let list = rpc(&mut w, &mut r, 1, "mcp.oauth.list", json!({})).await;
    assert_eq!(list["tokens"].as_array().unwrap().len(), 0);

    // get for an unknown server → { serverId, value: null }.
    let got = rpc(
        &mut w,
        &mut r,
        2,
        "mcp.oauth.get",
        json!({ "serverId": "ghost" }),
    )
    .await;
    assert_eq!(got, json!({ "serverId": "ghost", "value": Value::Null }));

    // Missing serverId → -32602.
    let bad = call(&mut w, &mut r, 3, "mcp.oauth.get", json!({})).await;
    assert_eq!(bad["error"]["code"], -32602);
    let bad = call(
        &mut w,
        &mut r,
        4,
        "mcp.oauth.set",
        json!({ "tokenBag": {"x": 1} }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602);
    let bad = call(&mut w, &mut r, 5, "mcp.oauth.delete", json!({})).await;
    assert_eq!(bad["error"]["code"], -32602);
    let bad = call(
        &mut w,
        &mut r,
        6,
        "mcp.oauth.set",
        json!({ "serverId": "srv" }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "missing tokenBag rejected");

    // set stores the bag but the response only carries presence.
    let bag = json!({
        "access_token": DUMMY,
        "refresh_token": DUMMY,
        "expires_at": 1_700_000_000_u64,
        "token_type": "Bearer",
    });
    let set = rpc(
        &mut w,
        &mut r,
        7,
        "mcp.oauth.set",
        json!({ "serverId": "srv-1", "tokenBag": bag }),
    )
    .await;
    assert_eq!(set["serverId"], json!("srv-1"));
    assert!(set["value"].is_string());
    assert_no_dummy(&set, "mcp.oauth.set response");

    // get after set → placeholder value, no bag leak.
    let got = rpc(
        &mut w,
        &mut r,
        8,
        "mcp.oauth.get",
        json!({ "serverId": "srv-1" }),
    )
    .await;
    assert_eq!(got["serverId"], json!("srv-1"));
    assert!(got["value"].is_string());
    assert_no_dummy(&got, "mcp.oauth.get response");

    // list after set → one entry with presence only.
    let list = rpc(&mut w, &mut r, 9, "mcp.oauth.list", json!({})).await;
    assert_no_dummy(&list, "mcp.oauth.list response");
    let arr = list["tokens"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["serverId"], json!("srv-1"));
    assert!(arr[0]["value"].is_string());

    // A second bag on a different server keeps list sorted by serverId.
    rpc(
        &mut w,
        &mut r,
        10,
        "mcp.oauth.set",
        json!({ "serverId": "srv-0", "tokenBag": { "access_token": DUMMY } }),
    )
    .await;
    let list = rpc(&mut w, &mut r, 11, "mcp.oauth.list", json!({})).await;
    assert_no_dummy(&list, "mcp.oauth.list after second set");
    let arr = list["tokens"].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["serverId"], json!("srv-0"));
    assert_eq!(arr[1]["serverId"], json!("srv-1"));

    // delete removes the bag; second delete is idempotent success.
    let del = rpc(
        &mut w,
        &mut r,
        12,
        "mcp.oauth.delete",
        json!({ "serverId": "srv-1" }),
    )
    .await;
    assert_eq!(del, json!({ "success": true }));
    let del = rpc(
        &mut w,
        &mut r,
        13,
        "mcp.oauth.delete",
        json!({ "serverId": "srv-1" }),
    )
    .await;
    assert_eq!(del, json!({ "success": true }));
    let got = rpc(
        &mut w,
        &mut r,
        14,
        "mcp.oauth.get",
        json!({ "serverId": "srv-1" }),
    )
    .await;
    assert_eq!(got["value"], Value::Null);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
