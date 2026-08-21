//! Integration test for the TB-4 subscription engine on the `note` channel:
//! `note.subscribe` returns `{ subscriptionId }`, then a `subscription.push`
//! snapshot (seq 0), then ordered `{ added, updated, removedIds }` deltas on
//! note create/update/delete. Also proves `replaceGroup` atomic-swap,
//! `note.unsubscribe` cleanup, and coexistence with the `events.subscribe`
//! firehose (PROTOCOL §6, TB-0 §1).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
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

/// Generous per-await deadline for every bounded wait in this file (each
/// frame read, connect retry, subscriber-count poll gets its own window; it is
/// not a whole-test cap). Under full-suite parallel load a scheduling/fsync
/// stall can exceed several seconds (monorepo#601: the old fixed 2s windows
/// tripped exactly there), so the deadline only bounds how long a genuinely
/// broken run takes to fail — it never delays a passing run.
const DEADLINE: Duration = Duration::from_secs(60);

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    // The whole retry loop (including any single hung connect attempt) is
    // bounded by one `timeout`; `Timeout` polls the inner future before the
    // deadline check, so a stall spanning a sleep still gets a final attempt.
    timeout(DEADLINE, async {
        loop {
            if let Ok(s) = UnixStream::connect(socket).await {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("could not connect to {}", socket.display()))
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(DEADLINE, reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    timeout(DEADLINE, async {
        while bus.subscriber_count() != target {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "subscriber_count never reached {target} (last={})",
            bus.subscriber_count()
        )
    });
}

/// Issue one JSON-RPC request on a dedicated (non-subscribed) connection and
/// return its `result` object.
async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

fn boot(
    bus: &EventBus,
) -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
    let socket = sock_dir.path().join("uds.sock");
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn intent_core::WorkspaceApi> = Arc::new(
        Services::new(bus.store().clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
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
    (socket, server, shutdown_tx, ws_root, sock_dir)
}

#[tokio::test]
async fn note_subscribe_snapshot_then_ordered_deltas() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

    // RPC connection (mutations + their responses only).
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = tokio::io::BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();
    let a = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note A" }),
    )
    .await;
    let note_a = a["note"]["id"].as_str().unwrap().to_string();

    // Subscriber connection: subscribe → response → snapshot (seq 0).
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(&mut sub_write, &format!(r#"{{"jsonrpc":"2.0","id":1,"method":"note.subscribe","params":{{"workspaceId":"{ws_id}"}}}}"#)).await;
    let resp = read_json(&mut sub_reader).await;
    assert_eq!(resp["id"], 1);
    let sub_id = resp["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sub_id.starts_with("ws-sub-"));

    let snap = read_json(&mut sub_reader).await;
    assert_eq!(snap["method"], "subscription.push");
    assert_eq!(snap["params"]["subscriptionId"], sub_id.as_str());
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    let arr = snap["params"]["snapshot"]
        .as_array()
        .expect("snapshot array");
    let found = arr
        .iter()
        .find(|n| n["id"] == note_a.as_str())
        .expect("note A in snapshot");
    assert_eq!(found["title"], "Note A");
    assert!(found["rev"].is_number(), "rev echoed on snapshot entity");

    // note.create → delta seq 1: added.
    let b = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note B" }),
    )
    .await;
    let note_b = b["note"]["id"].as_str().unwrap().to_string();
    let d1 = read_json(&mut sub_reader).await;
    assert_eq!(d1["params"]["kind"], "delta");
    assert_eq!(d1["params"]["seq"], 1);
    assert_eq!(d1["params"]["delta"]["added"][0]["id"], note_b.as_str());
    let rev_added = d1["params"]["delta"]["added"][0]["rev"].as_i64().unwrap();

    // note.update → delta seq 2: updated, with a bumped rev (TB-1).
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        13,
        "note.update",
        json!({ "workspaceId": ws_id, "noteId": note_b, "content": "hi" }),
    )
    .await;
    let d2 = read_json(&mut sub_reader).await;
    assert_eq!(d2["params"]["kind"], "delta");
    assert_eq!(d2["params"]["seq"], 2);
    assert_eq!(d2["params"]["delta"]["updated"][0]["id"], note_b.as_str());
    let rev_updated = d2["params"]["delta"]["updated"][0]["rev"].as_i64().unwrap();
    assert!(
        rev_updated > rev_added,
        "rev bumped on update ({rev_added} -> {rev_updated})"
    );

    // note.delete → delta seq 3: removedIds.
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        14,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": note_b }),
    )
    .await;
    let d3 = read_json(&mut sub_reader).await;
    assert_eq!(d3["params"]["kind"], "delta");
    assert_eq!(d3["params"]["seq"], 3);
    assert_eq!(d3["params"]["delta"]["removedIds"][0], note_b.as_str());

    // note.unsubscribe frees the bus subscription.
    wait_for_subscriber_count(&bus, 1).await;
    send(&mut sub_write, &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"note.unsubscribe","params":{{"subscriptionId":"{sub_id}"}}}}"#)).await;
    let unsub = read_json(&mut sub_reader).await;
    assert_eq!(unsub["id"], 2);
    assert_eq!(unsub["result"]["success"], true);
    wait_for_subscriber_count(&bus, 0).await;

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn replace_group_swaps_and_firehose_coexists() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = tokio::io::BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    // The firehose still works: a separate connection subscribes via events.subscribe.
    let (fh_read, mut fh_write) = connect_retry(&socket).await.into_split();
    let mut fh_reader = tokio::io::BufReader::new(fh_read);
    send(&mut fh_write, &format!(r#"{{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{{"eventTypes":["note:*"],"workspaceId":"{ws_id}"}}}}"#)).await;
    let _ = read_json(&mut fh_reader).await;

    // Two note.subscribe in the same replaceGroup over one connection: the
    // second atomically drops the first (subscriber_count stays at 1 for notes).
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(&mut sub_write, &format!(r#"{{"jsonrpc":"2.0","id":1,"method":"note.subscribe","params":{{"workspaceId":"{ws_id}","replaceGroup":"note:{ws_id}"}}}}"#)).await;
    let r1 = read_json(&mut sub_reader).await;
    let first = r1["result"]["subscriptionId"].as_str().unwrap().to_string();
    let _ = read_json(&mut sub_reader).await; // snapshot of first
    wait_for_subscriber_count(&bus, 2).await; // firehose + first note sub

    send(&mut sub_write, &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"note.subscribe","params":{{"workspaceId":"{ws_id}","replaceGroup":"note:{ws_id}"}}}}"#)).await;
    let r2 = read_json(&mut sub_reader).await;
    let second = r2["result"]["subscriptionId"].as_str().unwrap().to_string();
    assert_ne!(first, second);
    let snap2 = read_json(&mut sub_reader).await; // snapshot of second
    assert_eq!(snap2["params"]["kind"], "snapshot");
    assert_eq!(snap2["params"]["subscriptionId"], second.as_str());
    wait_for_subscriber_count(&bus, 2).await; // firehose + replacement (prior dropped)

    // A mutation: the firehose sees events.event; the replacement sub sees a delta.
    let c = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        20,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "C" }),
    )
    .await;
    let note_c = c["note"]["id"].as_str().unwrap().to_string();
    let fh = read_json(&mut fh_reader).await;
    assert_eq!(fh["method"], "events.event");
    assert_eq!(fh["params"]["event"]["type"], "note:created");
    let delta = read_json(&mut sub_reader).await;
    assert_eq!(delta["params"]["subscriptionId"], second.as_str());
    assert_eq!(delta["params"]["delta"]["added"][0]["id"], note_c.as_str());

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
