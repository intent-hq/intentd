//! Integration test for the TB-4 subscription engine on the `note` channel:
//! `note.subscribe` returns `{ subscriptionId }`, then a `subscription.push`
//! snapshot (seq 0), then ordered `{ added, updated, removedIds }` deltas on
//! note create/update/delete. Also proves `replaceGroup` atomic-swap,
//! `note.unsubscribe` cleanup, and coexistence with the `events.subscribe`
//! firehose (PROTOCOL §6, TB-0 §1).

#![cfg(unix)]

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

struct TempDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        let dir = common::test_tempdir("intentd-uds-");
        let path = dir.path().join("intentd.db");
        Self { _dir: dir, path }
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
    let server = intent_core::spawn_daemon({
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

#[intent_test_macros::daemon_test]
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

#[intent_test_macros::daemon_test]
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

#[intent_test_macros::daemon_test]
async fn note_delete_grace_uds_events_errors_scope_and_legacy_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);
    let (rd, mut wr) = connect_retry(&socket).await.into_split();
    let mut rd = BufReader::new(rd);
    let hello = rpc(&mut wr, &mut rd, 1, "client.hello", json!({})).await;
    assert_eq!(hello["server"]["capabilities"]["noteDeleteGrace"], 1);
    let mut fixtures = Vec::new();
    for i in 0..2 {
        let workspace = rpc(
            &mut wr,
            &mut rd,
            10 + i,
            "workspace.create",
            json!({"title":format!("grace-{i}")}),
        )
        .await;
        let ws = workspace["workspace"]["id"].as_str().unwrap().to_string();
        let created = rpc(
            &mut wr,
            &mut rd,
            20 + i,
            "note.create",
            json!({"workspaceId":ws,"title":"kept","content":"😀 exact\r\nbody"}),
        )
        .await;
        let note = created["note"]["id"].as_str().unwrap().to_string();
        let status = rpc(
            &mut wr,
            &mut rd,
            30 + i,
            "note.deleteStatus",
            json!({"workspaceId":ws,"noteId":note}),
        )
        .await;
        let key = json!({"epoch":status["epoch"],"issuedTickMs":status["serverTickMs"],"nonce":uuid::Uuid::new_v4().to_string()});
        let schedule = json!({"workspaceId":ws,"noteId":note,"operationKey":key,"noteInstanceId":status["current"]["noteInstanceId"],"sourceRevision":status["current"]["sourceRevision"],"expectedVersion":status["current"]["revision"],"undoDelayMs":60000});
        fixtures.push((ws, note, key, schedule));
    }
    let (er, mut ew) = connect_retry(&socket).await.into_split();
    let mut er = BufReader::new(er);
    rpc(
        &mut ew,
        &mut er,
        40,
        "events.subscribe",
        json!({"workspaceId":fixtures[0].0,"eventTypes":["note:delete-operation"]}),
    )
    .await;
    // Reject malformed controls before any destructive admission. Unknown old
    // methods have no fallback to legacy immediate note.delete.
    for (method, params, code) in [
        (
            "note.deleteSchedule",
            {
                let mut p = fixtures[0].3.clone();
                p["undoDelayMs"] = json!(60001);
                p
            },
            -32602,
        ),
        (
            "note.deleteSchedule",
            {
                let mut p = fixtures[0].3.clone();
                p["expectedVersion"] = json!(9_007_199_254_740_992_u64);
                p
            },
            -32602,
        ),
        ("note.deleteFutureUnknown", fixtures[0].3.clone(), -32601),
    ] {
        send(
            &mut wr,
            &json!({"jsonrpc":"2.0","id":50,"method":method,"params":params}).to_string(),
        )
        .await;
        let response = read_json(&mut rd).await;
        assert_eq!(response["error"]["code"], code);
    }
    rpc(
        &mut wr,
        &mut rd,
        60,
        "note.deleteSchedule",
        fixtures[1].3.clone(),
    )
    .await;
    // Long JSON-RPC ids remain transport-owned; the grace 512KiB limit is on
    // result only, not a new 128-byte request-id constraint.
    let long_id = "i".repeat(1024);
    send(&mut wr,&json!({"jsonrpc":"2.0","id":long_id,"method":"note.deleteSchedule","params":fixtures[0].3}).to_string()).await;
    let scheduled = read_json(&mut rd).await;
    assert_eq!(scheduled["id"], long_id);
    assert!(scheduled.get("error").is_none());
    assert!(serde_json::to_vec(&scheduled["result"]).unwrap().len() < 524_288);
    let event = read_json(&mut er).await;
    assert_eq!(event["method"], "events.event");
    assert_eq!(event["params"]["event"]["type"], "note:delete-operation");
    let data = &event["params"]["event"]["data"];
    assert_eq!(data["workspaceId"], fixtures[0].0);
    assert_eq!(data["operationKey"], fixtures[0].2);
    assert_eq!(data["state"], "PENDING");
    assert_eq!(data.as_object().unwrap().len(), 8);
    let snapshot = rpc(
        &mut wr,
        &mut rd,
        61,
        "note.deleteStatus",
        json!({"workspaceId":fixtures[0].0}),
    )
    .await;
    assert_eq!(snapshot["pending"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pending"][0]["canCancel"], true);
    assert!(snapshot["sequence"].as_u64().unwrap() >= data["sequence"].as_u64().unwrap());
    for (ws, note, key, _) in &fixtures {
        let cancelled = rpc(
            &mut wr,
            &mut rd,
            70,
            "note.deleteCancel",
            json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
        )
        .await;
        assert_eq!(cancelled["operation"]["state"], "CANCELLED");
        timeout(DEADLINE, async {
            loop {
                let status = rpc(
                    &mut wr,
                    &mut rd,
                    71,
                    "note.deleteStatus",
                    json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
                )
                .await;
                if status["operation"]["expiresTickMs"].is_number() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let note = rpc(
            &mut wr,
            &mut rd,
            72,
            "note.get",
            json!({"workspaceId":ws,"noteId":note}),
        )
        .await;
        assert_eq!(note["note"]["content"], "😀 exact\r\nbody");
    }
    let cancelled = read_json(&mut er).await;
    assert_eq!(cancelled["params"]["event"]["data"]["state"], "CANCELLED");
    // Legacy delete remains immediate, without a grace operation or recreate.
    rpc(
        &mut wr,
        &mut rd,
        80,
        "note.delete",
        json!({"workspaceId":fixtures[0].0,"noteId":fixtures[0].1}),
    )
    .await;
    let absent = rpc(
        &mut wr,
        &mut rd,
        81,
        "note.deleteStatus",
        json!({"workspaceId":fixtures[0].0,"noteId":fixtures[0].1}),
    )
    .await;
    assert!(absent["current"].is_null());
    assert_eq!(absent["pending"], json!([]));
    drop(wr);
    drop(rd);
    drop(ew);
    drop(er);
    shutdown_tx.send(()).unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap();
    bus.store().close().await;
}
