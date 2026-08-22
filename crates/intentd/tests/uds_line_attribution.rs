//! Integration test for `note.lineAttribution.*` (PROTOCOL §5.2.1): drives the
//! wire methods over UDS end-to-end and asserts that `computeNow` persists the
//! FE-parity payload and publishes a `line-attribution:updated` event whose
//! `data.attributions` matches what `line-attribution.load` then returns.

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

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(common::rpc_read_timeout(), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

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

/// Read frames until we see the expected `events.event` notification for
/// `line-attribution:updated`, ignoring other pushed events (e.g. `note:*`).
async fn next_line_attribution_event(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for line-attribution:updated"
        );
        let mut line = String::new();
        let n = timeout(remaining, reader.read_line(&mut line))
            .await
            .expect("read timed out")
            .expect("read failed");
        assert!(n > 0, "connection closed while waiting for event");
        let v: Value = serde_json::from_str(line.trim_end()).expect("invalid JSON frame");
        if v["method"] == json!("events.event")
            && v["params"]["event"]["type"] == json!("line-attribution:updated")
        {
            return v["params"]["event"].clone();
        }
    }
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
async fn line_attribution_compute_now_persists_and_emits_event() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

    // RPC connection (mutations + their responses).
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "LA-WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();
    let note = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Attribution Note",
            "content": "Line 1\nLine 2\nLine 3",
        }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().unwrap().to_string();

    // Load before compute: expect JSON null since nothing has been persisted yet.
    let before = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "note.lineAttribution.load",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(before.is_null(), "expected null, got {before}");

    // Subscribe on a dedicated connection so the RPC channel stays clean.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{{"eventTypes":["line-attribution:updated"],"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let resp = read_json(&mut sub_reader).await;
    assert_eq!(resp["id"], 1);
    let sub_id = resp["result"]["subscriptionId"]
        .as_str()
        .expect("subscription id")
        .to_string();
    assert!(sub_id.starts_with("ws-sub-"));

    // Force an immediate compute (bypasses the debounce).
    let compute = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        13,
        "note.lineAttribution.computeNow",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(compute, json!({ "ok": true }));

    // The subscriber must see one `line-attribution:updated` notification
    // whose payload matches the FE `LineAttributionData`.
    let evt = next_line_attribution_event(&mut sub_reader).await;
    assert_eq!(evt["type"], "line-attribution:updated");
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    let data = &evt["data"];
    assert_eq!(data["workspaceId"], ws_id.as_str());
    assert_eq!(data["noteId"], note_id.as_str());
    let attributions = data["attributions"]
        .as_object()
        .expect("attributions object");
    for key in ["1", "2", "3"] {
        let entry = attributions.get(key).expect("line entry");
        assert!(entry["timestamp"].is_number(), "line {key}: {entry}");
        let author = &entry["author"];
        // `note.create` over the JSON-RPC (FE) path resolves to the `user`
        // author (reference parity with `notes.service.ts`).
        assert_eq!(author["type"], "user");
        assert!(author["id"].is_string());
    }

    // Regression for monorepo#720 (finding 2): the event is broadcast-only
    // (transient publish path), so no `line-attribution:updated` row may land
    // in the durable `event` table.
    let persisted = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        15,
        "event.query",
        json!({ "workspaceId": ws_id, "eventType": "line-attribution:updated" }),
    )
    .await;
    assert_eq!(
        persisted,
        json!([]),
        "line-attribution:updated must not be persisted"
    );

    // A follow-up load returns the persisted snapshot (FE-parity shape).
    let after = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        14,
        "note.lineAttribution.load",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(after["noteId"], note_id.as_str());
    assert_eq!(after["workspaceId"], ws_id.as_str());
    assert!(after["computedAt"].is_string());
    let after_map = after["attributions"]
        .as_object()
        .expect("attributions on load");
    assert_eq!(after_map.len(), attributions.len());

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
