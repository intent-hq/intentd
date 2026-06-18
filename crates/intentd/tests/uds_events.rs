//! Integration test for the M2.3 UDS event fast-path: a client subscribes,
//! receives pushed `events.event` notifications for matching published events,
//! unsubscribes, and a dropped connection releases its subscriptions (§6).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{ActorType, EventActor, WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::{NewEvent, Store};
use intent_transport::serve_uds;
use serde_json::Value;
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

fn new_event(event_type: &str, workspace_id: &str) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(workspace_id),
        timestamp: "2026-06-17T04:35:04.055Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some("agent-123".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        data: serde_json::json!({ "noteId": "spec", "action": "update" }),
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
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn expect_no_frame(reader: &mut BufReader<OwnedReadHalf>) {
    let mut line = String::new();
    let r = timeout(Duration::from_millis(400), reader.read_line(&mut line)).await;
    if let Ok(Ok(n)) = r {
        assert!(n == 0, "unexpected frame after unsubscribe: {line}");
    }
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    for _ in 0..100 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "subscriber_count never reached {target} (last={})",
        bus.subscriber_count()
    );
}

#[tokio::test]
async fn subscribe_push_filter_unsubscribe_and_disconnect_cleanup() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
    let socket = std::env::temp_dir().join(format!("intentd-uds-{}.sock", Uuid::new_v4()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (read_half, mut write_half) = connect_retry(&socket).await.into_split();
    let mut reader = BufReader::new(read_half);

    send(&mut write_half, r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["note:*"],"workspaceId":"ws-1"}}"#).await;
    let resp = read_json(&mut reader).await;
    assert_eq!(resp["id"], 1);
    let sub_id = resp["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sub_id.starts_with("ws-sub-"));
    wait_for_subscriber_count(&bus, 1).await;

    // Filtered push: agent:idle and the ws-2 event are dropped; only the two
    // matching note:* events in ws-1 are delivered, in order.
    bus.publish(&new_event("agent:idle", "ws-1")).await.unwrap();
    let m1 = bus
        .publish(&new_event("note:updated", "ws-1"))
        .await
        .unwrap();
    let n1 = read_json(&mut reader).await;
    assert_eq!(n1["method"], "events.event");
    assert_eq!(n1["params"]["subscriptionId"], sub_id.as_str());
    assert_eq!(n1["params"]["event"]["type"], "note:updated");
    assert_eq!(n1["params"]["event"]["id"], m1.id.as_str());
    assert_eq!(n1["params"]["event"]["workspaceId"], "ws-1");

    bus.publish(&new_event("note:created", "ws-2"))
        .await
        .unwrap();
    let m2 = bus
        .publish(&new_event("note:created", "ws-1"))
        .await
        .unwrap();
    let n2 = read_json(&mut reader).await;
    assert_eq!(n2["params"]["event"]["id"], m2.id.as_str());

    send(&mut write_half, &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"events.unsubscribe","params":{{"subscriptionId":"{sub_id}"}}}}"#)).await;
    let unsub = read_json(&mut reader).await;
    assert_eq!(unsub["id"], 2);
    assert_eq!(unsub["result"]["success"], true);
    wait_for_subscriber_count(&bus, 0).await;

    bus.publish(&new_event("note:updated", "ws-1"))
        .await
        .unwrap();
    expect_no_frame(&mut reader).await;

    // Disconnect cleanup: a fresh connection subscribes, then drops its socket.
    let (read2, mut write2) = connect_retry(&socket).await.into_split();
    let mut reader2 = BufReader::new(read2);
    send(&mut write2, r#"{"jsonrpc":"2.0","id":9,"method":"events.subscribe","params":{"eventTypes":["note:*"]}}"#).await;
    let _ = read_json(&mut reader2).await;
    wait_for_subscriber_count(&bus, 1).await;
    drop(write2);
    drop(reader2);
    wait_for_subscriber_count(&bus, 0).await;

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
