//! Drive the real bus subscription forwarder with a deliberately full bulk lane.
use super::*;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use intent_core::events::{TERMINAL_DATA, TERMINAL_EXIT};
use intent_core::{ActorType, EventActor};
use intent_store::{NewEvent, Store};

#[tokio::test]
async fn congested_terminal_forwarder_preserves_cursor_ranges_and_exit_order() {
    let dir = tempfile::Builder::new()
        .prefix("terminal-cursor-forwarder-")
        .tempdir()
        .unwrap();
    let store = Store::open(&dir.path().join("bus.db")).await.unwrap();
    let bus = EventBus::new(store);
    // Closing the bus flushes this whole batch without any sleep or timing race.
    let sub = bus.subscribe(SubscriptionFilter {
        batch_window: Some(std::time::Duration::from_secs(3600)),
        ..Default::default()
    });
    let publish = |event_type: &str, data: Value| {
        let _ = bus.publish_transient(&NewEvent {
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: intent_core::now_iso(),
            event_type: event_type.to_string(),
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        });
    };
    // The leading barrier occupies the sole bulk slot. The same batch's terminal
    // events must therefore pass through conflation before the exit barrier drains.
    publish(NOTE_UPDATED, json!({"id":"barrier"}));
    for (boot, start) in [("boot-1", 0), ("boot-1", 4), ("boot-1", 12), ("boot-2", 0)] {
        publish(
            TERMINAL_DATA,
            json!({"terminalId":"t-1","chunk":BASE64.encode(b"same"),"daemonBootId":boot,"startOffset":start.to_string(),"endOffset":(start+4).to_string()}),
        );
    }
    publish(TERMINAL_EXIT, json!({"terminalId":"t-1","exitCode":0}));
    drop(bus);
    let (priority, _priority_rx) = mpsc::channel(1);
    let (bulk, mut rx) = mpsc::channel(1);
    let forwarder = forward_subscription(
        sub,
        None,
        None,
        "sub-1".into(),
        OutboundSender { priority, bulk },
        false,
    );
    let reader = async {
        let mut frames = Vec::new();
        while let Some(frame) = rx.recv().await {
            frames.push(serde_json::from_str::<Value>(&frame).unwrap());
        }
        frames
    };
    let (_, frames) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(forwarder, reader)
    })
    .await
    .expect("forwarder drains without blocking");
    assert_eq!(
        frames.len(),
        5,
        "one barrier, three positioned outputs, one exit"
    );
    assert_eq!(frames[0]["params"]["event"]["type"], NOTE_UPDATED);
    for (frame, (boot, start, end, bytes)) in frames[1..4].iter().zip([
        ("boot-1", "0", "8", b"samesame".as_slice()),
        ("boot-1", "12", "16", b"same".as_slice()),
        ("boot-2", "0", "4", b"same".as_slice()),
    ]) {
        assert_eq!(frame["jsonrpc"], "2.0");
        assert_eq!(frame["params"]["event"]["type"], TERMINAL_DATA);
        let data = &frame["params"]["event"]["data"];
        assert_eq!(data["daemonBootId"], boot);
        assert_eq!(data["startOffset"], start);
        assert_eq!(data["endOffset"], end);
        assert_eq!(
            BASE64.decode(data["chunk"].as_str().unwrap()).unwrap(),
            bytes
        );
    }
    assert_eq!(frames[4]["params"]["event"]["type"], TERMINAL_EXIT);
}
