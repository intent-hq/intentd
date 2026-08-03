//! Unit tests for the event bus transient/persisted publish paths.

use super::*;
use intent_core::events::{
    AGENT_STREAM_END, AGENT_STREAM_STATUS, AGENT_TOOL_CALL, CHAT_STREAM_DELTA,
};
use intent_core::{ActorType, Event, EventActor, WorkspaceId};
use intent_store::{NewEvent, Store};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Drive a mock agent turn with stream chunks (transient) and other events
/// (persisted), then assert chunks broadcast but never land in the store while
/// status/end/tool:call rows exist.
#[tokio::test]
async fn agent_stream_chunks_are_transient() {
    // Dir-based guard: dropping it also sweeps SQLite's `-wal`/`-shm`
    // sidecars, which a NamedTempFile would leave behind.
    let tmp = tempfile::tempdir().expect("temp db dir");
    let store = Store::open(&tmp.path().join("events.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let ws = WorkspaceId::new();
    let agent_id = "agent-123";
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some(agent_id.to_string()),
        ..Default::default()
    };

    // Subscribe to the bus and collect all events (chunks + persisted).
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.to_string()),
        event_types: vec![],
        actor_ids: vec![],
        exclude_actor_ids: vec![],
        actor_types: vec![],
        since: None,
        batch_window: None,
        exclude_agent_events: false,
    });
    let received: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let recv_clone = Arc::clone(&received);
    let sub_handle = tokio::spawn(async move {
        while let Some(batch) = sub.recv().await {
            recv_clone.lock().await.extend(batch);
        }
    });

    // Publish a mix of transient chunks and persisted events, mirroring a real turn.
    // 1) Status update (persisted).
    bus.publish(&NewEvent {
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: AGENT_STREAM_STATUS.to_string(),
        actor: agent.clone(),
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "agentId": agent_id, "phase": "thinking", "message": "Starting", "level": "info" }),
    })
    .await
    .expect("publish status");

    // 2) Chat stream delta (transient).
    let chunk_event = NewEvent {
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: CHAT_STREAM_DELTA.to_string(),
        actor: agent.clone(),
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "agentId": agent_id, "content": "Hello ", "messageId": "m1", "blockIndex": 0, "blockId": "m1:0", "blockType": "text" }),
    };
    let chunk1 = bus.publish_transient(&chunk_event);
    assert!(!chunk1.id.is_empty(), "chunk should have minted id");

    // 3) Another chat stream delta (transient).
    let chunk2 = bus.publish_transient(&NewEvent {
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: CHAT_STREAM_DELTA.to_string(),
        actor: agent.clone(),
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "agentId": agent_id, "content": "world!", "messageId": "m1", "blockIndex": 0, "blockId": "m1:0", "blockType": "text" }),
    });
    assert!(!chunk2.id.is_empty(), "chunk2 should have minted id");

    // 4) Tool call (persisted).
    bus.publish(&NewEvent {
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: AGENT_TOOL_CALL.to_string(),
        actor: agent.clone(),
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "agentId": agent_id, "toolName": "view", "toolKind": "mcp" }),
    })
    .await
    .expect("publish tool call");

    // 5) Stream end (persisted).
    bus.publish(&NewEvent {
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: AGENT_STREAM_END.to_string(),
        actor: agent.clone(),
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "agentId": agent_id }),
    })
    .await
    .expect("publish stream end");

    // Give the subscriber task time to drain.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Subscribers saw all 5 events (2 chunks + 3 persisted).
    let recv = received.lock().await;
    assert_eq!(recv.len(), 5, "subscriber should see all 5 events");
    let chunk_count = recv
        .iter()
        .filter(|e| e.event_type == CHAT_STREAM_DELTA)
        .count();
    assert_eq!(chunk_count, 2, "subscriber should see 2 chunks");

    drop(recv);
    drop(bus); // Close the bus to stop the subscriber.
    let _ = sub_handle.await;

    // The store contains ONLY the 3 persisted events; chunks are absent.
    let stored = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("query store");
    assert_eq!(
        stored.len(),
        3,
        "store should have 3 persisted events (status, tool call, end)"
    );
    let stored_chunks = stored
        .iter()
        .filter(|e| e.event_type == CHAT_STREAM_DELTA)
        .count();
    assert_eq!(stored_chunks, 0, "chunks should NOT be in the store");

    // Verify the 3 persisted event types.
    let types: Vec<&str> = stored.iter().map(|e| e.event_type.as_str()).collect();
    assert!(types.contains(&AGENT_STREAM_STATUS), "status persisted");
    assert!(types.contains(&AGENT_TOOL_CALL), "tool call persisted");
    assert!(types.contains(&AGENT_STREAM_END), "stream end persisted");
}
