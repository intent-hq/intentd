//! Integration tests for the [`EventBus`] over a temp SQLite store: publish
//! appends-to-store-and-broadcasts, type-glob matching, `excludeSelf`, and
//! `batchWindow` coalescing. Pure matching semantics live in `filter`.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{ActorType, EventActor, WorkspaceId};
use intent_store::{EventQuery, NewEvent, Store};
use serde_json::json;
use tokio::time::timeout;

use super::bus::EventBus;
use super::filter::SubscriptionFilter;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-bus-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

fn new_event(event_type: &str, actor_id: Option<&str>, actor_type: ActorType) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from("ws-1"),
        timestamp: "2026-01-01T00:00:00Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type,
            id: actor_id.map(|s| s.to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({}),
    }
}

async fn bus() -> (TempDb, EventBus) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    (tmp, EventBus::new(store))
}

#[tokio::test]
async fn publish_appends_to_store_and_broadcasts() {
    let (_tmp, bus) = bus().await;
    // Subscribe before publishing; no batching → immediate single-event batch.
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let stored = bus
        .publish(&new_event(
            "note:created",
            Some("agent-1"),
            ActorType::Agent,
        ))
        .await
        .expect("publish");

    // Broadcast leg: the subscriber receives the persisted event.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, stored.id);
    assert_eq!(batch[0].event_type, "note:created");

    // Store leg: the event is durably queryable.
    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, stored.id);
}

#[tokio::test]
async fn type_glob_and_exclude_self() {
    let (_tmp, bus) = bus().await;
    // agent:* with excludeSelf for agent-self; no batching for determinism.
    let mut filter = SubscriptionFilter::for_subscriber(
        &["agent:*".to_string()],
        Some("agent-self"),
        true,
        None,
    );
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    // Dropped: own event (excludeSelf) and a non-matching type.
    bus.publish(&new_event(
        "agent:idle",
        Some("agent-self"),
        ActorType::Agent,
    ))
    .await
    .expect("publish self");
    bus.publish(&new_event(
        "file:changed",
        Some("agent-other"),
        ActorType::System,
    ))
    .await
    .expect("publish file");
    // Kept: matching type from a different actor.
    let kept = bus
        .publish(&new_event(
            "agent:idle",
            Some("agent-other"),
            ActorType::Agent,
        ))
        .await
        .expect("publish other");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, kept.id);
    assert_eq!(batch[0].actor.id.as_deref(), Some("agent-other"));
}

#[tokio::test]
async fn batch_window_coalesces_matched_events() {
    let (_tmp, bus) = bus().await;
    // Generous real-time window so the three quick inserts coalesce into one.
    let filter = SubscriptionFilter::for_subscriber(
        &["note:*".to_string()],
        None,
        false,
        Some(Duration::from_millis(300)),
    );
    let mut sub = bus.subscribe(filter);

    for _ in 0..3 {
        bus.publish(&new_event("note:created", Some("u"), ActorType::User))
            .await
            .expect("publish");
    }

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 3, "events within the window should coalesce");
}

#[tokio::test]
async fn subscriber_count_tracks_live_subscriptions() {
    let (_tmp, bus) = bus().await;
    assert_eq!(bus.subscriber_count(), 0);
    let s1 = bus.subscribe(SubscriptionFilter::default());
    let s2 = bus.subscribe(SubscriptionFilter::default());
    assert_eq!(bus.subscriber_count(), 2);
    // Dropping a Subscription aborts its delivery task, which drops the
    // broadcast receiver and decrements the live-subscriber count.
    drop(s1);
    // The abort is asynchronous; yield until the receiver actually drops.
    for _ in 0..50 {
        if bus.subscriber_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(bus.subscriber_count(), 1);
    drop(s2);
}

#[tokio::test]
async fn non_matching_events_are_filtered_out_silently() {
    let (_tmp, bus) = bus().await;
    // Filter that matches nothing the test publishes; the delivery task must
    // simply skip each event (the `continue` branch) without surfacing them.
    let mut filter = SubscriptionFilter::for_subscriber(&["task:*".to_string()], None, false, None);
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    bus.publish(&new_event("note:created", None, ActorType::User))
        .await
        .expect("publish");
    bus.publish(&new_event("file:changed", None, ActorType::System))
        .await
        .expect("publish");

    // Nothing matches → recv must time out rather than deliver a stray batch.
    let got = timeout(Duration::from_millis(150), sub.recv()).await;
    assert!(
        got.is_err(),
        "non-matching events must not be delivered: {got:?}"
    );
}

#[tokio::test]
async fn dropping_bus_flushes_buffered_batch_before_close() {
    let (_tmp, bus) = bus().await;
    // Long batch window so the published events stay buffered in the delivery
    // task; dropping the bus must trigger the `Closed`-branch flush instead of
    // discarding the in-flight batch.
    let filter = SubscriptionFilter::for_subscriber(
        &["note:*".to_string()],
        None,
        false,
        Some(Duration::from_secs(30)),
    );
    let mut sub = bus.subscribe(filter);

    bus.publish(&new_event("note:created", Some("u"), ActorType::User))
        .await
        .expect("publish");
    bus.publish(&new_event("note:updated", Some("u"), ActorType::User))
        .await
        .expect("publish");

    // Drop the bus → broadcast Sender drops → delivery task observes Closed,
    // flushes the buffered batch, then returns.
    drop(bus);

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("flush timed out")
        .expect("flush yielded no batch");
    assert_eq!(batch.len(), 2, "buffered events must flush on bus close");
    // After the flush the channel is closed; no further batches arrive.
    assert!(sub.recv().await.is_none());
}
