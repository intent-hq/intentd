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

#[tokio::test]
async fn concurrent_burst_batches_events_correctly() {
    let (_tmp, bus) = bus().await;
    // Subscribe to capture all events (no batching for simpler per-publisher assertions).
    let mut filter = SubscriptionFilter::default();
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    const PUBLISHERS: usize = 30;
    const EVENTS_PER_PUBLISHER: usize = 17; // ~510 total events
    const TOTAL_EVENTS: usize = PUBLISHERS * EVENTS_PER_PUBLISHER;

    // Spawn 30 concurrent tasks, each publishing 17 events.
    let handles: Vec<_> = (0..PUBLISHERS)
        .map(|publisher_id| {
            let bus = bus.clone();
            tokio::spawn(async move {
                let mut results = Vec::new();
                for seq in 0..EVENTS_PER_PUBLISHER {
                    let ev = new_event(
                        "test:burst",
                        Some(&format!("publisher-{}", publisher_id)),
                        ActorType::Agent,
                    );
                    let mut ev_with_seq = ev;
                    ev_with_seq.data = serde_json::json!({
                        "publisher_id": publisher_id,
                        "seq": seq,
                    });
                    let stored = bus.publish(&ev_with_seq).await.expect("publish");
                    results.push((stored.id.clone(), publisher_id, seq));
                }
                results
            })
        })
        .collect();

    // Await all publishers; collect the IDs each returned.
    let mut all_published = Vec::new();
    for h in handles {
        all_published.extend(h.await.expect("join"));
    }
    assert_eq!(
        all_published.len(),
        TOTAL_EVENTS,
        "all concurrent publishes should succeed"
    );

    // Collect all events from the subscriber.
    let start = std::time::Instant::now();
    let mut received = Vec::new();
    for _ in 0..TOTAL_EVENTS {
        let batch = timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("subscriber recv timed out")
            .expect("subscription closed early");
        assert_eq!(batch.len(), 1, "no batching; each event is single-element");
        received.push(batch.into_iter().next().unwrap());
    }
    let elapsed = start.elapsed();

    // All published events should be received (check by id set equality).
    let published_ids: std::collections::HashSet<_> =
        all_published.iter().map(|(id, _, _)| id.as_str()).collect();
    let received_ids: std::collections::HashSet<_> =
        received.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        published_ids, received_ids,
        "subscriber should receive all published events"
    );

    // Verify per-publisher ordering: for each publisher, the received events
    // with that publisher_id should have monotonically increasing seq.
    for publisher_id in 0..PUBLISHERS {
        let seqs: Vec<_> = received
            .iter()
            .filter_map(|e| {
                if e.data["publisher_id"] == publisher_id {
                    Some(e.data["seq"].as_u64().unwrap())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            seqs.len(),
            EVENTS_PER_PUBLISHER,
            "publisher {} should have {} events",
            publisher_id,
            EVENTS_PER_PUBLISHER
        );
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(
            seqs, sorted,
            "publisher {} events should be in order",
            publisher_id
        );
    }

    // Verify all events are in the store.
    let queried = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            event_types: vec!["test:burst".to_string()],
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(
        queried.len(),
        TOTAL_EVENTS,
        "all events should be durably stored"
    );

    // Throughput sanity: the burst should complete reasonably fast. With batching,
    // we expect this to take well under what 510 sequential single-insert transactions
    // would take (~500ms+ on most systems). Assert an upper bound (e.g. 2s) to catch
    // regressions where batching breaks.
    assert!(
        elapsed < Duration::from_secs(3),
        "burst should complete in <3s with batching; took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn insert_events_failure_resolves_oneshots_with_error() {
    let (_tmp, bus) = bus().await;
    // Subscribe to verify nothing is broadcast on failure.
    let mut filter = SubscriptionFilter::default();
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    // Close the store's pools to force insert_events to fail.
    bus.store().write_pool().close().await;
    bus.store().read_pool().close().await;

    // Try to publish 3 events; all should fail with the same error.
    let mut errors = Vec::new();
    for i in 0..3 {
        let result = bus
            .publish(&new_event(
                "test:failure",
                Some(&format!("publisher-{}", i)),
                ActorType::Agent,
            ))
            .await;
        assert!(result.is_err(), "publish should fail with closed pool");
        errors.push(result.unwrap_err().to_string());
    }

    // All errors should mention the batch insert failure.
    for err in &errors {
        assert!(
            err.contains("batch insert failed"),
            "error should indicate batch insert failure: {}",
            err
        );
    }

    // Verify nothing was broadcast to subscribers (no events succeed on failure).
    let got = timeout(Duration::from_millis(150), sub.recv()).await;
    assert!(
        got.is_err(),
        "no events should be broadcast on insert_events failure"
    );
}

#[tokio::test]
async fn oneshot_receiver_drop_is_handled_gracefully() {
    let (_tmp, bus) = bus().await;
    // Publish an event, but drop the returned future immediately (drops the oneshot receiver).
    // The writer task should handle the dropped receiver gracefully (the oneshot send fails,
    // but the task continues processing other events).
    let event = new_event("test:dropped", Some("agent-1"), ActorType::Agent);
    let publish_fut = bus.publish(&event);
    drop(publish_fut);

    // Wait a bit for the writer task to process the dropped event.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now publish a normal event; it should succeed despite the prior dropped receiver.
    let result = bus
        .publish(&new_event("test:normal", Some("agent-2"), ActorType::Agent))
        .await;
    assert!(
        result.is_ok(),
        "subsequent publish should succeed after oneshot receiver drop"
    );
}
