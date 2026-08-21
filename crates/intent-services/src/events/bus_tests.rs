//! Integration tests for the [`EventBus`] over a temp SQLite store: publish
//! appends-to-store-and-broadcasts, type-glob matching, `excludeSelf`, and
//! `batchWindow` coalescing. Pure matching semantics live in `filter`.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{ActorType, Event, EventActor, WorkspaceId};
use intent_store::{EventQuery, NewEvent, Store};
use serde_json::json;
use tokio::time::timeout;

use super::bus::{Delivery, EventBus, LagWarnThrottle, BROADCAST_CAPACITY, LAG_WARN_INTERVAL};
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
            id: actor_id.map(std::string::ToString::to_string),
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

/// Hybrid `file:*` persistence: non-agent file events broadcast but are never
/// written to SQLite, while agent-attributed ones stay durable.
#[tokio::test]
async fn non_agent_file_events_broadcast_without_persisting() {
    let (_tmp, bus) = bus().await;
    let mut filter = SubscriptionFilter::for_subscriber(&["file:*".to_string()], None, false, None);
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    for (event_type, actor) in [
        ("file:changed", ActorType::System),
        ("file:created", ActorType::User),
        ("file:deleted", ActorType::System),
    ] {
        let ev = bus
            .publish(&new_event(event_type, Some("system"), actor))
            .await
            .expect("publish");
        // The returned event still carries a minted id, as for persisted events.
        assert!(!ev.id.is_empty());
    }
    // Agent-attributed file event: persisted.
    let agent_ev = bus
        .publish(&new_event(
            "file:changed",
            Some("agent-1"),
            ActorType::Agent,
        ))
        .await
        .expect("publish agent file event");

    // Broadcast leg: all four events reached the subscriber.
    let mut seen = Vec::new();
    for _ in 0..4 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription closed");
        seen.extend(batch);
    }
    assert_eq!(seen.len(), 4);

    // Store leg: only the agent-attributed event is durable.
    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1, "only agent file:* events persist: {rows:?}");
    assert_eq!(rows[0].id, agent_ev.id);
    assert_eq!(rows[0].actor.actor_type, ActorType::Agent);
}

/// A non-agent `file:*` burst larger than the broadcast buffer must still reach a
/// subscriber that is actively draining. Transient events are unrecoverable once
/// dropped (nothing to re-read from the log), so unlike the persisted path — which
/// yields on `writer_tx.send()` — the transient path must yield explicitly to let
/// delivery tasks keep up with a non-collapsing burst such as the watcher's
/// shutdown `flush_all`.
#[tokio::test]
async fn large_transient_file_burst_reaches_a_draining_subscriber() {
    let (_tmp, bus) = bus().await;
    let mut filter = SubscriptionFilter::for_subscriber(&["file:*".to_string()], None, false, None);
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    // Exceed BROADCAST_CAPACITY (1024) in one uninterrupted publish loop.
    const BURST: usize = 1500;
    let consumer = tokio::spawn(async move {
        let mut received = 0usize;
        while received < BURST {
            match timeout(Duration::from_secs(5), sub.recv()).await {
                Ok(Some(batch)) => received += batch.len(),
                _ => break,
            }
        }
        received
    });

    for _ in 0..BURST {
        bus.publish(&new_event(
            "file:changed",
            Some("system"),
            ActorType::System,
        ))
        .await
        .expect("publish");
    }

    let received = consumer.await.expect("consumer task");
    assert_eq!(received, BURST, "no transient file event may be dropped");
}

/// The transient downgrade is scoped to `file:*` — non-agent events in other
/// categories keep persisting.
#[tokio::test]
async fn non_agent_non_file_events_still_persist() {
    let (_tmp, bus) = bus().await;
    bus.publish(&new_event("note:created", Some("u"), ActorType::User))
        .await
        .expect("publish note");
    bus.publish(&new_event("git:commit", Some("system"), ActorType::System))
        .await
        .expect("publish git");

    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 2);
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
async fn idle_publish_resolves_without_batch_window_delay() {
    let (_tmp, bus) = bus().await;
    // A lone serial publisher must not pay a fixed batch-window wait on every
    // publish: with the writer's idle path flushing immediately, each publish
    // is bounded only by its SQLite commit. Under the previous 20 ms window,
    // 20 sequential publishes took >= 400 ms by construction, so any bound
    // below that proves the fix; 350 ms leaves headroom for slow/contended CI.
    let start = std::time::Instant::now();
    for i in 0..20 {
        bus.publish(&new_event(
            "test:idle",
            Some(&format!("publisher-{i}")),
            ActorType::Agent,
        ))
        .await
        .expect("publish");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(350),
        "20 sequential idle publishes should flush immediately; took {elapsed:?}"
    );
}

#[tokio::test]
async fn concurrent_burst_batches_events_correctly() {
    let (_tmp, bus) = bus().await;
    // Subscribe to capture all events (no batching for simpler per-publisher assertions).
    let filter = SubscriptionFilter {
        batch_window: None,
        ..Default::default()
    };
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
                        Some(&format!("publisher-{publisher_id}")),
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
            "publisher {publisher_id} should have {EVENTS_PER_PUBLISHER} events"
        );
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(
            seqs, sorted,
            "publisher {publisher_id} events should be in order"
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
        "burst should complete in <3s with batching; took {elapsed:?}"
    );
}

#[tokio::test]
async fn insert_events_failure_resolves_oneshots_with_error() {
    let (_tmp, bus) = bus().await;
    // Subscribe to verify nothing is broadcast on failure.
    let filter = SubscriptionFilter {
        batch_window: None,
        ..Default::default()
    };
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
                Some(&format!("publisher-{i}")),
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
            "error should indicate batch insert failure: {err}"
        );
    }

    // Verify nothing was broadcast to subscribers (no events succeed on failure).
    let got = timeout(Duration::from_millis(150), sub.recv()).await;
    assert!(
        got.is_err(),
        "no events should be broadcast on insert_events failure"
    );
}

/// A `NewEvent` for `agent:tool:call` with the given `output` payload.
fn tool_call_event(output: serde_json::Value) -> NewEvent {
    let mut ev = new_event("agent:tool:call", Some("agent-1"), ActorType::Agent);
    ev.data = json!({
        "toolCallId": "tc-1",
        "toolName": "launch-process",
        "title": "run tests",
        "status": "completed",
        "input": { "command": "cargo test" },
        "output": output,
    });
    ev
}

#[tokio::test]
async fn oversized_tool_call_payload_is_capped_in_store_but_full_on_broadcast() {
    let (_tmp, bus) = bus().await;
    let filter = SubscriptionFilter {
        batch_window: None,
        ..Default::default()
    };
    let mut sub = bus.subscribe(filter);

    // Output alone (~64 KiB) pushes the payload well past the 16 KiB cap.
    let big_output = "x".repeat(64 * 1024);
    let stored = bus
        .publish(&tool_call_event(json!(big_output)))
        .await
        .expect("publish");

    // Publisher's returned event and the broadcast keep the FULL payload.
    assert_eq!(stored.data["output"], json!(big_output));
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].data["output"], json!(big_output));

    // The durable row is capped: output replaced with a truncation marker.
    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    let data = &rows[0].data;
    assert_eq!(data["output"]["truncated"], json!(true));
    assert_eq!(data["output"]["originalBytes"], json!(64 * 1024 + 2)); // + JSON quotes
    let preview = data["output"]["preview"].as_str().expect("preview string");
    assert!(preview.len() <= 2 * 1024, "preview bounded to 2 KiB");
    // Identity + small fields persist verbatim.
    assert_eq!(data["toolCallId"], json!("tc-1"));
    assert_eq!(data["toolName"], json!("launch-process"));
    assert_eq!(data["status"], json!("completed"));
    assert_eq!(data["input"], json!({ "command": "cargo test" }));
    // Overall persisted payload is within the cap.
    assert!(serde_json::to_string(data).unwrap().len() <= 16 * 1024);
}

#[tokio::test]
async fn small_tool_call_payload_persists_verbatim() {
    let (_tmp, bus) = bus().await;
    let stored = bus
        .publish(&tool_call_event(json!("short output")))
        .await
        .expect("publish");

    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data, stored.data, "under-cap payload is untouched");
    assert_eq!(rows[0].data["output"], json!("short output"));
}

#[tokio::test]
async fn oversized_non_tool_call_event_is_not_truncated() {
    let (_tmp, bus) = bus().await;
    let mut ev = new_event("note:created", Some("agent-1"), ActorType::Agent);
    let big = "y".repeat(32 * 1024);
    ev.data = json!({ "content": big });
    bus.publish(&ev).await.expect("publish");

    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].data["content"].as_str().map(str::len),
        Some(32 * 1024),
        "only agent:tool:call payloads are capped"
    );
}

#[tokio::test]
async fn tool_call_fallback_drops_unexpected_huge_fields() {
    let (_tmp, bus) = bus().await;
    // Huge payload in a field outside output/input/registeredAttachments:
    // per-field truncation cannot bound it, so the fallback keeps only
    // identity fields and sets a top-level truncated flag.
    let mut ev = new_event("agent:tool:call", Some("agent-1"), ActorType::Agent);
    ev.data = json!({
        "toolCallId": "tc-2",
        "toolName": "custom",
        "toolKind": "execute",
        "status": "completed",
        "unexpectedBlob": "z".repeat(64 * 1024),
    });
    bus.publish(&ev).await.expect("publish");

    let rows = bus
        .store()
        .query_events(&EventQuery {
            workspace_id: Some(WorkspaceId::from("ws-1")),
            ..Default::default()
        })
        .await
        .expect("query");
    assert_eq!(rows.len(), 1);
    let data = &rows[0].data;
    assert_eq!(data["truncated"], json!(true));
    assert_eq!(data["toolCallId"], json!("tc-2"));
    assert_eq!(data["toolName"], json!("custom"));
    assert_eq!(data["toolKind"], json!("execute"));
    assert!(data.get("unexpectedBlob").is_none(), "huge field dropped");
    assert!(serde_json::to_string(data).unwrap().len() <= 16 * 1024);
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

// --- Batch-insert retry on transient failure (monorepo#2673) ---------------
//
// `flush_prepared` is the insert-retry + resolve/broadcast core of the writer
// task's `flush_batch`, generic over the insert operation so these tests can
// inject failures without a real contended pool. `start_paused` auto-advances
// the retry backoff sleeps.

/// A stored `Event` as `insert_events` would return it for `ev`.
fn stored_event(id: &str, ev: &NewEvent) -> Event {
    Event {
        id: id.to_string(),
        workspace_id: ev.workspace_id.clone(),
        timestamp: ev.timestamp.clone(),
        event_type: ev.event_type.clone(),
        actor: ev.actor.clone(),
        session_id: ev.session_id.clone(),
        correlation_id: ev.correlation_id.clone(),
        parent_event_id: ev.parent_event_id.clone(),
        metadata: ev.metadata.clone(),
        data: ev.data.clone(),
    }
}

/// The observed failure mode: `pool.acquire()` timing out under contention.
fn transient_error() -> intent_core::Error {
    intent_core::Error::Internal(
        "acquire connection failed: pool timed out while waiting for an open connection"
            .to_string(),
    )
}

/// Drift guard: classify an error built through the SAME construction path as
/// `insert_events` — `format!("acquire connection failed: {e}")` over a real
/// `sqlx::Error::PoolTimedOut` — so a wording change on either side (the
/// `event_repo.rs` prefix or sqlx's Display) breaks this test instead of
/// silently disabling retries.
#[test]
fn transient_classification_matches_insert_events_wording() {
    let sqlx_err = sqlx::Error::PoolTimedOut;
    let as_insert_events_builds_it =
        intent_core::Error::Internal(format!("acquire connection failed: {sqlx_err}"));
    assert!(
        super::bus::is_transient_insert_error(&as_insert_events_builds_it),
        "acquire-timeout error must classify as transient: {as_insert_events_builds_it}"
    );
    // And a permanent failure through the same lens stays permanent.
    let permanent =
        intent_core::Error::Internal("insert failed: UNIQUE constraint failed: event.id".into());
    assert!(!super::bus::is_transient_insert_error(&permanent));
}

/// Regression (monorepo#2673): a transient acquire failure must not drop the
/// batch — the retry succeeds and the events are delivered both to the
/// publisher (oneshot) and to live subscribers (broadcast).
#[tokio::test(start_paused = true)]
async fn transient_batch_insert_failure_retries_and_delivers() {
    let (btx, mut brx) = tokio::sync::broadcast::channel(16);
    let (otx, orx) = tokio::sync::oneshot::channel();
    let ev = new_event("note:created", Some("agent-1"), ActorType::Agent);
    let stored = stored_event("evt-1", &ev);
    let mut pending: Vec<super::bus::WriterRequest> = vec![(ev, otx)];

    let attempts = std::cell::Cell::new(0u32);
    super::bus::flush_prepared(
        || {
            attempts.set(attempts.get() + 1);
            let fail = attempts.get() == 1;
            let stored = stored.clone();
            async move {
                if fail {
                    Err(transient_error())
                } else {
                    Ok(vec![stored])
                }
            }
        },
        &mut pending,
        &btx,
    )
    .await;

    assert_eq!(attempts.get(), 2, "one transient failure, one retry");
    assert!(pending.is_empty());
    let resolved = orx
        .await
        .expect("oneshot resolved")
        .expect("publisher sees Ok after retry");
    assert_eq!(resolved.id, "evt-1");
    let broadcast = brx.recv().await.expect("broadcast delivered");
    assert_eq!(broadcast.id, "evt-1");
}

/// Permanent failures (e.g. a constraint violation) must NOT retry: one
/// attempt, publishers resolve with the error, nothing broadcast.
#[tokio::test(start_paused = true)]
async fn permanent_batch_insert_failure_does_not_retry() {
    let (btx, mut brx) = tokio::sync::broadcast::channel(16);
    let (otx, orx) = tokio::sync::oneshot::channel();
    let ev = new_event("note:created", Some("agent-1"), ActorType::Agent);
    let mut pending: Vec<super::bus::WriterRequest> = vec![(ev, otx)];

    let attempts = std::cell::Cell::new(0u32);
    super::bus::flush_prepared(
        || {
            attempts.set(attempts.get() + 1);
            async {
                Err(intent_core::Error::Internal(
                    "insert events failed: UNIQUE constraint failed: event.id".to_string(),
                ))
            }
        },
        &mut pending,
        &btx,
    )
    .await;

    assert_eq!(attempts.get(), 1, "permanent failures never retry");
    assert!(pending.is_empty());
    let err = orx
        .await
        .expect("oneshot resolved")
        .expect_err("publisher sees the error");
    assert!(
        err.to_string().contains("batch insert failed"),
        "existing error shape preserved: {err}"
    );
    assert!(
        matches!(
            brx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "nothing broadcast on durable failure"
    );
}

/// A persistently transient failure exhausts the bounded retry budget, then
/// resolves publishers with the error (existing behavior) without broadcast.
#[tokio::test(start_paused = true)]
async fn transient_batch_insert_failure_exhausts_retries() {
    let (btx, mut brx) = tokio::sync::broadcast::channel(16);
    let (otx, orx) = tokio::sync::oneshot::channel();
    let ev = new_event("note:created", Some("agent-1"), ActorType::Agent);
    let mut pending: Vec<super::bus::WriterRequest> = vec![(ev, otx)];

    let attempts = std::cell::Cell::new(0u32);
    super::bus::flush_prepared(
        || {
            attempts.set(attempts.get() + 1);
            async { Err(transient_error()) }
        },
        &mut pending,
        &btx,
    )
    .await;

    assert_eq!(
        attempts.get(),
        super::bus::INSERT_RETRY_MAX_ATTEMPTS,
        "retry budget is bounded"
    );
    assert!(pending.is_empty());
    let err = orx
        .await
        .expect("oneshot resolved")
        .expect_err("publisher sees the error after exhausted retries");
    assert!(err.to_string().contains("batch insert failed"));
    assert!(
        matches!(
            brx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "nothing broadcast when retries are exhausted"
    );
}

/// Test-only capturing `tracing` subscriber (mirrors the hand-rolled
/// `test_capture` in `intent-transport/src/protocol.rs`; `tracing-subscriber`
/// is not a dependency of this crate). Records `(level, rendered fields)` per
/// event.
#[derive(Clone, Default)]
struct Capture(std::sync::Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

impl Capture {
    fn warns(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(level, _)| *level == tracing::Level::WARN)
            .map(|(_, line)| line.clone())
            .collect()
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write as _;
                let _ = write!(self.0, "{}={:?} ", field.name(), value);
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), visitor.0));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Deterministic throttle semantics: the first lag warns immediately, reports
/// inside [`LAG_WARN_INTERVAL`] are suppressed with their counts accumulated,
/// and the next allowed WARN carries the accumulated total.
#[test]
fn lag_warn_throttle_rate_limits_and_accumulates() {
    let t0 = std::time::Instant::now();
    let mut throttle = LagWarnThrottle::default();
    assert_eq!(
        throttle.record(5, t0),
        Some(5),
        "first lag warns immediately"
    );
    assert_eq!(throttle.record(3, t0 + LAG_WARN_INTERVAL / 2), None);
    assert_eq!(throttle.record(2, t0 + LAG_WARN_INTERVAL / 2), None);
    assert_eq!(
        throttle.record(1, t0 + LAG_WARN_INTERVAL),
        Some(6),
        "post-interval WARN carries the counts accumulated while throttled"
    );
}

/// A subscriber driven past BROADCAST_CAPACITY must surface the loss: the
/// delivery task's `Lagged` arm emits a WARN carrying the skipped count and
/// the subscription's filter scope (event types + workspace).
#[tokio::test]
async fn broadcast_lag_emits_warn_with_skipped_count_and_filter_context() {
    let (_tmp, bus) = bus().await;
    let mut filter = SubscriptionFilter::for_subscriber(&["note:*".to_string()], None, false, None);
    filter.batch_window = None;
    filter.workspace_id = Some("ws-1".to_string());
    let mut sub = bus.subscribe(filter);

    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());

    // Flood the broadcast ring in one non-yielding loop (`publish_transient`
    // never awaits) so the delivery task cannot drain in between: its receiver
    // falls exactly OVERFLOW events behind and reports `Lagged` on next recv.
    const OVERFLOW: usize = 64;
    for _ in 0..(BROADCAST_CAPACITY + OVERFLOW) {
        bus.publish_transient(&new_event("note:created", Some("u"), ActorType::User));
    }

    // Delivery resumes after the lag report: the surviving events arrive.
    let batch = timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert!(!batch.is_empty());

    let warns = capture.warns();
    let warn = warns
        .iter()
        .find(|l| l.contains("skipped="))
        .unwrap_or_else(|| panic!("no broadcast-lag WARN captured; got: {warns:?}"));
    assert!(
        warn.contains(&format!("skipped={OVERFLOW}")),
        "WARN must carry the exact skipped count, got: {warn}"
    );
    assert!(
        warn.contains("note:*"),
        "WARN must carry the filter's event types, got: {warn}"
    );
    assert!(
        warn.contains("ws-1"),
        "WARN must carry the workspace scope, got: {warn}"
    );
    assert!(
        warn.contains("subscriber lagged"),
        "WARN must describe the lag drop, got: {warn}"
    );
}

/// A subscriber driven past BROADCAST_CAPACITY must ALSO see the loss in-band:
/// `recv_delivery` yields a `Delivery::Lagged(n)` marker at the gap position
/// (before the surviving post-drop events), carrying the ring's skipped count,
/// so consumers like the chat forwarder can run a bounded recovery. Plain
/// `recv` consumers keep seeing only event batches.
#[tokio::test]
async fn broadcast_lag_yields_in_band_lagged_marker_before_surviving_events() {
    let (_tmp, bus) = bus().await;
    let mut filter = SubscriptionFilter::for_subscriber(&["note:*".to_string()], None, false, None);
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    const OVERFLOW: usize = 64;
    for _ in 0..(BROADCAST_CAPACITY + OVERFLOW) {
        bus.publish_transient(&new_event("note:created", Some("u"), ActorType::User));
    }

    let first = timeout(Duration::from_secs(5), sub.recv_delivery())
        .await
        .expect("recv_delivery timed out")
        .expect("subscription closed");
    match first {
        Delivery::Lagged(n) => assert_eq!(
            n, OVERFLOW as u64,
            "marker carries the ring's exact skipped count"
        ),
        Delivery::Batch(_) => panic!("expected the lag marker before any surviving event"),
    }
    let second = timeout(Duration::from_secs(5), sub.recv_delivery())
        .await
        .expect("recv_delivery timed out")
        .expect("subscription closed");
    assert!(
        matches!(second, Delivery::Batch(ref b) if !b.is_empty()),
        "surviving events follow the marker"
    );
}

/// [`Subscription::recv`] must skip lag markers transparently: existing
/// consumers (watchers, script/terminal streams) see only event batches.
#[tokio::test]
async fn recv_skips_lag_markers_transparently() {
    let (_tmp, bus) = bus().await;
    let mut filter = SubscriptionFilter::for_subscriber(&["note:*".to_string()], None, false, None);
    filter.batch_window = None;
    let mut sub = bus.subscribe(filter);

    for _ in 0..(BROADCAST_CAPACITY + 8) {
        bus.publish_transient(&new_event("note:created", Some("u"), ActorType::User));
    }

    let batch = timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert!(
        !batch.is_empty(),
        "recv yields the surviving events, never a marker"
    );
}
