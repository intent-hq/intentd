//! End-to-end service checks for delayed attribution writes and cancellation.
use std::time::Duration;

use intent_core::{events::LINE_ATTRIBUTION_UPDATED, NoteId, NoteVersionAuthor, WorkspaceId};
use serde_json::Value;

use super::{note, workspace, TempDb};
use crate::{EventBus, Services, Subscription, SubscriptionFilter};

async fn setup() -> (TempDb, Services, EventBus, WorkspaceId, WorkspaceId, NoteId) {
    let tmp = TempDb::new();
    let store = intent_store::Store::open(&tmp.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    let ws = WorkspaceId::new();
    let other_ws = WorkspaceId::new();
    let id = NoteId::from("same-note");
    for (workspace_id, content) in [(&ws, "old"), (&other_ws, "other workspace")] {
        store
            .insert_workspace(&workspace(workspace_id))
            .await
            .unwrap();
        let note = note(workspace_id, id.as_str(), content);
        store
            .insert_note_with_version(
                &note,
                &NoteVersionAuthor {
                    id: workspace_id.to_string(),
                    name: "Author".into(),
                    author_type: "user".into(),
                },
                "2026-10-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    (tmp, services, bus, ws, other_ws, id)
}

async fn attribution_event(sub: &mut Subscription) -> Value {
    let events = tokio::time::timeout(Duration::from_secs(20), sub.recv())
        .await
        .expect("attribution event without another edit")
        .expect("subscription open");
    assert_eq!(events.len(), 1);
    let event = serde_json::to_value(&events[0]).unwrap();
    assert_eq!(event["type"], LINE_ATTRIBUTION_UPDATED);
    event
}

#[intent_test_macros::daemon_test]
async fn attribution_retry_persists_and_emits_without_another_edit() {
    let (_tmp, services, bus, ws, _other_ws, id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.to_string()),
        ..Default::default()
    });
    let held = services.store.write_pool().acquire().await.unwrap();
    services.schedule_line_attribution_recompute(&ws, &id);
    // Store coverage shortens the pool timeout and observes the exact retry.
    // Here the public Store uses its production timeout; hold across debounce
    // and the first acquisition window to exercise the scheduler/event path.
    let hold = crate::LINE_ATTRIBUTION_DEBOUNCE
        + services.store.write_pool().options().get_acquire_timeout()
        + Duration::from_secs(1);
    tokio::time::sleep(hold).await;
    drop(held);
    let event = attribution_event(&mut sub).await;
    let snapshot = services
        .store
        .get_note_line_attribution(&ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event["data"]["workspaceId"], ws.as_str());
    assert_eq!(event["data"]["noteId"], id.as_str());
    assert_eq!(
        event["data"]["attributions"],
        serde_json::to_value(snapshot.attributions).unwrap()
    );
    assert_eq!(
        event["data"]["attributions"]["1"]["author"]["id"],
        ws.as_str()
    );
    assert!(
        sub.try_recv_delivery().is_none(),
        "one completed refresh emits once"
    );
}

#[intent_test_macros::daemon_test]
async fn newer_attribution_schedule_cancels_retry_without_cross_workspace_cancellation() {
    let (_tmp, services, bus, ws, other_ws, id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.to_string()),
        ..Default::default()
    });
    let mut other_sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(other_ws.to_string()),
        ..Default::default()
    });
    let mut held = services.store.write_pool().acquire().await.unwrap();
    services.schedule_line_attribution_recompute(&ws, &id);
    services.schedule_line_attribution_recompute(&other_ws, &id);
    let old =
        services.line_attribution_debouncers.lock().unwrap()[&(ws.clone(), id.clone())].clone();
    let hold = crate::LINE_ATTRIBUTION_DEBOUNCE
        + services.store.write_pool().options().get_acquire_timeout()
        + Duration::from_secs(1);
    tokio::time::sleep(hold).await;
    assert!(!old.is_finished(), "old refresh must still be retrying");
    // Simulate the committed newer edit with the connection we already own.
    // Its new first line has no history attribution; the old line moves to line 2.
    sqlx::query("UPDATE note SET content = 'new first line\nold', rev = rev + 1 WHERE workspace_id = ? AND id = ?")
        .bind(ws.as_str()).bind(id.as_str()).execute(&mut *held).await.unwrap();
    services.schedule_line_attribution_recompute(&ws, &id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !old.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new schedule aborts old retry");
    drop(held);
    let event = attribution_event(&mut sub).await;
    let other_event = attribution_event(&mut other_sub).await;
    assert!(event["data"]["attributions"].get("1").is_none());
    assert_eq!(
        event["data"]["attributions"]["2"]["author"]["id"],
        ws.as_str()
    );
    assert_eq!(
        other_event["data"]["attributions"]["1"]["author"]["id"],
        other_ws.as_str()
    );
    let snapshot = services
        .store
        .get_note_line_attribution(&ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event["data"]["attributions"],
        serde_json::to_value(snapshot.attributions).unwrap()
    );
    assert!(
        sub.try_recv_delivery().is_none(),
        "cancelled retry cannot emit stale attribution"
    );
}
