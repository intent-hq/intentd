//! Payload-bearing task events must follow committed state, without body reads.
use super::{note, workspace, TempDb};
use crate::{events::EventBus, note_change_event, Services};
use intent_core::{
    events::NOTE_UPDATED, NoteId, TaskMetadata, TaskStatus, WorkspaceApi, WorkspaceId,
};
use intent_store::{EventQuery, Store};

async fn events(store: &Store, kind: &str) -> Vec<intent_core::Event> {
    store
        .query_events(&EventQuery {
            event_types: vec![kind.into()],
            ..Default::default()
        })
        .await
        .unwrap()
}

#[intent_test_macros::daemon_test]
async fn task_push_has_complete_rows_and_changed_only_statistics() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("task-push");
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    store
        .insert_note(&note(&ws, "spec", "[Task](intent://local/task/task)"))
        .await
        .unwrap();
    let mut task = note(&ws, "task", "body must not be pushed");
    task.parent_id = Some(NoteId::from("spec"));
    task.metadata.task = Some(TaskMetadata::default());
    store.insert_note(&task).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    services
        .task_update_note_status(
            ws.clone(),
            task.id.clone(),
            "in_progress".into(),
            None,
            None,
        )
        .await
        .unwrap();
    let pushed = events(&store, "task:updated").await;
    assert_eq!(pushed.len(), 1);
    let expected = services.task_list(ws.clone(), None).await.unwrap();
    assert_eq!(
        pushed[0].data["tasks"],
        serde_json::to_value(&expected.tasks).unwrap()
    );
    assert_eq!(pushed[0].data["removedIds"], serde_json::json!([]));
    let stats = events(&store, "workspace:task-stats-changed").await;
    assert_eq!(stats.len(), 1);
    assert_eq!(
        stats[0].data["taskStats"],
        serde_json::to_value(&expected.stats).unwrap()
    );
    let note_events = events(&store, "task:status-changed").await;
    let data = &note_events
        .iter()
        .find(|e| e.data["noteId"] == "task")
        .unwrap()
        .data;
    assert_eq!(data["note"]["metadata"]["task"]["status"], "in_progress");
    assert!(data["note"].get("content").is_none());
    assert!(data["note"]["rev"].as_i64().unwrap() > 0);
    let mut updated = store.get_note(&ws, &task.id).await.unwrap();
    updated.title = "Renamed".into();
    store.update_note_metadata(&updated).await.unwrap();
    bus.publish(&note_change_event(
        &ws,
        &task.id,
        &updated.title,
        NOTE_UPDATED,
        "update",
    ))
    .await
    .unwrap();
    assert_eq!(
        events(&store, "workspace:task-stats-changed").await.len(),
        1
    );
    assert!(events(&store, "task:updated")
        .await
        .iter()
        .any(|e| e.data["tasks"][0]["title"] == "Renamed"));
    bus.shutdown().await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn task_push_first_body_write_does_not_publish_statistics() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("task-body-push");
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut task = note(&ws, "task", "old body");
    task.parent_id = Some(NoteId::from("spec"));
    task.metadata.task = Some(TaskMetadata {
        status: TaskStatus::InProgress,
        ..Default::default()
    });
    store.insert_note(&task).await.unwrap();
    let bus = EventBus::new(store.clone());
    task.content = "new body".into();
    store.update_note(&task).await.unwrap();
    bus.publish(&note_change_event(
        &ws,
        &task.id,
        &task.title,
        NOTE_UPDATED,
        "update",
    ))
    .await
    .unwrap();
    assert!(events(&store, "workspace:task-stats-changed")
        .await
        .is_empty());
    assert!(events(&store, NOTE_UPDATED).await[0].data["note"].is_object());
    bus.shutdown().await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn task_push_spec_links_dependents_deletion_and_duplicate_notifications() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("task-relations-push");
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut spec = note(
        &ws,
        "spec",
        "[A](intent://local/task/a)\n[B](intent://local/task/b)",
    );
    store.insert_note(&spec).await.unwrap();
    for id in ["a", "b"] {
        let mut task = note(&ws, id, "task body");
        task.parent_id = Some(NoteId::from("spec"));
        task.metadata.task = Some(TaskMetadata {
            depends_on: if id == "b" {
                vec![NoteId::from("a")]
            } else {
                vec![]
            },
            ..Default::default()
        });
        store.insert_note(&task).await.unwrap();
    }
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    services
        .task_update_note_status(ws.clone(), NoteId::from("a"), "complete".into(), None, None)
        .await
        .unwrap();
    let pushes = events(&store, "task:updated").await;
    assert_eq!(
        pushes.len(),
        1,
        "dependent note notifications must not duplicate the batch"
    );
    let rows = pushes[0].data["tasks"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let dependent = rows.iter().find(|r| r["id"] == "b").unwrap();
    assert!(
        dependent.get("unmetDependsOn").is_none(),
        "complete upsert clears omitted relations"
    );
    spec = store.get_note(&ws, &spec.id).await.unwrap();
    spec.content = "[B](intent://local/task/b)".into();
    store.update_note(&spec).await.unwrap();
    let notification = note_change_event(&ws, &spec.id, &spec.title, NOTE_UPDATED, "update");
    bus.publish(&notification).await.unwrap();
    let pushes = events(&store, "task:updated").await;
    assert_eq!(pushes.len(), 2);
    assert!(pushes
        .iter()
        .any(|p| p.data["tasks"][0]["id"] == "a" && p.data["tasks"][0]["specLinked"] == false));
    bus.publish(&notification).await.unwrap();
    assert_eq!(events(&store, "task:updated").await.len(), 2);
    assert_eq!(
        events(&store, "workspace:task-stats-changed").await.len(),
        2
    );
    services
        .delete_note(ws.clone(), NoteId::from("a"), None)
        .await
        .unwrap();
    let pushes = events(&store, "task:updated").await;
    let deleted = pushes
        .iter()
        .find(|p| p.data["removedIds"] == serde_json::json!(["a"]))
        .unwrap();
    assert_eq!(deleted.data["tasks"][0]["id"], "b");
    assert_eq!(
        deleted.data["tasks"][0]["unmetDependsOn"],
        serde_json::json!(["a"])
    );
    assert_eq!(
        events(&store, "workspace:task-stats-changed").await.len(),
        2,
        "deleting an unlinked task cannot change stats"
    );
    bus.shutdown().await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn task_push_rejected_write_emits_nothing_and_generic_metadata_is_current() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("task-generic-push");
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut task = note(&ws, "task", "untouched body");
    task.metadata.task = Some(TaskMetadata::default());
    store.insert_note(&task).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    assert!(services
        .update_note_metadata(
            ws.clone(),
            task.id.clone(),
            Some("rejected".into()),
            None,
            Some(999),
            None
        )
        .await
        .is_err());
    assert!(events(&store, "task:updated").await.is_empty());
    services
        .update_note_metadata(
            ws.clone(),
            task.id.clone(),
            Some("current".into()),
            Some(vec!["label".into()]),
            None,
            None,
        )
        .await
        .unwrap();
    let pushed = events(&store, NOTE_UPDATED).await;
    assert_eq!(pushed[0].data["note"]["title"], "current");
    assert_eq!(pushed[0].data["note"]["tags"], serde_json::json!(["label"]));
    assert_eq!(events(&store, "task:updated").await.len(), 1);
    assert!(events(&store, "workspace:task-stats-changed")
        .await
        .is_empty());
    // Late duplicate notifications read the current store, never the old title.
    bus.publish(&note_change_event(
        &ws,
        &task.id,
        "stale title",
        NOTE_UPDATED,
        "update",
    ))
    .await
    .unwrap();
    assert_eq!(events(&store, "task:updated").await.len(), 1);
    assert!(events(&store, NOTE_UPDATED)
        .await
        .iter()
        .all(|e| e.data["note"]["title"] == "current"));
    bus.shutdown().await.unwrap();
}
