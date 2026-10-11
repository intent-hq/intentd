//! Recovery CAS and derived note indexes share one cancellable transaction.
use super::{page, setup};
use crate::{
    note_annotation_repo::{AnnotationEpochs, FinalizerPause, FINALIZER_PAUSE},
    tests::{sample_agent_session, sample_comment, sample_workspace, TempDb},
    Store,
};
use intent_core::{
    AgentId, Error, Note, NoteId, NoteVersionAuthor, TaskMetadata, TaskStatus, WorkspaceId,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

#[derive(Debug, PartialEq)]
struct Snapshot {
    note: Note,
    epochs: AnnotationEpochs,
    head: (i64, i64, String, String),
    entries: Vec<(String, i64, String)>,
    pieces: Vec<(i64, i64, String, String)>,
    anchors: Vec<(String, i64, i64)>,
}

async fn snapshot(store: &Store, note: &Note) -> Snapshot {
    let ws = note.workspace_id.as_str();
    let id = note.id.as_str();
    Snapshot {
        note: store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        epochs: store.note_annotation_epochs(&note.workspace_id, &note.id).await.unwrap(),
        head: sqlx::query_as("SELECT current_rev,indexed_rev,generation,content_generation FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(ws).bind(id).fetch_one(store.read_pool()).await.unwrap(),
        entries: sqlx::query_as("SELECT collection,position,value FROM note_page_entry WHERE workspace_id=? AND note_id=? ORDER BY collection,position")
            .bind(ws).bind(id).fetch_all(store.read_pool()).await.unwrap(),
        pieces: sqlx::query_as("SELECT start,end,text,content_generation FROM note_page_piece WHERE workspace_id=? AND note_id=? ORDER BY start")
            .bind(ws).bind(id).fetch_all(store.read_pool()).await.unwrap(),
        anchors: sqlx::query_as("SELECT a.comment_id,a.start,a.end FROM note_comment_anchor a JOIN note_annotation_head h ON h.id=a.head_id WHERE h.workspace_id=? AND h.note_id=? ORDER BY a.occurrence_id")
            .bind(ws).bind(id).fetch_all(store.read_pool()).await.unwrap(),
    }
}

async fn fixture() -> (Store, TempDb, Note, AgentId) {
    let (store, tmp, mut note) = setup("😀<!--anchor:x:start-->ab<!--anchor:x:end-->").await;
    let agent = AgentId::from("recovery-owner");
    note.metadata.task = Some(TaskMetadata {
        status: TaskStatus::Blocked,
        assigned_agent_ids: vec![agent.clone()],
        ..TaskMetadata::default()
    });
    let author = NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    };
    note.rev = store
        .update_note_with_comment(
            &note,
            Some(note.rev),
            &sample_comment(&note.id, "x", "x"),
            &author,
        )
        .await
        .unwrap();
    let mut session = sample_agent_session(&agent, &note.workspace_id);
    session.task_note_id = Some(note.id.clone());
    store.insert_agent_session(&session).await.unwrap();
    (store, tmp, note, agent)
}

fn recovery(note: &Note) -> Note {
    let mut prepared = note.clone();
    prepared.content.clear();
    prepared.title = "Recovered task".into();
    prepared.metadata.task.as_mut().unwrap().status = TaskStatus::InProgress;
    prepared
}

async fn metadata_field(store: &Store, reference: &Value, name: &str) -> Value {
    let root = page(store, json!({"kind":"metadata","ref":reference})).await;
    let fields = page(
        store,
        json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
    )
    .await;
    fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == name)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn linked_metadata_publishes_index_and_persisted_body_anchors_together() {
    let (store, _tmp, note, agent) = fixture().await;
    let before = snapshot(&store, &note).await;
    let source = page(&store, json!({"kind":"source"})).await;
    let prepared = recovery(&note);
    assert_eq!(
        store
            .update_note_metadata_if_agent_linked(&prepared, note.rev, &agent)
            .await
            .unwrap(),
        Some(note.rev + 1)
    );
    let after = snapshot(&store, &note).await;
    assert_eq!(after.note.rev, note.rev + 1);
    assert_eq!(after.note.title, prepared.title);
    assert_eq!(after.note.metadata, prepared.metadata);
    assert_eq!(after.note.content, before.note.content);
    assert_eq!(after.head.0, after.note.rev);
    assert_eq!(after.head.1, after.note.rev);
    assert_ne!(after.head.2, before.head.2);
    assert_eq!(after.head.3, before.head.3);
    assert_eq!(after.pieces, before.pieces);
    assert_eq!(after.epochs.source_revision, after.note.rev);
    assert!(after.epochs.anchors_ready);
    assert_eq!(after.anchors, vec![("x".into(), 23, 25)]);
    assert_eq!(after.anchors, before.anchors);
    let current = page(&store, json!({"kind":"source"})).await;
    assert_eq!(current["text"], note.content);
    assert_ne!(current["sourceRevision"], source["sourceRevision"]);
    let title = metadata_field(&store, &current["metadataRef"], "title").await;
    let title_text = page(
        &store,
        json!({"kind":"context","contextRef":title["valueRef"]}),
    )
    .await;
    assert_eq!(title_text["items"][0]["text"], prepared.title);
    let metadata = metadata_field(&store, &current["metadataRef"], "metadata").await;
    let fields = page(
        &store,
        json!({"kind":"metadata","ref":metadata["childrenRef"]}),
    )
    .await;
    let task = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == "task")
        .unwrap();
    let fields = page(&store, json!({"kind":"metadata","ref":task["childrenRef"]})).await;
    let status = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == "status")
        .unwrap();
    let status_text = page(
        &store,
        json!({"kind":"context","contextRef":status["valueRef"]}),
    )
    .await;
    assert_eq!(status_text["items"][0]["text"], "in_progress");
}

#[tokio::test]
async fn linked_metadata_rejects_stale_missing_wrong_agent_and_workspace_without_finalizing() {
    let (store, _tmp, note, agent) = fixture().await;
    let mut other = note.clone();
    other.workspace_id = WorkspaceId::from("other-workspace");
    store
        .insert_workspace(&sample_workspace(&other.workspace_id, "Other", false))
        .await
        .unwrap();
    store.insert_note(&other).await.unwrap();
    let before = snapshot(&store, &note).await;
    let other_before = snapshot(&store, &other).await;
    let mut missing = recovery(&note);
    missing.id = NoteId::from("absent");
    let cases = [
        (recovery(&note), note.rev - 1, agent.clone()),
        (missing, note.rev, agent.clone()),
        (recovery(&note), note.rev, AgentId::from("wrong-agent")),
        (recovery(&other), other.rev, agent.clone()),
    ];
    for (prepared, revision, owner) in cases {
        // A no-op must not enter the anchor finalizer at all.
        let pause = Arc::new(FinalizerPause::default());
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            FINALIZER_PAUSE.scope(
                pause,
                store.update_note_metadata_if_agent_linked(&prepared, revision, &owner),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, None);
        assert_eq!(snapshot(&store, &note).await, before);
        assert_eq!(snapshot(&store, &other).await, other_before);
    }
}

#[tokio::test]
async fn linked_metadata_rejects_session_only_relink_without_note_or_index_changes() {
    let (store, _tmp, note, agent) = fixture().await;
    let mut next = note.clone();
    next.id = NoteId::from("new-task");
    store.insert_note(&next).await.unwrap();
    let prepared = recovery(&note);
    let before = snapshot(&store, &note).await;
    let next_before = snapshot(&store, &next).await;
    let mut session = store.get_agent_session(&agent).await.unwrap();
    session.task_note_id = Some(next.id.clone());
    store
        .update_agent_session(&note.workspace_id, &session)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .rev,
        note.rev
    );
    assert_eq!(
        store
            .update_note_metadata_if_agent_linked(&prepared, note.rev, &agent)
            .await
            .unwrap(),
        None
    );
    assert_eq!(snapshot(&store, &note).await, before);
    assert_eq!(snapshot(&store, &next).await, next_before);
    assert_eq!(
        store.get_agent_session(&agent).await.unwrap().task_note_id,
        Some(next.id)
    );
}

async fn rejected_write_rolls_back(trigger: &str, message: &str) {
    let (store, _tmp, note, agent) = fixture().await;
    let before = snapshot(&store, &note).await;
    sqlx::query(trigger)
        .execute(store.write_pool())
        .await
        .unwrap();
    let prepared = recovery(&note);
    let result = store
        .update_note_metadata_if_agent_linked(&prepared, note.rev, &agent)
        .await;
    assert!(
        matches!(result, Err(Error::Internal(ref detail)) if detail.contains(message)),
        "{result:?}"
    );
    assert_eq!(snapshot(&store, &note).await, before);
    sqlx::query("DROP TRIGGER reject_recovery")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .update_note_metadata_if_agent_linked(&prepared, note.rev, &agent)
            .await
            .unwrap(),
        Some(note.rev + 1)
    );
    let after = snapshot(&store, &note).await;
    assert!(after.epochs.anchors_ready);
    assert_eq!(after.note.content, before.note.content);
    assert_eq!(after.anchors, before.anchors);
}

#[tokio::test]
async fn linked_metadata_index_failure_rolls_back_and_writer_is_reusable() {
    rejected_write_rolls_back("CREATE TRIGGER reject_recovery BEFORE INSERT ON note_page_entry WHEN NEW.collection='m:root' BEGIN SELECT RAISE(ABORT,'recovery index rejected'); END", "recovery index rejected").await;
}

#[tokio::test]
async fn linked_metadata_finalizer_failure_rolls_back_and_writer_is_reusable() {
    rejected_write_rolls_back("CREATE TRIGGER reject_recovery BEFORE INSERT ON note_comment_anchor BEGIN SELECT RAISE(ABORT,'recovery anchor rejected'); END", "recovery anchor rejected").await;
}

#[tokio::test]
async fn linked_metadata_cancellation_rolls_back_note_index_and_anchor_epochs() {
    let (store, _tmp, note, agent) = fixture().await;
    let before = snapshot(&store, &note).await;
    let prepared = recovery(&note);
    let pause = Arc::new(FinalizerPause::default());
    let mut write = Box::pin(FINALIZER_PAUSE.scope(
        Arc::clone(&pause),
        store.update_note_metadata_if_agent_linked(&prepared, note.rev, &agent),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut write => panic!("write finished before finalizer: {result:?}"),
            () = pause.entered.notified() => {},
        }
    })
    .await
    .unwrap();
    let observed = pause.observed.lock().unwrap().clone().unwrap();
    assert_eq!(observed.source_revision, note.rev + 1);
    assert!(!observed.anchors_ready);
    // A concurrent reader cannot see the partially finalized transaction.
    assert_eq!(snapshot(&store, &note).await, before);
    drop(write);
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("BEGIN IMMEDIATE; ROLLBACK").execute(store.write_pool()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(snapshot(&store, &note).await, before);
    assert_eq!(
        store
            .update_note_metadata_if_agent_linked(&prepared, note.rev, &agent)
            .await
            .unwrap(),
        Some(note.rev + 1)
    );
    let after = snapshot(&store, &note).await;
    assert!(after.epochs.anchors_ready);
    assert_eq!(after.note.content, before.note.content);
    assert_eq!(after.anchors, before.anchors);
}
