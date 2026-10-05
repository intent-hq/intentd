//! A bounded begin-time epoch is evidence, not admitted marker authority.
use super::{count, request};
use crate::{tests::sample_comment, Store};
use serde_json::{json, Value};

async fn pin(store: &Store, operation: &str) -> Option<String> {
    sqlx::query_scalar("SELECT s.marker_admission FROM note_stage s JOIN note_operation o USING(operation_key) WHERE o.operation_id=?")
        .bind(operation).fetch_one(store.read_pool()).await.unwrap()
}

#[tokio::test]
async fn stage_marker_admission_distinguishes_comment_epochs_and_preserves_exact_replay() {
    let (store, tmp, note) =
        super::super::setup("<!--anchor:legacy:start-->a<!--anchor:legacy:end-->").await;
    let mut root = sample_comment(&note.id, "legacy", "legacy");
    store
        .insert_comment(&note.workspace_id, &root)
        .await
        .unwrap();
    let first = request(&store).await;
    let first_state = store.begin_note_stage("alice", &first).await.unwrap();
    let captured = pin(&store, &first.operation_id).await.unwrap();
    assert!(captured.len() <= 1024);
    let expected: String = sqlx::query_scalar("SELECT json_object('headId',a.id,'sourceRev',a.source_rev,'commentRevision',a.comment_revision,'stateGeneration',s.state_generation,'sourceRevision',s.source_revision) FROM note_annotation_head a JOIN note_annotation_state s USING(workspace_id,note_id) WHERE a.workspace_id='pages' AND a.note_id='spec'")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(captured, expected);
    root.content = "changed comment only".into();
    store
        .update_comment(&note.workspace_id, &root)
        .await
        .unwrap();
    let second = request(&store).await;
    assert_eq!(first.header.base_revision, second.header.base_revision);
    store.begin_note_stage("alice", &second).await.unwrap();
    let second_pin: Value =
        serde_json::from_str(&pin(&store, &second.operation_id).await.unwrap()).unwrap();
    let first_pin: Value = serde_json::from_str(&captured).unwrap();
    assert_eq!(first_pin["headId"], second_pin["headId"]);
    assert_eq!(first_pin["sourceRev"], second_pin["sourceRev"]);
    assert_ne!(first_pin["commentRevision"], second_pin["commentRevision"]);
    assert_ne!(first_pin["stateGeneration"], second_pin["stateGeneration"]);
    assert_eq!(first_pin["sourceRevision"], first.header.base_revision);
    assert_eq!(count(&store, "note_stage_root").await, 1);
    assert_eq!(count(&store, "note_stage_base_piece").await, 0);
    assert_eq!(
        store.begin_note_stage("alice", &first).await.unwrap(),
        first_state
    );
    assert_eq!(
        pin(&store, &first.operation_id).await.as_deref(),
        Some(captured.as_str())
    );
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        pin(&store, &first.operation_id).await.as_deref(),
        Some(captured.as_str())
    );
    assert_eq!(
        store.begin_note_stage("alice", &first).await.unwrap(),
        first_state
    );
}

#[tokio::test]
async fn stage_marker_admission_rolls_back_with_failed_begin_and_does_not_require_annotation_readiness(
) {
    let (store, _tmp, _note) = super::super::setup("unchanged source").await;
    let begin = request(&store).await;
    sqlx::query("CREATE TRIGGER reject_stage_stream BEFORE INSERT ON note_stage_stream BEGIN SELECT RAISE(ABORT,'injected after marker admission'); END")
        .execute(store.write_pool()).await.unwrap();
    assert!(store.begin_note_stage("alice", &begin).await.is_err());
    assert_eq!(count(&store, "note_operation").await, 0);
    assert_eq!(count(&store, "note_stage").await, 0);
    assert_eq!(count(&store, "note_stage_root").await, 0);
    sqlx::query("DROP TRIGGER reject_stage_stream")
        .execute(store.write_pool())
        .await
        .unwrap();
    // An inconsistent annotation revision must not masquerade as marker proof,
    // but the existing independently indexed source can still be staged.
    sqlx::query("UPDATE note_annotation_head SET source_rev=source_rev+1 WHERE workspace_id='pages' AND note_id='spec'")
        .execute(store.write_pool()).await.unwrap();
    let state = store.begin_note_stage("alice", &begin).await.unwrap();
    assert_eq!(state["phase"], "staging");
    assert!(pin(&store, &begin.operation_id).await.is_none());
    assert_eq!(count(&store, "note_stage_base_piece").await, 0);
}

#[tokio::test]
async fn stage_marker_admission_migration_does_not_invent_historical_ownership() {
    let (store, _tmp, note) = super::super::setup("source").await;
    // The low-level setup fixture does not finalize annotation readiness.
    // Use the ordinary metadata writer before capturing this positive witness.
    store.update_note_metadata(&note).await.unwrap();
    let begin = request(&store).await;
    let original = store.begin_note_stage("alice", &begin).await.unwrap();
    assert!(pin(&store, &begin.operation_id).await.is_some());
    // Reconstruct the prior table shape in this disposable migrated database,
    // then apply the exact additive migration over an existing staged operation.
    sqlx::query("ALTER TABLE note_stage DROP COLUMN marker_admission")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../migrations/0156_note_stage_marker_admission.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(pin(&store, &begin.operation_id).await.is_none());
    assert_eq!(
        store.begin_note_stage("alice", &begin).await.unwrap(),
        original
    );
    assert!(pin(&store, &begin.operation_id).await.is_none());
    let source = super::super::page(&store, json!({"kind":"source"})).await;
    assert_eq!(source["text"], "source");
}

#[tokio::test]
async fn stage_marker_admission_requires_ready_anchors_and_never_refreshes_an_absent_pin() {
    let (store, _tmp, _note) = super::super::setup("source").await;
    sqlx::query("UPDATE note_annotation_head SET anchors_rev=-1 WHERE workspace_id='pages' AND note_id='spec'")
        .execute(store.write_pool()).await.unwrap();
    let first = request(&store).await;
    let original = store.begin_note_stage("alice", &first).await.unwrap();
    assert!(pin(&store, &first.operation_id).await.is_none());
    // Model the unchanged-epoch pending-to-ready publication boundary. This
    // control is admission metadata only, not evidence of a canonical marker.
    sqlx::query("UPDATE note_annotation_head SET anchors_rev=source_rev WHERE workspace_id='pages' AND note_id='spec'")
        .execute(store.write_pool()).await.unwrap();
    assert_eq!(
        store.begin_note_stage("alice", &first).await.unwrap(),
        original
    );
    assert!(pin(&store, &first.operation_id).await.is_none());
    let second = request(&store).await;
    store.begin_note_stage("alice", &second).await.unwrap();
    assert!(pin(&store, &second.operation_id).await.is_some());
}
