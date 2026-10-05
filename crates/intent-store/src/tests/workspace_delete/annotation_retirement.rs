//! Partial annotation retirement must remain unavailable and resumable.
use super::{seed_heavy_workspace_children, seed_workspace, TempDb};
use crate::{workspace_annotation_cleanup, Store};
use intent_core::{note_page::NoteScope, NoteId};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn workspace_annotation_retirement_rejects_reads_and_regeneration_after_cancellation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let temporary = TempDb::new();
        let store = Store::open(&temporary.path).await.unwrap();
        let doomed = seed_workspace(&store, "doomed").await;
        let keeper = seed_workspace(&store, "keeper").await;
        seed_heavy_workspace_children(&store, &doomed, 2).await;
        seed_heavy_workspace_children(&store, &keeper, 2).await;
        // The heavy fixture seeds legacy rows directly; prepare their source
        // indexes through the same backfill used when opening a legacy store.
        let mut connection = store.write_pool().acquire().await.unwrap();
        crate::note_page_index::rebuild_pending(&mut connection).await.unwrap();
        drop(connection);
        sqlx::query("UPDATE comment SET content=? WHERE id='doomed-comment-1'")
            .bind("x".repeat(1024 * 1024))
            .execute(store.write_pool())
            .await
            .unwrap();
        let source = store
            .read_note_page(
                "doomed", "note-1", "alice",
                serde_json::from_value(json!({"kind":"source"})).unwrap(), &json!(1),
            )
            .await
            .unwrap();
        let scope: NoteScope = serde_json::from_value(source["scope"].clone()).unwrap();
        let request = serde_json::from_value(json!({"kind":"replies","maxItems":1,"maxWireBytes":4096})).unwrap();
        let page = store.read_note_annotation_page("alice", &scope,
            source["sourceRevision"].as_str().unwrap(), None, Some("thread"), &request, &json!(1))
            .await.unwrap();
        let context = serde_json::from_value(json!({"kind":"context","contextRef":page["items"][0]["bodyRef"],"maxWireBytes":4096})).unwrap();

        let started = Arc::new(tokio::sync::Notify::new());
        let observed = started.clone();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let mut blocked = Some(blocked);
        let mut connection = store.write_pool().acquire().await.unwrap();
        connection.lock_handle().await.unwrap().set_update_hook(move |update| {
            if update.table == "note_comment_detail_piece" {
                if let Some(blocked) = blocked.take() {
                    observed.notify_one();
                    blocked.recv_timeout(Duration::from_secs(15)).unwrap();
                }
            }
        });
        drop(connection);
        let worker_store = store.clone();
        let cleanup = tokio::spawn(async move {
            workspace_annotation_cleanup::drain(&worker_store, "doomed").await
        });
        started.notified().await;
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        // The SQLite worker completes its submitted statement independently.
        let mut connection = store.write_pool().acquire().await.unwrap();
        connection.lock_handle().await.unwrap().remove_update_hook();
        drop(connection);
        assert!(store.read_note_annotation_context("alice", &scope,
            source["sourceRevision"].as_str().unwrap(),page["commentRevision"].as_str().unwrap(),
            &context, &json!(1)).await.is_err());
        assert!(store.read_note_annotation_page("alice", &scope,
            source["sourceRevision"].as_str().unwrap(),None,Some("thread"),&request,&json!(1)).await.is_err());
        for statement in [
            "UPDATE note SET content='new source' WHERE workspace_id='doomed' AND id='note-1'",
            "UPDATE comment SET content='new body' WHERE id='doomed-comment-1'",
            "UPDATE note_line_attribution SET attributions_json='{}' WHERE workspace_id='doomed'",
            "INSERT INTO note_line_attribution(workspace_id,note_id,computed_at,attributions_json) VALUES('doomed','note-1','later','{}') ON CONFLICT(workspace_id,note_id) DO UPDATE SET attributions_json=excluded.attributions_json",
        ] {
            assert!(sqlx::query(statement).execute(store.write_pool()).await.is_err());
        }
        assert!(store.note_annotation_epochs(&keeper, &NoteId::from("note-1")).await.is_ok());
        store.close().await;
        let store = Store::open(&temporary.path).await.unwrap();
        assert!(store.note_annotation_epochs(&doomed, &NoteId::from("note-1")).await.is_err());
        store.delete_workspace(&doomed).await.unwrap();
        let fences: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_workspace_retirement")
            .fetch_one(store.read_pool()).await.unwrap();
        assert_eq!(fences, 0);
        let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM comment WHERE workspace_id='keeper'")
            .fetch_one(store.read_pool()).await.unwrap();
        assert_eq!(kept, 2);
        let foreign_keys = sqlx::query("PRAGMA foreign_key_check").fetch_all(store.read_pool()).await.unwrap();
        assert!(foreign_keys.is_empty());
    }).await.unwrap();
}

#[tokio::test]
async fn workspace_annotation_retirement_failed_marker_keeps_original_admission() {
    let temporary = TempDb::new();
    let store = Store::open(&temporary.path).await.unwrap();
    let workspace = seed_workspace(&store, "doomed").await;
    seed_heavy_workspace_children(&store, &workspace, 2).await;
    sqlx::query("CREATE TRIGGER fail_retirement BEFORE INSERT ON note_annotation_workspace_retirement BEGIN SELECT RAISE(ABORT,'injected marker failure'); END")
        .execute(store.write_pool()).await.unwrap();
    assert!(
        workspace_annotation_cleanup::begin_retirement(&store, "doomed")
            .await
            .is_err()
    );
    assert!(store
        .note_annotation_epochs(&workspace, &NoteId::from("note-1"))
        .await
        .is_ok());
    let fences: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_workspace_retirement")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(fences, 0);
    let comments: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM comment WHERE workspace_id='doomed'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(comments, 2);
    sqlx::query("UPDATE comment SET content='still admitted' WHERE id='doomed-comment-1'")
        .execute(store.write_pool())
        .await
        .unwrap();
}
