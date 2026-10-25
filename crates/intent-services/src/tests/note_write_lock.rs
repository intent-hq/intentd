//! Every writer that rewrites a note's text holds that note's write lane
//! from its read through its persist (intent-hq/intent#7184). The editor's
//! whole-note save (`note.update` with `content`) and `note.restoreVersion`
//! skipped it, so a laned writer that read the note could be overwritten
//! under it and burn its bounded `Conflict` retries.
use std::time::Duration;

use intent_core::{NoteId, NoteUpdateInput, WorkspaceApi, WorkspaceId};

use super::setup;
use crate::Services;

/// Wait until a second caller (the writer) is queued on `(ws, id)`'s
/// lock behind the test's own hold, failing if it never queues — which is
/// what an unlocked writer does: it lands without ever touching the lock.
async fn wait_for_queued_writer(services: &Services, ws: &WorkspaceId, id: &NoteId) {
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        async {
            while services.store.note_content_lock_users(ws, id) < 2 {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .expect("writer never queued on the note's write lane");
}

async fn stored_content(services: &Services, ws: &WorkspaceId, id: &NoteId) -> String {
    services
        .store
        .get_note(ws, id)
        .await
        .expect("read note")
        .content
}

/// The editor save waits for a held lane and lands only once it is released.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn editor_save_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup("original").await;
    let lane = services.lock_note_content(&ws, &id).await;
    assert_eq!(services.store.note_content_lock_users(&ws, &id), 1);

    let writer = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        intent_core::spawn_daemon(async move {
            services
                .update_note(
                    ws,
                    id,
                    NoteUpdateInput {
                        content: Some("saved by the editor".into()),
                        ..Default::default()
                    },
                )
                .await
        })
    };

    wait_for_queued_writer(&services, &ws, &id).await;
    assert!(
        !writer.is_finished(),
        "editor save landed under a held lane"
    );
    assert_eq!(stored_content(&services, &ws, &id).await, "original");

    drop(lane);
    let note = writer.await.expect("writer task").expect("editor save");
    assert_eq!(note.content, "saved by the editor");
    assert_eq!(
        stored_content(&services, &ws, &id).await,
        "saved by the editor"
    );
}

/// `note.restoreVersion` waits for a held lane and lands only once it is
/// released.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_version_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup("original").await;
    for content in ["version one", "version two"] {
        services
            .update_note(
                ws.clone(),
                id.clone(),
                NoteUpdateInput {
                    content: Some(content.into()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed version");
    }
    let mut target = None;
    for summary in services
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions")
    {
        let version = services
            .store
            .get_note_version(&ws, &id, summary.v)
            .await
            .expect("version");
        if version.content == "version one" {
            target = Some(version.v);
        }
    }
    let target = target.expect("version one snapshot");

    let lane = services.lock_note_content(&ws, &id).await;
    assert_eq!(services.store.note_content_lock_users(&ws, &id), 1);
    let writer = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        intent_core::spawn_daemon(async move {
            services.restore_note_version(ws, id, target, None).await
        })
    };

    wait_for_queued_writer(&services, &ws, &id).await;
    assert!(!writer.is_finished(), "restore landed under a held lane");
    assert_eq!(stored_content(&services, &ws, &id).await, "version two");

    drop(lane);
    writer.await.expect("writer task").expect("restore");
    assert_eq!(stored_content(&services, &ws, &id).await, "version one");
}
