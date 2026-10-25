//! Every writer that rewrites a note's text holds that note's write lane
//! from its read through its persist (intent-hq/intent#7184). The editor's
//! whole-note save (`note.update` with `content`) and `note.restoreVersion`
//! skipped it, so a laned writer that read the note could be overwritten
//! under it and burn its bounded `Conflict` retries. Metadata writes (task
//! status, title/tags, task relations, assignment) take the same lane, so a
//! restore or blind save cannot revert them from a stale copy and they cannot
//! move the rev inside a laned writer's read→persist window.
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

/// Hold `(ws, id)`'s lane, spawn `write`, and check it queues behind the
/// hold without moving the note's `rev`, then release and return its result.
/// A metadata write that skips the lane lands at once and bumps `rev`, which
/// is how it reverted (or exhausted the retries of) the laned writer whose
/// read→persist window it fell into.
async fn assert_waits_for_lane<T, F>(
    services: &Services,
    ws: &WorkspaceId,
    id: &NoteId,
    write: F,
) -> T
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    let lane = services.lock_note_content(ws, id).await;
    let rev_before = services.store.get_note(ws, id).await.expect("read").rev;
    let writer = intent_core::spawn_daemon(write);
    wait_for_queued_writer(services, ws, id).await;
    assert!(
        !writer.is_finished(),
        "metadata write landed under a held lane"
    );
    assert_eq!(
        services.store.get_note(ws, id).await.expect("read").rev,
        rev_before,
        "metadata write moved the rev under a held lane"
    );
    drop(lane);
    writer.await.expect("writer task")
}

async fn setup_task(content: &str) -> (super::TempDb, Services, WorkspaceId, NoteId) {
    let (tmp, services, ws, id) = setup(content).await;
    services
        .mark_as_task(
            ws.clone(),
            id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("markAsTask");
    (tmp, services, ws, id)
}

async fn task_status(
    services: &Services,
    ws: &WorkspaceId,
    id: &NoteId,
) -> intent_core::TaskStatus {
    services
        .store
        .get_note(ws, id)
        .await
        .expect("read")
        .metadata
        .task
        .expect("task")
        .status
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn task_note_status_write_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup_task("# Task").await;
    let write = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        async move {
            services
                .task_update_note_status(ws, id, "complete".into(), None, None)
                .await
        }
    };
    assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("task.updateNoteStatus");
    assert_eq!(
        task_status(&services, &ws, &id).await,
        intent_core::TaskStatus::Complete
    );
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn title_and_tags_write_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup("body").await;
    let write = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        async move {
            services
                .update_note_metadata(
                    ws,
                    id,
                    Some("Renamed".into()),
                    Some(vec!["t".into()]),
                    None,
                    None,
                )
                .await
        }
    };
    assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("note.updateMetadata");
    let note = services.store.get_note(&ws, &id).await.expect("read");
    assert_eq!(note.title, "Renamed");
    assert_eq!(note.tags, vec!["t".to_string()]);
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn note_update_metadata_arm_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup("body").await;
    let write = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        async move {
            services
                .update_note(
                    ws,
                    id,
                    NoteUpdateInput {
                        title: Some("Renamed".into()),
                        ..Default::default()
                    },
                )
                .await
        }
    };
    let note = assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("note.update metadata");
    assert_eq!(note.title, "Renamed");
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn mark_as_task_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup("# Task").await;
    let write = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        async move {
            services
                .mark_as_task(ws, id, "in_progress".into(), vec![], None, None, None, None)
                .await
        }
    };
    assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("task.markAsTask");
    assert_eq!(
        task_status(&services, &ws, &id).await,
        intent_core::TaskStatus::InProgress
    );
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn set_relations_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup_task("# Task").await;
    let dep = NoteId::from("dep");
    services
        .store
        .insert_note(&super::note(&ws, "dep", "# Dep"))
        .await
        .expect("dep note");
    services
        .mark_as_task(
            ws.clone(),
            dep.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("markAsTask dep");
    let write = {
        let (services, ws, id, dep) = (services.clone(), ws.clone(), id.clone(), dep.clone());
        async move {
            services
                .task_set_relations(ws, id, Some(vec![dep]), None)
                .await
        }
    };
    assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("task.setRelations");
    let note = services.store.get_note(&ws, &id).await.expect("read");
    assert_eq!(note.metadata.task.expect("task").depends_on, vec![dep]);
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn assign_agent_waits_for_note_write_lane() {
    let (_tmp, services, ws, id) = setup_task("# Task").await;
    let agent = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";
    let write = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        async move { services.assign_agent(ws, id, agent.into(), None).await }
    };
    assert_waits_for_lane(&services, &ws, &id, write)
        .await
        .expect("task.assignAgent");
    let note = services.store.get_note(&ws, &id).await.expect("read");
    let task = note.metadata.task.expect("task");
    assert_eq!(
        task.assigned_agent_ids,
        vec![intent_core::AgentId::from_string(agent.to_string())]
    );
}

/// Seed `contents` as successive content versions and return the version
/// number holding each, in order.
async fn seed_versions(
    services: &Services,
    ws: &WorkspaceId,
    id: &NoteId,
    contents: &[&str],
) -> Vec<i64> {
    for content in contents {
        services
            .update_note(
                ws.clone(),
                id.clone(),
                NoteUpdateInput {
                    content: Some((*content).into()),
                    ..Default::default()
                },
            )
            .await
            .expect("seed version");
    }
    let mut found = Vec::new();
    for content in contents {
        found.push(version_with(services, ws, id, content).await);
    }
    found
}

async fn version_with(services: &Services, ws: &WorkspaceId, id: &NoteId, content: &str) -> i64 {
    for summary in services
        .store
        .list_note_versions(ws, id)
        .await
        .expect("versions")
    {
        let version = services
            .store
            .get_note_version(ws, id, summary.v)
            .await
            .expect("version");
        if version.content == content {
            return version.v;
        }
    }
    panic!("no version holds {content:?}");
}

/// A task-status write that arrives while a restore is queued on the lane
/// waits for the restore instead of landing first and then being reverted
/// from the restore's copy; the row ends with the restored text *and* the
/// new status.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn status_write_queued_behind_restore_is_kept() {
    let (_tmp, services, ws, id) = setup_task("original").await;
    let target = seed_versions(&services, &ws, &id, &["version one", "version two"]).await[0];

    let lane = services.lock_note_content(&ws, &id).await;
    let restore = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        intent_core::spawn_daemon(async move {
            services.restore_note_version(ws, id, target, None).await
        })
    };
    wait_for_queued_writer(&services, &ws, &id).await;
    let status = {
        let (services, ws, id) = (services.clone(), ws.clone(), id.clone());
        intent_core::spawn_daemon(async move {
            services
                .task_update_note_status(ws, id, "complete".into(), None, None)
                .await
        })
    };
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        async {
            while services.store.note_content_lock_users(&ws, &id) < 3 {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .expect("status write never queued behind the restore");
    assert!(
        !status.is_finished(),
        "status write landed under a held lane"
    );

    drop(lane);
    restore.await.expect("restore task").expect("restore");
    status.await.expect("status task").expect("status");
    assert_eq!(stored_content(&services, &ws, &id).await, "version one");
    assert_eq!(
        task_status(&services, &ws, &id).await,
        intent_core::TaskStatus::Complete
    );
}

async fn add_comment(services: &Services, ws: &WorkspaceId, id: &NoteId, target: &str) -> String {
    let added = services
        .comment_add(
            ws.clone(),
            id.clone(),
            target.into(),
            target.into(),
            "a comment".into(),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("comment.add");
    assert!(added.anchored, "comment anchored");
    added.comment_id
}

/// Restoring a version from before a comment was anchored leaves the
/// comment without markers, so it is flagged orphaned like after any other
/// text write that drops its anchor.
#[intent_test_macros::daemon_test]
async fn restore_orphans_comment_whose_anchor_it_drops() {
    let (_tmp, services, ws, id) = setup("original").await;
    let target = seed_versions(&services, &ws, &id, &["alpha beta gamma"]).await[0];
    let comment_id = add_comment(&services, &ws, &id, "beta").await;
    assert!(stored_content(&services, &ws, &id)
        .await
        .contains(&comment_id));

    services
        .restore_note_version(ws.clone(), id.clone(), target, None)
        .await
        .expect("restore");

    assert_eq!(
        stored_content(&services, &ws, &id).await,
        "alpha beta gamma"
    );
    let comment = services
        .store
        .get_comment(&comment_id)
        .await
        .expect("comment");
    assert_eq!(
        comment.is_orphaned,
        Some(true),
        "restore orphaned the comment"
    );
}

/// Restoring a version that still carries a deleted comment's markers
/// scrubs them, like every other text write's phantom-marker pass.
#[intent_test_macros::daemon_test]
async fn restore_strips_markers_of_deleted_comment() {
    let (_tmp, services, ws, id) = setup("alpha beta gamma").await;
    let comment_id = add_comment(&services, &ws, &id, "beta").await;
    let anchored = stored_content(&services, &ws, &id).await;
    assert!(anchored.contains(&comment_id));
    let target = version_with(&services, &ws, &id, &anchored).await;
    services
        .comment_delete(ws.clone(), id.clone(), comment_id.clone())
        .await
        .expect("comment.delete");
    services
        .update_note(
            ws.clone(),
            id.clone(),
            NoteUpdateInput {
                content: Some("something else".into()),
                ..Default::default()
            },
        )
        .await
        .expect("later edit");

    let restored = services
        .restore_note_version(ws.clone(), id.clone(), target, None)
        .await
        .expect("restore");

    let stored = stored_content(&services, &ws, &id).await;
    assert!(
        !stored.contains(&comment_id),
        "stray markers restored: {stored}"
    );
    assert_eq!(stored, "alpha beta gamma");
    assert_eq!(restored.note.content, stored);
}
