use super::*;
use crate::{note_page_index::Projection, note_prepared_write::Attempt};
use intent_core::NoteVersionAuthor;

fn author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    }
}

async fn finish(
    store: &Store,
    note: &intent_core::Note,
    attempt: Attempt,
) -> intent_core::Result<(i64, i64)> {
    store
        .finish_prepared_note_write(
            note,
            Some(note.rev),
            &author(),
            &note.updated_at,
            &[],
            Some(attempt),
        )
        .await
}

#[tokio::test]
async fn prepared_wrapper_accepts_stale_index_and_commits_exact_owned_input() {
    let (store, _tmp, mut note) = setup("old source").await;
    sqlx::query(
        "UPDATE note SET title='same revision metadata' WHERE workspace_id='pages' AND id='spec'",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    note.content = "new **source** 🙂".into();
    let (rev, version) = store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap();
    assert_eq!(rev, note.rev + 1);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        note.content
    );
    assert_eq!(
        store
            .get_note_version(&note.workspace_id, &note.id, version)
            .await
            .unwrap()
            .content,
        note.content
    );
    note.rev = rev;
    note.content = "immutable preparation".into();
    let prepared = Attempt::try_prepare(&store, &note, rev)
        .await
        .unwrap()
        .unwrap();
    note.content = "mismatched later caller copy".into();
    finish(&store, &note, prepared).await.unwrap();
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "immutable preparation"
    );
}

#[tokio::test]
async fn prepared_and_overflowed_inputs_reject_intervening_metadata_and_recreation() {
    for overflow in [0, 1, 2] {
        for mutation in ["metadata", "recreate", "content", "profile"] {
            let (store, _tmp, mut note) = setup("original").await;
            note.content = "intended **replacement**".into();
            let attempt = if overflow != 0 {
                Attempt::try_prepare_using(&store, &note, note.rev, move |note| {
                    Projection::prepare_with_limits(
                        note,
                        if overflow == 1 { 1 } else { usize::MAX },
                        0,
                    )
                })
                .await
            } else {
                Attempt::try_prepare(&store, &note, note.rev).await
            }
            .unwrap()
            .unwrap();
            match mutation {
                "metadata" => {
                    sqlx::query("UPDATE note SET title='new live title' WHERE workspace_id='pages' AND id='spec'").execute(store.write_pool()).await.unwrap();
                }
                "recreate" => {
                    store
                        .delete_note(&note.workspace_id, &note.id)
                        .await
                        .unwrap();
                    let mut replacement = note.clone();
                    replacement.content = "new incarnation at the same revision".into();
                    store.insert_note(&replacement).await.unwrap();
                }
                "content" => {
                    let mut edited = note.clone();
                    edited.content = "concurrent edit or restored content".into();
                    store.update_note(&edited).await.unwrap();
                }
                "profile" => {
                    sqlx::query("UPDATE note_page_head SET profile_revision='different' WHERE workspace_id='pages' AND note_id='spec'").execute(store.write_pool()).await.unwrap();
                }
                _ => unreachable!(),
            }
            let before = store.get_note(&note.workspace_id, &note.id).await.unwrap();
            assert!(
                matches!(
                    finish(&store, &note, attempt).await,
                    Err(Error::Conflict { .. })
                ),
                "{overflow} {mutation}"
            );
            assert_eq!(
                store.get_note(&note.workspace_id, &note.id).await.unwrap(),
                before
            );
            assert!(store
                .list_note_versions(&note.workspace_id, &note.id)
                .await
                .unwrap()
                .is_empty());
        }
    }
}

#[tokio::test]
async fn prepared_delete_is_not_found_and_late_failure_rolls_back_snapshot() {
    let (store, _tmp, mut note) = setup("original").await;
    let attempt = Attempt::try_prepare(&store, &note, note.rev)
        .await
        .unwrap()
        .unwrap();
    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    assert!(matches!(
        finish(&store, &note, attempt).await,
        Err(Error::NotFound(_))
    ));
    store.insert_note(&note).await.unwrap();
    let before = page(&store, json!({"kind":"source"})).await;
    sqlx::query("CREATE TRIGGER fail_prepared_batch BEFORE INSERT ON note_page_entry WHEN (SELECT count(*) FROM note_page_entry)>=200 BEGIN SELECT RAISE(ABORT,'prepared batch failure'); END").execute(store.write_pool()).await.unwrap();
    note.content = "## Heading\n\n**body** and `code`\n\n".repeat(64);
    assert!(store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .is_err());
    assert_eq!(page(&store, json!({"kind":"source","snapshotId":before["snapshotId"],"sourceRevision":before["sourceRevision"],"noteInstanceId":before["scope"]["noteInstanceId"]})).await, before);
    assert!(store
        .list_note_versions(&note.workspace_id, &note.id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.note_preparation.available_permits(), 1);
}

#[tokio::test]
async fn abandoned_cpu_keeps_permit_without_holding_writer_or_publishing() {
    let (store, _tmp, mut note) = setup("original").await;
    note.content = "must never publish".into();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let work_store = store.clone();
    let work_note = note.clone();
    let task = tokio::spawn(async move {
        Attempt::try_prepare_using(&work_store, &work_note, work_note.rev, move |note| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            Projection::prepare(note)
        })
        .await
    });
    ready.await.unwrap();
    assert_eq!(store.note_preparation.available_permits(), 0);
    // No read or write pool connection is retained by the blocked CPU job.
    let conn = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.write_pool().acquire(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(conn);
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(store.note_preparation.available_permits(), 0);
    assert!(Attempt::try_prepare(&store.clone(), &note, note.rev)
        .await
        .unwrap()
        .is_none());
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.note_preparation.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "original"
    );
    store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancellation_after_preparation_before_writer_releases_resources() {
    let (store, _tmp, mut note) = setup("original").await;
    note.content = "abandoned prepared source".into();
    let attempt = Attempt::try_prepare(&store, &note, note.rev)
        .await
        .unwrap()
        .unwrap();
    let held = store.write_pool().acquire().await.unwrap();
    let work_store = store.clone();
    let task = tokio::spawn(async move { finish(&work_store, &note, attempt).await });
    tokio::task::yield_now().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    drop(held);
    assert_eq!(store.note_preparation.available_permits(), 1);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "original"
    );
}

#[tokio::test]
async fn busy_and_inadmissible_inputs_keep_the_existing_save_path() {
    let (store, _tmp, mut note) = setup("original").await;
    let held = store
        .note_preparation
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    note.content = "busy fast path still saves".into();
    note.rev = store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap()
        .0;
    drop(held);
    note.content = "x".repeat(2 * 1024 * 1024 + 1);
    assert!(Attempt::try_prepare(&store, &note, note.rev)
        .await
        .unwrap()
        .is_none());
    store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        note.content
    );
}

#[tokio::test]
async fn overflow_without_drift_still_saves_and_retires_old_snapshot() {
    let (store, _tmp, mut note) = setup("original").await;
    let old = page(&store, json!({"kind":"source"})).await;
    note.content = "replacement with **new** content".into();
    let attempt = Attempt::try_prepare_using(&store, &note, note.rev, |note| {
        Projection::prepare_with_limits(note, 1, 0)
    })
    .await
    .unwrap()
    .unwrap();
    finish(&store, &note, attempt).await.unwrap();
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        note.content
    );
    assert!(store.read_note_page("pages", "spec", "alice", request(json!({"kind":"source","snapshotId":old["snapshotId"],"sourceRevision":old["sourceRevision"],"noteInstanceId":old["scope"]["noteInstanceId"]})), &json!(1)).await.is_err());
}

#[test]
fn cancelled_queued_blocking_job_keeps_admission_until_it_drops() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (store, _tmp, note) = setup("original").await;
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
        ready.await.unwrap();
        let work_store = store.clone();
        let task =
            tokio::spawn(async move { Attempt::try_prepare(&work_store, &note, note.rev).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while store.note_preparation.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(store.note_preparation.available_permits(), 0);
        release.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while store.note_preparation.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            page(&store, json!({"kind":"source"})).await["text"],
            "original"
        );
    });
}

#[tokio::test]
async fn cancellation_drops_preparation_while_existing_sqlite_cleanup_settles() {
    let (store, _tmp, mut note) = setup("original").await;
    note.content = "abandoned SQL write".into();
    let observer = Arc::new(());
    let projection_lifetime = Arc::downgrade(&observer);
    let attempt = Attempt::try_prepare_using(&store, &note, note.rev, move |note| {
        let mut projection = Projection::prepare(note)?.unwrap();
        projection.drop_observer = Some(observer);
        Ok(Some(projection))
    })
    .await
    .unwrap()
    .unwrap();
    let input_lifetime = Arc::downgrade(&attempt.note());
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    {
        let mut conn = store.write_pool().acquire().await.unwrap();
        let mut started = Some(started);
        conn.lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |event| {
                if event.table == "note" {
                    if let Some(started) = started.take() {
                        started.send(()).unwrap();
                        wait.recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap();
                    }
                }
            });
    }
    let work_store = store.clone();
    let work_note = note.clone();
    let task = tokio::spawn(async move { finish(&work_store, &work_note, attempt).await });
    ready.await.unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    // The worker is still paused inside SQLite. Only its current statement and
    // existing connection cleanup outlive the caller, not the prepared buffers.
    assert!(projection_lifetime.upgrade().is_none());
    assert!(input_lifetime.upgrade().is_none());
    assert_eq!(store.note_preparation.available_permits(), 1);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "original"
    );
    note.content = "following writer".into();
    let next = Attempt::try_prepare(&store, &note, note.rev)
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    finish(&store, &note, next).await.unwrap();
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "following writer"
    );
    assert_eq!(
        store
            .list_note_versions(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .len(),
        1
    );
}
