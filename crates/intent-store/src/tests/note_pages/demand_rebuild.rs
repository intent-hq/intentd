//! Cold backlog regressions: canonical rows are seeded without invoking writers.
use super::*;

async fn backlog() -> (Store, TempDb, intent_core::Note) {
    let (store, tmp, note) = setup("# Existing note\n\n[A](intent://local/task/a)\n").await;
    let mut tx = store.write_pool().begin().await.unwrap();
    for index in 0..128 {
        sqlx::query("INSERT INTO note(id,workspace_id,title,content,content_type,tags,is_pinned,is_archived,is_default,parent_id,visibility,task_json,created_at,rev,updated_at) SELECT ?,workspace_id,title,?,content_type,tags,is_pinned,is_archived,0,parent_id,visibility,task_json,created_at,rev,updated_at FROM note WHERE workspace_id='pages' AND id='spec'")
            .bind(format!("unopened-{index}"))
            .bind("ordinary text 😀\r\n".repeat(1024))
            .execute(&mut *tx).await.unwrap();
    }
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1,profile_revision='retired-profile'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    (store, tmp, note)
}

async fn pending(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM note_page_head WHERE indexed_rev=-1")
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn demand_startup_never_converts_a_stale_backlog() {
    let (store, tmp, note) = backlog().await;
    // Reject the physical conversion, not merely a slow elapsed-time result.
    sqlx::query("CREATE TRIGGER reject_startup_conversion BEFORE INSERT ON note_page_piece BEGIN SELECT RAISE(ABORT,'startup converted a note'); END")
        .execute(store.write_pool()).await.unwrap();
    store.close().await;
    drop(store);
    let reopened = Store::open(&tmp.path)
        .await
        .expect("startup must not convert notes");
    assert_eq!(pending(&reopened).await, 129);
    assert_eq!(
        reopened
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        note.content
    );
    reopened.close().await;
    drop(reopened);
    let again = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        pending(&again).await,
        129,
        "restart must not resume an idle backlog"
    );
}

#[tokio::test]
async fn demand_first_access_converts_only_the_requested_note() {
    let (store, _tmp, note) = backlog().await;
    let first = page(&store, json!({"kind":"source"})).await;
    assert_eq!(first["text"], note.content);
    assert_eq!(pending(&store).await, 128);
    let tasks = page(&store, json!({"kind":"taskIds"})).await;
    assert_eq!(tasks["totalItems"], 1);
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_delete_does_not_convert_unrelated_notes() {
    let (store, _tmp, note) = backlog().await;
    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_full_write_does_not_convert_unrelated_notes() {
    let (store, _tmp, mut note) = backlog().await;
    note.content = "updated canonical source".into();
    store.update_note(&note).await.unwrap();
    assert_eq!(pending(&store).await, 128);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        note.content
    );
}

async fn start_paused(
    store: &Store,
    staged: bool,
) -> (
    std::sync::Arc<crate::note_index_job::Pause>,
    tokio::task::JoinHandle<intent_core::Result<()>>,
) {
    let pause = std::sync::Arc::new(crate::note_index_job::Pause {
        staged,
        ..Default::default()
    });
    *store.note_index_jobs.pause.lock().unwrap() = Some(pause.clone());
    let demand = store.clone();
    let request =
        tokio::spawn(async move { demand.ensure_note_index("pages", "spec", true).await });
    tokio::time::timeout(super::wait(), pause.entered.notified())
        .await
        .unwrap();
    (pause, request)
}

pub(super) async fn settled(store: &Store) -> intent_core::Result<()> {
    // Renew the demand lease until the physical job settles. A timeout is not a
    // successful empty page and must not allocate a page snapshot.
    tokio::time::timeout(super::wait(), async {
        loop {
            let result = store.ensure_note_index("pages", "spec", true).await;
            if !matches!(
                result,
                Err(intent_core::Error::NotePage(
                    intent_core::note_page::NotePageError::Preparing
                ))
            ) {
                return result;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn demand_concurrent_access_coalesces_across_store_handles() {
    let (store, tmp, _) = backlog().await;
    let other = Store::open(&tmp.path).await.unwrap();
    let (pause, first) = start_paused(&store, false).await;
    let request = tokio::spawn(async move { settled(&other).await });
    tokio::task::yield_now().await;
    pause.release.notify_one();
    let _ = first.await.unwrap();
    request.await.unwrap().unwrap();
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_edit_during_conversion_cannot_publish_obsolete_source() {
    let (store, _tmp, mut note) = backlog().await;
    let (pause, request) = start_paused(&store, false).await;
    note.content = "newer canonical source 😀".into();
    store.update_note(&note).await.unwrap();
    pause.release.notify_one();
    assert!(matches!(
        request.await.unwrap(),
        Err(intent_core::Error::NotePage(
            intent_core::note_page::NotePageError::Stale
        ))
    ));
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        note.content
    );
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_delete_during_staging_never_resurrects_a_note() {
    let (store, _tmp, note) = backlog().await;
    let (pause, request) = start_paused(&store, true).await;
    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    pause.release.notify_one();
    assert!(matches!(
        request.await.unwrap(),
        Err(intent_core::Error::NotFound(_))
    ));
    assert!(!store
        .note_exists(&note.workspace_id, &note.id)
        .await
        .unwrap());
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_metadata_update_keeps_retired_content_pending() {
    let (store, _tmp, mut note) = backlog().await;
    note.title = "new title".into();
    note.content.clear(); // A metadata-only caller deliberately omits source.
    store.update_note_metadata(&note).await.unwrap();
    assert_eq!(pending(&store).await, 129);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "# Existing note\n\n[A](intent://local/task/a)\n"
    );
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_old_cursor_expires_without_scheduling_a_build() {
    let (store, _tmp, _) = setup("hello".repeat(4096).as_str()).await;
    let first = page(&store, json!({"kind":"source","maxSourceBytes":1024})).await;
    sqlx::query("UPDATE note_page_head SET profile_revision='retired-profile'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let result = store
        .read_note_page(
            "pages",
            "spec",
            "alice",
            serde_json::from_value(
                json!({"kind":"source","cursor":first["nextCursor"],"maxSourceBytes":1024}),
            )
            .unwrap(),
            &json!(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(intent_core::Error::NotePage(
            intent_core::note_page::NotePageError::Expired
        ))
    ));
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn demand_retired_profile_expires_even_after_revision_change() {
    let (store, _tmp, _) = setup("hello".repeat(4096).as_str()).await;
    let first = page(&store, json!({"kind":"source","maxSourceBytes":1024})).await;
    sqlx::query(
        "UPDATE note_page_head SET profile_revision='retired-profile',current_rev=current_rev+1",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    let result = store
        .read_note_page(
            "pages",
            "spec",
            "alice",
            serde_json::from_value(
                json!({"kind":"source","cursor":first["nextCursor"],"maxSourceBytes":1024}),
            )
            .unwrap(),
            &json!(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(intent_core::Error::NotePage(
            intent_core::note_page::NotePageError::Expired
        ))
    ));
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn demand_shutdown_discards_staging_and_restart_needs_fresh_demand() {
    let (store, tmp, note) = backlog().await;
    let (pause, request) = start_paused(&store, true).await;
    let closing = store.clone();
    let close = tokio::spawn(async move { closing.close().await });
    // close must retain physical job ownership until the paused write settles.
    tokio::time::timeout(super::wait(), async {
        while !store
            .note_index_owner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!close.is_finished());
    pause.release.notify_one();
    tokio::time::timeout(super::wait(), close)
        .await
        .unwrap()
        .unwrap();
    let _ = request.await.unwrap();
    drop(store);
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_eq!(pending(&reopened).await, 129);
    assert_eq!(
        reopened
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        note.content
    );
    settled(&reopened).await.unwrap();
    assert_eq!(
        page(&reopened, json!({"kind":"source"})).await["text"],
        note.content
    );
    assert_eq!(pending(&reopened).await, 128);
}

#[tokio::test]
async fn demand_closing_one_handle_does_not_close_another_handle() {
    let (store, tmp, _) = backlog().await;
    let other = Store::open(&tmp.path).await.unwrap();
    store.close().await;
    settled(&other).await.unwrap();
    assert_eq!(pending(&other).await, 128);
}

#[tokio::test]
async fn demand_import_leaves_all_projections_pending_until_accessed() {
    let (source, _tmp, note) = backlog().await;
    let rows = source
        .transfer_export_rows(&note.workspace_id)
        .await
        .unwrap();
    let target_db = TempDb::new();
    let target = Store::open(&target_db.path).await.unwrap();
    target.transfer_import_rows(&rows).await.unwrap();
    assert_eq!(pending(&target).await, 129);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM note_page_piece")
        .fetch_one(target.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    settled(&target).await.unwrap();
    assert_eq!(
        page(&target, json!({"kind":"source"})).await["text"],
        note.content
    );
    assert_eq!(pending(&target).await, 128);
}

#[tokio::test]
async fn demand_oversized_source_returns_resource_error_without_publication() {
    let (store, _tmp, _) = setup("small").await;
    sqlx::query("UPDATE note SET content=CAST(zeroblob(?) AS TEXT),rev=rev+1 WHERE id='spec'")
        .bind(i64::try_from(crate::note_index_budget::SOURCE_BYTES + 1).unwrap())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        settled(&store).await,
        Err(Error::NotePage(NotePageError::ResourceLimit))
    ));
    assert_eq!(pending(&store).await, 1);
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn demand_timeout_keeps_one_owned_job_for_retry() {
    let (store, _tmp, _) = backlog().await;
    let (pause, request) = start_paused(&store, false).await;
    assert!(matches!(
        request.await.unwrap(),
        Err(Error::NotePage(NotePageError::Preparing))
    ));
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    *store.note_index_jobs.pause.lock().unwrap() = None;
    pause.release.notify_one();
    settled(&store).await.unwrap();
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn demand_projection_matches_canonical_writer_exactly() {
    async fn rows(store: &Store) -> Vec<String> {
        sqlx::query_scalar("SELECT json_array(collection,position,value,source_start,source_end) FROM note_page_entry WHERE workspace_id='pages' AND note_id='spec' ORDER BY collection,position")
            .fetch_all(store.read_pool()).await.unwrap()
    }
    let source = "# Heading 😀\r\n\n- **one**\n  - two\n\n| a | b |\n|---|---|\n|中|e\u{301}|\n\n<table><tr><td>x &amp; y</td></tr></table>\n\n```mermaid\ngraph TD; A-->B\n```\n\n[Task](intent://local/task/task-a)\n<!--anchor:root:point-->";
    let (store, _tmp, note) = setup(source).await;
    let expected = rows(&store).await;
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1,profile_revision='retired-profile'")
        .execute(store.write_pool())
        .await
        .unwrap();
    settled(&store).await.unwrap();
    assert_eq!(rows(&store).await, expected);
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
}

#[tokio::test]
async fn demand_root_change_during_conversion_rejects_old_annotation_inputs() {
    let (store, _tmp, mut note) = backlog().await;
    note.content.push_str("<!--anchor:root:point-->");
    store.update_note(&note).await.unwrap();
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1 WHERE note_id='spec'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let (pause, first) = start_paused(&store, false).await;
    sqlx::query("INSERT INTO comment(id,thread_id,note_id,workspace_id,kind,content,author,author_type,anchor_json,created_at,updated_at) VALUES('root','root','spec','pages','comment','body','author','user','{}','now','now')")
        .execute(store.write_pool()).await.unwrap();
    *store.note_index_jobs.pause.lock().unwrap() = None;
    pause.release.notify_one();
    assert!(matches!(
        first.await.unwrap(),
        Err(Error::NotePage(NotePageError::Stale))
    ));
    settled(&store).await.unwrap();
    let roots: Vec<String> = sqlx::query_scalar("SELECT comment_id FROM note_comment_anchor")
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert_eq!(roots, vec!["root"]);
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_deep_html_hits_owned_resource_boundary_without_staging() {
    let (store, _tmp, mut note) = setup("small").await;
    note.content = format!("{}text{}", "<div>".repeat(512), "</div>".repeat(512));
    let result = tokio::task::spawn_blocking(move || {
        crate::note_index_budget::run(std::sync::Arc::new(|| true), || {
            crate::note_page_index::prepare(&note)
        })
    })
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(Error::NotePage(NotePageError::ResourceLimit))
    ));
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "small"
    );
}

#[tokio::test]
async fn demand_aborted_waiter_does_not_cancel_shared_preparation() {
    let (store, _tmp, _) = backlog().await;
    let (pause, first) = start_paused(&store, false).await;
    first.abort();
    let _ = first.await;
    let other = store.clone();
    let second = tokio::spawn(async move { settled(&other).await });
    pause.release.notify_one();
    second.await.unwrap().unwrap();
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_expired_lease_keeps_staging_unpublished_until_fresh_demand() {
    let (store, _tmp, _) = backlog().await;
    let (pause, first) = start_paused(&store, true).await;
    store.note_index_jobs.expire_demands();
    *store.note_index_jobs.pause.lock().unwrap() = None;
    pause.release.notify_one();
    assert!(matches!(
        first.await.unwrap(),
        Err(Error::NotePage(NotePageError::Preparing))
    ));
    assert_eq!(pending(&store).await, 129);
    settled(&store).await.unwrap();
    assert_eq!(pending(&store).await, 128);
}

#[tokio::test]
async fn demand_queue_limit_never_hydrates_waiting_notes() {
    let (store, _tmp, _) = backlog().await;
    let (pause, first) = start_paused(&store, false).await;
    let mut requests = Vec::new();
    for index in 0..31 {
        let other = store.clone();
        requests.push(tokio::spawn(async move {
            other
                .ensure_note_index("pages", &format!("unopened-{index}"), false)
                .await
        }));
    }
    tokio::time::timeout(super::wait(), async {
        while store.note_index_jobs.queued_identities() < 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        store.ensure_note_index("pages", "unopened-31", false).await,
        Err(Error::NotePage(NotePageError::Preparing))
    ));
    assert_eq!(store.note_index_jobs.queued_identities(), 32);
    assert_eq!(
        store
            .note_index_jobs
            .builds
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(pending(&store).await, 129);
    store.note_index_jobs.expire_demands();
    pause.release.notify_one();
    let _ = first.await.unwrap();
    for request in requests {
        let _ = request.await.unwrap();
    }
    store.close().await;
}
