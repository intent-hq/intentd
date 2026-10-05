use super::super::{AnchorFilter, AnnotationKind, AnnotationPageRequest, AnnotationRange, Query};
use super::*;
use serde_json::json;
use std::time::{Duration, Instant};

async fn fixture(count: i64) -> (tempfile::TempDir, Store, Lease, AnnotationEpochs) {
    let dir = tempfile::tempdir().unwrap();
    let source_started = Instant::now();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let workspace = serde_json::from_value(json!({"id":"ws","title":"Test","branch":"test","status":"Active","activity":"idle","attention":"unread","createdAt":"date","updatedAt":"date","tags":[],"skipWorktree":true,"isRemote":false,"archived":false})).unwrap();
    store.insert_workspace(&workspace).await.unwrap();
    let note = serde_json::from_value(json!({"id":"spec","workspaceId":"ws","title":"Test","content":"0123456789","contentType":"markdown","tags":[],"isPinned":false,"isArchived":false,"isDefault":false,"parentId":null,"visibility":"workspace","createdAt":"date","updatedAt":"date","rev":0})).unwrap();
    store.insert_note(&note).await.unwrap();
    // Actual legacy writes populate the production projection, roots, counters,
    // and detail tables; no prebuilt snapshot stands in for preparation.
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO comment(id,workspace_id,note_id,thread_id,kind,content,author,author_type,status,anchor_json,created_at,updated_at) SELECT printf('dense%06d',x),'ws','spec',printf('dense%06d',x),'comment','body','author','user','open','null','date','date' FROM n")
        .bind(count).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO note_comment_anchor(head_id,comment_id,occurrence_id,thread_id,start,end) SELECT head_id,comment_id,'occurrence',thread_id,0,10 FROM note_comment_projection")
        .execute(store.write_pool()).await.unwrap();
    sqlx::query("UPDATE note_annotation_head SET anchors_rev=source_rev")
        .execute(store.write_pool())
        .await
        .unwrap();
    let ws = WorkspaceId::from("ws");
    let note = NoteId::from("spec");
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let state = store.read_note_page_state(&ws, &note, None).await.unwrap();
    let lease = Lease {
        scope: serde_json::from_value(state["scope"].clone()).unwrap(),
        principal: "alice".into(),
        source_revision: state["sourceRevision"].as_str().unwrap().into(),
        epoch: epochs.comment_revision.clone(),
        query: Query {
            kind: AnnotationKind::Comments,
            ranges: vec![AnnotationRange { start: 1, end: 2 }],
            anchor_state: AnchorFilter::Anchored,
            thread_id: None,
            items: 64,
            wire: 65_536,
        },
        expires_at: intent_core::iso_ms_from_now(300_000),
        expires_ms: i64::try_from(intent_core::now_epoch_ms()).unwrap() + 300_000,
    };
    let cover_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor_cover")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    eprintln!("source fixture {count} comments+anchors+detail projections: elapsed={:?}, coverRows={cover_rows}, dbBytes={}, WALBytes={}; source write cost, separate from query preparation",source_started.elapsed(),std::fs::metadata(dir.path().join("store.db")).unwrap().len(),std::fs::metadata(dir.path().join("store.db-wal")).map_or(0,|m|m.len()));
    (dir, store, lease, epochs)
}

async fn retained(store: &Store, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_snapshot WHERE id=?")
        .bind(id.simple().to_string())
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn annotation_actual_dense_preparation_cancellation_and_expiry() {
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (_dir, store, lease, epochs) = fixture(100_001).await;
    let id = store.save_annotation_lease(&lease).await.unwrap();
    let work = Arc::new(AtomicUsize::new(0));
    let task = {
        let store = store.clone();
        let lease = lease.clone();
        let epochs = epochs.clone();
        let work = Arc::clone(&work);
        tokio::spawn(async move {
            store
                .prepare_annotation_matches_observed(&lease, id, &epochs, work)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while work.load(Ordering::Relaxed) < 10_000 {
            assert!(
                !task.is_finished(),
                "preparation completed before cancellation observation"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Cancel the real request after SQLite has started its real matching query.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let permit = tokio::time::timeout(Duration::from_secs(10), PREPARATION_SLOT.acquire())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained(&store, id).await,
        0,
        "worker owns admission through retirement"
    );
    let partial: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_match_head")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(partial, 0);
    sqlx::raw_sql("BEGIN IMMEDIATE; ROLLBACK;")
        .execute(store.write_pool())
        .await
        .unwrap();
    drop(permit);

    // An originally short lease expires during actual dense preparation. It
    // cannot be renewed into a successful page or retained query snapshot.
    let mut short = lease.clone();
    short.expires_ms = i64::try_from(intent_core::now_epoch_ms()).unwrap() + 100;
    short.expires_at = intent_core::iso_ms_from_now(100);
    let id = store.save_annotation_lease(&short).await.unwrap();
    let work = Arc::new(AtomicUsize::new(0));
    let result = store
        .prepare_annotation_matches_observed(&short, id, &epochs, Arc::clone(&work))
        .await;
    assert!(
        matches!(
            result,
            Err(intent_core::Error::NotePage(NotePageError::Expired))
        ),
        "{result:?}"
    );
    assert!(
        work.load(Ordering::Relaxed) > 0,
        "expiry must cross an executing preparation"
    );
    assert_eq!(retained(&store, id).await, 0);
}

#[tokio::test]
async fn annotation_actual_dense_first_page_and_complete_continuation() {
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (dir, store, lease, _epochs) = fixture(100_001).await;
    let before_bytes = std::fs::metadata(dir.path().join("store.db"))
        .unwrap()
        .len();
    let started = Instant::now();
    let mut request = AnnotationPageRequest {
        kind: AnnotationKind::Comments,
        ranges: Some(lease.query.ranges.clone()),
        anchor_state: None,
        cursor: None,
        max_items: Some(64),
        max_wire_bytes: Some(65_536),
    };
    let mut page = store
        .read_note_annotation_page(
            "alice",
            &lease.scope,
            &lease.source_revision,
            None,
            None,
            &request,
            &json!(1),
        )
        .await
        .unwrap();
    let prepare_time = started.elapsed();
    let snapshot = page["snapshotId"].as_str().unwrap().to_owned();
    let steps: i64 = sqlx::query_scalar(
        "SELECT prepare_steps FROM note_annotation_match_head WHERE snapshot_id=?",
    )
    .bind(&snapshot)
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    let prepared_bytes = std::fs::metadata(dir.path().join("store.db"))
        .unwrap()
        .len();
    let wal_bytes = std::fs::metadata(dir.path().join("store.db-wal")).map_or(0, |m| m.len());
    eprintln!("dense first page completed: elapsed={prepare_time:?}, preparationVMsteps={steps}");
    let continuation = Instant::now();
    let mut seen = 0;
    loop {
        assert_eq!(page["totalThreads"], 100_001);
        assert_eq!(page["totalComments"], 100_001);
        assert!(super::super::wire_len(&page, &json!(1)) <= 65_536);
        for item in page["items"].as_array().unwrap() {
            seen += 1;
            assert_eq!(item["threadId"], format!("dense{seen:06}"));
            assert_eq!(item["rootCommentId"], item["threadId"]);
            assert_eq!(item["rootState"], "present");
        }
        if seen % 16_384 == 0 {
            eprintln!(
                "dense continuation progress: seen={seen}, elapsed={:?}",
                continuation.elapsed()
            );
        }
        let Some(cursor) = page["nextCursor"].as_str() else {
            break;
        };
        request.cursor = Some(cursor.into());
        page = store
            .read_note_annotation_page(
                "alice",
                &lease.scope,
                &lease.source_revision,
                Some(&lease.epoch),
                None,
                &request,
                &json!(1),
            )
            .await
            .unwrap();
    }
    assert_eq!(seen, 100_001);
    eprintln!("actual dense100001 preparation={prepare_time:?} VMsteps={steps} dbBefore={before_bytes} dbAfter={prepared_bytes} postPreparationWAL={wal_bytes} allContinuations={:?}; these are observed sizes, not peak transient bounds",continuation.elapsed());
    let id = Uuid::parse_str(&snapshot).unwrap();
    let retained_lease = store.annotation_lease(id).await.unwrap();
    store.retire_annotation_lease(id).await.unwrap();
    assert!(matches!(
        store
            .validate_annotation_lease(id, &retained_lease, &lease.scope, "alice")
            .await,
        Err(intent_core::Error::NotePage(NotePageError::Expired))
    ));
}

#[tokio::test]
async fn annotation_indexed_seek_preserves_earliest_matching_occurrence_and_half_open_points() {
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (_dir, store, lease, _epochs) = fixture(5).await;
    sqlx::query("DELETE FROM note_comment_anchor")
        .execute(store.write_pool())
        .await
        .unwrap();
    for (thread, occurrence, start, end) in [
        (1, 1, 0, 10),
        (1, 2, 8, 10),
        (2, 1, 2, 2),
        (2, 2, 4, 4),
        (3, 1, 4, 8),
        (3, 2, 8, 8),
        (4, 1, 0, 2),
        (4, 2, 9, 9),
        (5, 1, 0, 1),
        (5, 2, 8, 10),
    ] {
        sqlx::query("INSERT INTO note_comment_anchor(head_id,comment_id,occurrence_id,thread_id,start,end) SELECT head_id,comment_id,?,thread_id,?,? FROM note_comment_projection WHERE comment_id=?")
            .bind(format!("occurrence{occurrence}")).bind(start).bind(end).bind(format!("dense{thread:06}")).execute(store.write_pool()).await.unwrap();
    }
    let mut request:AnnotationPageRequest=serde_json::from_value(json!({"kind":"comments","ranges":[{"start":2,"end":4},{"start":8,"end":9}],"maxItems":1,"maxWireBytes":4096})).unwrap();
    for expected in ["dense000001", "dense000002", "dense000003", "dense000005"] {
        let page = store
            .read_note_annotation_page(
                "alice",
                &lease.scope,
                &lease.source_revision,
                Some(&lease.epoch),
                None,
                &request,
                &json!(1),
            )
            .await
            .unwrap();
        assert_eq!(page["totalThreads"], 4);
        assert_eq!(page["totalComments"], 4);
        assert_eq!(page["items"][0]["threadId"], expected);
        request.cursor = page["nextCursor"].as_str().map(str::to_owned);
    }
    assert!(request.cursor.is_none());
    request.ranges = Some(vec![]);
    let empty = store
        .read_note_annotation_page(
            "alice",
            &lease.scope,
            &lease.source_revision,
            Some(&lease.epoch),
            None,
            &request,
            &json!(1),
        )
        .await
        .unwrap();
    assert_eq!(empty["totalThreads"], 0);
    assert_eq!(empty["items"], json!([]));
    // Maximum admitted query shape still compiles and executes the production
    // seek, including many empty buckets and half-open point boundaries.
    request.ranges = Some(
        (0..32)
            .map(|i| AnnotationRange {
                start: i * 3,
                end: i * 3 + 1,
            })
            .collect(),
    );
    let page = store
        .read_note_annotation_page(
            "alice",
            &lease.scope,
            &lease.source_revision,
            Some(&lease.epoch),
            None,
            &request,
            &json!(1),
        )
        .await
        .unwrap();
    assert!(!page["items"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn annotation_duplicate_stream_prefixes_and_source_cover_mutations_remain_exact() {
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (_dir, store, lease, epochs) = fixture(3).await;
    let started = Instant::now();
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO note_comment_anchor(head_id,comment_id,occurrence_id,thread_id,start,end) SELECT p.head_id,p.comment_id,printf('duplicate%05d',x),p.thread_id,0,10 FROM n CROSS JOIN note_comment_projection p WHERE p.comment_id='dense000001'")
        .execute(store.write_pool()).await.unwrap();
    eprintln!(
        "10000 duplicate source anchor inserts including coverage index: {:?}",
        started.elapsed()
    );
    let head: i64 = sqlx::query_scalar("SELECT id FROM note_annotation_head")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    let ranges = [
        SourceRange { start: 1, end: 2 },
        SourceRange { start: 8, end: 9 },
    ];
    let mut conn = store.read_pool().acquire().await.unwrap();
    for (name, after, expected) in [
        ("first", None, "dense000001"),
        ("late", Some((1, "dense000001")), "dense000002"),
    ] {
        let work = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&work);
        conn.lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                counter.fetch_add(1, Ordering::Relaxed);
                true
            });
        let rows = match_summary_query(head, &ranges, 0, after, 2)
            .build()
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        conn.lock_handle().await.unwrap().remove_progress_handler();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<String, _>("thread_id"), expected);
        eprintln!(
            "single-thread10001anchors / same-position3threads {name} production page VMsteps={}",
            work.load(Ordering::Relaxed)
        );
    }
    drop(conn);
    let mut multi = lease.clone();
    multi.query.ranges = ranges
        .iter()
        .map(|r| AnnotationRange {
            start: r.start,
            end: r.end,
        })
        .collect();
    let id = store.save_annotation_lease(&multi).await.unwrap();
    store
        .prepare_annotation_matches(&multi, id, &epochs)
        .await
        .unwrap();
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT total_threads,total_comments FROM note_annotation_match_head WHERE snapshot_id=?",
    )
    .bind(id.simple().to_string())
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(counts, (3, 3));
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor_cover")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::query("DELETE FROM note_comment_anchor WHERE comment_id='dense000001'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_comment_anchor_cover")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        before
    );
    // Synthetic index coordinates exercise exact safe-integer arithmetic without
    // allocating a document of that size. One interval, no lossy float seek.
    sqlx::query("UPDATE note_comment_anchor SET start=1,end=9007199254740991 WHERE comment_id='dense000003'").execute(store.write_pool()).await.unwrap();
    let cells: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note_comment_anchor_cover WHERE thread_id='dense000003'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert!(cells <= 106);
    let rows = match_summary_query(
        head,
        &[SourceRange {
            start: 9_007_199_254_740_990,
            end: 9_007_199_254_740_991,
        }],
        0,
        None,
        2,
    )
    .build()
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("thread_id"), "dense000003");
    sqlx::query("DELETE FROM note_comment_anchor WHERE comment_id='dense000003'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM note_comment_anchor_cover WHERE thread_id='dense000003'"
        )
        .fetch_one(store.read_pool())
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn annotation_setup_failure_closes_detached_worker_and_preserves_cleanup_errors() {
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (_dir, store, _lease, _epochs) = fixture(1).await;
    sqlx::query("CREATE TABLE cleanup_probe(value INTEGER)")
        .execute(store.write_pool())
        .await
        .unwrap();
    let permit = PREPARATION_SLOT.acquire().await.unwrap();
    let mut conn = store.write_pool().acquire().await.unwrap().detach();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("INSERT INTO cleanup_probe VALUES(1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let error = sqlx::query("PRAGMA cache_size = (")
        .execute(&mut *tx)
        .await
        .unwrap_err();
    drop(tx);
    let primary = db_error(error);
    let expected = primary.to_string();
    let result = close_preparation_connection(conn, Err(primary)).await;
    assert_eq!(result.unwrap_err().to_string(), expected);
    drop(permit);
    let _reacquired = PREPARATION_SLOT.try_acquire().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cleanup_probe")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        0
    );
    let result = preparation_cleanup_result(
        Err(intent_core::Error::Internal("primary setup error".into())),
        Err(intent_core::Error::Internal("close error".into())),
    )
    .unwrap_err()
    .to_string();
    assert!(result.contains("primary setup error") && result.contains("close error"));
}

#[tokio::test]
async fn annotation_summary_projects_scoped_cursor_and_root_or_survivor_detail_owners() {
    let (_dir, store, lease, _) = fixture(2).await;
    sqlx::query("INSERT INTO comment(id,workspace_id,note_id,thread_id,parent_id,kind,content,author,author_type,status,anchor_json,created_at,updated_at) VALUES('reply','ws','spec','dense000001','dense000001','comment','survivor body','author','user','open','null','later','later')")
        .execute(store.write_pool()).await.unwrap();
    let request: AnnotationPageRequest = serde_json::from_value(json!({
        "kind":"comments","ranges":[],"anchorState":"all","maxItems":1,"maxWireBytes":4096
    }))
    .unwrap();
    let key = store.annotation_key().await.unwrap();
    for (deleted, expected_detail) in [(false, "dense000001"), (true, "reply")] {
        if deleted {
            sqlx::query("DELETE FROM comment WHERE id='dense000001'")
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        let page = store
            .read_note_annotation_page(
                "alice",
                &lease.scope,
                &lease.source_revision,
                None,
                None,
                &request,
                &json!(1),
            )
            .await
            .unwrap();
        let item = &page["items"][0];
        assert_eq!(item["threadId"], "dense000001");
        assert_eq!(item["rootCommentId"], "dense000001");
        assert_eq!(
            item["rootState"],
            if deleted { "deleted" } else { "present" }
        );
        assert_eq!(item["anchorRef"].is_null(), deleted);
        let detail =
            super::super::Token::decode(item["detailRef"].as_str().unwrap(), &key).unwrap();
        let actual_detail: String = sqlx::query_scalar("SELECT p.comment_id FROM note_comment_projection p JOIN note_annotation_head h ON h.id=p.head_id WHERE h.workspace_id=? AND h.note_id=? AND p.rowid=?")
            .bind(&lease.scope.workspace_id).bind(&lease.scope.note_id)
            .bind(i64::try_from(detail.owner).unwrap()).fetch_one(store.read_pool()).await.unwrap();
        assert_eq!(actual_detail, expected_detail);
        let cursor =
            super::super::Token::decode(page["nextCursor"].as_str().unwrap(), &key).unwrap();
        let thread: String =
            sqlx::query_scalar("SELECT thread_id FROM note_comment_thread WHERE rowid=?")
                .bind(i64::try_from(cursor.owner).unwrap())
                .fetch_one(store.read_pool())
                .await
                .unwrap();
        assert_eq!(thread, "dense000001");
    }
}
