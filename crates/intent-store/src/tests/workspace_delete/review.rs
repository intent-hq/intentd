//! Query work and cancellation regressions from the independent store review.

use super::{seed_heavy_workspace_children, seed_tab_and_draft, seed_workspace, TempDb};
use crate::agent_repo::{delete_agent_metadata_batch_statements, DELETE_AGENT_SESSION_SQL};
use crate::workspace_repo::{
    CLEAR_NOTE_PARENT_BATCH_SQL, DELETE_NOTE_COMMENT_BATCH_SQL, DELETE_WORKSPACE_BROWSER_BATCH_SQL,
    DELETE_WORKSPACE_SQL,
};
use crate::Store;
use intent_core::{ClientHostInfo, ClientId, Error};
use sqlx::Row;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Block the worker inside its first update of this table, so the test can
/// suspend the caller before allowing the statement to finish.
async fn block_next_update(
    store: &Store,
    table: &'static str,
) -> (Arc<tokio::sync::Notify>, std::sync::mpsc::SyncSender<()>) {
    let started = Arc::new(tokio::sync::Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut conn = store.write_pool().acquire().await.unwrap();
    let notify = Arc::clone(&started);
    let mut blocked = Some(blocked);
    conn.lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == table {
                if let Some(blocked) = blocked.take() {
                    notify.notify_one();
                    blocked.recv_timeout(Duration::from_secs(20)).unwrap();
                }
            }
        });
    (started, release)
}

async fn drain_browser_batch(store: &Store) {
    // The batch retains the writer until its overlay eviction is complete.
    let mut conn = store.write_pool().acquire().await.unwrap();
    conn.lock_handle().await.unwrap().remove_update_hook();
}

async fn count_steps(store: &Store) -> Arc<AtomicU64> {
    let steps = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&steps);
    let mut conn = store.write_pool().acquire().await.unwrap();
    conn.lock_handle()
        .await
        .unwrap()
        .set_progress_handler(100, move || {
            counter.fetch_add(100, Ordering::SeqCst);
            true
        });
    steps
}

async fn stop_counting(store: &Store) {
    let mut conn = store.write_pool().acquire().await.unwrap();
    conn.lock_handle().await.unwrap().remove_progress_handler();
}

async fn measure_batch(store: &Store, sql: &str) -> (u64, u64) {
    let counter = count_steps(store).await;
    let rows = sqlx::query(sql)
        .bind("doomed")
        .bind(500_i64)
        .execute(store.write_pool())
        .await
        .unwrap()
        .rows_affected();
    stop_counting(store).await;
    (rows, counter.load(Ordering::SeqCst))
}

async fn plan(store: &Store, sql: &str, bindings: &[&str]) -> Vec<String> {
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query(&explain);
    for binding in bindings {
        query = query.bind(*binding);
    }
    query
        .fetch_all(store.read_pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get("detail"))
        .collect()
}

async fn seed_unrelated_metadata(store: &Store) {
    store
        .upsert_client(
            &ClientId::from("host"),
            None,
            None,
            &ClientHostInfo::default(),
        )
        .await
        .unwrap();
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::query(
        "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<320) \
         INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
         SELECT 'keeper-' || n, 'keeper', 'kept agent', 'idle', 't0', 't0' FROM rows",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
         VALUES ('doomed-agent', 'doomed', 'empty agent', 'idle', 't0', 't0')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO completion_wake_delivery (parent_agent_id, child_agent_id, completion_identity, delivered_at) \
         SELECT p.id, c.id, 'done', 't0' FROM agent_session p CROSS JOIN agent_session c \
         WHERE p.workspace_id='keeper' AND c.workspace_id='keeper' LIMIT 10000",
    ).execute(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO advisory_wake_delivery (parent_agent_id, child_agent_id, delivered_at) \
         SELECT parent_agent_id, child_agent_id, delivered_at FROM completion_wake_delivery",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<10000) \
         INSERT INTO browser_tab (tab_id, workspace_id, host_client_id, url, created_at, updated_at, closed_at) \
         SELECT 'kept-tab-' || n, 'keeper', 'host', 'https://example.test', 't0', 't0', \
                CASE WHEN n%2=0 THEN 't1' END FROM rows",
    ).execute(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO browser_tab (tab_id, workspace_id, host_client_id, url, created_at, updated_at, closed_at) \
         VALUES ('closed-doomed', 'doomed', 'host', 'https://example.test', 't0', 't0', 't1')",
    ).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

/// Inspect explicit batches AND the FK probes hidden inside final deletes.
/// VM instructions also bound the real public deletion path independently
/// of machine load; elapsed time is deliberately not the assertion.
#[tokio::test]
async fn deletion_metadata_queries_do_not_scan_unrelated_rows() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let doomed = seed_workspace(&store, "doomed").await;
    seed_workspace(&store, "keeper").await;
    seed_unrelated_metadata(&store).await;
    let mut details = Vec::new();
    for sql in delete_agent_metadata_batch_statements() {
        details.extend(plan(&store, &sql, &["doomed-agent", "500"]).await);
    }
    details.extend(
        plan(
            &store,
            DELETE_AGENT_SESSION_SQL,
            &["doomed-agent", "doomed"],
        )
        .await,
    );
    details.extend(
        plan(
            &store,
            DELETE_WORKSPACE_BROWSER_BATCH_SQL,
            &["doomed", "500"],
        )
        .await,
    );
    details.extend(
        plan(
            &store,
            "SELECT tab_id FROM browser_tab WHERE workspace_id = ?",
            &["doomed"],
        )
        .await,
    );
    details.extend(plan(&store, DELETE_WORKSPACE_SQL, &["doomed"]).await);
    let scans: Vec<_> = details
        .iter()
        .filter(|detail| {
            [
                "completion_wake_delivery",
                "advisory_wake_delivery",
                "browser_tab",
            ]
            .iter()
            .any(|table| detail.starts_with(&format!("SCAN {table}")))
        })
        .collect();
    let counter = count_steps(&store).await;
    store.delete_workspace(&doomed).await.unwrap();
    stop_counting(&store).await;
    let steps = counter.load(Ordering::SeqCst);
    eprintln!("public deletion with 10k unrelated rows/table: {steps} VM steps; scans: {scans:?}");
    for table in [
        "completion_wake_delivery",
        "advisory_wake_delivery",
        "browser_tab",
    ] {
        let kept: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(kept, 10_000, "unrelated {table} rows preserved");
    }
    assert!(
        scans.is_empty(),
        "deletion must seek its scope, including closed tabs and FK checks: {scans:?}"
    );
    assert!(
        steps < 20_000,
        "unrelated rows must not dominate writer work: {steps}"
    );
}

#[tokio::test]
async fn note_cleanup_does_not_rescan_processed_prefixes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    seed_heavy_workspace_children(&store, &keeper, 3).await;
    sqlx::raw_sql(
        "INSERT INTO note (id, workspace_id, title, content, created_at, updated_at) \
         VALUES ('root', 'doomed', 'root', '', 't0', 't0'); \
         WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<25000) \
         INSERT INTO note (id, workspace_id, title, content, parent_id, created_at, updated_at) \
         SELECT 'prefix-' || n, 'doomed', 'note', '', 'root', 't0', 't0' FROM rows; \
         UPDATE note SET parent_id=NULL WHERE workspace_id='doomed' AND id <> 'prefix-25000'; \
         WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<1001) \
         INSERT INTO comment (id, thread_id, note_id, workspace_id, kind, content, author, author_type, anchor_json, created_at, updated_at) \
         SELECT 'distributed-' || n, 'thread', 'prefix-' || (23000+n), 'doomed', 'comment', '', 'a', 'user', '{}', 't0', 't0' FROM rows; \
         WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<5000) \
         INSERT INTO comment (id, thread_id, note_id, workspace_id, kind, content, author, author_type, anchor_json, created_at, updated_at) \
         SELECT 'no-note-' || n, 'thread', NULL, 'doomed', 'comment', '', 'a', 'user', '{}', 't0', 't0' FROM rows;",
    ).execute(store.write_pool()).await.unwrap();
    let parents = measure_batch(&store, CLEAR_NOTE_PARENT_BATCH_SQL).await;
    let empty_parents = measure_batch(&store, CLEAR_NOTE_PARENT_BATCH_SQL).await;
    let mut comments = Vec::new();
    loop {
        let batch = measure_batch(&store, DELETE_NOTE_COMMENT_BATCH_SQL).await;
        comments.push(batch);
        if batch.0 == 0 {
            break;
        }
    }
    eprintln!("parent batches (rows, VM steps): {parents:?}, {empty_parents:?}; distributed comments: {comments:?}");
    assert_eq!(parents.0, 1);
    assert_eq!(empty_parents.0, 0);
    assert_eq!(
        comments.iter().map(|batch| batch.0).collect::<Vec<_>>(),
        [500, 500, 1, 0]
    );
    let no_note: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM comment WHERE note_id IS NULL")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        no_note, 5000,
        "comments without notes keep their retention semantics"
    );
    let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM comment WHERE workspace_id='keeper'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(kept, 3);
    let kept_parents: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note WHERE workspace_id='keeper' AND parent_id IS NOT NULL",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(kept_parents, 2);
    assert!(
        parents.1 < 10_000 && empty_parents.1 < 10_000,
        "cleared prefixes must not be revisited: {parents:?}, {empty_parents:?}"
    );
    assert!(
        comments.iter().all(|batch| batch.1 < 80_000),
        "comment candidate work must advance: {comments:?}"
    );
}

/// Suspend the caller after `SQLite` starts the browser statement, observe its
/// commit on a separate read connection, and cancel without polling the
/// caller again. The write-pool barrier drains the completed batch before
/// checking the overlay. No sleep or scheduler timing assumption is needed.
#[tokio::test]
async fn browser_commit_eviction_survives_caller_cancellation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let doomed = seed_workspace(&store, "doomed").await;
        let keeper = seed_workspace(&store, "keeper").await;
        let host = ClientId::from("host");
        seed_tab_and_draft(&store, &doomed, &host).await;
        seed_tab_and_draft(&store, &keeper, &host).await;
        let (started, release) = block_next_update(&store, "browser_tab").await;
        let mut deleting = Box::pin(store.delete_workspace(&doomed));
        tokio::select! {
            biased;
            () = started.notified() => (),
            result = &mut deleting => panic!("delete completed before cancellation: {result:?}"),
        }
        // The worker cannot finish the statement until the caller has stopped
        // polling. It may now commit while the caller stays suspended.
        release.send(()).unwrap();
        loop {
            let tabs: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM browser_tab WHERE workspace_id='doomed'")
                    .fetch_one(store.read_pool())
                    .await
                    .unwrap();
            if tabs == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        drop(deleting);
        drain_browser_batch(&store).await;
        store
            .get_workspace(&doomed)
            .await
            .expect("cancelled caller has not finalized the root");
        store
            .delete_workspace(&doomed)
            .await
            .expect("retry completes remaining cleanup");
        assert_eq!(
            store.browser_tab_displayed.len(),
            1,
            "retry must not strand the committed batch's overlay"
        );
        assert_eq!(
            store
                .get_browser_tab(&keeper.0)
                .await
                .unwrap()
                .unwrap()
                .displayed,
            Some(true)
        );
    })
    .await
    .expect("cancellation regression completes");
}

async fn cancel_final_transaction(fail_commit: bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let doomed = seed_workspace(&store, "doomed").await;
        let keeper = seed_workspace(&store, "keeper").await;
        let host = ClientId::from("host");
        seed_tab_and_draft(&store, &doomed, &host).await;
        seed_tab_and_draft(&store, &keeper, &host).await;
        let (started, release) = block_next_update(&store, "browser_tab").await;
        let mut deleting = Box::pin(store.delete_workspace(&doomed));
        tokio::select! {
            biased;
            () = started.notified() => (),
            result = &mut deleting => panic!("delete completed before browser batch: {result:?}"),
        }
        release.send(()).unwrap();
        drain_browser_batch(&store).await;
        assert_eq!(store.browser_tab_displayed.len(), 1);

        // The caller has not consumed the short batch's result. Add a late
        // tab now, so the final transaction must clean its row and overlay.
        seed_tab_and_draft(&store, &doomed, &host).await;
        if fail_commit {
            sqlx::query(
                "CREATE TRIGGER reject_tombstone BEFORE INSERT ON deleted_workspace_id \
                 BEGIN SELECT RAISE(ABORT, 'injected final transaction failure'); END",
            )
            .execute(store.write_pool())
            .await
            .unwrap();
        }
        let (started, release) = block_next_update(&store, "workspace").await;
        tokio::select! {
            biased;
            () = started.notified() => (),
            result = &mut deleting => panic!("delete completed before final transaction: {result:?}"),
        }
        drop(deleting);
        release.send(()).unwrap();
        drain_browser_batch(&store).await;

        let tombstones: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM deleted_workspace_id WHERE id='doomed'",
        )
        .fetch_one(store.read_pool())
        .await
        .unwrap();
        if fail_commit {
            store.get_workspace(&doomed).await.unwrap();
            assert_eq!(tombstones, 0);
            assert_eq!(store.browser_tab_displayed.len(), 2);
            assert_eq!(
                store.get_browser_tab(&doomed.0).await.unwrap().unwrap().displayed,
                Some(true),
                "rolled-back final transaction preserves its overlay"
            );
            sqlx::query("DROP TRIGGER reject_tombstone")
                .execute(store.write_pool()).await.unwrap();
            store.delete_workspace(&doomed).await.unwrap();
        } else {
            assert!(matches!(store.get_workspace(&doomed).await, Err(Error::NotFound(_))));
            assert_eq!(tombstones, 1);
            assert!(store.get_browser_tab(&doomed.0).await.unwrap().is_none());
        }
        assert_eq!(store.browser_tab_displayed.len(), 1);
        assert_eq!(
            store.get_browser_tab(&keeper.0).await.unwrap().unwrap().displayed,
            Some(true)
        );
    }).await.expect("final transaction cancellation completes");
}

#[tokio::test]
async fn browser_final_transaction_eviction_survives_cancellation() {
    cancel_final_transaction(false).await;
}

#[tokio::test]
async fn browser_failed_final_transaction_keeps_overlay_after_cancellation() {
    cancel_final_transaction(true).await;
}

#[tokio::test]
async fn deletion_indexes_upgrade_preserves_existing_data() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let legacy = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 130)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    legacy.run(&pool).await.unwrap();
    let store = Store {
        write_pool: pool,
        read_pool: crate::connect_read(&tmp.path).await.unwrap(),
        browser_tab_displayed: crate::browser_tab_repo::DisplayedOverlay::default(),
    };
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    seed_unrelated_metadata(&store).await;
    seed_heavy_workspace_children(&store, &doomed, 3).await;
    seed_heavy_workspace_children(&store, &keeper, 3).await;
    assert_eq!(
        store.migration_status().await.unwrap().applied.last(),
        Some(&130)
    );
    let note_rows = "SELECT id, workspace_id, parent_id FROM note ORDER BY workspace_id, id";
    let before: Vec<(String, String, Option<String>)> = sqlx::query_as(note_rows)
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    store.close().await;

    let store = Store::open(&tmp.path)
        .await
        .expect("upgrade populated pre-index database");
    assert!(store.migration_status().await.unwrap().is_current());
    let after: Vec<(String, String, Option<String>)> = sqlx::query_as(note_rows)
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert_eq!(before, after);
    for (table, expected) in [
        ("agent_session", 321),
        ("completion_wake_delivery", 10_000),
        ("advisory_wake_delivery", 10_000),
        ("browser_tab", 10_001),
        ("note_version", 6),
        ("comment", 6),
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(
            count, expected,
            "existing {table} rows preserved by upgrade"
        );
    }
    let closed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM browser_tab WHERE closed_at IS NOT NULL")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(closed, 5001);
    for index in [
        "idx_completion_wake_delivery_child",
        "idx_advisory_wake_delivery_child",
        "idx_browser_tab_workspace",
        "idx_note_workspace_parented",
        "idx_comment_workspace_note",
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index' AND name=?)",
        )
        .bind(index)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
        assert!(exists, "upgrade creates {index}");
    }
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(store.read_pool())
        .await
        .unwrap()
        .is_empty());
    store.delete_workspace(&doomed).await.unwrap();
    store.get_workspace(&keeper).await.unwrap();
    assert!(matches!(
        store.get_workspace(&doomed).await,
        Err(Error::NotFound(_))
    ));
}
