//! Real migrations, indexed empty probes, fenced writers and partial cleanup.
use super::*;
use intent_core::{LineAttributionData, NoteId, WorkspaceId};
use sqlx::Row;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

async fn fixture() -> (tempfile::TempDir, Store, i64, i64) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    for workspace in ["doomed", "keeper"] {
        sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES(?,?,'main','t0','t0')")
            .bind(workspace).bind(workspace).execute(store.write_pool()).await.unwrap();
        sqlx::query("INSERT INTO note(id,workspace_id,title,content,created_at,updated_at) VALUES('note',?,'Note','x','t0','t0')")
            .bind(workspace).execute(store.write_pool()).await.unwrap();
    }
    let doomed =
        sqlx::query_scalar("SELECT id FROM note_annotation_head WHERE workspace_id='doomed'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    let keeper =
        sqlx::query_scalar("SELECT id FROM note_annotation_head WHERE workspace_id='keeper'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    (dir, store, doomed, keeper)
}

async fn seed_lines(store: &Store, head: i64, count: i64) {
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO note_attribution_line(head_id,line,start,end,timestamp,has_author) SELECT ?,x,x-1,x,0,1 FROM n")
        .bind(count).bind(head).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO note_attribution_author(head_id,line,author_json) SELECT head_id,line,'{\"id\":\"a\",\"name\":\"A\",\"type\":\"user\"}' FROM note_attribution_line WHERE head_id=?")
        .bind(head).execute(store.write_pool()).await.unwrap();
}

async fn presence(store: &Store, heads: &[i64]) -> i64 {
    presence_query(heads)
        .build_query_scalar()
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

async fn child_count(store: &Store, table: &str, head: i64) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE head_id=?"))
        .bind(head)
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn empty_presence_uses_indexed_owner_probes_and_preserves_keeper() {
    let (_dir, store, doomed, keeper) = fixture().await;
    seed_lines(&store, keeper, 10_000).await;
    let heads = vec![doomed; usize::try_from(COMMENT_BATCH).expect("positive bounded head batch")];
    let mut conn = store.read_pool().acquire().await.unwrap();
    let steps = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&steps);
    conn.lock_handle()
        .await
        .unwrap()
        .set_progress_handler(100, move || {
            counter.fetch_add(100, Ordering::SeqCst);
            true
        });
    let mut query = presence_query(&heads);
    let present: i64 = query
        .build_query_scalar()
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    conn.lock_handle().await.unwrap().remove_progress_handler();
    assert_eq!(present, 0);
    assert!(
        steps.load(Ordering::SeqCst) < 10_000,
        "empty owners must not scan keeper rows"
    );
    let sql = format!("EXPLAIN QUERY PLAN {}", query.sql());
    let mut plan = sqlx::query(&sql);
    for _ in HEAD_CHILDREN {
        for head in &heads {
            plan = plan.bind(head);
        }
    }
    let details: Vec<String> = plan
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get("detail"))
        .collect();
    for (table, _, _) in HEAD_CHILDREN {
        assert!(
            details
                .iter()
                .any(|s| s.contains(&format!("SEARCH {table} USING"))),
            "{table}: {details:?}"
        );
        assert!(
            !details.iter().any(|s| s.contains(&format!("SCAN {table}"))),
            "{details:?}"
        );
    }
    drop(conn);
    drain(&store, "doomed").await.unwrap();
    assert_eq!(presence(&store, &[doomed]).await, 0);
    assert_eq!(
        child_count(&store, "note_attribution_line", keeper).await,
        10_000
    );
    assert_eq!(presence(&store, &[keeper]).await, 7);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(store.read_pool())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn populated_presence_cancel_reopen_resumes_bounded_child_order() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (dir, store, doomed, keeper) = fixture().await;
        seed_lines(&store, doomed, 1_201).await;
        seed_lines(&store, keeper, 3).await;
        assert_eq!(presence(&store, &[doomed]).await, 7);
        let started = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::clone(&started);
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let mut blocked = Some(blocked);
        let mut conn = store.write_pool().acquire().await.unwrap();
        conn.lock_handle().await.unwrap().set_update_hook(move |update| {
            if update.table == "note_attribution_author_piece" {
                if let Some(blocked) = blocked.take() {
                    observed.notify_one();
                    blocked.recv_timeout(Duration::from_secs(15)).unwrap();
                }
            }
        });
        drop(conn);
        let worker_store = store.clone();
        let worker = tokio::spawn(async move { drain(&worker_store, "doomed").await });
        started.notified().await;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut conn = store.write_pool().acquire().await.unwrap();
        conn.lock_handle().await.unwrap().remove_update_hook();
        drop(conn);
        assert_eq!(child_count(&store, "note_attribution_author_piece", doomed).await, 701);
        assert_eq!(child_count(&store, "note_attribution_author", doomed).await, 1_201);
        assert_eq!(child_count(&store, "note_attribution_line", doomed).await, 1_201);
        assert!(store.note_annotation_epochs(&WorkspaceId::from("doomed"), &NoteId::from("note")).await.is_err());
        store.close().await;
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        drain(&store, "doomed").await.unwrap();
        assert_eq!(presence(&store, &[doomed]).await, 0);
        assert_eq!(child_count(&store, "note_attribution_author_piece", keeper).await, 3);
        assert_eq!(child_count(&store, "note_attribution_author", keeper).await, 3);
        assert_eq!(child_count(&store, "note_attribution_line", keeper).await, 3);
        let fence: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement WHERE workspace_id='doomed')").fetch_one(store.read_pool()).await.unwrap();
        assert!(fence);
        assert!(sqlx::query("PRAGMA foreign_key_check").fetch_all(store.read_pool()).await.unwrap().is_empty());
    }).await.unwrap();
}

#[tokio::test]
async fn retired_empty_probe_cannot_be_invalidated_by_admitted_annotation_writers() {
    let (_dir, store, doomed, _) = fixture().await;
    let workspace = WorkspaceId::from("doomed");
    let note = NoteId::from("note");
    let epochs = store
        .note_annotation_epochs(&workspace, &note)
        .await
        .unwrap();
    let ticket = store
        .begin_note_attribution(&workspace, &note, epochs.source_revision)
        .await
        .unwrap();
    let epochs = store
        .note_annotation_epochs(&workspace, &note)
        .await
        .unwrap();
    begin_retirement(&store, "doomed").await.unwrap();
    assert_eq!(presence(&store, &[doomed]).await, 0);
    let data = LineAttributionData {
        workspace_id: workspace.clone(),
        note_id: note.clone(),
        computed_at: "t0".into(),
        attributions: std::collections::BTreeMap::default(),
    };
    assert!(store
        .publish_note_attribution(&ticket, "x", &data)
        .await
        .is_err());
    assert!(store
        .publish_comment_anchors(&workspace, &note, &epochs, &[])
        .await
        .is_err());
    for sql in [
        "UPDATE note SET content='changed' WHERE workspace_id='doomed'",
        "INSERT INTO comment(id,workspace_id,note_id,thread_id,kind,content,author,author_type,created_at,updated_at) VALUES('c','doomed','note','c','comment','body','a','user','t0','t0')",
        "INSERT INTO note_line_attribution(workspace_id,note_id,computed_at,attributions_json) VALUES('doomed','note','t0','{}')",
    ] {
        let error = sqlx::query(sql).execute(store.write_pool()).await.unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(ref db) if db.message()=="workspace note retirement in progress"), "{error}");
    }
    assert_eq!(presence(&store, &[doomed]).await, 0);
}
