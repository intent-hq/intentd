use super::*;
use sqlx::{Connection, Executor};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

// The final include is the proposed migration supplied separately for the
// integration owner. No test-local replacement for reclamation schema/triggers.
const MIGRATIONS: [&str; 3] = [
    include_str!("../../migrations/0150_note_operations.sql"),
    include_str!("../../migrations/0152_note_stages.sql"),
    include_str!("../../migrations/0153_note_operation_reclaim.sql"),
];
async fn connection(url: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::connect(url).await.unwrap();
    conn.execute("PRAGMA foreign_keys=ON").await.unwrap();
    conn.execute("PRAGMA busy_timeout=5000").await.unwrap();
    conn
}
async fn schema(conn: &mut SqliteConnection) {
    conn.execute("CREATE TABLE note_page_head(workspace_id TEXT,note_id TEXT); CREATE TABLE note_page_piece(workspace_id TEXT,note_id TEXT,start INTEGER,end INTEGER,text TEXT);").await.unwrap();
    for migration in MIGRATIONS {
        sqlx::raw_sql(migration).execute(&mut *conn).await.unwrap();
    }
}
async fn fixture() -> SqliteConnection {
    let mut conn = connection("sqlite::memory:").await;
    schema(&mut conn).await;
    conn
}
async fn operation(conn: &mut SqliteConnection, key: &str, until: i64) {
    sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome) VALUES(?,'p','b','w','n','i',?,'digest',100,?,?)")
        .bind(key).bind(key).bind(until).bind(r#"{"kind":"noteStageState","phase":"staging","expiresAt":"1970-01-01T00:01:40.123Z","streams":[]}"#)
        .execute(conn).await.unwrap();
}
async fn stage(conn: &mut SqliteConnection, key: &str, root: &str, phase: &str, until: i64) {
    operation(conn, key, until).await;
    sqlx::query("INSERT OR IGNORE INTO note_stage_root VALUES(?,'w',?,'i',?,0,0)")
        .bind(root)
        .bind(root)
        .bind(root)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_operation SET method_kind='staged' WHERE operation_key=?")
        .bind(key)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage(operation_key,root_key,header_digest,header,base_revision,phase) VALUES(?,?,?,'{}','base',?)")
        .bind(key).bind(root).bind("a".repeat(64)).bind(phase).execute(conn).await.unwrap();
}
async fn pieces(conn: &mut SqliteConnection, root: &str, n: i64) {
    for i in 0..n {
        sqlx::query("INSERT INTO note_stage_base_piece VALUES(?,?,?,'a')")
            .bind(root)
            .bind(i)
            .bind(i + 1)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
}
async fn records(conn: &mut SqliteConnection, op: &str, n: i64) {
    // A single chunk has at most 128 records. Fanout crosses valid chunks.
    for i in 0..n {
        let sequence = i / 128;
        sqlx::query("INSERT OR IGNORE INTO note_stage_chunk VALUES(?,'dirty',?,NULL,?,128)")
            .bind(op)
            .bind(sequence)
            .bind("a".repeat(64))
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_stage_record VALUES(?,'dirty',?,?, '{}')")
            .bind(op)
            .bind(sequence)
            .bind(i % 128)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
}
async fn count(conn: &mut SqliteConnection, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(conn)
        .await
        .unwrap()
}
async fn tick(conn: &mut SqliteConnection, now: i64) -> NoteOperationReclaimStats {
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let stats = reclaim_batch(&mut tx, now).await.unwrap();
    assert!(stats.child_rows <= 64 && stats.root_pieces <= 64);
    assert!(stats.operations <= 1 && stats.roots <= 1);
    tx.commit().await.unwrap();
    stats
}
async fn finish(conn: &mut SqliteConnection, now: i64) {
    for _ in 0..1000 {
        tick(conn, now).await;
        let pending:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM note_operation_reclaim WHERE due_ms<=?)+(SELECT count(*) FROM note_stage_root_reclaim WHERE due_ms<=?)")
            .bind(now).bind(now).fetch_one(&mut *conn).await.unwrap();
        if pending == 0 {
            return;
        }
    }
    panic!("cleanup did not finish fixture");
}

#[tokio::test]
async fn exact_millisecond_expiry_fences_before_bounded_drain_and_retains_identity() {
    let mut conn = fixture().await;
    stage(&mut conn, "op", "root", "staging", 1000).await;
    records(&mut conn, "op", 145).await;
    pieces(&mut conn, "root", 130).await;
    assert_eq!(
        tick(&mut conn, 100_122).await,
        NoteOperationReclaimStats::default()
    );
    assert_eq!(count(&mut conn, "note_stage_record").await, 145);
    let first = tick(&mut conn, 100_123).await;
    assert_eq!((first.child_rows, first.root_pieces), (64, 64));
    let state: (String, String) = sqlx::query_as(
        "SELECT s.phase,o.outcome FROM note_stage s JOIN note_operation o USING(operation_key)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(state.0, "expired");
    let outcome: serde_json::Value = serde_json::from_str(&state.1).unwrap();
    assert_eq!(outcome["expiresAt"], "1970-01-01T00:01:40.123Z");
    assert_eq!(outcome["phase"], "expired");
    finish(&mut conn, 100_123).await;
    assert_eq!(count(&mut conn, "note_stage_record").await, 0);
    assert_eq!(count(&mut conn, "note_stage_base_piece").await, 0);
    assert_eq!(count(&mut conn, "note_stage").await, 1);
    assert_eq!(count(&mut conn, "note_operation").await, 1);
    assert_eq!(count(&mut conn, "note_stage_root").await, 1);
    assert_eq!(
        tick(&mut conn, 999_999).await,
        NoteOperationReclaimStats::default()
    );
    finish(&mut conn, 1_000_000).await;
    assert_eq!(count(&mut conn, "note_operation").await, 0);
    assert_eq!(count(&mut conn, "note_stage_root").await, 0);
}

#[tokio::test]
async fn committed_frozen_view_and_shared_live_root_survive_other_owner_cleanup() {
    let mut conn = fixture().await;
    stage(&mut conn, "cancelled", "shared", "cancelled", 1000).await;
    stage(&mut conn, "receipt", "shared", "committed", 2000).await;
    pieces(&mut conn, "shared", 130).await;
    records(&mut conn, "cancelled", 130).await;
    records(&mut conn, "receipt", 130).await;
    sqlx::query("INSERT INTO note_stage_text(operation_key,text_id,length,utf8_bytes) VALUES('receipt','text',1,1)").execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO note_stage_text_piece VALUES('receipt','text',0,1,'x')")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage_view VALUES('receipt',0,NULL,NULL,1)")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage_view_piece VALUES('receipt',0,0,1,'text','text',0)")
        .execute(&mut conn)
        .await
        .unwrap();
    finish(&mut conn, 1_000_000).await;
    assert_eq!(count(&mut conn, "note_stage_record").await, 130);
    assert_eq!(count(&mut conn, "note_stage_base_piece").await, 130);
    assert_eq!(count(&mut conn, "note_stage_text_piece").await, 1);
    assert_eq!(count(&mut conn, "note_stage_view_piece").await, 1);
    assert_eq!(count(&mut conn, "note_operation").await, 1);
    finish(&mut conn, 2_000_000).await;
    assert_eq!(count(&mut conn, "note_stage_root").await, 0);
    assert_eq!(count(&mut conn, "note_operation").await, 0);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut conn)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn receipt_children_are_drained_before_expired_operation_cascade() {
    let mut conn = fixture().await;
    operation(&mut conn, "receipt", 1000).await;
    for i in 0..140 {
        sqlx::query("INSERT INTO note_operation_item VALUES('receipt','mapping',?,'{}')")
            .bind(i)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_operation_source VALUES('receipt','base',?,?,'a')")
            .bind(i)
            .bind(i + 1)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_operation_text VALUES('receipt',?,'base',0,1,1,1,?)")
            .bind(i.to_string())
            .bind("a".repeat(64))
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_operation_detail VALUES('receipt','ref',?,'{}')")
            .bind(i)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_operation_reference VALUES('receipt',?)")
            .bind(i.to_string())
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_operation_scalar VALUES('receipt',?,'id','value','phase',0)")
            .bind(i.to_string())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    // Any accidental cascading delete with remaining bulk fails this guard.
    conn.execute("CREATE TRIGGER assert_drained BEFORE DELETE ON note_operation BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM note_operation_item WHERE operation_key=old.operation_key) OR EXISTS(SELECT 1 FROM note_operation_source WHERE operation_key=old.operation_key) OR EXISTS(SELECT 1 FROM note_operation_text WHERE operation_key=old.operation_key) OR EXISTS(SELECT 1 FROM note_operation_detail WHERE operation_key=old.operation_key) OR EXISTS(SELECT 1 FROM note_operation_reference WHERE operation_key=old.operation_key) OR EXISTS(SELECT 1 FROM note_operation_scalar WHERE operation_key=old.operation_key) THEN RAISE(ABORT,'bulk cascade') END; END;").await.unwrap();
    assert_eq!(
        tick(&mut conn, 999_999).await,
        NoteOperationReclaimStats::default()
    );
    finish(&mut conn, 1_000_000).await;
    assert_eq!(count(&mut conn, "note_operation").await, 0);
}

#[tokio::test]
async fn failed_batch_rolls_back_progress_and_reopen_resumes_committed_work() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("reclaim.db").display()
    );
    let mut conn = connection(&url).await;
    schema(&mut conn).await;
    stage(&mut conn, "op", "root", "cancelled", 1000).await;
    records(&mut conn, "op", 140).await;
    pieces(&mut conn, "root", 130).await;
    tick(&mut conn, 0).await;
    assert_eq!(count(&mut conn, "note_stage_record").await, 76);
    conn.execute("CREATE TRIGGER fail_root BEFORE DELETE ON note_stage_base_piece BEGIN SELECT RAISE(ABORT,'late failure'); END;").await.unwrap();
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(reclaim_batch(&mut tx, 0).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(count(&mut conn, "note_stage_record").await, 76);
    assert_eq!(count(&mut conn, "note_stage_base_piece").await, 66);
    conn.execute("DROP TRIGGER fail_root").await.unwrap();
    conn.close().await.unwrap();
    let mut conn = connection(&url).await;
    assert_eq!(count(&mut conn, "note_stage_record").await, 76);
    finish(&mut conn, 0).await;
    assert_eq!(count(&mut conn, "note_stage_record").await, 0);
    assert_eq!(count(&mut conn, "note_operation").await, 1);
}

#[tokio::test]
async fn cancelled_worker_settles_writer_before_next_batch_reacquires() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("cancel.db").display()
    );
    let mut conn = connection(&url).await;
    schema(&mut conn).await;
    stage(&mut conn, "op", "root", "cancelled", 1000).await;
    records(&mut conn, "op", 140).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let signal = reached.clone();
    let worker = tokio::spawn(async move {
        let mut other = connection(&url).await;
        let mut tx = other.begin_with("BEGIN IMMEDIATE").await.unwrap();
        reclaim_batch(&mut tx, 0).await.unwrap();
        signal.notify_one();
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
        assert_eq!(count(&mut tx, "note_stage_record").await, 140);
        let stats = reclaim_batch(&mut tx, 0).await.unwrap();
        assert_eq!(stats.child_rows, 64);
        tx.commit().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn indexed_cleanup_cost_does_not_scan_keeper_or_processed_prefix() {
    let mut conn = fixture().await;
    stage(&mut conn, "target", "target-root", "cancelled", 1000).await;
    records(&mut conn, "target", 256).await;
    stage(&mut conn, "keeper", "keeper-root", "committed", 2000).await;
    records(&mut conn, "keeper", 10_000).await;
    // Many future identities must not contribute a per-tick scan.
    conn.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000) INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome) SELECT 'future-'||i,'p','b','w','n','i','future-'||i,'digest',100,2000,'{}' FROM n;").await.unwrap();
    let mut costs = Vec::new();
    for _ in 0..4 {
        let steps = Arc::new(AtomicU64::new(0));
        let observed = steps.clone();
        conn.lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                observed.fetch_add(1, Ordering::Relaxed);
                true
            });
        let stats = tick(&mut conn, 0).await;
        conn.lock_handle().await.unwrap().remove_progress_handler();
        assert_eq!(stats.child_rows, 64);
        costs.push(steps.load(Ordering::Relaxed));
    }
    eprintln!("actual reclamation 64-child batch VM steps: {costs:?}");
    assert!(costs.iter().all(|n| *n < 80_000));
    // First tick also retires an empty root queue; allow that fixed extra work.
    assert!(costs[3] <= costs[1] + 1000);
    assert_eq!(count(&mut conn, "note_stage_record").await, 10_000);
    assert_eq!(count(&mut conn, "note_operation").await, 10_002);
    let plan=sqlx::query("EXPLAIN QUERY PLAN SELECT operation_key FROM note_operation_reclaim WHERE due_ms<=0 ORDER BY due_ms,operation_key LIMIT 1")
        .fetch_all(&mut conn).await.unwrap();
    assert!(plan
        .iter()
        .any(|row| row.get::<String, _>("detail").contains("SEARCH")));
    assert!(plan
        .iter()
        .all(|row| !row.get::<String, _>("detail").contains("TEMP B-TREE")));
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut conn)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn copy_on_write_uses_live_pins_and_does_not_regenerate_expired_root_pieces() {
    let mut conn = fixture().await;
    let now: i64 = sqlx::query_scalar("SELECT unixepoch()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    stage(&mut conn, "expired", "expired-root", "staging", now + 1000).await;
    stage(&mut conn, "live", "live-root", "committed", now + 1000).await;
    for root in ["expired-root", "live-root"] {
        sqlx::query("INSERT INTO note_page_piece(workspace_id,note_id,start,end,text,content_generation) VALUES('w',?,0,1,'x',?)")
            .bind(root).bind(root).execute(&mut conn).await.unwrap();
        sqlx::query("DELETE FROM note_page_piece WHERE note_id=?")
            .bind(root)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let retained: Vec<String> =
        sqlx::query_scalar("SELECT root_key FROM note_stage_base_piece ORDER BY root_key")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(retained, vec!["live-root"]);
    // Status metadata has not been processed; the exact pin deadline alone must
    // prevent second-rounded/expired headers from recreating reclaimed pieces.
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM note_stage WHERE operation_key='expired'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(phase, "staging");
}
