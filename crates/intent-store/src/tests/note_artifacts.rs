//! Real disposable `SQLite` invariants for the prepared artifact lifecycle.
//! Seeded jobs are not source-authorization or profile-producer acceptance.
use super::TempDb;
use crate::Store;
use sqlx::Row;

async fn setup() -> (Store, TempDb) {
    let temporary = TempDb::new();
    let store = Store::open(&temporary.path).await.unwrap();
    store
        .configure_test_note_artifact_arena(
            &temporary.path.with_extension("artifacts.sqlite"),
            1024,
        )
        .await
        .unwrap();
    for (kind, id) in [
        ("global", ""),
        ("principal", "alice"),
        ("workspace", "workspace"),
    ] {
        sqlx::query("INSERT INTO note_artifact_capacity(scope_kind,scope_id,payload_limit,record_limit,index_limit,storage_limit,job_limit) VALUES (?,?,1024,2,2,4096,1)")
            .bind(kind).bind(id).execute(store.artifact_pool().unwrap()).await.unwrap();
    }
    sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) VALUES ('alice','workspace','job','00000000000000000000000000000001','runtime',?, '{}','00000000000000000000000000000002','revision','note','instance','f:source','building',100,200,1024,2,2,4096,?)")
        .bind("0".repeat(64)).bind("0".repeat(64)).execute(store.artifact_pool().unwrap()).await.unwrap();
    (store, temporary)
}

#[tokio::test]
async fn artifact_payload_purge_is_bounded_scoped_and_never_refunds_charges() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), false)
        .await
        .unwrap();
    append(&store, 1, &"1".repeat(64), &"2".repeat(64), true)
        .await
        .unwrap();
    let status = store
        .note_artifact_journal_status("alice", "workspace", "job", &"0".repeat(64))
        .await
        .unwrap()
        .unwrap();
    assert!(store
        .purge_note_artifact_journal_records("alice", "workspace", &status.job_ref, 1)
        .await
        .is_err());
    store.expire_note_artifact_journals(1).await.unwrap();
    for (principal, workspace, limit) in [
        ("bob", "workspace", 1),
        ("alice", "other", 1),
        ("alice", "workspace", 0),
        ("alice", "workspace", 129),
    ] {
        assert!(store
            .purge_note_artifact_journal_records(principal, workspace, &status.job_ref, limit)
            .await
            .is_err());
    }
    let pool = store.artifact_pool().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_record")
            .fetch_one(pool)
            .await
            .unwrap(),
        2
    );
    let first = store
        .purge_note_artifact_journal_records("alice", "workspace", &status.job_ref, 1)
        .await
        .unwrap();
    assert_eq!(
        first,
        crate::ArtifactJournalPurge {
            deleted_records: 1,
            more_records: true
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sequence FROM note_artifact_record")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sequence FROM note_artifact_ack")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
    let second = store
        .purge_note_artifact_journal_records("alice", "workspace", &status.job_ref, 128)
        .await
        .unwrap();
    assert_eq!(
        second,
        crate::ArtifactJournalPurge {
            deleted_records: 1,
            more_records: false
        }
    );
    assert_eq!(
        store
            .purge_note_artifact_journal_records("alice", "workspace", &status.job_ref, 1)
            .await
            .unwrap(),
        crate::ArtifactJournalPurge {
            deleted_records: 0,
            more_records: false
        }
    );
    let retained = store
        .note_artifact_journal_status("alice", "workspace", "job", &"0".repeat(64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, "expired");
    assert_eq!(retained.next_sequence, 2);
    assert!(!retained.cleanup_complete);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(pool)
            .await
            .unwrap(),
        3 * 4096
    );
    let plan = sqlx::query("EXPLAIN QUERY PLAN SELECT sequence FROM note_artifact_record WHERE generation=? ORDER BY sequence LIMIT ?")
        .bind(&status.generation).bind(1).fetch_all(pool).await.unwrap();
    let details = plan
        .iter()
        .map(|r| r.get::<String, _>("detail"))
        .collect::<Vec<_>>();
    assert!(
        details
            .iter()
            .any(|s| s.contains("SEARCH note_artifact_record USING COVERING INDEX")),
        "{details:?}"
    );
    assert!(
        !details.iter().any(|s| s.contains("TEMP B-TREE")),
        "{details:?}"
    );
}

#[tokio::test]
async fn artifact_payload_purge_preserves_unelapsed_receipt_retention() {
    let (store, _temporary) = setup().await;
    let pool = store.artifact_pool().unwrap();
    sqlx::query("UPDATE note_artifact_capacity SET payload_limit=2048,record_limit=4,index_limit=4,storage_limit=8192,job_limit=2")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) SELECT principal,workspace_id,'retained','00000000000000000000000000000003',runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,'building',100,9007199254740991,payload_limit,record_limit,index_limit,storage_limit,header_digest FROM note_artifact_job WHERE job_id='job'")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES ('00000000000000000000000000000003',0,?,?,'{}',0,1024,1)")
        .bind("0".repeat(64)).bind("1".repeat(64)).execute(pool).await.unwrap();
    let status = store
        .note_artifact_journal_status("alice", "workspace", "retained", &"0".repeat(64))
        .await
        .unwrap()
        .unwrap();
    store.expire_note_artifact_journals(128).await.unwrap();
    assert!(store
        .purge_note_artifact_journal_records("alice", "workspace", &status.job_ref, 128)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_record")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_ack")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn artifact_expiry_is_batched_and_keeps_storage_charged() {
    let (store, _temporary) = setup().await;
    sqlx::query("UPDATE note_artifact_capacity SET payload_limit=4096,record_limit=8,index_limit=8,storage_limit=16384,job_limit=4")
        .execute(store.artifact_pool().unwrap()).await.unwrap();
    for (id, expiry) in [(2, 100_i64), (3, 100), (4, 9_007_199_254_740_991)] {
        sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) SELECT principal,workspace_id,?,?,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,'building',?,?,payload_limit,record_limit,index_limit,storage_limit,header_digest FROM note_artifact_job WHERE job_id='job'")
            .bind(format!("job{id}")).bind(format!("{id:032x}")).bind(expiry).bind(expiry)
            .execute(store.artifact_pool().unwrap()).await.unwrap();
    }
    for limit in [0, 129, u32::MAX] {
        assert!(store.expire_note_artifact_journals(limit).await.is_err());
    }
    let first = store.expire_note_artifact_journals(1).await.unwrap();
    assert_eq!(first, vec!["00000000000000000000000000000001"]);
    assert_eq!(
        store.expire_note_artifact_journals(1).await.unwrap().len(),
        1
    );
    assert_eq!(
        store
            .expire_note_artifact_journals(128)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .expire_note_artifact_journals(128)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM note_artifact_job WHERE state='expired' AND cleanup_complete=0"
        )
        .fetch_one(store.artifact_pool().unwrap())
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        3 * 4 * 4096
    );
    let plan = sqlx::query("EXPLAIN QUERY PLAN SELECT generation FROM note_artifact_job WHERE state IN ('building','sealed','admitted') AND expires_at<=? ORDER BY expires_at,generation LIMIT ?")
        .bind(100_i64).bind(1_i64).fetch_all(store.artifact_pool().unwrap()).await.unwrap();
    let detail = plan
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();
    assert!(
        detail
            .iter()
            .any(|s| s.contains("SEARCH note_artifact_job USING INDEX note_artifact_job_expiry")),
        "{detail:?}"
    );
}

#[tokio::test]
async fn artifact_expiry_retires_lease_but_preserves_records_and_ack() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), true)
        .await
        .unwrap();
    sqlx::query("UPDATE note_artifact_job SET state='sealed'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES ('00000000000000000000000000000001','admission','lease',?,100)")
        .bind("1".repeat(64)).execute(store.artifact_pool().unwrap()).await.unwrap();
    assert_eq!(
        store.expire_note_artifact_journals(1).await.unwrap().len(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT released FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        1
    );
    for table in ["note_artifact_record", "note_artifact_ack"] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
            1
        );
    }
    assert_eq!(
        store
            .note_artifact_journal_status("alice", "workspace", "job", &"0".repeat(64))
            .await
            .unwrap()
            .unwrap()
            .state,
        "expired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        3 * 4096
    );
}

#[tokio::test]
async fn artifact_cancelled_abort_does_not_poison_writer_transaction() {
    let (store, _temporary) = setup().await;
    let status = store
        .note_artifact_journal_status("alice", "workspace", "job", &"0".repeat(64))
        .await
        .unwrap()
        .unwrap();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let mut reached_tx = Some(reached_tx);
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            if let Some(sender) = reached_tx.take() {
                let _ = sender.send(());
                // Pause the SQLite worker at an observable instruction while the
                // caller cancels its pending BEGIN. The timeout is only a deadlock
                // guard; the test resumes it explicitly, without timing sleeps.
                return resume_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .is_ok();
            }
            true
        });
    drop(connection);
    let mut abort =
        Box::pin(store.abort_note_artifact_journal("alice", "workspace", &status.job_ref));
    let reached = tokio::select! {
        result = tokio::time::timeout(std::time::Duration::from_secs(10), reached_rx) => matches!(result, Ok(Ok(()))),
        _ = &mut abort => false,
    };
    drop(abort);
    let _ = resume_tx.send(());
    assert!(
        reached,
        "abort did not reach the instrumented SQLite instruction"
    );
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .remove_progress_handler();
    drop(connection);
    let tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("cancelled abort must not leave a transaction on the pooled writer");
    tx.rollback().await.unwrap();
    let current = store
        .note_artifact_journal_status("alice", "workspace", "job", &"0".repeat(64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.state, "building");
    assert!(!current.cleanup_complete);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        3 * 4096
    );
    assert_eq!(
        store
            .abort_note_artifact_journal("alice", "workspace", &status.job_ref)
            .await
            .unwrap()
            .state,
        "aborted"
    );
}

async fn append(
    store: &Store,
    sequence: i64,
    previous: &str,
    digest: &str,
    manifest: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES ('00000000000000000000000000000001',?,?,?,?,1,1024,?)")
        .bind(sequence).bind(previous).bind(digest).bind(r#"{"kind":"diff.row","value":{}}"#)
        .bind(manifest).execute(store.artifact_pool().unwrap()).await.map(|_|())
}

#[tokio::test]
async fn artifact_sequence_and_budget_failure_leave_counters_unchanged() {
    let (store, _temporary) = setup().await;
    let initial = "0".repeat(64);
    let first = "1".repeat(64);
    assert!(append(&store, 1, &initial, &first, false).await.is_err());
    assert!(append(&store, 0, &first, &first, false).await.is_err());
    append(&store, 0, &initial, &first, false).await.unwrap();
    assert!(append(&store, 0, &initial, &first, false).await.is_err());
    let row = sqlx::query("SELECT next_sequence,accepted_bytes,index_entries,storage_charge FROM note_artifact_job WHERE generation='00000000000000000000000000000001'")
        .fetch_one(store.artifact_pool().unwrap()).await.unwrap();
    assert_eq!(row.get::<i64, _>("next_sequence"), 1);
    assert_eq!(row.get::<i64, _>("index_entries"), 1);
    assert_eq!(row.get::<i64, _>("storage_charge"), 1024);
    let accepted: i64 = row.get("accepted_bytes");
    assert!(sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES ('00000000000000000000000000000001',1,?,?,?,1,1024,0)")
        .bind(&first).bind("2".repeat(64)).bind("x".repeat(1024)).execute(store.artifact_pool().unwrap()).await.is_err());
    let row = sqlx::query(
        "SELECT next_sequence,accepted_bytes FROM note_artifact_job WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.artifact_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(row.get::<i64, _>("next_sequence"), 1);
    assert_eq!(row.get::<i64, _>("accepted_bytes"), accepted);
}

#[tokio::test]
async fn artifact_manifest_seal_and_lease_are_ordered_and_cannot_revive() {
    let (store, _temporary) = setup().await;
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET state='sealed' WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.artifact_pool().unwrap())
    .await
    .is_err());
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), true)
        .await
        .unwrap();
    assert!(append(&store, 1, &"1".repeat(64), &"2".repeat(64), false)
        .await
        .is_err());
    let lease = "INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES ('00000000000000000000000000000001','admission','lease',?,100)";
    assert!(sqlx::query(lease)
        .bind("1".repeat(64))
        .execute(store.artifact_pool().unwrap())
        .await
        .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='sealed' WHERE generation='00000000000000000000000000000001'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(lease)
        .bind("1".repeat(64))
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    assert!(sqlx::query(lease)
        .bind("1".repeat(64))
        .execute(store.artifact_pool().unwrap())
        .await
        .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    let released: i64 = sqlx::query_scalar(
        "SELECT released FROM note_artifact_lease WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.artifact_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(released, 1);
    assert!(
        sqlx::query("UPDATE note_artifact_lease SET released=0 WHERE generation='00000000000000000000000000000001'")
            .execute(store.artifact_pool().unwrap())
            .await
            .is_err()
    );
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET state='building' WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.artifact_pool().unwrap())
    .await
    .is_err());
}

#[tokio::test]
async fn artifact_abort_does_not_claim_physical_reclamation() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), false)
        .await
        .unwrap();
    assert!(sqlx::query(
        "UPDATE note_artifact_record SET record='changed' WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.artifact_pool().unwrap())
    .await
    .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    let row = sqlx::query("SELECT cleanup_complete,storage_charge FROM note_artifact_job WHERE generation='00000000000000000000000000000001'")
        .fetch_one(store.artifact_pool().unwrap()).await.unwrap();
    assert_eq!(row.get::<i64, _>("cleanup_complete"), 0);
    assert_eq!(row.get::<i64, _>("storage_charge"), 1024);
    let records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM note_artifact_record WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.artifact_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(records, 1);
}

#[tokio::test]
async fn artifact_record_acknowledgements_keep_original_cumulative_bytes() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), false)
        .await
        .unwrap();
    append(&store, 1, &"1".repeat(64), &"2".repeat(64), true)
        .await
        .unwrap();
    sqlx::query("UPDATE note_artifact_job SET state='sealed'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    let rows =
        sqlx::query("SELECT sequence,accepted_bytes FROM note_artifact_ack ORDER BY sequence")
            .fetch_all(store.artifact_pool().unwrap())
            .await
            .unwrap();
    let bytes = i64::try_from(r#"{"kind":"diff.row","value":{}}"#.len()).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64, _>("sequence"), 0);
    assert_eq!(rows[0].get::<i64, _>("accepted_bytes"), bytes);
    assert_eq!(rows[1].get::<i64, _>("accepted_bytes"), bytes * 2);
    assert!(sqlx::query("UPDATE note_artifact_ack SET accepted_bytes=1")
        .execute(store.artifact_pool().unwrap())
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_seal_rejects_missing_final_record_even_with_cached_totals() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), true)
        .await
        .unwrap();
    sqlx::query("DELETE FROM note_artifact_record")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    assert!(sqlx::query("UPDATE note_artifact_job SET state='sealed'")
        .execute(store.artifact_pool().unwrap())
        .await
        .is_err());
    let state: String = sqlx::query_scalar("SELECT state FROM note_artifact_job")
        .fetch_one(store.artifact_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(state, "building");
}

#[tokio::test]
async fn artifact_admit_requires_all_logical_reservations_to_remain_held() {
    let (store, _temporary) = setup().await;
    append(&store, 0, &"0".repeat(64), &"1".repeat(64), true)
        .await
        .unwrap();
    sqlx::query("UPDATE note_artifact_job SET state='sealed'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    sqlx::query("DELETE FROM note_artifact_capacity WHERE scope_kind='global'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    assert!(sqlx::query("INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES ('00000000000000000000000000000001','admission','lease',?,100)")
        .bind("1".repeat(64)).execute(store.artifact_pool().unwrap()).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn artifact_reservations_release_once_only_after_explicit_cleanup() {
    let (store, _temporary) = setup().await;
    let reserved: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(reserved, 3 * 4096);
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET expires_at=101 WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.artifact_pool().unwrap())
    .await
    .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    let after_abort: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(after_abort, reserved);
    // Represents acknowledgement from the future physical file/arena owner,
    // not a claim that DELETE or logical abort returns filesystem capacity.
    for _ in 0..2 {
        sqlx::query(
            "UPDATE note_artifact_job SET cleanup_complete=1 WHERE generation='00000000000000000000000000000001'",
        )
        .execute(store.artifact_pool().unwrap())
        .await
        .unwrap();
    }
    let after_cleanup: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(after_cleanup, 0);
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET cleanup_complete=0 WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.artifact_pool().unwrap())
    .await
    .is_err());
}

#[tokio::test]
async fn artifact_status_and_abort_preserve_principal_and_retired_identity() {
    let (store, _temporary) = setup().await;
    let digest = "0".repeat(64);
    let status = store
        .note_artifact_journal_status("alice", "workspace", "job", &digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "building");
    for (principal, workspace) in [("bob", "workspace"), ("alice", "another")] {
        assert!(store
            .note_artifact_journal_status(principal, workspace, "job", &digest)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .abort_note_artifact_journal(principal, workspace, &status.job_ref)
            .await
            .is_err());
    }
    assert!(store
        .note_artifact_journal_status("alice", "workspace", "job", &"1".repeat(64))
        .await
        .is_err());
    // Source snapshot is intentionally absent/expired: this cleanup handle does
    // not renew it and cannot become a source or consumer read grant.
    for _ in 0..2 {
        let aborted = store
            .abort_note_artifact_journal("alice", "workspace", &status.job_ref)
            .await
            .unwrap();
        assert_eq!(aborted.state, "aborted");
        assert_eq!(aborted.header_digest, digest);
        assert!(!aborted.cleanup_complete);
    }
    let final_status = store
        .note_artifact_journal_status("alice", "workspace", "job", &digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_status.state, "aborted");
    assert_eq!(final_status.job_ref, status.job_ref);
}
