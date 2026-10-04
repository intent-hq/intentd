//! Real disposable SQLite invariants for the prepared artifact lifecycle.
//! Seeded jobs are not source-authorization or profile-producer acceptance.
use super::TempDb;
use crate::Store;
use sqlx::Row;

async fn setup() -> (Store, TempDb) {
    let temporary = TempDb::new();
    let store = Store::open(&temporary.path).await.unwrap();
    for (kind, id) in [
        ("global", ""),
        ("principal", "alice"),
        ("workspace", "workspace"),
    ] {
        sqlx::query("INSERT INTO note_artifact_capacity(scope_kind,scope_id,payload_limit,record_limit,index_limit,storage_limit,job_limit) VALUES (?,?,1024,2,2,4096,1)")
            .bind(kind).bind(id).execute(store.write_pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) VALUES ('alice','workspace','job','00000000000000000000000000000001','runtime',?, '{}','00000000000000000000000000000002','revision','note','instance','f:source','building',100,200,1024,2,2,4096,?)")
        .bind("0".repeat(64)).bind("0".repeat(64)).execute(store.write_pool()).await.unwrap();
    (store, temporary)
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
        .bind(manifest).execute(store.write_pool()).await.map(|_|())
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
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(row.get::<i64, _>("next_sequence"), 1);
    assert_eq!(row.get::<i64, _>("index_entries"), 1);
    assert_eq!(row.get::<i64, _>("storage_charge"), 1024);
    let accepted: i64 = row.get("accepted_bytes");
    assert!(sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES ('00000000000000000000000000000001',1,?,?,?,1,1024,0)")
        .bind(&first).bind("2".repeat(64)).bind("x".repeat(1024)).execute(store.write_pool()).await.is_err());
    let row = sqlx::query(
        "SELECT next_sequence,accepted_bytes FROM note_artifact_job WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.read_pool())
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
    .execute(store.write_pool())
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
        .execute(store.write_pool())
        .await
        .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='sealed' WHERE generation='00000000000000000000000000000001'")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query(lease)
        .bind("1".repeat(64))
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(sqlx::query(lease)
        .bind("1".repeat(64))
        .execute(store.write_pool())
        .await
        .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let released: i64 = sqlx::query_scalar(
        "SELECT released FROM note_artifact_lease WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(released, 1);
    assert!(
        sqlx::query("UPDATE note_artifact_lease SET released=0 WHERE generation='00000000000000000000000000000001'")
            .execute(store.write_pool())
            .await
            .is_err()
    );
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET state='building' WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.write_pool())
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
    .execute(store.write_pool())
    .await
    .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let row = sqlx::query("SELECT cleanup_complete,storage_charge FROM note_artifact_job WHERE generation='00000000000000000000000000000001'")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(row.get::<i64, _>("cleanup_complete"), 0);
    assert_eq!(row.get::<i64, _>("storage_charge"), 1024);
    let records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM note_artifact_record WHERE generation='00000000000000000000000000000001'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(records, 1);
}

#[tokio::test]
async fn artifact_reservations_release_once_only_after_explicit_cleanup() {
    let (store, _temporary) = setup().await;
    let reserved: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(reserved, 3 * 4096);
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET expires_at=101 WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.write_pool())
    .await
    .is_err());
    sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation='00000000000000000000000000000001'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let after_abort: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(after_abort, reserved);
    // Represents acknowledgement from the future physical file/arena owner,
    // not a claim that DELETE or logical abort returns filesystem capacity.
    for _ in 0..2 {
        sqlx::query(
            "UPDATE note_artifact_job SET cleanup_complete=1 WHERE generation='00000000000000000000000000000001'",
        )
        .execute(store.write_pool())
        .await
        .unwrap();
    }
    let after_cleanup: i64 =
        sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(after_cleanup, 0);
    assert!(sqlx::query(
        "UPDATE note_artifact_job SET cleanup_complete=0 WHERE generation='00000000000000000000000000000001'"
    )
    .execute(store.write_pool())
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
