//! A single lease must not enumerate other jobs sharing its source snapshot.
use crate::Store;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn release_steps(store: &Store, reference: &str) -> usize {
    let steps = Arc::new(AtomicUsize::new(0));
    let observed = steps.clone();
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            observed.fetch_add(1, Ordering::Relaxed);
            true
        });
    drop(connection);
    store
        .release_note_artifact_lease("alice", "ws", reference)
        .await
        .unwrap();
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .remove_progress_handler();
    steps.load(Ordering::Relaxed)
}

#[tokio::test]
async fn artifact_release_does_not_materialize_other_snapshot_jobs() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(&directory.path().join("source.sqlite"))
        .await
        .unwrap();
    store
        .configure_note_artifact_arena(&directory.path().join("arena.sqlite"), 4096)
        .await
        .unwrap();
    let pool = store.artifact_pool().unwrap();
    for (kind, id) in [("global", ""), ("principal", "alice"), ("workspace", "ws")] {
        sqlx::query("INSERT INTO note_artifact_capacity(scope_kind,scope_id,payload_limit,record_limit,index_limit,storage_limit,job_limit) VALUES (?,?,1000000,10000,10000,10000000,2048)")
            .bind(kind).bind(id).execute(pool).await.unwrap();
    }
    let digest = "0".repeat(64);
    let generation = "00000000000000000000000000000001";
    let snapshot = "00000000000000000000000000000002";
    let lease = "00000000000000000000000000000003";
    let insert = "INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) VALUES ('alice','ws',?,?,'runtime',?,'{}',?,'revision','note','instance','f:source','building',100,200,100,1,1,4096,?)";
    sqlx::query(insert)
        .bind("target")
        .bind(generation)
        .bind(&digest)
        .bind(snapshot)
        .bind(&digest)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES (?,0,?,?,'{}',1,100,1)")
        .bind(generation).bind(&digest).bind(&digest).execute(pool).await.unwrap();
    sqlx::query("UPDATE note_artifact_job SET state='sealed' WHERE generation=?")
        .bind(generation)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES (?,'admission',?,?,100)")
        .bind(generation).bind(lease).bind(&digest).execute(pool).await.unwrap();
    let reference = store.note_pages.reference(snapshot, &format!("l:{lease}"));
    // Both measurements replay the same already-released receipt. No timer,
    // cache warmth or first-release mutation is used as the growth oracle.
    store
        .release_note_artifact_lease("alice", "ws", &reference)
        .await
        .unwrap();
    let before = release_steps(&store, &reference).await;
    let mut tx = pool.begin().await.unwrap();
    for ordinal in 16..1040 {
        sqlx::query(insert)
            .bind(format!("other-{ordinal}"))
            .bind(format!("{ordinal:032x}"))
            .bind(&digest)
            .bind(snapshot)
            .bind(&digest)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let after = release_steps(&store, &reference).await;
    println!("artifact release VM steps: one job={before}, 1025 jobs={after}");
    assert!(
        after <= before * 4 + 512,
        "single lease release enumerated unrelated jobs: {before} -> {after}"
    );
    let untouched: i64 =
        sqlx::query_scalar("SELECT count(*) FROM note_artifact_job WHERE state='building'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(untouched, 1024);
    store.close().await;
}
