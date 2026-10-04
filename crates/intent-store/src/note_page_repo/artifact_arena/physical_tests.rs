//! Allocation observations on the real filesystem, not a pre-write quota proof.
use super::*;
use serde_json::{json, Value};
use std::os::unix::fs::MetadataExt;

fn allocation(path: &Path) -> Value {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            json!({"exists":true,"length":metadata.len(),"allocated":metadata.blocks()*512})
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            json!({"exists":false,"length":0,"allocated":0})
        }
        Err(error) => panic!("allocation observation failed: {error}"),
    }
}

fn observe(arena: &Path, phase: &str) -> Value {
    let journal = arena.with_file_name("arena.sqlite-journal");
    let wal = arena.with_file_name("arena.sqlite-wal");
    let shm = arena.with_file_name("arena.sqlite-shm");
    let observation = json!({"phase":phase,"database":allocation(arena),"journal":allocation(&journal),"wal":allocation(&wal),"shm":allocation(&shm)});
    println!("artifact arena allocation: {observation}");
    assert_eq!(observation["wal"]["exists"], false);
    assert_eq!(observation["shm"]["exists"], false);
    observation
}

#[tokio::test]
async fn artifact_arena_measures_retained_journal_and_allocated_page_reuse() {
    let directory = tempfile::tempdir().unwrap();
    let main_path = directory.path().join("main.sqlite");
    let arena_path = directory.path().join("arena.sqlite");
    let store = Store::open(&main_path).await.unwrap();
    store
        .configure_test_note_artifact_arena(&arena_path, 64)
        .await
        .unwrap();
    let pool = store.artifact_pool().unwrap();
    sqlx::query("CREATE TABLE arena_allocation_probe(id INTEGER PRIMARY KEY, body BLOB NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
    // Exercise cache spilling into the actual rollback journal as well as DB
    // free-page reuse; these settings belong only to this diagnostic fixture.
    sqlx::query("PRAGMA cache_size=2")
        .execute(pool)
        .await
        .unwrap();
    let baseline = observe(&arena_path, "empty");
    let mut counts = Vec::new();
    let mut high_water = Vec::new();
    for cycle in 0..3 {
        let mut count = 0;
        for _ in 0..128 {
            match sqlx::query("INSERT INTO arena_allocation_probe(body) VALUES (randomblob(8192))")
                .execute(pool)
                .await
            {
                Ok(_) => count += 1,
                Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("13") => break,
                other => panic!("unexpected write result: {other:?}"),
            }
        }
        assert!((1..128).contains(&count));
        counts.push(count);
        let full = observe(&arena_path, &format!("cycle-{cycle}-full"));
        assert!(full["database"]["length"].as_u64().unwrap() <= 64 * 4096);
        high_water.push(full["database"]["allocated"].as_u64().unwrap());
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("UPDATE arena_allocation_probe SET body=randomblob(8192)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let pending = observe(&arena_path, &format!("cycle-{cycle}-uncommitted"));
        assert!(pending["journal"]["allocated"].as_u64().unwrap() > 0);
        tx.commit().await.unwrap();
        let committed = observe(&arena_path, &format!("cycle-{cycle}-committed"));
        // With exclusive locking, SQLite retains the rollback-journal backing
        // even though journal_mode reports DELETE. Commit is not disk retirement.
        assert!(committed["journal"]["allocated"].as_u64().unwrap() > 0);
        sqlx::query("DELETE FROM arena_allocation_probe")
            .execute(pool)
            .await
            .unwrap();
        let deleted = observe(&arena_path, &format!("cycle-{cycle}-deleted"));
        assert_eq!(
            deleted["database"]["allocated"],
            full["database"]["allocated"]
        );
        let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(pool)
            .await
            .unwrap();
        assert!(free > 0);
    }
    assert!(counts.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(high_water.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(high_water[0] > baseline["database"]["allocated"].as_u64().unwrap());
    store.close().await;
    let closed = observe(&arena_path, "physically-closed");
    assert_eq!(
        closed["database"]["allocated"].as_u64().unwrap(),
        high_water[0]
    );
    // Report actual journal disposition rather than assuming close shrinks it.
    // Temp/statement journals and construction heap are not measured by this probe.
}
