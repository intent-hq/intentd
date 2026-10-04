//! Observe anonymous statement-journal allocation in an isolated process. This
//! samples real allocated blocks; it is neither a write quota nor a peak proof.
use super::*;
use serde::Serialize;
use serde_json::json;
use sqlx::Acquire;
use std::{collections::BTreeMap, os::unix::fs::MetadataExt, sync::Arc};

const WORKER_ENV: &str = "INTENT_ARTIFACT_STATEMENT_PROBE";

#[derive(Clone, Debug, Serialize)]
struct Backing {
    name: String,
    device: u64,
    inode: u64,
    length: u64,
    allocated: u64,
}

fn open_statement_backing() -> std::io::Result<Vec<Backing>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fd")? {
        let path = entry?.path();
        let Ok(target) = std::fs::read_link(&path) else {
            continue;
        };
        let name = target.to_string_lossy();
        if !name.contains("etilqs_") || !name.ends_with(" (deleted)") {
            continue;
        }
        let metadata = std::fs::metadata(&path)?;
        files.push(Backing {
            name: name.into_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            allocated: metadata.blocks() * 512,
        });
    }
    Ok(files)
}

#[tokio::test]
async fn artifact_arena_statement_journal_worker() {
    if std::env::var_os(WORKER_ENV).is_none() {
        return;
    }
    // The parent runs only this worker. No unrelated SQLite connection can
    // contribute anonymous temp files to the process-level FD observations.
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(&directory.path().join("main.sqlite"))
        .await
        .unwrap();
    let arena_path = directory.path().join("arena.sqlite");
    store
        .configure_test_note_artifact_arena(&arena_path, 512)
        .await
        .unwrap();
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    sqlx::query("CREATE TABLE statement_probe(id INTEGER PRIMARY KEY, body BLOB NOT NULL, marker INTEGER NOT NULL)").execute(&mut *connection).await.unwrap();
    for id in 1..=64 {
        sqlx::query("INSERT INTO statement_probe VALUES (?,zeroblob(8192),0)")
            .bind(id)
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    sqlx::query("CREATE TRIGGER statement_probe_abort BEFORE UPDATE ON statement_probe WHEN new.marker=2 AND old.id=64 BEGIN SELECT RAISE(ABORT,'late statement abort'); END").execute(&mut *connection).await.unwrap();
    // Test-only pressure makes both rollback and implicit statement spilling
    // observable without modifying any process-global SQLite configuration.
    sqlx::query("PRAGMA cache_size=2")
        .execute(&mut *connection)
        .await
        .unwrap();
    assert!(open_statement_backing().unwrap().is_empty());
    let peaks = Arc::new(std::sync::Mutex::new(BTreeMap::<(u64, u64), Backing>::new()));
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let callback_peaks = peaks.clone();
    let callback_errors = errors.clone();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(10, move || match open_statement_backing() {
            Ok(files) => {
                let mut peaks = callback_peaks.lock().unwrap();
                for file in files {
                    let peak = peaks
                        .entry((file.device, file.inode))
                        .or_insert_with(|| file.clone());
                    peak.length = peak.length.max(file.length);
                    peak.allocated = peak.allocated.max(file.allocated);
                }
                true
            }
            Err(error) => {
                callback_errors.lock().unwrap().push(error.to_string());
                false
            }
        });
    let mut tx = connection.begin().await.unwrap();
    sqlx::query("UPDATE statement_probe SET body=randomblob(8192), marker=1")
        .execute(&mut *tx)
        .await
        .unwrap();
    let before: Vec<String> =
        sqlx::query_scalar("SELECT hex(body) FROM statement_probe ORDER BY id")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    let failure = sqlx::query("UPDATE statement_probe SET body=zeroblob(8192), marker=2")
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert!(failure.to_string().contains("late statement abort"));
    let after_abort = open_statement_backing().unwrap();
    let restored: Vec<String> =
        sqlx::query_scalar("SELECT hex(body) FROM statement_probe ORDER BY id")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        before, restored,
        "statement rollback lost the earlier transaction state"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM statement_probe WHERE marker=1")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        64
    );
    tx.rollback().await.unwrap();
    let after_rollback = open_statement_backing().unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM statement_probe WHERE marker=0 AND body=zeroblob(8192)"
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        64
    );
    connection
        .lock_handle()
        .await
        .unwrap()
        .remove_progress_handler();
    assert!(errors.lock().unwrap().is_empty(), "FD observation failed");
    let observed: Vec<_> = peaks.lock().unwrap().values().cloned().collect();
    assert!(
        observed.iter().any(|file| file.allocated > 65_536),
        "did not observe real statement-journal spill: {observed:?}"
    );
    drop(connection);
    store.close().await;
    let after_close = open_statement_backing().unwrap();
    assert!(
        after_close.is_empty(),
        "anonymous backing survived physical close"
    );
    println!(
        "statement-journal-observation: {}",
        json!({"pid":std::process::id(),"sampledPeaks":observed,"afterStatementAbort":after_abort,"afterTransactionRollback":after_rollback,"afterPhysicalClose":after_close,"limits":"Isolated Linux FD observations; not all writes, total peaks, a reservation or a production SQL-shape bound."})
    );
}

#[tokio::test]
async fn artifact_arena_observes_implicit_statement_journal_and_rollback() {
    let root = tempfile::tempdir().unwrap();
    let child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "note_page_repo::artifact_arena::statement_journal_tests::artifact_arena_statement_journal_worker", "--nocapture", "--test-threads=1"])
        .env(WORKER_ENV, "1")
        .env("TMPDIR", root.path())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child)
        .await
        .unwrap()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "statement worker failed: {stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("statement-journal-observation:"),
        "worker did not execute: {stdout}"
    );
    println!("{stdout}");
}
