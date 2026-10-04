//! Actual process termination at the arena commit boundary. These controls prove
//! process-crash recovery, not power-loss durability or physical quota admission.
use super::*;
use std::{io::Write, path::Path, process::Stdio};

const REPORT_ENV: &str = "INTENT_ARTIFACT_CRASH_REPORT";
const PHASE_ENV: &str = "INTENT_ARTIFACT_CRASH_PHASE";

fn publish_report(path: &Path, report: &Value) {
    let temporary = path.with_extension("pending");
    let mut file = std::fs::File::create(&temporary).unwrap();
    file.write_all(&serde_json::to_vec(report).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    std::fs::rename(temporary, path).unwrap();
}

#[tokio::test]
async fn artifact_arena_crash_worker() {
    let Some(report_path) = std::env::var_os(REPORT_ENV) else {
        return;
    };
    let phase = std::env::var(PHASE_ENV).unwrap();
    let (store, temporary, _note, begin) = artifact_begin_fixture().await;
    let job = store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    let append = artifact_append_request(
        &job,
        0,
        &job.header_digest,
        r#"{"kind":"diff.row","value":{}}"#,
    );
    let report = json!({
        "pid":std::process::id(), "path":temporary.path,
        "jobId":begin.job_id, "headerDigest":job.header_digest,
        "generation":job.generation, "append":append,
    });
    if phase == "before" {
        let report_path = std::path::PathBuf::from(&report_path);
        let report = report.clone();
        let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_commit_hook(move || {
                // SQL executed, but SQLite has not committed. The parent kills this
                // process after the atomic marker; no Rust/SQLite destructors run.
                publish_report(&report_path, &report);
                loop {
                    std::thread::park();
                }
            });
        drop(connection);
    }
    let cost = crate::ArtifactJournalRecordCost {
        index_entries: 1,
        storage_bytes: 128,
        final_manifest: false,
    };
    store
        .append_note_artifact_journal("alice", "pages", &append, &cost)
        .await
        .unwrap();
    assert_eq!(phase, "after");
    publish_report(Path::new(&report_path), &report);
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn artifact_arena_crash_preserves_atomic_records_ack_and_reservations() {
    for (phase, accepted) in [("before", 0_i64), ("after", 1)] {
        // The child's fixture directories live beneath this parent's RAII root,
        // so forced termination leaves no unowned test database behind.
        let root = tempfile::tempdir().unwrap();
        let report_path = root.path().join("report.json");
        let log_path = root.path().join("child.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::note_pages::artifact_crash::artifact_arena_crash_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(REPORT_ENV, &report_path)
            .env(PHASE_ENV, phase)
            .env("TMPDIR", root.path())
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                if report_path.exists() {
                    break;
                }
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited: {}",
                    std::fs::read_to_string(&log_path).unwrap()
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            observed.is_ok(),
            "child did not reach {phase} commit: {}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        let report: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
        assert_ne!(
            report["pid"].as_u64().unwrap(),
            u64::from(std::process::id())
        );
        child.kill().await.unwrap();
        assert!(!child.wait().await.unwrap().success());
        let main_path = std::path::PathBuf::from(report["path"].as_str().unwrap());
        assert!(main_path.starts_with(root.path()));
        let reopened = Store::open(&main_path).await.unwrap();
        reopened
            .configure_test_note_artifact_arena(&main_path.with_extension("artifacts.sqlite"), 1024)
            .await
            .unwrap();
        let status = reopened
            .note_artifact_journal_status(
                "alice",
                "pages",
                report["jobId"].as_str().unwrap(),
                report["headerDigest"].as_str().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.generation, report["generation"].as_str().unwrap());
        assert_eq!(status.state, "building");
        assert_eq!(status.next_sequence, accepted);
        for table in ["note_artifact_record", "note_artifact_ack"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(reopened.artifact_pool().unwrap())
                    .await
                    .unwrap(),
                accepted,
                "{phase} {table}"
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT sum(storage_reserved) FROM note_artifact_capacity"
            )
            .fetch_one(reopened.artifact_pool().unwrap())
            .await
            .unwrap(),
            3 * 4096
        );
        let append: intent_core::note_artifact::request::ArtifactAppend =
            serde_json::from_value(report["append"].clone()).unwrap();
        if accepted == 1 {
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT accepted_bytes FROM note_artifact_ack")
                    .fetch_one(reopened.artifact_pool().unwrap())
                    .await
                    .unwrap(),
                i64::try_from(append.record.len()).unwrap()
            );
            assert_eq!(status.current_digest, append.digest);
        }
        // Receipts survive; restart never recreates the prior source lease.
        let cost = crate::ArtifactJournalRecordCost {
            index_entries: 1,
            storage_bytes: 128,
            final_manifest: false,
        };
        assert!(reopened
            .append_note_artifact_journal("alice", "pages", &append, &cost)
            .await
            .is_err());
        reopened.close().await;
    }
}
