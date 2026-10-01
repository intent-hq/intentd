//! Disposable-store barriers prove shutdown logs precede waits, not just returns.
#![cfg(unix)]

mod common;

use std::future::{poll_fn, Future};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use intent_store::Store;
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Capture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        let capture = self.clone();
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || capture.clone())
            .finish()
    }
}

fn record<'a>(log: &'a str, phase: &str, state: &str) -> &'a str {
    log.lines()
        .find(|line| {
            line.contains(&format!("phase=\"{phase}\""))
                && line.contains(&format!("state=\"{state}\""))
        })
        .unwrap_or_else(|| panic!("missing {phase}/{state}: {log}"))
}

#[tokio::test]
async fn held_read_checkout_exposes_pool_drain_before_completion() {
    let dir = common::test_tempdir("shutdown-read-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let held = store.read_pool().acquire().await.unwrap();
    let capture = Capture::default();
    let close = store.close().with_subscriber(capture.subscriber());
    tokio::pin!(close);
    tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), async {
        tokio::select! {
            () = store.read_pool().close_event() => {},
            () = &mut close => panic!("close returned while a read connection was held"),
        }
    })
    .await
    .expect("read pool never entered close");

    let before = capture.text();
    record(&before, "wal_checkpoint", "completed");
    record(&before, "write_pool_close", "completed");
    let start = record(&before, "read_pool_close", "started");
    for field in [
        "write_pool_size=0",
        "write_pool_idle=",
        "read_pool_size=",
        "read_pool_idle=",
        "elapsed_ms=0",
    ] {
        assert!(start.contains(field), "missing {field}: {start}");
    }
    assert!(!before.contains("phase=\"read_pool_close\" state=\"completed\""));
    drop(held);
    tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), close)
        .await
        .expect("read pool did not finish after checkout release");
    let after = capture.text();
    let complete = record(&after, "read_pool_close", "completed");
    assert!(complete.contains("read_pool_size=0"), "{complete}");
    assert!(complete.contains("elapsed_ms="), "{complete}");
}

#[tokio::test]
async fn held_write_checkout_exposes_checkpoint_and_cancellation_is_not_success() {
    let dir = common::test_tempdir("shutdown-write-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let held = store.write_pool().acquire().await.unwrap();
    let capture = Capture::default();
    let mut close = Box::pin(store.close().with_subscriber(capture.subscriber()));
    // Poll exactly once: acquiring the only write connection must return Pending.
    poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let before = capture.text();
    let start = record(&before, "wal_checkpoint", "started");
    assert!(start.contains("write_pool_size=1"), "{start}");
    assert!(start.contains("write_pool_idle=0"), "{start}");
    assert!(!before.contains("state=\"completed\""), "{before}");
    drop(close);
    assert_eq!(capture.text(), before, "cancellation must not log success");
    drop(held);
    store.close().await;
}

#[tokio::test]
async fn failed_checkpoint_is_distinct_from_successful_pool_closure() {
    let dir = common::test_tempdir("shutdown-error-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    store.write_pool().close().await;
    let capture = Capture::default();
    store.close().with_subscriber(capture.subscriber()).await;
    let log = capture.text();
    record(&log, "wal_checkpoint", "failed");
    assert!(!log.contains("phase=\"wal_checkpoint\" state=\"completed\""));
    record(&log, "write_pool_close", "completed");
    record(&log, "read_pool_close", "completed");
}

#[test]
fn one_shot_command_has_no_daemon_lifecycle_logs() {
    let dir = common::test_tempdir("shutdown-oneshot-");
    let log_path = dir.path().join("stderr.log");
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    command
        .arg("settings")
        .env("INTENTD_DATA_DIR", dir.path())
        .env("INTENTD_SECRETS_FILE", dir.path().join("secrets.json"))
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&log_path).unwrap());
    let mut child = intentd_test_support::GuardedChild::spawn(&mut command).unwrap();
    let status = child
        .wait_with_timeout(common::test_timeout(Duration::from_secs(10)))
        .unwrap()
        .expect("one-shot command timed out");
    // No daemon exists in this disposable data directory: the command should
    // fail normally, without pretending to shut down a serving daemon.
    assert!(!status.success());
    assert!(std::fs::read_to_string(&log_path)
        .unwrap()
        .contains("cannot connect to daemon"));
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "log") {
            let text = std::fs::read_to_string(path).unwrap();
            assert!(!text.contains("intentd::shutdown"), "{text}");
            assert!(!text.contains("intent_store::close"), "{text}");
        }
    }
}

#[tokio::test]
async fn busy_checkpoint_is_not_reported_as_completed() {
    let dir = common::test_tempdir("shutdown-busy-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    // Only this disposable connection changes its busy timeout: avoid a wall-clock
    // delay while a reader deliberately pins a WAL snapshot across a later write.
    sqlx::query("PRAGMA busy_timeout=0")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE checkpoint_fixture (value INTEGER)")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO checkpoint_fixture VALUES (1)")
        .execute(store.write_pool())
        .await
        .unwrap();
    let mut held = store.read_pool().begin().await.unwrap();
    sqlx::query("SELECT * FROM checkpoint_fixture")
        .fetch_all(&mut *held)
        .await
        .unwrap();
    sqlx::query("INSERT INTO checkpoint_fixture VALUES (2)")
        .execute(store.write_pool())
        .await
        .unwrap();
    let capture = Capture::default();
    let close = store.close().with_subscriber(capture.subscriber());
    tokio::pin!(close);
    tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), async {
        tokio::select! {
            () = store.read_pool().close_event() => {},
            () = &mut close => panic!("close returned while a read transaction was held"),
        }
    })
    .await
    .expect("read pool never entered close after busy checkpoint");
    let log = capture.text();
    record(&log, "wal_checkpoint", "busy");
    assert!(!log.contains("phase=\"wal_checkpoint\" state=\"completed\""));
    held.rollback().await.unwrap();
    tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), close)
        .await
        .expect("close did not finish after transaction release");
}

#[test]
fn serve_error_logs_failure_and_still_times_normal_runtime_drop() {
    let dir = common::test_tempdir("shutdown-serve-error-");
    let log_path = dir.path().join("stderr.log");
    let mut command = common::serve_command();
    command
        .args(["--mode", "invalid-test-mode"])
        .env("INTENTD_DATA_DIR", dir.path())
        .env("INTENTD_SECRETS_FILE", dir.path().join("secrets.json"))
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&log_path).unwrap());
    let mut child = intentd_test_support::GuardedChild::spawn(&mut command).unwrap();
    let status = child
        .wait_with_timeout(common::test_timeout(Duration::from_secs(10)))
        .unwrap()
        .expect("invalid serve command timed out");
    assert!(!status.success());
    let log = std::fs::read_to_string(log_path).unwrap();
    record(&log, "serve_lifetime", "failed");
    record(&log, "runtime_drop", "started");
    record(&log, "runtime_drop", "completed");
    assert!(!log.contains("phase=\"serve_lifetime\" state=\"completed\""));
}

#[test]
fn one_shot_legacy_dry_run_closes_fresh_store_without_timing_logs() {
    let dir = common::test_tempdir("shutdown-import-");
    let root = dir.path().join("legacy");
    let app = dir.path().join("app");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&app).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    let stderr = data.join("stderr.log");
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    command
        .args(["import-legacy", "--dry-run", "--root"])
        .arg(&root)
        .arg("--app-dir")
        .arg(&app)
        .env("INTENTD_DATA_DIR", &data)
        .env("INTENTD_SECRETS_FILE", data.join("secrets.json"))
        .env("INTENTD_WORKSPACES_DIR", dir.path().join("workspaces"))
        // Explicit user directives must not re-enable daemon lifecycle output.
        .env(
            "RUST_LOG",
            "info,intentd::shutdown=trace,intent_store::close=trace",
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap());
    let mut child = intentd_test_support::GuardedChild::spawn(&mut command).unwrap();
    let status = child
        .wait_with_timeout(common::test_timeout(Duration::from_secs(20)))
        .unwrap()
        .expect("legacy dry-run timed out");
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(&stderr).unwrap()
    );
    assert!(
        !data.join("intentd.db").exists(),
        "fresh dry-run DB was not removed after close"
    );
    let mut logs = 0;
    for entry in std::fs::read_dir(&data).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "log") {
            logs += 1;
            let text = std::fs::read_to_string(path).unwrap();
            assert!(!text.contains("intentd::shutdown"), "{text}");
            assert!(!text.contains("intent_store::close"), "{text}");
            assert!(!text.contains("phase="), "{text}");
        }
    }
    assert!(logs >= 2, "must check both stderr and the rotated file");
}
