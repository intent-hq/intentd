//! Saturate production admission with real, blocked Node repair children.

use std::fs;
use std::future::{poll_fn, Future};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use intent_acp::npx_cache::{repair_npx_cache, CommandEnvironment, NPX_CACHE_REPAIR_BUDGET};
use intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE;
use intentd_test_support::test_timeout;
use nix::errno::Errno;
use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use super::{turn, CwdLease, Fixture};

// Holders' caller futures are polled explicitly. Their owned production jobs
// still run normally, but advancing the queue's clock cannot first expire a
// holder caller and accidentally admit the queued job we mean to test.
async fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = future;
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[track_caller]
fn marker(path: &Path) -> Option<Value> {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

#[track_caller]
fn child_pid(row: &Value) -> Pid {
    Pid::from_raw(i32::try_from(row["pid"].as_i64().unwrap()).unwrap())
}

// The production owned jobs reap normally. Also kill blocked groups on panic,
// so an assertion regression cannot leave a Node child stalled indefinitely.
struct BlockedChildrenGuard(Vec<PathBuf>);

impl Drop for BlockedChildrenGuard {
    fn drop(&mut self) {
        for path in &self.0 {
            if let Some(row) = marker(path) {
                let pid = child_pid(&row);
                if kill(pid, None) != Err(Errno::ESRCH) {
                    let _ = killpg(pid, Signal::SIGKILL);
                    let _ = kill(pid, Signal::SIGKILL);
                }
            }
        }
    }
}

#[track_caller]
fn lease(started: &Path) -> (CwdLease, PathBuf, oneshot::Receiver<Value>) {
    let cwd = tempfile::Builder::new()
        .prefix("npx-queue-owner-")
        .tempdir()
        .unwrap();
    let path = cwd.path().to_owned();
    let (dropped, receipt) = oneshot::channel();
    (
        CwdLease {
            cwd: Some(cwd),
            started: started.to_owned(),
            dropped: Some(dropped),
        },
        path,
        receipt,
    )
}

#[track_caller]
fn block_installed_manifest(fixture: &mut Fixture, started: &Path) -> PathBuf {
    let processes = fixture.instrument_node();
    fixture.env.insert(
        "NPX_RECOVERY_STARTED".into(),
        started.to_string_lossy().into_owned(),
    );
    fixture.env.insert(
        "NPX_RECOVERY_BLOCK_MANIFEST".into(),
        fixture
            .package
            .join("package.json")
            .to_string_lossy()
            .into_owned(),
    );
    processes
}

#[track_caller]
fn assert_never_started(fixture: &Fixture, processes: &Path, started: &Path) {
    assert!(!processes.exists(), "queued caller started a Node process");
    assert!(!started.exists(), "queued caller reached the manifest read");
    assert!(
        !fixture.manifest().exists(),
        "queued caller wrote a manifest"
    );
    fixture.assert_unrelated_preserved();
}

#[tokio::test]
async fn npx_cache_recovery_saturated_queue_cancellation_and_deadline_spawn_no_late_child() {
    let Some(mut first) = Fixture::new() else {
        return;
    };
    let Some(mut second) = Fixture::new() else {
        return;
    };
    let Some(mut cancelled) = Fixture::new() else {
        return;
    };
    let Some(mut expired) = Fixture::new() else {
        return;
    };
    let Some(mut fresh) = Fixture::new() else {
        return;
    };
    // Complete all npm bootstrap work before starting the holders' budgets.
    fresh.prime_registry_metadata().await;
    let first_marker = first.root.path().join("queue-holder.json");
    let second_marker = second.root.path().join("queue-holder.json");
    let cancelled_marker = cancelled.root.path().join("queue-cancelled.json");
    let expired_marker = expired.root.path().join("queue-expired.json");
    let first_processes = block_installed_manifest(&mut first, &first_marker);
    let second_processes = block_installed_manifest(&mut second, &second_marker);
    let cancelled_processes = block_installed_manifest(&mut cancelled, &cancelled_marker);
    let expired_processes = block_installed_manifest(&mut expired, &expired_marker);
    let _children = BlockedChildrenGuard(vec![
        first_marker.clone(),
        second_marker.clone(),
        cancelled_marker.clone(),
        expired_marker.clone(),
    ]);
    let (first_lease, first_cwd, mut first_cleanup) = lease(&first_marker);
    let (second_lease, second_cwd, mut second_cleanup) = lease(&second_marker);
    let (cancelled_lease, cancelled_cwd, cancelled_cleanup) = lease(&cancelled_marker);
    let (expired_lease, expired_cwd, expired_cleanup) = lease(&expired_marker);
    let mut first_command = first.command();
    first_command.current_dir(&first_cwd);
    let mut second_command = second.command();
    second_command.current_dir(&second_cwd);
    let mut cancelled_command = cancelled.command();
    cancelled_command.current_dir(&cancelled_cwd);
    let mut expired_command = expired.command();
    expired_command.current_dir(&expired_cwd);
    let mut first_repair = Box::pin(repair_npx_cache(
        &first_command,
        CLAUDE_AGENT_ACP_NPX_PACKAGE,
        CommandEnvironment::Complete,
        first_lease,
    ));
    let mut second_repair = Box::pin(repair_npx_cache(
        &second_command,
        CLAUDE_AGENT_ACP_NPX_PACKAGE,
        CommandEnvironment::Complete,
        second_lease,
    ));
    assert!(poll_once(first_repair.as_mut()).await.is_pending());
    assert!(poll_once(second_repair.as_mut()).await.is_pending());
    // Real clock while npm config and Node bootstrap are still running. Pausing
    // earlier lets Tokio auto-advance the bootstrap timeout before npm is ready.
    let holders = tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            if let (Some(first), Some(second)) = (marker(&first_marker), marker(&second_marker)) {
                break [first, second];
            }
            // timing-guard: Poll child-written lstat markers, not a guessed startup delay.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both real repair children reach installed-manifest lstatSync");
    assert_ne!(holders[0]["pid"], holders[1]["pid"]);
    for (row, cwd) in holders.iter().zip([&first_cwd, &second_cwd]) {
        assert_eq!(kill(child_pid(row), None), Ok(()), "holder must be alive");
        assert_eq!(row["cwd"], json!(fs::canonicalize(cwd).unwrap()));
    }
    assert!(matches!(
        first_cleanup.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        second_cleanup.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    let mut queued_cancel = Box::pin(repair_npx_cache(
        &cancelled_command,
        CLAUDE_AGENT_ACP_NPX_PACKAGE,
        CommandEnvironment::Complete,
        cancelled_lease,
    ));
    assert!(poll_once(queued_cancel.as_mut()).await.is_pending());
    tokio::task::yield_now().await;
    assert!(poll_once(queued_cancel.as_mut()).await.is_pending());
    assert_never_started(&cancelled, &cancelled_processes, &cancelled_marker);
    drop(queued_cancel);
    let cancellation =
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), cancelled_cleanup)
            .await
            .expect("cancelled admission releases its owner")
            .unwrap();
    assert!(cancellation["child"].is_null());
    assert_eq!(cancellation["cwdExistedUntilLeaseDrop"], true);
    assert!(!cancelled_cwd.exists());

    tokio::time::pause();
    let mut queued_deadline = Box::pin(repair_npx_cache(
        &expired_command,
        CLAUDE_AGENT_ACP_NPX_PACKAGE,
        CommandEnvironment::Complete,
        expired_lease,
    ));
    assert!(poll_once(queued_deadline.as_mut()).await.is_pending());
    tokio::task::yield_now().await;
    assert!(poll_once(queued_deadline.as_mut()).await.is_pending());
    tokio::time::advance(NPX_CACHE_REPAIR_BUDGET + Duration::from_secs(1)).await;
    let deadline_result = poll_once(queued_deadline.as_mut()).await;
    // Resume before any real process cleanup wait or assertion can fail.
    tokio::time::resume();
    assert_eq!(
        deadline_result,
        Poll::Ready(false),
        "queued caller must expire"
    );
    drop(queued_deadline);
    let deadline = tokio::time::timeout(test_timeout(Duration::from_secs(30)), expired_cleanup)
        .await
        .expect("expired admission releases its owner")
        .unwrap();
    assert!(deadline["child"].is_null());
    assert_eq!(deadline["cwdExistedUntilLeaseDrop"], true);
    assert!(!expired_cwd.exists());
    assert_never_started(&expired, &expired_processes, &expired_marker);
    // Even after the queue deadline, both actual children and their owners are
    // still present. No semaphore substitute or synthetic permit is involved.
    for row in &holders {
        assert_eq!(kill(child_pid(row), None), Ok(()));
    }
    assert!(first_cwd.is_dir() && second_cwd.is_dir());
    assert!(matches!(
        first_cleanup.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        second_cleanup.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(first_repair);
    drop(second_repair);
    let first_receipt = tokio::time::timeout(test_timeout(Duration::from_secs(30)), first_cleanup)
        .await
        .expect("first holder cleanup")
        .unwrap();
    let second_receipt =
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), second_cleanup)
            .await
            .expect("second holder cleanup")
            .unwrap();
    for receipt in [&first_receipt, &second_receipt] {
        assert_eq!(receipt["childReapedBeforeLeaseDrop"], true, "{receipt}");
        assert_eq!(receipt["cwdExistedUntilLeaseDrop"], true, "{receipt}");
    }
    assert!(!first_cwd.exists() && !second_cwd.exists());
    for fixture in [&first, &second] {
        assert!(!fixture.manifest().exists());
        fixture.assert_unrelated_preserved();
    }

    let subsequent = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        repair_npx_cache(
            &fresh.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        ),
    )
    .await
    .expect("fresh real-npm repair acquires a released slot");
    assert!(subsequent);
    let manifest = fresh.assert_repaired();
    let requests = turn(&fresh, "after-saturated-queue", false).await;
    // The queue jobs have dropped their owners; a full subsequent npm/ACP
    // exchange also gives a meaningful interval in which a late spawn is caught.
    assert_never_started(&cancelled, &cancelled_processes, &cancelled_marker);
    assert_never_started(&expired, &expired_processes, &expired_marker);
    let process_journals: Vec<Value> = [first_processes, second_processes]
        .iter()
        .map(|path| {
            json!({"path": path, "rows": fs::read_to_string(path).unwrap().lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>()})
        })
        .collect();
    fresh.report(
        "saturated-queue-cancellation-deadline",
        &json!({
            "holders": holders, "maxActiveRepairMarkers": 2,
            "processJournals": process_journals,
            "queueCancellation": {"polledPending": true, "childStarts": 0, "cleanup": cancellation},
            "queueDeadline": {"advancedBudgetSeconds": NPX_CACHE_REPAIR_BUDGET.as_secs() + 1,
                "result": false, "childStarts": 0, "cleanup": deadline},
            "noLateSpawnAfterSlotReuse": true,
            "ownerCleanup": [first_receipt, second_receipt],
            "subsequentRealNpmRepair": subsequent, "manifest": manifest,
            "requests": requests, "promptCount": 1,
        }),
    );
}
