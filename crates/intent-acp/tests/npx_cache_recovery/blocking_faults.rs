//! A real occupied blocking worker must not turn abandoned repair work into a child.

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;

use intent_acp::npx_cache::{repair_npx_cache, CommandEnvironment, NPX_CACHE_REPAIR_BUDGET};
use intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE;
use intentd_test_support::test_timeout;
use serde_json::json;
use tokio::sync::oneshot;

use super::{turn, CwdLease, Fixture};

#[derive(Clone, Default)]
struct ParkFence(Arc<Mutex<Option<oneshot::Sender<()>>>>);

impl ParkFence {
    fn parked(&self) {
        if let Some(sender) = self.0.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }

    async fn wait(&self) {
        let (sender, receiver) = oneshot::channel();
        assert!(self.0.lock().unwrap().replace(sender).is_none());
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), receiver)
            .await
            .expect("current-thread scheduler drains runnable repair tasks")
            .expect("scheduler park fence acknowledged");
    }
}

#[derive(Default)]
struct BlockingWorker(Arc<(Mutex<bool>, Condvar)>);

impl BlockingWorker {
    fn release(&self) {
        let (released, condition) = &*self.0;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        condition.notify_all();
    }

    fn start(&self) -> (oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
        let state = self.0.clone();
        let (sender, ready) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let (released, condition) = &*state;
            let mut released = released.lock().unwrap();
            let _ = sender.send(());
            while !*released {
                released = condition.wait(released).unwrap();
            }
        });
        (ready, worker)
    }
}

impl Drop for BlockingWorker {
    fn drop(&mut self) {
        // Declared after the runtime, so panic unwinding releases the worker
        // before Runtime::drop waits for blocking tasks to finish.
        self.release();
    }
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[track_caller]
fn assert_no_start(fixture: &Fixture, processes: &std::path::Path) {
    assert!(!processes.exists(), "abandoned startup launched Node");
    assert!(
        !fixture.manifest().exists(),
        "abandoned startup wrote a manifest"
    );
    fixture.assert_unrelated_preserved();
}

fn blocking_pool_scenario(cancel: bool) {
    let Some(mut first) = Fixture::new() else {
        return;
    };
    let Some(mut second) = Fixture::new() else {
        return;
    };
    let Some(mut fresh) = Fixture::new() else {
        return;
    };
    let fence = ParkFence::default();
    let park = fence.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .on_thread_park(move || park.parked())
        .build()
        .unwrap();
    let blocker = BlockingWorker::default();
    runtime.block_on(async {
        fresh.prime_registry_metadata().await;
        let first_processes = first.instrument_node();
        let second_processes = second.instrument_node();
        let fixtures = [&first, &second];
        let journals = [&first_processes, &second_processes];
        let mut commands = Vec::new();
        let mut owners = Vec::new();
        let mut weak_owners = Vec::new();
        let mut cleanups = Vec::new();
        let mut cwd_paths = Vec::new();
        for fixture in fixtures {
            let cwd = tempfile::Builder::new()
                .prefix("npx-blocking-owner-")
                .tempdir()
                .unwrap();
            let path = cwd.path().to_owned();
            let (dropped, cleanup) = oneshot::channel();
            let owner = Arc::new(CwdLease {
                cwd: Some(cwd),
                started: fixture.root.path().join("never-started.json"),
                dropped: Some(dropped),
            });
            weak_owners.push(Arc::downgrade(&owner));
            owners.push(owner);
            cleanups.push(cleanup);
            let mut command = fixture.command();
            command.current_dir(&path);
            commands.push(command);
            cwd_paths.push(path);
        }
        let (ready, worker) = blocker.start();
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), ready)
            .await
            .expect("the sole real blocking worker becomes occupied")
            .unwrap();
        let mut repairs: Vec<_> = commands
            .iter()
            .zip(owners)
            .map(|(command, owner)| {
                Box::pin(repair_npx_cache(
                    command,
                    CLAUDE_AGENT_ACP_NPX_PACKAGE,
                    CommandEnvironment::Complete,
                    owner,
                ))
            })
            .collect();
        for repair in &mut repairs {
            assert!(poll_once(repair.as_mut()).await.is_pending());
        }
        // Both production slots start free in this scenario. Parking drains
        // every runnable owned job: each acquires a slot synchronously and
        // queues spawn_blocking before it can become pending. A mere yield
        // or a pending caller alone would not establish that ordering.
        fence.wait().await;
        assert!(!worker.is_finished());
        for ((fixture, journal), owner) in fixtures.iter().zip(journals).zip(&weak_owners) {
            assert!(owner.upgrade().is_some());
            assert_no_start(fixture, journal);
        }
        tokio::time::pause();
        let deadline_result = if cancel {
            drop(repairs);
            None
        } else {
            tokio::time::advance(NPX_CACHE_REPAIR_BUDGET + Duration::from_secs(1)).await;
            let results: Vec<_> = {
                let mut results = Vec::new();
                for repair in &mut repairs {
                    results.push(poll_once(repair.as_mut()).await);
                }
                results
            };
            drop(repairs);
            Some(results)
        };
        tokio::time::resume();
        if let Some(results) = &deadline_result {
            assert_eq!(results, &[Poll::Ready(false), Poll::Ready(false)]);
        }
        let mut cleanup_receipts = Vec::new();
        for cleanup in cleanups {
            let receipt = tokio::time::timeout(test_timeout(Duration::from_secs(30)), cleanup)
                .await
                .expect("queued repair releases its owner while the worker remains occupied")
                .unwrap();
            assert!(receipt["child"].is_null());
            assert_eq!(receipt["cwdExistedUntilLeaseDrop"], true);
            cleanup_receipts.push(receipt);
        }
        assert!(!worker.is_finished(), "cleanup must precede pool release");
        for (((fixture, journal), owner), cwd) in fixtures
            .iter()
            .zip(journals)
            .zip(&weak_owners)
            .zip(&cwd_paths)
        {
            assert!(owner.upgrade().is_none(), "queued owner Arc was retained");
            assert!(!cwd.exists());
            assert_no_start(fixture, journal);
        }
        blocker.release();
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), worker)
            .await
            .expect("occupied worker finishes after release")
            .unwrap();
        // FIFO sentinel runs after every queued repair startup, including
        // aborted queue entries, so absence is checked after draining the pool.
        tokio::time::timeout(
            test_timeout(Duration::from_secs(30)),
            tokio::task::spawn_blocking(|| ()),
        )
        .await
        .expect("blocking queue drains")
        .unwrap();
        for (fixture, journal) in fixtures.iter().zip(journals) {
            assert_no_start(fixture, journal);
        }
        let repaired = tokio::time::timeout(
            test_timeout(Duration::from_secs(30)),
            repair_npx_cache(
                &fresh.command(),
                CLAUDE_AGENT_ACP_NPX_PACKAGE,
                CommandEnvironment::Complete,
                (),
            ),
        )
        .await
        .expect("real npm repair reuses released admission");
        assert!(repaired);
        let manifest = fresh.assert_repaired();
        let requests = turn(&fresh, "after-blocking-pool", false).await;
        for (fixture, journal) in fixtures.iter().zip(journals) {
            assert_no_start(fixture, journal);
        }
        fresh.report(
            if cancel { "blocking-pool-cancellation" } else { "blocking-pool-deadline" },
            &json!({
                "runtime": "current_thread", "maxBlockingThreads": 1,
                "blockerReadyBeforePause": true, "queuedProductionJobs": 2,
                "admissionEvidence": "two free production slots; owned jobs drained to scheduler park behind one occupied blocking worker",
                "mode": if cancel { "cancel" } else { "deadline" },
                "advancedBudgetSeconds": if cancel { 0 } else { NPX_CACHE_REPAIR_BUDGET.as_secs() + 1 },
                "deadlineReturnedFalse": deadline_result.is_some(),
                "ownersReleasedBeforePoolResume": cleanup_receipts,
                "ownerArcsReleased": true, "blockingQueueDrainSentinel": true,
                "processJournals": journals, "childStartsBeforeAndAfterPoolResume": 0,
                "noManifestBeforeAndAfterPoolResume": true,
                "subsequentRealNpmRepair": repaired, "manifest": manifest,
                "requests": requests, "promptCount": 1,
            }),
        );
    });
}

#[test]
fn npx_cache_recovery_blocking_pool_cancellation_releases_owners_without_late_spawn() {
    blocking_pool_scenario(true);
}

#[test]
fn npx_cache_recovery_blocking_pool_deadline_releases_owners_without_late_spawn() {
    blocking_pool_scenario(false);
}
