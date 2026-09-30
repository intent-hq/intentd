//! Actual Store/Git/P effects under explicit original transport fixtures. The
//! daemon target separately proves real sockets; these callers do not claim it.
use super::credential_tests::{services, Server};
use super::*;
use crate::repository_admission_source_tests::fixtures::Fixture as Git;
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::{HostRole, WorkspaceApi};

async fn refused_stage_response(after_commit: bool, expire_queue: bool) {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let first = if after_commit {
        f.stage("before-refused-create.txt");
        Stage::Commit
    } else {
        Stage::CreatePr
    };
    let mut query = f.query(first);
    query.options.create_pr_after_push = after_commit;
    let p = s.prepare(&f, query).await;
    let q = command(&f, &p, first);
    let connection = s.concrete(&f).await;
    let op = connection.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    with_review_clock(s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&connection).unwrap();
        op.progress.lock().unwrap().started = true;
        let create = create_lock(&connection, &op).unwrap();
        let held = create.clone().lock_owned().await;
        let deadline = Instant::now() + FRAME_TTL;
        let work = async {
            let _capacity = capacity;
            let _finish = Completion(op.clone());
            run(&connection, &op, &q, deadline).await
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("work ended before branch-pair wait: {result:?}"),
            () = wait_until(|| op.progress.lock().unwrap().engine.is_some()
                && Arc::strong_count(&create) > 2) => {},
        }
        assert_eq!(
            op.progress.lock().unwrap().effects.len(),
            usize::from(after_commit)
        );
        let metadata_holder = if expire_queue {
            tokio::time::advance(FRAME_TTL).await;
            None
        } else {
            // The real worker has passed preflight and is waiting for its
            // branch pair. Hold the original optional config lock only now;
            // the later synchronous P comparison must refuse its stage claim.
            let (entered, waiting) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            let facts = op.metadata.provider.clone();
            let holder = std::thread::spawn(move || {
                facts.hold_native_review_metadata_for_test(entered, released);
            });
            waiting.await.unwrap();
            let mut claimed = false;
            assert!(op
                .metadata
                .with_metadata(|| {
                    claimed = true;
                    Ok(())
                })
                .is_err());
            assert!(!claimed);
            Some((release, holder))
        };
        drop(held);
        assert!(work.await.is_err());
        // The original run and Completion have joined before final disclosure.
        // Removing contention cannot renew or retry the consumed command.
        if let Some((release, holder)) = metadata_holder {
            release.send(()).unwrap();
            holder.join().unwrap();
        }
        assert!(op.progress.lock().unwrap().settled.is_some());
        assert_eq!(connection.review.workers.available_permits(), WORKERS);
        assert_eq!(
            connection
                .services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS
        );
        frame.retire();
    }))
    .await;
    let response = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    let reconciled = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(response["reviewExecution"], reconciled["reviewExecution"]);
    let receipts = if after_commit {
        json!([{"stage":"commit", "commitHash":f.git.git(&f.git.path, &["rev-parse", "HEAD"]).trim()}])
    } else {
        json!([])
    };
    assert_eq!(response["reviewExecution"]["gitReceipts"], receipts);
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    assert_eq!(
        response["success"], false,
        "refused attempt settled successful: {response}"
    );
    assert_eq!(
        response["reviewExecution"]["outcome"]["status"], "failed",
        "{response}"
    );
    assert_eq!(response["reviewExecution"]["outcome"]["stage"], "create-pr");
    assert_eq!(
        response["reviewExecution"]["outcome"]["code"],
        "repository-admission-retired"
    );
}

#[intent_test_macros::daemon_test]
async fn native_review_refused_queue_stage_settles_failed_in_original_history() {
    refused_stage_response(false, true).await;
}

#[intent_test_macros::daemon_test]
async fn native_review_refused_consuming_stage_settles_failed_in_original_history() {
    refused_stage_response(false, false).await;
}

#[intent_test_macros::daemon_test]
async fn native_review_refused_later_stage_retains_actual_commit_in_original_history() {
    refused_stage_response(true, false).await;
}

// Keep paused time from auto-advancing while the real SQLite/provider workers
// run. Tests advance only the deadline under examination, without wall sleeps.
async fn with_review_clock<T>(work: impl Future<Output = T>) -> T {
    tokio::time::pause();
    let result = tokio::select! {
        result = work => result,
        () = async { loop { tokio::task::yield_now().await; } } => unreachable!(),
    };
    tokio::time::resume();
    result
}

#[intent_test_macros::daemon_test]
async fn native_review_queue_deadline_does_not_retire_admitted_commit_or_next_stage() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("queue-crossing.txt");
    let mut query = f.query(Stage::Commit);
    query.options.create_pr_after_push = true;
    let p = s.prepare(&f, query).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    with_review_clock(s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        // A real SQLite writer holds post-commit attribution. Reads, original
        // admission and the Git primitive still use their unchanged paths.
        let transaction = f
            .services
            .store
            .read_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        let deadline = Instant::now() + FRAME_TTL;
        let work = async {
            let _capacity = capacity;
            let _finish = Completion(op.clone());
            run(&c, &op, &q, deadline).await
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("work ended before held attribution: {result:?}"),
            () = wait_until(|| !op.progress.lock().unwrap().effects.is_empty()
                && f.services.store.write_pool().num_idle() == 0) => {},
        }
        tokio::time::advance(FRAME_TTL + Duration::from_secs(1)).await;
        // Poll the actual sole owner after the deadline while its real worker
        // is still held. This makes the old queue timeout branch deterministic.
        std::future::poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let still_eligible = op.write_current().is_ok();
        assert_eq!(c.review.workers.available_permits(), WORKERS - 1);
        assert_eq!(
            c.services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS - 1
        );
        assert!(op.progress.lock().unwrap().settled.is_none());
        assert!(f
            .services
            .worktree_locks
            .try_with_lock(&f.git.path, || async {})
            .await
            .is_none());
        transaction.rollback().await.unwrap();
        work.await.unwrap();
        frame.retire();
        // Join/release the owned work even in the red schedule before reporting
        // the violated boundary, retaining the actual primitive receipt.
        assert_eq!(c.review.workers.available_permits(), WORKERS);
        assert!(
            still_eligible,
            "15s queue deadline retired an already admitted commit"
        );
    }))
    .await;
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        state["reviewExecution"]["outcome"]["status"], "created",
        "{state}"
    );
    assert_eq!(
        state["reviewExecution"]["gitReceipts"],
        json!([{
            "stage":"commit", "commitHash":f.git.git(&f.git.path, &["rev-parse", "HEAD"]).trim()
        }])
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn native_review_queue_expiry_drops_unentered_work_before_capacity_and_lock_release() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("expired-queue.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    with_review_clock(s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        f.services
            .worktree_locks
            .with_lock(&f.git.path, || async {
                let deadline = Instant::now() + FRAME_TTL;
                let work = async {
                    let _capacity = capacity;
                    let _finish = Completion(op.clone());
                    run(&c, &op, &q, deadline).await
                };
                tokio::pin!(work);
                std::future::poll_fn(|cx| {
                    assert!(work.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                // Tokio rounds timer deadlines up to a millisecond tick. Cross
                // that tick while leaving the production deadline unchanged.
                tokio::time::advance(FRAME_TTL + Duration::from_millis(1)).await;
                assert!(work.await.is_err());
                assert_eq!(c.review.workers.available_permits(), WORKERS);
                assert_eq!(
                    c.services
                        .repository_review_capacity
                        .workers
                        .available_permits(),
                    GLOBAL_WORKERS
                );
                assert!(op.progress.lock().unwrap().engine.is_none());
                assert!(op.progress.lock().unwrap().effects.is_empty());
            })
            .await;
        // The original future has been dropped before the held worktree becomes
        // available; letting the executor run cannot revive late observation.
        tokio::task::yield_now().await;
        assert!(op.progress.lock().unwrap().engine.is_none());
        assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), before);
        frame.retire();
    }))
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(state["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(state["reviewExecution"]["outcome"]["status"], "failed");
}

#[intent_test_macros::daemon_test]
async fn native_review_queue_deadline_wins_ready_first_claim_after_observation() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    with_review_clock(s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        let create = create_lock(&c, &op).unwrap();
        let held = create.clone().lock_owned().await;
        let deadline = Instant::now() + FRAME_TTL;
        let work = async {
            let _capacity = capacity;
            let _finish = Completion(op.clone());
            run(&c, &op, &q, deadline).await
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("work ended before branch-pair wait: {result:?}"),
            () = wait_until(|| op.progress.lock().unwrap().engine.is_some()
                && Arc::strong_count(&create) > 2) => {},
        }
        assert!(f
            .services
            .worktree_locks
            .try_with_lock(&f.git.path, || async {})
            .await
            .is_none());
        tokio::time::advance(FRAME_TTL).await;
        // Both expiry and the real blocking worker can now progress. Even if
        // that worker wins scheduling, its first consuming claim is too late.
        drop(held);
        assert!(work.await.is_err());
        assert!(op.write_current().is_err());
        assert_eq!(c.review.workers.available_permits(), WORKERS);
        assert_eq!(
            c.services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS
        );
        assert!(op.progress.lock().unwrap().effects.is_empty());
        assert!(f
            .services
            .worktree_locks
            .try_with_lock(&f.git.path, || async {})
            .await
            .is_some());
        frame.retire();
    }))
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_member_boundary_refusal_preserves_failed_history_without_effects() {
    // Freeze time before preparation creates its monitor. The actual comparison
    // is refused by the held original config, not by an uncontrolled timer race.
    with_review_clock(async {
        let fixture = Fixture::new().await;
        let (socket, _) = member(&fixture).await;
        let remote_sha = fixture.server.control.sha.lock().unwrap().clone();
        let matching = super::credential_tests::review(&remote_sha);
        *fixture.server.control.reviews.lock().unwrap() = vec![matching.clone()];
        let head = fixture.git.git(&fixture.git.path, &["rev-parse", "HEAD"]);
        let status = fixture
            .git
            .git(&fixture.git.path, &["status", "--porcelain=v1"]);
        let fixed_time = Instant::now();
        let prepared = socket.prepare(&fixture, fixture.query(Stage::CreatePr)).await;
        let command = command(&fixture, &prepared, Stage::CreatePr);
        let connection = socket.concrete(&fixture).await;
        let operation = connection.review.feed.lock().unwrap().records
            [&command.review.operation_id]
            .clone();
        socket
            .entered(async {
                let frame = socket
                    .owner
                    .capture_review(&Frame::Execute(command.clone()))
                    .unwrap();
                let capacity = job(&connection).unwrap();
                operation.progress.lock().unwrap().started = true;
                let branch_pair = create_lock(&connection, &operation).unwrap();
                let held_pair = branch_pair.clone().lock_owned().await;
                let work = async {
                    let _capacity = capacity;
                    let _finish = Completion(operation.clone());
                    run(&connection, &operation, &command, fixed_time + FRAME_TTL).await
                };
                tokio::pin!(work);
                tokio::select! {
                    result = &mut work => panic!("run ended before branch-pair barrier: {result:?}"),
                    () = wait_until(|| operation.progress.lock().unwrap().engine.is_some()
                        && Arc::strong_count(&branch_pair) > 2) => {},
                }
                assert_eq!(connection.review.workers.available_permits(), WORKERS - 1);
                assert_eq!(
                    fixture.services.repository_review_capacity.workers.available_permits(),
                    GLOBAL_WORKERS - 1
                );
                assert!(fixture.services.worktree_locks
                    .try_with_lock(&fixture.git.path, || async {}).await.is_none());
                let (entered, waiting) = tokio::sync::oneshot::channel();
                let (release, released) = std::sync::mpsc::channel();
                let facts = operation.metadata.provider.clone();
                let holder = std::thread::spawn(move || {
                    facts.hold_native_review_metadata_for_test(entered, released);
                });
                waiting.await.unwrap();
                let mut claims = 0;
                assert!(operation.metadata.with_metadata(|| {
                    claims += 1;
                    Ok(())
                }).is_err());
                assert_eq!(claims, 0);
                assert_eq!(Instant::now(), fixed_time);
                drop(held_pair);
                assert!(work.await.is_err());
                // Only completed original work can release capacity. Metadata
                // remains held until the original engine and Completion settle.
                {
                    let progress = operation.progress.lock().unwrap();
                    assert!(progress.settled.is_some());
                    assert!(progress.effects.is_empty());
                    assert!(progress.review_effect.is_none());
                    assert!(matches!(progress.execution.as_ref().unwrap().outcome,
                        Outcome::Failed { stage: Stage::CreatePr, .. }));
                }
                assert_eq!(connection.review.workers.available_permits(), WORKERS);
                assert_eq!(
                    fixture.services.repository_review_capacity.workers.available_permits(),
                    GLOBAL_WORKERS
                );
                assert!(fixture.services.worktree_locks
                    .try_with_lock(&fixture.git.path, || async {}).await.is_some());
                assert_eq!(fixture.server.control.posts.load(Ordering::SeqCst), 0);
                release.send(()).unwrap();
                holder.join().unwrap();
                frame.retire();
            })
            .await;
        // This observes the already consumed command, with no renewed admission.
        let response = socket
            .request(&fixture.services, Frame::Execute(command.clone()))
            .await
            .unwrap();
        let reconciled = socket
            .request(&fixture.services, Frame::Reconcile(bound(&command)))
            .await
            .unwrap();
        assert_eq!(response["success"], false, "{response}");
        assert_eq!(response["reviewExecution"], reconciled["reviewExecution"]);
        assert_eq!(response["reviewExecution"]["outcome"]["status"], "failed");
        assert_eq!(response["reviewExecution"]["outcome"]["stage"], "create-pr");
        assert_eq!(response["reviewExecution"]["outcome"]["code"], "repository-admission-retired");
        assert_eq!(response["reviewExecution"]["gitReceipts"], json!([]));
        for side in ["source", "target"] {
            assert!(prepared["reviewPreparation"][side].get("connection").is_none());
            assert!(response["reviewExecution"]["preparation"][side].get("connection").is_none());
        }
        assert_eq!(fixture.server.control.posts.load(Ordering::SeqCst), 0);
        assert_eq!(*fixture.server.control.reviews.lock().unwrap(), vec![matching]);
        assert_eq!(*fixture.server.control.sha.lock().unwrap(), remote_sha);
        assert_eq!(fixture.git.git(&fixture.git.path, &["rev-parse", "HEAD"]), head);
        assert_eq!(fixture.git.git(&fixture.git.path, &["status", "--porcelain=v1"]), status);
        assert_eq!(Instant::now(), fixed_time);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_member_boundary_admitted_reuse_survives_retirement_and_reply_failure() {
    // The original monitor cannot contend before admission: its clock is fixed
    // from before preparation. Real Store/Git/HTTP and every R/P check still run.
    with_review_clock(async {
        let fixture = Fixture::new().await;
        let (socket, _) = member(&fixture).await;
        let remote_sha = fixture.server.control.sha.lock().unwrap().clone();
        let matching = super::credential_tests::review(&remote_sha);
        *fixture.server.control.reviews.lock().unwrap() = vec![matching.clone()];
        let head = fixture.git.git(&fixture.git.path, &["rev-parse", "HEAD"]);
        let status = fixture
            .git
            .git(&fixture.git.path, &["status", "--porcelain=v1"]);
        let fixed_time = Instant::now();
        let prepared = socket
            .prepare(&fixture, fixture.query(Stage::CreatePr))
            .await;
        let command = command(&fixture, &prepared, Stage::CreatePr);
        let connection = socket.concrete(&fixture).await;
        let operation =
            connection.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
        *fixture.server.control.pause.lock().unwrap() = Some("/merge_requests".into());
        let response = socket
            .entered(async {
                let frame = socket
                    .owner
                    .capture_review(&Frame::Execute(command.clone()))
                    .unwrap();
                let mut observed = None;
                {
                    let call = frame.scope(Box::pin(async {
                        let reply = fixture
                            .services
                            .native_review_execute(command.clone())
                            .await
                            .unwrap();
                        assert_eq!(reply["success"], true, "{reply}");
                        assert_eq!(reply["reviewExecution"]["outcome"]["status"], "reused");
                        assert!(matches!(
                            operation.progress.lock().unwrap().review_effect,
                            Some(Outcome::Reused { .. })
                        ));
                        let mut transfers = 0;
                        assert!(frame
                            .deliver(RepositoryReadReplyKind::Result, &mut || {
                                transfers += 1;
                                Err(Error::Internal("owned Member reply consumed".into()))
                            })
                            .await
                            .is_err());
                        assert!(frame
                            .deliver(RepositoryReadReplyKind::Result, &mut || {
                                transfers += 1;
                                Ok(())
                            })
                            .await
                            .is_err());
                        assert_eq!(transfers, 1);
                        observed = Some(reply);
                    }));
                    tokio::pin!(call);
                    tokio::select! {
                        () = &mut call => panic!("response preceded admitted matching GET barrier"),
                        () = fixture.server.control.entered.notified() => {},
                    }
                    {
                        let requests = fixture.server.control.requests.lock().unwrap();
                        let (method, path) = requests.last().unwrap();
                        assert_eq!(method, "GET");
                        assert!(path.contains("/merge_requests"));
                    }
                    assert_eq!(Instant::now(), fixed_time);
                    assert_eq!(fixture.server.control.posts.load(Ordering::SeqCst), 0);
                    // Actual arrival at the authenticated HTTP fixture is later than
                    // the original stage and request claims, not preflight evidence.
                    operation.retire();
                    assert!(operation.write_current().is_err());
                    assert!(operation.disclosure_current().is_ok());
                    assert!(operation.progress.lock().unwrap().settled.is_none());
                    assert!(operation.progress.lock().unwrap().review_effect.is_none());
                    assert_eq!(connection.review.workers.available_permits(), WORKERS - 1);
                    assert_eq!(
                        fixture
                            .services
                            .repository_review_capacity
                            .workers
                            .available_permits(),
                        GLOBAL_WORKERS - 1
                    );
                    assert!(fixture
                        .services
                        .worktree_locks
                        .try_with_lock(&fixture.git.path, || async {})
                        .await
                        .is_none());
                    fixture.server.control.release.notify_one();
                    call.await;
                }
                frame.retire();
                observed.unwrap()
            })
            .await;
        assert!(operation.progress.lock().unwrap().settled.is_some());
        assert_eq!(connection.review.workers.available_permits(), WORKERS);
        assert_eq!(
            fixture
                .services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS
        );
        assert!(fixture
            .services
            .worktree_locks
            .try_with_lock(&fixture.git.path, || async {})
            .await
            .is_some());
        let requests = fixture.server.control.requests.lock().unwrap().clone();
        let reconciled = socket
            .request(&fixture.services, Frame::Reconcile(bound(&command)))
            .await
            .unwrap();
        assert_eq!(response["reviewExecution"], reconciled["reviewExecution"]);
        assert_eq!(reconciled["reviewExecution"]["outcome"]["status"], "reused");
        assert_eq!(reconciled["reviewExecution"]["gitReceipts"], json!([]));
        for side in ["source", "target"] {
            assert!(prepared["reviewPreparation"][side]
                .get("connection")
                .is_none());
            assert!(reconciled["reviewExecution"]["preparation"][side]
                .get("connection")
                .is_none());
        }
        assert_eq!(*fixture.server.control.requests.lock().unwrap(), requests);
        assert_eq!(fixture.server.control.posts.load(Ordering::SeqCst), 0);
        assert_eq!(
            *fixture.server.control.reviews.lock().unwrap(),
            vec![matching]
        );
        assert_eq!(*fixture.server.control.sha.lock().unwrap(), remote_sha);
        assert_eq!(
            fixture.git.git(&fixture.git.path, &["rev-parse", "HEAD"]),
            head
        );
        assert_eq!(
            fixture
                .git
                .git(&fixture.git.path, &["status", "--porcelain=v1"]),
            status
        );
        assert_eq!(Instant::now(), fixed_time);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_queue_stage_wall_retains_owned_work_receipt_and_stops_next_stage() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("stage-wall.txt");
    let mut query = f.query(Stage::Commit);
    query.options.create_pr_after_push = true;
    let p = s.prepare(&f, query).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    with_review_clock(s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        let transaction = f
            .services
            .store
            .read_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        let deadline = Instant::now() + FRAME_TTL;
        let work = async {
            let _capacity = capacity;
            let _finish = Completion(op.clone());
            run(&c, &op, &q, deadline).await
        };
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("work ended before held attribution: {result:?}"),
            () = wait_until(|| !op.progress.lock().unwrap().effects.is_empty()
                && f.services.store.write_pool().num_idle() == 0) => {},
        }
        let effects = op.progress.lock().unwrap().effects.clone();
        tokio::task::yield_now().await;
        tokio::time::advance(STAGE_TTL.checked_sub(Duration::from_secs(1)).unwrap()).await;
        std::future::poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(op.write_current().is_ok());
        tokio::time::advance(Duration::from_secs(2)).await;
        wait_until(|| op.write_current().is_err()).await;
        std::future::poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(c.review.workers.available_permits(), WORKERS - 1);
        assert_eq!(
            c.services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS - 1
        );
        assert!(op.progress.lock().unwrap().settled.is_none());
        assert_eq!(op.progress.lock().unwrap().effects, effects);
        assert!(f
            .services
            .worktree_locks
            .try_with_lock(&f.git.path, || async {})
            .await
            .is_none());
        transaction.rollback().await.unwrap();
        work.await.unwrap();
        assert_eq!(c.review.workers.available_permits(), WORKERS);
        assert_eq!(
            c.services
                .repository_review_capacity
                .workers
                .available_permits(),
            GLOBAL_WORKERS
        );
        frame.retire();
    }))
    .await;
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        state["reviewExecution"]["gitReceipts"],
        json!([{"stage":"commit", "commitHash":head.trim()}])
    );
    assert_eq!(
        state["reviewExecution"]["outcome"]["status"], "failed",
        "{state}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.request(&f.services, Frame::Execute(q)).await.unwrap()["reviewExecution"],
        state["reviewExecution"]
    );
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
}

struct Fixture {
    git: Git,
    services: Arc<Services>,
    registry: Arc<crate::SettingsRegistry>,
    server: Server,
}
impl Fixture {
    async fn new() -> Self {
        let git = Git::new().await;
        git.git(&git.path, &["config", "user.name", "Fixture"]);
        git.git(
            &git.path,
            &["config", "user.email", "fixture@example.invalid"],
        );
        let server = Server::new().await;
        *server.control.sha.lock().unwrap() =
            git.git(&git.path, &["rev-parse", "HEAD"]).trim().into();
        let (services, registry) = services(&server, &git).await;
        Self {
            git,
            services,
            registry,
            server,
        }
    }
    fn query(&self, action: Stage) -> Prepare {
        serde_json::from_value(json!({"workspaceId":self.git.workspace.id,"action":action,"review":{"root":self.git.root(),"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":"https://gitlab.test/forge","projectPath":"group/project"}},"targetBranch":"trunk"}})).unwrap()
    }
    async fn socket(&self) -> Socket {
        let p = self.services.store.get_primary_principal().await.unwrap();
        Socket::new(
            &self.services,
            Caller::Wire {
                principal_id: p.id,
                host_role: HostRole::Owner,
            },
            None,
        )
        .await
    }
    fn stage(&self, name: &str) {
        std::fs::write(self.git.path.join(name), name).unwrap();
        self.git.git(&self.git.path, &["add", name]);
    }
}
struct Socket {
    owner: Arc<dyn RepositoryReadConnection>,
    caller: Caller,
    wire: Option<WireCredential>,
    _context: Mutex<Box<dyn RepositoryReadRetirements>>,
    receiver: Mutex<Option<Box<dyn NativeReviewRetirements>>>,
}
impl Socket {
    async fn new(s: &Arc<Services>, caller: Caller, wire: Option<WireCredential>) -> Self {
        let owner = with_caller(
            caller.clone(),
            with_wire_credential(wire.clone(), async {
                super::super::connection(
                    s,
                    if wire.is_some() {
                        RepositoryWireEntry::Bearer
                    } else {
                        RepositoryWireEntry::AdmittedLocal
                    },
                )
                .unwrap()
            }),
        )
        .await;
        let context = owner.take_retirements().unwrap();
        let receiver = owner.take_review_retirements().unwrap();
        Self {
            owner,
            caller,
            wire,
            _context: Mutex::new(context),
            receiver: Mutex::new(Some(receiver)),
        }
    }
    async fn entered<T>(&self, f: impl Future<Output = T>) -> T {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.wire.clone(), f),
        )
        .await
    }
    async fn request(&self, s: &Services, f: Frame) -> Result<Value> {
        self.entered(async {
            let scope = self.owner.capture_review(&f).unwrap();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let reply = match f {
                        Frame::Prepare(q) => s.native_review_prepare(q).await,
                        Frame::Execute(q) => s.native_review_execute(q).await,
                        Frame::Reconcile(q) => s.native_review_reconcile(q).await,
                        Frame::Release(q) => s.native_review_release(q).await,
                    };
                    let mut count = 0;
                    let delivered = scope
                        .deliver(
                            if reply.is_ok() {
                                RepositoryReadReplyKind::Result
                            } else {
                                RepositoryReadReplyKind::ServiceError
                            },
                            &mut || {
                                count += 1;
                                Ok(())
                            },
                        )
                        .await;
                    assert!(count <= 1);
                    result = Some(delivered.and(reply));
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }
    async fn prepare(&self, f: &Fixture, q: Prepare) -> Value {
        self.request(&f.services, Frame::Prepare(q)).await.unwrap()
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.owner.retire();
    }
}
fn command(f: &Fixture, p: &Value, stage: Stage) -> Execute {
    serde_json::from_value(json!({"workspaceId":f.git.workspace.id,"action":stage,"review":{"operationId":p["reviewOperation"]["operationId"],"root":p["reviewOperation"]["root"]},"commitMessage":"owned commit","prTitle":"owned ready MR","prBody":"body"})).unwrap()
}
fn bound(q: &Execute) -> Bound {
    Bound {
        workspace_id: q.workspace_id.clone(),
        operation_id: q.review.operation_id.clone(),
        root: q.review.root.clone(),
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_commit_and_separate_create_retain_original_effects() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("staged.txt");
    std::fs::write(f.git.path.join("unstaged.txt"), "later").unwrap();
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let value = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        value["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{value}"
    );
    assert!(value["success"].as_bool().unwrap(), "{value}");
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    assert_eq!(
        value["reviewExecution"]["gitReceipts"][0]["commitHash"],
        head.trim()
    );
    assert_eq!(
        f.git
            .git(&f.git.path, &["show", "--pretty=", "--name-only", "HEAD"])
            .trim(),
        "staged.txt"
    );
    let again = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(value, again);
    let mut changed = q.clone();
    changed.commit_message = Some("different".into());
    assert!(s
        .request(&f.services, Frame::Execute(changed))
        .await
        .is_err());
    let history = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(history["reviewExecution"], value["reviewExecution"]);
    let create = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let create = command(&f, &create, Stage::CreatePr);
    let result = s
        .request(&f.services, Frame::Execute(create))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["outcome"]["status"], "created",
        "{result}"
    );
    assert_eq!(result["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    let after = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(before.selection_revision(), after.selection_revision());
    assert_eq!(before.selection(), after.selection());
}
#[intent_test_macros::daemon_test]
async fn native_review_repeated_create_reuses_exact_and_uncertain_never_repairs() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.server.control.lost_post.store(true, Ordering::SeqCst);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let lost = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        lost["reviewExecution"]["outcome"]["status"], "uncertain",
        "{lost}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    f.server.control.lost_post.store(false, Ordering::SeqCst);
    let again = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(again["reviewExecution"], lost["reviewExecution"]);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let newer = s
        .request(
            &f.services,
            Frame::Execute(command(&f, &p, Stage::CreatePr)),
        )
        .await
        .unwrap();
    assert_eq!(
        newer["reviewExecution"]["outcome"]["status"], "reused",
        "{newer}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}
#[intent_test_macros::daemon_test]
async fn native_review_confirmation_changes_release_and_foreign_socket_refuse() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let other = f.socket().await;
    f.stage("initial");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    assert!(other
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .is_err());
    f.stage("changed-index");
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    let result = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(result["success"], false, "{result}");
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    s.request(&f.services, Frame::Release(bound(&q)))
        .await
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}
#[intent_test_macros::daemon_test]
async fn native_review_unsupported_provider_ssh_and_changed_settings_do_not_fallback() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let mut q = f.query(Stage::CreatePr);
    if let Choice::ExplicitTarget { target } = &mut q.review.choice {
        target.provider = RepositoryProvider::Github;
        target.instance_base_url = "https://github.com".into();
    }
    assert!(s.request(&f.services, Frame::Prepare(q)).await.is_err());
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "origin",
            "git@gitlab.test:group/project.git",
        ],
    );
    let mut q = f.query(Stage::Push);
    q.review.push_remote = Some("origin".into());
    assert!(s.request(&f.services, Frame::Prepare(q)).await.is_err());
    f.git.git(&f.git.path, &["remote", "remove", "origin"]);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    f.registry
        .apply(&[("git.autoCommit".into(), json!(true))])
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_primitive_commit_observation_precedes_panic_and_never_infers_head() {
    let f = Fixture::new().await;
    f.stage("first");
    let seen = Mutex::new(Vec::new());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        intent_git::commit::commit_observed(&f.git.path, "observed", |sha| {
            seen.lock().unwrap().push(sha.to_string());
            panic!("after original primitive observation");
        })
    }));
    assert!(panic.is_err());
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        f.git.git(&f.git.path, &["rev-parse", "HEAD"]).trim(),
        seen[0]
    );
    let calls = AtomicUsize::new(0);
    assert!(
        intent_git::commit::commit_observed(&f.git.path, "empty", |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[intent_test_macros::daemon_test]
async fn native_review_publication_uses_confirmed_source_and_actual_ancestry() {
    let f = Fixture::new().await;
    let original = f
        .git
        .git(&f.git.path, &["rev-parse", "HEAD"])
        .trim()
        .to_string();
    f.stage("ahead");
    let next = intent_git::commit::commit(&f.git.path, "ahead")
        .unwrap()
        .hash;
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Ok(Some(original.clone()))),
        Publication::LocalAhead { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(original.clone()), Ok(Some(next.clone()))),
        Publication::Included { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Ok(None)),
        Publication::RemoteBranchMissing { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Err(unavailable())),
        Publication::Unknown {
            remote_source_sha: None,
            ..
        }
    ));
    assert!(matches!(
        publication(
            &f.git.path,
            Some(next.clone()),
            Ok(Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()))
        ),
        Publication::Unknown { .. }
    ));
    f.git
        .git(&f.git.path, &["checkout", "-b", "other", &original]);
    f.stage("other");
    let other = intent_git::commit::commit(&f.git.path, "other")
        .unwrap()
        .hash;
    assert!(matches!(
        publication(&f.git.path, Some(next), Ok(Some(other))),
        Publication::Diverged { .. }
    ));
}
#[intent_test_macros::daemon_test]
async fn native_review_construction_poll_and_unpolled_command_keep_original_owner() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let q = f.query(Stage::CreatePr);
    assert!(f.services.native_review_prepare(q.clone()).await.is_err());
    let scope = s
        .entered(async { s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap() })
        .await;
    let mut denied = false;
    s.entered(scope.scope(Box::pin(async {
        let constructed = f.services.native_review_prepare(q.clone());
        denied = with_caller(Caller::Daemon, constructed).await.is_err();
    })))
    .await;
    assert!(denied);
    scope.retire();
    let p = s.prepare(&f, q).await;
    let command = command(&f, &p, Stage::CreatePr);
    let unpolled = s
        .entered(async {
            s.owner
                .capture_review(&Frame::Execute(command.clone()))
                .unwrap()
        })
        .await;
    unpolled.retire();
    drop(unpolled);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&command)))
        .await
        .unwrap();
    assert_eq!(state["state"], "settled");
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

impl Socket {
    async fn concrete(&self, f: &Fixture) -> Arc<Connection> {
        self.entered(async {
            let frame = self
                .owner
                .capture_review(&Frame::Prepare(f.query(Stage::Commit)))
                .unwrap();
            let mut out = None;
            frame
                .scope(Box::pin(async {
                    out = Some(REVIEW_REQUEST.with(|r| r.connection.clone()));
                }))
                .await;
            frame.retire();
            out.unwrap()
        })
        .await
    }
}
async fn wait_until(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_post_completion_after_cancel_retains_receipt_and_capacity() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    f.server.control.pause_post.store(true, Ordering::SeqCst);
    s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let call = frame.scope(Box::pin(async { let _ = f.services.native_review_execute(q.clone()).await; }));
        tokio::pin!(call);
        tokio::select! {()=&mut call=>panic!("response preceded fixture effect barrier"),()=f.server.control.post_entered.notified()=>{}}
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
        frame.retire();
        assert_eq!(c.review.workers.available_permits(), WORKERS-1);
        assert!(op.progress.lock().unwrap().settled.is_none());
        assert!(f.services.worktree_locks.try_with_lock(&f.git.path, || async {}).await.is_none());
        f.server.control.post_release.notify_one();
        call.await;
    }).await;
    wait_until(|| op.progress.lock().unwrap().settled.is_some()).await;
    wait_until(|| c.review.workers.available_permits() == WORKERS).await;
    let result = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["outcome"]["status"], "created",
        "{result}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.request(&f.services, Frame::Execute(q)).await.unwrap()["reviewExecution"],
        result["reviewExecution"]
    );
}
#[intent_test_macros::daemon_test]
async fn native_review_cancelled_lock_wait_never_observes_or_changes_git() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("owned.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    f.services.worktree_locks.with_lock(&f.git.path, || async {
        s.entered(async {
            let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
            let call=frame.scope(Box::pin(async { let _=f.services.native_review_execute(q.clone()).await; }));
            tokio::pin!(call);
            tokio::select! {()=&mut call=>panic!("held lock operation finished early"),()=wait_until(|| c.review.workers.available_permits()==WORKERS-1)=>{}}
            frame.retire(); call.await;
            wait_until(|| c.review.workers.available_permits()==WORKERS).await;
            assert!(op.progress.lock().unwrap().engine.is_none());
        }).await;
    }).await;
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), before);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(state["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(state["reviewExecution"]["outcome"]["status"], "failed");
}
#[intent_test_macros::daemon_test]
async fn native_review_final_reply_error_keeps_original_effect_and_receiver_loss_refuses() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("effect.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        frame
            .scope(Box::pin(async {
                let reply = f.services.native_review_execute(q.clone()).await.unwrap();
                assert_eq!(
                    reply["reviewExecution"]["gitReceipts"]
                        .as_array()
                        .unwrap()
                        .len(),
                    1
                );
                let mut effects = 0;
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Err(Error::Internal("fixture consumed".into()))
                    })
                    .await
                    .is_err());
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 1);
            }))
            .await;
        frame.retire();
    })
    .await;
    let result = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    assert_eq!(
        result["reviewExecution"]["gitReceipts"][0]["commitHash"],
        head.trim()
    );
    s.receiver.lock().unwrap().take();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
}
#[intent_test_macros::daemon_test]
async fn native_review_private_feed_overflow_and_original_observation_budget() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    for _ in 0..OBSERVATIONS {
        assert_eq!(
            s.request(&f.services, Frame::Reconcile(bound(&q)))
                .await
                .unwrap()["state"],
            "prepared"
        );
    }
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    let c = s.concrete(&f).await;
    for _ in 0..=NOTICES {
        c.review.notice(&q.review.operation_id);
    }
    let mut receiver = s.receiver.lock().unwrap().take().unwrap();
    let event = receiver.next().await.unwrap();
    assert!(event.terminal && event.all_retired);
    assert!(event.operation_ids.is_empty());
    assert!(event.sequence.parse::<u64>().is_ok());
    assert!(receiver.next().await.is_none());
    assert!(s
        .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
        .await
        .is_err());
}

async fn member(f: &Fixture) -> (Socket, intent_core::Principal) {
    use intent_core::{now_iso, HostInvite, Principal, PrincipalId};
    let person = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(8217),
        login: Some("review-member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    f.services.store.upsert_principal(&person).await.unwrap();
    let owner = f.services.store.get_primary_principal().await.unwrap();
    let invite = HostInvite::new(
        "native-review-member".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "review-proof".into(),
        None,
    )
    .unwrap();
    f.services.store.insert_host_invite(&invite).await.unwrap();
    let generation = f
        .services
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    f.services
        .store
        .join_host_by_invite(
            &invite.id,
            &person,
            intent_store::HostJoinCredential::Proof {
                token_hash: "review-member-credential",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    let s = Socket::new(
        &f.services,
        Caller::Wire {
            principal_id: person.id.clone(),
            host_role: HostRole::Member,
        },
        Some(WireCredential::Principal {
            principal_id: person.id.clone(),
            token_hash: "review-member-credential".into(),
        }),
    )
    .await;
    (s, person)
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_member_projection_and_durable_revocation() {
    let f = Fixture::new().await;
    let (s, person) = member(&f).await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert!(p["reviewPreparation"]["source"].get("connection").is_none());
    assert!(p["reviewPreparation"]["target"].get("connection").is_none());
    let q = command(&f, &p, Stage::CreatePr);
    let value = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        value["reviewExecution"]["outcome"]["status"], "created",
        "{value}"
    );
    assert!(value["reviewExecution"]["preparation"]["target"]
        .get("connection")
        .is_none());
    f.services
        .store
        .remove_host_member(&person.id)
        .await
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_selection_change_and_pending_delete_refuse_original() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let snapshot = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    f.services
        .store
        .write_repository_selection(
            &snapshot,
            intent_store::RepositorySelectionChange::Automatic,
        )
        .await
        .result
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    use intent_store::RepositoryLifecycleObserver;
    let pending = f
        .services
        .repository_lifecycle_registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(
            f.git.workspace.id.clone(),
        )])
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .is_err());
    pending.settle_confirmed();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

fn source_input(op: &Operation) -> RepositorySourceInput {
    RepositorySourceInput {
        facts: op.facts.clone(),
        context: RepositoryContextInput {
            scope: op.facts.preparation.scope.clone(),
            revision: op.facts.preparation.context_revision.clone(),
            roots: vec![AdmittedRepositoryRoot {
                root: op.query.review.root.clone(),
                path: op.metadata.root.path().to_path_buf(),
                saved_selection: saved(&op.metadata.selection).unwrap(),
                explicit_target: explicit(&op.query).cloned(),
                targets: vec![target_context(
                    &op.facts.preparation.source.repository,
                    Some(&op.metadata.provider),
                )],
            }],
        },
        resolver: resolver(Some(&op.metadata.provider)).unwrap(),
        environment: GitConfigEnvironment::default(),
        before_lock: None,
    }
}

// Direct original source-entry proof: the callback marker is later than lock
// acquisition/root validation, and earlier than the engine's first observation.
#[intent_test_macros::daemon_test]
async fn native_review_observation_entry_precedes_early_source_error_and_false_drop_is_final() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    s.entered(async {
        let (life,_registration)=c.new_lifetime().unwrap();
        let retired=life.retirement();
        let entered=AtomicBool::new(false); let action=AtomicBool::new(false);
        let mut input=source_input(&op);
        input.facts.source_ref="refs/heads/not-the-original".into();
        let result=with_repository_lifecycle_source_observed(&f.services,original(&c).unwrap(),"early-source-error".into(),vec![Stage::Commit],input,(life,&entered), |_| async {action.store(true,Ordering::Release); Ok(())}).await;
        assert!(result.is_err()); assert!(entered.load(Ordering::Acquire));
        assert!(!action.load(Ordering::Acquire)); assert!(retired.check_current().is_err());
        let (life,_registration)=c.new_lifetime().unwrap(); let retired=life.retirement();
        let entered=AtomicBool::new(false); let action=AtomicBool::new(false);
        let ready=Arc::new(Notify::new()); let mut input=source_input(&op); input.before_lock=Some(ready.clone());
        f.services.worktree_locks.with_lock(&f.git.path,|| async {
            let capacity=job(&c).unwrap();
            {
                let work=with_repository_lifecycle_source_observed(&f.services,original(&c).unwrap(),"drop-unentered".into(),vec![Stage::Commit],input,(life,&entered), |_| async {action.store(true,Ordering::Release); Ok(())});
                tokio::pin!(work);
                tokio::select! {r=&mut work=>panic!("held lock source ended: {r:?}"),()=ready.notified()=>{}}
                retired.end_scope(); assert!(!entered.load(Ordering::Acquire));
                // This future has exactly one poller. No task can start an
                // observation between this false check and its lexical drop.
            }
            assert!(retired.check_current().is_err());
            drop(capacity);
            assert_eq!(c.review.workers.available_permits(),WORKERS);
        }).await;
        tokio::task::yield_now().await;
        assert!(!entered.load(Ordering::Acquire)); assert!(!action.load(Ordering::Acquire));
    }).await;
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), before);
}

#[intent_test_macros::daemon_test]
async fn native_review_owned_primitive_sink_survives_post_observation_worker_panic() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("primitive.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    s.entered(async {
        // Claim the actual original command before running this deliberate
        // callback-panic schedule in the original source/owned worker.
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        let owned = op.clone();
        let connection = c.clone();
        let task = tokio::spawn(with_caller(
            c.caller.caller().clone(),
            with_wire_credential(c.caller.wire_credential().cloned(), async move {
                let _capacity = capacity;
                let _finish = Completion(owned.clone());
                let (life, _registration) = connection.new_lifetime().unwrap();
                let flag = AtomicBool::new(false);
                let result = with_repository_lifecycle_source_observed(
                    &connection.services,
                    original(&connection).unwrap(),
                    owned.id.clone(),
                    vec![Stage::Commit],
                    source_input(&owned),
                    (life, &flag),
                    |admission| async move {
                        owned.progress.lock().unwrap().engine = Some(admission.clone());
                        let checked =
                            engine::revalidate_repository_stage(&admission, Stage::Commit).await?;
                        let stamp = engine::begin_native_repository_stage(checked, |claim| {
                            owned.metadata.with_metadata(claim)
                        })?;
                        let sink = owned.clone();
                        let joined = tokio::task::spawn_blocking(move || {
                            let _stamp = stamp;
                            intent_git::commit::commit_observed(
                                sink.metadata.root.path(),
                                "primitive observed",
                                |sha| {
                                    sink.primitive(GitReceipt::Commit {
                                        commit_hash: sha.into(),
                                    });
                                    panic!(
                                        "deliberate callback failure after retained original SHA"
                                    );
                                },
                            )
                        })
                        .await;
                        assert!(joined.unwrap_err().is_panic());
                        Err::<(), _>(AdmissionError::Unavailable)
                    },
                )
                .await;
                assert!(flag.load(Ordering::Acquire));
                assert!(result.is_err());
            }),
        ));
        task.await.unwrap();
        frame.retire();
    })
    .await;
    assert_eq!(c.review.workers.available_permits(), WORKERS);
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        state["reviewExecution"]["gitReceipts"],
        json!([{"stage":"commit","commitHash":head.trim()}])
    );
    assert_eq!(state["reviewExecution"]["outcome"]["status"], "uncertain");
    let repeated = s.request(&f.services, Frame::Execute(q)).await.unwrap();
    assert_eq!(repeated["reviewExecution"], state["reviewExecution"]);
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_concurrent_registered_roots_coordinate_complete_branch_pair() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let alternate = f.git.dir.path().join("second-repo");
    f.git.git(
        f.git.dir.path(),
        &[
            "clone",
            f.git.path.to_str().unwrap(),
            alternate.to_str().unwrap(),
        ],
    );
    f.git.git(&alternate, &["remote", "remove", "origin"]);
    let id = intent_core::WorkspaceGitRootId::new();
    let row=serde_json::from_value(json!({"id":id,"workspaceId":f.git.workspace.id,"path":alternate,"source":"auto","registeredByAgentIds":[],"createdAt":intent_core::now_iso(),"updatedAt":intent_core::now_iso()})).unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&row)
        .await
        .unwrap();
    let first = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let first = command(&f, &first, Stage::CreatePr);
    let mut query = f.query(Stage::CreatePr);
    query.review.root.kind = RepositoryRootKind::Registered { git_root_id: id };
    let second = s.prepare(&f, query).await;
    let second = command(&f, &second, Stage::CreatePr);
    let c = s.concrete(&f).await;
    f.server.control.pause_post.store(true, Ordering::SeqCst);
    let first_task = {
        let f = f.clone();
        let s = s.clone();
        tokio::spawn(async move { s.request(&f.services, Frame::Execute(first)).await })
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        f.server.control.post_entered.notified(),
    )
    .await
    .unwrap();
    let second_task = {
        let f = f.clone();
        let s = s.clone();
        tokio::spawn(async move { s.request(&f.services, Frame::Execute(second)).await })
    };
    wait_until(|| {
        c.services
            .repository_review_capacity
            .creates
            .lock()
            .unwrap()
            .values()
            .any(|lock| lock.strong_count() >= 2)
    })
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    f.server.control.post_release.notify_one();
    let first = first_task.await.unwrap().unwrap();
    let second = second_task.await.unwrap().unwrap();
    assert_eq!(
        first["reviewExecution"]["outcome"]["status"], "created",
        "{first}"
    );
    assert_eq!(
        second["reviewExecution"]["outcome"]["status"], "reused",
        "{second}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(first["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(second["reviewExecution"]["gitReceipts"], json!([]));
}

#[intent_test_macros::daemon_test]
async fn native_review_ambiguous_and_malformed_provider_results_never_guess_or_retry() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let sha = f.server.control.sha.lock().unwrap().clone();
    let review = super::credential_tests::review(&sha);
    let mut other = review.clone();
    other["iid"] = json!(8);
    *f.server.control.reviews.lock().unwrap() = vec![review, other];
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let refused = s
        .request(
            &f.services,
            Frame::Execute(command(&f, &p, Stage::CreatePr)),
        )
        .await
        .unwrap();
    assert_eq!(
        refused["reviewExecution"]["outcome"]["status"], "failed",
        "{refused}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    f.server.control.reviews.lock().unwrap().clear();
    f.server
        .control
        .malformed_post
        .store(true, Ordering::SeqCst);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let uncertain = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        uncertain["reviewExecution"]["outcome"]["status"], "uncertain",
        "{uncertain}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.request(&f.services, Frame::Reconcile(bound(&q)))
            .await
            .unwrap()["reviewExecution"],
        uncertain["reviewExecution"]
    );
    assert_eq!(
        s.request(&f.services, Frame::Execute(q)).await.unwrap()["reviewExecution"],
        uncertain["reviewExecution"]
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn native_review_prepare_subscription_outlives_helper_and_denies_final_root_change() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    s.entered(async {
        let q = f.query(Stage::Commit);
        let frame = s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap();
        frame
            .scope(Box::pin(async {
                let p = f.services.native_review_prepare(q).await.unwrap();
                let original = REVIEW_REQUEST.with(Clone::clone);
                original.check().unwrap(); // acquisition returned with root subscription retained
                let op = original.operation().unwrap();
                assert!(!op.published.load(Ordering::Acquire));
                f.git.git(
                    &f.git.path,
                    &["checkout", "-b", "changed-before-disclosure"],
                );
                let mut sent = 0;
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        sent += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(sent, 0);
                assert!(!op.published.load(Ordering::Acquire));
                assert_eq!(p["reviewOperation"]["operationId"], op.id);
            }))
            .await;
        frame.retire();
    })
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_virtual_bounds_and_checked_cursor_never_revive_original() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    tokio::time::pause();
    tokio::time::advance(LEASE_TTL + Duration::from_secs(1)).await;
    assert!(op.write_current().is_err());
    tokio::time::resume();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let frame = s
        .entered(async { s.owner.capture_review(&Frame::Execute(q.clone())).unwrap() })
        .await;
    tokio::time::pause();
    tokio::time::advance(FRAME_TTL + Duration::from_secs(1)).await;
    tokio::time::resume();
    frame.retire();
    assert_eq!(
        s.request(&f.services, Frame::Reconcile(bound(&q)))
            .await
            .unwrap()["reviewExecution"]["outcome"]["status"],
        "failed"
    );
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    s.request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    tokio::time::pause();
    tokio::time::advance(RECEIPT_TTL + Duration::from_secs(1)).await;
    assert!(op.disclosure_current().is_err());
    tokio::time::resume();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    c.review.feed.lock().unwrap().sequence = u64::MAX;
    c.review.notice("original-only");
    let mut feed = s.receiver.lock().unwrap().take().unwrap();
    let notice = feed.next().await.unwrap();
    assert_eq!(notice.sequence, u64::MAX.to_string());
    assert!(notice.terminal && notice.all_retired);
    assert!(feed.next().await.is_none());
    assert!(s
        .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn native_review_cancelled_acquisition_keeps_worker_until_original_http_finishes() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.concrete(&f).await;
    *f.server.control.pause.lock().unwrap() = Some("/projects/".into());
    s.entered(async {
        let q=f.query(Stage::CreatePr);let frame=s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap();
        let work=frame.scope(Box::pin(async {assert!(f.services.native_review_prepare(q).await.is_err());}));
        tokio::pin!(work);
        tokio::select! {()=&mut work=>panic!("acquisition completed before fixture response"),()=f.server.control.entered.notified()=>{}}
        frame.retire();work.await;
        assert_eq!(c.review.workers.available_permits(),WORKERS-1);
        assert!(c.review.feed.lock().unwrap().records.is_empty());
        f.server.control.release.notify_one();
    }).await;
    wait_until(|| c.review.workers.available_permits() == WORKERS).await;
    assert_eq!(c.review.records.available_permits(), RECORDS);
    assert_eq!(
        f.services
            .repository_review_capacity
            .records
            .available_permits(),
        GLOBAL_RECORDS
    );
    assert!(c.review.feed.lock().unwrap().records.is_empty());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_saved_automatic_reset_historical_and_explicit_choice_preserve_store_history()
{
    use intent_store::RepositorySelectionChange as Change;
    let mut f = Fixture::new().await;
    let s = f.socket().await;
    let mut saved_query = f.query(Stage::CreatePr);
    saved_query.review.choice = Choice::Saved;
    assert!(s
        .request(&f.services, Frame::Prepare(saved_query.clone()))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(
        before.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap()
            .selection(),
        before.selection()
    );
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "forge",
            "https://gitlab.test/forge/group/project.git",
        ],
    );
    for change in [
        Change::Automatic,
        Change::Reset,
        Change::ExplicitRemote {
            remote_name: "forge".into(),
        },
    ] {
        let snapshot = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        f.services
            .store
            .write_repository_selection(&snapshot, change)
            .await
            .result
            .unwrap();
        let snapshot = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        let p = s.prepare(&f, saved_query.clone()).await;
        assert_eq!(
            p["reviewPreparation"]["target"]["repository"]["projectPath"],
            "group/project"
        );
        let after = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        assert_eq!(snapshot.selection(), after.selection());
        assert_eq!(snapshot.selection_revision(), after.selection_revision());
    }
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "other",
            "https://gitlab.test/forge/group/other.git",
        ],
    );
    let snapshot = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    f.services
        .store
        .write_repository_selection(&snapshot, Change::Automatic)
        .await
        .result
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Prepare(saved_query.clone()))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap()
            .selection(),
        before.selection()
    );
    // A new incarnation with legacy intent remains unresolved, not migrated by preparation.
    f.services
        .store
        .delete_workspace(&f.git.workspace.id)
        .await
        .unwrap();
    f.git.workspace.repository_owner = Some("historical".into());
    f.git.workspace.repository_name = Some("old".into());
    f.services
        .store
        .insert_workspace(&f.git.workspace)
        .await
        .unwrap();
    let fresh = f.socket().await;
    assert!(fresh
        .request(&f.services, Frame::Prepare(saved_query))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert!(matches!(
        before.selection(),
        Some(RepositoryStoredSelection::Saved(
            intent_core::SavedReviewSelection::UnresolvedHistorical { .. }
        ))
    ));
    let _ = fresh.prepare(&f, f.query(Stage::CreatePr)).await;
    let after = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(before.selection(), after.selection());
    assert_eq!(before.selection_revision(), after.selection_revision());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_record_and_worker_limits_release_only_original_owners() {
    let f = Fixture::new().await;
    let mut sockets = Vec::new();
    // This schedule isolates record capacity from the separately tested periodic
    // metadata observation. Strict consuming admission may legitimately refuse
    // on contention; filling the quota must not rely on retrying that refusal.
    // Keep the executor runnable so paused time cannot auto-advance during real
    // Store/provider I/O. All preparations still use the original full path.
    tokio::time::pause();
    let frozen = Instant::now();
    let extra = tokio::select! {
        socket = async {
            for _ in 0..GLOBAL_RECORDS / RECORDS {
                let s = f.socket().await;
                for _ in 0..RECORDS {
                    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
                }
                assert!(s
                    .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
                    .await
                    .is_err());
                sockets.push(s);
            }
            assert_eq!(
                f.services
                    .repository_review_capacity
                    .records
                    .available_permits(),
                0
            );
            let extra = f.socket().await;
            assert!(extra
                .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
                .await
                .is_err());
            extra
        } => socket,
        () = async { loop { tokio::task::yield_now().await; } } => unreachable!(),
    };
    assert_eq!(Instant::now(), frozen);
    tokio::time::resume();
    for s in &sockets {
        s.owner.retire();
    }
    wait_until(|| {
        f.services
            .repository_review_capacity
            .records
            .available_permits()
            == GLOBAL_RECORDS
    })
    .await;
    let mut held = Vec::new();
    let mut connections = Vec::new();
    for _ in 0..GLOBAL_WORKERS / WORKERS {
        let s = f.socket().await;
        let c = s.concrete(&f).await;
        for _ in 0..WORKERS {
            held.push(job(&c).unwrap());
        }
        assert!(job(&c).is_err());
        connections.push((s, c));
    }
    assert_eq!(
        f.services
            .repository_review_capacity
            .workers
            .available_permits(),
        0
    );
    assert!(job(extra.concrete(&f).await.as_ref()).is_err());
    drop(held);
    assert_eq!(
        f.services
            .repository_review_capacity
            .workers
            .available_permits(),
        GLOBAL_WORKERS
    );
    assert!(connections
        .iter()
        .all(|(_, c)| c.review.workers.available_permits() == WORKERS));
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_background_lock_contention_preserves_original_until_real_change() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("monitor.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let baseline = Arc::strong_count(&op.metadata);
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let facts = op.metadata.provider.clone();
    let holder =
        std::thread::spawn(move || facts.hold_native_review_metadata_for_test(entered, released));
    waiting.await.unwrap();
    // Wait for the actual monitor to retain its original metadata for the blocking
    // observation. The real config lock is held; no synthetic admission is used.
    wait_until(|| Arc::strong_count(&op.metadata) > baseline).await;
    let mut consumed = 0;
    assert!(op
        .metadata
        .with_metadata(|| {
            consumed += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(consumed, 0);
    assert!(op.write_current().is_ok());
    release.send(()).unwrap();
    holder.join().unwrap();
    wait_until(|| Arc::strong_count(&op.metadata) == baseline).await;
    assert!(op.write_current().is_ok());
    let result = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let before = op.progress.lock().unwrap().effects.clone();
    let prepared = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let create = command(&f, &prepared, Stage::CreatePr);
    let next = c.review.feed.lock().unwrap().records[&create.review.operation_id].clone();
    f.services
        .gitlab_connect_pat(f.server.host.clone(), "stored-pat".into())
        .await
        .unwrap();
    wait_until(|| next.write_current().is_err()).await;
    assert_eq!(op.progress.lock().unwrap().effects, before);
    assert!(s
        .request(&f.services, Frame::Execute(create))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

fn companion_query(f: &Fixture) -> Prepare {
    let mut value = serde_json::to_value(f.query(Stage::Commit)).unwrap();
    value.as_object_mut().unwrap().remove("options");
    value["review"]["companion"] = json!({"kind":"create-pr"});
    serde_json::from_value(value).unwrap()
}
fn companion_child(f: &Fixture, parent: &Execute) -> Prepare {
    serde_json::from_value(json!({"workspaceId":f.git.workspace.id,"action":"create-pr","review":{"root":parent.review.root,"choice":{"kind":"afterCommit","operationId":parent.review.operation_id,"captureId":uuid::Uuid::new_v4().to_string()}}})).unwrap()
}
async fn companion_parent(f: &Fixture, s: &Socket) -> (Execute, Value, Arc<Operation>) {
    f.stage("companion-staged.txt");
    let prepared = s.prepare(f, companion_query(f)).await;
    let command = command(f, &prepared, Stage::Commit);
    let result = s
        .request(&f.services, Frame::Execute(command.clone()))
        .await
        .unwrap();
    assert_eq!(result["success"], true, "{result}");
    let c = s.concrete(f).await;
    let op = c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
    assert!(op.write_current().is_err());
    assert!(op.companion_normal());
    assert!(
        op.companion
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .delivered
    );
    assert_eq!(c.review.workers.available_permits(), WORKERS);
    (command, result, op)
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_actual_owner_member_separate_create_and_reuse() {
    for member_role in [false, true] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = if member_role {
                member(&f).await.0
            } else {
                f.socket().await
            };
            std::fs::write(f.git.path.join("not-staged.txt"), "untouched").unwrap();
            let original_remote = f.server.control.sha.lock().unwrap().clone();
            if member_role {
                *f.server.control.reviews.lock().unwrap() =
                    vec![super::credential_tests::review(&original_remote)];
            }
            let (parent, result, op) = companion_parent(&f, &s).await;
            let head = f
                .git
                .git(&f.git.path, &["rev-parse", "HEAD"])
                .trim()
                .to_owned();
            assert_ne!(head, original_remote);
            assert_eq!(
                result["reviewExecution"]["gitReceipts"],
                json!([{"stage":"commit","commitHash":head}])
            );
            let before = f.git.git(&f.git.path, &["status", "--porcelain=v1"]);
            assert!(before.contains("?? not-staged.txt"));
            let child_query = companion_child(&f, &parent);
            let child_prepared = s.prepare(&f, child_query.clone()).await;
            let child = command(&f, &child_prepared, Stage::CreatePr);
            assert_ne!(child.review.operation_id, parent.review.operation_id);
            assert_eq!(child_prepared["reviewPreparation"]["localHeadSha"], head);
            assert_eq!(
                child_prepared["reviewPreparation"]["target"]["branch"],
                "trunk"
            );
            let c = s.concrete(&f).await;
            let fresh = c.review.feed.lock().unwrap().records[&child.review.operation_id].clone();
            assert!(!Arc::ptr_eq(&fresh, &op));
            assert!(fresh.companion.is_none());
            assert_ne!(fresh.facts.preparation.scope, op.facts.preparation.scope);
            assert!(s
                .request(&f.services, Frame::Prepare(child_query))
                .await
                .is_err());
            let reply = s
                .request(&f.services, Frame::Execute(child.clone()))
                .await
                .unwrap();
            assert_eq!(reply["success"], true, "{reply}");
            assert_eq!(
                reply["reviewExecution"]["outcome"]["status"],
                if member_role { "reused" } else { "created" }
            );
            assert_eq!(reply["reviewExecution"]["gitReceipts"], json!([]));
            assert_eq!(
                reply["reviewExecution"]["publication"]["state"],
                "local-ahead"
            );
            assert_eq!(
                f.server.control.posts.load(Ordering::SeqCst),
                usize::from(!member_role)
            );
            assert_eq!(*f.server.control.sha.lock().unwrap(), original_remote);
            assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]).trim(), head);
            assert_eq!(
                f.git.git(&f.git.path, &["status", "--porcelain=v1"]),
                before
            );
            let history = s
                .request(&f.services, Frame::Reconcile(bound(&parent)))
                .await
                .unwrap();
            assert_eq!(history["reviewExecution"], result["reviewExecution"]);
            if member_role {
                for side in ["source", "target"] {
                    assert!(history["reviewExecution"]["preparation"][side]
                        .get("connection")
                        .is_none());
                    assert!(child_prepared["reviewPreparation"][side]
                        .get("connection")
                        .is_none());
                }
            }
            assert!(s
                .request(&f.services, Frame::Prepare(companion_child(&f, &child)))
                .await
                .is_err());
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_delivery_failure_and_undisclosed_receipt_never_grant() {
    for delivery in [0, 1, 2] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            f.stage("companion-staged.txt");
            let prepared = s.prepare(&f, companion_query(&f)).await;
            let command = command(&f, &prepared, Stage::Commit);
            s.entered(async {
                let frame = s
                    .owner
                    .capture_review(&Frame::Execute(command.clone()))
                    .unwrap();
                frame
                    .scope(Box::pin(async {
                        let reply = f
                            .services
                            .native_review_execute(command.clone())
                            .await
                            .unwrap();
                        assert_eq!(reply["success"], true, "{reply}");
                        let c = s.concrete(&f).await;
                        assert_eq!(c.review.workers.available_permits(), WORKERS);
                        // Nested/callback capture is forbidden independently of receipt data.
                        let nested = s
                            .owner
                            .capture_review(&Frame::Prepare(companion_child(&f, &command)))
                            .unwrap();
                        nested.retire();
                        if delivery != 0 {
                            let result = frame
                                .deliver(RepositoryReadReplyKind::Result, &mut || {
                                    if delivery == 1 {
                                        Err(Error::Internal("owned failed transfer".into()))
                                    } else {
                                        Ok(())
                                    }
                                })
                                .await;
                            assert_eq!(result.is_ok(), delivery == 2);
                        }
                    }))
                    .await;
                frame.retire();
            })
            .await;
            let history = s
                .request(&f.services, Frame::Reconcile(bound(&command)))
                .await
                .unwrap();
            assert_eq!(
                history["reviewExecution"]["gitReceipts"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            let captured = s
                .request(&f.services, Frame::Prepare(companion_child(&f, &command)))
                .await;
            assert_eq!(captured.is_ok(), delivery == 2);
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_original_reply_serializes_immediate_capture() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        f.stage("companion-staged.txt");
        let prepared = s.prepare(&f, companion_query(&f)).await;
        let command = command(&f, &prepared, Stage::Commit);
        let c = s.concrete(&f).await;
        let child = companion_child(&f, &command);
        let mut captured = None;
        s.entered(async {
            let frame = s
                .owner
                .capture_review(&Frame::Execute(command.clone()))
                .unwrap();
            frame
                .scope(Box::pin(async {
                    let reply = f
                        .services
                        .native_review_execute(command.clone())
                        .await
                        .unwrap();
                    assert_eq!(reply["success"], true, "{reply}");
                    assert_eq!(c.review.workers.available_permits(), WORKERS);
                    assert_eq!(
                        c.services
                            .repository_review_capacity
                            .workers
                            .available_permits(),
                        GLOBAL_WORKERS
                    );
                    assert!(c
                        .services
                        .worktree_locks
                        .try_with_lock(&f.git.path, || async {})
                        .await
                        .is_some());
                    let runtime = tokio::runtime::Handle::current();
                    let original = c.clone();
                    let early = child.clone();
                    let refused = std::thread::spawn(move || {
                        runtime.block_on(with_caller(
                            original.caller.caller().clone(),
                            with_wire_credential(
                                original.caller.wire_credential().cloned(),
                                async { capture_frame(&original, Frame::Prepare(early)) },
                            ),
                        ))
                    })
                    .join()
                    .unwrap();
                    // Before the original transfer, a receipt alone does not qualify.
                    refused
                        .scope(Box::pin(async {
                            assert!(f
                                .services
                                .native_review_prepare(child.clone())
                                .await
                                .is_err());
                        }))
                        .await;
                    refused.retire();
                    let mut thread = None;
                    let mut received = None;
                    frame
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            let runtime = tokio::runtime::Handle::current();
                            let original = c.clone();
                            let next = child.clone();
                            let (started, starting) = std::sync::mpsc::channel();
                            let (done, result) = std::sync::mpsc::channel();
                            thread = Some(std::thread::spawn(move || {
                                runtime.block_on(with_caller(
                                    original.caller.caller().clone(),
                                    with_wire_credential(
                                        original.caller.wire_credential().cloned(),
                                        async {
                                            started.send(()).unwrap();
                                            let scope =
                                                capture_frame(&original, Frame::Prepare(next));
                                            done.send(scope).unwrap();
                                        },
                                    ),
                                ));
                            }));
                            starting.recv().unwrap();
                            assert!(result.try_recv().is_err());
                            received = Some(result);
                            Ok(())
                        })
                        .await
                        .unwrap();
                    thread.take().unwrap().join().unwrap();
                    captured = Some(received.take().unwrap().recv().unwrap());
                }))
                .await;
            // Normal request disposal follows transfer/capture and cannot cancel
            // the legitimately captured child; the original worker is already done.
            frame.retire();
        })
        .await;
        let child_frame = captured.unwrap();
        s.entered(async {
            child_frame
                .scope(Box::pin(async {
                    let response = f
                        .services
                        .native_review_prepare(child.clone())
                        .await
                        .unwrap();
                    child_frame
                        .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                        .await
                        .unwrap();
                    assert_ne!(
                        response["reviewOperation"]["operationId"],
                        prepared["reviewOperation"]["operationId"]
                    );
                }))
                .await;
        })
        .await;
        child_frame.retire();
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_release_capture_expiry_and_tombstone_orders() {
    for order in 0..6 {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            let (parent, receipt, op) = companion_parent(&f, &s).await;
            let child = companion_child(&f, &parent);
            if order == 0 {
                s.request(&f.services, Frame::Release(bound(&parent)))
                    .await
                    .unwrap();
            }
            if order == 1 {
                tokio::time::advance(LEASE_TTL).await;
            }
            let frame = s
                .entered(async {
                    s.owner
                        .capture_review(&Frame::Prepare(child.clone()))
                        .unwrap()
                })
                .await;
            if order == 2 {
                s.request(&f.services, Frame::Release(bound(&parent)))
                    .await
                    .unwrap();
            }
            if order == 3 {
                tokio::time::advance(LEASE_TTL).await;
            }
            let mut prepared = None;
            s.entered(async {
                frame
                    .scope(Box::pin(async {
                        let response = f.services.native_review_prepare(child.clone()).await;
                        if order < 4 {
                            assert!(response.is_err());
                        } else {
                            prepared = Some(response.unwrap());
                            frame
                                .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                                .await
                                .unwrap();
                        }
                    }))
                    .await;
            })
            .await;
            frame.retire();
            if let Some(p) = prepared {
                let command = command(&f, &p, Stage::CreatePr);
                let c = s.concrete(&f).await;
                let fresh =
                    c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
                if order == 4 {
                    s.request(&f.services, Frame::Release(bound(&parent)))
                        .await
                        .unwrap();
                    assert!(fresh.write_current().is_err());
                } else {
                    // Parent intent expiration is independent of a published
                    // child's fresh lease; fixed time advanced only after admission.
                    let remaining =
                        (op.created + LEASE_TTL).saturating_duration_since(Instant::now());
                    tokio::time::advance(remaining).await;
                    // A child published at the same fixed instant also expires here.
                    assert!(s
                        .request(&f.services, Frame::Prepare(companion_child(&f, &parent)))
                        .await
                        .is_err());
                }
            }
            assert!(s.request(&f.services, Frame::Prepare(child)).await.is_err());
            let history = s
                .request(&f.services, Frame::Reconcile(bound(&parent)))
                .await
                .unwrap();
            assert_eq!(history["reviewExecution"], receipt["reviewExecution"]);
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_private_git_continuity_at_capture_publication_and_stage() {
    // Each mutation is applied at a fixed phase to a fresh real operation. No
    // retry or public SHA/account field is used to restore the original witness.
    for phase in 0..3 {
        for mutation in 0..8 {
            with_review_clock(async {
                let f = Fixture::new().await;
                let s = f.socket().await;
                let (parent, receipt, op) = companion_parent(&f, &s).await;
                let child = companion_child(&f, &parent);
                let mutate = || match mutation {
                    0 => {
                        f.git.git(
                            &f.git.path,
                            &["commit", "--allow-empty", "-m", "unrelated HEAD"],
                        );
                    }
                    1 => {
                        f.stage("unrelated-index.txt");
                    }
                    2 => {
                        f.git
                            .git(&f.git.path, &["symbolic-ref", "HEAD", "refs/heads/other"]);
                    }
                    3 => {
                        f.git.git(
                            &f.git.path,
                            &[
                                "config",
                                "remote.forge.pushurl",
                                "https://gitlab.test/forge/group/other.git",
                            ],
                        );
                    }
                    4 => {
                        f.git.git(
                            &f.git.path,
                            &["config", "url.https://unused.invalid/.insteadOf", "unused:"],
                        );
                    }
                    5 => {
                        f.git.git(
                            &f.git.path,
                            &[
                                "config",
                                "remote.forge.url",
                                "https://second.invalid/group/project.git",
                            ],
                        );
                    }
                    6 => {
                        let relocated = f.git.dir.path().join("relocated.git");
                        std::fs::rename(f.git.path.join(".git"), &relocated).unwrap();
                        std::fs::write(
                            f.git.path.join(".git"),
                            format!("gitdir: {}\n", relocated.display()),
                        )
                        .unwrap();
                    }
                    _ => {
                        let common = f.git.dir.path().join("common.git");
                        let local = f.git.path.join(".git");
                        std::fs::rename(&local, &common).unwrap();
                        std::fs::create_dir(&local).unwrap();
                        std::fs::write(local.join("commondir"), format!("{}\n", common.display()))
                            .unwrap();
                        std::fs::copy(common.join("HEAD"), local.join("HEAD")).unwrap();
                        std::fs::copy(common.join("index"), local.join("index")).unwrap();
                    }
                };
                if phase == 0 {
                    mutate();
                    assert!(
                        s.request(&f.services, Frame::Prepare(child)).await.is_err(),
                        "mutation {mutation}"
                    );
                } else if phase == 1 {
                    s.entered(async {
                        let frame = s
                            .owner
                            .capture_review(&Frame::Prepare(child.clone()))
                            .unwrap();
                        frame
                            .scope(Box::pin(async {
                                let _ = f.services.native_review_prepare(child).await.unwrap();
                                mutate();
                                let mut sent = false;
                                assert!(frame
                                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                                        sent = true;
                                        Ok(())
                                    })
                                    .await
                                    .is_err());
                                assert!(!sent);
                            }))
                            .await;
                        frame.retire();
                    })
                    .await;
                } else {
                    let p = s.prepare(&f, child).await;
                    let next = command(&f, &p, Stage::CreatePr);
                    mutate();
                    let answer = s.request(&f.services, Frame::Execute(next.clone())).await;
                    if let Ok(value) = answer {
                        assert_eq!(
                            value["success"], false,
                            "phase {phase}, mutation {mutation}: {value}"
                        );
                    }
                }
                assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
                let history = s
                    .request(&f.services, Frame::Reconcile(bound(&parent)))
                    .await
                    .unwrap();
                assert_eq!(history["reviewExecution"], receipt["reviewExecution"]);
                assert_eq!(op.progress.lock().unwrap().effects.len(), 1);
            })
            .await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_authority_selection_account_and_root_never_rebind() {
    for mutation in 0..9 {
        with_review_clock(async {
            let f = Fixture::new().await;
            let (s, member) = member(&f).await;
            let (parent, receipt, op) = companion_parent(&f, &s).await;
            let child = companion_child(&f, &parent);
            match mutation {
                0 => {
                    let snapshot = f
                        .services
                        .store
                        .repository_selection_snapshot(&f.git.root())
                        .await
                        .unwrap();
                    f.services
                        .store
                        .write_repository_selection(
                            &snapshot,
                            intent_store::RepositorySelectionChange::Automatic,
                        )
                        .await
                        .result
                        .unwrap();
                }
                1 => {
                    f.services
                        .gitlab_connect_pat(f.server.host.clone(), "stored-pat".into())
                        .await
                        .unwrap();
                }
                2 => {
                    with_caller(
                        Caller::Daemon,
                        f.services.settings_update(json!([
                            {"path":"sourceControl.gitlab.oauthClientId","value":"changed"}
                        ])),
                    )
                    .await
                    .unwrap();
                }
                3 => {
                    f.services
                        .store
                        .remove_host_member(&member.id)
                        .await
                        .unwrap();
                }
                4 => {
                    use intent_store::RepositoryLifecycleObserver;
                    let pending = f
                        .services
                        .repository_lifecycle_registry
                        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(
                            f.git.workspace.id.clone(),
                        )])
                        .unwrap();
                    assert!(s
                        .request(&f.services, Frame::Prepare(child.clone()))
                        .await
                        .is_err());
                    pending.settle_confirmed();
                }
                5 => {
                    s.receiver.lock().unwrap().take();
                }
                7 => {
                    let mut workspace = f
                        .services
                        .store
                        .get_workspace(&f.git.workspace.id)
                        .await
                        .unwrap();
                    workspace.repository_path = Some(
                        f.git
                            .dir
                            .path()
                            .join("changed-root")
                            .to_string_lossy()
                            .into(),
                    );
                    f.services.store.update_workspace(&workspace).await.unwrap();
                }
                8 => {
                    f.services
                        .store
                        .delete_workspace(&f.git.workspace.id)
                        .await
                        .unwrap();
                    f.services
                        .store
                        .insert_workspace(&f.git.workspace)
                        .await
                        .unwrap();
                }
                _ => {
                    let foreign = f.socket().await;
                    assert!(foreign
                        .request(&f.services, Frame::Prepare(child.clone()))
                        .await
                        .is_err());
                    let fresh = companion_child(&f, &parent);
                    s.prepare(&f, fresh).await;
                }
            }
            assert!(s.request(&f.services, Frame::Prepare(child)).await.is_err());
            assert_eq!(op.progress.lock().unwrap().effects.len(), 1);
            if mutation == 0 || mutation == 6 {
                let history = s
                    .request(&f.services, Frame::Reconcile(bound(&parent)))
                    .await
                    .unwrap();
                assert_eq!(history["reviewExecution"], receipt["reviewExecution"]);
            }
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_pending_parent_cancel_and_owned_resource_release() {
    for entered in [false, true] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            f.stage("companion-staged.txt");
            let p = s.prepare(&f, companion_query(&f)).await;
            let command = command(&f, &p, Stage::Commit);
            let c = s.concrete(&f).await;
            let op = c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
            let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
            let transaction = if entered { Some(f.services.store.read_pool().begin_with("BEGIN IMMEDIATE").await.unwrap()) } else { None };
            let (held, ready) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let store = f.services.clone(); let path = f.git.path.clone();
            let lock_holder = if entered { None } else { Some(tokio::spawn(async move {
                store.worktree_locks.with_lock(&path, || async { held.send(()).unwrap(); let _ = released.await; }).await;
            })) };
            if !entered { ready.await.unwrap(); }
            s.entered(async {
                let frame = s.owner.capture_review(&Frame::Execute(command.clone())).unwrap();
                let call = frame.scope(Box::pin(async { let _ = f.services.native_review_execute(command.clone()).await; }));
                tokio::pin!(call);
                tokio::select! {
                    () = &mut call => panic!("original work did not remain owned"),
                    () = wait_until(|| if entered { !op.progress.lock().unwrap().effects.is_empty()
                        && f.services.store.write_pool().num_idle() == 0 } else { c.review.workers.available_permits() == WORKERS - 1 }) => {},
                }
                assert!(op.progress.lock().unwrap().settled.is_none());
                assert!(op.companion_witness("missing").is_err());
                frame.retire();
                if entered {
                    assert_eq!(c.review.workers.available_permits(), WORKERS - 1);
                    assert!(f.services.worktree_locks.try_with_lock(&f.git.path, || async {}).await.is_none());
                    transaction.unwrap().rollback().await.unwrap();
                } else { release.send(()).unwrap(); }
                call.await;
                wait_until(|| c.review.workers.available_permits() == WORKERS).await;
            }).await;
            if let Some(lock_holder) = lock_holder { lock_holder.await.unwrap(); }
            assert!(s.request(&f.services, Frame::Prepare(companion_child(&f, &command))).await.is_err());
            let history = s.request(&f.services, Frame::Reconcile(bound(&command))).await.unwrap();
            assert_eq!(history["reviewExecution"]["gitReceipts"].as_array().unwrap().len(), usize::from(entered));
            assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]) == before, !entered);
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        }).await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_child_acquisition_cancel_retains_original_http_and_capacity() {
    for entered in [false, true] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            let (parent, _, _) = companion_parent(&f, &s).await;
            let c = s.concrete(&f).await;
            let child = companion_child(&f, &parent);
            let hold = f.services.worktree_locks.clone();
            let (ready, waiting) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let path = f.git.path.clone();
            let lock = if entered { None } else { Some(tokio::spawn(async move {
                hold.with_lock(&path, || async { ready.send(()).unwrap(); let _ = released.await; }).await;
            })) };
            if entered { *f.server.control.pause.lock().unwrap() = Some("/projects/".into()); }
            else { waiting.await.unwrap(); }
            s.entered(async {
                let frame = s.owner.capture_review(&Frame::Prepare(child.clone())).unwrap();
                let call = frame.scope(Box::pin(async { assert!(f.services.native_review_prepare(child.clone()).await.is_err()); }));
                tokio::pin!(call);
                if entered {
                    tokio::select! { ()=&mut call => panic!("HTTP did not stay held"), ()=f.server.control.entered.notified()=>{} }
                } else {
                    tokio::select! { ()=&mut call=>panic!("lock wait did not stay owned"), ()=wait_until(|| c.review.workers.available_permits()==WORKERS-1)=>{} }
                }
                // Cancellation from the original parent also cancels the pending
                // capture. The already entered provider request remains owned.
                s.request(&f.services, Frame::Release(bound(&parent))).await.unwrap();
                call.await;
                if entered {
                    assert_eq!(c.review.workers.available_permits(), WORKERS-1);
                    f.server.control.release.notify_one();
                } else { release.send(()).unwrap(); }
                wait_until(|| c.review.workers.available_permits()==WORKERS).await;
                frame.retire();
            }).await;
            if let Some(lock) = lock { lock.await.unwrap(); }
            assert!(s.request(&f.services, Frame::Prepare(child)).await.is_err());
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
        }).await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_duplicate_capacity_and_unpolled_capture_do_not_renew() {
    for scenario in 0..6 {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            let (parent, _, _) = companion_parent(&f, &s).await;
            let c = s.concrete(&f).await;
            let child = companion_child(&f, &parent);
            let first = s
                .entered(async {
                    s.owner
                        .capture_review(&Frame::Prepare(child.clone()))
                        .unwrap()
                })
                .await;
            assert!(s
                .request(&f.services, Frame::Prepare(child.clone()))
                .await
                .is_err());
            assert!(s
                .request(&f.services, Frame::Prepare(companion_child(&f, &parent)))
                .await
                .is_err());
            let capacity = if scenario == 0 {
                Some(
                    c.review
                        .workers
                        .clone()
                        .try_acquire_many_owned(u32::try_from(WORKERS).unwrap())
                        .unwrap(),
                )
            } else {
                None
            };
            let record_capacity = match scenario {
                3 => Some(
                    c.review
                        .records
                        .clone()
                        .try_acquire_many_owned(u32::try_from(RECORDS - 1).unwrap())
                        .unwrap(),
                ),
                4 => Some(
                    f.services
                        .repository_review_capacity
                        .records
                        .clone()
                        .try_acquire_many_owned(u32::try_from(GLOBAL_RECORDS - 1).unwrap())
                        .unwrap(),
                ),
                5 => Some(
                    f.services
                        .repository_review_capacity
                        .workers
                        .clone()
                        .try_acquire_many_owned(u32::try_from(GLOBAL_WORKERS).unwrap())
                        .unwrap(),
                ),
                _ => None,
            };
            if scenario == 1 {
                tokio::time::advance(FRAME_TTL).await;
            }
            if scenario == 2 {
                first.retire();
            }
            s.entered(async {
                first
                    .scope(Box::pin(async {
                        assert!(f
                            .services
                            .native_review_prepare(child.clone())
                            .await
                            .is_err());
                    }))
                    .await;
            })
            .await;
            drop(capacity);
            drop(record_capacity);
            first.retire();
            assert!(s
                .request(&f.services, Frame::Prepare(companion_child(&f, &parent)))
                .await
                .is_err());
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_child_admitted_reuse_and_uncertain_post_survive_release() {
    for reused in [true, false] {
        with_review_clock(async {
            let f=Fixture::new().await; let s=member(&f).await.0;
            let (parent, receipt, _) = companion_parent(&f,&s).await;
            let p=s.prepare(&f,companion_child(&f,&parent)).await;
            let child=command(&f,&p,Stage::CreatePr);
            let c=s.concrete(&f).await;
            let op=c.review.feed.lock().unwrap().records[&child.review.operation_id].clone();
            if reused {
                let sha=f.server.control.sha.lock().unwrap().clone();
                *f.server.control.reviews.lock().unwrap()=vec![super::credential_tests::review(&sha)];
                *f.server.control.pause.lock().unwrap()=Some("/merge_requests".into());
            } else {
                f.server.control.pause_post.store(true,Ordering::SeqCst);
                f.server.control.lost_post.store(true,Ordering::SeqCst);
            }
            let mut result=None;
            s.entered(async {
                let frame=s.owner.capture_review(&Frame::Execute(child.clone())).unwrap();
                let call=frame.scope(Box::pin(async {
                    let response=f.services.native_review_execute(child.clone()).await.unwrap();
                    assert_eq!(response["reviewExecution"]["outcome"]["status"],if reused {"reused"} else {"uncertain"},"{response}");
                    let mut sent=0;
                    assert!(frame.deliver(RepositoryReadReplyKind::Result,&mut || {sent+=1;Err(Error::Internal("owned reply failure".into()))}).await.is_err());
                    assert_eq!(sent,1);
                    result=Some(response);
                }));
                tokio::pin!(call);
                if reused {tokio::select! {()=&mut call=>panic!("expected admitted held GET"),()=f.server.control.entered.notified()=>{}}}
                else {tokio::select! {()=&mut call=>panic!("expected admitted held POST"),()=f.server.control.post_entered.notified()=>{}}}
                assert!(op.progress.lock().unwrap().settled.is_none());
                s.request(&f.services,Frame::Release(bound(&parent))).await.unwrap();
                assert!(op.write_current().is_err());
                assert_eq!(c.review.workers.available_permits(),WORKERS-1);
                assert!(f.services.worktree_locks.try_with_lock(&f.git.path,||async{}).await.is_none());
                if reused { f.server.control.release.notify_one(); } else { f.server.control.post_release.notify_one(); }
                call.await; frame.retire();
            }).await;
            wait_until(|| c.review.workers.available_permits()==WORKERS).await;
            let history=s.request(&f.services,Frame::Reconcile(bound(&child))).await.unwrap();
            assert_eq!(history["reviewExecution"],result.unwrap()["reviewExecution"]);
            assert_eq!(history["reviewExecution"]["gitReceipts"],json!([]));
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst),usize::from(!reused));
            let requests=f.server.control.requests.lock().unwrap().clone();
            s.request(&f.services,Frame::Execute(child)).await.unwrap();
            assert_eq!(*f.server.control.requests.lock().unwrap(),requests);
            let parent_history=s.request(&f.services,Frame::Reconcile(bound(&parent))).await.unwrap();
            assert_eq!(parent_history["reviewExecution"],receipt["reviewExecution"]);
        }).await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_actual_primitive_without_valid_success_witness_refuses() {
    for panic_after_primitive in [true, false] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            f.stage("companion-staged.txt");
            let p = s.prepare(&f, companion_query(&f)).await;
            let command = command(&f, &p, Stage::Commit);
            let c = s.concrete(&f).await;
            let op = c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
            s.entered(async {
                let frame = s
                    .owner
                    .capture_review(&Frame::Execute(command.clone()))
                    .unwrap();
                let capacity = job(&c).unwrap();
                op.progress.lock().unwrap().started = true;
                let owned = op.clone();
                let connection = c.clone();
                let task = tokio::spawn(with_caller(
                    c.caller.caller().clone(),
                    with_wire_credential(c.caller.wire_credential().cloned(), async move {
                        let mut finish = CompanionCompletion {
                            operation: owned.clone(),
                            capacity: Some(capacity),
                            normal: false,
                        };
                        let (life, _registration) = connection.new_lifetime().unwrap();
                        let flag = AtomicBool::new(false);
                        let result = with_repository_lifecycle_source_observed(
                            &connection.services,
                            original(&connection).unwrap(),
                            owned.id.clone(),
                            vec![Stage::Commit],
                            source_input(&owned),
                            (life, &flag),
                            |admission| async move {
                                owned.progress.lock().unwrap().engine = Some(admission.clone());
                                let checked =
                                    engine::revalidate_repository_stage(&admission, Stage::Commit)
                                        .await?;
                                let stamp =
                                    engine::begin_native_repository_stage(checked, |claim| {
                                        owned.metadata.with_metadata(claim)
                                    })?;
                                let sink = owned.clone();
                                let joined = tokio::task::spawn_blocking(move || {
                                    let outcome = intent_git::commit::commit_observed(
                                        sink.metadata.root.path(),
                                        "actual primitive",
                                        |sha| {
                                            sink.primitive(GitReceipt::Commit {
                                                commit_hash: sha.into(),
                                            });
                                            assert!(
                                                !panic_after_primitive,
                                                "deliberate original observer panic"
                                            );
                                        },
                                    );
                                    if let Ok(outcome) = outcome {
                                        // Test-owned filesystem failure AFTER the real helper
                                        // succeeded: the private post-read cannot certify it.
                                        let config = sink.metadata.root.path().join(".git/config");
                                        let original = std::fs::read(&config).unwrap();
                                        std::fs::write(&config, "[malformed").unwrap();
                                        let after = fingerprint(sink.metadata.root.path()).ok();
                                        assert!(after.is_none());
                                        assert!(commit_witness(
                                            &sink,
                                            &outcome.hash,
                                            after.as_deref()
                                        )
                                        .is_err());
                                        std::fs::write(&config, original).unwrap();
                                        let execution = engine::classify_repository_completion(
                                            stamp,
                                            RepositoryCompletion::Committed {
                                                hash: outcome.hash,
                                                staging_after: after,
                                            },
                                        )
                                        .unwrap();
                                        sink.retain(execution);
                                    }
                                })
                                .await;
                                assert_eq!(joined.is_err(), panic_after_primitive);
                                if panic_after_primitive {
                                    Err(AdmissionError::Unavailable)
                                } else {
                                    Ok(())
                                }
                            },
                        )
                        .await;
                        assert!(flag.load(Ordering::Acquire));
                        finish.normal = result.is_ok();
                    }),
                ));
                task.await.unwrap();
                // A successful history transfer cannot manufacture the missing
                // helper/post-state witness or repair an uncertain primitive.
                frame
                    .scope(Box::pin(async {
                        frame
                            .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                            .await
                            .unwrap();
                    }))
                    .await;
                frame.retire();
            })
            .await;
            let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
            let history = s
                .request(&f.services, Frame::Reconcile(bound(&command)))
                .await
                .unwrap();
            assert_eq!(
                history["reviewExecution"]["gitReceipts"],
                json!([{"stage":"commit","commitHash":head.trim()}])
            );
            assert_eq!(
                history["reviewExecution"]["outcome"]["status"],
                if panic_after_primitive {
                    "uncertain"
                } else {
                    "not-attempted"
                }
            );
            assert!(s
                .request(&f.services, Frame::Prepare(companion_child(&f, &command)))
                .await
                .is_err());
            assert_eq!(c.review.workers.available_permits(), WORKERS);
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_unmarked_commit_and_direct_service_are_not_predecessors() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        f.stage("ordinary.txt");
        let p = s.prepare(&f, f.query(Stage::Commit)).await;
        let command = command(&f, &p, Stage::Commit);
        let reply = s
            .request(&f.services, Frame::Execute(command.clone()))
            .await
            .unwrap();
        assert_eq!(reply["success"], true);
        let q = companion_child(&f, &command);
        assert!(s
            .request(&f.services, Frame::Prepare(q.clone()))
            .await
            .is_err());
        assert!(f.services.native_review_prepare(q.clone()).await.is_err());
        assert!(
            with_caller(Caller::Daemon, f.services.native_review_prepare(q))
                .await
                .is_err()
        );
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    })
    .await;
}

async fn companion_advance_and_join_monitor(op: &Operation, duration: Duration, observers: usize) {
    let baseline = Arc::strong_count(&op.metadata);
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let facts = op.metadata.provider.clone();
    let holder =
        std::thread::spawn(move || facts.hold_native_review_metadata_for_test(entered, released));
    waiting.await.unwrap();
    tokio::time::advance(duration).await;
    wait_until(|| Arc::strong_count(&op.metadata) >= baseline + observers).await;
    release.send(()).unwrap();
    holder.join().unwrap();
    wait_until(|| Arc::strong_count(&op.metadata) == baseline).await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_published_lease_is_fresh_without_renewing_parent_intent() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        let (parent, _, op) = companion_parent(&f, &s).await;
        let witness = op
            .companion
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .witness
            .clone()
            .unwrap();
        assert_eq!(
            witness.observed.config_fingerprint,
            op.observed.config_fingerprint
        );
        assert_ne!(
            witness.observed.change_inputs.fingerprint,
            op.observed.change_inputs.fingerprint
        );
        companion_advance_and_join_monitor(&op, Duration::from_secs(60), 1).await;
        let p = s.prepare(&f, companion_child(&f, &parent)).await;
        let child = command(&f, &p, Stage::CreatePr);
        let c = s.concrete(&f).await;
        let fresh = c.review.feed.lock().unwrap().records[&child.review.operation_id].clone();
        wait_until(|| c.review.workers.available_permits() == WORKERS).await;
        assert_eq!(fresh.lease_start(), Instant::now());
        companion_advance_and_join_monitor(&op, Duration::from_secs(240), 2).await;
        assert!(Instant::now() >= op.created + LEASE_TTL);
        assert!(fresh.write_current().is_ok());
        assert!(s
            .request(&f.services, Frame::Prepare(companion_child(&f, &parent)))
            .await
            .is_err());
        let reply = s.request(&f.services, Frame::Execute(child)).await.unwrap();
        assert_eq!(reply["success"], true, "{reply}");
        assert_eq!(reply["reviewExecution"]["gitReceipts"], json!([]));
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_concurrent_original_frames_claim_only_one_child() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        let (parent, _, _) = companion_parent(&f, &s).await;
        let c = s.concrete(&f).await;
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let original = c.clone();
            let gate = barrier.clone();
            let q = companion_child(&f, &parent);
            let runtime = tokio::runtime::Handle::current();
            threads.push(std::thread::spawn(move || {
                runtime.block_on(with_caller(
                    original.caller.caller().clone(),
                    with_wire_credential(original.caller.wire_credential().cloned(), async {
                        gate.wait();
                        let scope = capture_frame(&original, Frame::Prepare(q.clone()));
                        (scope, q)
                    }),
                ))
            }));
        }
        let mut successes = 0;
        for thread in threads {
            let (scope, q) = thread.join().unwrap();
            s.entered(async {
                scope
                    .scope(Box::pin(async {
                        if f.services.native_review_prepare(q).await.is_ok() {
                            scope
                                .deliver(RepositoryReadReplyKind::Result, &mut || Ok(()))
                                .await
                                .unwrap();
                            successes += 1;
                        }
                    }))
                    .await;
            })
            .await;
            scope.retire();
        }
        assert_eq!(successes, 1);
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        assert_eq!(c.review.records.available_permits(), RECORDS - 2);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_pending_capture_expires_at_parent_deadline_and_joins() {
    with_review_clock(async {
        let f=Fixture::new().await;let s=f.socket().await;
        let (parent,receipt,op)=companion_parent(&f,&s).await;
        eprintln!("expiry control: parent complete");
        companion_advance_and_join_monitor(&op,Duration::from_secs(299),1).await;
        eprintln!("expiry control: original monitor joined");
        let c=s.concrete(&f).await;let q=companion_child(&f,&parent);
        *f.server.control.pause.lock().unwrap()=Some("/projects/".into());
        s.entered(async {
            let frame=s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap();
            let call=frame.scope(Box::pin(async { assert!(f.services.native_review_prepare(q.clone()).await.is_err()); }));
            tokio::pin!(call);
            tokio::select! { ()=&mut call=>panic!("expected original held read"), ()=f.server.control.entered.notified()=>{} }
            eprintln!("expiry control: original provider read held");
            // Cross the absolute timer's millisecond wake granularity;
            // synchronous capture/publication still rejects at the exact bound.
            tokio::time::advance(Duration::from_millis(1001)).await;
            eprintln!("expiry control: parent deadline elapsed");
            call.await;
            eprintln!("expiry control: caller refused");
            assert_eq!(c.review.workers.available_permits(),WORKERS-1);
            assert_eq!(c.review.records.available_permits(),RECORDS-2);
            f.server.control.release.notify_one();
            wait_until(|| c.review.workers.available_permits()==WORKERS).await;
            assert_eq!(c.review.records.available_permits(),RECORDS-1);
            eprintln!("expiry control: actual worker joined");
            frame.retire();
        }).await;
        assert!(s.request(&f.services,Frame::Prepare(q)).await.is_err());
        // The original operation remains a separate known historical effect.
        assert_eq!(op.state(true).unwrap()["reviewExecution"],receipt["reviewExecution"]);
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
    }).await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_intended_target_is_validated_before_plain_commit() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        f.stage("companion-staged.txt");
        let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
        for target in ["main", "invalid..ref", "missing"] {
            let mut query = companion_query(&f);
            query.review.target_branch = Some(target.into());
            assert!(s.request(&f.services, Frame::Prepare(query)).await.is_err());
        }
        *f.server.control.project.lock().unwrap() =
            Some((200, json!({"id":0,"path_with_namespace":"group/project"})));
        assert!(s
            .request(&f.services, Frame::Prepare(companion_query(&f)))
            .await
            .is_err());
        assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_original_pretransfer_refusal_permanently_closes_eligibility() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        f.stage("companion-staged.txt");
        let prepared = s.prepare(&f, companion_query(&f)).await;
        let command = command(&f, &prepared, Stage::Commit);
        let c = s.concrete(&f).await;
        let op = c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
        s.entered(async {
            let frame = s
                .owner
                .capture_review(&Frame::Execute(command.clone()))
                .unwrap();
            frame
                .scope(Box::pin(async {
                    let reply = f
                        .services
                        .native_review_execute(command.clone())
                        .await
                        .unwrap();
                    assert_eq!(reply["success"], true, "{reply}");
                    let (entered, waiting) = tokio::sync::oneshot::channel();
                    let (release, released) = std::sync::mpsc::channel();
                    let facts = op.metadata.provider.clone();
                    let holder = std::thread::spawn(move || {
                        facts.hold_native_review_metadata_for_test(entered, released);
                    });
                    waiting.await.unwrap();
                    let mut packets = 0;
                    assert!(frame
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            packets += 1;
                            Ok(())
                        })
                        .await
                        .is_err());
                    assert_eq!(packets, 0);
                    release.send(()).unwrap();
                    holder.join().unwrap();
                    // Removing real pre-transfer contention must not make this
                    // failed original delivery eligible on a second call.
                    assert!(frame
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            packets += 1;
                            Ok(())
                        })
                        .await
                        .is_err());
                    assert_eq!(packets, 0);
                }))
                .await;
            frame.retire();
        })
        .await;
        assert!(s
            .request(&f.services, Frame::Prepare(companion_child(&f, &command)))
            .await
            .is_err());
        let history = s
            .request(&f.services, Frame::Reconcile(bound(&command)))
            .await
            .unwrap();
        assert_eq!(
            history["reviewExecution"]["gitReceipts"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_failed_attribution_keeps_commit_without_child_grant() {
    with_review_clock(async {
        let f = Fixture::new().await;
        let s = f.socket().await;
        f.stage("companion-staged.txt");
        f.services.store.upsert_tracked_change(&intent_store::NewTrackedChange {
            workspace_id: f.git.workspace.id.clone(), path: "companion-staged.txt".into(),
            stage: "staged".into(), status: "modified".into(), agent_id: None,
            session_id: None, turn: None, commit_hash: None, old_blob_sha: None,
            new_blob_sha: None, additions: 1, deletions: 0,
        }).await.unwrap();
        sqlx::query("CREATE TRIGGER companion_attribution_refusal BEFORE UPDATE OF stage ON tracked_changes BEGIN SELECT RAISE(ABORT, 'owned attribution refusal'); END")
            .execute(f.services.store.write_pool()).await.unwrap();
        let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
        let prepared = s.prepare(&f, companion_query(&f)).await;
        let command = command(&f, &prepared, Stage::Commit);
        let receipt = s.request(&f.services, Frame::Execute(command.clone())).await.unwrap();
        let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
        assert_ne!(before, head);
        assert_eq!(receipt["reviewExecution"]["gitReceipts"], json!([{"stage":"commit","commitHash":head.trim()}]));
        let rows = f.services.store.list_tracked_changes(&f.git.workspace.id).await.unwrap();
        assert_eq!(rows.len(), 1); assert_eq!(rows[0].stage, "staged");
        assert!(s.request(&f.services, Frame::Prepare(companion_child(&f, &command))).await.is_err());
        let history = s.request(&f.services, Frame::Reconcile(bound(&command))).await.unwrap();
        assert_eq!(history["reviewExecution"], receipt["reviewExecution"]);
        assert_eq!(s.concrete(&f).await.review.workers.available_permits(), WORKERS);
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    }).await;
}

#[intent_test_macros::daemon_test]
async fn native_review_companion_overlapping_original_delivery_attempts_never_reopen() {
    for first_polled in [false, true] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            f.stage("companion-staged.txt");
            let p = s.prepare(&f, companion_query(&f)).await;
            let command = command(&f, &p, Stage::Commit);
            let c = s.concrete(&f).await;
            let op = c.review.feed.lock().unwrap().records[&command.review.operation_id].clone();
            s.entered(async {
                let frame = s
                    .owner
                    .capture_review(&Frame::Execute(command.clone()))
                    .unwrap();
                frame
                    .scope(Box::pin(async {
                        let result = f
                            .services
                            .native_review_execute(command.clone())
                            .await
                            .unwrap();
                        assert_eq!(result["success"], true);
                        let (entered, waiting) = tokio::sync::oneshot::channel();
                        let (release, released) = std::sync::mpsc::channel();
                        let facts = op.metadata.provider.clone();
                        let holder = std::thread::spawn(move || {
                            facts.hold_native_review_metadata_for_test(entered, released);
                        });
                        waiting.await.unwrap();
                        let packets = std::sync::atomic::AtomicUsize::new(0);
                        let mut one = || {
                            packets.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        };
                        let mut two = || {
                            packets.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        };
                        // Two original delivery futures coexist. The sole attempt
                        // must be reserved before either future can await guards.
                        let first = frame.deliver(RepositoryReadReplyKind::Result, &mut one);
                        let second = frame.deliver(RepositoryReadReplyKind::Result, &mut two);
                        if first_polled {
                            assert!(first.await.is_err());
                            release.send(()).unwrap();
                            holder.join().unwrap();
                            assert!(second.await.is_err());
                        } else {
                            // The second future cannot overtake the already
                            // reserved original attempt when metadata recovers.
                            release.send(()).unwrap();
                            holder.join().unwrap();
                            assert!(second.await.is_err());
                            assert!(first.await.is_err());
                        }
                        assert_eq!(packets.load(Ordering::SeqCst), 0);
                    }))
                    .await;
                frame.retire();
            })
            .await;
            assert!(s
                .request(&f.services, Frame::Prepare(companion_child(&f, &command)))
                .await
                .is_err());
            let history = s
                .request(&f.services, Frame::Reconcile(bound(&command)))
                .await
                .unwrap();
            assert_eq!(
                history["reviewExecution"]["gitReceipts"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

// Sidebar preview contract: preparation describes the existing index when the
// original commit does not request staging. The original owner still admits it.
#[intent_test_macros::daemon_test]
async fn native_review_sidebar_staged_preview_original_owner() {
    for form in ["marked", "omitted", "null", "empty"] {
        with_review_clock(async {
            let f = Fixture::new().await;
            let s = f.socket().await;
            std::fs::write(f.git.path.join("staged.txt"), "index line\n").unwrap();
            f.git.git(&f.git.path, &["add", "staged.txt"]);
            std::fs::write(f.git.path.join("unstaged.txt"), "workdir only\n").unwrap();
            if form != "marked" {
                std::fs::write(f.git.path.join("staged.txt"), "index line\nworkdir tail\n").unwrap();
            }
            let workdir = std::fs::read(f.git.path.join("staged.txt")).unwrap();
            let index_tree = f.git.git(&f.git.path, &["write-tree"]);
            let remote = f.server.control.sha.lock().unwrap().clone();
            let mut wire = serde_json::to_value(f.query(Stage::Commit)).unwrap();
            wire.as_object_mut().unwrap().remove("files");
            wire.as_object_mut().unwrap().remove("options");
            match form {
                "marked" => wire["review"]["companion"] = json!({"kind":"create-pr"}),
                "null" => wire["files"] = Value::Null,
                "empty" => wire["files"] = json!([]),
                _ => {}
            }
            let prepared = s.prepare(&f, serde_json::from_value(wire).unwrap()).await;
            eprintln!("sidebar A1 {form} preparation={prepared} indexTree={} posts={}", index_tree.trim(), f.server.control.posts.load(Ordering::SeqCst));
            assert_eq!(prepared["files"], json!([{"path":"staged.txt","staged":true,"additions":1,"deletions":0}]));
            assert_eq!(prepared["filesCount"], 1);
            assert_eq!(prepared["additions"], 1);
            assert_eq!(prepared["deletions"], 0);
            let q = command(&f, &prepared, Stage::Commit);
            let reply = s.request(&f.services, Frame::Execute(q.clone())).await;
            let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
            let tree = f.git.git(&f.git.path, &["rev-parse", "HEAD^{tree}"]);
            let status = f.git.git(&f.git.path, &["status", "--porcelain=v1"]);
            let c = s.concrete(&f).await;
            let workers = c.review.workers.available_permits();
            eprintln!("sidebar A1 {form} execution={reply:?} head={} tree={} status={status:?} workers={workers} posts={}", head.trim(), tree.trim(), f.server.control.posts.load(Ordering::SeqCst));
            let reply = reply.unwrap();
            assert_eq!(reply["success"], true);
            assert_eq!(reply["reviewExecution"]["gitReceipts"], json!([{"stage":"commit","commitHash":head.trim()}]));
            assert_eq!(tree, index_tree);
            assert_eq!(f.git.git(&f.git.path, &["write-tree"]), index_tree);
            assert_eq!(std::fs::read(f.git.path.join("staged.txt")).unwrap(), workdir);
            assert_eq!(std::fs::read_to_string(f.git.path.join("unstaged.txt")).unwrap(), "workdir only\n");
            assert!(status.contains("?? unstaged.txt"));
            assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
            assert_eq!(*f.server.control.sha.lock().unwrap(), remote);
            assert_eq!(workers, WORKERS);
            assert_eq!(f.services.repository_review_capacity.workers.available_permits(), GLOBAL_WORKERS);
            let history = s.request(&f.services, Frame::Reconcile(bound(&q))).await.unwrap();
            assert_eq!(history["reviewExecution"], reply["reviewExecution"]);
        }).await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_sidebar_preview_partial_rename_binary_and_empty_index() {
    let f = Fixture::new().await;
    for name in ["old.txt", "deleted.txt", "partial.txt"] {
        std::fs::write(f.git.path.join(name), "baseline\n").unwrap();
    }
    f.git.git(
        &f.git.path,
        &["add", "old.txt", "deleted.txt", "partial.txt"],
    );
    f.git
        .git(&f.git.path, &["commit", "-m", "preview baseline"]);
    f.git.git(&f.git.path, &["mv", "old.txt", "renamed.txt"]);
    f.git.git(&f.git.path, &["rm", "deleted.txt"]);
    std::fs::write(f.git.path.join("partial.txt"), "index one\nindex two\n").unwrap();
    std::fs::write(f.git.path.join("added.txt"), "added\n").unwrap();
    std::fs::write(f.git.path.join("binary.bin"), [0, 1, 0, 2]).unwrap();
    f.git.git(
        &f.git.path,
        &["add", "partial.txt", "added.txt", "binary.bin"],
    );
    std::fs::write(
        f.git.path.join("partial.txt"),
        "index one\nindex two\nworkdir three\n",
    )
    .unwrap();
    std::fs::write(f.git.path.join("untracked.txt"), "not staged\n").unwrap();
    let before = f.git.git(&f.git.path, &["status", "--porcelain=v1"]);
    let tree = f.git.git(&f.git.path, &["write-tree"]);
    let preview =
        crate::accept_changes::build_native_prepare_value(&f.git.path, &f.query(Stage::Commit))
            .unwrap();
    eprintln!(
        "sidebar A2 preview={preview} status={before:?} indexTree={}",
        tree.trim()
    );
    assert_eq!(
        preview["files"],
        json!([
            {"path":"added.txt","staged":true,"additions":1,"deletions":0},
            {"path":"binary.bin","staged":true,"additions":0,"deletions":0},
            {"path":"deleted.txt","staged":true,"additions":0,"deletions":1},
            {"path":"old.txt","staged":true,"additions":0,"deletions":1},
            {"path":"partial.txt","staged":true,"additions":2,"deletions":1},
            {"path":"renamed.txt","staged":true,"additions":1,"deletions":0}
        ])
    );
    assert_eq!(preview["filesCount"], 6);
    assert_eq!(preview["additions"], 4);
    assert_eq!(preview["deletions"], 3);
    assert_eq!(
        f.git.git(&f.git.path, &["status", "--porcelain=v1"]),
        before
    );
    assert_eq!(f.git.git(&f.git.path, &["write-tree"]), tree);
    f.git.git(&f.git.path, &["reset", "--mixed", "HEAD"]);
    let before = f.git.git(&f.git.path, &["status", "--porcelain=v1"]);
    let empty =
        crate::accept_changes::build_native_prepare_value(&f.git.path, &f.query(Stage::Commit))
            .unwrap();
    eprintln!("sidebar A2 zero-index={empty} status={before:?}");
    assert_eq!(empty["files"], json!([]));
    assert_eq!(empty["filesCount"], 0);
    assert_eq!(empty["additions"], 0);
    assert_eq!(empty["deletions"], 0);
    assert_eq!(
        f.git.git(&f.git.path, &["status", "--porcelain=v1"]),
        before
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_sidebar_preview_explicit_stage_all_and_other_actions() {
    let f = Fixture::new().await;
    std::fs::write(f.git.path.join("partial.txt"), "index\n").unwrap();
    f.git.git(&f.git.path, &["add", "partial.txt"]);
    std::fs::write(f.git.path.join("partial.txt"), "index\nworkdir\n").unwrap();
    std::fs::write(f.git.path.join("extra.txt"), "extra\n").unwrap();
    let before = f.git.git(&f.git.path, &["status", "--porcelain=v1"]);
    let tree = f.git.git(&f.git.path, &["write-tree"]);
    for mode in [
        "explicit",
        "stage-all",
        "create-pr",
        "push",
        "after-commit",
        "commit-push-create",
    ] {
        let mut q = f.query(Stage::Commit);
        match mode {
            "explicit" => q.files = Some(vec!["partial.txt".into()]),
            "stage-all" => q.options.stage_unstaged = true,
            "create-pr" => q.action = Stage::CreatePr,
            "push" => q.action = Stage::Push,
            "after-commit" => {
                q.action = Stage::CreatePr;
                q.review.choice = Choice::AfterCommit {
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    capture_id: uuid::Uuid::new_v4().to_string(),
                };
                q.review.target_branch = None;
            }
            "commit-push-create" => {
                q.options.push_after_commit = true;
                q.options.create_pr_after_push = true;
            }
            _ => unreachable!(),
        }
        let preview = crate::accept_changes::build_native_prepare_value(&f.git.path, &q).unwrap();
        eprintln!("sidebar A3 {mode} preview={preview}");
        let rows = preview["files"].as_array().unwrap();
        let staged_only = mode == "commit-push-create";
        assert_eq!(
            rows.len(),
            if staged_only {
                1
            } else if mode == "explicit" {
                2
            } else {
                3
            }
        );
        assert!(rows
            .iter()
            .any(|r| r["path"] == "partial.txt" && r["staged"] == true));
        assert_eq!(
            rows.iter()
                .any(|r| r["path"] == "partial.txt" && r["staged"] == false),
            !staged_only
        );
        assert_eq!(
            rows.iter().any(|r| r["path"] == "extra.txt"),
            !staged_only && mode != "explicit"
        );
        assert_eq!(preview["filesCount"], rows.len());
    }
    for filter in [None, Some(vec!["partial.txt".into()])] {
        let legacy = crate::accept_changes::build_prepare_value(
            &f.git.path,
            &f.git.workspace,
            "commit",
            filter.as_deref(),
        )
        .unwrap();
        eprintln!("sidebar A3 legacy filter={filter:?} preview={legacy}");
        let rows = legacy["files"].as_array().unwrap();
        assert!(rows
            .iter()
            .any(|r| r["path"] == "partial.txt" && r["staged"] == true));
        assert!(rows
            .iter()
            .any(|r| r["path"] == "partial.txt" && r["staged"] == false));
        assert_eq!(
            rows.iter().any(|r| r["path"] == "extra.txt"),
            filter.is_none()
        );
    }
    assert_eq!(
        f.git.git(&f.git.path, &["status", "--porcelain=v1"]),
        before
    );
    assert_eq!(f.git.git(&f.git.path, &["write-tree"]), tree);
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

// companion-observation: begin qualification
fn diagnostic_checkpoint(observer: &companion_observation::Collector, label: &str, value: Value) {
    let mut record = json!({"checkpoint":label,"frames":observer.lines()});
    record["actual"] = value;
    println!("COMPANION6328 {record}");
}

fn diagnostic_reply(value: &Result<Value>) -> Value {
    match value {
        Ok(value) => json!({"result":value}),
        Err(error) => json!({"error":error.to_string()}),
    }
}
async fn diagnostic_request(
    f: &Fixture,
    s: &Socket,
    query: Frame,
    observer: &companion_observation::Collector,
) -> Result<Value> {
    s.entered(async {
        let frame=s.owner.capture_review(&query).unwrap();
        let mut result=None;
        frame.scope(Box::pin(async {
            let reply=match query {
                Frame::Prepare(q)=>f.services.native_review_prepare(q).await,
                Frame::Execute(q)=>f.services.native_review_execute(q).await,
                Frame::Reconcile(q)=>f.services.native_review_reconcile(q).await,
                Frame::Release(q)=>f.services.native_review_release(q).await,
            };
            let mut transfers=0;
            let delivered=frame.deliver(if reply.is_ok(){RepositoryReadReplyKind::Result}else{RepositoryReadReplyKind::ServiceError},&mut||{transfers+=1;Ok(())}).await;
            diagnostic_checkpoint(observer,"original-response",json!({"body":diagnostic_reply(&reply),"deliveryOk":delivered.is_ok(),"transfers":transfers,"posts":f.server.control.posts.load(Ordering::SeqCst)}));
            assert!(transfers<=1);
            result=Some(delivered.and(reply));
        })).await;
        frame.retire();
        result.unwrap()
    }).await
}
async fn diagnostic_parent(
    f: &Fixture,
    s: &Socket,
    observer: &companion_observation::Collector,
) -> (Execute, Value, Arc<Operation>) {
    f.stage("companion-staged.txt");
    let index = f.git.git(&f.git.path, &["write-tree"]);
    let prepared = diagnostic_request(f, s, Frame::Prepare(companion_query(f)), observer)
        .await
        .unwrap();
    let q = command(f, &prepared, Stage::Commit);
    let receipt = diagnostic_request(f, s, Frame::Execute(q.clone()), observer)
        .await
        .unwrap();
    let c = s.concrete(f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let tree = f.git.git(&f.git.path, &["rev-parse", "HEAD^{tree}"]);
    diagnostic_checkpoint(
        observer,
        "parent-owned-completion",
        json!({"receipt":receipt,"index":index.trim(),"tree":tree.trim(),"workers":c.review.workers.available_permits(),"globalWorkers":f.services.repository_review_capacity.workers.available_permits()}),
    );
    assert_eq!(receipt["success"], true);
    assert_eq!(
        receipt["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(index, tree);
    assert_eq!(c.review.workers.available_permits(), WORKERS);
    assert_eq!(
        f.services
            .repository_review_capacity
            .workers
            .available_permits(),
        GLOBAL_WORKERS
    );
    (q, receipt, op)
}
struct DiagnosticPanicEvidence {
    observer: companion_observation::Collector,
    label: String,
}
impl Drop for DiagnosticPanicEvidence {
    fn drop(&mut self) {
        if std::thread::panicking() {
            println!(
                "COMPANION6328-PARTIAL {}",
                json!({"case":self.label,"frames":self.observer.lines(),"terminal":false})
            );
        }
    }
}
async fn diagnostic_case<F: Future<Output = ()>>(
    label: &str,
    work: impl FnOnce(companion_observation::Collector) -> F,
) {
    use tracing::instrument::WithSubscriber;
    let (dispatch, observer) = companion_observation::Collector::new(None);
    let process = observer.process();
    let _partial = DiagnosticPanicEvidence {
        observer: observer.clone(),
        label: label.into(),
    };
    work(observer.clone()).with_subscriber(dispatch).await;
    drop(process);
    // Original Request cancellation owners may still be unwinding their own task.
    // This wait observes diagnostics only; worker/lock assertions are separate.
    let finished = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if companion_observation::read(&observer.lines())
                .incomplete
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let report = observer.finish();
    diagnostic_checkpoint(
        &observer,
        label,
        json!({"joinedObservationOwners":finished.is_ok(),"report":report}),
    );
    assert!(finished.is_ok());
    assert_eq!(report["complete"], true, "{report}");
}
#[intent_test_macros::daemon_test]
async fn companion_diagnostic_group1_owner_member_completion() {
    for member_role in [false, true] {
        diagnostic_case(if member_role{"g1-member"}else{"g1-owner"},|observer|async move {
            with_review_clock(async {
                let f=Fixture::new().await;
                let s=if member_role{member(&f).await.0}else{f.socket().await};
                std::fs::write(f.git.path.join("unstaged-proof.txt"),"untouched").unwrap();
                let (parent,receipt,op)=diagnostic_parent(&f,&s,&observer).await;
                let before=f.git.git(&f.git.path,&["status","--porcelain=v1"]);
                let child=diagnostic_request(&f,&s,Frame::Prepare(companion_child(&f,&parent)),&observer).await.unwrap();
                diagnostic_checkpoint(&observer,"child-prepared",json!({"child":child,"parent":receipt,"postCount":f.server.control.posts.load(Ordering::SeqCst),"status":f.git.git(&f.git.path,&["status","--porcelain=v1"])}));
                assert_ne!(child["reviewOperation"]["operationId"],parent.review.operation_id);
                assert_eq!(before,f.git.git(&f.git.path,&["status","--porcelain=v1"]));
                assert_eq!(op.progress.lock().unwrap().effects.len(),1);
                assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
                let c=s.concrete(&f).await;
                wait_until(||c.review.workers.available_permits()==WORKERS).await;
            }).await;
        }).await;
    }
}
#[intent_test_macros::daemon_test]
async fn companion_diagnostic_group2_once_cancel_expiry_capacity() {
    for scenario in 0..6 {
        diagnostic_case(&format!("g2-{scenario}"),|observer|async move {
            with_review_clock(async {
                let f=Fixture::new().await;let s=f.socket().await;
                let (parent,receipt,op)=diagnostic_parent(&f,&s,&observer).await;
                let c=s.concrete(&f).await;let child=companion_child(&f,&parent);
                if scenario<2 {
                    let first=s.entered(async{s.owner.capture_review(&Frame::Prepare(child.clone())).unwrap()}).await;
                    let repeated=if scenario==0{child.clone()}else{companion_child(&f,&parent)};
                    let refused=diagnostic_request(&f,&s,Frame::Prepare(repeated),&observer).await;
                    first.retire();drop(first);assert!(refused.is_err());
                } else if scenario<4 {
                    let (ready,waiting)=tokio::sync::oneshot::channel();
                    let (release,released)=tokio::sync::oneshot::channel();
                    let path=f.git.path.clone();let locks=f.services.worktree_locks.clone();
                    let holder=if scenario==2{Some(tokio::spawn(async move{locks.with_lock(&path,||async {ready.send(()).unwrap();let _=released.await;}).await;}))}else{None};
                    if scenario==2{waiting.await.unwrap();}else{*f.server.control.pause.lock().unwrap()=Some("/projects/".into());}
                    let call=diagnostic_request(&f,&s,Frame::Prepare(child.clone()),&observer);tokio::pin!(call);
                    if scenario==2 {tokio::select!{result=&mut call=>panic!("unexpected early result:{result:?}"),()=wait_until(||c.review.workers.available_permits()==WORKERS-1)=>{}}}
                    else {tokio::select!{result=&mut call=>panic!("unexpected early result:{result:?}"),()=f.server.control.entered.notified()=>{}}}
                    let released_parent=diagnostic_request(&f,&s,Frame::Release(bound(&parent)),&observer).await;
                    let reply=call.await;
                    diagnostic_checkpoint(&observer,"cancel-before-owned-join",json!({"body":diagnostic_reply(&reply),"parentRelease":diagnostic_reply(&released_parent),"workers":c.review.workers.available_permits()}));
                    if scenario==2{release.send(()).unwrap();}else{f.server.control.release.notify_one();}
                    if let Some(holder)=holder{holder.await.unwrap();}
                    wait_until(||c.review.workers.available_permits()==WORKERS).await;
                    assert!(released_parent.is_ok());assert!(reply.is_err());
                } else {
                    let hold=if scenario==5{Some(c.review.workers.clone().try_acquire_many_owned(u32::try_from(WORKERS).unwrap()).unwrap())}else{None};
                    if scenario==4{tokio::time::advance(LEASE_TTL).await;}
                    let refused=diagnostic_request(&f,&s,Frame::Prepare(child),&observer).await;
                    drop(hold);assert!(refused.is_err());
                }
                diagnostic_checkpoint(&observer,"once-only-retained-parent",json!({"receipt":receipt,"effectCount":op.progress.lock().unwrap().effects.len(),"workers":c.review.workers.available_permits(),"posts":f.server.control.posts.load(Ordering::SeqCst)}));
                assert_eq!(op.progress.lock().unwrap().effects.len(),1);
                assert_eq!(c.review.workers.available_permits(),WORKERS);assert_eq!(f.services.repository_review_capacity.workers.available_permits(),GLOBAL_WORKERS);
                assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
                assert!(diagnostic_request(&f,&s,Frame::Prepare(companion_child(&f,&parent)),&observer).await.is_err());
            }).await;
        }).await;
    }
}
#[intent_test_macros::daemon_test]
async fn companion_diagnostic_group3_continuity_authority_disclosure() {
    for scenario in 0..6 {
        diagnostic_case(&format!("g3-{scenario}"),|observer|async move {
            with_review_clock(async {
                let f=Fixture::new().await;let(s,person)=member(&f).await;
                let(parent,receipt,op)=diagnostic_parent(&f,&s,&observer).await;
                let child=companion_child(&f,&parent);
                if scenario==5 {
                    s.entered(async {
                        let frame=s.owner.capture_review(&Frame::Prepare(child.clone())).unwrap();
                        frame.scope(Box::pin(async {
                            let body=f.services.native_review_prepare(child).await;
                            diagnostic_checkpoint(&observer,"protected-child-before-revocation",diagnostic_reply(&body));
                            assert!(body.is_ok());
                            f.services.store.remove_host_member(&person.id).await.unwrap();
                            let mut transfers=0;
                            let delivered=frame.deliver(RepositoryReadReplyKind::Result,&mut||{transfers+=1;Ok(())}).await;
                            diagnostic_checkpoint(&observer,"refused-first-protected-transfer",json!({"deliveryOk":delivered.is_ok(),"protectedTransfers":transfers}));
                            assert!(delivered.is_err());assert_eq!(transfers,0);
                        })).await;
                        frame.retire();
                    }).await;
                    assert!(diagnostic_request(&f,&s,Frame::Reconcile(bound(&parent)),&observer).await.is_err());
                } else {
                    match scenario {
                        0=>{f.git.git(&f.git.path,&["commit","--allow-empty","-m","unrelated"]);},
                        1=>{f.stage("unrelated-index.txt");},
                        2=>{f.services.store.remove_host_member(&person.id).await.unwrap();},
                        3=>{f.services.store.revoke_principal_credential("review-member-credential").await.unwrap();},
                        4=>{let selection=f.services.store.repository_selection_snapshot(&f.git.root()).await.unwrap();f.services.store.write_repository_selection(&selection,intent_store::RepositorySelectionChange::Automatic).await.result.unwrap();},
                        _=>unreachable!(),
                    }
                    assert!(diagnostic_request(&f,&s,Frame::Prepare(child),&observer).await.is_err());
                }
                diagnostic_checkpoint(&observer,"private-receipt-after-refusal",json!({"original":receipt,"effects":op.progress.lock().unwrap().effects,"posts":f.server.control.posts.load(Ordering::SeqCst)}));
                assert_eq!(op.progress.lock().unwrap().effects.len(),1);
                assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
            }).await;
        }).await;
    }
}
#[intent_test_macros::daemon_test]
async fn companion_diagnostic_group4_project_metadata_deadline() {
    for deadline in [false, true] {
        diagnostic_case(if deadline{"g4-acquisition-deadline"}else{"g4-project-then-unavailable"},|observer|async move {
            with_review_clock(async {
                let f=Fixture::new().await;let s=f.socket().await;
                let(parent,receipt,op)=diagnostic_parent(&f,&s,&observer).await;
                let c=s.concrete(&f).await;
                let before=f.server.control.requests.lock().unwrap().iter().filter(|(_,path)|path.contains("/repository/branches")).count();
                *f.server.control.pause.lock().unwrap()=Some("/projects/".into());
                let call=diagnostic_request(&f,&s,Frame::Prepare(companion_child(&f,&parent)),&observer);tokio::pin!(call);
                tokio::select!{result=&mut call=>panic!("original project GET not held:{result:?}"),()=f.server.control.entered.notified()=>{}}
                let holder=if deadline {None} else {
                    let (entered,waiting)=tokio::sync::oneshot::channel();let (release,released)=std::sync::mpsc::channel();let facts=op.metadata.provider.clone();
                    let task=std::thread::spawn(move||facts.hold_native_review_metadata_for_test(entered,released));waiting.await.unwrap();Some((release,task))
                };
                if deadline{tokio::time::advance(ACQUIRE).await;}else{f.server.control.release.notify_one();}
                let reply=call.await;
                diagnostic_checkpoint(&observer,"held-project-original-return",json!({"body":diagnostic_reply(&reply),"receipt":receipt,"workers":c.review.workers.available_permits(),"requests":f.server.control.requests.lock().unwrap().clone()}));
                if let Some((release,task))=holder{release.send(()).unwrap();task.join().unwrap();}
                if deadline{f.server.control.release.notify_one();}
                wait_until(||c.review.workers.available_permits()==WORKERS).await;
                let after=f.server.control.requests.lock().unwrap().iter().filter(|(_,path)|path.contains("/repository/branches")).count();
                assert!(reply.is_err());assert_eq!(before,after);assert_eq!(f.server.control.posts.load(Ordering::SeqCst),0);
                let frames=observer.lines().into_iter().filter_map(|raw|serde_json::from_str::<companion_observation::FrameRecord>(&raw).ok()).collect::<Vec<_>>();
                assert!(frames.iter().any(|f|f.phase==companion_observation::Phase::Project));
                if !deadline {assert!(frames.iter().any(|f|f.phase==companion_observation::Phase::Branch&&f.outcome==companion_observation::Outcome::Error));}
                assert_eq!(op.progress.lock().unwrap().effects.len(),1);
            }).await;
        }).await;
    }
}
#[test]
fn companion_diagnostic_group5_reader_bounds_privacy() {
    use companion_observation::{Collector, FrameRecord, Outcome, Phase};
    use std::os::unix::fs::PermissionsExt;
    let (_dispatch, collector) = Collector::new(None);
    let probe = collector.process();
    let ok = companion_observe!(probe, Project, Ok::<_, ()>(37));
    assert_eq!(ok, Ok(37));
    let err = companion_observe!(probe, Branch, Err::<(), _>(41));
    assert_eq!(err, Err(41));
    probe.link(Phase::SourceEntered);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _scope = probe.span(Phase::Acquire);
        panic!("controlled unwind");
    }));
    assert!(panic.is_err());
    drop(probe);
    let valid = collector.lines();
    println!(
        "COMPANION6328-READER {}",
        json!({"valid":valid,"report":collector.finish()})
    );
    assert_eq!(collector.finish()["complete"], true);
    let parse = |lines: &[String]| companion_observation::read(lines);
    for scenario in 0..14 {
        let mut lines = valid.clone();
        match scenario {
            0 => {
                lines.pop();
            }
            1 => {
                lines.remove(0);
            }
            2 => {
                lines.insert(1, lines[1].clone());
            }
            3 => {
                lines.swap(1, 2);
            }
            4 => {
                lines[1] = "{".into();
            }
            5 => {
                let mut value: Value = serde_json::from_str(&lines[1]).unwrap();
                value["token"] = json!("private");
                lines[1] = value.to_string();
            }
            6 => {
                let mut value: Value = serde_json::from_str(&lines[1]).unwrap();
                value["operation"] = json!("https://forbidden.invalid");
                lines[1] = value.to_string();
            }
            7 => {
                let mut f: FrameRecord = serde_json::from_str(lines.last().unwrap()).unwrap();
                f.sequence = 1;
                f.observed = 1;
                f.span = None;
                lines = vec![serde_json::to_string(&f).unwrap()];
            }
            8 => {
                let mut f: FrameRecord = serde_json::from_str(lines.last().unwrap()).unwrap();
                f.sequence = 1;
                f.observed = 1;
                f.span = None;
                f.outcome = Outcome::Unwind;
                lines = vec![serde_json::to_string(&f).unwrap()];
            }
            9 => {
                let mut f: FrameRecord = serde_json::from_str(lines.last().unwrap()).unwrap();
                f.span = Some(99);
                *lines.last_mut().unwrap() = serde_json::to_string(&f).unwrap();
            }
            10 => {
                let mut f: FrameRecord = serde_json::from_str(&lines[1]).unwrap();
                f.domain = "foreign-clock".into();
                lines[1] = serde_json::to_string(&f).unwrap();
            }
            11 => {
                lines.clear();
            }
            12 => {
                let mut f: FrameRecord = serde_json::from_str(lines.last().unwrap()).unwrap();
                f.sequence = 1;
                f.observed = 1;
                f.span = Some(1);
                lines = vec![serde_json::to_string(&f).unwrap()];
            }
            _ => {
                let mut f: FrameRecord = serde_json::from_str(lines.last().unwrap()).unwrap();
                f.phase = Phase::Branch;
                *lines.last_mut().unwrap() = serde_json::to_string(&f).unwrap();
            }
        }
        let report = parse(&lines);
        println!(
            "COMPANION6328-READER {}",
            json!({"subcase":scenario,"report":report})
        );
        assert!(!report.errors.is_empty() || !report.incomplete.is_empty());
    }
    let (_dispatch, limited) = Collector::new(None);
    let probe = limited.process();
    for _ in 0..300 {
        probe.link(Phase::SourceEntered);
    }
    drop(probe);
    let report = limited.finish();
    println!("COMPANION6328-READER {report}");
    assert_eq!(report["complete"], false);
    let (_dispatch, multi) = Collector::new(None);
    for _ in 0..2 {
        let p = multi.process();
        p.link(Phase::SourceEntered);
        drop(p);
    }
    assert_eq!(multi.finish()["complete"], true);
    let (_dispatch, capped) = Collector::new(None);
    for _ in 0..9 {
        let p = capped.process();
        for _ in 0..250 {
            p.link(Phase::SourceEntered);
        }
        drop(p);
    }
    let full = capped.finish();
    println!("COMPANION6328-READER {full}");
    assert_eq!(full["complete"], false);
    assert!(full["accounting"]["overflow"].as_u64().unwrap() > 0);
    let mut oversized = valid.clone();
    oversized[1] = "x".repeat(companion_observation::FRAME_BYTES + 1);
    assert!(!parse(&oversized).errors.is_empty());
    let directory = crate::test_support::test_tempdir("companion-observer-io");
    let disk_path = directory.path().join("actual-private-output.jsonl");
    let (_dispatch, disk) = Collector::new(Some(&disk_path));
    let p = disk.process();
    p.link(Phase::SourceEntered);
    drop(p);
    let report = disk.finish();
    assert_eq!(report["complete"], true);
    assert_eq!(
        std::fs::read_to_string(&disk_path)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        disk.lines().iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(
        std::fs::metadata(disk_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let (_dispatch, io) = Collector::new(Some(&directory.path().join("absent/output")));
    let probe = io.process();
    let result = companion_observe!(probe, Project, Ok::<_, ()>(43));
    drop(probe);
    assert_eq!(result, Ok(43));
    assert_eq!(io.finish()["accounting"]["io"], 1);
    assert!(!valid.iter().any(|line| line.contains("stored-pat")
        || line.contains("group/project")
        || line.contains("account")));
}
// companion-observation: end qualification
