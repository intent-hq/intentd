//! Actual disposable Store ownership with a fixture observer, not R or Services delivery.

use super::*;
use intent_core::Workspace;
use intent_store::{RepositoryLifecycleMutationTicket, RepositoryLifecycleObserver};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Waker};
use std::time::Duration;

#[derive(Default)]
struct Probe {
    state: Arc<Mutex<ProbeState>>,
    callback: Mutex<Option<(Arc<tokio::sync::Notify>, std::sync::mpsc::Receiver<()>)>>,
    reject: AtomicBool,
}

#[derive(Default)]
struct ProbeState {
    next: usize,
    pending: HashMap<usize, Vec<RepositoryLifecycleKey>>,
    begins: usize,
}

struct Ticket {
    state: Arc<Mutex<ProbeState>>,
    id: usize,
}

impl RepositoryLifecycleMutationTicket for Ticket {
    fn settle_confirmed(self: Box<Self>) {
        assert!(self
            .state
            .lock()
            .unwrap()
            .pending
            .remove(&self.id)
            .is_some());
    }
}

impl Probe {
    fn ticket(
        &self,
        keys: &[RepositoryLifecycleKey],
        reversible: bool,
    ) -> Box<dyn RepositoryLifecycleMutationTicket> {
        let mut s = self.state.lock().unwrap();
        s.next += 1;
        let id = s.next;
        s.pending.insert(id, keys.to_vec());
        s.begins += usize::from(reversible);
        Box::new(Ticket {
            state: self.state.clone(),
            id,
        })
    }

    fn blocked(&self, subject: &PendingDeleteSubject) -> bool {
        self.state.lock().unwrap().pending.values().any(|keys| {
            keys.contains(&subject.lifecycle_key())
                || keys.contains(&RepositoryLifecycleKey::Database)
        })
    }

    async fn install(self: &Arc<Self>, store: &Store) {
        store
            .install_repository_lifecycle_observer(self.clone())
            .await
            .unwrap();
    }
}

impl RepositoryLifecycleObserver for Probe {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        Ok(self.ticket(keys, false))
    }

    fn begin_pending_delete(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        let ticket = self.ticket(keys, true);
        let callback = self.callback.lock().unwrap().take();
        if let Some((entered, release)) = callback {
            entered.notify_one();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        if self.reject.load(Ordering::SeqCst) {
            return Err(Error::Internal("fixture unknown begin".into()));
        }
        Ok(ticket)
    }
}

struct Fixture {
    store: Store,
    registry: OwnedPendingDeletes,
    subject: PendingDeleteSubject,
    probe: Arc<Probe>,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new(observed: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("owned.db")).await.unwrap();
        let workspace: Workspace = serde_json::from_value(serde_json::json!({
            "id":"ws-owned-delete", "title":"Owned", "branch":"main", "status":"Active",
            "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
            "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
        }))
        .unwrap();
        store.insert_workspace(&workspace).await.unwrap();
        let subject = PendingDeleteSubject::Workspace(workspace.id);
        let probe = Arc::new(Probe::default());
        if observed {
            probe.install(&store).await;
        }
        Self {
            registry: OwnedPendingDeletes::new(store.clone()),
            store,
            subject,
            probe,
            dir,
        }
    }
}

fn pending<F: Future>(mut f: Pin<&mut F>) {
    assert!(f
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
}

async fn no_delete(_claim: PendingDeleteClaim) {
    panic!("cancelled timer invoked its deletion closure");
}

#[tokio::test(start_paused = true)]
async fn legacy_deadline_duplicate_claim_cancel_and_cap_are_unchanged() {
    assert_eq!(clamp_undo_delay_ms(0), 0);
    assert_eq!(clamp_undo_delay_ms(u64::MAX), 60_000);
    let registry = PendingDeletes::default();
    let captured = Arc::new(AtomicU64::new(0));
    let save = captured.clone();
    assert_eq!(
        registry.schedule("a".into(), "deadline".into(), move |token| {
            save.store(token, Ordering::SeqCst);
            tokio::spawn(std::future::pending())
        }),
        None
    );
    assert_eq!(
        registry.schedule("a".into(), "other".into(), |_| panic!("duplicate spawn")),
        Some("deadline".into())
    );
    assert_eq!(registry.deadline("a"), Some("deadline".into()));
    let old = captured.load(Ordering::SeqCst);
    assert!(registry.cancel("a"));
    assert!(!registry.cancel("a"));
    let save = captured.clone();
    registry.schedule("a".into(), "new".into(), move |token| {
        save.store(token, Ordering::SeqCst);
        tokio::spawn(async {})
    });
    assert!(!registry.claim("a", old));
    assert!(registry.claim("a", captured.load(Ordering::SeqCst)));
    assert!(!registry.cancel("a"));
}

#[tokio::test]
async fn zero_delay_claim_is_original_and_marker_is_removed_before_user_code() {
    let f = Fixture::new(true).await;
    tokio::time::pause();
    let registry = f.registry.clone();
    let subject = f.subject.clone();
    let probe = f.probe.clone();
    let (sent, received) = oneshot::channel();
    let result = f
        .registry
        .schedule_owned(f.subject.clone(), 0, move |claim| async move {
            assert_eq!(claim.subject(), &subject);
            assert!(registry.deadline(&subject).unwrap().is_none());
            assert!(probe.blocked(&subject));
            assert!(sent.send(claim).is_ok());
        })
        .await
        .unwrap();
    assert!(result.newly_armed);
    let claim = received.await.unwrap();
    assert!(!f.registry.cancel_owned(&f.subject).await.unwrap());
    assert!(f
        .registry
        .take_for_cascade(&f.subject)
        .await
        .unwrap()
        .is_none());
    // An immediate operation after claim gets a DIFFERENT owned ticket.
    let immediate = f
        .registry
        .take_for_immediate_delete(f.subject.clone())
        .await
        .unwrap();
    assert_eq!(f.probe.state.lock().unwrap().begins, 2);
    immediate.settle_confirmed();
    assert!(f.probe.blocked(&f.subject));
    claim.settle_confirmed();
    assert!(!f.probe.blocked(&f.subject));
}

#[tokio::test]
async fn duplicate_keeps_deadline_clamps_delay_and_cancellation_prevents_user_code() {
    let f = Fixture::new(true).await;
    tokio::time::pause();
    let a = f
        .registry
        .schedule_owned(f.subject.clone(), u64::MAX, no_delete)
        .await
        .unwrap();
    let duplicate = f
        .registry
        .schedule_owned(f.subject.clone(), 0, no_delete)
        .await
        .unwrap();
    assert_eq!(a.delete_at, duplicate.delete_at);
    assert!(a.newly_armed && !duplicate.newly_armed);
    assert_eq!(f.probe.state.lock().unwrap().begins, 1);
    tokio::time::advance(Duration::from_millis(MAX_UNDO_DELAY_MS - 1)).await;
    assert!(f.registry.deadline(&f.subject).unwrap().is_some());
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(f.registry.deadline(&f.subject).unwrap().is_none());
    assert!(!f.probe.blocked(&f.subject));
}

#[tokio::test]
async fn mismatched_agent_workspace_metadata_never_takes_or_rebinds_original() {
    let f = Fixture::new(true).await;
    tokio::time::pause();
    let original = PendingDeleteSubject::Agent {
        workspace_id: WorkspaceId::from("original"),
        agent_id: AgentId::from("a"),
    };
    let wrong = PendingDeleteSubject::Agent {
        workspace_id: WorkspaceId::from("replacement"),
        agent_id: AgentId::from("a"),
    };
    f.registry
        .schedule_owned(original.clone(), 60_000, no_delete)
        .await
        .unwrap();
    assert!(f.registry.deadline(&wrong).is_err());
    assert!(f.registry.cancel_owned(&wrong).await.is_err());
    assert!(f.registry.take_for_cascade(&wrong).await.is_err());
    assert!(f
        .registry
        .take_for_immediate_delete(wrong.clone())
        .await
        .is_err());
    assert!(f
        .registry
        .schedule_owned(wrong, 1, no_delete)
        .await
        .is_err());
    assert_eq!(f.probe.state.lock().unwrap().begins, 1);
    assert!(f.probe.blocked(&original));
    let workspace = PendingDeleteSubject::Workspace(WorkspaceId::from("a"));
    f.registry
        .schedule_owned(workspace.clone(), 60_000, no_delete)
        .await
        .unwrap();
    assert!(f.registry.cancel_owned(&workspace).await.unwrap());
    assert!(f.probe.blocked(&original));
    assert!(f.registry.cancel_owned(&original).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparation_waiters_are_pinned_and_unrelated_keys_progress_during_retirement() {
    let f = Fixture::new(true).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    *f.probe.callback.lock().unwrap() = Some((entered.clone(), blocked));
    let registry = f.registry.clone();
    let subject = f.subject.clone();
    let winner =
        tokio::spawn(async move { registry.schedule_owned(subject, 60_000, no_delete).await });
    entered.notified().await;
    assert!(f.probe.blocked(&f.subject));
    assert!(f.registry.deadline(&f.subject).unwrap().is_none());
    let mut duplicate = Box::pin(f.registry.schedule_owned(f.subject.clone(), 0, no_delete));
    let mut cancel = Box::pin(f.registry.cancel_owned(&f.subject));
    pending(duplicate.as_mut());
    pending(cancel.as_mut());
    // Dropping a different waiter cannot remove the winner's preparation.
    let mut abandoned_waiter = Box::pin(f.registry.cancel_owned(&f.subject));
    pending(abandoned_waiter.as_mut());
    drop(abandoned_waiter);
    let other = PendingDeleteSubject::Workspace(WorkspaceId::from("unrelated"));
    tokio::time::timeout(
        Duration::from_secs(5),
        f.registry.schedule_owned(other.clone(), 60_000, no_delete),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(f.registry.cancel_owned(&other).await.unwrap());
    release.send(()).unwrap();
    let first = winner.await.unwrap().unwrap();
    let token = f.registry.original_attempt(&f.subject).unwrap().unwrap();
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
    let successor = f
        .registry
        .schedule_owned(f.subject.clone(), 40_000, no_delete)
        .await
        .unwrap();
    assert_ne!(first.delete_at, successor.delete_at);
    assert!(
        !cancel.await.unwrap(),
        "a preparing waiter cannot cancel a successor"
    );
    assert_eq!(duplicate.await.unwrap().delete_at, first.delete_at);
    assert!(
        f.registry.inner.take_armed(&token).is_none(),
        "old timer token is not a current-key lookup"
    );
    assert_eq!(
        f.registry.deadline(&f.subject).unwrap(),
        Some(successor.delete_at)
    );
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
}

#[tokio::test]
async fn cancelled_preparation_and_waiters_cannot_touch_the_next_reservation() {
    let f = Fixture::new(true).await;
    let held = f.store.write_pool().acquire().await.unwrap();
    // The actual existing Store writer owns domain serialization while waiting
    // for its database connection, before it can begin changing SQL.
    let principal: intent_core::Principal = serde_json::from_value(serde_json::json!({
        "id":"writer", "githubUserId":17, "login":"writer", "isPrimary":false, "createdAt":"time", "updatedAt":"time"
    }))
    .unwrap();
    let mut writer = Box::pin(f.store.upsert_principal(&principal));
    pending(writer.as_mut());
    let mut original = Box::pin(
        f.registry
            .schedule_owned(f.subject.clone(), 60_000, no_delete),
    );
    pending(original.as_mut());
    let mut duplicate = Box::pin(f.registry.schedule_owned(f.subject.clone(), 0, no_delete));
    let mut cancel = Box::pin(f.registry.cancel_owned(&f.subject));
    pending(duplicate.as_mut());
    pending(cancel.as_mut());
    drop(original);
    let mut successor = Box::pin(
        f.registry
            .schedule_owned(f.subject.clone(), 60_000, no_delete),
    );
    pending(successor.as_mut());
    assert!(duplicate.await.is_err());
    assert!(!cancel.await.unwrap());
    assert!(f.registry.original_attempt(&f.subject).unwrap().is_some());
    drop(held);
    writer.await.unwrap();
    assert!(successor.await.unwrap().newly_armed);
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
}

#[tokio::test]
async fn immediate_and_cascade_move_original_guard_without_settlement_or_reacquisition() {
    let f = Fixture::new(true).await;
    tokio::time::pause();
    f.registry
        .schedule_owned(f.subject.clone(), 60_000, no_delete)
        .await
        .unwrap();
    let immediate = f
        .registry
        .take_for_immediate_delete(f.subject.clone())
        .await
        .unwrap();
    assert_eq!(f.probe.state.lock().unwrap().begins, 1);
    assert!(f.probe.blocked(&f.subject));
    assert!(!f.registry.cancel_owned(&f.subject).await.unwrap());
    f.registry
        .schedule_owned(f.subject.clone(), 60_000, no_delete)
        .await
        .unwrap();
    let cascade = f
        .registry
        .take_for_cascade(&f.subject)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.probe.state.lock().unwrap().begins, 2);
    assert!(f
        .registry
        .take_for_cascade(&f.subject)
        .await
        .unwrap()
        .is_none());
    immediate.settle_confirmed();
    assert!(f.probe.blocked(&f.subject));
    drop(cascade); // Unknown work cannot be confirmed by another operation.
    let own = f
        .registry
        .take_for_immediate_delete(f.subject.clone())
        .await
        .unwrap();
    own.settle_confirmed();
    assert!(f.probe.blocked(&f.subject));
}

#[tokio::test]
async fn claim_survives_response_ack_and_is_settled_only_by_original_background_work() {
    let f = Fixture::new(true).await;
    let (handoff, received) = oneshot::channel();
    f.registry
        .schedule_owned(f.subject.clone(), 0, |claim| async move {
            assert!(handoff.send(claim).is_ok());
        })
        .await
        .unwrap();
    let claim = received.await.unwrap();
    assert!(f.probe.blocked(&f.subject));
    assert!(f.registry.deadline(&f.subject).unwrap().is_none());
    let (finish, finishing) = oneshot::channel();
    let store = f.store.clone();
    let PendingDeleteSubject::Workspace(id) = f.subject.clone() else {
        unreachable!()
    };
    let background = tokio::spawn(async move {
        finishing.await.unwrap();
        let outcome = store.delete_workspace_with_outcome(&id).await;
        assert_eq!(
            outcome.disposition,
            intent_store::RepositoryWorkspaceDeleteDisposition::Committed
        );
        outcome.result.unwrap();
        claim.settle_confirmed();
    });
    assert!(!f.registry.cancel_owned(&f.subject).await.unwrap());
    assert!(f.probe.blocked(&f.subject));
    finish.send(()).unwrap();
    background.await.unwrap();
    assert!(!f.probe.blocked(&f.subject));
}

#[tokio::test]
async fn dropped_panicked_and_cancelled_claims_keep_original_unknown_ownership() {
    let f = Fixture::new(true).await;
    for kind in 0..3 {
        let subject = PendingDeleteSubject::Workspace(WorkspaceId::from(format!("unknown-{kind}")));
        let (ready, received) = oneshot::channel();
        f.registry
            .schedule_owned(subject.clone(), 0, move |claim| async move {
                let _claim = claim;
                assert!(ready.send(()).is_ok());
                assert_ne!(kind, 1, "fixture deletion future panic");
                if kind == 2 {
                    std::future::pending::<()>().await;
                }
            })
            .await
            .unwrap();
        let abort = {
            let map = f.registry.inner.entries.lock().unwrap();
            let Some(OwnedEntry::Armed(entry)) = map.get(&subject.key()) else {
                panic!("armed before yield")
            };
            entry.handle.abort_handle()
        };
        received.await.unwrap();
        if kind == 2 {
            abort.abort();
            tokio::task::yield_now().await;
        }
        assert!(f.registry.deadline(&subject).unwrap().is_none());
        assert!(!f.registry.cancel_owned(&subject).await.unwrap());
        assert!(f.probe.blocked(&subject));
    }
}

#[tokio::test]
async fn deletion_future_constructor_panic_does_not_settle_claim_or_hold_map_lock() {
    let f = Fixture::new(true).await;
    let (called, received) = oneshot::channel();
    let registry = f.registry.clone();
    let subject = f.subject.clone();
    f.registry
        .schedule_owned(
            f.subject.clone(),
            0,
            move |claim| -> std::future::Ready<()> {
                assert!(registry.deadline(&subject).unwrap().is_none());
                let _claim = claim;
                assert!(called.send(()).is_ok());
                panic!("fixture future constructor panic");
            },
        )
        .await
        .unwrap();
    received.await.unwrap();
    assert!(f.probe.blocked(&f.subject));
}

#[tokio::test]
async fn no_observer_armed_cancel_and_unknown_claim_have_distinct_reopen_outcomes() {
    let f = Fixture::new(false).await;
    f.registry
        .schedule_owned(f.subject.clone(), 60_000, no_delete)
        .await
        .unwrap();
    assert!(f
        .store
        .install_repository_lifecycle_observer(f.probe.clone())
        .await
        .is_err());
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
    f.probe.install(&f.store).await;

    let other = Fixture::new(false).await;
    let claim = other
        .registry
        .take_for_immediate_delete(other.subject.clone())
        .await
        .unwrap();
    drop(claim);
    drop(other.registry);
    drop(other.store);
    let reopened = Store::open(&other.dir.path().join("owned.db"))
        .await
        .unwrap();
    assert!(reopened
        .install_repository_lifecycle_observer(other.probe)
        .await
        .is_err());
}

#[tokio::test]
async fn unknown_begin_removes_only_own_preparing_marker_and_survives_reopen() {
    let f = Fixture::new(true).await;
    f.probe.reject.store(true, Ordering::SeqCst);
    assert!(f
        .registry
        .schedule_owned(f.subject.clone(), 60_000, no_delete)
        .await
        .is_err());
    assert!(f.registry.deadline(&f.subject).unwrap().is_none());
    assert!(f.probe.blocked(&f.subject));
    f.probe.reject.store(false, Ordering::SeqCst);
    f.registry
        .schedule_owned(f.subject.clone(), 60_000, no_delete)
        .await
        .unwrap();
    assert!(f.registry.cancel_owned(&f.subject).await.unwrap());
    assert!(f.probe.blocked(&f.subject));
    drop(f.registry);
    drop(f.store);
    let reopened = Store::open(&f.dir.path().join("owned.db")).await.unwrap();
    f.probe.install(&reopened).await;
    assert!(f.probe.blocked(&f.subject));
}

#[test]
fn spawn_panic_drops_only_original_reservation_without_confirming_uncertainty() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let f = runtime.block_on(Fixture::new(true));
    let other = PendingDeleteSubject::Workspace(WorkspaceId::from("other"));
    runtime
        .block_on(f.registry.schedule_owned(other.clone(), 60_000, no_delete))
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut schedule = Box::pin(f.registry.schedule_owned(
            f.subject.clone(),
            60_000,
            no_delete,
        ));
        let _ = schedule
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
    }));
    assert!(result.is_err());
    assert!(f.registry.deadline(&f.subject).unwrap().is_none());
    assert!(f.registry.deadline(&other).unwrap().is_some());
    assert!(f.probe.blocked(&f.subject));
    assert!(runtime.block_on(f.registry.cancel_owned(&other)).unwrap());
    assert!(f.probe.blocked(&f.subject));
}
