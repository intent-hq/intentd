//! Real Store/SQLite plus a fixture invalidation observer. No R delivery claim.
use super::*;
use crate::{
    RepositorySelectionChange, RepositorySelectionPersistence, RepositorySelectionWriteResult,
};
use intent_core::{RepositoryRootId, RepositoryRootKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Default)]
struct ProbeState {
    next: usize,
    pending: HashMap<usize, Vec<RepositoryLifecycleKey>>,
    leaves: Vec<(RepositoryLifecycleKey, Arc<AtomicBool>)>,
}
#[derive(Default)]
struct Probe {
    state: Arc<Mutex<ProbeState>>,
    hold: Mutex<Option<(Arc<tokio::sync::Notify>, std::sync::mpsc::Receiver<()>)>>,
    reject: AtomicBool,
    panic_settle: Arc<AtomicBool>,
}
struct Ticket {
    state: Arc<Mutex<ProbeState>>,
    id: usize,
    panic: Arc<AtomicBool>,
}
impl RepositoryLifecycleMutationTicket for Ticket {
    fn settle_confirmed(self: Box<Self>) {
        assert!(
            !self.panic.load(Ordering::SeqCst),
            "fixture settlement failure"
        );
        assert!(self
            .state
            .lock()
            .unwrap()
            .pending
            .remove(&self.id)
            .is_some());
    }
}
impl RepositoryLifecycleObserver for Probe {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        let id = {
            let mut s = self.state.lock().unwrap();
            s.next += 1;
            let id = s.next;
            s.pending.insert(id, keys.to_vec());
            for (key, live) in &s.leaves {
                if keys.contains(&RepositoryLifecycleKey::Database) || keys.contains(key) {
                    live.store(false, Ordering::SeqCst);
                }
            }
            id
        };
        let hold = self.hold.lock().unwrap().take();
        if let Some((entered, rx)) = hold {
            entered.notify_one();
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        if self.reject.load(Ordering::SeqCst) {
            return Err(Error::Internal("fixture begin refusal".into()));
        }
        Ok(Box::new(Ticket {
            state: self.state.clone(),
            id,
            panic: self.panic_settle.clone(),
        }))
    }
}
impl Probe {
    fn leaf(&self, key: RepositoryLifecycleKey) -> Arc<AtomicBool> {
        let live = Arc::new(AtomicBool::new(true));
        self.state.lock().unwrap().leaves.push((key, live.clone()));
        live
    }
    fn count(&self, key: &RepositoryLifecycleKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .pending
            .values()
            .filter(|v| v.contains(key))
            .count()
    }
    async fn install(self: &Arc<Self>, store: &Store) -> Arc<dyn RepositoryLifecycleObserver> {
        let observer: Arc<dyn RepositoryLifecycleObserver> = self.clone();
        store
            .install_repository_lifecycle_observer(observer.clone())
            .await
            .unwrap();
        observer
    }
}
async fn fixture() -> (tempfile::TempDir, Store, RepositoryRootId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("selection.db")).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES('ws','Test','main','same','same')")
        .execute(store.write_pool()).await.unwrap();
    let root = RepositoryRootId {
        workspace_id: WorkspaceId::from("ws"),
        kind: RepositoryRootKind::Primary,
    };
    (dir, store, root)
}
fn selection_key(root: &RepositoryRootId) -> RepositoryLifecycleKey {
    RepositoryLifecycleKey::Selection {
        workspace_id: root.workspace_id.clone(),
        git_root_id: None,
    }
}
fn change() -> RepositorySelectionChange {
    RepositorySelectionChange::ExplicitRemote {
        remote_name: "upstream".into(),
    }
}

#[tokio::test]
async fn choice_invalidation_preserves_unrelated_physical_and_mandatory_fixture_leaves() {
    let (_dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    probe.install(&store).await;
    let choice = probe.leaf(selection_key(&root));
    let physical = probe.leaf(RepositoryLifecycleKey::Agent(AgentId::from("agent")));
    let mandatory = probe.leaf(RepositoryLifecycleKey::Workspace(root.workspace_id.clone()));
    let sibling = probe.leaf(RepositoryLifecycleKey::Selection {
        workspace_id: root.workspace_id.clone(),
        git_root_id: Some(WorkspaceGitRootId::from("other")),
    });
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    let out = store.write_repository_selection(&before, change()).await;
    assert!(matches!(
        out.result,
        Ok(RepositorySelectionWriteResult::Applied(_))
    ));
    assert!(!choice.load(Ordering::SeqCst));
    assert!(
        physical.load(Ordering::SeqCst)
            && mandatory.load(Ordering::SeqCst)
            && sibling.load(Ordering::SeqCst)
    );
    assert_eq!(probe.count(&selection_key(&root)), 0);
    let fresh = probe.leaf(selection_key(&root));
    let latest = store.repository_selection_snapshot(&root).await.unwrap();
    let out = store.write_repository_selection(&latest, change()).await;
    assert_eq!(out.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(fresh.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_observer_releases_writer_serialization_and_second_comparison_detects_race() {
    let (_dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    probe.install(&store).await;
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, rx) = std::sync::mpsc::channel();
    *probe.hold.lock().unwrap() = Some((entered.clone(), rx));
    let writer = store.clone();
    let task =
        tokio::spawn(async move { writer.write_repository_selection(&before, change()).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let current = tokio::time::timeout(
        Duration::from_secs(5),
        store.repository_selection_snapshot(&root),
    )
    .await
    .unwrap()
    .unwrap();
    let other = store
        .write_repository_selection(&current, RepositorySelectionChange::Automatic)
        .await;
    assert!(matches!(
        other.result,
        Ok(RepositorySelectionWriteResult::Applied(_))
    ));
    assert_eq!(
        probe.count(&selection_key(&root)),
        1,
        "only other ticket settled"
    );
    release.send(()).unwrap();
    let out = task.await.unwrap();
    assert_eq!(out.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(matches!(
        out.result,
        Ok(RepositorySelectionWriteResult::Conflict(_))
    ));
    assert_eq!(probe.count(&selection_key(&root)), 0);
}

#[tokio::test]
async fn preobserver_original_owner_blocks_install_and_unknown_survives_last_handle_reopen() {
    let (dir, store, root) = fixture().await;
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    let mut original = store.repository_lifecycle_write().await.unwrap();
    original.begin_selection_change(&root).unwrap();
    assert!(store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .is_err());
    // A second known owner cannot settle the predecessor.
    let mut other = store.repository_lifecycle_write().await.unwrap();
    other.begin_selection_change(&root).unwrap();
    other.settle();
    drop(original);
    drop(store);
    let reopened = Store::open(&dir.path().join("selection.db")).await.unwrap();
    assert!(reopened
        .install_repository_lifecycle_observer(observer)
        .await
        .is_err());
}

#[tokio::test]
async fn confirmed_preobserver_owner_allows_install_but_installed_unknown_never_resets() {
    let (_dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    let mut original = store.repository_lifecycle_write().await.unwrap();
    original.begin_selection_change(&root).unwrap();
    original.settle();
    let observer = probe.install(&store).await;
    let mut unknown = store.repository_lifecycle_write().await.unwrap();
    unknown.begin_selection_change(&root).unwrap();
    drop(unknown);
    store
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    store
        .write_repository_selection(&before, change())
        .await
        .result
        .unwrap();
    assert_eq!(probe.count(&selection_key(&root)), 1);
}

#[tokio::test]
async fn observer_refusal_and_settlement_panic_preserve_original_barrier_and_commit_fact() {
    let (_dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    probe.install(&store).await;
    probe.reject.store(true, Ordering::SeqCst);
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    let refused = store.write_repository_selection(&before, change()).await;
    assert!(refused.result.is_err());
    assert_eq!(
        refused.persistence,
        RepositorySelectionPersistence::NotAttempted
    );
    assert_eq!(probe.count(&selection_key(&root)), 1);
    probe.reject.store(false, Ordering::SeqCst);
    probe.panic_settle.store(true, Ordering::SeqCst);
    let committed = store.write_repository_selection(&before, change()).await;
    assert!(committed.result.is_err());
    assert!(matches!(
        committed.persistence,
        RepositorySelectionPersistence::Committed { .. }
    ));
    assert_eq!(probe.count(&selection_key(&root)), 2);
    let after = store.repository_selection_snapshot(&root).await.unwrap();
    assert!(after.selection_revision().unwrap().get() > before.selection_revision().unwrap().get());
}

#[tokio::test]
async fn unpolled_selection_and_invalid_resume_never_allocate_a_second_ticket() {
    let (_dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    probe.install(&store).await;
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    drop(store.write_repository_selection(&before, change()));
    assert_eq!(probe.count(&selection_key(&root)), 0);
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    assert!(owner.resume_serialization().await.is_err());
    owner.begin_selection_change(&root).unwrap();
    assert!(owner.begin_selection_change(&root).is_err());
    owner.resume_serialization().await.unwrap();
    assert!(owner.resume_serialization().await.is_err());
    owner.settle();
    assert_eq!(probe.count(&selection_key(&root)), 0);
}

async fn cancelled_commit(installed: bool) {
    let (dir, store, root) = fixture().await;
    let probe = Arc::new(Probe::default());
    let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
    if installed {
        probe.install(&store).await;
    }
    let before = store.repository_selection_snapshot(&root).await.unwrap();
    let original_revision = before.selection_revision().unwrap();
    let pool = store.write_pool().clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let (release, rx) = std::sync::mpsc::channel();
    let mut rx = Some(rx);
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(rx) = rx.take() {
                notify.notify_one();
                rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            true
        });
    drop(connection);
    let task =
        tokio::spawn(async move { store.write_repository_selection(&before, change()).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(matches!(task.await,Err(e) if e.is_cancelled()));
    release.send(()).unwrap();
    let mut connection = pool.acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_commit_hook();
    let revision: i64 = sqlx::query_scalar(
        "SELECT selection_revision FROM repository_selection_state WHERE workspace_id='ws'",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert!(
        u64::try_from(revision).unwrap() > original_revision.get(),
        "late real SQLite COMMIT happened"
    );
    drop(connection);
    drop(pool);
    let reopened = Store::open(&dir.path().join("selection.db")).await.unwrap();
    if installed {
        assert!(reopened.has_repository_lifecycle_observer(&observer));
        reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .unwrap();
        assert_eq!(probe.count(&selection_key(&root)), 1);
        let latest = reopened.repository_selection_snapshot(&root).await.unwrap();
        reopened
            .reset_repository_selection(&latest)
            .await
            .result
            .unwrap();
        assert_eq!(probe.count(&selection_key(&root)), 1);
    } else {
        assert!(reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .is_err());
    }
}
#[tokio::test]
async fn canceled_sqlite_commit_without_observer_remains_unknown_on_managed_reopen() {
    cancelled_commit(false).await;
}
#[tokio::test]
async fn canceled_sqlite_commit_with_observer_remains_unknown_after_later_success() {
    cancelled_commit(true).await;
}
