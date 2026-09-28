//! Actual Store/SQLite fixtures with a test observer, not R or timer admission.

use intent_core::Workspace;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::RepositoryWorkspaceDeleteDisposition::{Committed, NoEffect, Unknown};

use super::*;

struct Fixture {
    store: Store,
    workspace: Workspace,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new(present: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("pending.db")).await.unwrap();
        let workspace: Workspace = serde_json::from_value(serde_json::json!({
            "id":"ws-pending-delete", "title":"Pending", "branch":"main", "status":"Active",
            "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
            "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
        }))
        .unwrap();
        if present {
            store.insert_workspace(&workspace).await.unwrap();
        }
        Self {
            store,
            workspace,
            dir,
        }
    }

    async fn draft(&self) {
        self.store
            .upsert_client(
                &intent_core::ClientId::from("client"),
                None,
                None,
                &intent_core::ClientHostInfo::default(),
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO draft(workspace_id,agent_id,client_id,text,updated_at,attachments) VALUES (?, 'agent', 'client', 'draft', 'same-time', '[]')")
            .bind(&self.workspace.id.0).execute(self.store.write_pool()).await.unwrap();
    }

    async fn drafts(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM draft WHERE workspace_id=?")
            .bind(&self.workspace.id.0)
            .fetch_one(self.store.read_pool())
            .await
            .unwrap()
    }

    async fn ignore_final_delete(&self) {
        sqlx::query("CREATE TRIGGER ignore_final_delete BEFORE DELETE ON workspace BEGIN SELECT RAISE(IGNORE); END")
            .execute(self.store.write_pool()).await.unwrap();
    }
}

#[tokio::test]
async fn legacy_absent_not_found_preserves_opaque_draft() {
    let f = Fixture::new(false).await;
    f.draft().await;
    assert!(matches!(
        f.store.delete_workspace(&f.workspace.id).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(f.drafts().await, 1);
    assert!(f.dir.path().join("pending.db").exists());
}

#[tokio::test]
async fn legacy_late_not_found_has_already_committed_cleanup() {
    let f = Fixture::new(true).await;
    f.draft().await;
    f.ignore_final_delete().await;
    assert!(matches!(
        f.store.delete_workspace(&f.workspace.id).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(f.drafts().await, 0);
    assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
}

#[derive(Default)]
struct ProbeState {
    next: usize,
    pending: HashMap<usize, Vec<RepositoryLifecycleKey>>,
    ordinary: usize,
    reversible: usize,
}

#[derive(Default)]
struct Probe {
    state: Arc<Mutex<ProbeState>>,
    callback: Mutex<Option<(Arc<tokio::sync::Notify>, std::sync::mpsc::Receiver<()>)>>,
    reject_pending: AtomicBool,
    panic_pending: AtomicBool,
    reject_ordinary: AtomicBool,
    panic_settlement: Arc<AtomicBool>,
    settled: Arc<tokio::sync::Notify>,
}

struct Ticket {
    state: Arc<Mutex<ProbeState>>,
    id: usize,
    panic_settlement: Arc<AtomicBool>,
    settled: Arc<tokio::sync::Notify>,
}

impl RepositoryLifecycleMutationTicket for Ticket {
    fn settle_confirmed(self: Box<Self>) {
        assert!(
            !self.panic_settlement.load(Ordering::SeqCst),
            "fixture settlement panic"
        );
        assert!(self
            .state
            .lock()
            .unwrap()
            .pending
            .remove(&self.id)
            .is_some());
        self.settled.notify_one();
    }
}

impl Probe {
    fn allocate(
        &self,
        keys: &[RepositoryLifecycleKey],
        reversible: bool,
    ) -> Box<dyn RepositoryLifecycleMutationTicket> {
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        state.pending.insert(id, keys.to_vec());
        if reversible {
            state.reversible += 1;
        } else {
            state.ordinary += 1;
        }
        Box::new(Ticket {
            state: self.state.clone(),
            id,
            panic_settlement: self.panic_settlement.clone(),
            settled: self.settled.clone(),
        })
    }

    fn blocked(&self, key: &RepositoryLifecycleKey) -> bool {
        self.state
            .lock()
            .unwrap()
            .pending
            .values()
            .any(|keys| keys.contains(&RepositoryLifecycleKey::Database) || keys.contains(key))
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

impl RepositoryLifecycleObserver for Probe {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        if self.reject_ordinary.load(Ordering::SeqCst) {
            return Err(Error::Internal("fixture ordinary rejection".into()));
        }
        Ok(self.allocate(keys, false))
    }

    fn begin_pending_delete(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        let ticket = self.allocate(keys, true);
        let block = self.callback.lock().unwrap().take();
        if let Some((entered, release)) = block {
            entered.notify_one();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        assert!(
            !self.panic_pending.load(Ordering::SeqCst),
            "fixture begin panic"
        );
        if self.reject_pending.load(Ordering::SeqCst) {
            return Err(Error::Internal("fixture pending rejection".into()));
        }
        Ok(ticket)
    }
}

impl Fixture {
    fn key(&self) -> RepositoryLifecycleKey {
        RepositoryLifecycleKey::Workspace(self.workspace.id.clone())
    }

    async fn tombstones(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id WHERE id=?")
            .bind(&self.workspace.id.0)
            .fetch_one(self.store.read_pool())
            .await
            .unwrap()
    }

    async fn browser(&self) {
        self.draft().await;
        self.store
            .upsert_browser_tab(
                &intent_core::ClientId::from("client"),
                intent_core::BrowserTabInput {
                    tab_id: "tab".into(),
                    workspace_id: self.workspace.id.clone(),
                    url: "https://example.test/".into(),
                    requested_url: None,
                    title: None,
                    owner_agent_id: None,
                    owner_agent_name: None,
                    visibility: intent_core::BrowserTabVisibility::Visible,
                    emulated_size: None,
                    displayed: Some(true),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn unobserved_original_guard_blocks_first_install_until_confirmed() {
    let f = Fixture::new(true).await;
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    let guard = f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .unwrap();
    assert!(f
        .store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .is_err());
    guard.settle_confirmed();
    f.store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    assert!(f.store.has_repository_lifecycle_observer(&observer));
}

#[tokio::test]
async fn default_observer_is_unavailable_without_ordinary_fallback() {
    struct OrdinaryOnly(Arc<Probe>);
    impl RepositoryLifecycleObserver for OrdinaryOnly {
        fn begin_mutation(
            &self,
            keys: &[RepositoryLifecycleKey],
        ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
            self.0.begin_mutation(keys)
        }
    }
    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    let observer = Arc::new(OrdinaryOnly(probe.clone()));
    f.store
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
    assert!(f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .is_err());
    assert_eq!(probe.state.lock().unwrap().ordinary, 0);
    assert_eq!(probe.state.lock().unwrap().reversible, 0);
    assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
}

#[tokio::test]
async fn own_confirmation_does_not_settle_predecessor_or_unrelated_ticket() {
    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    let key = f.key();
    let other = RepositoryLifecycleKey::Agent(AgentId::from("other"));
    let first = f
        .store
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap();
    let second = f
        .store
        .clone()
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap();
    let unrelated = f
        .store
        .begin_repository_pending_delete(std::slice::from_ref(&other))
        .await
        .unwrap();
    second.settle_confirmed();
    assert!(probe.blocked(&key));
    assert!(probe.blocked(&other));
    first.settle_confirmed();
    assert!(!probe.blocked(&key));
    assert!(probe.blocked(&other));
    unrelated.settle_confirmed();
    assert!(!probe.blocked(&other));
}

#[tokio::test]
async fn actual_observer_and_unknown_ticket_survive_last_store_reopen() {
    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    let observer = probe.install(&f.store).await;
    let key = f.key();
    let guard = f
        .store
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap();
    let replacement: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(f
        .store
        .install_repository_lifecycle_observer(replacement)
        .await
        .is_err());
    drop(guard);
    drop(f.store);
    let reopened = Store::open(&f.dir.path().join("pending.db")).await.unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&observer));
    reopened
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
    reopened
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap()
        .settle_confirmed();
    assert!(
        probe.blocked(&key),
        "neither reinstall nor later success settles the original"
    );
}

#[tokio::test]
async fn absent_observer_unknown_guard_survives_last_store_reopen() {
    let f = Fixture::new(true).await;
    let guard = f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .unwrap();
    drop(guard);
    drop(f.store);
    let reopened = Store::open(&f.dir.path().join("pending.db")).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(reopened
        .install_repository_lifecycle_observer(observer)
        .await
        .is_err());
}

#[tokio::test]
async fn retained_original_can_confirm_after_independent_managed_open() {
    let f = Fixture::new(true).await;
    let guard = f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .unwrap();
    drop(f.store);
    let reopened = Store::open(&f.dir.path().join("pending.db")).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(reopened
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .is_err());
    guard.settle_confirmed();
    reopened
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
}

fn assert_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(future.poll(&mut context).is_pending());
}

#[tokio::test]
async fn unpolled_and_cancelled_queued_begins_do_not_invent_ownership() {
    let f = Fixture::new(true).await;
    let keys = [f.key()];
    drop(f.store.begin_repository_pending_delete(&keys));
    let serial = f
        .store
        .repository_lifecycle
        .writers
        .clone()
        .lock_owned()
        .await;
    let mut queued = Box::pin(f.store.begin_repository_pending_delete(&keys));
    assert_pending(queued.as_mut());
    drop(queued);
    drop(serial);
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    f.store
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
}

#[tokio::test]
async fn begin_and_first_install_have_both_serialized_orders() {
    for install_first in [false, true] {
        let f = Fixture::new(true).await;
        let probe = Arc::new(Probe::default());
        let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
        let serial = f
            .store
            .repository_lifecycle
            .writers
            .clone()
            .lock_owned()
            .await;
        let keys = [f.key()];
        let mut begin = Box::pin(f.store.begin_repository_pending_delete(&keys));
        let mut install = Box::pin(
            f.store
                .install_repository_lifecycle_observer(observer.clone()),
        );
        if install_first {
            assert_pending(install.as_mut());
            assert_pending(begin.as_mut());
        } else {
            assert_pending(begin.as_mut());
            assert_pending(install.as_mut());
        }
        drop(serial);
        if install_first {
            install.await.unwrap();
            let guard = begin.await.unwrap();
            assert!(probe.blocked(&keys[0]));
            guard.settle_confirmed();
            assert!(!probe.blocked(&keys[0]));
        } else {
            let guard = begin.await.unwrap();
            assert!(install.await.is_err());
            guard.settle_confirmed();
            f.store
                .install_repository_lifecycle_observer(observer)
                .await
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_observer_callback_does_not_hold_store_writer_or_install_lock() {
    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    let observer = probe.install(&f.store).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    *probe.callback.lock().unwrap() = Some((entered.clone(), blocked));
    let store = f.store.clone();
    let key = f.key();
    let task = tokio::spawn(async move { store.begin_repository_pending_delete(&[key]).await });
    entered.notified().await;
    let mut other = f.workspace.clone();
    other.id = WorkspaceId::from("independent");
    tokio::time::timeout(Duration::from_secs(3), f.store.insert_workspace(&other))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        f.store.install_repository_lifecycle_observer(observer),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(probe.blocked(&f.key()));
    release.send(()).unwrap();
    task.await.unwrap().unwrap().settle_confirmed();
    assert!(!probe.blocked(&f.key()));
}

#[tokio::test]
async fn observer_error_and_panic_never_confirm_a_started_attempt() {
    for panic in [false, true] {
        let f = Fixture::new(true).await;
        let probe = Arc::new(Probe::default());
        probe.install(&f.store).await;
        probe.panic_pending.store(panic, Ordering::SeqCst);
        probe.reject_pending.store(!panic, Ordering::SeqCst);
        let store = f.store.clone();
        let key = f.key();
        let task = tokio::spawn(async move { store.begin_repository_pending_delete(&[key]).await });
        match task.await {
            Err(error) => assert!(panic && error.is_panic()),
            Ok(result) => assert!(!panic && result.is_err()),
        }
        assert!(probe.blocked(&f.key()));
        assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
    }
}

#[tokio::test]
async fn aborted_unpolled_and_panicked_guard_owners_never_confirm() {
    for panic in [false, true] {
        let f = Fixture::new(true).await;
        let probe = Arc::new(Probe::default());
        probe.install(&f.store).await;
        let guard = f
            .store
            .begin_repository_pending_delete(&[f.key()])
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            assert!(!panic, "fixture worker panic");
            std::future::pending::<()>().await;
            guard.settle_confirmed();
        });
        if !panic {
            task.abort();
        }
        let error = task.await.unwrap_err();
        assert!(if panic {
            error.is_panic()
        } else {
            error.is_cancelled()
        });
        assert!(probe.blocked(&f.key()));
    }
}

#[tokio::test]
async fn empty_keys_and_exhausted_domain_reject_without_an_observer_callback() {
    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    assert!(f.store.begin_repository_pending_delete(&[]).await.is_err());
    // Exhaustion control only: this is not supplied authority or a generation.
    f.store
        .repository_lifecycle
        .state
        .lock()
        .unwrap()
        .active_writers = usize::MAX;
    assert!(f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .is_err());
    f.store
        .repository_lifecycle
        .state
        .lock()
        .unwrap()
        .active_writers = 0;
    assert_eq!(probe.state.lock().unwrap().reversible, 0);
}

#[tokio::test]
async fn replaced_original_database_cannot_begin_or_borrow_another_domain() {
    let f = Fixture::new(true).await;
    f.store.close().await;
    let path = f.dir.path().join("pending.db");
    let previous = f.dir.path().join("previous.db");
    std::fs::rename(&path, &previous).unwrap();
    std::fs::copy(&previous, &path).unwrap();
    assert!(Store::open(&path).await.is_err());
    assert!(f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .is_err());
    let other = Fixture::new(true).await;
    other
        .store
        .begin_repository_pending_delete(&[other.key()])
        .await
        .unwrap()
        .settle_confirmed();
    assert!(f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .is_err());
}

#[tokio::test]
async fn factual_absence_is_no_effect_and_preserves_draft() {
    let f = Fixture::new(false).await;
    f.draft().await;
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, NoEffect);
    assert!(matches!(outcome.result, Err(Error::NotFound(_))));
    assert_eq!(f.drafts().await, 1);
    assert_eq!(f.tombstones().await, 0);
}

#[tokio::test]
async fn factual_commit_removes_rows_tombstones_id_and_evicts_display_cache() {
    let f = Fixture::new(true).await;
    f.browser().await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    let guard = f
        .store
        .begin_repository_pending_delete(&[f.key()])
        .await
        .unwrap();
    assert_eq!(f.store.browser_tab_displayed.len(), 1);
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Committed);
    outcome.result.unwrap();
    assert!(matches!(
        f.store.get_workspace(&f.workspace.id).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(f.drafts().await, 0);
    assert_eq!(f.tombstones().await, 1);
    assert_eq!(f.store.browser_tab_displayed.len(), 0);
    assert!(f.store.get_browser_tab("tab").await.unwrap().is_none());
    assert!(
        probe.blocked(&f.key()),
        "Store completion is not pending-owner settlement"
    );
    guard.settle_confirmed();
    assert!(!probe.blocked(&f.key()));
}

#[tokio::test]
async fn late_zero_row_not_found_is_unknown_after_committed_cleanup() {
    let f = Fixture::new(true).await;
    f.draft().await;
    f.ignore_final_delete().await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Unknown);
    assert!(matches!(outcome.result, Err(Error::NotFound(_))));
    assert_eq!(f.drafts().await, 0);
    assert_eq!(f.tombstones().await, 0);
    assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
    assert!(probe.blocked(&f.key()));
}

#[tokio::test]
async fn real_batch_and_final_rollback_errors_remain_unknown() {
    for final_failure in [false, true] {
        let f = Fixture::new(true).await;
        f.draft().await;
        let sql = if final_failure {
            "CREATE TRIGGER reject_delete BEFORE INSERT ON deleted_workspace_id BEGIN SELECT RAISE(ABORT, 'fixture final failure'); END"
        } else {
            "CREATE TRIGGER reject_delete BEFORE DELETE ON draft BEGIN SELECT RAISE(ABORT, 'fixture batch failure'); END"
        };
        sqlx::query(sql)
            .execute(f.store.write_pool())
            .await
            .unwrap();
        let probe = Arc::new(Probe::default());
        probe.install(&f.store).await;
        let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
        assert_eq!(outcome.disposition, Unknown);
        assert!(
            matches!(outcome.result, Err(Error::Internal(message)) if message.contains("fixture"))
        );
        assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
        assert_eq!(f.drafts().await, i64::from(!final_failure));
        assert_eq!(f.tombstones().await, 0);
        assert!(probe.blocked(&f.key()));
    }
}

#[tokio::test]
async fn real_final_commit_rejection_is_unknown_not_absence() {
    let f = Fixture::new(true).await;
    f.draft().await;
    let deleting = Arc::new(AtomicBool::new(false));
    let seen = deleting.clone();
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    let mut handle = connection.lock_handle().await.unwrap();
    handle.set_update_hook(move |update| {
        if update.table == "workspace" {
            seen.store(true, Ordering::SeqCst);
        }
    });
    handle.set_commit_hook(move || !deleting.load(Ordering::SeqCst));
    drop(handle);
    drop(connection);
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Unknown);
    assert!(
        matches!(outcome.result, Err(Error::Internal(message)) if message.contains("commit failed"))
    );
    assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
    assert_eq!(f.drafts().await, 0);
    assert_eq!(f.tombstones().await, 0);
}

#[tokio::test]
async fn initial_read_failure_and_observer_rejection_do_not_invent_no_effect() {
    let f = Fixture::new(true).await;
    f.store.read_pool().close().await;
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Unknown);
    assert!(
        matches!(outcome.result, Err(Error::Internal(message)) if message.contains("check failed"))
    );

    let f = Fixture::new(true).await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    probe.reject_ordinary.store(true, Ordering::SeqCst);
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Unknown);
    assert!(
        matches!(outcome.result, Err(Error::Internal(message)) if message == "fixture ordinary rejection")
    );
    assert!(f.store.get_workspace(&f.workspace.id).await.is_ok());
}

#[tokio::test]
async fn acknowledged_commit_survives_later_ticket_settlement_panic() {
    let f = Fixture::new(true).await;
    f.browser().await;
    let probe = Arc::new(Probe::default());
    probe.install(&f.store).await;
    probe.panic_settlement.store(true, Ordering::SeqCst);
    let outcome = f.store.delete_workspace_with_outcome(&f.workspace.id).await;
    assert_eq!(outcome.disposition, Committed);
    assert!(
        matches!(outcome.result, Err(Error::Internal(message)) if message.contains("final task failed"))
    );
    assert_eq!(f.tombstones().await, 1);
    assert_eq!(f.store.browser_tab_displayed.len(), 0);
    assert!(
        probe.blocked(&f.key()),
        "a real commit does not fabricate ticket settlement"
    );
}

async fn cancelled_batch(installed: bool) {
    let f = Fixture::new(true).await;
    f.draft().await;
    let key = f.key();
    let probe = Arc::new(Probe::default());
    let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
    if installed {
        probe.install(&f.store).await;
    }
    let guard = f
        .store
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap();
    let pool = f.store.write_pool().clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == "draft" {
                if let Some(blocked) = blocked.take() {
                    notify.notify_one();
                    blocked.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            }
        });
    drop(connection);
    let store = f.store;
    let id = f.workspace.id;
    let task = tokio::spawn(async move {
        let outcome = store.delete_workspace_with_outcome(&id).await;
        if outcome.disposition == Committed {
            guard.settle_confirmed();
        }
        outcome
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let mut connection = pool.acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_update_hook();
    let drafts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM draft")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(
        drafts, 0,
        "the real SQLite worker completed after caller cancellation"
    );
    drop(connection);
    let reopened = Store::open(&f.dir.path().join("pending.db")).await.unwrap();
    if installed {
        assert!(reopened.has_repository_lifecycle_observer(&observer));
        reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .unwrap();
        assert!(probe.blocked(&key));
        reopened
            .begin_repository_pending_delete(std::slice::from_ref(&key))
            .await
            .unwrap()
            .settle_confirmed();
        assert!(probe.blocked(&key));
    } else {
        assert!(reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn cancelled_actual_batch_and_last_store_reopen_without_observer() {
    cancelled_batch(false).await;
}

#[tokio::test]
async fn cancelled_actual_batch_and_last_store_reopen_with_original_observer() {
    cancelled_batch(true).await;
}

#[tokio::test]
async fn cancelled_caller_does_not_cancel_original_final_commit_or_settle_pending_guard() {
    let f = Fixture::new(true).await;
    let key = f.key();
    let probe = Arc::new(Probe::default());
    let observer = probe.install(&f.store).await;
    let guard = f
        .store
        .begin_repository_pending_delete(std::slice::from_ref(&key))
        .await
        .unwrap();
    let pool = f.store.write_pool().clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let deleting = Arc::new(AtomicBool::new(false));
    let seen = deleting.clone();
    let mut connection = pool.acquire().await.unwrap();
    let mut handle = connection.lock_handle().await.unwrap();
    handle.set_update_hook(move |update| {
        if update.table == "workspace" {
            seen.store(true, Ordering::SeqCst);
        }
    });
    handle.set_commit_hook(move || {
        if deleting.load(Ordering::SeqCst) {
            if let Some(blocked) = blocked.take() {
                notify.notify_one();
                blocked.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }
        true
    });
    drop(handle);
    drop(connection);
    let store = f.store;
    let id = f.workspace.id;
    let task = tokio::spawn(async move {
        let outcome = store.delete_workspace_with_outcome(&id).await;
        if outcome.disposition == Committed {
            guard.settle_confirmed();
        }
        outcome
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.settled.notified())
        .await
        .unwrap();
    let reopened = Store::open(&f.dir.path().join("pending.db")).await.unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&observer));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace WHERE id='ws-pending-delete'")
            .fetch_one(reopened.read_pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        probe.state.lock().unwrap().pending.len(),
        1,
        "only the vanished pending owner remains"
    );
    assert!(probe.blocked(&key));
}
