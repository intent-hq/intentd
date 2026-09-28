//! Actual Services/Store/registry tests. Physical origin allocation is test-only;
//! retirement leaves, managed writers and disposable `SQLite` are the real modules.
use crate::repository_admission::lifecycle::{
    FixtureOriginOwner, RepositoryLifecycleRegistry, RepositorySourceLifetime,
    RepositorySubscription,
};
use crate::repository_admission::{AdmissionError, RepositoryRetirement};
use crate::tests::{workspace, TempDb, WorkspacesRoot};
use crate::Services;
use intent_core::caller::Caller;
use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceApi, WorkspaceId};
use intent_store::{RepositoryLifecycleKey, Store};
use std::sync::Arc;

struct Harness {
    db: TempDb,
    root: WorkspacesRoot,
    services: Services,
    registry: Arc<RepositoryLifecycleRegistry>,
    owner: FixtureOriginOwner,
    ws: WorkspaceId,
    agent: AgentId,
}
impl Harness {
    async fn new() -> Self {
        let db = TempDb::new();
        let root = WorkspacesRoot::new();
        let store = Store::open(&db.path).await.unwrap();
        let ws = WorkspaceId::new();
        let agent = AgentId::new();
        store.insert_workspace(&workspace(&ws)).await.unwrap();
        store
            .insert_agent_session(&session(&ws, &agent.0))
            .await
            .unwrap();
        let services = Services::new(store).with_workspaces_root(root.path().to_path_buf());
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&services.store).await.unwrap();
        let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
        Self {
            db,
            root,
            services,
            registry,
            owner,
            ws,
            agent,
        }
    }
    fn request(
        &self,
        agent: bool,
    ) -> Result<(RepositoryRetirement, RepositorySubscription), AdmissionError> {
        let leaf = RepositoryRetirement::default();
        let lifetime = RepositorySourceLifetime::new(
            self.registry.clone(),
            Some(self.owner.origin()),
            leaf.clone(),
        );
        let mut keys = vec![
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::Workspace(self.ws.clone()),
        ];
        if agent {
            keys.push(RepositoryLifecycleKey::Agent(self.agent.clone()));
        }
        let subscription = lifetime.subscribe(&self.services.store, &Caller::Daemon, &keys)?;
        Ok((leaf, subscription))
    }
}
fn session(ws: &WorkspaceId, id: &str) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: id.to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Idle,
        is_active: false,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[intent_test_macros::daemon_test]
async fn workspace_schedule_cancel_retires_original_requests() {
    let h = Harness::new().await;
    let (old, _subscription) = h.request(false).unwrap();
    h.services
        .schedule_workspace_delete(h.ws.clone(), 60_000)
        .await
        .unwrap();
    assert_eq!(old.check_current(), Err(AdmissionError::Retired));
    assert!(h.request(false).is_err());
    assert!(h
        .services
        .cancel_workspace_delete(h.ws.clone())
        .await
        .unwrap());
    assert_eq!(old.check_current(), Err(AdmissionError::Retired));
    let (fresh, _new) = h.request(false).unwrap();
    assert!(fresh.check_current().is_ok());
}

#[intent_test_macros::daemon_test]
async fn agent_schedule_cancel_retires_original_requests() {
    let h = Harness::new().await;
    let (old, _subscription) = h.request(true).unwrap();
    h.services
        .agent_schedule_delete_op(h.agent.clone(), Some(h.ws.clone()), 60_000)
        .await
        .unwrap();
    assert_eq!(old.check_current(), Err(AdmissionError::Retired));
    assert!(h.request(true).is_err());
    assert!(h
        .services
        .agent_cancel_delete_op(h.agent.clone(), Some(h.ws.clone()))
        .await
        .unwrap());
    assert_eq!(old.check_current(), Err(AdmissionError::Retired));
    let (fresh, _new) = h.request(true).unwrap();
    assert!(fresh.check_current().is_ok());
}

#[intent_test_macros::daemon_test]
async fn timer_claim_keeps_barrier_before_first_delete_sql() {
    let h = Harness::new().await;
    let (reached, resume) = h.services.workspace_delete_test_gate.arm(h.ws.clone());
    h.services
        .schedule_workspace_delete(h.ws.clone(), 0)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let blocked = h.request(false).is_err();
    resume.notify_one();
    assert!(
        blocked,
        "removing the pending marker cannot admit a fresh request before SQL"
    );
}

// Scheduling-only probes. Every paused path resumes the actual SQL/filesystem code.
#[derive(Clone, Default)]
pub(crate) struct DeletionGates(Arc<std::sync::Mutex<GateState>>);
#[derive(Default)]
struct GateState {
    pauses: std::collections::HashMap<
        (String, String),
        (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>),
    >,
    workers: std::collections::HashMap<
        String,
        (Arc<tokio::sync::Notify>, std::sync::mpsc::Receiver<()>),
    >,
    finished: std::collections::HashMap<String, Arc<tokio::sync::Notify>>,
    worker_finished: std::collections::HashMap<String, Arc<tokio::sync::Notify>>,
    aborts: std::collections::HashMap<String, tokio::task::AbortHandle>,
}
impl DeletionGates {
    fn arm(&self, phase: &str, key: &str) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let pair = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        self.0
            .lock()
            .unwrap()
            .pauses
            .insert((phase.into(), key.into()), pair.clone());
        pair
    }
    pub(crate) async fn pause(&self, phase: &str, key: &str) {
        let pair = self
            .0
            .lock()
            .unwrap()
            .pauses
            .remove(&(phase.into(), key.into()));
        if let Some((reached, resume)) = pair {
            reached.notify_one();
            resume.notified().await;
        }
    }
    fn arm_worker(
        &self,
        key: &str,
    ) -> (
        Arc<tokio::sync::Notify>,
        std::sync::mpsc::Sender<()>,
        Arc<tokio::sync::Notify>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        let reached = Arc::new(tokio::sync::Notify::new());
        let done = Arc::new(tokio::sync::Notify::new());
        let mut state = self.0.lock().unwrap();
        state.workers.insert(key.into(), (reached.clone(), rx));
        state.worker_finished.insert(key.into(), done.clone());
        (reached, tx, done)
    }
    pub(crate) fn pause_worker(&self, key: &str) {
        let pair = self.0.lock().unwrap().workers.remove(key);
        if let Some((reached, resume)) = pair {
            reached.notify_one();
            resume.recv().unwrap();
        }
    }
    pub(crate) fn worker_finished(&self, key: &str) {
        if let Some(done) = self.0.lock().unwrap().worker_finished.remove(key) {
            done.notify_one();
        }
    }
    fn completion(&self, key: &str) -> Arc<tokio::sync::Notify> {
        let done = Arc::new(tokio::sync::Notify::new());
        self.0
            .lock()
            .unwrap()
            .finished
            .insert(key.into(), done.clone());
        done
    }
    pub(crate) fn finished(&self, key: &str) {
        if let Some(done) = self.0.lock().unwrap().finished.remove(key) {
            done.notify_one();
        }
    }
    pub(crate) fn track(&self, key: &str, abort: tokio::task::AbortHandle) {
        self.0.lock().unwrap().aborts.insert(key.into(), abort);
    }
    async fn abort(&self, key: &str) {
        let abort = self.0.lock().unwrap().aborts.remove(key).unwrap();
        abort.abort();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
async fn reached(notify: &tokio::sync::Notify) {
    tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
}
fn subject(h: &Harness, agent: bool) -> crate::delete_grace::PendingDeleteSubject {
    if agent {
        crate::delete_grace::PendingDeleteSubject::Agent {
            workspace_id: h.ws.clone(),
            agent_id: h.agent.clone(),
        }
    } else {
        crate::delete_grace::PendingDeleteSubject::Workspace(h.ws.clone())
    }
}

#[intent_test_macros::daemon_test]
async fn duplicates_rearm_and_unrelated_workspace_keep_original_identity() {
    let h = Harness::new().await;
    let other = WorkspaceId::new();
    h.services
        .store
        .insert_workspace(&workspace(&other))
        .await
        .unwrap();
    let other_leaf = RepositoryRetirement::default();
    let lifetime = RepositorySourceLifetime::new(
        h.registry.clone(),
        Some(h.owner.origin()),
        other_leaf.clone(),
    );
    let _other = lifetime
        .subscribe(
            &h.services.store,
            &Caller::Daemon,
            &[
                RepositoryLifecycleKey::Database,
                RepositoryLifecycleKey::Workspace(other),
            ],
        )
        .unwrap();
    for agent in [false, true] {
        let first = h
            .services
            .pending_workspace_deletes
            .schedule_owned(subject(&h, agent), 60_000, |_| async {})
            .await
            .unwrap();
        // Both fields must address the very same typed allocation.
        let duplicate = h
            .services
            .pending_agent_deletes
            .schedule_owned(subject(&h, agent), 1, |_| async {
                panic!("duplicate callback")
            })
            .await
            .unwrap();
        assert_eq!(first.delete_at, duplicate.delete_at);
        assert!(!duplicate.newly_armed);
        assert!(h
            .services
            .pending_agent_deletes
            .cancel_owned(&subject(&h, agent))
            .await
            .unwrap());
        let (old, _old) = h.request(agent).unwrap();
        let rearmed = h
            .services
            .pending_workspace_deletes
            .schedule_owned(subject(&h, agent), 60_000, |_| async {})
            .await
            .unwrap();
        assert!(rearmed.newly_armed);
        assert_eq!(old.check_current(), Err(AdmissionError::Retired));
        h.services
            .pending_workspace_deletes
            .cancel_owned(&subject(&h, agent))
            .await
            .unwrap();
    }
    assert!(other_leaf.check_current().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_dispatch_holds_every_scheduling_waiter_and_writer_first_rejects_delivery() {
    use intent_core::with_caller;
    for agent in [false, true] {
        let h = Harness::new().await;
        let (leaf, _subscription) = h.request(agent).unwrap();
        let (started, start) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let dispatch_leaf = leaf.clone();
        let dispatch = std::thread::spawn(move || {
            dispatch_leaf.with_deletion_test_dispatch(|| {
                started.send(()).unwrap();
                released.recv().unwrap();
                Ok(19)
            })
        });
        start
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let services = h.services.clone();
        let ws = h.ws.clone();
        let id = h.agent.clone();
        let scheduled = tokio::spawn(with_caller(Caller::Daemon, async move {
            if agent {
                services
                    .agent_schedule_delete_op(id, Some(ws), 60_000)
                    .await
            } else {
                services.schedule_workspace_delete(ws, 60_000).await
            }
        }));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while h.request(agent).is_ok() {
            assert!(std::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        let services = h.services.clone();
        let ws = h.ws.clone();
        let id = h.agent.clone();
        let duplicate = tokio::spawn(with_caller(Caller::Daemon, async move {
            if agent {
                services
                    .agent_schedule_delete_op(id, Some(ws), 60_000)
                    .await
            } else {
                services.schedule_workspace_delete(ws, 60_000).await
            }
        }));
        assert!(!scheduled.is_finished());
        assert!(!duplicate.is_finished());
        release.send(()).unwrap();
        assert_eq!(dispatch.join().unwrap(), Ok(19));
        let first = scheduled.await.unwrap().unwrap();
        assert_eq!(duplicate.await.unwrap().unwrap(), first);
        assert_eq!(
            leaf.with_deletion_test_dispatch(|| Ok(20)),
            Err(AdmissionError::Retired)
        );
        h.services
            .pending_workspace_deletes
            .cancel_owned(&subject(&h, agent))
            .await
            .unwrap();
        let (fresh, _fresh) = h.request(agent).unwrap();
        assert_eq!(fresh.with_deletion_test_dispatch(|| Ok(21)), Ok(21));
    }
}

#[intent_test_macros::daemon_test]
async fn agent_zero_delay_claim_survives_cancel_and_store_failure() {
    let h = Harness::new().await;
    let (pause, resume) = h
        .services
        .pending_delete_test_gate
        .arm("agent-sql", &h.agent.0);
    sqlx::query("CREATE TRIGGER reject_owned_agent BEFORE DELETE ON agent_session BEGIN SELECT RAISE(ABORT, 'fixture failure'); END").execute(h.services.store.write_pool()).await.unwrap();
    let finished = h.services.pending_delete_test_gate.completion(&h.agent.0);
    h.services
        .agent_schedule_delete_op(h.agent.clone(), Some(h.ws.clone()), 0)
        .await
        .unwrap();
    reached(&pause).await;
    assert!(h.request(true).is_err());
    assert!(!h
        .services
        .agent_cancel_delete_op(h.agent.clone(), Some(h.ws.clone()))
        .await
        .unwrap());
    resume.notify_one();
    reached(&finished).await;
    h.services.store.get_agent_session(&h.agent).await.unwrap();
    assert!(h.request(true).is_err());
}

#[intent_test_macros::daemon_test]
async fn cascade_transfers_agent_claim_until_background_completion() {
    let h = Harness::new().await;
    h.services
        .agent_schedule_delete_op(h.agent.clone(), None, 60_000)
        .await
        .unwrap();
    let done = h.services.pending_delete_test_gate.completion(&h.ws.0);
    let (pause, resume) = h
        .services
        .pending_delete_test_gate
        .arm("background", &h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&pause).await;
    assert!(h.services.store.get_agent_session(&h.agent).await.is_err());
    assert!(h.request(true).is_err());
    assert!(!h
        .services
        .pending_agent_deletes
        .cancel_owned(&subject(&h, true))
        .await
        .unwrap());
    assert!(h.request(true).is_err());
    resume.notify_one();
    reached(&done).await;
    assert!(
        h.request(true).is_ok(),
        "only terminal original cleanup settles both claims"
    );
}

#[intent_test_macros::daemon_test]
async fn immediate_takes_original_timer_claim_and_responds_before_filesystem() {
    let h = Harness::new().await;
    let dir = h.root.path().join(&h.ws.0);
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("retained"), "bytes").unwrap();
    h.services
        .schedule_workspace_delete(h.ws.clone(), 60_000)
        .await
        .unwrap();
    let done = h.services.pending_delete_test_gate.completion(&h.ws.0);
    let (pause, resume) = h
        .services
        .pending_delete_test_gate
        .arm("background", &h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&pause).await;
    assert!(dir.exists());
    assert!(h.request(false).is_err());
    assert!(!h
        .services
        .cancel_workspace_delete(h.ws.clone())
        .await
        .unwrap());
    resume.notify_one();
    reached(&done).await;
    assert!(!dir.exists());
    assert!(h.request(false).is_ok());
}

#[intent_test_macros::daemon_test]
async fn failed_filesystem_remains_unknown_after_another_success_and_reopen() {
    let h = Harness::new().await;
    let dir = h.root.path().join(&h.ws.0);
    std::fs::write(&dir, "regular file cannot be removed as directory").unwrap();
    let done = h.services.pending_delete_test_gate.completion(&h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&done).await;
    assert!(dir.exists());
    assert!(h.request(false).is_err());
    std::fs::remove_file(&dir).unwrap();
    let again = h.services.pending_delete_test_gate.completion(&h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&again).await;
    assert!(
        h.request(false).is_err(),
        "success cannot settle the first unknown claim"
    );
    let reopened = Store::open(&h.db.path).await.unwrap();
    h.registry.install(&reopened).await.unwrap();
    let owner = FixtureOriginOwner::new(&h.registry, Caller::Daemon).unwrap();
    let source = RepositorySourceLifetime::new(
        h.registry.clone(),
        Some(owner.origin()),
        RepositoryRetirement::default(),
    );
    assert!(source
        .subscribe(
            &reopened,
            &Caller::Daemon,
            &[
                RepositoryLifecycleKey::Database,
                RepositoryLifecycleKey::Workspace(h.ws.clone())
            ]
        )
        .is_err());
}

#[intent_test_macros::daemon_test]
async fn canceled_background_with_late_real_filesystem_worker_never_confirms() {
    let h = Harness::new().await;
    let dir = h.root.path().join(&h.ws.0);
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("late"), "bytes").unwrap();
    let (pause, resume, worker_done) = h.services.pending_delete_test_gate.arm_worker(&h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&pause).await;
    h.services.pending_delete_test_gate.abort(&h.ws.0).await;
    assert!(h.request(false).is_err());
    resume.send(()).unwrap();
    reached(&worker_done).await;
    assert!(!dir.exists());
    assert!(h.request(false).is_err());
}

#[intent_test_macros::daemon_test]
async fn initial_absence_completes_but_late_store_failure_cannot_settle() {
    let h = Harness::new().await;
    let missing = WorkspaceId::new();
    let done = h.services.pending_delete_test_gate.completion(&missing.0);
    h.services.delete_workspace(missing.clone()).await.unwrap();
    reached(&done).await;
    let leaf = RepositoryRetirement::default();
    let source = RepositorySourceLifetime::new(h.registry.clone(), Some(h.owner.origin()), leaf);
    let _ok = source
        .subscribe(
            &h.services.store,
            &Caller::Daemon,
            &[
                RepositoryLifecycleKey::Database,
                RepositoryLifecycleKey::Workspace(missing),
            ],
        )
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_owned_workspace BEFORE DELETE ON workspace BEGIN SELECT RAISE(ABORT, 'fixture failure'); END").execute(h.services.store.write_pool()).await.unwrap();
    assert!(h.services.delete_workspace(h.ws.clone()).await.is_err());
    assert!(h.request(false).is_err());
    assert!(h.services.store.get_workspace(&h.ws).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn no_observer_schedule_blocks_first_install_and_cancellation_confirms_only_original() {
    let db = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let services = Services::new(store.clone()).with_workspaces_root(root.path().to_path_buf());
    services
        .schedule_workspace_delete(ws.clone(), 60_000)
        .await
        .unwrap();
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    assert!(registry.install(&store).await.is_err());
    assert!(services.cancel_workspace_delete(ws.clone()).await.unwrap());
    registry.install(&store).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn display_mismatch_omits_deadline_without_canceling_or_rebinding() {
    let h = Harness::new().await;
    let other = WorkspaceId::new();
    h.services
        .store
        .insert_workspace(&workspace(&other))
        .await
        .unwrap();
    h.services
        .agent_schedule_delete_op(h.agent.clone(), Some(h.ws.clone()), 60_000)
        .await
        .unwrap();
    // Unmanaged SQL is fixture setup for an observed identity mismatch, not writer coverage.
    sqlx::query("UPDATE agent_session SET workspace_id = ? WHERE id = ?")
        .bind(&other.0)
        .bind(&h.agent.0)
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    let displayed = h
        .services
        .agent_get_op(h.agent.clone(), Some(other.clone()))
        .await
        .unwrap();
    assert!(displayed.pending_delete_at.is_none());
    assert!(h
        .services
        .agent_get_session_op(h.agent.clone())
        .await
        .is_err());
    assert!(h
        .services
        .agent_cancel_delete_op(h.agent.clone(), Some(other))
        .await
        .is_err());
    assert!(h.request(true).is_err());
    assert!(h
        .services
        .pending_agent_deletes
        .deadline(&subject(&h, true))
        .unwrap()
        .is_some());
    h.services
        .pending_agent_deletes
        .cancel_owned(&subject(&h, true))
        .await
        .unwrap();
}

pub(crate) struct DeletionCompletion {
    gates: DeletionGates,
    key: String,
}
impl Drop for DeletionCompletion {
    fn drop(&mut self) {
        self.gates.finished(&self.key);
    }
}
impl DeletionGates {
    pub(crate) fn observe_completion(&self, key: &str) -> DeletionCompletion {
        DeletionCompletion {
            gates: self.clone(),
            key: key.into(),
        }
    }
}

#[intent_test_macros::daemon_test]
async fn real_git_worktree_cleanup_keeps_claim_until_detach_and_removal_complete() {
    let h = Harness::new().await;
    let repo_path = h.root.path().join("origin");
    let repo = git2::Repository::init(&repo_path).unwrap();
    let tree_id = repo.index().unwrap().write_tree().unwrap();
    let tree = repo.find_tree(tree_id).unwrap();
    let sig = git2::Signature::now("Fixture", "fixture@example.test").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[])
        .unwrap();
    let checkout = h.root.path().join(&h.ws.0).join("checkout");
    std::fs::create_dir_all(checkout.parent().unwrap()).unwrap();
    repo.worktree("owned-topic", &checkout, None).unwrap();
    std::fs::write(checkout.join("untracked"), "original bytes").unwrap();
    let mut ws = h.services.store.get_workspace(&h.ws).await.unwrap();
    ws.repository_path = Some(repo_path.to_string_lossy().into_owned());
    ws.worktree_path = Some(checkout.to_string_lossy().into_owned());
    ws.branch = "owned-topic".into();
    h.services.store.update_workspace(&ws).await.unwrap();
    let done = h.services.pending_delete_test_gate.completion(&h.ws.0);
    let (pause, resume) = h
        .services
        .pending_delete_test_gate
        .arm("background", &h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&pause).await;
    assert!(checkout.exists());
    assert!(h.request(false).is_err());
    resume.notify_one();
    reached(&done).await;
    assert!(!checkout.exists());
    assert!(repo.find_worktree("owned-topic").is_err());
    assert!(h.request(false).is_ok());
}

#[intent_test_macros::daemon_test]
async fn late_zero_row_store_not_found_is_public_success_but_never_confirmed() {
    let h = Harness::new().await;
    sqlx::query("CREATE TRIGGER skip_owned_workspace BEFORE DELETE ON workspace BEGIN SELECT RAISE(IGNORE); END").execute(h.services.store.write_pool()).await.unwrap();
    let done = h.services.pending_delete_test_gate.completion(&h.ws.0);
    h.services.delete_workspace(h.ws.clone()).await.unwrap();
    reached(&done).await;
    assert!(h.services.store.get_workspace(&h.ws).await.is_ok());
    assert!(h.request(false).is_err());
}

#[intent_test_macros::daemon_test]
async fn original_agent_metadata_error_keeps_legacy_ack_without_confirming_claim() {
    let h = Harness::new().await;
    // Corrupt only the disposable fixture's full transcript read; summary validation
    // still establishes the schedule's original workspace/Agent selectors.
    sqlx::query("ALTER TABLE agent_message RENAME TO missing_original_transcript")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    assert!(h.services.store.get_agent_session(&h.agent).await.is_err());
    let finished = h.services.pending_delete_test_gate.completion(&h.agent.0);
    h.services
        .agent_schedule_delete_op(h.agent.clone(), Some(h.ws.clone()), 0)
        .await
        .unwrap();
    reached(&finished).await;
    assert!(h
        .services
        .store
        .get_agent_session_summary(&h.agent)
        .await
        .is_ok());
    assert!(h.request(true).is_err());
    assert_eq!(
        h.services
            .agent_delete_op(h.agent.clone(), None)
            .await
            .unwrap(),
        serde_json::json!({"success":true})
    );
    assert!(
        h.request(true).is_err(),
        "metadata failure cannot take or settle somebody else's claim"
    );
}

#[intent_test_macros::daemon_test]
async fn unobserved_claimed_cancellation_survives_last_services_store_drop_and_reopen() {
    let db = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let services = Services::new(store).with_workspaces_root(root.path().to_path_buf());
    let (pause, resume) = services.pending_delete_test_gate.arm("background", &ws.0);
    services.delete_workspace(ws.clone()).await.unwrap();
    reached(&pause).await;
    services.pending_delete_test_gate.abort(&ws.0).await;
    resume.notify_one();
    drop(services);
    let reopened = Store::open(&db.path).await.unwrap();
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    assert!(
        registry.install(&reopened).await.is_err(),
        "unconfirmed pre-observer owner survives all handles"
    );
}
