//! Explicit observer fixture: this proves the Store boundary, not R admission.

use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::{AgentSession, Workspace};

use super::*;

type Leaf = Arc<AtomicBool>;

#[derive(Default)]
struct ProbeState {
    pending: HashMap<RepositoryLifecycleKey, usize>,
    leaves: Vec<(Vec<RepositoryLifecycleKey>, Leaf)>,
    starts: usize,
}

#[derive(Default)]
struct Probe {
    state: Arc<Mutex<ProbeState>>,
    reject: AtomicBool,
    started: tokio::sync::Notify,
}

impl Probe {
    fn capture(&self, mut keys: Vec<RepositoryLifecycleKey>) -> Option<Leaf> {
        keys.push(RepositoryLifecycleKey::Database);
        let mut state = self.state.lock().unwrap();
        if keys.iter().any(|key| state.pending.contains_key(key)) {
            return None;
        }
        let leaf = Arc::new(AtomicBool::new(true));
        state.leaves.push((keys, leaf.clone()));
        Some(leaf)
    }

    fn starts(&self) -> usize {
        self.state.lock().unwrap().starts
    }
}

struct Ticket {
    state: Arc<Mutex<ProbeState>>,
    keys: Vec<RepositoryLifecycleKey>,
}

impl RepositoryLifecycleMutationTicket for Ticket {
    fn settle_confirmed(self: Box<Self>) {
        let mut state = self.state.lock().unwrap();
        for key in &self.keys {
            let remaining = state.pending.get_mut(key).unwrap();
            *remaining -= 1;
            if *remaining == 0 {
                state.pending.remove(key);
            }
        }
    }
}

impl RepositoryLifecycleObserver for Probe {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(Error::Internal(
                "test observer refused before effect".into(),
            ));
        }
        let mut state = self.state.lock().unwrap();
        state.starts += 1;
        for key in keys {
            *state.pending.entry(key.clone()).or_default() += 1;
        }
        for (captured, leaf) in &state.leaves {
            if keys.iter().any(|key| captured.contains(key)) {
                leaf.store(false, Ordering::SeqCst);
            }
        }
        self.started.notify_one();
        Ok(Box::new(Ticket {
            state: self.state.clone(),
            keys: keys.to_vec(),
        }))
    }
}

struct Fixture {
    store: Store,
    workspace: Workspace,
    agent: AgentSession,
    probe: Arc<Probe>,
    observer: Arc<dyn RepositoryLifecycleObserver>,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("lifecycle.db")).await.unwrap();
        let workspace: Workspace = serde_json::from_value(serde_json::json!({
            "id":"ws-lifecycle", "title":"Lifecycle", "branch":"main", "status":"Active",
            "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
            "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
        }))
        .unwrap();
        store.insert_workspace(&workspace).await.unwrap();
        let agent: AgentSession = serde_json::from_value(serde_json::json!({
            "id":"agent-lifecycle", "workspaceId":workspace.id, "name":"Lifecycle", "status":"idle",
            "createdAt":"same-time", "updatedAt":"same-time", "skipAutoCommit":false,
            "harnessVersion":intent_core::CURRENT_HARNESS_VERSION
        }))
        .unwrap();
        store.insert_agent_session(&agent).await.unwrap();
        let probe = Arc::new(Probe::default());
        let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
        store
            .install_repository_lifecycle_observer(observer.clone())
            .await
            .unwrap();
        Self {
            store,
            workspace,
            agent,
            probe,
            observer,
            dir,
        }
    }

    fn agent_key(&self) -> RepositoryLifecycleKey {
        RepositoryLifecycleKey::Agent(self.agent.id.clone())
    }

    fn workspace_key(&self) -> RepositoryLifecycleKey {
        RepositoryLifecycleKey::Workspace(self.workspace.id.clone())
    }
}

#[tokio::test]
async fn real_model_aba_retires_old_leaf_and_allows_only_fresh_capture() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "A", Some("p"), "same-time")
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let a = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "B", Some("p"), "same-time")
        .await
        .unwrap();
    f.store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "A", Some("p"), "same-time")
        .await
        .unwrap();
    assert!(!a.load(Ordering::SeqCst));
    assert!(f.probe.capture(vec![f.agent_key()]).is_some());
}

#[tokio::test]
async fn observer_refusal_precedes_real_sql() {
    let f = Fixture::new().await;
    f.probe.reject.store(true, Ordering::SeqCst);
    assert!(f
        .store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "changed", None, "same-time")
        .await
        .is_err());
    assert_eq!(
        f.store
            .get_agent_session_summary(&f.agent.id)
            .await
            .unwrap()
            .model,
        None
    );
}

#[tokio::test]
async fn managed_reopen_and_clone_share_the_installed_owner() {
    let f = Fixture::new().await;
    assert!(f
        .store
        .clone()
        .has_repository_lifecycle_observer(&f.observer));
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    let reopened = Store::open(&f.dir.path().join("lifecycle.db"))
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    assert!(reopened.has_repository_lifecycle_observer(&f.observer));
    let current = f.probe.capture(vec![f.agent_key()]).unwrap();
    reopened
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "new", None, "same-time")
        .await
        .unwrap();
    assert!(!current.load(Ordering::SeqCst));
    let other: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(!reopened.has_repository_lifecycle_observer(&other));
    assert!(reopened
        .install_repository_lifecycle_observer(other)
        .await
        .is_err());
}

#[tokio::test]
async fn real_workspace_branch_aba_and_archive_restore_retire_original() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.workspace_key()]).unwrap();
    f.store
        .update_workspace_with_branch(&f.workspace, Some("other"))
        .await
        .unwrap();
    f.store
        .update_workspace_with_branch(&f.workspace, Some("main"))
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let active = f.probe.capture(vec![f.workspace_key()]).unwrap();
    f.store
        .archive_workspace_detaching_guests(&f.workspace.id, "same-time")
        .await
        .unwrap();
    f.store
        .unarchive_workspace_if_archived(&f.workspace.id, "same-time")
        .await
        .unwrap();
    assert!(!active.load(Ordering::SeqCst));
}

#[tokio::test]
async fn metadata_and_known_noops_do_not_retire_requests() {
    let f = Fixture::new().await;
    let old = f
        .probe
        .capture(vec![f.agent_key(), f.workspace_key()])
        .unwrap();
    let mut agent = f.agent.clone();
    agent.name = "Renamed".into();
    f.store
        .update_agent_session(&f.workspace.id, &agent)
        .await
        .unwrap();
    let mut workspace = f.workspace.clone();
    workspace.title = "Renamed".into();
    f.store.update_workspace(&workspace).await.unwrap();
    assert!(!f
        .store
        .set_agent_session_retired_at(&f.workspace.id, &f.agent.id, None, "same-time")
        .await
        .unwrap());
    assert!(!f
        .store
        .unarchive_workspace_if_archived(&f.workspace.id, "same-time")
        .await
        .unwrap());
    assert!(!f
        .store
        .rehome_agent_session_provider(
            &f.workspace.id,
            &f.agent.id,
            Some("absent"),
            "other",
            None,
            "same-time"
        )
        .await
        .unwrap());
    assert!(old.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), 0);
}

#[tokio::test]
async fn real_agent_retire_restore_delete_recreate_and_acp_aba() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .set_agent_session_retired_at(&f.workspace.id, &f.agent.id, Some("same-time"), "same-time")
        .await
        .unwrap();
    f.store
        .set_agent_session_retired_at(&f.workspace.id, &f.agent.id, None, "same-time")
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .replace_acp_session_id(&f.workspace.id, &f.agent.id, "A", "B")
        .await
        .unwrap();
    f.store
        .replace_acp_session_id(&f.workspace.id, &f.agent.id, "B", "A")
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    assert!(f
        .store
        .delete_agent_session(&f.workspace.id, &f.agent.id)
        .await
        .unwrap());
    f.store
        .insert_agent_session_with_messages(&f.agent, &[])
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.capture(vec![f.agent_key()]).is_some());
}

#[tokio::test]
async fn full_row_execution_binding_edits_retire_but_idle_transitions_do_not() {
    let f = Fixture::new().await;
    let mut agent = f.agent.clone();
    for change in 0..5 {
        let old = f.probe.capture(vec![f.agent_key()]).unwrap();
        match change {
            0 => agent.backend_session_id = Some(AgentId::new()),
            1 => agent.parent_agent_id = Some(AgentId::new()),
            2 => agent.sandbox_id = Some("sandbox".into()),
            3 => agent.sandbox_path = Some("root".into()),
            _ => agent.sandbox_branch = Some("other".into()),
        }
        f.store
            .update_agent_session(&f.workspace.id, &agent)
            .await
            .unwrap();
        assert!(!old.load(Ordering::SeqCst), "binding {change}");
    }
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    f.store
        .set_agent_session_status(
            &f.workspace.id,
            &f.agent.id,
            intent_core::AgentStatus::Active,
            true,
            "same-time",
            None,
        )
        .await
        .unwrap();
    f.store
        .set_agent_session_status(
            &f.workspace.id,
            &f.agent.id,
            intent_core::AgentStatus::Idle,
            false,
            "same-time",
            None,
        )
        .await
        .unwrap();
    assert!(old.load(Ordering::SeqCst));
    f.store
        .set_agent_session_status(
            &f.workspace.id,
            &f.agent.id,
            intent_core::AgentStatus::Deleted,
            false,
            "same-time",
            None,
        )
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    agent.status = intent_core::AgentStatus::Idle;
    f.store
        .update_agent_session(&f.workspace.id, &agent)
        .await
        .unwrap();
    assert!(!old.load(Ordering::SeqCst));
}

#[tokio::test]
async fn real_workspace_delete_cascade_and_same_id_recreation_retires_both() {
    let f = Fixture::new().await;
    let old = f
        .probe
        .capture(vec![f.workspace_key(), f.agent_key()])
        .unwrap();
    f.store.delete_workspace(&f.workspace.id).await.unwrap();
    f.store.insert_workspace(&f.workspace).await.unwrap();
    f.store.insert_agent_session(&f.agent).await.unwrap();
    assert!(!old.load(Ordering::SeqCst));
    assert!(f
        .probe
        .capture(vec![f.workspace_key(), f.agent_key()])
        .is_some());
}

#[tokio::test]
async fn registered_root_delete_recreate_is_atomic_with_precise_invalidation() {
    let f = Fixture::new().await;
    let root: intent_core::WorkspaceGitRoot = serde_json::from_value(serde_json::json!({
        "id":"gitroot-lifecycle", "workspaceId":f.workspace.id,
        "path":f.dir.path().to_string_lossy(), "source":"agent",
        "createdAt":"same-time", "updatedAt":"same-time"
    }))
    .unwrap();
    let key = RepositoryLifecycleKey::GitRoot(root.id.clone());
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let old = f.probe.capture(vec![key.clone()]).unwrap();
    let workspace = f.probe.capture(vec![f.workspace_key()]).unwrap();
    let mut metadata = root.clone();
    metadata.repo_name = Some("renamed metadata".into());
    f.store.upsert_workspace_git_root(&metadata).await.unwrap();
    assert!(old.load(Ordering::SeqCst));
    f.store.delete_workspace_git_root(&root.id).await.unwrap();
    assert!(!old.load(Ordering::SeqCst));
    assert!(workspace.load(Ordering::SeqCst));
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    assert!(f.probe.capture(vec![key]).is_some());
}

#[tokio::test]
async fn rejection_blocks_all_registered_root_keys_before_any_insert() {
    let f = Fixture::new().await;
    let root: intent_core::WorkspaceGitRoot = serde_json::from_value(serde_json::json!({
        "id":"gitroot-lifecycle", "workspaceId":f.workspace.id,
        "path":f.dir.path().to_string_lossy(), "source":"agent",
        "createdAt":"same-time", "updatedAt":"same-time"
    }))
    .unwrap();
    let before = f.probe.capture(vec![f.workspace_key()]).unwrap();
    f.probe.reject.store(true, Ordering::SeqCst);
    assert!(f.store.upsert_workspace_git_root(&root).await.is_err());
    assert!(f.store.get_workspace_git_root(&root.id).await.is_err());
    assert!(before.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), 0);
}

#[tokio::test]
async fn managed_open_before_install_shares_owner_after_install() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let first = Store::open(&path).await.unwrap();
    let second = Store::open(&path).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    first
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    assert!(second.has_repository_lifecycle_observer(&observer));
    assert!(second
        .install_repository_lifecycle_observer(observer)
        .await
        .is_ok());
}

#[tokio::test]
async fn managed_symlink_open_shares_the_same_file_domain() {
    let f = Fixture::new().await;
    let alias = f.dir.path().join("alias.db");
    std::os::unix::fs::symlink(f.dir.path().join("lifecycle.db"), &alias).unwrap();
    let reopened = Store::open(&alias).await.unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&f.observer));
}

#[tokio::test]
async fn overlapping_owned_tickets_cannot_settle_each_other() {
    let f = Fixture::new().await;
    let mut first = f.store.repository_lifecycle_write().await.unwrap();
    first.begin(&[f.agent_key(), f.workspace_key()]).unwrap();
    first.release_serialization();
    let mut second = f.store.clone().repository_lifecycle_write().await.unwrap();
    second.begin(&[f.agent_key()]).unwrap();
    first.settle();
    assert!(f.probe.capture(vec![f.workspace_key()]).is_some());
    assert!(f.probe.capture(vec![f.agent_key()]).is_none());
    second.settle();
    assert!(f.probe.capture(vec![f.agent_key()]).is_some());
}

#[tokio::test]
async fn dropping_original_owner_keeps_every_mutated_key_blocked() {
    let f = Fixture::new().await;
    let old = f
        .probe
        .capture(vec![f.agent_key(), f.workspace_key()])
        .unwrap();
    let mut owner = f.store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[f.agent_key(), f.workspace_key()]).unwrap();
    drop(owner);
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.capture(vec![f.agent_key()]).is_none());
    assert!(f.probe.capture(vec![f.workspace_key()]).is_none());
}

#[tokio::test]
async fn installation_cannot_reclassify_an_unconfirmed_unobserved_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[RepositoryLifecycleKey::Database]).unwrap();
    owner.release_serialization();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .is_err());
    drop(owner);
    assert!(store
        .install_repository_lifecycle_observer(observer)
        .await
        .is_err());
}

#[tokio::test]
async fn installation_waits_for_preexisting_confirmed_writer() {
    use std::future::Future;
    use std::task::Poll;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[RepositoryLifecycleKey::Database]).unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    let mut install = Box::pin(store.install_repository_lifecycle_observer(observer.clone()));
    std::future::poll_fn(|cx| {
        assert!(install.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(!store.has_repository_lifecycle_observer(&observer));
    owner.settle();
    install.await.unwrap();
    assert!(store.has_repository_lifecycle_observer(&observer));
}

#[tokio::test]
async fn real_writer_cancellation_does_not_settle_its_retirement() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    let connection = f.store.write_pool().acquire().await.unwrap();
    let store = f.store.clone();
    let workspace = f.workspace.id.clone();
    let agent = f.agent.id.clone();
    let task = tokio::spawn(async move {
        store
            .set_agent_session_model(&workspace, &agent, "blocked", None, "same-time")
            .await
    });
    f.probe.started.notified().await;
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.capture(vec![f.agent_key()]).is_none());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(connection);
    assert!(f.probe.capture(vec![f.agent_key()]).is_none());
    assert_eq!(
        f.store
            .get_agent_session_summary(&f.agent.id)
            .await
            .unwrap()
            .model,
        None
    );
}

#[tokio::test]
async fn post_barrier_sql_failure_never_revives_original_requests() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER reject_lifecycle_model BEFORE UPDATE OF model ON agent_session WHEN NEW.model='rejected' BEGIN SELECT RAISE(ABORT,'fixture rejection'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    assert!(f
        .store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "rejected", None, "same-time")
        .await
        .is_err());
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(
        f.store
            .get_agent_session_summary(&f.agent.id)
            .await
            .unwrap()
            .model,
        None
    );
    // The wrapper cannot claim confirmation from an opaque returned error.
    assert!(f.probe.capture(vec![f.agent_key()]).is_none());
}

#[tokio::test]
async fn pre_barrier_scope_failure_and_lost_acp_cas_preserve_requests() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    let starts = f.probe.starts();
    assert!(f
        .store
        .set_agent_session_model(&WorkspaceId::new(), &f.agent.id, "other", None, "same-time")
        .await
        .is_err());
    assert_eq!(
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent.id, "not-A", "B")
            .await
            .unwrap(),
        "A"
    );
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    assert!(old.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), starts);
}

#[tokio::test]
async fn confirmed_no_effect_ticket_allows_fresh_but_never_revives_old() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    let mut owner = f.store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[f.agent_key()]).unwrap();
    // The actual original owner confirms this transaction's rollback.
    let mut tx = f.store.write_pool().begin().await.unwrap();
    sqlx::query("UPDATE agent_session SET model='temporary' WHERE id=?")
        .bind(&f.agent.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    owner.settle();
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.capture(vec![f.agent_key()]).is_some());
    assert_eq!(
        f.store
            .get_agent_session_summary(&f.agent.id)
            .await
            .unwrap()
            .model,
        None
    );
}

#[tokio::test]
async fn managed_reopen_rejects_replacement_of_a_live_database_incarnation() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.workspace_key()]).unwrap();
    f.store.close().await;
    let path = f.dir.path().join("lifecycle.db");
    let saved = f.dir.path().join("previous.db");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::copy(&saved, &path).unwrap();
    assert!(Store::open(&path).await.is_err());
    assert!(!old.load(Ordering::SeqCst));
    assert!(!f.store.has_repository_lifecycle_observer(&f.observer));
    assert!(f
        .store
        .install_repository_lifecycle_observer(f.observer.clone())
        .await
        .is_err());
    assert!(f.probe.capture(vec![f.workspace_key()]).is_none());
}

#[tokio::test]
async fn file_identity_alias_joins_existing_domain_without_a_second_owner() {
    let f = Fixture::new().await;
    let alias = f.dir.path().join("hardlink.db");
    std::fs::hard_link(f.dir.path().join("lifecycle.db"), &alias).unwrap();
    // This tests identity only: SQLite WAL sidecar compatibility through hard
    // links is not asserted, and no pool is opened on the alias.
    let same = domain_for(&alias).unwrap();
    assert!(Arc::ptr_eq(&same, &f.store.repository_lifecycle));
}

#[tokio::test]
async fn real_provider_rehome_and_workspace_root_binding_aba() {
    let f = Fixture::new().await;
    f.store
        .set_agent_session_model(
            &f.workspace.id,
            &f.agent.id,
            "model",
            Some("A"),
            "same-time",
        )
        .await
        .unwrap();
    let old = f.probe.capture(vec![f.agent_key()]).unwrap();
    assert!(f
        .store
        .rehome_agent_session_provider(
            &f.workspace.id,
            &f.agent.id,
            Some("A"),
            "B",
            Some("model"),
            "same-time"
        )
        .await
        .unwrap());
    assert!(f
        .store
        .rehome_agent_session_provider(
            &f.workspace.id,
            &f.agent.id,
            Some("B"),
            "A",
            Some("model"),
            "same-time"
        )
        .await
        .unwrap());
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.workspace_key()]).unwrap();
    let mut workspace = f.workspace.clone();
    workspace.worktree_path = Some(f.dir.path().join("worktree").to_string_lossy().into());
    f.store.update_workspace(&workspace).await.unwrap();
    f.store.update_workspace(&f.workspace).await.unwrap();
    assert!(!old.load(Ordering::SeqCst));
}

#[tokio::test]
async fn branch_reconcile_winning_change_retires_and_known_cas_loss_preserves() {
    let f = Fixture::new().await;
    let old = f.probe.capture(vec![f.workspace_key()]).unwrap();
    assert!(f
        .store
        .reconcile_workspace_branch(&f.workspace, "other")
        .await
        .unwrap());
    assert!(!old.load(Ordering::SeqCst));
    let old = f.probe.capture(vec![f.workspace_key()]).unwrap();
    let starts = f.probe.starts();
    assert!(!f
        .store
        .reconcile_workspace_branch(&f.workspace, "main")
        .await
        .unwrap());
    assert!(old.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), starts);
}

#[tokio::test]
async fn unconfirmed_owner_survives_last_store_drop_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[RepositoryLifecycleKey::Database]).unwrap();
    drop(owner);
    drop(store);

    let reopened = Store::open(&path).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(
        reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .is_err(),
        "last-handle drop must not erase an unconfirmed writer"
    );
}

#[tokio::test]
async fn installed_observer_survives_last_store_drop_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let probe = Arc::new(Probe::default());
    let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
    store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[RepositoryLifecycleKey::Database]).unwrap();
    drop(owner);
    drop(store);

    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&observer));
    assert!(probe
        .capture(vec![RepositoryLifecycleKey::Database])
        .is_none());
    let replacement: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(reopened
        .install_repository_lifecycle_observer(replacement)
        .await
        .is_err());
}

#[tokio::test]
async fn confirmed_unobserved_domain_can_be_reclaimed_after_last_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let domain = Arc::downgrade(&store.repository_lifecycle);
    let mut owner = store.repository_lifecycle_write().await.unwrap();
    owner.begin(&[RepositoryLifecycleKey::Database]).unwrap();
    owner.settle();
    drop(store);
    assert!(domain.upgrade().is_none());

    let reopened = Store::open(&path).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    reopened
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&observer));
}

#[tokio::test]
async fn queued_sqlite_write_outlives_last_owner_without_resetting_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let workspace: Workspace = serde_json::from_value(serde_json::json!({
        "id":"ws-pending", "title":"Pending", "branch":"main", "status":"Active",
        "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
        "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
    }))
    .unwrap();
    let workspace_id = workspace.id.clone();
    let pool = store.write_pool().clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == "workspace" {
                if let Some(blocked) = blocked.take() {
                    notify.notify_one();
                    blocked
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
            }
        });
    drop(connection);

    // Only the request owns Store; the pool/SQLite worker carries no domain Arc.
    let task = tokio::spawn(async move { store.insert_workspace(&workspace).await });
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // Release the actual in-flight SQLite write after its caller and last Store
    // disappeared. Drain its connection before opening the next managed Store.
    release.send(()).unwrap();
    let mut connection = pool.acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_update_hook();
    drop(connection);

    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened.get_workspace(&workspace_id).await.is_ok());
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Probe::default());
    assert!(
        reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .is_err(),
        "a later observed commit does not settle the vanished original owner"
    );
}

#[tokio::test]
async fn retired_file_incarnation_survives_last_store_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    store.close().await;
    let previous = dir.path().join("previous.db");
    std::fs::rename(&path, &previous).unwrap();
    std::fs::copy(&previous, &path).unwrap();
    assert!(Store::open(&path).await.is_err());
    drop(store);
    assert!(
        Store::open(&path).await.is_err(),
        "dropping the last Store must not erase a retired live incarnation"
    );
}

fn precise_root(f: &Fixture, id: &str, path: &str) -> intent_core::WorkspaceGitRoot {
    serde_json::from_value(serde_json::json!({
        "id":id,"workspaceId":f.workspace.id,"path":path,"source":"agent",
        "createdAt":"same-time","updatedAt":"same-time"
    }))
    .unwrap()
}

fn precise_selection(root: &intent_core::WorkspaceGitRoot) -> RepositoryLifecycleKey {
    RepositoryLifecycleKey::Selection {
        workspace_id: root.workspace_id.clone(),
        git_root_id: Some(root.id.clone()),
    }
}

#[tokio::test]
async fn precise_registration_insert_preserves_workspace_and_retires_its_selection() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "precise-root", "/registered");
    let workspace = f.probe.capture(vec![f.workspace_key()]).unwrap();
    let selection = f.probe.capture(vec![precise_selection(&root)]).unwrap();
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    assert_eq!(
        (
            workspace.load(Ordering::SeqCst),
            selection.load(Ordering::SeqCst)
        ),
        (true, false)
    );
}

#[tokio::test]
async fn precise_registration_delete_preserves_workspace_and_retires_its_selection() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "precise-root", "/registered");
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let workspace = f.probe.capture(vec![f.workspace_key()]).unwrap();
    let selection = f.probe.capture(vec![precise_selection(&root)]).unwrap();
    f.store.delete_workspace_git_root(&root.id).await.unwrap();
    assert_eq!(
        (
            workspace.load(Ordering::SeqCst),
            selection.load(Ordering::SeqCst)
        ),
        (true, false)
    );
}

fn precise_keys(root: &intent_core::WorkspaceGitRoot) -> Vec<RepositoryLifecycleKey> {
    vec![
        RepositoryLifecycleKey::RootInventory(root.workspace_id.clone()),
        RepositoryLifecycleKey::GitRoot(root.id.clone()),
        precise_selection(root),
    ]
}

#[tokio::test]
async fn precise_registration_mutates_only_inventory_root_and_registered_choice() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "precise-root", "/registered");
    let other = precise_root(&f, "other-root", "/other");
    let primary = RepositoryLifecycleKey::Selection {
        workspace_id: f.workspace.id.clone(),
        git_root_id: None,
    };
    for insert in [true, false] {
        let matching: Vec<_> = precise_keys(&root)
            .into_iter()
            .map(|key| f.probe.capture(vec![key]).unwrap())
            .collect();
        let unrelated = f
            .probe
            .capture(vec![
                f.workspace_key(),
                f.agent_key(),
                primary.clone(),
                RepositoryLifecycleKey::GitRoot(other.id.clone()),
                precise_selection(&other),
                RepositoryLifecycleKey::RootInventory(WorkspaceId::from("elsewhere")),
            ])
            .unwrap();
        if insert {
            f.store.upsert_workspace_git_root(&root).await.unwrap();
        } else {
            f.store.delete_workspace_git_root(&root.id).await.unwrap();
        }
        assert!(matching.iter().all(|leaf| !leaf.load(Ordering::SeqCst)));
        assert!(unrelated.load(Ordering::SeqCst));
        assert!(f.probe.capture(precise_keys(&root)).is_some());
    }
}

#[tokio::test]
async fn precise_registration_metadata_and_missing_delete_preserve_original_coverage() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "original-root", "/registered");
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let current = f.probe.capture(precise_keys(&root)).unwrap();
    let starts = f.probe.starts();
    let mut metadata = root.clone();
    metadata.id = intent_core::WorkspaceGitRootId::from("submitted-other-id");
    metadata.repo_name = Some("ordinary metadata".into());
    let (stored, inserted) = f.store.upsert_workspace_git_root(&metadata).await.unwrap();
    assert!(!inserted);
    assert_eq!(stored.id, root.id);
    assert_eq!(stored.workspace_id, root.workspace_id);
    assert_eq!(stored.path, root.path);
    f.store.update_workspace_git_root_pr(&stored).await.unwrap();
    assert!(f
        .store
        .delete_workspace_git_root(&metadata.id)
        .await
        .is_err());
    assert!(current.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), starts);
}

#[tokio::test]
async fn precise_registration_competitors_invalidate_only_the_actual_inserted_identity() {
    let f = Fixture::new().await;
    let independent = Store::open(&f.dir.path().join("lifecycle.db"))
        .await
        .unwrap();
    let a = precise_root(&f, "candidate-a", "/same");
    let b = precise_root(&f, "candidate-b", "/same");
    let a_leaf = f.probe.capture(vec![precise_selection(&a)]).unwrap();
    let b_leaf = f.probe.capture(vec![precise_selection(&b)]).unwrap();
    let inventory = f
        .probe
        .capture(vec![RepositoryLifecycleKey::RootInventory(
            f.workspace.id.clone(),
        )])
        .unwrap();
    let starts = f.probe.starts();
    let (left, right) = tokio::join!(
        f.store.upsert_workspace_git_root(&a),
        independent.upsert_workspace_git_root(&b)
    );
    let (left, inserted_left) = left.unwrap();
    let (right, inserted_right) = right.unwrap();
    assert_ne!(inserted_left, inserted_right);
    assert_eq!(left.id, right.id);
    assert_eq!(f.probe.starts(), starts + 1);
    assert!(!inventory.load(Ordering::SeqCst));
    assert_eq!(a_leaf.load(Ordering::SeqCst), left.id != a.id);
    assert_eq!(b_leaf.load(Ordering::SeqCst), left.id != b.id);
}

#[tokio::test]
async fn precise_registration_recreation_keeps_tombstone_and_other_choices() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "same-id", "/same");
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let primary = intent_core::RepositoryRootId {
        workspace_id: f.workspace.id.clone(),
        kind: intent_core::RepositoryRootKind::Primary,
    };
    let registered = intent_core::RepositoryRootId {
        workspace_id: f.workspace.id.clone(),
        kind: intent_core::RepositoryRootKind::Registered {
            git_root_id: root.id.clone(),
        },
    };
    let primary_snapshot = f
        .store
        .repository_selection_snapshot(&primary)
        .await
        .unwrap();
    f.store
        .reset_repository_selection(&primary_snapshot)
        .await
        .result
        .unwrap();
    let primary_snapshot = f
        .store
        .repository_selection_snapshot(&primary)
        .await
        .unwrap();
    let original = f
        .store
        .repository_selection_snapshot(&registered)
        .await
        .unwrap();
    f.store
        .write_repository_selection(
            &original,
            crate::RepositorySelectionChange::ExplicitRemote {
                remote_name: "old".into(),
            },
        )
        .await
        .result
        .unwrap();
    let original = f
        .store
        .repository_selection_snapshot(&registered)
        .await
        .unwrap();
    f.store.delete_workspace_git_root(&root.id).await.unwrap();
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let current = f
        .store
        .repository_selection_snapshot(&registered)
        .await
        .unwrap();
    assert!(current.root_incarnation().unwrap().get() > original.root_incarnation().unwrap().get());
    assert!(matches!(
        current.selection(),
        Some(crate::RepositoryStoredSelection::Saved(
            intent_core::SavedReviewSelection::UnresolvedHistorical { .. }
        ))
    ));
    assert!(matches!(
        f.store.reset_repository_selection(&original).await.result,
        Ok(crate::RepositorySelectionWriteResult::Conflict(_))
    ));
    let after = f
        .store
        .repository_selection_snapshot(&primary)
        .await
        .unwrap();
    assert_eq!(after.selection(), primary_snapshot.selection());
    assert_eq!(
        after.selection_revision(),
        primary_snapshot.selection_revision()
    );
    assert_eq!(
        after.root_incarnation(),
        primary_snapshot.root_incarnation()
    );
}

#[tokio::test]
async fn precise_registration_failed_sql_keeps_only_original_unknown_keys() {
    let f = Fixture::new().await;
    let root = precise_root(&f, "same-id", "/original");
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let mut collision = root.clone();
    collision.path = "/collision".into();
    let original = f.probe.capture(precise_keys(&root)).unwrap();
    let unrelated = f
        .probe
        .capture(vec![f.workspace_key(), f.agent_key()])
        .unwrap();
    assert!(f.store.upsert_workspace_git_root(&collision).await.is_err());
    assert_eq!(
        f.store.get_workspace_git_root(&root.id).await.unwrap().path,
        root.path
    );
    assert!(!original.load(Ordering::SeqCst));
    assert!(unrelated.load(Ordering::SeqCst));
    assert!(f.probe.capture(precise_keys(&root)).is_none());
    let other = precise_root(&f, "another", "/another");
    f.store.upsert_workspace_git_root(&other).await.unwrap();
    assert!(
        f.probe.capture(precise_keys(&root)).is_none(),
        "another known completion cannot settle the collision"
    );
    assert!(f
        .probe
        .capture(vec![RepositoryLifecycleKey::GitRoot(other.id)])
        .is_some());
}

async fn precise_canceled_delete(installed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let mut workspace = intent_core::chief_workspace();
    workspace.id = WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let root: intent_core::WorkspaceGitRoot = serde_json::from_value(serde_json::json!({
        "id":"canceled-root","workspaceId":workspace.id,"path":"/root","source":"agent",
        "createdAt":"same-time","updatedAt":"same-time"
    }))
    .unwrap();
    store.upsert_workspace_git_root(&root).await.unwrap();
    let probe = Arc::new(Probe::default());
    let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
    if installed {
        store
            .install_repository_lifecycle_observer(observer.clone())
            .await
            .unwrap();
    }
    let pool = store.write_pool().clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == "workspace_git_root" {
                if let Some(blocked) = blocked.take() {
                    notify.notify_one();
                    blocked
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
            }
        });
    drop(connection);
    let id = root.id.clone();
    let worker = tokio::spawn(async move { store.delete_workspace_git_root(&id).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let mut connection = pool.acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_update_hook();
    drop(connection);
    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened.get_workspace_git_root(&root.id).await.is_err());
    if installed {
        assert!(reopened.has_repository_lifecycle_observer(&observer));
        assert!(probe.capture(precise_keys(&root)).is_none());
        reopened.upsert_workspace_git_root(&root).await.unwrap();
        assert!(probe.capture(precise_keys(&root)).is_none());
    } else {
        assert!(reopened
            .install_repository_lifecycle_observer(observer)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn precise_registration_canceled_sqlite_delete_retains_installed_unknown_on_reopen() {
    precise_canceled_delete(true).await;
}

#[tokio::test]
async fn precise_registration_canceled_sqlite_delete_blocks_first_observer_on_reopen() {
    precise_canceled_delete(false).await;
}
