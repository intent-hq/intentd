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
async fn registered_root_delete_recreate_is_atomic_with_workspace_invalidation() {
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
    assert!(!workspace.load(Ordering::SeqCst));
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
