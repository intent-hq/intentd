//! Real Store fixtures with an explicit owner observer, not production R proof.

use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::{AgentSession, Workspace};

use super::*;
use crate::{RepositoryLifecycleKey, RepositoryLifecycleMutationTicket};
use std::sync::Mutex;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Installing,
    Confirmed,
    Retired,
}

struct Attempt {
    binding: RepositoryInitializationBinding,
    phase: Mutex<Phase>,
    loaded: AtomicBool,
}

#[derive(Default)]
struct State {
    attempts: Vec<std::sync::Weak<Attempt>>,
    leaves: Vec<Arc<AtomicBool>>,
    blocked: usize,
    starts: usize,
}

#[derive(Default)]
struct Observer {
    state: Arc<Mutex<State>>,
    reject_completion: AtomicBool,
}

struct Proof {
    owner: std::sync::Weak<Observer>,
    attempt: Arc<Attempt>,
    handed_off: bool,
}

impl Drop for Proof {
    fn drop(&mut self) {
        if !self.handed_off {
            *self.attempt.phase.lock().unwrap() = Phase::Retired;
        }
    }
}

struct Completion {
    owner: std::sync::Weak<Observer>,
    attempt: Arc<Attempt>,
}

struct InitTicket {
    owner: Arc<Observer>,
    attempt: Arc<Attempt>,
    settled: bool,
}

impl Drop for InitTicket {
    fn drop(&mut self) {
        if !self.settled {
            *self.attempt.phase.lock().unwrap() = Phase::Retired;
        }
    }
}

impl RepositoryInitializationTicket for InitTicket {
    fn finish_confirmed(mut self: Box<Self>) -> Result<Box<dyn Any + Send>> {
        let mut state = self.owner.state.lock().unwrap();
        let mut phase = self.attempt.phase.lock().unwrap();
        if *phase != Phase::Installing || self.owner.reject_completion.load(Ordering::SeqCst) {
            *phase = Phase::Retired;
            state.blocked -= 1;
            self.settled = true;
            return Err(lifecycle_error("fixture original owner retired"));
        }
        *phase = Phase::Confirmed;
        state.blocked -= 1;
        self.settled = true;
        Ok(Box::new(Completion {
            owner: Arc::downgrade(&self.owner),
            attempt: self.attempt.clone(),
        }))
    }

    fn settle_no_effect(mut self: Box<Self>) {
        self.owner.state.lock().unwrap().blocked -= 1;
        *self.attempt.phase.lock().unwrap() = Phase::Retired;
        self.settled = true;
    }

    fn settle_committed_without_confirmation(mut self: Box<Self>) -> Result<()> {
        self.owner.state.lock().unwrap().blocked -= 1;
        *self.attempt.phase.lock().unwrap() = Phase::Retired;
        self.settled = true;
        Ok(())
    }
}

struct Mutation(Arc<Mutex<State>>);
impl RepositoryLifecycleMutationTicket for Mutation {
    fn settle_confirmed(self: Box<Self>) {
        self.0.lock().unwrap().blocked -= 1;
    }
}

impl Observer {
    fn invalidate(state: &mut State, original: Option<&Arc<Attempt>>) {
        for attempt in state.attempts.iter().filter_map(std::sync::Weak::upgrade) {
            if !original.is_some_and(|original| Arc::ptr_eq(original, &attempt)) {
                *attempt.phase.lock().unwrap() = Phase::Retired;
            }
        }
        for leaf in &state.leaves {
            leaf.store(false, Ordering::SeqCst);
        }
    }

    fn proof(self: &Arc<Self>, binding: RepositoryInitializationBinding) -> Proof {
        let attempt = Arc::new(Attempt {
            loaded: AtomicBool::new(matches!(
                &binding.action,
                RepositoryAcpInitialization::Loaded { .. }
            )),
            binding,
            phase: Mutex::new(Phase::Pending),
        });
        self.state
            .lock()
            .unwrap()
            .attempts
            .push(Arc::downgrade(&attempt));
        Proof {
            owner: Arc::downgrade(self),
            attempt,
            handed_off: false,
        }
    }

    fn leaf(&self) -> Arc<AtomicBool> {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.blocked, 0);
        let leaf = Arc::new(AtomicBool::new(true));
        state.leaves.push(leaf.clone());
        leaf
    }

    fn consume(self: &Arc<Self>, proof: Box<dyn Any + Send>) -> bool {
        let Ok(proof) = proof.downcast::<Completion>() else {
            return false;
        };
        let state = self.state.lock().unwrap();
        let mut phase = proof.attempt.phase.lock().unwrap();
        let valid = proof
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, self))
            && *phase == Phase::Confirmed
            && state.blocked == 0;
        *phase = Phase::Retired;
        valid
    }
}

impl RepositoryLifecycleObserver for Observer {
    fn begin_mutation(
        &self,
        _keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        let mut state = self.state.lock().unwrap();
        Self::invalidate(&mut state, None);
        state.blocked += 1;
        Ok(Box::new(Mutation(self.state.clone())))
    }

    fn begin_initialization(
        &self,
        original_owner: Box<dyn Any + Send>,
        binding: &RepositoryInitializationBinding,
    ) -> Result<Box<dyn RepositoryInitializationTicket>> {
        let mut proof = original_owner
            .downcast::<Proof>()
            .map_err(|_| lifecycle_error("fixture invalid proof"))?;
        let owner = proof
            .owner
            .upgrade()
            .ok_or_else(|| lifecycle_error("fixture owner gone"))?;
        if !std::ptr::eq(self, Arc::as_ptr(&owner)) || proof.attempt.binding != *binding {
            return Err(lifecycle_error("fixture wrong original owner"));
        }
        let mut state = self.state.lock().unwrap();
        if state.blocked != 0
            || *proof.attempt.phase.lock().unwrap() != Phase::Pending
            || (matches!(binding.action, RepositoryAcpInitialization::Loaded { .. })
                && !proof.attempt.loaded.load(Ordering::SeqCst))
        {
            return Err(lifecycle_error("fixture unavailable attempt"));
        }
        Self::invalidate(&mut state, Some(&proof.attempt));
        *proof.attempt.phase.lock().unwrap() = Phase::Installing;
        state.blocked += 1;
        state.starts += 1;
        proof.handed_off = true;
        Ok(Box::new(InitTicket {
            owner,
            attempt: proof.attempt.clone(),
            settled: false,
        }))
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
    workspace: Workspace,
    agent: AgentSession,
    owner: Arc<Observer>,
    observer: Arc<dyn RepositoryLifecycleObserver>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("initialization.db"))
            .await
            .unwrap();
        let workspace: Workspace = serde_json::from_value(serde_json::json!({
            "id":"ws-init", "title":"Initialization", "branch":"main", "status":"Active",
            "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
            "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
        }))
        .unwrap();
        store.insert_workspace(&workspace).await.unwrap();
        let agent: AgentSession = serde_json::from_value(serde_json::json!({
            "id":"agent-init", "workspaceId":workspace.id, "name":"Initialization", "status":"idle",
            "createdAt":"same-time", "updatedAt":"same-time", "skipAutoCommit":false,
            "harnessVersion":intent_core::CURRENT_HARNESS_VERSION
        }))
        .unwrap();
        store.insert_agent_session(&agent).await.unwrap();
        let owner = Arc::new(Observer::default());
        let observer: Arc<dyn RepositoryLifecycleObserver> = owner.clone();
        store
            .install_repository_lifecycle_observer(observer.clone())
            .await
            .unwrap();
        Self {
            dir,
            store,
            workspace,
            agent,
            owner,
            observer,
        }
    }

    fn binding(&self, action: RepositoryAcpInitialization) -> RepositoryInitializationBinding {
        RepositoryInitializationBinding {
            workspace_id: self.workspace.id.clone(),
            agent_id: self.agent.id.clone(),
            action,
        }
    }

    fn claim(&self, binding: &RepositoryInitializationBinding) -> RepositoryInitializationClaim {
        self.store
            .bind_repository_initialization_claim(
                self.observer.clone(),
                Box::new(self.owner.proof(binding.clone())),
            )
            .unwrap()
    }

    async fn initialize(
        &self,
        action: RepositoryAcpInitialization,
    ) -> Result<RepositoryInitializationConfirmation> {
        let binding = self.binding(action);
        self.store
            .initialize_repository_acp_session(self.claim(&binding), binding)
            .await
    }

    fn consume(&self, confirmation: RepositoryInitializationConfirmation) -> bool {
        let (_, proof) = self
            .store
            .consume_repository_initialization_confirmation(&self.observer, confirmation)
            .unwrap();
        self.owner.consume(proof)
    }

    async fn id(&self) -> Option<String> {
        self.store
            .get_agent_session(&self.agent.id)
            .await
            .unwrap()
            .acp_session_id
    }
}

fn first(id: &str) -> RepositoryAcpInitialization {
    RepositoryAcpInitialization::FirstSet {
        session_id: id.into(),
    }
}
fn loaded(id: &str) -> RepositoryAcpInitialization {
    RepositoryAcpInitialization::Loaded {
        session_id: id.into(),
    }
}
fn replace(expected: Option<&str>, id: &str) -> RepositoryAcpInitialization {
    RepositoryAcpInitialization::Replace {
        expected: expected.map(str::to_owned),
        session_id: id.into(),
    }
}

#[tokio::test]
async fn first_set_confirms_only_actual_winner_and_retires_live_and_competing_owner() {
    let f = Fixture::new().await;
    let old = f.owner.leaf();
    let competing = f.binding(first("A"));
    let stale = f.claim(&competing);
    let confirmation = f.initialize(first("A")).await.unwrap();
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert!(f.consume(confirmation));
    assert!(f
        .store
        .initialize_repository_acp_session(stale, competing)
        .await
        .is_err());
    assert!(f.initialize(first("A")).await.is_err());
    assert!(f.initialize(first("B")).await.is_err());
    assert_eq!(f.owner.state.lock().unwrap().starts, 1);
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    assert_eq!(
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent.id, "stale", "B")
            .await
            .unwrap(),
        "A"
    );
}

#[tokio::test]
async fn loaded_confirms_original_load_and_retires_old_origin_without_changing_row() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    let old = f.owner.leaf();
    assert!(f.consume(f.initialize(loaded("A")).await.unwrap()));
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert!(f.initialize(loaded("wrong")).await.is_err());
}

#[tokio::test]
async fn forged_retired_and_nonload_proofs_cannot_confirm() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    let binding = f.binding(loaded("A"));
    let forged = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new("A"))
        .unwrap();
    assert!(f
        .store
        .initialize_repository_acp_session(forged, binding.clone())
        .await
        .is_err());
    let proof = f.owner.proof(binding.clone());
    proof.attempt.loaded.store(false, Ordering::SeqCst);
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
        .unwrap();
    assert!(f
        .store
        .initialize_repository_acp_session(claim, binding.clone())
        .await
        .is_err());
    let stale = f.claim(&binding);
    f.store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "changed", None, "same-time")
        .await
        .unwrap();
    assert!(f
        .store
        .initialize_repository_acp_session(stale, binding)
        .await
        .is_err());
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

#[tokio::test]
async fn wrong_workspace_agent_empty_session_missing_row_and_same_value_replace_do_not_confirm() {
    let f = Fixture::new().await;
    for binding in [
        RepositoryInitializationBinding {
            workspace_id: WorkspaceId("other".into()),
            ..f.binding(first("A"))
        },
        RepositoryInitializationBinding {
            agent_id: AgentId("missing".into()),
            ..f.binding(first("A"))
        },
        f.binding(first("")),
        f.binding(loaded("")),
        f.binding(replace(None, "")),
    ] {
        assert!(f
            .store
            .initialize_repository_acp_session(f.claim(&binding), binding)
            .await
            .is_err());
    }
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    assert!(f.initialize(replace(Some("A"), "A")).await.is_err());
    assert!(f.initialize(replace(None, "B")).await.is_err());
    assert!(f.initialize(replace(Some("old"), "B")).await.is_err());
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

#[tokio::test]
async fn captured_binding_cannot_be_retargeted_or_replayed() {
    let f = Fixture::new().await;
    let a = f.binding(first("A"));
    assert!(f
        .store
        .initialize_repository_acp_session(f.claim(&a), f.binding(first("B")))
        .await
        .is_err());
    let proof = f.owner.proof(a.clone());
    let replay = Proof {
        owner: proof.owner.clone(),
        attempt: proof.attempt.clone(),
        handed_off: false,
    };
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
        .unwrap();
    let confirmation = f
        .store
        .initialize_repository_acp_session(claim, a.clone())
        .await
        .unwrap();
    assert!(f.consume(confirmation));
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(replay))
        .unwrap();
    assert!(f
        .store
        .initialize_repository_acp_session(claim, a)
        .await
        .is_err());
}

#[tokio::test]
async fn clone_and_managed_open_share_domain_but_other_database_or_observer_cannot_use_proof() {
    let f = Fixture::new().await;
    let g = Fixture::new().await;
    let binding = f.binding(first("A"));
    assert!(g
        .store
        .initialize_repository_acp_session(f.claim(&binding), binding.clone())
        .await
        .is_err());
    assert!(f
        .store
        .bind_repository_initialization_claim(
            g.observer.clone(),
            Box::new(g.owner.proof(binding.clone()))
        )
        .is_err());
    let reopened = Store::open(&f.dir.path().join("initialization.db"))
        .await
        .unwrap();
    let confirmation = reopened
        .clone()
        .initialize_repository_acp_session(f.claim(&binding), binding)
        .await
        .unwrap();
    assert!(g
        .store
        .consume_repository_initialization_confirmation(&g.observer, confirmation)
        .is_err());
    let confirmation = f.initialize(loaded("A")).await.unwrap();
    assert!(f
        .store
        .consume_repository_initialization_confirmation(&g.observer, confirmation)
        .is_err());
    assert!(f.consume(f.initialize(loaded("A")).await.unwrap()));
}

#[tokio::test]
async fn committed_confirmation_remains_rejectable_after_intervening_real_mutation() {
    let f = Fixture::new().await;
    let confirmation = f.initialize(first("A")).await.unwrap();
    f.store
        .set_agent_session_model(&f.workspace.id, &f.agent.id, "changed", None, "same-time")
        .await
        .unwrap();
    assert!(!f.consume(confirmation));
    assert_eq!(f.id().await.as_deref(), Some("A"));
}

#[tokio::test]
async fn rejected_completion_preserves_committed_effect_without_false_sql_uncertainty() {
    let f = Fixture::new().await;
    f.owner.reject_completion.store(true, Ordering::SeqCst);
    assert!(f.initialize(first("A")).await.is_err());
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
}

#[tokio::test]
async fn replacement_banks_actual_snapshot_once_and_first_set_and_load_leave_it_untouched() {
    let f = Fixture::new().await;
    let totals = intent_core::TokenUsageTotals {
        input_tokens: 10,
        output_tokens: 7,
        cache_read_tokens: 3,
        cache_creation_tokens: 2,
        thought_tokens: 1,
        cost: None,
    };
    f.store
        .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
        .await
        .unwrap();
    assert!(f.consume(f.initialize(first("A")).await.unwrap()));
    assert!(f.consume(f.initialize(loaded("A")).await.unwrap()));
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert_eq!(rows[0].2.as_ref(), Some(&totals));
    assert!(rows[0].3.is_none());
    assert!(f.consume(f.initialize(replace(Some("A"), "B")).await.unwrap()));
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert!(rows[0].2.is_none());
    assert_eq!(rows[0].3.as_ref(), Some(&totals));
    assert!(f.initialize(replace(Some("A"), "C")).await.is_err());
    assert_eq!(f.id().await.as_deref(), Some("B"));
    assert_eq!(
        f.store
            .get_workspace_agent_usage_data(&f.workspace.id)
            .await
            .unwrap()[0]
            .3
            .as_ref(),
        Some(&totals)
    );

    let g = Fixture::new().await;
    g.store
        .set_agent_session_token_usage(&g.workspace.id, &g.agent.id, &totals)
        .await
        .unwrap();
    assert!(g.consume(g.initialize(replace(None, "initial")).await.unwrap()));
    let rows = g
        .store
        .get_workspace_agent_usage_data(&g.workspace.id)
        .await
        .unwrap();
    assert!(rows[0].2.is_none());
    assert_eq!(rows[0].3.as_ref(), Some(&totals));
}

#[tokio::test]
async fn ignored_update_returns_no_proof_or_positive_rollback_receipt() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER ignore_init BEFORE UPDATE OF acp_session_id ON agent_session BEGIN SELECT RAISE(IGNORE); END")
        .execute(f.store.write_pool()).await.unwrap();
    let old = f.owner.leaf();
    assert!(f.initialize(first("A")).await.is_err());
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(f.id().await, None);
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    sqlx::query("DROP TRIGGER ignore_init")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f.initialize(first("A")).await.is_err());
}

#[tokio::test]
async fn body_error_and_failed_rollback_never_confirm_or_release_unknown_owner() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER abort_init AFTER UPDATE OF acp_session_id ON agent_session BEGIN SELECT RAISE(ROLLBACK,'initialization rollback'); END")
        .execute(f.store.write_pool()).await.unwrap();
    assert!(f.initialize(first("A")).await.is_err());
    assert_eq!(f.id().await, None);
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    // The original rollback guard poisons/discards the connection after SQLite
    // already rolled the transaction back. Pool recovery does not settle proof.
    sqlx::query("DROP TRIGGER abort_init")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f.initialize(first("A")).await.is_err());
}

#[tokio::test]
async fn default_observer_denies_initialization_without_affecting_ordinary_setter() {
    struct Ordinary(Arc<Mutex<State>>);
    impl RepositoryLifecycleObserver for Ordinary {
        fn begin_mutation(
            &self,
            _keys: &[RepositoryLifecycleKey],
        ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
            self.0.lock().unwrap().blocked += 1;
            Ok(Box::new(Mutation(self.0.clone())))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("ordinary.db")).await.unwrap();
    let seed = Fixture::new().await;
    store.insert_workspace(&seed.workspace).await.unwrap();
    store.insert_agent_session(&seed.agent).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Ordinary(Arc::default()));
    assert!(store
        .bind_repository_initialization_claim(observer.clone(), Box::new(()))
        .is_err());
    store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    let claim = store
        .bind_repository_initialization_claim(observer, Box::new(()))
        .unwrap();
    assert!(store
        .initialize_repository_acp_session(claim, seed.binding(first("A")))
        .await
        .is_err());
    store
        .set_acp_session_id(&seed.workspace.id, &seed.agent.id, "A")
        .await
        .unwrap();
}

#[tokio::test]
async fn managed_reopen_retires_pending_claim_and_committed_confirmation() {
    let f = Fixture::new().await;
    let binding = f.binding(first("A"));
    let pending = f.claim(&binding);
    let reopened = Store::open(&f.dir.path().join("initialization.db"))
        .await
        .unwrap();
    assert!(reopened
        .initialize_repository_acp_session(pending, binding)
        .await
        .is_err());
    let confirmation = f.initialize(first("A")).await.unwrap();
    let _other = Store::open(&f.dir.path().join("initialization.db"))
        .await
        .unwrap();
    assert!(!f.consume(confirmation));
}

#[tokio::test]
async fn retired_database_incarnation_rejects_confirmation() {
    let f = Fixture::new().await;
    let confirmation = f.initialize(first("A")).await.unwrap();
    f.store.close().await;
    let path = f.dir.path().join("initialization.db");
    let old = f.dir.path().join("old.db");
    std::fs::rename(&path, &old).unwrap();
    std::fs::copy(&old, &path).unwrap();
    assert!(Store::open(&path).await.is_err());
    assert!(f
        .store
        .consume_repository_initialization_confirmation(&f.observer, confirmation)
        .is_err());
}

#[tokio::test]
async fn canceled_commit_worker_and_last_store_drop_cannot_reset_unknown_initialization() {
    let f = Fixture::new().await;
    let binding = f.binding(first("A"));
    let claim = f.claim(&binding);
    let pool = f.store.write_pool().clone();
    let path = f.dir.path().join("initialization.db");
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(blocked) = blocked.take() {
                signal.notify_one();
                blocked
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .unwrap();
            }
            true
        });
    drop(connection);
    let store = f.store;
    let task = tokio::spawn(async move {
        store
            .initialize_repository_acp_session(claim, binding)
            .await
    });
    entered.notified().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    release.send(()).unwrap();
    // The actual SQLite COMMIT finishes after its owner disappeared. Draining
    // the same worker observes the effect without inventing owner settlement.
    let mut connection = pool.acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(|| true);
    let stored: Option<String> =
        sqlx::query_scalar("SELECT acp_session_id FROM agent_session WHERE id=?")
            .bind(&f.agent.id.0)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert_eq!(stored.as_deref(), Some("A"));
    drop(connection);
    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened.has_repository_lifecycle_observer(&f.observer));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    let replacement: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Observer::default());
    assert!(reopened
        .install_repository_lifecycle_observer(replacement)
        .await
        .is_err());
    let binding = RepositoryInitializationBinding {
        workspace_id: f.workspace.id,
        agent_id: f.agent.id,
        action: loaded("A"),
    };
    let claim = reopened
        .bind_repository_initialization_claim(f.observer, Box::new(f.owner.proof(binding.clone())))
        .unwrap();
    assert!(reopened
        .initialize_repository_acp_session(claim, binding)
        .await
        .is_err());
}

#[tokio::test]
async fn rejected_sqlite_commit_returns_no_proof_and_keeps_original_barrier() {
    let f = Fixture::new().await;
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(|| false);
    drop(connection);
    assert!(f.initialize(first("A")).await.is_err());
    assert_eq!(f.id().await, None);
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
}

#[tokio::test]
async fn ignored_update_cannot_commit_trigger_effects_as_confirmed_no_effect() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TRIGGER ignore_with_effect BEFORE UPDATE OF acp_session_id ON agent_session BEGIN UPDATE agent_session SET name='side effect' WHERE id=OLD.id; SELECT RAISE(IGNORE); END")
        .execute(f.store.write_pool()).await.unwrap();
    assert!(f.initialize(first("A")).await.is_err());
    assert_eq!(
        f.store.get_agent_session(&f.agent.id).await.unwrap().name,
        f.agent.name
    );
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
}

#[tokio::test]
async fn trigger_changed_winner_cannot_return_original_initialization_proof() {
    for initial in [false, true] {
        let f = Fixture::new().await;
        if initial {
            f.store
                .set_acp_session_id(&f.workspace.id, &f.agent.id, "old")
                .await
                .unwrap();
        }
        sqlx::query("CREATE TRIGGER change_winner AFTER UPDATE OF acp_session_id ON agent_session WHEN NEW.acp_session_id='A' BEGIN UPDATE agent_session SET acp_session_id='other' WHERE id=NEW.id; END")
            .execute(f.store.write_pool()).await.unwrap();
        let action = if initial {
            replace(Some("old"), "A")
        } else {
            first("A")
        };
        assert!(
            f.initialize(action).await.is_err(),
            "an affected-row count does not confirm the actual stored winner"
        );
        assert_eq!(
            f.id().await.as_deref(),
            if initial { Some("old") } else { None }
        );
        assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    }
}

#[tokio::test]
async fn loaded_proof_is_one_use_even_when_the_row_still_matches() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    let binding = f.binding(loaded("A"));
    let proof = f.owner.proof(binding.clone());
    let replay = Proof {
        owner: proof.owner.clone(),
        attempt: proof.attempt.clone(),
        handed_off: false,
    };
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
        .unwrap();
    let confirmation = f
        .store
        .initialize_repository_acp_session(claim, binding.clone())
        .await
        .unwrap();
    assert!(f.consume(confirmation));
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(replay))
        .unwrap();
    assert!(f
        .store
        .initialize_repository_acp_session(claim, binding)
        .await
        .is_err());
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(f.owner.state.lock().unwrap().starts, 1);
}

#[tokio::test]
async fn same_observer_on_different_database_does_not_bridge_claim_domain() {
    let f = Fixture::new().await;
    let other = Store::open(&f.dir.path().join("different.db"))
        .await
        .unwrap();
    other.insert_workspace(&f.workspace).await.unwrap();
    other.insert_agent_session(&f.agent).await.unwrap();
    other
        .install_repository_lifecycle_observer(f.observer.clone())
        .await
        .unwrap();
    let binding = f.binding(first("A"));
    assert!(other
        .initialize_repository_acp_session(f.claim(&binding), binding)
        .await
        .is_err());
    assert_eq!(f.id().await, None);
    assert_eq!(
        other
            .get_agent_session(&f.agent.id)
            .await
            .unwrap()
            .acp_session_id,
        None
    );
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

#[tokio::test]
async fn shared_legacy_body_keeps_canonical_and_missing_row_fallback_without_claiming_winner() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "canonical")
        .await
        .unwrap();
    for (agent, expected, canonical) in [
        (f.agent.id.clone(), Some("stale"), "canonical"),
        (AgentId("absent".into()), Some("stale"), "fresh"),
        (AgentId("absent".into()), None, "fresh"),
    ] {
        let mut conn = f.store.write_pool().acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .unwrap();
        let outcome = Store::write_acp_session_id_in_transaction(
            &mut conn,
            &f.workspace.id,
            &agent,
            expected,
            "fresh",
            || panic!("no changing existing row"),
        )
        .await;
        let outcome = crate::commit_with_rollback_guard(conn, outcome, "fixture commit")
            .await
            .unwrap();
        assert_eq!(outcome.canonical, canonical);
        assert_eq!(outcome.rows_affected, 0);
    }
}

#[tokio::test]
async fn legacy_same_value_replace_keeps_accounting_behavior_without_lifecycle_change() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    let totals = intent_core::TokenUsageTotals {
        input_tokens: 17,
        ..Default::default()
    };
    f.store
        .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
        .await
        .unwrap();
    let old = f.owner.leaf();
    assert_eq!(
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent.id, "A", "A")
            .await
            .unwrap(),
        "A"
    );
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert!(rows[0].2.is_none());
    assert_eq!(rows[0].3.as_ref(), Some(&totals));
    assert!(old.load(Ordering::SeqCst));
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

async fn outcome(
    f: &Fixture,
    action: RepositoryAcpInitialization,
) -> RepositoryInitializationOutcome {
    let binding = f.binding(action);
    f.store
        .initialize_repository_acp_session_outcome(f.claim(&binding), binding)
        .await
}

fn observed_session(id: Option<&str>) -> RepositoryInitializationPersistence {
    RepositoryInitializationPersistence::NoEffect {
        observed: RepositoryInitializationObservation::Present {
            session_id: id.map(str::to_owned),
        },
    }
}

#[tokio::test]
async fn outcome_cas_loser_preserves_transaction_canonical_without_write_or_owner() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "canonical")
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER forbid_outcome_write BEFORE UPDATE ON agent_session BEGIN SELECT RAISE(ABORT,'unexpected loser write'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let old = f.owner.leaf();
    let result = outcome(&f, replace(Some("stale"), "fresh")).await;
    assert_eq!(result.persistence, observed_session(Some("canonical")));
    assert!(result.confirmation.is_err());
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
    assert!(old.load(Ordering::SeqCst));
    sqlx::query("DROP TRIGGER forbid_outcome_write")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    f.store
        .replace_acp_session_id(&f.workspace.id, &f.agent.id, "canonical", "later")
        .await
        .unwrap();
    assert_eq!(f.id().await.as_deref(), Some("later"));
    // This receipt is the original transaction's observation, not a late reread.
    assert_eq!(result.persistence, observed_session(Some("canonical")));
}

#[tokio::test]
async fn outcome_first_set_same_value_and_load_mismatch_are_not_owned_writes() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    for action in [first("A"), first("B"), loaded("B")] {
        let result = outcome(&f, action).await;
        assert_eq!(result.persistence, observed_session(Some("A")));
        assert!(result.confirmation.is_err());
    }
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
    // The legacy same-value setter keeps its existing successful no-op behavior.
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
}

#[tokio::test]
async fn outcome_missing_scoped_row_is_distinct_from_present_null() {
    let f = Fixture::new().await;
    for action in [loaded("A"), replace(Some("old"), "A")] {
        let result = outcome(&f, action).await;
        assert_eq!(result.persistence, observed_session(None));
        assert!(result.confirmation.is_err());
    }
    for wrong_workspace in [false, true] {
        let mut binding = f.binding(first("A"));
        if wrong_workspace {
            binding.workspace_id = WorkspaceId("other-workspace".into());
        } else {
            binding.agent_id = AgentId("absent-agent".into());
        }
        let result = f
            .store
            .initialize_repository_acp_session_outcome(f.claim(&binding), binding)
            .await;
        assert_eq!(
            result.persistence,
            RepositoryInitializationPersistence::NoEffect {
                observed: RepositoryInitializationObservation::Missing,
            }
        );
        assert!(result.confirmation.is_err());
    }
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

#[tokio::test]
async fn outcome_first_load_and_replace_preserve_confirmation_and_accounting() {
    let f = Fixture::new().await;
    let totals = intent_core::TokenUsageTotals {
        input_tokens: 19,
        ..Default::default()
    };
    f.store
        .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
        .await
        .unwrap();
    let first = outcome(&f, first("A")).await;
    assert_eq!(
        first.persistence,
        RepositoryInitializationPersistence::Committed {
            session_id: "A".into()
        }
    );
    assert!(f.consume(first.confirmation.unwrap()));
    let loaded = outcome(&f, loaded("A")).await;
    assert_eq!(loaded.persistence, observed_session(Some("A")));
    assert!(f.consume(loaded.confirmation.unwrap()));
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert_eq!(rows[0].2.as_ref(), Some(&totals));
    assert!(rows[0].3.is_none());
    let replaced = outcome(&f, replace(Some("A"), "B")).await;
    assert_eq!(
        replaced.persistence,
        RepositoryInitializationPersistence::Committed {
            session_id: "B".into()
        }
    );
    assert!(f.consume(replaced.confirmation.unwrap()));
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert!(rows[0].2.is_none());
    assert_eq!(rows[0].3.as_ref(), Some(&totals));
}

#[tokio::test]
async fn outcome_committed_effect_survives_denied_completion_or_later_consumption() {
    let f = Fixture::new().await;
    f.owner.reject_completion.store(true, Ordering::SeqCst);
    let denied = outcome(&f, first("A")).await;
    assert_eq!(
        denied.persistence,
        RepositoryInitializationPersistence::Committed {
            session_id: "A".into()
        }
    );
    assert!(denied.confirmation.is_err());
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
    f.owner.reject_completion.store(false, Ordering::SeqCst);
    let changed = outcome(&f, replace(Some("A"), "B")).await;
    f.store
        .replace_acp_session_id(&f.workspace.id, &f.agent.id, "B", "C")
        .await
        .unwrap();
    assert!(!f.consume(changed.confirmation.unwrap()));
    assert_eq!(
        changed.persistence,
        RepositoryInitializationPersistence::Committed {
            session_id: "B".into()
        }
    );
}

#[tokio::test]
async fn outcome_foreign_domain_empty_binding_and_closed_pool_are_not_attempted() {
    let f = Fixture::new().await;
    let other = Fixture::new().await;
    let binding = f.binding(first("A"));
    let foreign = other
        .store
        .initialize_repository_acp_session_outcome(f.claim(&binding), binding)
        .await;
    assert_eq!(
        foreign.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(foreign.confirmation.is_err());
    let empty = outcome(&f, first("")).await;
    assert_eq!(
        empty.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(empty.confirmation.is_err());
    f.store.write_pool().close().await;
    let closed = outcome(&f, first("A")).await;
    assert_eq!(
        closed.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(closed.confirmation.is_err());
}

#[tokio::test]
async fn outcome_retired_owner_after_read_has_no_effect_and_no_confirmation() {
    let f = Fixture::new().await;
    let binding = f.binding(first("A"));
    let proof = f.owner.proof(binding.clone());
    *proof.attempt.phase.lock().unwrap() = Phase::Retired;
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
        .unwrap();
    let result = f
        .store
        .initialize_repository_acp_session_outcome(claim, binding)
        .await;
    assert_eq!(result.persistence, observed_session(None));
    assert!(result.confirmation.is_err());
    assert_eq!(f.id().await, None);
    assert_eq!(f.owner.state.lock().unwrap().starts, 0);
}

#[tokio::test]
async fn outcome_trigger_rollback_or_changed_winner_remains_unknown_not_no_effect() {
    for trigger in [
        "CREATE TRIGGER outcome_fault BEFORE UPDATE OF acp_session_id ON agent_session BEGIN UPDATE agent_session SET name='side effect' WHERE id=OLD.id; SELECT RAISE(IGNORE); END",
        "CREATE TRIGGER outcome_fault AFTER UPDATE OF acp_session_id ON agent_session BEGIN SELECT RAISE(ROLLBACK,'fixture abort'); END",
        "CREATE TRIGGER outcome_fault AFTER UPDATE OF acp_session_id ON agent_session WHEN NEW.acp_session_id='A' BEGIN UPDATE agent_session SET acp_session_id='different' WHERE id=NEW.id; END",
    ] {
        let f = Fixture::new().await;
        sqlx::query(trigger).execute(f.store.write_pool()).await.unwrap();
        let result = outcome(&f, first("A")).await;
        assert_eq!(result.persistence, RepositoryInitializationPersistence::Unknown);
        assert!(result.confirmation.is_err());
        assert_eq!(f.id().await, None);
        assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
        assert_eq!(f.store.get_agent_session(&f.agent.id).await.unwrap().name, f.agent.name);
    }
}

#[tokio::test]
async fn outcome_rejected_commit_stays_unknown_and_retains_barrier() {
    let f = Fixture::new().await;
    let mut conn = f.store.write_pool().acquire().await.unwrap();
    conn.lock_handle().await.unwrap().set_commit_hook(|| false);
    drop(conn);
    let result = outcome(&f, first("A")).await;
    assert_eq!(
        result.persistence,
        RepositoryInitializationPersistence::Unknown
    );
    assert!(result.confirmation.is_err());
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    assert_eq!(f.id().await, None);
}

#[tokio::test]
async fn outcome_same_id_and_null_expectation_do_not_claim_legacy_effects() {
    let f = Fixture::new().await;
    let null = outcome(&f, replace(Some("old"), "A")).await;
    assert_eq!(null.persistence, observed_session(None));
    assert!(null.confirmation.is_err());
    // Ordinary replacement still accepts its established current-NULL fallback.
    assert_eq!(
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent.id, "old", "A")
            .await
            .unwrap(),
        "A"
    );
    let totals = intent_core::TokenUsageTotals {
        input_tokens: 23,
        ..Default::default()
    };
    f.store
        .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
        .await
        .unwrap();
    let same = outcome(&f, replace(Some("A"), "A")).await;
    assert_eq!(same.persistence, observed_session(Some("A")));
    assert!(same.confirmation.is_err());
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert_eq!(rows[0].2.as_ref(), Some(&totals));
    assert!(rows[0].3.is_none());
    assert_eq!(
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent.id, "A", "A")
            .await
            .unwrap(),
        "A"
    );
    let rows = f
        .store
        .get_workspace_agent_usage_data(&f.workspace.id)
        .await
        .unwrap();
    assert!(rows[0].2.is_none());
    assert_eq!(rows[0].3.as_ref(), Some(&totals));
}

async fn compatible(
    f: &Fixture,
    action: RepositoryAcpInitialization,
    with_proof: bool,
) -> RepositoryAcpCompatibilityOutcome {
    let binding = f.binding(action);
    let claim = with_proof.then(|| f.claim(&binding));
    f.store
        .initialize_repository_acp_session_compatible(claim, binding)
        .await
}

#[tokio::test]
async fn compatible_same_id_folds_once_without_strict_confirmation() {
    for with_proof in [false, true] {
        let f = Fixture::new().await;
        f.store
            .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
            .await
            .unwrap();
        let totals = intent_core::TokenUsageTotals {
            input_tokens: 29,
            ..Default::default()
        };
        f.store
            .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
            .await
            .unwrap();
        let result = compatible(&f, replace(Some("A"), "A"), with_proof).await;
        assert_eq!(
            result.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: "A".into(),
                effect: RepositoryAcpCompatibilityEffect::AccountingOnly,
            }
        );
        assert_eq!(
            result.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                observed: RepositoryInitializationObservation::Present {
                    session_id: Some("A".into())
                },
                affected_rows: 1,
            }
        );
        assert!(result.confirmation.is_err());
        assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
        let rows = f
            .store
            .get_workspace_agent_usage_data(&f.workspace.id)
            .await
            .unwrap();
        assert!(rows[0].2.is_none());
        assert_eq!(rows[0].3.as_ref(), Some(&totals));
    }
}

#[tokio::test]
async fn compatible_captured_some_current_null_persists_and_banks_without_proof() {
    for with_proof in [false, true] {
        let f = Fixture::new().await;
        let totals = intent_core::TokenUsageTotals {
            input_tokens: 31,
            ..Default::default()
        };
        f.store
            .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
            .await
            .unwrap();
        assert!(outcome(&f, replace(Some("old"), "new"))
            .await
            .confirmation
            .is_err());
        assert_eq!(f.id().await, None);
        let result = compatible(&f, replace(Some("old"), "new"), with_proof).await;
        assert_eq!(
            result.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: "new".into(),
                effect: RepositoryAcpCompatibilityEffect::Replace { previous: None },
            }
        );
        assert!(matches!(
            result.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(result.confirmation.is_err());
        assert_eq!(f.id().await.as_deref(), Some("new"));
        assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
        let rows = f
            .store
            .get_workspace_agent_usage_data(&f.workspace.id)
            .await
            .unwrap();
        assert!(rows[0].2.is_none());
        assert_eq!(rows[0].3.as_ref(), Some(&totals));
    }
}

#[tokio::test]
async fn compatible_winners_and_loaded_keep_one_use_original_confirmation() {
    let f = Fixture::new().await;
    for action in [first("A"), loaded("A"), replace(Some("A"), "B")] {
        let result = compatible(&f, action, true).await;
        assert!(result.result.is_ok());
        assert!(f.consume(result.confirmation.unwrap()));
    }
    let g = Fixture::new().await;
    let result = compatible(&g, first("A"), false).await;
    assert!(matches!(
        result.result,
        Ok(RepositoryAcpCompatibilityResult::Committed { .. })
    ));
    assert!(result.confirmation.is_err());
    assert_eq!(g.id().await.as_deref(), Some("A"));
}

#[tokio::test]
async fn compatible_noops_missing_and_original_load_do_not_invent_canonical_ownership() {
    let f = Fixture::new().await;
    f.store
        .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
        .await
        .unwrap();
    for action in [first("A"), replace(Some("stale"), "fresh")] {
        let result = compatible(&f, action, true).await;
        assert_eq!(
            result.result.unwrap(),
            RepositoryAcpCompatibilityResult::Observed {
                session_id: "A".into()
            }
        );
        assert!(matches!(
            result.persistence,
            RepositoryAcpCompatibilityPersistence::NoEffect { .. }
        ));
        assert!(result.confirmation.is_err());
    }
    let conflict = compatible(&f, first("B"), true).await;
    assert!(
        matches!(conflict.result, Err(intent_core::Error::Internal(ref message)) if message == "acpSessionId is write-once")
    );
    let mut binding = f.binding(first("B"));
    binding.agent_id = AgentId("missing".into());
    let missing = f
        .store
        .initialize_repository_acp_session_compatible(None, binding)
        .await;
    assert!(matches!(
        missing.result,
        Err(intent_core::Error::NotFound(_))
    ));
    assert_eq!(
        missing.persistence,
        RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::Missing
        }
    );
    let loaded = compatible(&f, loaded("original-loaded"), false).await;
    assert_eq!(
        loaded.result.unwrap(),
        RepositoryAcpCompatibilityResult::SubmittedFallback {
            session_id: "original-loaded".into()
        }
    );
    assert!(loaded.confirmation.is_err());
    assert_eq!(f.id().await.as_deref(), Some("A"));
}

#[derive(Clone, Copy)]
enum CompatibilityObserverMode {
    UnknownBegin,
    RefuseOrdinary,
    DefaultSettlement,
}

struct CompatibilityObserver {
    inner: Arc<Observer>,
    mode: CompatibilityObserverMode,
}

struct DefaultSettlementTicket(Box<dyn RepositoryInitializationTicket>);

impl RepositoryInitializationTicket for DefaultSettlementTicket {
    fn finish_confirmed(self: Box<Self>) -> Result<Box<dyn Any + Send>> {
        self.0.finish_confirmed()
    }

    fn settle_no_effect(self: Box<Self>) {
        self.0.settle_no_effect();
    }
    // Deliberately use the real default: no manufactured proof/settlement.
}

impl RepositoryLifecycleObserver for CompatibilityObserver {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        if matches!(self.mode, CompatibilityObserverMode::RefuseOrdinary) {
            return Err(lifecycle_error("fixture ordinary mutation refused"));
        }
        self.inner.begin_mutation(keys)
    }

    fn begin_initialization(
        &self,
        proof: Box<dyn Any + Send>,
        binding: &RepositoryInitializationBinding,
    ) -> Result<Box<dyn RepositoryInitializationTicket>> {
        let ticket = self.inner.begin_initialization(proof, binding)?;
        match self.mode {
            CompatibilityObserverMode::DefaultSettlement => {
                Ok(Box::new(DefaultSettlementTicket(ticket)))
            }
            CompatibilityObserverMode::UnknownBegin | CompatibilityObserverMode::RefuseOrdinary => {
                // An actual begun fixture barrier is left unsettled. A generic
                // error does not prove that the callback created no barrier.
                drop(ticket);
                Err(lifecycle_error(
                    "fixture initialization left unknown barrier",
                ))
            }
        }
    }
}

async fn compatibility_observer_fixture(mode: CompatibilityObserverMode) -> Fixture {
    let original = Fixture::new().await;
    // Reuse fixture row VALUES, not its Store/domain or installed observer.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("initialization.db"))
        .await
        .unwrap();
    store.insert_workspace(&original.workspace).await.unwrap();
    store.insert_agent_session(&original.agent).await.unwrap();
    let owner = Arc::new(Observer::default());
    let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(CompatibilityObserver {
        inner: owner.clone(),
        mode,
    });
    store
        .install_repository_lifecycle_observer(observer.clone())
        .await
        .unwrap();
    Fixture {
        dir,
        store,
        workspace: original.workspace,
        agent: original.agent,
        owner,
        observer,
    }
}

#[tokio::test]
async fn compatible_failed_initialization_ordinary_commit_preserves_original_unknown_barrier() {
    let f = compatibility_observer_fixture(CompatibilityObserverMode::UnknownBegin).await;
    let old = f.owner.leaf();
    let result = compatible(&f, first("A"), true).await;
    assert!(matches!(
        result.result,
        Ok(RepositoryAcpCompatibilityResult::Committed { .. })
    ));
    assert!(matches!(
        result.persistence,
        RepositoryAcpCompatibilityPersistence::Committed { .. }
    ));
    assert!(
        matches!(result.confirmation, Err(ref error) if error.to_string().contains("fixture initialization left unknown barrier"))
    );
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(
        f.owner.state.lock().unwrap().blocked,
        1,
        "ordinary commit must not settle the earlier unknown owner"
    );
    assert_eq!(f.owner.state.lock().unwrap().starts, 1);
    // A new no-proof ordinary mutation also owns only its own barrier.
    let next = compatible(&f, replace(Some("A"), "B"), false).await;
    assert!(next.result.is_ok());
    assert!(next.confirmation.is_err());
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
}

#[tokio::test]
async fn compatible_ordinary_refusal_prevents_dml_without_erasing_original_error() {
    let f = compatibility_observer_fixture(CompatibilityObserverMode::RefuseOrdinary).await;
    let result = compatible(&f, first("A"), true).await;
    assert!(
        matches!(result.result, Err(ref error) if error.to_string().contains("fixture ordinary mutation refused"))
    );
    assert!(
        matches!(result.confirmation, Err(ref error) if error.to_string().contains("fixture initialization left unknown barrier"))
    );
    assert_eq!(
        result.persistence,
        RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::Present { session_id: None },
        }
    );
    assert_eq!(f.id().await, None);
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
}

#[tokio::test]
async fn compatible_default_settlement_keeps_known_commit_and_unsettled_owner() {
    let f = compatibility_observer_fixture(CompatibilityObserverMode::DefaultSettlement).await;
    let result = compatible(&f, replace(Some("old"), "A"), true).await;
    assert!(matches!(
        result.result,
        Ok(RepositoryAcpCompatibilityResult::Committed { .. })
    ));
    assert!(matches!(
        result.persistence,
        RepositoryAcpCompatibilityPersistence::Committed {
            affected_rows: 1,
            ..
        }
    ));
    assert!(
        matches!(result.confirmation, Err(ref error) if error.to_string().contains("unconfirmed commit settlement is unavailable"))
    );
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
}

#[tokio::test]
async fn compatible_same_id_or_load_without_usable_proof_preserves_ordinary_invalidation_policy() {
    for action in [replace(Some("A"), "A"), loaded("A")] {
        for stale_proof in [false, true] {
            let f = Fixture::new().await;
            f.store
                .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
                .await
                .unwrap();
            let binding = f.binding(action.clone());
            let claim = if stale_proof {
                let proof = f.owner.proof(binding.clone());
                *proof.attempt.phase.lock().unwrap() = Phase::Retired;
                Some(
                    f.store
                        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
                        .unwrap(),
                )
            } else {
                None
            };
            let old = f.owner.leaf();
            let result = f
                .store
                .initialize_repository_acp_session_compatible(claim, binding)
                .await;
            assert!(result.result.is_ok());
            assert!(result.confirmation.is_err());
            assert!(
                old.load(Ordering::SeqCst),
                "no effective binding mutation and no successful original begin"
            );
            assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
        }
    }
}

#[tokio::test]
async fn compatible_retired_proof_uses_only_ordinary_ticket_and_foreign_claim_never_downgrades() {
    let f = Fixture::new().await;
    let binding = f.binding(first("A"));
    let proof = f.owner.proof(binding.clone());
    *proof.attempt.phase.lock().unwrap() = Phase::Retired;
    let claim = f
        .store
        .bind_repository_initialization_claim(f.observer.clone(), Box::new(proof))
        .unwrap();
    let old = f.owner.leaf();
    let result = f
        .store
        .initialize_repository_acp_session_compatible(Some(claim), binding)
        .await;
    assert!(result.result.is_ok());
    assert!(result.confirmation.is_err());
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert!(!old.load(Ordering::SeqCst));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
    let other = Fixture::new().await;
    let binding = f.binding(first("B"));
    let claim = f.claim(&binding);
    let foreign = other
        .store
        .initialize_repository_acp_session_compatible(Some(claim), binding)
        .await;
    assert_eq!(
        foreign.persistence,
        RepositoryAcpCompatibilityPersistence::NotAttempted
    );
    assert!(foreign.result.is_err());
    assert!(foreign.confirmation.is_err());
    assert_eq!(other.id().await, None);
}

#[tokio::test]
async fn compatible_denied_completion_keeps_actual_commit_and_original_error() {
    let f = Fixture::new().await;
    f.owner.reject_completion.store(true, Ordering::SeqCst);
    let result = compatible(&f, first("A"), true).await;
    assert!(matches!(
        result.result,
        Ok(RepositoryAcpCompatibilityResult::Committed { .. })
    ));
    assert!(matches!(
        result.persistence,
        RepositoryAcpCompatibilityPersistence::Committed { .. }
    ));
    assert!(
        matches!(result.confirmation, Err(ref error) if error.to_string().contains("fixture original owner retired"))
    );
    assert_eq!(f.id().await.as_deref(), Some("A"));
    assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
}

#[tokio::test]
async fn compatible_committed_trigger_effects_do_not_claim_an_id_winner() {
    for trigger in [
        "CREATE TRIGGER compatible_fault BEFORE UPDATE OF acp_session_id ON agent_session BEGIN UPDATE agent_session SET name='side effect' WHERE id=OLD.id; SELECT RAISE(IGNORE); END",
        "CREATE TRIGGER compatible_fault AFTER UPDATE OF acp_session_id ON agent_session WHEN NEW.acp_session_id='A' BEGIN UPDATE agent_session SET acp_session_id='different' WHERE id=NEW.id; END",
    ] {
        let f = Fixture::new().await;
        sqlx::query(trigger).execute(f.store.write_pool()).await.unwrap();
        let result = compatible(&f, first("A"), true).await;
        assert_eq!(result.result.unwrap(), RepositoryAcpCompatibilityResult::SubmittedFallback { session_id: "A".into() });
        assert!(matches!(result.persistence, RepositoryAcpCompatibilityPersistence::Committed { .. }));
        assert!(result.confirmation.is_err());
        assert_ne!(f.id().await.as_deref(), Some("A"));
        assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
    }
}

#[tokio::test]
async fn compatible_canceled_commit_worker_preserves_unknown_owner_after_reopen() {
    for with_proof in [false, true] {
        let f = Fixture::new().await;
        let binding = f.binding(replace(Some("old"), "A"));
        let claim = with_proof.then(|| f.claim(&binding));
        let pool = f.store.write_pool().clone();
        let path = f.dir.path().join("initialization.db");
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let mut blocked = Some(blocked);
        let mut connection = pool.acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_commit_hook(move || {
                if let Some(blocked) = blocked.take() {
                    signal.notify_one();
                    blocked
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
                true
            });
        drop(connection);
        let store = f.store;
        let task = tokio::spawn(async move {
            store
                .initialize_repository_acp_session_compatible(claim, binding)
                .await
        });
        entered.notified().await;
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
        release.send(()).unwrap();
        let mut connection = pool.acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_commit_hook(|| true);
        let actual: Option<String> =
            sqlx::query_scalar("SELECT acp_session_id FROM agent_session WHERE id=?")
                .bind(&f.agent.id.0)
                .fetch_one(&mut *connection)
                .await
                .unwrap();
        assert_eq!(actual.as_deref(), Some("A"));
        drop(connection);
        let reopened = Store::open(&path).await.unwrap();
        assert!(reopened.has_repository_lifecycle_observer(&f.observer));
        assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
        let replacement: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Observer::default());
        assert!(reopened
            .install_repository_lifecycle_observer(replacement)
            .await
            .is_err());
    }
}

// Composition fixtures exercise real Store writers/SQLite, not R request
// subscriptions, physical-owner policy or a final transport-delivery fence.
mod mixed_lifecycle {
    use super::*;

    struct LoggedMutation {
        token: Arc<()>,
        keys: Vec<RepositoryLifecycleKey>,
    }

    struct MixedObserver {
        inner: Arc<Observer>,
        mutations: Arc<Mutex<Vec<LoggedMutation>>>,
    }

    struct LoggedTicket {
        inner: Box<dyn RepositoryLifecycleMutationTicket>,
        mutations: Arc<Mutex<Vec<LoggedMutation>>>,
        token: Arc<()>,
    }

    impl RepositoryLifecycleMutationTicket for LoggedTicket {
        fn settle_confirmed(self: Box<Self>) {
            self.inner.settle_confirmed();
            self.mutations
                .lock()
                .unwrap()
                .retain(|entry| !Arc::ptr_eq(&entry.token, &self.token));
        }
    }

    impl MixedObserver {
        fn record(
            &self,
            keys: &[RepositoryLifecycleKey],
            inner: Box<dyn RepositoryLifecycleMutationTicket>,
        ) -> Box<dyn RepositoryLifecycleMutationTicket> {
            let token = Arc::new(());
            self.mutations.lock().unwrap().push(LoggedMutation {
                token: token.clone(),
                keys: keys.to_vec(),
            });
            Box::new(LoggedTicket {
                inner,
                mutations: self.mutations.clone(),
                token,
            })
        }

        fn pending(&self, key: &RepositoryLifecycleKey) -> usize {
            self.mutations
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.keys.contains(key))
                .count()
        }
    }

    impl RepositoryLifecycleObserver for MixedObserver {
        fn begin_mutation(
            &self,
            keys: &[RepositoryLifecycleKey],
        ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
            Ok(self.record(keys, self.inner.begin_mutation(keys)?))
        }

        fn begin_pending_delete(
            &self,
            keys: &[RepositoryLifecycleKey],
        ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
            // A separate fixture ticket, without an ordinary mutation's
            // physical-attempt invalidation. This is not the R override.
            self.inner.state.lock().unwrap().blocked += 1;
            Ok(self.record(keys, Box::new(Mutation(self.inner.state.clone()))))
        }

        fn begin_initialization(
            &self,
            proof: Box<dyn Any + Send>,
            binding: &RepositoryInitializationBinding,
        ) -> Result<Box<dyn RepositoryInitializationTicket>> {
            self.inner.begin_initialization(proof, binding)
        }
    }

    async fn fixture() -> (Fixture, Arc<MixedObserver>, intent_core::Principal) {
        let rows = Fixture::new().await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("initialization.db"))
            .await
            .unwrap();
        store.insert_workspace(&rows.workspace).await.unwrap();
        store.insert_agent_session(&rows.agent).await.unwrap();
        let principal = intent_core::Principal {
            id: intent_core::PrincipalId::new(),
            identity: None,
            github_user_id: Some(42),
            login: Some("mixed-lifecycle".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        };
        store.upsert_principal(&principal).await.unwrap();
        let owner = Arc::new(Observer::default());
        let probe = Arc::new(MixedObserver {
            inner: owner.clone(),
            mutations: Arc::default(),
        });
        let observer: Arc<dyn RepositoryLifecycleObserver> = probe.clone();
        store
            .install_repository_lifecycle_observer(observer.clone())
            .await
            .unwrap();
        (
            Fixture {
                dir,
                store,
                workspace: rows.workspace,
                agent: rows.agent,
                owner,
                observer,
            },
            probe,
            principal,
        )
    }

    async fn unknown_wire_write(f: &Fixture, principal: &intent_core::Principal) {
        sqlx::query("CREATE TRIGGER mixed_wire_fault BEFORE INSERT ON principal_credential BEGIN SELECT RAISE(ABORT, 'mixed wire fault'); END")
            .execute(f.store.write_pool()).await.unwrap();
        assert!(f
            .store
            .insert_principal_credential(&principal.id, "failed-wire")
            .await
            .is_err());
        let absent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM principal_credential WHERE token_hash='failed-wire'",
        )
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
        assert_eq!(absent, 0);
        // Removing the disposable fault does not acknowledge the failed owner.
        sqlx::query("DROP TRIGGER mixed_wire_fault")
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn compatible_commit_cannot_settle_independent_pending_owner() {
        let (f, probe, _) = fixture().await;
        let key = RepositoryLifecycleKey::Workspace(f.workspace.id.clone());
        let pending = f
            .store
            .begin_repository_pending_delete(std::slice::from_ref(&key))
            .await
            .unwrap();
        let second = f
            .store
            .begin_repository_pending_delete(std::slice::from_ref(&key))
            .await
            .unwrap();
        let result = compatible(&f, first("A"), true).await;
        assert!(matches!(
            result.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(matches!(
            result.result,
            Ok(RepositoryAcpCompatibilityResult::Committed {
                effect: RepositoryAcpCompatibilityEffect::FirstSet,
                ..
            })
        ));
        assert!(result.confirmation.is_err());
        assert_eq!(f.id().await.as_deref(), Some("A"));
        assert_eq!(probe.pending(&key), 2);
        assert_eq!(f.owner.state.lock().unwrap().blocked, 2);
        second.settle_confirmed();
        assert_eq!(probe.pending(&key), 1);
        assert!(outcome(&f, loaded("A")).await.confirmation.is_err());
        pending.settle_confirmed();
        assert_eq!(probe.pending(&key), 0);
        assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
        let fresh = compatible(&f, replace(Some("A"), "B"), true).await;
        assert!(f.consume(fresh.confirmation.unwrap()));
    }

    #[tokio::test]
    async fn wire_unknown_survives_compatible_commit_and_other_known_settlements() {
        let (f, probe, principal) = fixture().await;
        unknown_wire_write(&f, &principal).await;
        let wire = RepositoryLifecycleKey::WireAuthority;
        assert_eq!(probe.pending(&wire), 1);
        let pending = f
            .store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Workspace(
                f.workspace.id.clone(),
            )])
            .await
            .unwrap();
        let result = compatible(&f, replace(Some("old"), "A"), true).await;
        assert!(matches!(
            result.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(matches!(
            result.result,
            Ok(RepositoryAcpCompatibilityResult::Committed {
                effect: RepositoryAcpCompatibilityEffect::Replace { previous: None },
                ..
            })
        ));
        assert!(result.confirmation.is_err());
        assert_eq!(f.owner.state.lock().unwrap().blocked, 2);
        pending.settle_confirmed();
        f.store
            .insert_principal_credential(&principal.id, "known-successor")
            .await
            .unwrap();
        let next = compatible(&f, replace(Some("A"), "B"), false).await;
        assert!(next.result.is_ok());
        assert!(next.confirmation.is_err());
        assert_eq!(f.id().await.as_deref(), Some("B"));
        assert_eq!(
            probe.pending(&wire),
            1,
            "same-coordinate success cannot settle the predecessor"
        );
        assert_eq!(
            probe.pending(&RepositoryLifecycleKey::Agent(f.agent.id.clone())),
            0
        );
        assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
    }

    #[tokio::test]
    async fn same_id_accounting_stays_factual_without_settling_pending_ownership() {
        for with_proof in [false, true] {
            let (f, probe, _) = fixture().await;
            f.store
                .set_acp_session_id(&f.workspace.id, &f.agent.id, "A")
                .await
                .unwrap();
            let totals = intent_core::TokenUsageTotals {
                input_tokens: 37,
                ..Default::default()
            };
            f.store
                .set_agent_session_token_usage(&f.workspace.id, &f.agent.id, &totals)
                .await
                .unwrap();
            let key = RepositoryLifecycleKey::Workspace(f.workspace.id.clone());
            let pending = f
                .store
                .begin_repository_pending_delete(std::slice::from_ref(&key))
                .await
                .unwrap();
            for _ in 0..2 {
                let result = compatible(&f, replace(Some("A"), "A"), with_proof).await;
                assert_eq!(
                    result.result.unwrap(),
                    RepositoryAcpCompatibilityResult::Committed {
                        session_id: "A".into(),
                        effect: RepositoryAcpCompatibilityEffect::AccountingOnly
                    }
                );
                assert!(matches!(
                    result.persistence,
                    RepositoryAcpCompatibilityPersistence::Committed {
                        affected_rows: 1,
                        ..
                    }
                ));
                assert!(result.confirmation.is_err());
                let rows = f
                    .store
                    .get_workspace_agent_usage_data(&f.workspace.id)
                    .await
                    .unwrap();
                assert!(rows[0].2.is_none());
                assert_eq!(
                    rows[0].3.as_ref(),
                    Some(&totals),
                    "the original snapshot is banked once"
                );
                assert_eq!(probe.pending(&key), 1);
                assert_eq!(f.owner.state.lock().unwrap().blocked, 1);
            }
            pending.settle_confirmed();
            assert_eq!(f.owner.state.lock().unwrap().blocked, 0);
        }
    }

    #[tokio::test]
    async fn canceled_compatible_worker_and_wire_predecessor_survive_last_store_reopen() {
        let (f, probe, principal) = fixture().await;
        unknown_wire_write(&f, &principal).await;
        let workspace_key = RepositoryLifecycleKey::Workspace(f.workspace.id.clone());
        let agent_key = RepositoryLifecycleKey::Agent(f.agent.id.clone());
        let pending = f
            .store
            .begin_repository_pending_delete(std::slice::from_ref(&workspace_key))
            .await
            .unwrap();
        let binding = f.binding(replace(Some("old"), "A"));
        let claim = f.claim(&binding);
        let pool = f.store.write_pool().clone();
        let path = f.dir.path().join("initialization.db");
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let mut blocked = Some(blocked);
        let mut connection = pool.acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_commit_hook(move || {
                if let Some(blocked) = blocked.take() {
                    signal.notify_one();
                    blocked
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
                true
            });
        drop(connection);
        let store = f.store;
        let task = tokio::spawn(async move {
            store
                .initialize_repository_acp_session_compatible(Some(claim), binding)
                .await
        });
        entered.notified().await;
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(probe.pending(&agent_key), 1);
        assert_eq!(probe.pending(&RepositoryLifecycleKey::WireAuthority), 1);
        pending.settle_confirmed();
        assert_eq!(probe.pending(&workspace_key), 0);
        assert_eq!(f.owner.state.lock().unwrap().blocked, 2);
        release.send(()).unwrap();
        let mut connection = pool.acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_commit_hook(|| true);
        let actual: Option<String> =
            sqlx::query_scalar("SELECT acp_session_id FROM agent_session WHERE id=?")
                .bind(&f.agent.id.0)
                .fetch_one(&mut *connection)
                .await
                .unwrap();
        assert_eq!(actual.as_deref(), Some("A"));
        drop(connection);
        // The original task owned the last Store. SQLite's late commit and a
        // same-observer managed reopen cannot acknowledge that lost owner.
        let reopened = Store::open(&path).await.unwrap();
        assert!(reopened.has_repository_lifecycle_observer(&f.observer));
        assert_eq!(probe.pending(&agent_key), 1);
        assert_eq!(probe.pending(&RepositoryLifecycleKey::WireAuthority), 1);
        assert_eq!(f.owner.state.lock().unwrap().blocked, 2);
        let replacement: Arc<dyn RepositoryLifecycleObserver> = Arc::new(Observer::default());
        assert!(reopened
            .install_repository_lifecycle_observer(replacement)
            .await
            .is_err());
    }
}
