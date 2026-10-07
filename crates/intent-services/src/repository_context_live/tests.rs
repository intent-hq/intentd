//! Real Store/Git/`FileSecretStore` owners; HTTP and initial ACP completion are
//! explicitly scripted disposable fixtures, never authorization substitutes.
use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner,
};
use crate::repository_admission::read_request::PreparedRepositoryOptional;
use crate::repository_admission::request_context::RepositoryPromptRequest;
use crate::repository_read_source::tests::{ActualRead, ReadServer};
use intent_acp::mcp_server::WorkspaceMcpServer;
use intent_core::{
    AgentId, AgentSession, HistoricalTargetSource, PrincipalId, ReviewSelectionOutcome,
    WorkspaceGitRoot, WorkspaceGitRootId,
};
use intent_store::RepositoryLifecycleObserver;
use intent_store::{RepositorySelectionChange, RepositorySelectionWriteResult};
use serde_json::json;

pub(crate) struct LiveFixture {
    pub(crate) base: ActualRead,
    pub(crate) session: AgentSession,
    pub(crate) physical: RepositoryPhysicalOwner,
    pub(crate) owner: Arc<RepositoryContextOwner>,
}
impl LiveFixture {
    pub(crate) async fn new(http: &ReadServer) -> Self {
        Self::from_base(ActualRead::new(http).await).await
    }
    async fn from_base(base: ActualRead) -> Self {
        let session: AgentSession = serde_json::from_value(json!({
            "id":AgentId::new(),"workspaceId":base.git.workspace.id,"name":"real-context",
            "status":"active","harnessVersion":"3.0", "createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"
        })).unwrap();
        base.auth
            .service
            .store
            .insert_agent_session(&session)
            .await
            .unwrap();
        let registry = base
            .auth
            .service
            .repository_lifecycle_registry()
            .await
            .unwrap();
        let physical = RepositoryCreationOwner::allocate(
            &registry,
            &base.auth.service.store,
            base.git.workspace.id.clone(),
            session.id.clone(),
            RepositoryCreationIntent::FirstSet,
        )
        .unwrap()
        .initialize(&base.auth.service.store, || async {
            Ok("scripted initial ACP completion; real original owner".into())
        })
        .await
        .unwrap();
        let owner = RepositoryContextOwner::bind(
            base.auth.service.clone(),
            RepositoryReadOwner::capture(base.auth.service.clone()),
            &physical,
        )
        .unwrap();
        Self {
            base,
            session,
            physical,
            owner,
        }
    }
    pub(crate) fn server(&self) -> WorkspaceMcpServer {
        WorkspaceMcpServer::new(self.base.api(), self.session.workspace_id.clone())
            .with_caller_agent_id(Some(self.session.id.clone()))
            .with_request_context(self.owner.mcp_context())
            .with_repository_guidance(&self.session, self.owner.guidance_source())
    }
    pub(crate) async fn selection(&self, change: RepositorySelectionChange) {
        let store = &self.base.auth.service.store;
        let before = store
            .repository_selection_snapshot(&self.base.git.root())
            .await
            .unwrap();
        let result = store.write_repository_selection(&before, change).await;
        assert!(matches!(
            result.result.unwrap(),
            RepositorySelectionWriteResult::Applied(_)
                | RepositorySelectionWriteResult::Unchanged(_)
        ));
    }
    pub(crate) async fn prompt_capture(
        &self,
    ) -> crate::repository_context_output::RepositoryPromptContext {
        with_caller(
            Caller::Agent {
                agent_id: self.session.id.clone(),
            },
            async { self.owner.capture_prompt().unwrap() },
        )
        .await
    }
    pub(crate) async fn observe(
        &self,
    ) -> (
        RepositoryPromptRequest,
        PreparedRepositoryOptional<Arc<PreparedContextFacts>>,
    ) {
        with_caller(
            Caller::Agent {
                agent_id: self.session.id.clone(),
            },
            async {
                let original = self.owner.callback.capture_prompt().unwrap();
                let prepared = original
                    .run(Box::pin(async {
                        let scope = original.read().capture_optional()?;
                        scope.run_optional(|local| self.owner.prepare(local))?.await
                    }))
                    .unwrap()
                    .await
                    .unwrap();
                (original, prepared)
            },
        )
        .await
    }
    pub(crate) async fn root(&self, name: &str) -> WorkspaceGitRoot {
        let path = self.base.git.dir.path().join(name);
        let repo = git2::Repository::init_opts(
            &path,
            git2::RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "fixture", &tree, &[])
            .unwrap();
        self.base.git.git(
            &path,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/team/registered.git",
            ],
        );
        let row:WorkspaceGitRoot=serde_json::from_value(json!({"id":WorkspaceGitRootId::new(),"workspaceId":self.session.workspace_id,"path":path,"source":"agent","registeredByAgentIds":[self.session.id],"createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"})).unwrap();
        self.base
            .auth
            .service
            .store
            .upsert_workspace_git_root(&row)
            .await
            .unwrap()
            .0
    }
}

#[intent_test_macros::daemon_test]
async fn live_context_actual_selection_inventory_and_stable_revision_without_provider_io() {
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let before = std::fs::read(f.base.auth.service.gitlab_secret_store.path()).unwrap();
    let (_, first) = f.observe().await;
    let (_, same) = f.observe().await;
    assert_eq!(first.value().context, same.value().context);
    assert_eq!(
        first.value().context.scope.daemon_id,
        f.base.auth.service.daemon_boot_id
    );
    assert!(first.value().context.scope.authority_generation > 0);
    let c = &first.value().context;
    assert!(matches!(
        c.roots[0].review_selection.saved,
        SavedReviewSelection::Automatic
    ));
    assert!(matches!(
        c.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::Resolved { .. }
    ));
    assert_eq!(
        c.roots[0].targets[0].availability,
        RepositoryAvailability::Connected
    );
    assert!(c.roots[0].targets[0]
        .capabilities
        .iter()
        .filter(|v| matches!(
            v.operation,
            RepositoryOperation::ReadReview | RepositoryOperation::ReadIssue
        ))
        .all(|v| v.state == RepositoryCapabilityState::Unknown));
    f.base.git.git(
        &f.base.git.path,
        &[
            "config",
            "remote.origin.pushurl",
            "https://github.com/team/push-only.git",
        ],
    );
    let (_, changed) = f.observe().await;
    assert_ne!(changed.value().context.revision, c.revision);
    assert_eq!(
        changed.value().context.roots[0].review_selection,
        c.roots[0].review_selection
    );
    assert_eq!(changed.value().context.roots[0].targets.len(), 2);
    f.selection(RepositorySelectionChange::ExplicitRemote {
        remote_name: "missing".into(),
    })
    .await;
    let (_, missing) = f.observe().await;
    assert!(matches!(
        missing.value().context.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::SelectionRequired { .. }
    ));
    f.selection(RepositorySelectionChange::Reset).await;
    let registered = f.root("registered").await;
    let (_, inventory) = f.observe().await;
    assert_eq!(inventory.value().context.roots.len(), 2);
    f.base
        .auth
        .service
        .store
        .delete_workspace_git_root(&registered.id)
        .await
        .unwrap();
    let (_, deleted) = f.observe().await;
    assert_eq!(deleted.value().context.roots.len(), 1);
    assert_ne!(
        inventory.value().context.revision,
        deleted.value().context.revision
    );
    assert_eq!(http.count(), 0);
    assert_eq!(
        std::fs::read(f.base.auth.service.gitlab_secret_store.path()).unwrap(),
        before
    );
    f.owner.drain_jobs().await;
}

#[intent_test_macros::daemon_test]
async fn live_context_original_allocation_epoch_invalidation_and_checked_overflow() {
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let foreign = Arc::new(f.base.auth.service.as_ref().clone());
    assert!(RepositoryContextOwner::bind(
        foreign,
        RepositoryReadOwner::capture(f.base.auth.service.clone()),
        &f.physical
    )
    .is_err());
    assert!(RepositoryContextOwner::bind(
        f.base.auth.service.clone(),
        Err(AdmissionError::Unavailable),
        &f.physical
    )
    .is_err());
    let (_, first) = f.observe().await;
    assert!(first.value().with_revision(|live| live));
    f.owner.invalidate();
    assert!(!first.value().with_revision(|live| live));
    let (_, next) = f.observe().await;
    assert_ne!(
        first.value().context.revision,
        next.value().context.revision
    );
    let separate = RepositoryContextOwner::bind(
        f.base.auth.service.clone(),
        RepositoryReadOwner::capture(f.base.auth.service.clone()),
        &f.physical,
    )
    .unwrap();
    assert_ne!(separate.epoch, f.owner.epoch);
    assert_ne!(separate.scope, f.owner.scope);
    {
        let mut revision = f.owner.revision.lock().unwrap();
        revision.sequence = u64::MAX;
        revision.published = None;
    }
    let result = with_caller(
        Caller::Agent {
            agent_id: f.session.id.clone(),
        },
        async {
            let original = f.owner.callback.capture_prompt().unwrap();
            original
                .run(Box::pin(async {
                    original
                        .read()
                        .capture_optional()?
                        .run_optional(|local| f.owner.prepare(local))?
                        .await
                }))
                .unwrap()
                .await
        },
    )
    .await;
    assert!(result.is_err());
    assert!(f.owner.revision.lock().unwrap().unavailable);
    assert_eq!(http.count(), 0);
}

/// Blocking fixture barrier retains the actual reader worker and worktree lock.
pub(crate) struct BlockingHold {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    ready: std::sync::Condvar,
}
impl BlockingHold {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            ready: std::sync::Condvar::new(),
        })
    }
    pub(crate) fn wait(&self) {
        self.entered.notify_one();
        let guard = self.released.lock().unwrap();
        let (guard, timed) = self
            .ready
            .wait_timeout_while(guard, std::time::Duration::from_secs(30), |done| !*done)
            .unwrap();
        drop(guard);
        assert!(!timed.timed_out(), "blocking fixture release deadline");
    }
    pub(crate) async fn reached(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(15), self.entered.notified())
            .await
            .unwrap();
    }
    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.ready.notify_all();
    }
}

#[intent_test_macros::daemon_test]
async fn live_context_cancelled_blocking_job_keeps_real_lock_and_rejects_late_publication() {
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let (_, old) = f.observe().await;
    let held = BlockingHold::new();
    let probe = held.clone();
    *f.owner.git_probe.lock().unwrap() = Some(Arc::new(move || probe.wait()));
    let capture = f.prompt_capture().await;
    let task = tokio::spawn(capture.prepare());
    held.reached().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(
        f.owner.jobs.active.load(Ordering::Acquire),
        1,
        "actual blocking worker still owns its job"
    );
    assert!(
        !old.value().with_revision(|current| current),
        "cancel closes publication immediately"
    );
    assert!(
        f.base
            .auth
            .service
            .worktree_locks
            .try_with_lock(&f.base.git.path, || async {})
            .await
            .is_none(),
        "the real worktree lease stays with the worker"
    );
    held.release();
    tokio::time::timeout(std::time::Duration::from_secs(15), f.owner.drain_jobs())
        .await
        .unwrap();
    assert_eq!(f.owner.jobs.active.load(Ordering::Acquire), 0);
    assert!(
        f.owner.revision.lock().unwrap().published.is_none(),
        "late result never repairs canceled observation"
    );
    let (_, fresh) = f.observe().await;
    assert_ne!(old.value().context.revision, fresh.value().context.revision);
    assert_eq!(http.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn live_context_concurrent_observation_cannot_publish_an_older_attempt() {
    let http = ReadServer::new().await;
    let f = Arc::new(LiveFixture::new(&http).await);
    let held = BlockingHold::new();
    let probe = held.clone();
    *f.owner.git_probe.lock().unwrap() = Some(Arc::new(move || probe.wait()));
    let a = f.prompt_capture().await;
    let first = tokio::spawn(a.prepare());
    held.reached().await;
    let b = f.prompt_capture().await;
    let second = tokio::spawn(b.prepare());
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while f.owner.revision.lock().unwrap().attempt < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    held.release();
    assert!(first.await.unwrap().is_none());
    assert!(second.await.unwrap().is_some());
    f.owner.drain_jobs().await;
    assert_eq!(http.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn live_context_historical_primary_and_root_incarnation_never_self_heal_from_remotes() {
    let http = ReadServer::new().await;
    let mut base = ActualRead::new(&http).await;
    base.auth
        .service
        .store
        .delete_workspace(&base.git.workspace.id)
        .await
        .unwrap();
    base.git.workspace.repository_owner = Some("historical-owner".into());
    base.git.workspace.repository_name = Some("historical-name".into());
    base.auth
        .service
        .store
        .insert_workspace(&base.git.workspace)
        .await
        .unwrap();
    let f = LiveFixture::from_base(base).await;
    let (_, context) = f.observe().await;
    let saved = &context.value().context.roots[0].review_selection.saved;
    assert!(
        matches!(saved,SavedReviewSelection::UnresolvedHistorical{source:Some(HistoricalTargetSource::WorkspaceMetadata),record_id:Some(id)} if id==f.session.workspace_id.as_str())
    );
    let refused =
        crate::repository_read_source::tests::run(&f.server(), "return await ws.pr.snapshot(4);")
            .await;
    assert!(!refused.to_string().contains("actual review"));
    assert_eq!(http.count(), 0);
    f.selection(RepositorySelectionChange::ExplicitRemote {
        remote_name: "origin".into(),
    })
    .await;
    let selected =
        crate::repository_read_source::tests::run(&f.server(), "return await ws.pr.snapshot(4);")
            .await;
    assert!(selected.to_string().contains("actual review"), "{selected}");
    let root = f.root("recreated").await;
    let id = RepositoryRootId {
        workspace_id: f.session.workspace_id.clone(),
        kind: RepositoryRootKind::Registered {
            git_root_id: root.id.clone(),
        },
    };
    let old = f
        .base
        .auth
        .service
        .store
        .repository_selection_snapshot(&id)
        .await
        .unwrap();
    f.base
        .auth
        .service
        .store
        .write_repository_selection(
            &old,
            RepositorySelectionChange::ExplicitRemote {
                remote_name: "origin".into(),
            },
        )
        .await
        .result
        .unwrap();
    f.base
        .auth
        .service
        .store
        .delete_workspace_git_root(&root.id)
        .await
        .unwrap();
    f.base
        .auth
        .service
        .store
        .upsert_workspace_git_root(&root)
        .await
        .unwrap();
    let (_, recreated) = f.observe().await;
    assert!(matches!(
        recreated.value().context.roots[1].review_selection.saved,
        SavedReviewSelection::UnresolvedHistorical { .. }
    ));
    let current = f
        .base
        .auth
        .service
        .store
        .repository_selection_snapshot(&id)
        .await
        .unwrap();
    assert_ne!(old.root_incarnation(), current.root_incarnation());
    assert!(matches!(
        f.base
            .auth
            .service
            .store
            .write_repository_selection(&old, RepositorySelectionChange::Automatic)
            .await
            .result
            .unwrap(),
        RepositorySelectionWriteResult::Conflict(_)
    ));
}

#[intent_test_macros::daemon_test]
async fn live_context_foreign_domain_pending_wire_and_physical_loss_never_gain_optional_ownership()
{
    use intent_core::caller::WireCredential;
    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    let other_http = ReadServer::new().await;
    let other = LiveFixture::new(&other_http).await;
    assert!(RepositoryContextOwner::bind(
        f.base.auth.service.clone(),
        RepositoryReadOwner::capture(f.base.auth.service.clone()),
        &other.physical
    )
    .is_err());
    let caller = Caller::Agent {
        agent_id: f.session.id.clone(),
    };
    let wire = WireCredential::Principal {
        principal_id: PrincipalId::new(),
        token_hash: "opaque negative context fixture".into(),
    };
    assert!(with_caller(
        caller.clone(),
        with_wire_credential(Some(wire), async { f.owner.capture_prompt() })
    )
    .await
    .is_err());
    let registry = f
        .base
        .auth
        .service
        .repository_lifecycle_registry()
        .await
        .unwrap();
    let pending = registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(
            f.session.workspace_id.clone(),
        )])
        .unwrap();
    assert!(RepositoryContextOwner::bind(
        f.base.auth.service.clone(),
        RepositoryReadOwner::capture(f.base.auth.service.clone()),
        &f.physical
    )
    .is_err());
    pending.settle_confirmed();
    let original = f.prompt_capture().await;
    drop(f.physical);
    assert!(with_caller(caller.clone(), original.prepare())
        .await
        .is_none());
    assert!(with_caller(caller, async { f.owner.capture_prompt() })
        .await
        .is_err());
    assert_eq!(http.count(), 0);
    assert_eq!(other_http.count(), 0);
    f.owner.drain_jobs().await;
    other.owner.drain_jobs().await;
}
