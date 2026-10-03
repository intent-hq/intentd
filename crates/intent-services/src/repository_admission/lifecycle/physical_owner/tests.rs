//! Actual Store confirmation and R lifetime composition. Provider completion
//! futures are explicit fixtures; manager/ACP producer wiring is still absent.

use intent_core::caller::with_caller;
use intent_core::{chief_workspace, AgentSession, Workspace};
use intent_store::{RepositoryAcpCompatibilityEffect, RepositoryInitializationObservation};

use super::*;

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
    workspace: Workspace,
    registry: Arc<RepositoryLifecycleRegistry>,
    agent: AgentId,
}

impl Fixture {
    async fn new(session: Option<&str>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("owner.db")).await.unwrap();
        let mut workspace = chief_workspace();
        workspace.id = WorkspaceId::new();
        store.insert_workspace(&workspace).await.unwrap();
        let agent = AgentId::new();
        let row: AgentSession = serde_json::from_value(serde_json::json!({
            "id":agent,"workspaceId":workspace.id,"name":"physical fixture","status":"active",
            "acpSessionId":session,"createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
        })).unwrap();
        store.insert_agent_session(&row).await.unwrap();
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&store).await.unwrap();
        Self {
            dir,
            store,
            workspace,
            registry,
            agent,
        }
    }

    fn creator(&self, intent: RepositoryCreationIntent) -> RepositoryCreationOwner {
        RepositoryCreationOwner::allocate(
            &self.registry,
            &self.store,
            self.workspace.id.clone(),
            self.agent.clone(),
            intent,
        )
        .unwrap()
    }

    fn caller(&self) -> Caller {
        Caller::Agent {
            agent_id: self.agent.clone(),
        }
    }
}

async fn current(
    capture: &crate::repository_admission::request_context::RepositoryCapturedRequest,
    caller: Caller,
) -> bool {
    with_caller(caller, async { capture.source_lifetime().is_ok() }).await
}

#[tokio::test]
async fn read_forwarding_keeps_creator_store_and_only_one_weak_physical_capture() {
    use intent_acp::mcp_server::request_context::McpRequestContext;

    let f = Fixture::new(None).await;
    let clone = f.store.clone();
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let owner = creator
        .initialize(&clone, || async { Ok("original".into()) })
        .await
        .unwrap();
    let (registry, _, forwarded) = owner.callback_binding();
    assert!(Arc::ptr_eq(registry, &f.registry));
    assert!(forwarded.shares_repository_lifecycle_domain(&f.store));
    let callback = owner.callback();
    let before = f.registry.state.lock().unwrap().next_id;
    let scope = McpRequestContext::capture(&callback);
    {
        let state = f.registry.state.lock().unwrap();
        assert_eq!(state.next_id, before + 1);
        assert_eq!(state.subscriptions.len(), 1);
    }
    drop(owner);
    let mut ran = false;
    with_caller(
        f.caller(),
        scope.scope(Box::pin(async {
            assert!(matches!(
                crate::repository_admission::request_context::current_source_lifetime(),
                Err(AdmissionError::Retired)
            ));
            ran = true;
        })),
    )
    .await;
    assert!(ran);
    assert!(!current(&callback.capture(), f.caller()).await);
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
}

#[tokio::test]
async fn committed_original_creation_mints_distinct_live_callback_and_request_lifetime() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let pending_callback = creator.callback();
    let pending = pending_callback.capture();
    let owner = creator
        .initialize(&f.store, || async { Ok("created".into()) })
        .await
        .unwrap();
    assert!(!current(&pending, f.caller()).await);
    assert!(!current(&pending_callback.capture(), f.caller()).await);
    let callback = owner.callback();
    assert!(current(&callback.capture(), f.caller()).await);
    let retained = callback.capture();
    owner.interrupt_requests();
    assert!(!current(&retained, f.caller()).await);
    assert!(current(&callback.capture(), f.caller()).await);
    let last = callback.capture();
    let retire = owner.retirement();
    retire.retire();
    retire.retire();
    assert!(!current(&last, f.caller()).await);
    assert!(!current(&callback.capture(), f.caller()).await);
}

#[tokio::test]
async fn loaded_confirmation_retires_live_and_competing_pending_allocations() {
    let f = Fixture::new(None).await;
    let first = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize(&f.store, || async { Ok("session".into()) })
        .await
        .unwrap();
    let old_callback = first.callback();
    let old_request = old_callback.capture();
    let competitor = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "session".into(),
    });
    let original = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "session".into(),
    });
    let loaded = original
        .initialize(&f.store, || async { Ok("session".into()) })
        .await
        .unwrap();
    assert!(!current(&old_request, f.caller()).await);
    assert!(!current(&old_callback.capture(), f.caller()).await);
    assert!(current(&loaded.callback().capture(), f.caller()).await);
    assert!(matches!(
        competitor
            .initialize(&f.store, || async { Ok("session".into()) })
            .await,
        Err(AdmissionError::Retired)
    ));
    drop(first);
    assert!(current(&loaded.callback().capture(), f.caller()).await);
    let after_drop = loaded.callback();
    drop(loaded);
    assert!(!current(&after_drop.capture(), f.caller()).await);
}

#[tokio::test]
async fn equal_first_set_lost_replace_and_wrong_loaded_completion_never_create_origin() {
    for intent in [
        RepositoryCreationIntent::FirstSet,
        RepositoryCreationIntent::Replace {
            expected: Some("foreign".into()),
        },
        RepositoryCreationIntent::Loaded {
            session_id: "original".into(),
        },
    ] {
        let f = Fixture::new(Some("original")).await;
        let creator = f.creator(intent.clone());
        let pending = creator.callback();
        let result = creator
            .initialize(&f.store, || async {
                Ok(
                    if matches!(intent, RepositoryCreationIntent::Loaded { .. }) {
                        "wrong"
                    } else {
                        "original"
                    }
                    .into(),
                )
            })
            .await;
        assert!(result.is_err());
        assert!(!current(&pending.capture(), f.caller()).await);
        let recovered = f
            .creator(RepositoryCreationIntent::Loaded {
                session_id: "original".into(),
            })
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .unwrap();
        assert!(current(&recovered.callback().capture(), f.caller()).await);
    }
}

#[tokio::test]
async fn actual_null_and_value_replacement_require_original_winning_creator() {
    for expected in [None, Some("previous")] {
        let f = Fixture::new(expected).await;
        let owner = f
            .creator(RepositoryCreationIntent::Replace {
                expected: expected.map(str::to_owned),
            })
            .initialize(&f.store, || async { Ok("replacement".into()) })
            .await
            .unwrap();
        assert!(current(&owner.callback().capture(), f.caller()).await);
        f.store
            .replace_acp_session_id(&f.workspace.id, &f.agent, "replacement", "next")
            .await
            .unwrap();
        assert!(!current(&owner.callback().capture(), f.caller()).await);
    }
}

#[tokio::test]
async fn original_producer_wait_and_cancel_never_borrow_a_later_store_winner() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let pending = creator.callback();
    let result = creator
        .initialize(&f.store, || async {
            f.store
                .set_acp_session_id(&f.workspace.id, &f.agent, "another")
                .await
                .unwrap();
            Ok("original".into())
        })
        .await;
    assert!(matches!(result, Err(AdmissionError::Retired)));
    assert!(!current(&pending.capture(), f.caller()).await);
    let canceled = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "another".into(),
    });
    let old_id = canceled.id;
    let mut future = Box::pin(canceled.initialize(&f.store, || {
        std::future::pending::<AdmissionResult<String>>()
    }));
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(future);
    assert!(!f
        .registry
        .state
        .lock()
        .unwrap()
        .creations
        .contains_key(&old_id));
}

#[tokio::test]
async fn confirmation_cannot_cross_domain_or_outlive_intervening_mutation() {
    for foreign_domain in [false, true] {
        let f = Fixture::new(None).await;
        let creator = f.creator(RepositoryCreationIntent::FirstSet);
        let (claim, binding) = creator.claim_after_success("created".into()).unwrap();
        let confirmation = f
            .store
            .initialize_repository_acp_session(claim, binding)
            .await
            .unwrap();
        if foreign_domain {
            let other = Fixture::new(None).await;
            assert!(creator.consume(&other.store, confirmation).is_err());
        } else {
            f.store
                .replace_acp_session_id(&f.workspace.id, &f.agent, "created", "changed")
                .await
                .unwrap();
            assert!(matches!(
                creator.consume(&f.store, confirmation),
                Err(AdmissionError::Retired)
            ));
        }
        assert!(f.registry.state.lock().unwrap().origins.is_empty());
    }
}

// Explicit observer settlement fixture: no fake SQL completion is claimed.
fn begin_fixture(creator: &RepositoryCreationOwner) -> Box<dyn RepositoryInitializationTicket> {
    let binding = RepositoryInitializationBinding {
        workspace_id: creator.workspace.clone(),
        agent_id: creator.agent.clone(),
        action: RepositoryAcpInitialization::FirstSet {
            session_id: "fixture".into(),
        },
    };
    {
        let mut state = creator.registry.state.lock().unwrap();
        let pending = state.creations.get_mut(&creator.id).unwrap();
        pending.phase = Phase::Claimed;
        pending.binding = Some(binding.clone());
    }
    creator
        .registry
        .begin_initialization(
            Box::new(OriginalProof {
                registry: Arc::downgrade(&creator.registry),
                token: Arc::downgrade(&creator.token),
                id: creator.id,
                binding: binding.clone(),
                consumed: false,
            }),
            &binding,
        )
        .unwrap()
}

#[tokio::test]
async fn unknown_initialization_ticket_blocks_but_known_completion_cannot_erase_another_barrier() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let ticket = begin_fixture(&creator);
    let mutation = f
        .registry
        .begin_mutation(&[RepositoryLifecycleKey::Agent(f.agent.clone())])
        .unwrap();
    assert!(ticket.finish_confirmed().is_err());
    assert_eq!(f.registry.state.lock().unwrap().pending.len(), 1);
    mutation.settle_confirmed();
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
    let fresh = f.creator(RepositoryCreationIntent::FirstSet);
    drop(begin_fixture(&fresh));
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet
    )
    .is_err());
}

#[tokio::test]
async fn forged_owner_and_known_no_effect_cannot_mint_confirmation() {
    let f = Fixture::new(None).await;
    let binding = RepositoryInitializationBinding {
        workspace_id: f.workspace.id.clone(),
        agent_id: f.agent.clone(),
        action: RepositoryAcpInitialization::Loaded {
            session_id: "claimed".into(),
        },
    };
    assert!(f
        .registry
        .begin_initialization(Box::new(binding.clone()), &binding)
        .is_err());
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    begin_fixture(&creator).settle_no_effect();
    assert!(creator.claim_after_success("after".into()).is_err());
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn committed_without_confirmation_settles_original_barrier_without_live_owner() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let callback = creator.callback();
    begin_fixture(&creator)
        .settle_committed_without_confirmation()
        .unwrap();
    assert!(creator.claim_after_success("late".into()).is_err());
    assert!(!current(&callback.capture(), f.caller()).await);
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
    let fresh = f.creator(RepositoryCreationIntent::FirstSet);
    assert!(fresh
        .initialize(&f.store, || async { Ok("fresh".into()) })
        .await
        .is_ok());
}

#[tokio::test]
async fn committed_without_confirmation_preserves_other_unknown_barrier() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let original = begin_fixture(&creator);
    let original_barriers = f
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let unknown = f
        .registry
        .begin_mutation(&[RepositoryLifecycleKey::Agent(f.agent.clone())])
        .unwrap();
    let other_barriers = f
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .keys()
        .copied()
        .filter(|id| !original_barriers.contains(id))
        .collect::<HashSet<_>>();
    assert_eq!(other_barriers.len(), 1);
    drop(unknown);
    original.settle_committed_without_confirmation().unwrap();
    assert_eq!(
        f.registry
            .state
            .lock()
            .unwrap()
            .pending
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        other_barriers
    );
    assert!(creator.claim_after_success("late".into()).is_err());
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .is_err());
}

#[tokio::test]
async fn one_workspace_initialization_does_not_retire_an_unrelated_pending_owner() {
    let f = Fixture::new(None).await;
    let mut other = f.workspace.clone();
    other.id = WorkspaceId::new();
    f.store.insert_workspace(&other).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(serde_json::json!({"id":agent,"workspaceId":other.id,"name":"other","status":"active","createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"})).unwrap();
    f.store.insert_agent_session(&row).await.unwrap();
    let unrelated = RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        other.id,
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap();
    let original = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize(&f.store, || async { Ok("one".into()) })
        .await
        .unwrap();
    let other = unrelated
        .initialize(&f.store, || async { Ok("two".into()) })
        .await
        .unwrap();
    assert!(current(&original.callback().capture(), f.caller()).await);
    assert!(
        current(
            &other.callback().capture(),
            Caller::Agent { agent_id: agent }
        )
        .await
    );
}

#[tokio::test]
async fn concurrent_hard_retire_callers_join_the_original_leaf_before_returning() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let f = Fixture::new(None).await;
    let owner = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize(&f.store, || async { Ok("original".into()) })
        .await
        .unwrap();
    let leaf = crate::repository_admission::RepositoryRetirement::default();
    let _subscription = f
        .registry
        .subscribe(
            &owner.origin(),
            &f.caller(),
            &[RepositoryLifecycleKey::Database],
            leaf.clone(),
        )
        .unwrap();
    let (entered, entering) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let active = leaf.clone();
    let worker = std::thread::spawn(move || {
        active.dispatch(|| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        })
    });
    entering.recv_timeout(Duration::from_secs(5)).unwrap();
    let first = owner.retirement();
    let (first_done, first_returned) = mpsc::channel();
    let first_worker = std::thread::spawn(move || {
        first.retire();
        first_done.send(()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !f
            .registry
            .state
            .lock()
            .unwrap()
            .origins
            .contains_key(&owner.id)
        {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(f.registry.state.try_lock().is_ok());
    assert!(first_returned.try_recv().is_err());
    f.registry
        .begin_mutation(&[RepositoryLifecycleKey::GitRoot(
            intent_core::WorkspaceGitRootId::new(),
        )])
        .unwrap()
        .settle_confirmed();
    let second = owner.retirement();
    let (started, starting) = mpsc::channel();
    let (done, returned) = mpsc::channel();
    let second_worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        second.retire();
        done.send(()).unwrap();
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    let premature = returned.recv_timeout(Duration::from_millis(50)).is_ok();
    release.send(()).unwrap();
    assert!(worker.join().unwrap().is_ok());
    first_worker.join().unwrap();
    second_worker.join().unwrap();
    assert!(
        !premature,
        "second hard-retire caller returned before original leaf retirement"
    );
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
    assert!(!current(&owner.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn original_allocation_cannot_initialize_another_database_with_the_same_observer() {
    let f = Fixture::new(None).await;
    let directory = tempfile::tempdir().unwrap();
    let foreign = Store::open(&directory.path().join("foreign.db"))
        .await
        .unwrap();
    foreign.insert_workspace(&f.workspace).await.unwrap();
    let agent = f.store.get_agent_session(&f.agent).await.unwrap();
    foreign.insert_agent_session(&agent).await.unwrap();
    f.registry.install(&foreign).await.unwrap();
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    assert!(creator
        .initialize(&foreign, || async { Ok("foreign".into()) })
        .await
        .is_err());
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
    assert!(foreign
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    let original = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize(&f.store, || async { Ok("original".into()) })
        .await
        .unwrap();
    assert!(current(&original.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn pending_retirement_during_original_producer_wait_blocks_late_initialization() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let pending = creator.callback();
    let retirement = creator.retirement();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut future = Box::pin(creator.initialize(&f.store, || async move {
        entered.send(()).unwrap();
        released.await.unwrap();
        Ok("created".into())
    }));
    tokio::select! {
        result = &mut future => panic!("producer returned before release: {}", result.is_ok()),
        result = entering => result.unwrap(),
    }
    retirement.retire();
    release.send(()).unwrap();
    assert!(matches!(future.await, Err(AdmissionError::Retired)));
    assert!(!current(&pending.capture(), f.caller()).await);
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn pending_retirement_after_commit_preserves_effect_without_live_confirmation() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let retirement = creator.retirement();
    let (claim, binding) = creator.claim_after_success("committed".into()).unwrap();
    let confirmation = f
        .store
        .initialize_repository_acp_session(claim, binding)
        .await
        .unwrap();
    retirement.retire();
    assert!(matches!(
        creator.consume(&f.store, confirmation),
        Err(AdmissionError::Retired)
    ));
    assert_eq!(
        f.store
            .get_agent_session(&f.agent)
            .await
            .unwrap()
            .acp_session_id
            .as_deref(),
        Some("committed")
    );
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn pending_retirement_never_settles_an_unknown_initialization_barrier() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let retirement = creator.retirement();
    // This fixture begins only the observer ticket, never a claimed SQL effect.
    drop(begin_fixture(&creator));
    retirement.retire();
    drop(creator);
    retirement.retire();
    assert_eq!(f.registry.state.lock().unwrap().pending.len(), 1);
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet
    )
    .is_err());
}

#[tokio::test]
async fn pending_retirement_is_weak_and_cannot_target_a_replacement_by_agent_id() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let id = creator.id;
    let retirement = creator.retirement();
    drop(creator);
    assert!(retirement.allocation.upgrade().is_none());
    assert!(!f.registry.state.lock().unwrap().creations.contains_key(&id));
    let replacement = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize(&f.store, || async { Ok("replacement".into()) })
        .await
        .unwrap();
    retirement.retire();
    assert!(current(&replacement.callback().capture(), f.caller()).await);
    let creator = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "replacement".into(),
    });
    let pending = creator.retirement();
    let physical = creator
        .initialize(&f.store, || async { Ok("replacement".into()) })
        .await
        .unwrap();
    let callback = physical.callback();
    assert!(pending.allocation.upgrade().is_some());
    drop(physical);
    assert!(pending.allocation.upgrade().is_none());
    assert!(!current(&callback.capture(), f.caller()).await);
}

#[tokio::test]
async fn every_pending_retirement_caller_joins_the_confirmed_original_leaf() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let pending = creator.retirement();
    let owner = creator
        .initialize(&f.store, || async { Ok("original".into()) })
        .await
        .unwrap();
    let leaf = crate::repository_admission::RepositoryRetirement::default();
    let _subscription = f
        .registry
        .subscribe(
            &owner.origin(),
            &f.caller(),
            &[RepositoryLifecycleKey::Database],
            leaf.clone(),
        )
        .unwrap();
    let (entered, entering) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let active = leaf.clone();
    let worker = std::thread::spawn(move || {
        active.dispatch(|| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        })
    });
    entering.recv_timeout(Duration::from_secs(5)).unwrap();
    let first = pending.clone();
    let first_worker = std::thread::spawn(move || first.retire());
    let deadline = Instant::now() + Duration::from_secs(5);
    while f
        .registry
        .state
        .lock()
        .unwrap()
        .origins
        .contains_key(&owner.id)
    {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(f.registry.state.try_lock().is_ok());
    assert!(pending
        .allocation
        .upgrade()
        .unwrap()
        .state
        .try_lock()
        .is_ok());
    f.registry
        .begin_mutation(&[RepositoryLifecycleKey::GitRoot(
            intent_core::WorkspaceGitRootId::new(),
        )])
        .unwrap()
        .settle_confirmed();
    let (started, starting) = mpsc::channel();
    let (done, returned) = mpsc::channel();
    let second_worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        pending.retire();
        done.send(()).unwrap();
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    let premature = returned.recv_timeout(Duration::from_millis(50)).is_ok();
    release.send(()).unwrap();
    assert!(worker.join().unwrap().is_ok());
    first_worker.join().unwrap();
    second_worker.join().unwrap();
    assert!(
        !premature,
        "pending pre-abort caller returned before original leaf retirement"
    );
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
    assert!(!current(&owner.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn outcome_retains_nonclone_producer_payload_and_original_commit() {
    struct Payload(Arc<()>);
    let f = Fixture::new(None).await;
    let marker = Arc::new(());
    let payload = Payload(marker.clone());
    let executions = std::sync::atomic::AtomicUsize::new(0);
    let count = &executions;
    let outcome = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_with_outcome(&f.store, || async move {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, AdmissionError>(("created".into(), payload))
        })
        .await;
    assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&outcome.producer.unwrap().0, &marker));
    assert_eq!(
        outcome.persistence,
        RepositoryInitializationPersistence::Committed {
            session_id: "created".into()
        }
    );
    let owner = outcome.owner.unwrap();
    assert!(current(&owner.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn outcome_preserves_original_producer_error_without_attempting_store() {
    #[derive(Debug)]
    struct ProducerError(&'static str);
    let f = Fixture::new(None).await;
    let result = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_with_outcome(&f.store, || async {
            Err::<(String, ()), _>(ProducerError("original provider failure"))
        })
        .await;
    assert_eq!(result.producer.unwrap_err().0, "original provider failure");
    assert_eq!(
        result.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(matches!(result.owner, Err(AdmissionError::Unavailable)));
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn outcome_preserves_producer_success_after_pending_retirement_without_repairing_claim() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let retirement = creator.retirement();
    let result = creator
        .initialize_with_outcome(&f.store, || async {
            retirement.retire();
            Ok::<_, AdmissionError>(("produced".into(), "original response"))
        })
        .await;
    assert_eq!(result.producer.unwrap(), "original response");
    assert_eq!(
        result.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(matches!(result.owner, Err(AdmissionError::Retired)));
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
}

#[tokio::test]
async fn outcome_keeps_actual_committed_effect_when_finish_or_consume_rejects_owner() {
    for retire_during_commit in [true, false] {
        let f = Fixture::new(None).await;
        let creator = f.creator(RepositoryCreationIntent::FirstSet);
        let retirement = creator.retirement();
        let result: RepositoryCreationOutcome<&str, AdmissionError> = if retire_during_commit {
            let mut conn = f.store.write_pool().acquire().await.unwrap();
            conn.lock_handle().await.unwrap().set_commit_hook(move || {
                retirement.retire();
                true
            });
            drop(conn);
            creator
                .initialize_with_outcome(&f.store, || async {
                    Ok(("committed".into(), "actual response"))
                })
                .await
        } else {
            // Actual transaction receipt, then retirement before its one-use
            // consumption. This deliberately exercises the narrow completion edge.
            let (claim, binding) = creator.claim_after_success("committed".into()).unwrap();
            let receipt = f
                .store
                .initialize_repository_acp_session_outcome(claim, binding)
                .await;
            retirement.retire();
            creator.complete_outcome(&f.store, "actual response", receipt)
        };
        assert_eq!(result.producer.unwrap(), "actual response");
        assert_eq!(
            result.persistence,
            RepositoryInitializationPersistence::Committed {
                session_id: "committed".into()
            }
        );
        assert!(result.owner.is_err());
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("committed")
        );
        assert!(f.registry.state.lock().unwrap().pending.is_empty());
        assert!(f.registry.state.lock().unwrap().origins.is_empty());
    }
}

#[tokio::test]
async fn outcome_no_effect_preserves_only_transaction_observations_and_strict_behavior() {
    use intent_store::RepositoryInitializationObservation;
    for (stored, intent, missing, confirmed) in [
        (
            Some("winner"),
            RepositoryCreationIntent::FirstSet,
            false,
            false,
        ),
        (
            Some("winner"),
            RepositoryCreationIntent::Replace {
                expected: Some("old".into()),
            },
            false,
            false,
        ),
        (
            None,
            RepositoryCreationIntent::Replace {
                expected: Some("old".into()),
            },
            false,
            false,
        ),
        (
            Some("fresh"),
            RepositoryCreationIntent::Replace {
                expected: Some("fresh".into()),
            },
            false,
            false,
        ),
        (
            Some("fresh"),
            RepositoryCreationIntent::Loaded {
                session_id: "fresh".into(),
            },
            false,
            true,
        ),
        (None, RepositoryCreationIntent::FirstSet, true, false),
    ] {
        let mut f = Fixture::new(stored).await;
        if missing {
            f.agent = AgentId::new();
        }
        let result = f
            .creator(intent)
            .initialize_with_outcome(&f.store, || async {
                Ok::<_, AdmissionError>(("fresh".into(), "original response"))
            })
            .await;
        assert_eq!(result.producer.unwrap(), "original response");
        assert_eq!(
            result.persistence,
            RepositoryInitializationPersistence::NoEffect {
                observed: if missing {
                    RepositoryInitializationObservation::Missing
                } else {
                    RepositoryInitializationObservation::Present {
                        session_id: stored.map(str::to_owned),
                    }
                }
            }
        );
        assert_eq!(result.owner.is_ok(), confirmed);
        if !missing {
            assert_eq!(
                f.store
                    .get_agent_session(&f.agent)
                    .await
                    .unwrap()
                    .acp_session_id
                    .as_deref(),
                stored
            );
        }
    }
}

#[tokio::test]
async fn outcome_after_actual_sql_failure_remains_unknown_and_retains_original_barrier() {
    let f = Fixture::new(None).await;
    sqlx::query("CREATE TRIGGER reject_initialization AFTER UPDATE OF acp_session_id ON agent_session BEGIN SELECT RAISE(ROLLBACK,'fixture failure'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let result = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, AdmissionError>(("attempted".into(), "original response"))
        })
        .await;
    assert_eq!(result.producer.unwrap(), "original response");
    assert_eq!(
        result.persistence,
        RepositoryInitializationPersistence::Unknown
    );
    assert!(result.owner.is_err());
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert_eq!(f.registry.state.lock().unwrap().pending.len(), 1);
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet
    )
    .is_err());
}

#[tokio::test]
async fn outcome_rejects_foreign_store_without_losing_the_actual_producer_result() {
    let f = Fixture::new(None).await;
    let foreign = Store::open(&f.dir.path().join("foreign.db")).await.unwrap();
    foreign.insert_workspace(&f.workspace).await.unwrap();
    foreign
        .insert_agent_session(&f.store.get_agent_session(&f.agent).await.unwrap())
        .await
        .unwrap();
    f.registry.install(&foreign).await.unwrap();
    let result = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_with_outcome(&foreign, || async {
            Ok::<_, AdmissionError>(("produced".into(), "original response"))
        })
        .await;
    assert_eq!(result.producer.unwrap(), "original response");
    assert_eq!(
        result.persistence,
        RepositoryInitializationPersistence::NotAttempted
    );
    assert!(result.owner.is_err());
    for store in [&f.store, &foreign] {
        assert!(store
            .get_agent_session(&f.agent)
            .await
            .unwrap()
            .acp_session_id
            .is_none());
    }
}

#[tokio::test]
async fn compatible_original_producer_payload_and_fresh_callback_survive_once() {
    struct Payload(Arc<()>);
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let pending = creator.callback();
    let marker = Arc::new(());
    let payload = Payload(marker.clone());
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let count = &calls;
    let outcome = creator
        .initialize_compatible(|| async move {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, AdmissionError>(("created".into(), payload))
        })
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&outcome.producer.unwrap().0, &marker));
    assert_eq!(
        outcome.result.unwrap(),
        RepositoryAcpCompatibilityResult::Committed {
            session_id: "created".into(),
            effect: RepositoryAcpCompatibilityEffect::FirstSet,
        }
    );
    assert_eq!(
        outcome.persistence,
        RepositoryAcpCompatibilityPersistence::Committed {
            observed: RepositoryInitializationObservation::Present {
                session_id: Some("created".into())
            },
            affected_rows: 1,
        }
    );
    let owner = outcome.owner.unwrap();
    assert!(!current(&pending.capture(), f.caller()).await);
    assert!(current(&owner.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn compatible_original_producer_error_never_becomes_a_store_attempt() {
    #[derive(Debug)]
    struct ProducerError(Arc<()>);
    let f = Fixture::new(None).await;
    let marker = Arc::new(());
    let error = ProducerError(marker.clone());
    let outcome = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_compatible(|| async move { Err::<(String, ()), _>(error) })
        .await;
    assert!(Arc::ptr_eq(&outcome.producer.unwrap_err().0, &marker));
    assert_eq!(
        outcome.persistence,
        RepositoryAcpCompatibilityPersistence::NotAttempted
    );
    assert!(outcome.result.is_err());
    assert!(outcome.owner.is_err());
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert!(f.registry.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn compatible_setup_writes_preserve_results_and_retire_only_binding_changes() {
    for setup in 0..3 {
        let f = Fixture::new(None).await;
        let creator = f.creator(RepositoryCreationIntent::FirstSet);
        let pending = creator.callback();
        let retirement = creator.retirement();
        let outcome = creator
            .initialize_compatible(|| async {
                if setup == 2 {
                    f.store
                        .set_agent_session_system_prompt(
                            &f.workspace.id,
                            &f.agent,
                            "original prompt",
                        )
                        .await
                        .unwrap();
                } else if setup == 1 {
                    f.store
                        .set_agent_session_model(
                            &f.workspace.id,
                            &f.agent,
                            "different-model",
                            None,
                            "2026-09-27T02:00:00Z",
                        )
                        .await
                        .unwrap();
                } else {
                    retirement.retire();
                }
                Ok::<_, AdmissionError>(("ordinary".into(), "actual original response"))
            })
            .await;
        assert_eq!(outcome.producer.unwrap(), "actual original response");
        assert_eq!(
            outcome.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: "ordinary".into(),
                effect: RepositoryAcpCompatibilityEffect::FirstSet,
            }
        );
        assert!(matches!(
            outcome.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        let owner = outcome.owner.ok();
        assert_eq!(owner.is_some(), setup == 2);
        let row = f.store.get_agent_session(&f.agent).await.unwrap();
        assert_eq!(row.acp_session_id.as_deref(), Some("ordinary"));
        if setup == 2 {
            assert_eq!(row.system_prompt.as_deref(), Some("original prompt"));
        }
        assert!(!current(&pending.capture(), f.caller()).await);
        assert!(f.registry.state.lock().unwrap().pending.is_empty());
        drop(owner);
        assert!(f.registry.state.lock().unwrap().origins.is_empty());
    }
}

#[tokio::test]
async fn compatible_same_id_and_current_null_accounting_commit_without_owner() {
    for same_id in [false, true] {
        let f = Fixture::new(same_id.then_some("original")).await;
        let totals = intent_core::TokenUsageTotals {
            input_tokens: 37,
            ..Default::default()
        };
        f.store
            .set_agent_session_token_usage(&f.workspace.id, &f.agent, &totals)
            .await
            .unwrap();
        let creator = f.creator(RepositoryCreationIntent::Replace {
            expected: Some("original".into()),
        });
        let pending = creator.callback();
        let session = if same_id { "original" } else { "replacement" };
        let outcome = creator
            .initialize_compatible(|| async {
                Ok::<_, AdmissionError>((session.into(), "response"))
            })
            .await;
        assert_eq!(outcome.producer.unwrap(), "response");
        assert_eq!(
            outcome.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: session.into(),
                effect: if same_id {
                    RepositoryAcpCompatibilityEffect::AccountingOnly
                } else {
                    RepositoryAcpCompatibilityEffect::Replace { previous: None }
                },
            }
        );
        assert!(matches!(
            outcome.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(outcome.owner.is_err());
        let rows = f
            .store
            .get_workspace_agent_usage_data(&f.workspace.id)
            .await
            .unwrap();
        assert!(rows[0].2.is_none());
        assert_eq!(rows[0].3.as_ref(), Some(&totals));
        assert!(!current(&pending.capture(), f.caller()).await);
        assert!(f.registry.state.lock().unwrap().pending.is_empty());
        assert!(f.registry.state.lock().unwrap().origins.is_empty());
        // Only a distinct successful load can obtain a later owner.
        let fresh = f
            .creator(RepositoryCreationIntent::Loaded {
                session_id: session.into(),
            })
            .initialize_compatible(|| async { Ok::<_, AdmissionError>((session.into(), ())) })
            .await;
        assert!(fresh.owner.is_ok());
        let rows = f
            .store
            .get_workspace_agent_usage_data(&f.workspace.id)
            .await
            .unwrap();
        assert_eq!(rows[0].3.as_ref(), Some(&totals));
    }
}

#[tokio::test]
async fn compatible_observed_canonical_and_submitted_fallback_are_never_proof() {
    let f = Fixture::new(Some("winner")).await;
    let replaced = f
        .creator(RepositoryCreationIntent::Replace {
            expected: Some("stale".into()),
        })
        .initialize_compatible(|| async {
            Ok::<_, AdmissionError>(("submitted".into(), "original response"))
        })
        .await;
    assert_eq!(replaced.producer.unwrap(), "original response");
    assert_eq!(
        replaced.result.unwrap(),
        RepositoryAcpCompatibilityResult::Observed {
            session_id: "winner".into()
        }
    );
    assert_eq!(
        replaced.persistence,
        RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::Present {
                session_id: Some("winner".into())
            },
        }
    );
    assert!(replaced.owner.is_err());
    let creator = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "winner".into(),
    });
    f.store
        .delete_agent_session(&f.workspace.id, &f.agent)
        .await
        .unwrap();
    let missing = creator
        .initialize_compatible(|| async {
            Ok::<_, AdmissionError>(("winner".into(), "load response"))
        })
        .await;
    assert_eq!(missing.producer.unwrap(), "load response");
    assert_eq!(
        missing.result.unwrap(),
        RepositoryAcpCompatibilityResult::SubmittedFallback {
            session_id: "winner".into()
        }
    );
    assert_eq!(
        missing.persistence,
        RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::Missing
        }
    );
    assert!(missing.owner.is_err());
    assert!(f.registry.state.lock().unwrap().origins.is_empty());
}

#[tokio::test]
async fn compatible_committed_effect_survives_retired_confirmation_or_consumption() {
    for during_commit in [false, true] {
        let f = Fixture::new(None).await;
        let creator = f.creator(RepositoryCreationIntent::FirstSet);
        let retirement = creator.retirement();
        let outcome: RepositoryCompatibleCreationOutcome<&str, AdmissionError> = if during_commit {
            let mut conn = f.store.write_pool().acquire().await.unwrap();
            conn.lock_handle().await.unwrap().set_commit_hook(move || {
                retirement.retire();
                true
            });
            drop(conn);
            creator
                .initialize_compatible(|| async { Ok(("committed".into(), "original")) })
                .await
        } else {
            let (claim, binding) = creator.claim_after_success("committed".into()).unwrap();
            let outcome = f
                .store
                .initialize_repository_acp_session_compatible(Some(claim), binding)
                .await;
            retirement.retire();
            creator.complete_compatible("original", outcome)
        };
        assert_eq!(outcome.producer.unwrap(), "original");
        assert!(matches!(
            outcome.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(matches!(
            outcome.result,
            Ok(RepositoryAcpCompatibilityResult::Committed { .. })
        ));
        assert!(outcome.owner.is_err());
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("committed")
        );
        assert!(f.registry.state.lock().unwrap().pending.is_empty());
        assert!(f.registry.state.lock().unwrap().origins.is_empty());
    }
}

#[tokio::test]
async fn compatible_failed_original_begin_keeps_its_error_and_other_barrier() {
    let f = Fixture::new(None).await;
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let (claim, binding) = creator.claim_after_success("ordinary".into()).unwrap();
    let unknown = f
        .registry
        .begin_mutation(&[RepositoryLifecycleKey::Agent(f.agent.clone())])
        .unwrap();
    let barriers = f
        .registry
        .state
        .lock()
        .unwrap()
        .pending
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    drop(unknown);
    let stored = f
        .store
        .initialize_repository_acp_session_compatible(Some(claim), binding)
        .await;
    let original_error = stored.confirmation.as_ref().err().unwrap().to_string();
    let outcome: RepositoryCompatibleCreationOutcome<&str, AdmissionError> =
        creator.complete_compatible("response", stored);
    assert_eq!(outcome.owner.err().unwrap().to_string(), original_error);
    assert_eq!(outcome.producer.unwrap(), "response");
    assert!(matches!(
        outcome.result,
        Ok(RepositoryAcpCompatibilityResult::Committed { .. })
    ));
    assert!(matches!(
        outcome.persistence,
        RepositoryAcpCompatibilityPersistence::Committed {
            affected_rows: 1,
            ..
        }
    ));
    assert_eq!(
        f.registry
            .state
            .lock()
            .unwrap()
            .pending
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        barriers
    );
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet
    )
    .is_err());
}

#[tokio::test]
async fn compatible_sql_failure_preserves_unknown_receipt_and_original_barrier() {
    let f = Fixture::new(None).await;
    sqlx::query("CREATE TRIGGER compatibility_fault AFTER UPDATE OF acp_session_id ON agent_session BEGIN SELECT RAISE(ROLLBACK,'original compatibility failure'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let outcome = f
        .creator(RepositoryCreationIntent::FirstSet)
        .initialize_compatible(|| async {
            Ok::<_, AdmissionError>(("produced".into(), "producer response"))
        })
        .await;
    assert_eq!(outcome.producer.unwrap(), "producer response");
    assert_eq!(
        outcome.persistence,
        RepositoryAcpCompatibilityPersistence::Unknown
    );
    assert!(outcome
        .result
        .err()
        .unwrap()
        .to_string()
        .contains("original compatibility failure"));
    assert!(outcome.owner.is_err());
    assert!(f
        .store
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert_eq!(f.registry.state.lock().unwrap().pending.len(), 1);
    assert!(RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        f.agent.clone(),
        RepositoryCreationIntent::FirstSet
    )
    .is_err());
}

#[tokio::test]
async fn compatible_wrong_loaded_producer_preserves_response_without_changing_intent() {
    let f = Fixture::new(Some("original")).await;
    let outcome = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "original".into(),
        })
        .initialize_compatible(|| async {
            Ok::<_, AdmissionError>(("different".into(), "original response"))
        })
        .await;
    assert_eq!(outcome.producer.unwrap(), "original response");
    assert_eq!(
        outcome.persistence,
        RepositoryAcpCompatibilityPersistence::NotAttempted
    );
    assert!(outcome.result.is_err());
    assert!(outcome.owner.is_err());
    assert_eq!(
        f.store
            .get_agent_session(&f.agent)
            .await
            .unwrap()
            .acp_session_id
            .as_deref(),
        Some("original")
    );
}

#[tokio::test]
async fn compatible_uses_only_captured_store_even_with_same_observer_and_foreign_ids() {
    let f = Fixture::new(None).await;
    let foreign = Store::open(&f.dir.path().join("foreign-compatibility.db"))
        .await
        .unwrap();
    foreign.insert_workspace(&f.workspace).await.unwrap();
    let row = f.store.get_agent_session(&f.agent).await.unwrap();
    foreign.insert_agent_session(&row).await.unwrap();
    f.registry.install(&foreign).await.unwrap();
    let creator = f.creator(RepositoryCreationIntent::FirstSet);
    let outcome = creator
        .initialize_compatible(|| async {
            Ok::<_, AdmissionError>(("original database".into(), "response"))
        })
        .await;
    assert_eq!(outcome.producer.unwrap(), "response");
    assert!(outcome.owner.is_ok());
    assert!(foreign
        .get_agent_session(&f.agent)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
    assert_eq!(
        f.store
            .get_agent_session(&f.agent)
            .await
            .unwrap()
            .acp_session_id
            .as_deref(),
        Some("original database")
    );
}
