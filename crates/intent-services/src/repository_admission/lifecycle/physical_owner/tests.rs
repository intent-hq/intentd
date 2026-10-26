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
    entering
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_secs(5)))
        .unwrap();
    let first = owner.retirement();
    let (first_done, first_returned) = mpsc::channel();
    let first_worker = std::thread::spawn(move || {
        first.retire();
        first_done.send(()).unwrap();
    });
    let deadline = Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(5));
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
    starting
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_secs(5)))
        .unwrap();
    let premature = returned
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_millis(
            50,
        )))
        .is_ok();
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
    entering
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_secs(5)))
        .unwrap();
    let first = pending.clone();
    let first_worker = std::thread::spawn(move || first.retire());
    let deadline = Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(5));
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
    starting
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_secs(5)))
        .unwrap();
    let premature = returned
        .recv_timeout(intentd_test_support::test_timeout(Duration::from_millis(
            50,
        )))
        .is_ok();
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

#[tokio::test]
async fn workspace_and_database_mutations_retire_live_and_pending_original_owners() {
    for database in [false, true] {
        let f = Fixture::new(None).await;
        let owner = f
            .creator(RepositoryCreationIntent::FirstSet)
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .unwrap();
        let request = owner.callback().capture();
        let pending = f.creator(RepositoryCreationIntent::Loaded {
            session_id: "original".into(),
        });
        let key = if database {
            RepositoryLifecycleKey::Database
        } else {
            RepositoryLifecycleKey::Workspace(f.workspace.id.clone())
        };
        // Explicit original observer boundary: no SQL effect is asserted here.
        let mutation = f.registry.begin_mutation(&[key]).unwrap();
        assert!(!current(&request, f.caller()).await);
        assert!(RepositoryCreationOwner::allocate(
            &f.registry,
            &f.store,
            f.workspace.id.clone(),
            f.agent.clone(),
            RepositoryCreationIntent::Loaded {
                session_id: "original".into()
            }
        )
        .is_err());
        mutation.settle_confirmed();
        assert!(!current(&owner.callback().capture(), f.caller()).await);
        assert!(pending
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .is_err());
        let fresh = f
            .creator(RepositoryCreationIntent::Loaded {
                session_id: "original".into(),
            })
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .unwrap();
        assert!(current(&fresh.callback().capture(), f.caller()).await);
    }
}

#[tokio::test]
async fn private_native_family_original_startup_and_load_keep_distinct_attempts() {
    let f = Fixture::new(None).await;
    let original = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let first = serde_json::to_value(original.context()).unwrap();
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent, "native")
        .await
        .unwrap()
        .is_none());
    let outcome = original
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, AdmissionError>(("native".into(), ()))
        })
        .await
        .unwrap();
    let owner = outcome.owner.unwrap();
    assert!(current(&owner.callback().capture(), f.caller()).await);
    let loaded = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "native".into(),
        })
        .prepare_review()
        .await
        .unwrap();
    let second = serde_json::to_value(loaded.context()).unwrap();
    assert_eq!(first["ownerKey"], second["ownerKey"]);
    assert_ne!(first["loadAttemptId"], second["loadAttemptId"]);
    let outcome = loaded
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, AdmissionError>(("native".into(), ()))
        })
        .await
        .unwrap();
    let loaded_owner = outcome.owner.unwrap();
    assert!(!current(&owner.callback().capture(), f.caller()).await);
    assert!(current(&loaded_owner.callback().capture(), f.caller()).await);
}

#[tokio::test]
async fn private_native_family_missing_and_retired_original_never_backfill() {
    let f = Fixture::new(Some("legacy")).await;
    assert!(f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "legacy".into()
        })
        .prepare_review()
        .await
        .is_err());
    let creator = f.creator(RepositoryCreationIntent::Replace {
        expected: Some("legacy".into()),
    });
    let retirement = creator.retirement();
    let original = creator.prepare_review().await.unwrap();
    retirement.retire();
    let ran = std::sync::atomic::AtomicBool::new(false);
    let outcome = original
        .initialize_with_outcome(&f.store, || async {
            ran.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, AdmissionError>(("replacement".into(), ()))
        })
        .await;
    assert!(outcome.is_err());
    assert!(
        !ran.load(std::sync::atomic::Ordering::SeqCst),
        "retired startup must not call its producer"
    );
    assert!(f
        .store
        .get_review_context_enrollment(&f.workspace.id, &f.agent, "replacement")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn private_native_family_rewind_invalidates_loaded_candidate_and_original_owner() {
    let f = Fixture::new(None).await;
    let original = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let outcome = original
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, AdmissionError>(("native".into(), ()))
        })
        .await
        .unwrap();
    let owner = outcome.owner.unwrap();
    let loaded = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "native".into(),
        })
        .prepare_review()
        .await
        .unwrap();
    f.store.replace_agent_messages(&f.agent, &[]).await.unwrap();
    assert!(!current(&owner.callback().capture(), f.caller()).await);
    let outcome = loaded
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, AdmissionError>(("native".into(), ()))
        })
        .await;
    assert!(outcome.is_err());
}

#[tokio::test]
async fn private_history_bootstrap_is_pending_only_and_cannot_upgrade_a_callback() {
    let f = Fixture::new(None).await;
    let creation = f.creator(RepositoryCreationIntent::FirstSet);
    let callback = creation.callback();
    let review = creation.prepare_review().await.unwrap();
    let bootstrap = review.bootstrap().unwrap();
    assert_eq!(bootstrap.workspace(), &f.workspace.id);
    assert_eq!(bootstrap.agent(), &f.agent);
    bootstrap.with_current(|| Ok(())).unwrap();
    assert!(!current(&callback.capture(), f.caller()).await);
    let outcome = review
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, ()>(("private-original".into(), ()))
        })
        .await
        .unwrap();
    let owner = outcome.owner.unwrap();
    assert!(
        bootstrap.with_current(|| Ok(())).is_err(),
        "confirmation upgraded a bootstrap capture"
    );
    assert!(!current(&callback.capture(), f.caller()).await);
    assert!(current(&owner.callback().capture(), f.caller()).await);
    let next = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "private-original".into(),
        })
        .prepare_review()
        .await
        .unwrap();
    let fresh = next.bootstrap().unwrap();
    assert!(bootstrap.with_current(|| Ok(())).is_err());
    fresh.with_current(|| Ok(())).unwrap();
    drop(next);
    assert!(
        fresh.with_current(|| Ok(())).is_err(),
        "dropped original startup remained eligible"
    );
}

#[tokio::test]
async fn private_history_bootstrap_mutation_joins_original_transfer_and_preserves_sibling() {
    let f = Fixture::new(None).await;
    let review = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let bootstrap = review.bootstrap().unwrap();
    let sibling_id = AgentId::new();
    let mut sibling_row = f.store.get_agent_session(&f.agent).await.unwrap();
    sibling_row.id = sibling_id.clone();
    f.store.insert_agent_session(&sibling_row).await.unwrap();
    let sibling = RepositoryCreationOwner::allocate(
        &f.registry,
        &f.store,
        f.workspace.id.clone(),
        sibling_id,
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap()
    .prepare_review()
    .await
    .unwrap();
    let sibling_capture = sibling.bootstrap().unwrap();
    let (entered, started) = std::sync::mpsc::sync_channel(0);
    let (release, held) = std::sync::mpsc::sync_channel(0);
    let original = bootstrap.clone();
    let transfer = std::thread::spawn(move || {
        original.with_current(|| {
            entered.send(()).unwrap();
            held.recv().unwrap();
            Ok(())
        })
    });
    started.recv().unwrap();
    let registry = f.registry.clone();
    let key = RepositoryLifecycleKey::MonitorHistory(f.agent.clone());
    let (mutating, mutation_started) = std::sync::mpsc::sync_channel(0);
    let (joined, joined_result) = std::sync::mpsc::channel();
    let mutation = std::thread::spawn(move || {
        mutating.send(()).unwrap();
        let ticket = registry.begin_mutation(&[key]).unwrap();
        joined.send(()).unwrap();
        ticket
    });
    mutation_started.recv().unwrap();
    assert!(joined_result
        .recv_timeout(intentd_test_support::test_timeout(
            std::time::Duration::from_millis(30)
        ))
        .is_err());
    release.send(()).unwrap();
    transfer.join().unwrap().unwrap();
    joined_result
        .recv_timeout(intentd_test_support::test_timeout(
            std::time::Duration::from_secs(5),
        ))
        .unwrap();
    let ticket = mutation.join().unwrap();
    assert!(bootstrap.with_current(|| Ok(())).is_err());
    sibling_capture.with_current(|| Ok(())).unwrap();
    drop(ticket);
    assert!(
        bootstrap.with_current(|| Ok(())).is_err(),
        "mutation completion revived old capture"
    );
    assert!(bootstrap
        .subscribe(&[RepositoryLifecycleKey::MonitorHistory(f.agent.clone())])
        .is_err());
}

async fn committed_history_correlation(
    f: &Fixture,
    id: &str,
    account: &str,
    number: u64,
    instance: &str,
) -> intent_acp::review_monitoring::history::ReviewHistoryMessage {
    use intent_core::{
        PrMonitor, PrMonitorId, PrMonitorState, RepositoryProvider, RepositoryResourceKind,
        RepositoryTarget, ReviewTarget,
    };
    let target = ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: instance.into(),
            project_path: "o/r".into(),
        },
        kind: RepositoryResourceKind::MergeRequest,
        number,
    };
    let binding: intent_core::review_monitor::ReviewMonitorBinding = serde_json::from_value(serde_json::json!({
        "scope":{"daemonId":"original-daemon","authorityScopeId":"original-owner","authorityGeneration":"3"},
        "connection":{"connectionId":"original-connection","accountId":account,"connectionGeneration":"7"},
        "resource":target,
    })).unwrap();
    let now = intent_core::now_iso();
    let monitor = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: f.workspace.id.clone(),
        agent_id: f.agent.clone(),
        repo_owner: "o".into(),
        repo_name: "r".into(),
        pr_number: i64::try_from(number).unwrap(),
        state: PrMonitorState::Active,
        last_snapshot: Some(
            serde_json::json!({"monitorBinding":binding,"qualifiedSnapshot":{"state":"open"}})
                .to_string(),
        ),
        baseline_snapshot: Some("{}".into()),
        pending_changes: vec!["change".into()],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    assert!(f
        .store
        .insert_qualified_pr_monitor(
            &monitor,
            &target,
            intent_store::MonitorTargetProvenance::CapturedRequest
        )
        .await
        .unwrap());
    let row = f
        .store
        .get_qualified_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    let metadata = serde_json::from_value(serde_json::json!({
        "type":"pr_monitor_wake","monitorId":monitor.monitor_id,"repo":"o/r","prNumber":number,"reason":"changes","resource":target,
    })).unwrap();
    let text = format!("Original private observation {id}");
    let prepared = f
        .store
        .prepare_monitor_message(&row, &binding, id, &text, &metadata, &now)
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || prepared.commit())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let proof = f
        .store
        .get_monitor_message_provenance(&f.agent, id)
        .await
        .unwrap()
        .unwrap();
    let store = proof
        .envelope_sha256
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            std::fmt::Write::write_fmt(&mut output, format_args!("{byte:02x}"))
                .expect("writing to a String cannot fail");
            output
        });
    let params = intent_acp::review_monitoring::payload::classify_prompt(
        serde_json::json!({"sessionId":"private-original","prompt":[{"type":"text","text":text}]}),
        id,
        &store,
    )
    .unwrap();
    intent_acp::review_monitoring::history::ReviewHistoryMessage {
        message_id: id.into(),
        store_envelope_digest: store,
        acp_payload_digest: params["_meta"]["reviewMonitoringMessage"]["acpPayloadDigest"]
            .as_str()
            .unwrap()
            .into(),
    }
}

#[tokio::test]
async fn private_history_bootstrap_retains_complete_original_envelopes_without_account_grants() {
    let f = Fixture::new(None).await;
    let initial = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    assert!(
        initial
            .bootstrap()
            .unwrap()
            .verify_history(&[])
            .await
            .is_err(),
        "new startup has no loaded history family"
    );
    let created = initial
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, ()>(("private-original".into(), ()))
        })
        .await
        .unwrap();
    let _owner = created.owner.unwrap();
    let a =
        committed_history_correlation(&f, "history-a", "account-a", 42, "https://gitlab.example")
            .await;
    let b =
        committed_history_correlation(&f, "history-b", "account-b", 43, "https://gitlab.example")
            .await;
    let loaded = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "private-original".into(),
        })
        .prepare_review()
        .await
        .unwrap();
    let bootstrap = loaded.bootstrap().unwrap();
    let verified = bootstrap
        .verify_history(&[a.clone(), b.clone()])
        .await
        .unwrap();
    assert_eq!(verified.envelopes().len(), 2);
    assert_eq!(
        verified.envelopes()[0].binding.connection.account_id,
        "account-a"
    );
    assert_eq!(
        verified.envelopes()[1].binding.connection.account_id,
        "account-b"
    );
    // No current monitor lookup may replace original message evidence.
    for mutation in ["store", "payload", "missing", "duplicate", "destination"] {
        let mut altered = b.clone();
        match mutation {
            "store" => altered.store_envelope_digest = "0".repeat(64),
            "payload" => altered.acp_payload_digest = "0".repeat(64),
            "missing" => altered.message_id = "unknown-original".into(),
            "duplicate" => altered = a.clone(),
            "destination" => {
                let params = intent_acp::review_monitoring::payload::classify_prompt(
                    serde_json::json!({"sessionId":"other-session","prompt":[{"type":"text","text":"Original private observation history-b"}]}),
                    &b.message_id, &b.store_envelope_digest,
                ).unwrap();
                altered.acp_payload_digest = params["_meta"]["reviewMonitoringMessage"]
                    ["acpPayloadDigest"]
                    .as_str()
                    .unwrap()
                    .into();
            }
            _ => unreachable!(),
        }
        assert!(
            bootstrap
                .verify_history(&[a.clone(), altered])
                .await
                .is_err(),
            "{mutation} was partially accepted"
        );
    }
    verified.with_current(|| Ok(())).unwrap();
    let exact_messages = verified
        .envelopes()
        .iter()
        .map(|proof| proof.message.clone())
        .collect::<Vec<_>>();
    let replacement = exact_messages
        .iter()
        .map(|message| intent_store::ReplaceMessage {
            role: &message.role,
            content: &message.content,
            metadata: message.metadata.as_ref(),
            created_at: &message.created_at,
        })
        .collect::<Vec<_>>();
    f.store
        .replace_agent_messages(&f.agent, &replacement)
        .await
        .unwrap();
    assert!(
        verified.with_current(|| Ok(())).is_err(),
        "identical replacement retained local eligibility"
    );
    assert!(bootstrap.verify_history(&[a, b]).await.is_err());
    assert!(f
        .store
        .get_monitor_message_provenance(&f.agent, "history-a")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn private_history_bootstrap_fresh_account_admission_retires_without_stored_grants() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let store = auth.service.store.clone();
    let mut workspace = chief_workspace();
    workspace.id = WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(serde_json::json!({
        "id":agent,"workspaceId":workspace.id,"name":"history bootstrap","status":"active",
        "createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
    }))
    .unwrap();
    store.insert_agent_session(&row).await.unwrap();
    let registry = auth.service.repository_lifecycle_registry().await.unwrap();
    let f = Fixture {
        dir: tempfile::tempdir().unwrap(),
        store,
        workspace,
        registry,
        agent,
    };
    let initial = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let outcome = initial
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, ()>(("private-original".into(), ()))
        })
        .await
        .unwrap();
    let _owner = outcome.owner.unwrap();
    let selected = auth.service.gitlab_repository_settled_connection().unwrap();
    let account = selected.selected().binding.scope.account_id.clone();
    let instance = server.descriptor.instance().as_str();
    let a = committed_history_correlation(&f, "source-a", &account, 42, instance).await;
    let b =
        committed_history_correlation(&f, "source-b", "other-original-account", 43, instance).await;
    let loaded = f
        .creator(RepositoryCreationIntent::Loaded {
            session_id: "private-original".into(),
        })
        .prepare_review()
        .await
        .unwrap();
    let bootstrap = loaded.bootstrap().unwrap();
    assert!(
        intent_core::caller::current_caller().is_none(),
        "bootstrap must not fabricate a caller"
    );
    let calls = server.control.requests.lock().unwrap().len();
    let admitted = bootstrap
        .verify_history(std::slice::from_ref(&a))
        .await
        .unwrap()
        .admit(auth.service.clone())
        .await
        .unwrap();
    let mut delivered = 0;
    admitted
        .with_current(|| {
            delivered += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(delivered, 1);
    assert!(
        bootstrap
            .verify_history(&[a.clone(), b])
            .await
            .unwrap()
            .admit(auth.service.clone())
            .await
            .is_err(),
        "a matching first account cannot omit the other source"
    );
    assert_eq!(
        server.control.requests.lock().unwrap().len(),
        calls,
        "history admission must not fetch provider content"
    );
    let unrelated = AuthFixture::new(&server).await;
    assert!(
        bootstrap
            .verify_history(std::slice::from_ref(&a))
            .await
            .unwrap()
            .admit(unrelated.service.clone())
            .await
            .is_err(),
        "foreign Store cannot supply authority"
    );
    // Explicit authenticated account mutations are independent of the callerless
    // bootstrap. Directory::retire is terminal daemon shutdown, not replacement.
    with_caller(
        Caller::Daemon,
        auth.service
            .gitlab_connect_pat(server.host.clone(), "pat-second".into()),
    )
    .await
    .unwrap();
    assert!(admitted
        .with_current(|| {
            delivered += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(delivered, 1);
    assert!(
        bootstrap
            .verify_history(std::slice::from_ref(&a))
            .await
            .unwrap()
            .admit(auth.service.clone())
            .await
            .is_err(),
        "account B cannot admit original account A history"
    );
    with_caller(
        Caller::Daemon,
        auth.service
            .gitlab_connect_pat(server.host.clone(), "stored-pat".into()),
    )
    .await
    .unwrap();
    assert!(
        admitted.with_current(|| Ok(())).is_err(),
        "fresh same-account verification cannot revive an old source receipt"
    );
    let fresh = bootstrap
        .verify_history(std::slice::from_ref(&a))
        .await
        .unwrap()
        .admit(auth.service.clone())
        .await
        .unwrap();
    fresh.with_current(|| Ok(())).unwrap();
    for variant in [
        "live",
        "changed-target",
        "account-replaced",
        "retained-original",
    ] {
        let attempt = f
            .creator(RepositoryCreationIntent::Loaded {
                session_id: "private-original".into(),
            })
            .prepare_review()
            .await
            .unwrap();
        let attempt_bootstrap = attempt.bootstrap().unwrap();
        original_history_load_request(
            &attempt_bootstrap,
            &auth.service,
            &attempt,
            &a,
            &server,
            variant,
        )
        .await;
    }
    let fresh = bootstrap
        .verify_history(&[a])
        .await
        .unwrap()
        .admit(auth.service.clone())
        .await
        .unwrap();
    fresh.with_current(|| Ok(())).unwrap();
    drop(loaded);
    assert!(
        fresh.with_current(|| Ok(())).is_err(),
        "source eligibility cannot outlive original startup"
    );
    assert!(intent_core::caller::current_caller().is_none());
}

// Actual ACP request/queue and Services account fences; the remote history
// classifier and load response are controlled, not native runtime evidence.
async fn original_history_load_request(
    bootstrap: &RepositoryReviewBootstrap,
    services: &Arc<crate::Services>,
    creation: &RepositoryReviewCreationOwner,
    correlation: &intent_acp::review_monitoring::history::ReviewHistoryMessage,
    auth_server: &crate::source_control_auth_ops::repository_owner::secret_reader::tests::Server,
    variant: &str,
) {
    use intent_acp::transport::{Connection, ConnectionHooks};
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let context = creation.context();
    let (client, peer) = tokio::io::duplex(16384);
    let (read, write) = tokio::io::split(client);
    let connection = Arc::new(Connection::new(
        write,
        read,
        None,
        ConnectionHooks::default(),
    ));
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = captured.clone();
    let message = json!({"messageId":correlation.message_id,"storeEnvelopeDigest":correlation.store_envelope_digest,"acpPayloadDigest":correlation.acp_payload_digest});
    let expected_context = serde_json::to_value(context).unwrap();
    let server = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "initialize" => {
                    json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[],"_meta":{"reviewMonitoring":1,"intentCallbackRegistration":{"version":1,"method":"_intent/session/register_mcp_callback"}}})
                }
                "_intent/session/prepare_review_history" => {
                    assert_eq!(request["params"]["sessionId"], "private-original");
                    assert_eq!(request["params"]["ownerKey"], expected_context["ownerKey"]);
                    assert_eq!(
                        request["params"]["loadAttemptId"],
                        expected_context["loadAttemptId"]
                    );
                    json!({"preparationId":"held-original","snapshotDigest":"a".repeat(64),"monitorMessages":[message]})
                }
                "session/load" => {
                    assert_eq!(
                        request["params"]["_meta"]["reviewMonitoringContext"],
                        expected_context
                    );
                    assert_eq!(
                        request["params"]["_meta"]["reviewMonitoringHistory"]["preparationId"],
                        "held-original"
                    );
                    json!({"_meta":{"reviewMonitoring":1,"reviewMonitoringContext":expected_context,"intentCallbackRegistration":{"version":1,"queryReceipt":"11111111-1111-4111-8111-111111111111"}}})
                }
                "_intent/session/retire_review_context" => {
                    assert_eq!(request["params"]["sessionId"], "private-original");
                    assert_eq!(
                        request["params"]["loadAttemptId"],
                        expected_context["loadAttemptId"]
                    );
                    json!({"retired":true})
                }
                other => panic!("unexpected private request {other}"),
            };
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection,
        &provider,
        intent_acp::callback_registration::CallbackOffer::V1,
        intent_acp::review_monitoring::ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let callbacks = handshake.callbacks.unwrap();
    let prepared = callbacks
        .prepare_review_history("private-original", context.clone())
        .await
        .unwrap();
    let target = prepared.load_target("/original", vec![], None).unwrap();
    let verified = bootstrap.verify_history(prepared.messages()).await.unwrap();
    let admitted = verified.admit(services.clone()).await.unwrap();
    assert!(
        admitted.retain_native_context().is_err(),
        "source admission must not invent a native allocation"
    );
    let native = if variant == "retained-original" {
        let native =
            crate::repository_admission::lifecycle::native_allocation::NativeAllocation::new(
                callbacks.clone(),
                context.clone(),
            );
        creation.attach_native(native.clone()).unwrap();
        services
            .store_tasks
            .spawn_draining_with_current_caller(native.clone().drive())
            .unwrap();
        admitted.retain_native_context().unwrap();
        Some(native)
    } else {
        None
    };
    let admission = admitted.bind_load(target);
    if variant == "account-replaced" {
        with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(auth_server.host.clone(), "pat-second".into()),
        )
        .await
        .unwrap();
    }
    let cwd = if variant == "changed-target" {
        "/replacement"
    } else {
        "/original"
    };
    let result = prepared
        .load(&callbacks, cwd, vec![], None, admission)
        .await;
    if variant == "live" || variant == "retained-original" {
        let query = result.unwrap().query.unwrap();
        let receipt = query.pending_review_monitoring().unwrap();
        assert!(receipt.matches_context(context));
        assert!(
            receipt.acknowledged().is_none(),
            "history load cannot acknowledge new delivery"
        );
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            1
        );
    } else {
        assert!(result.is_err(), "{variant}");
        tokio::task::yield_now().await;
        assert!(
            !captured
                .lock()
                .unwrap()
                .iter()
                .any(|r| r["method"] == "session/load"),
            "refused private load or fallback reached peer"
        );
    }
    if let Some(native) = native {
        // Bind the actual successful load response before credential mutation.
        native.bind_response("private-original").unwrap();
        native.producer_finished();
        with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(auth_server.host.clone(), "pat-second".into()),
        )
        .await
        .unwrap();
        assert!(native.is_retired());
        assert!(
            native.is_joined(),
            "credential completion must join the retained original allocation"
        );
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "_intent/session/retire_review_context")
                .count(),
            1
        );
        with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(auth_server.host.clone(), "stored-pat".into()),
        )
        .await
        .unwrap();
    }
    server.abort();
    let _ = server.await;
    if variant == "account-replaced" {
        with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(auth_server.host.clone(), "stored-pat".into()),
        )
        .await
        .unwrap();
    }
    assert!(intent_core::caller::current_caller().is_none());
}

#[tokio::test]
async fn services_original_private_history_resume_retains_all_source_native_owner() {
    services_original_history_case("live").await;
}

#[tokio::test]
async fn services_pending_private_load_shutdown_waits_for_original_response_and_join() {
    services_original_history_case("shutdown-load").await;
}

#[tokio::test]
async fn services_pending_private_load_shutdown_retains_cancelled_waiter() {
    services_original_history_case("shutdown-load-cancelled").await;
}

#[tokio::test]
async fn services_pending_private_load_shutdown_reports_refused_join() {
    services_original_history_case("shutdown-load-refused").await;
}

#[tokio::test]
async fn services_pending_private_load_shutdown_reports_lost_join() {
    services_original_history_case("shutdown-load-lost").await;
}

#[tokio::test]
async fn services_private_load_preserves_independent_original_history_output() {
    services_original_history_case("history-loaded").await;
}

async fn services_original_history_case(variant: &'static str) {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    use intent_acp::transport::{Connection, ConnectionHooks};
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let forge = Server::new().await;
    let auth = AuthFixture::new(&forge).await;
    let store = auth.service.store.clone();
    let mut workspace = chief_workspace();
    workspace.id = WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(json!({
        "id":agent,"workspaceId":workspace.id,"name":"original Services history","status":"active", "provider":"claude-code",
        "createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
    })).unwrap();
    store.insert_agent_session(&row).await.unwrap();
    let registry = auth.service.repository_lifecycle_registry().await.unwrap();
    let f = Fixture {
        dir: tempfile::tempdir().unwrap(),
        store,
        workspace,
        registry,
        agent,
    };
    let initial = f
        .creator(RepositoryCreationIntent::FirstSet)
        .prepare_review()
        .await
        .unwrap();
    let outcome = initial
        .initialize_with_outcome(&f.store, || async {
            Ok::<_, ()>(("private-original".into(), ()))
        })
        .await
        .unwrap();
    let owner = outcome.owner.unwrap();
    let account = auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap()
        .selected()
        .binding
        .scope
        .account_id
        .clone();
    let mut correlation = committed_history_correlation(
        &f,
        "services-original-history",
        &account,
        42,
        forge.descriptor.instance().as_str(),
    )
    .await;
    drop(owner);
    if variant.starts_with("invalid-enrollment-") {
        // Corruption fixture only: production migration keeps these immutable.
        let mut conn = f.store.write_pool().acquire().await.unwrap();
        sqlx::query("DROP TRIGGER review_context_enrollment_update_guard")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("PRAGMA ignore_check_constraints=ON")
            .execute(&mut *conn)
            .await
            .unwrap();
        let sql = if variant == "invalid-enrollment-missing"
            || variant == "invalid-enrollment-lost-provenance"
        {
            "DELETE FROM review_context_enrollment WHERE agent_id=?"
        } else if variant == "invalid-enrollment-version" {
            "UPDATE review_context_enrollment SET version=2 WHERE agent_id=?"
        } else {
            "UPDATE review_context_enrollment SET owner_key='00000000-0000-0000-0000-000000000000' WHERE agent_id=?"
        };
        sqlx::query(sql)
            .bind(&f.agent.0)
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("PRAGMA ignore_check_constraints=OFF")
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    if variant == "invalid-enrollment-lost-provenance" {
        sqlx::query("DELETE FROM pr_monitor_message_provenance WHERE agent_id=?")
            .bind(&f.agent.0)
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }
    if variant == "bad-correlation" {
        correlation.store_envelope_digest = "0".repeat(64);
    }
    if variant == "other-account" {
        with_caller(
            Caller::Daemon,
            auth.service
                .gitlab_connect_pat(forge.host.clone(), "pat-second".into()),
        )
        .await
        .unwrap();
    }
    let history = if variant == "history-loaded" {
        use crate::repository_read_source::tests::native_monitor::NativeRecipient;
        let recipient = NativeRecipient::new(auth.service.clone()).await;
        let (value, output, subscription) = recipient
            .history_subscription(f.agent.clone(), f.workspace.id.clone())
            .await;
        assert!(value.to_string().contains("Original private observation"));
        output.with_current(&mut || Ok(())).unwrap();
        Some((recipient, output, subscription))
    } else {
        None
    };
    let shutdown_load = variant.starts_with("shutdown-load");
    let (retiring, retirement) = tokio::sync::oneshot::channel();
    let (finish_join, mut joined) = tokio::sync::oneshot::channel();
    let (entered, mut waiting) = tokio::sync::oneshot::channel();
    let (release, mut released) = tokio::sync::oneshot::channel();
    let (client, peer) = tokio::io::duplex(16384);
    let (read, write) = tokio::io::split(client);
    let connection = Arc::new(Connection::new(
        write,
        read,
        None,
        ConnectionHooks::default(),
    ));
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = captured.clone();
    let server = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut prepared_context = Value::Null;
        let mut entered = Some(entered);
        let mut retiring = Some(retiring);
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "initialize" => {
                    json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[],"_meta":{"reviewMonitoring":1,"intentCallbackRegistration":{"version":1,"method":"_intent/session/register_mcp_callback"}}})
                }
                "_intent/session/prepare_review_history" => {
                    assert_eq!(request["params"]["sessionId"], "private-original");
                    prepared_context = json!({"version":1,"ownerKey":request["params"]["ownerKey"],"loadAttemptId":request["params"]["loadAttemptId"]});
                    if variant == "cancel-preflight" {
                        entered.take().unwrap().send(()).unwrap();
                        (&mut released).await.unwrap();
                    }
                    json!({"preparationId":"services-held","snapshotDigest":"a".repeat(64),"monitorMessages":[{
                        "messageId":correlation.message_id,"storeEnvelopeDigest":correlation.store_envelope_digest,"acpPayloadDigest":correlation.acp_payload_digest
                    }]})
                }
                "session/load" if variant.starts_with("invalid-enrollment-") => json!({}),
                "session/new" if variant.starts_with("invalid-enrollment-") => {
                    json!({"sessionId":"unqualified-fallback"})
                }
                "session/load" => {
                    assert_eq!(
                        request["params"]["_meta"]["reviewMonitoringContext"],
                        prepared_context
                    );
                    assert_eq!(
                        request["params"]["_meta"]["reviewMonitoringHistory"]["preparationId"],
                        "services-held"
                    );
                    if variant == "cancel-load" || shutdown_load {
                        entered.take().unwrap().send(()).unwrap();
                        (&mut released).await.unwrap();
                    }
                    json!({"_meta":{"reviewMonitoring":1,"reviewMonitoringContext":if variant == "bad-echo" { Value::Null } else { prepared_context.clone() },"intentCallbackRegistration":{"version":1,"queryReceipt":"11111111-1111-4111-8111-111111111111"}}})
                }
                "_intent/session/retire_review_context" => {
                    assert_eq!(request["params"]["sessionId"], "private-original");
                    assert_eq!(
                        request["params"]["loadAttemptId"],
                        prepared_context["loadAttemptId"]
                    );
                    if shutdown_load {
                        retiring.take().unwrap().send(()).unwrap();
                        (&mut joined).await.unwrap();
                        if variant == "shutdown-load-lost" {
                            break;
                        }
                    }
                    json!({"retired":variant != "shutdown-load-refused"})
                }
                other => panic!("unexpected Services private history request {other}"),
            };
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        intent_acp::callback_registration::CallbackOffer::V1,
        intent_acp::review_monitoring::ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let callbacks = handshake.callbacks.unwrap();
    let creation = f.creator(RepositoryCreationIntent::Loaded {
        session_id: "private-original".into(),
    });
    let mut resume = Box::pin(auth.service.resume_repository_acp_session(
        (&connection, Some(&callbacks)),
        &handshake.ordinary.initialize,
        &f.agent,
        "/original",
        vec![],
        creation,
    ));
    if shutdown_load {
        tokio::select! {
            result = &mut resume => panic!("pending LOAD escaped held response: {}", result.is_ok()),
            _ = &mut waiting => {},
            () = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("private LOAD not dispatched"),
        }
        let bus = crate::events::EventBus::new(f.store.clone());
        // The actual manager joins Services' registry-owned producer; this
        // controlled peer has no process handle and proves no process teardown.
        let manager = crate::agent_manager::AgentManager::new(
            (*auth.service).clone(),
            Arc::new(crate::agent_manager::BusEventSink::new(bus)),
            4,
        );
        manager.begin_shutdown();
        drop(resume);
        let mut shutdown = Box::pin(manager.shutdown_with_native_outcome());
        assert!(tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_millis(50)),
            &mut shutdown
        )
        .await
        .is_err());
        assert!(
            connection.is_alive(),
            "shutdown closed pending LOAD transport"
        );
        if variant == "shutdown-load-cancelled" {
            drop(shutdown);
            shutdown = Box::pin(manager.shutdown_with_native_outcome());
            assert!(tokio::time::timeout(
                intentd_test_support::test_timeout(std::time::Duration::from_millis(50)),
                &mut shutdown
            )
            .await
            .is_err());
        }
        release.send(()).unwrap();
        tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
            retirement,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(std::time::Duration::from_millis(50)),
                &mut shutdown
            )
            .await
            .is_err(),
            "shutdown completed before exact retirement response"
        );
        assert!(connection.is_alive());
        finish_join.send(()).unwrap();
        let result = tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
            shutdown,
        )
        .await
        .unwrap();
        assert_eq!(
            result.is_ok(),
            !matches!(variant, "shutdown-load-refused" | "shutdown-load-lost")
        );
        tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
            auth.service.store_tasks.drain_finite(),
        )
        .await
        .unwrap();
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("private-original")
        );
        {
            let requests = captured.lock().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .filter(|r| r["method"] == "session/load")
                    .count(),
                1
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|r| r["method"] == "_intent/session/retire_review_context")
                    .count(),
                1
            );
            assert!(!requests.iter().any(|r| r["method"] == "session/new"));
        }
        server.abort();
        return;
    }
    if variant.starts_with("cancel-") {
        tokio::select! {
            result = &mut resume => panic!("load escaped original held phase: {}", result.is_ok()),
            _ = &mut waiting => {},
            () = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("load phase not reached"),
        }
        drop(resume);
        release.send(()).unwrap();
        tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
            auth.service.store_tasks.drain_finite(),
        )
        .await
        .expect("cancelled load lost its original retirement owner");
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            usize::from(variant == "cancel-load")
        );
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "_intent/session/retire_review_context")
                .count(),
            1
        );
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("private-original")
        );
        server.abort();
        return;
    }
    let result = resume.await;
    if variant.starts_with("invalid-enrollment-") {
        {
            let requests = captured.lock().unwrap();
            assert!(
                !requests.iter().any(|r| matches!(
                    r["method"].as_str(),
                    Some("session/load" | "session/new" | "_intent/session/prepare_review_history")
                )),
                "invalid private enrollment reached an ordinary fallback: {requests:?}"
            );
            assert!(
                result.is_err(),
                "invalid private enrollment returned success"
            );
        }
        if variant == "invalid-enrollment-missing"
            || variant == "invalid-enrollment-lost-provenance"
        {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM review_context_enrollment WHERE agent_id=?",
            )
            .bind(&f.agent.0)
            .fetch_one(f.store.read_pool())
            .await
            .unwrap();
            assert_eq!(
                count, 0,
                "missing private lineage cannot be reconstructed from public IDs"
            );
            server.abort();
            return;
        }
        let saved: (String, i64) = sqlx::query_as(
            "SELECT owner_key,version FROM review_context_enrollment WHERE agent_id=?",
        )
        .bind(&f.agent.0)
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
        if variant == "invalid-enrollment-version" {
            assert_eq!(saved.1, 2);
        } else {
            assert_eq!(saved.0, "00000000-0000-0000-0000-000000000000");
        }
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("private-original")
        );
        server.abort();
        return;
    }
    if !matches!(variant, "live" | "history-loaded") {
        assert!(
            result.is_err(),
            "{variant} received original Services success"
        );
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            usize::from(variant == "bad-echo")
        );
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "_intent/session/retire_review_context")
                .count(),
            1
        );
        assert_eq!(
            f.store
                .get_agent_session(&f.agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some("private-original")
        );
        server.abort();
        return;
    }
    let resumed = result
        .expect("qualified original history must use admitted Services load")
        .unwrap();
    assert_eq!(resumed.response.session_id, "private-original");
    let owner = resumed.owner.unwrap();
    let native = owner.native_allocation_for_test();
    if let Some((_, output, subscription)) = &history {
        output
            .with_current(&mut || Ok(()))
            .expect("unchanged persisted history retired by physical LOAD");
        subscription
            .bind_output("later ordinary chat frame".into())
            .with_current(&mut || Ok(()))
            .unwrap();
    }
    let receipt = resumed.query.unwrap().pending_review_monitoring().unwrap();
    assert!(
        receipt.acknowledged().is_none(),
        "history bootstrap cannot acknowledge new delivery"
    );
    with_caller(
        Caller::Daemon,
        auth.service
            .gitlab_connect_pat(forge.host.clone(), "pat-second".into()),
    )
    .await
    .unwrap();
    if let Some((_, output, subscription)) = &history {
        assert!(output.with_current(&mut || Ok(())).is_err());
        assert!(subscription
            .bind_output("later public frame".into())
            .with_current(&mut || Ok(()))
            .is_err());
    }
    assert!(native.is_retired());
    assert!(
        native.is_joined(),
        "source mutation must join the actual retained Services allocation"
    );
    assert_eq!(
        captured
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "session/load")
            .count(),
        1
    );
    assert_eq!(
        captured
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "_intent/session/retire_review_context")
            .count(),
        1
    );
    drop(owner);
    server.abort();
}

#[tokio::test]
async fn services_original_private_history_rejects_wrong_provenance() {
    services_original_history_case("bad-correlation").await;
}

#[tokio::test]
async fn services_original_private_history_rejects_replacement_account() {
    services_original_history_case("other-account").await;
}

#[tokio::test]
async fn services_original_private_history_rejects_mismatched_original_receipt() {
    services_original_history_case("bad-echo").await;
}

#[tokio::test]
async fn services_original_private_history_joins_cancelled_preflight() {
    services_original_history_case("cancel-preflight").await;
}

#[tokio::test]
async fn services_original_private_history_joins_cancelled_load() {
    services_original_history_case("cancel-load").await;
}

#[tokio::test]
async fn services_private_history_invalid_enrollment_version_refuses_raw_fallback() {
    services_original_history_case("invalid-enrollment-version").await;
}

#[tokio::test]
async fn services_private_history_invalid_enrollment_owner_refuses_raw_fallback() {
    services_original_history_case("invalid-enrollment-owner").await;
}

#[tokio::test]
async fn services_private_history_missing_enrollment_refuses_raw_fallback() {
    services_original_history_case("invalid-enrollment-missing").await;
}

#[tokio::test]
async fn services_private_history_lost_enrollment_and_provenance_refuses_raw_fallback() {
    services_original_history_case("invalid-enrollment-lost-provenance").await;
}

#[path = "native_history_tests.rs"]
mod native_history_tests;
