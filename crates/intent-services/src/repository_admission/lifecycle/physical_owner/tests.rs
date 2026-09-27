//! Actual Store confirmation and R lifetime composition. Provider completion
//! futures are explicit fixtures; manager/ACP producer wiring is still absent.

use intent_core::caller::with_caller;
use intent_core::{chief_workspace, AgentSession, Workspace};

use super::*;

struct Fixture {
    _dir: tempfile::TempDir,
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
            _dir: dir,
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
