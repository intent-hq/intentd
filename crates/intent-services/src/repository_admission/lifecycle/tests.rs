use std::sync::mpsc;
use std::time::{Duration, Instant};

use intent_core::{AgentId, WorkspaceId};

use super::*;

fn keys(workspace: &WorkspaceId) -> Vec<RepositoryLifecycleKey> {
    vec![
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::Workspace(workspace.clone()),
    ]
}

fn subscribe(
    registry: &Arc<RepositoryLifecycleRegistry>,
    owner: &FixtureOriginOwner,
    workspace: &WorkspaceId,
    leaf: &RepositoryRetirement,
) -> AdmissionResult<RepositorySubscription> {
    registry.subscribe(
        &owner.origin(),
        &Caller::Daemon,
        &keys(workspace),
        leaf.clone(),
    )
}

#[test]
fn multi_key_capture_is_atomic_and_dropped_mutation_cannot_settle() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let a = WorkspaceId::new();
    let b = WorkspaceId::new();
    let ticket = registry
        .begin_mutation(&[RepositoryLifecycleKey::Workspace(b.clone())])
        .unwrap();
    let mut both = keys(&a);
    both.push(RepositoryLifecycleKey::Workspace(b.clone()));
    assert!(matches!(
        registry.subscribe(
            &owner.origin(),
            &Caller::Daemon,
            &both,
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Unavailable)
    ));
    assert!(registry.state.lock().unwrap().subscriptions.is_empty());
    drop(ticket);
    assert!(matches!(
        subscribe(&registry, &owner, &b, &RepositoryRetirement::default()),
        Err(AdmissionError::Unavailable)
    ));
    let leaf = RepositoryRetirement::default();
    let _live = subscribe(&registry, &owner, &a, &leaf).unwrap();
    assert!(leaf.check_current().is_ok());
}

#[test]
fn overlapping_tickets_are_owned_and_confirmation_never_revives_old_leaves() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let workspace = WorkspaceId::new();
    let leaf = RepositoryRetirement::default();
    let _old = subscribe(&registry, &owner, &workspace, &leaf).unwrap();
    let mutation_keys = [RepositoryLifecycleKey::Workspace(workspace.clone())];
    let first = registry.begin_mutation(&mutation_keys).unwrap();
    let second = registry.begin_mutation(&mutation_keys).unwrap();
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
    first.settle_confirmed();
    assert!(matches!(
        subscribe(
            &registry,
            &owner,
            &workspace,
            &RepositoryRetirement::default()
        ),
        Err(AdmissionError::Unavailable)
    ));
    second.settle_confirmed();
    let fresh = RepositoryRetirement::default();
    let _new = subscribe(&registry, &owner, &workspace, &fresh).unwrap();
    assert!(fresh.check_current().is_ok());
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
}

#[test]
fn database_barrier_retires_every_origin_and_every_subscription() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let a = WorkspaceId::new();
    let b = WorkspaceId::new();
    let leaf_a = RepositoryRetirement::default();
    let leaf_b = RepositoryRetirement::default();
    let _a = subscribe(&registry, &owner, &a, &leaf_a).unwrap();
    let _b = subscribe(&registry, &owner, &b, &leaf_b).unwrap();
    let mutation = registry
        .begin_mutation(&[RepositoryLifecycleKey::Database])
        .unwrap();
    assert_eq!(leaf_a.check_current(), Err(AdmissionError::Retired));
    assert_eq!(leaf_b.check_current(), Err(AdmissionError::Retired));
    assert!(matches!(
        FixtureOriginOwner::new(&registry, Caller::Daemon),
        Err(AdmissionError::Unavailable)
    ));
    mutation.settle_confirmed();
    assert!(matches!(
        subscribe(&registry, &owner, &a, &RepositoryRetirement::default()),
        Err(AdmissionError::Retired)
    ));
    let replacement = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let _fresh = subscribe(
        &registry,
        &replacement,
        &a,
        &RepositoryRetirement::default(),
    )
    .unwrap();
}

#[test]
fn physical_origin_drop_and_agent_mutation_do_not_rebind_by_id() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let id = AgentId::new();
    let caller = Caller::Agent {
        agent_id: id.clone(),
    };
    let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
    let original = owner.origin();
    let leaf = RepositoryRetirement::default();
    let workspace = WorkspaceId::new();
    let _live = registry
        .subscribe(&original, &caller, &keys(&workspace), leaf.clone())
        .unwrap();
    registry
        .begin_mutation(&[RepositoryLifecycleKey::Agent(id)])
        .unwrap()
        .settle_confirmed();
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
    assert!(matches!(
        registry.subscribe(
            &original,
            &caller,
            &keys(&workspace),
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Retired)
    ));
    let replacement = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
    let new_leaf = RepositoryRetirement::default();
    let _new = registry
        .subscribe(
            &replacement.origin(),
            &caller,
            &keys(&workspace),
            new_leaf.clone(),
        )
        .unwrap();
    let escaped = replacement.origin();
    drop(replacement);
    assert_eq!(new_leaf.check_current(), Err(AdmissionError::Retired));
    assert!(matches!(
        registry.subscribe(
            &escaped,
            &caller,
            &keys(&workspace),
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Retired)
    ));
}

#[test]
fn origin_mismatch_and_missing_database_key_cannot_subscribe() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let foreign = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let workspace = WorkspaceId::new();
    assert!(matches!(
        foreign.subscribe(
            &owner.origin(),
            &Caller::Daemon,
            &keys(&workspace),
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Denied)
    ));
    assert!(matches!(
        registry.subscribe(
            &owner.origin(),
            &Caller::Agent {
                agent_id: AgentId::new()
            },
            &keys(&workspace),
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Denied)
    ));
    assert!(matches!(
        registry.subscribe(
            &owner.origin(),
            &Caller::Daemon,
            &[RepositoryLifecycleKey::Workspace(workspace)],
            RepositoryRetirement::default()
        ),
        Err(AdmissionError::Unavailable)
    ));
    assert!(registry.state.lock().unwrap().subscriptions.is_empty());
}

#[test]
fn retirement_waits_for_admitted_leaf_but_does_not_hold_registry_lock() {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let workspace = WorkspaceId::new();
    let leaf = RepositoryRetirement::default();
    let _live = subscribe(&registry, &owner, &workspace, &leaf).unwrap();
    let (entered, wait) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let held = leaf.clone();
    let worker = std::thread::spawn(move || {
        held.dispatch(|| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        })
    });
    wait.recv_timeout(Duration::from_secs(5)).unwrap();
    let mutation_registry = registry.clone();
    let mutation_workspace = workspace.clone();
    let (finished, finish) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let ticket = mutation_registry
            .begin_mutation(&[RepositoryLifecycleKey::Workspace(mutation_workspace)])
            .unwrap();
        finished.send(()).unwrap();
        ticket
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !registry.state.lock().unwrap().pending.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(registry.state.try_lock().is_ok());
    assert!(finish.try_recv().is_err());
    assert!(matches!(
        subscribe(
            &registry,
            &owner,
            &workspace,
            &RepositoryRetirement::default()
        ),
        Err(AdmissionError::Unavailable)
    ));
    // A second owner must wait for the same in-progress retirement even
    // after the first owner detached this subscription from the live map.
    let second_registry = registry.clone();
    let (returned, returned_rx) = mpsc::channel();
    let second = std::thread::spawn(move || {
        let ticket = second_registry
            .begin_mutation(&[RepositoryLifecycleKey::Workspace(workspace)])
            .unwrap();
        returned.send(()).unwrap();
        ticket
    });
    let premature = returned_rx.recv_timeout(Duration::from_millis(50)).is_ok();
    release.send(()).unwrap();
    assert!(worker.join().unwrap().is_ok());
    second.join().unwrap().settle_confirmed();
    assert!(
        !premature,
        "second writer returned before original leaf retirement"
    );
    finish.recv_timeout(Duration::from_secs(5)).unwrap();
    writer.join().unwrap().settle_confirmed();
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
}

#[tokio::test]
async fn source_lifetime_requires_same_installed_observer_and_original_origin() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let store = Store::open(file.path()).await.unwrap();
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let owner = FixtureOriginOwner::new(&registry, Caller::Daemon).unwrap();
    let leaf = RepositoryRetirement::default();
    let lifetime =
        RepositorySourceLifetime::new(registry.clone(), Some(owner.origin()), leaf.clone());
    let workspace = WorkspaceId::new();
    assert!(matches!(
        lifetime.subscribe(&store, &Caller::Daemon, &keys(&workspace)),
        Err(AdmissionError::Unavailable)
    ));
    registry.install(&store).await.unwrap();
    registry.install(&store).await.unwrap();
    let foreign = Arc::new(RepositoryLifecycleRegistry::default());
    assert_eq!(
        foreign.install(&store).await,
        Err(AdmissionError::Unavailable)
    );
    let missing = RepositorySourceLifetime::new(registry, None, RepositoryRetirement::default());
    assert!(matches!(
        missing.subscribe(&store, &Caller::Daemon, &keys(&workspace)),
        Err(AdmissionError::Unavailable)
    ));
    let active = lifetime
        .subscribe(&store, &Caller::Daemon, &keys(&workspace))
        .unwrap();
    assert!(lifetime.retirement().check_current().is_ok());
    drop(active);
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
}

// Real Store/registry/physical/request composition. Only the provider completion
// is a fixture; public grace timers, Services forwarding and read policy are absent.
#[cfg(unix)]
mod pending_delete {
    use super::*;
    use crate::repository_admission::request_context::RepositoryCapturedRequest;
    use intent_core::caller::with_caller;
    use intent_core::{chief_workspace, AgentSession, Workspace};
    use intent_store::RepositoryPendingDeleteGuard;
    use physical_owner::{
        RepositoryCreationIntent, RepositoryCreationOwner, RepositoryPhysicalOwner,
    };

    struct Subject {
        workspace: Workspace,
        agent: AgentId,
    }

    impl Subject {
        fn caller(&self) -> Caller {
            Caller::Agent {
                agent_id: self.agent.clone(),
            }
        }

        fn keys(&self) -> Vec<RepositoryLifecycleKey> {
            vec![
                RepositoryLifecycleKey::Database,
                RepositoryLifecycleKey::Workspace(self.workspace.id.clone()),
                RepositoryLifecycleKey::Agent(self.agent.clone()),
            ]
        }
    }

    struct Fixture {
        dir: tempfile::TempDir,
        store: Store,
        registry: Arc<RepositoryLifecycleRegistry>,
        first: Subject,
        other: Subject,
    }

    impl Fixture {
        async fn new() -> Self {
            // This source is also compiled by the standalone admission harness,
            // which has no crate test_support module. Preserve scratch cleanup
            // and the same keep-on-failure opt-in without another module copy.
            let mut dir = tempfile::Builder::new()
                .prefix("pending-delete-observer-")
                .tempdir()
                .unwrap();
            if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
                dir.disable_cleanup(true);
            }
            let store = Store::open(&dir.path().join("store.db")).await.unwrap();
            let first = Self::insert_subject(&store).await;
            let other = Self::insert_subject(&store).await;
            let registry = Arc::new(RepositoryLifecycleRegistry::default());
            registry.install(&store).await.unwrap();
            Self {
                dir,
                store,
                registry,
                first,
                other,
            }
        }

        async fn insert_subject(store: &Store) -> Subject {
            let mut workspace = chief_workspace();
            workspace.id = WorkspaceId::new();
            store.insert_workspace(&workspace).await.unwrap();
            let agent = AgentId::new();
            let row: AgentSession = serde_json::from_value(serde_json::json!({
                "id":agent,"workspaceId":workspace.id,"name":"pending delete fixture",
                "status":"active","createdAt":"2026-09-28T00:00:00Z",
                "updatedAt":"2026-09-28T00:00:00Z"
            }))
            .unwrap();
            store.insert_agent_session(&row).await.unwrap();
            Subject { workspace, agent }
        }

        fn creator(
            &self,
            subject: &Subject,
            intent: RepositoryCreationIntent,
        ) -> RepositoryCreationOwner {
            RepositoryCreationOwner::allocate(
                &self.registry,
                &self.store,
                subject.workspace.id.clone(),
                subject.agent.clone(),
                intent,
            )
            .unwrap()
        }

        async fn live(&self, subject: &Subject) -> RepositoryPhysicalOwner {
            self.creator(subject, RepositoryCreationIntent::FirstSet)
                .initialize(&self.store, || async { Ok("original".into()) })
                .await
                .unwrap()
        }

        async fn source(
            &self,
            request: &RepositoryCapturedRequest,
            subject: &Subject,
        ) -> RepositorySourceLifetime {
            with_caller(subject.caller(), async { request.source_lifetime() })
                .await
                .unwrap()
        }
    }

    async fn current(request: &RepositoryCapturedRequest, subject: &Subject) -> bool {
        with_caller(subject.caller(), async {
            request.source_lifetime().is_ok()
        })
        .await
    }

    struct HeldDispatch {
        release: Option<mpsc::Sender<()>>,
        worker: Option<std::thread::JoinHandle<AdmissionResult<()>>>,
    }

    impl HeldDispatch {
        fn start(leaf: RepositoryRetirement) -> Self {
            let (entered, wait) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                // Explicit scheduling fixture holds the real consuming fence.
                leaf.dispatch(|| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                })
            });
            let held = Self {
                release: Some(release),
                worker: Some(worker),
            };
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            held
        }

        fn finish(mut self) {
            self.release.take().unwrap().send(()).unwrap();
            assert_eq!(self.worker.take().unwrap().join().unwrap(), Ok(()));
        }
    }

    impl Drop for HeldDispatch {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    fn begin_on_worker(
        store: Store,
        key: RepositoryLifecycleKey,
    ) -> tokio::task::JoinHandle<intent_core::Result<RepositoryPendingDeleteGuard>> {
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            runtime.block_on(store.begin_repository_pending_delete(&[key]))
        })
    }

    async fn wait_pending(registry: &RepositoryLifecycleRegistry, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if registry.state.lock().unwrap().pending.len() == count {
                return;
            }
            assert!(Instant::now() < deadline, "original tickets did not begin");
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn known_settlement_requires_fresh_request_and_preserves_physical_owner() {
        for agent_key in [false, true] {
            let f = Fixture::new().await;
            let owner = f.live(&f.first).await;
            let callback = owner.callback();
            let request = callback.capture();
            let source = f.source(&request, &f.first).await;
            let _subscription = source
                .subscribe(&f.store, &f.first.caller(), &f.first.keys())
                .unwrap();
            assert_eq!(source.retirement().dispatch(|| Ok(7)), Ok(7));
            let key = if agent_key {
                RepositoryLifecycleKey::Agent(f.first.agent.clone())
            } else {
                RepositoryLifecycleKey::Workspace(f.first.workspace.id.clone())
            };
            let guard = f
                .store
                .begin_repository_pending_delete(&[key])
                .await
                .unwrap();
            let denied_capture = callback.capture();
            assert!(!current(&request, &f.first).await);
            assert!(!current(&denied_capture, &f.first).await);
            assert_eq!(
                source
                    .retirement()
                    .dispatch(|| panic!("retired request dispatched")),
                Err::<(), _>(AdmissionError::Retired)
            );
            assert!(f
                .registry
                .state
                .lock()
                .unwrap()
                .origins
                .values()
                .all(|o| !o.retired));
            guard.settle_confirmed();
            assert!(current(&callback.capture(), &f.first).await);
            assert!(!current(&request, &f.first).await);
            assert!(!current(&denied_capture, &f.first).await);
            assert!(f.registry.state.lock().unwrap().pending.is_empty());
        }
    }

    #[tokio::test]
    async fn database_marker_preserves_original_creator_and_live_owner_until_actual_delete() {
        let f = Fixture::new().await;
        let owner = f.live(&f.first).await;
        let creator = f.creator(&f.other, RepositoryCreationIntent::FirstSet);
        let pending_callback = creator.callback();
        let old = owner.callback().capture();
        let guard = f
            .store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Database])
            .await
            .unwrap();
        assert!(!current(&old, &f.first).await);
        assert!(!current(&owner.callback().capture(), &f.first).await);
        guard.settle_confirmed();
        assert!(current(&owner.callback().capture(), &f.first).await);
        let other_owner = creator
            .initialize(&f.store, || async { Ok("original other".into()) })
            .await
            .unwrap();
        assert!(current(&other_owner.callback().capture(), &f.other).await);
        assert!(!current(&pending_callback.capture(), &f.other).await);
        let pending_load = f.creator(
            &f.first,
            RepositoryCreationIntent::Loaded {
                session_id: "original".into(),
            },
        );
        let fresh = owner.callback().capture();
        assert!(f
            .store
            .delete_agent_session(&f.first.workspace.id, &f.first.agent)
            .await
            .unwrap());
        assert!(!current(&fresh, &f.first).await);
        assert!(!current(&owner.callback().capture(), &f.first).await);
        assert!(pending_load
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .is_err());
        assert!(current(&other_owner.callback().capture(), &f.other).await);
    }

    #[tokio::test]
    async fn unknown_ticket_survives_other_settlement_installation_and_managed_reopen() {
        let f = Fixture::new().await;
        let first = f.live(&f.first).await;
        let other = f.live(&f.other).await;
        let first_keys = [RepositoryLifecycleKey::Agent(f.first.agent.clone())];
        let unknown = f
            .store
            .begin_repository_pending_delete(&first_keys)
            .await
            .unwrap();
        let original_ids: HashSet<_> = f
            .registry
            .state
            .lock()
            .unwrap()
            .pending
            .keys()
            .copied()
            .collect();
        let known = f
            .store
            .begin_repository_pending_delete(&first_keys)
            .await
            .unwrap();
        let unrelated = f
            .store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Workspace(
                f.other.workspace.id.clone(),
            )])
            .await
            .unwrap();
        known.settle_confirmed();
        drop(unknown);
        assert!(!current(&first.callback().capture(), &f.first).await);
        assert!(!current(&other.callback().capture(), &f.other).await);
        unrelated.settle_confirmed();
        assert_eq!(
            f.registry
                .state
                .lock()
                .unwrap()
                .pending
                .keys()
                .copied()
                .collect::<HashSet<_>>(),
            original_ids
        );
        assert!(current(&other.callback().capture(), &f.other).await);
        f.registry.install(&f.store).await.unwrap();
        let reopened = Store::open(&f.dir.path().join("store.db")).await.unwrap();
        let observer: Arc<dyn RepositoryLifecycleObserver> = f.registry.clone();
        assert!(reopened.has_repository_lifecycle_observer(&observer));
        reopened
            .begin_repository_pending_delete(&first_keys)
            .await
            .unwrap()
            .settle_confirmed();
        assert_eq!(
            f.registry
                .state
                .lock()
                .unwrap()
                .pending
                .keys()
                .copied()
                .collect::<HashSet<_>>(),
            original_ids
        );
        assert!(!current(&first.callback().capture(), &f.first).await);
        assert!(f
            .registry
            .state
            .lock()
            .unwrap()
            .origins
            .values()
            .all(|o| !o.retired));
    }

    #[tokio::test]
    async fn workspace_marker_preserves_pending_creator_but_blocks_new_allocation() {
        let f = Fixture::new().await;
        let creator = f.creator(&f.first, RepositoryCreationIntent::FirstSet);
        let pending = creator.callback().capture();
        let other = f.live(&f.other).await;
        let guard = f
            .store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Workspace(
                f.first.workspace.id.clone(),
            )])
            .await
            .unwrap();
        assert!(matches!(
            RepositoryCreationOwner::allocate(
                &f.registry,
                &f.store,
                f.first.workspace.id.clone(),
                f.first.agent.clone(),
                RepositoryCreationIntent::FirstSet,
            ),
            Err(AdmissionError::Unavailable)
        ));
        assert!(current(&other.callback().capture(), &f.other).await);
        guard.settle_confirmed();
        let owner = creator
            .initialize(&f.store, || async { Ok("original".into()) })
            .await
            .unwrap();
        assert!(current(&owner.callback().capture(), &f.first).await);
        assert!(!current(&pending, &f.first).await);
        assert!(current(&other.callback().capture(), &f.other).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_concurrent_marker_joins_detached_leaf_without_blocking_unrelated_store_work() {
        let f = Fixture::new().await;
        let owner = f.live(&f.first).await;
        let other = f.live(&f.other).await;
        let request = owner.callback().capture();
        let source = f.source(&request, &f.first).await;
        let _subscription = source
            .subscribe(&f.store, &f.first.caller(), &f.first.keys())
            .unwrap();
        let held = HeldDispatch::start(source.retirement());
        let key = RepositoryLifecycleKey::Agent(f.first.agent.clone());
        let first = begin_on_worker(f.store.clone(), key.clone());
        wait_pending(&f.registry, 1).await;
        assert!(!f.registry.state.lock().unwrap().retiring.is_empty());
        let second = begin_on_worker(f.store.clone(), key);
        wait_pending(&f.registry, 2).await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        assert!(f.registry.state.try_lock().is_ok());
        let unavailable = owner.callback().capture();
        assert!(!current(&unavailable, &f.first).await);
        assert!(current(&other.callback().capture(), &f.other).await);
        tokio::time::timeout(Duration::from_secs(5), async {
            f.registry.install(&f.store).await.unwrap();
            let _inserted = Fixture::insert_subject(&f.store).await;
            f.store
                .begin_repository_pending_delete(&[RepositoryLifecycleKey::Agent(
                    f.other.agent.clone(),
                )])
                .await
                .unwrap()
                .settle_confirmed();
        })
        .await
        .unwrap();
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        held.finish();
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert!(!current(&request, &f.first).await);
        first.settle_confirmed();
        assert!(!current(&owner.callback().capture(), &f.first).await);
        second.settle_confirmed();
        assert!(current(&owner.callback().capture(), &f.first).await);
        assert!(!current(&unavailable, &f.first).await);
        assert_eq!(
            source.retirement().check_current(),
            Err(AdmissionError::Retired)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subsequent_actual_delete_joins_original_retirement_and_never_revives_owner() {
        let f = Fixture::new().await;
        let owner = f.live(&f.first).await;
        let request = owner.callback().capture();
        let source = f.source(&request, &f.first).await;
        let _subscription = source
            .subscribe(&f.store, &f.first.caller(), &f.first.keys())
            .unwrap();
        let held = HeldDispatch::start(source.retirement());
        let pending = begin_on_worker(
            f.store.clone(),
            RepositoryLifecycleKey::Agent(f.first.agent.clone()),
        );
        wait_pending(&f.registry, 1).await;
        let runtime = tokio::runtime::Handle::current();
        let store = f.store.clone();
        let workspace = f.first.workspace.id.clone();
        let agent = f.first.agent.clone();
        let delete = tokio::task::spawn_blocking(move || {
            runtime.block_on(store.delete_agent_session(&workspace, &agent))
        });
        wait_pending(&f.registry, 2).await;
        assert!(!pending.is_finished());
        assert!(!delete.is_finished());
        held.finish();
        let pending = pending.await.unwrap().unwrap();
        assert!(delete.await.unwrap().unwrap());
        assert_eq!(f.registry.state.lock().unwrap().pending.len(), 1);
        pending.settle_confirmed();
        assert!(f.registry.state.lock().unwrap().pending.is_empty());
        assert!(!current(&request, &f.first).await);
        assert!(!current(&owner.callback().capture(), &f.first).await);
    }

    #[tokio::test]
    async fn empty_or_exhausted_ticket_does_not_detach_original_requests() {
        let f = Fixture::new().await;
        let owner = f.live(&f.first).await;
        let request = owner.callback().capture();
        assert!(f.store.begin_repository_pending_delete(&[]).await.is_err());
        f.registry.state.lock().unwrap().next_id = u64::MAX;
        assert!(f
            .store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Agent(
                f.first.agent.clone()
            ),])
            .await
            .is_err());
        {
            let state = f.registry.state.lock().unwrap();
            assert!(state.pending.is_empty());
            assert!(state.retiring.is_empty());
            assert!(state.origins.values().all(|o| !o.retired));
        }
        assert!(current(&request, &f.first).await);
    }
}
