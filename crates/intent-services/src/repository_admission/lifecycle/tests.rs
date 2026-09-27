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
