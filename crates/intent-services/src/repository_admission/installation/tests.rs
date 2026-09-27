//! Actual Services/Store installation. No provider or physical producer runs.

use intent_core::caller::{with_caller, Caller};
use intent_core::{chief_workspace, AgentId, AgentSession, Workspace, WorkspaceId};
use intent_store::Store;

use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner,
};
use crate::WorkspaceApi;

struct Fixture {
    dir: tempfile::TempDir,
    services: Services,
    workspace: Workspace,
    agent: AgentId,
}

impl Fixture {
    async fn new() -> Self {
        let dir = crate::test_support::test_tempdir("repository-installation-");
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let mut workspace = chief_workspace();
        workspace.id = WorkspaceId::new();
        store.insert_workspace(&workspace).await.unwrap();
        let agent = AgentId::new();
        let row: AgentSession = serde_json::from_value(serde_json::json!({
            "id": agent, "workspaceId": workspace.id, "name": "installation fixture",
            "status": "active", "createdAt": "2026-09-27T00:00:00Z",
            "updatedAt": "2026-09-27T00:00:00Z"
        }))
        .unwrap();
        store.insert_agent_session(&row).await.unwrap();
        Self {
            dir,
            services: Services::new(store),
            workspace,
            agent,
        }
    }

    fn assert_creation_unavailable(&self, registry: &Arc<RepositoryLifecycleRegistry>) {
        assert!(matches!(
            RepositoryCreationOwner::allocate(
                registry,
                self.services.store(),
                self.workspace.id.clone(),
                self.agent.clone(),
                RepositoryCreationIntent::FirstSet,
            ),
            Err(AdmissionError::Unavailable)
        ));
    }
}

async fn assert_local_read(services: &Services, workspace: &Workspace) {
    let observed = with_caller(Caller::Daemon, services.get_workspace(workspace.id.clone()))
        .await
        .unwrap();
    assert_eq!(observed.id, workspace.id);
    assert_eq!(observed.title, workspace.title);
}

#[tokio::test]
async fn construction_is_uninstalled_and_clones_install_the_same_original_registry() {
    let f = Fixture::new().await;
    let observer: Arc<dyn RepositoryLifecycleObserver> =
        f.services.repository_lifecycle_registry.clone();
    assert!(!f
        .services
        .store()
        .has_repository_lifecycle_observer(&observer));
    f.assert_creation_unavailable(&f.services.repository_lifecycle_registry);
    let clone = f.services.clone();
    let directory = f.services.repository_connection_directory();
    let (first, second) = tokio::join!(
        f.services.repository_lifecycle_registry(),
        clone.repository_lifecycle_registry(),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(
        &first,
        &f.services.repository_lifecycle_registry
    ));
    assert!(clone.store().has_repository_lifecycle_observer(&observer));
    assert!(Arc::ptr_eq(
        &directory,
        &clone.repository_connection_directory()
    ));
    assert_local_read(&clone, &f.workspace).await;
}

#[tokio::test]
async fn competing_services_instances_cannot_replace_the_winning_observer() {
    let f = Fixture::new().await;
    let other = Services::new(f.services.store().clone());
    let (first, second) = tokio::join!(
        f.services.repository_lifecycle_registry(),
        other.repository_lifecycle_registry(),
    );
    let (winner, loser, installed) = match (first, second) {
        (Ok(registry), Err(AdmissionError::Unavailable)) => (&f.services, &other, registry),
        (Err(AdmissionError::Unavailable), Ok(registry)) => (&other, &f.services, registry),
        _ => panic!("exactly one original Services observer must win"),
    };
    assert!(Arc::ptr_eq(
        &installed,
        &winner.repository_lifecycle_registry
    ));
    assert!(matches!(
        loser.repository_lifecycle_registry().await,
        Err(AdmissionError::Unavailable)
    ));
    assert!(Arc::ptr_eq(
        &installed,
        &winner.repository_lifecycle_registry().await.unwrap()
    ));
    assert_local_read(loser, &f.workspace).await;
}

#[tokio::test]
async fn a_preinstalled_foreign_observer_denies_installation_but_preserves_local_reads() {
    let f = Fixture::new().await;
    let foreign = Arc::new(RepositoryLifecycleRegistry::default());
    foreign.install(f.services.store()).await.unwrap();
    assert!(matches!(
        f.services.repository_lifecycle_registry().await,
        Err(AdmissionError::Unavailable)
    ));
    let observer: Arc<dyn RepositoryLifecycleObserver> = foreign;
    assert!(f
        .services
        .store()
        .has_repository_lifecycle_observer(&observer));
    f.assert_creation_unavailable(&f.services.repository_lifecycle_registry);
    assert_local_read(&f.services, &f.workspace).await;
}

#[tokio::test]
async fn installing_another_database_does_not_supply_the_original_services_observer() {
    let original = Fixture::new().await;
    let foreign = Fixture::new().await;
    let foreign_registry = foreign
        .services
        .repository_lifecycle_registry()
        .await
        .unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = foreign_registry.clone();
    assert!(!original
        .services
        .store()
        .has_repository_lifecycle_observer(&observer));
    original.assert_creation_unavailable(&foreign_registry);
    let original_registry = original
        .services
        .repository_lifecycle_registry()
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&original_registry, &foreign_registry));
    foreign.assert_creation_unavailable(&original_registry);
}

#[tokio::test]
async fn unknown_preinstallation_write_survives_reopen_and_keeps_local_reads_available() {
    let f = Fixture::new().await;
    // A real duplicate insert fails after the Store mutation barrier. Its
    // unknown completion is retained even after the last managed Store drops.
    assert!(f
        .services
        .store()
        .insert_workspace(&f.workspace)
        .await
        .is_err());
    assert!(matches!(
        f.services.repository_lifecycle_registry().await,
        Err(AdmissionError::Unavailable)
    ));
    assert_local_read(&f.services, &f.workspace).await;
    let Fixture {
        dir,
        services,
        workspace,
        ..
    } = f;
    drop(services);
    let reopened = Store::open(&dir.path().join("store.db")).await.unwrap();
    let replacement = Services::new(reopened);
    assert!(matches!(
        replacement.repository_lifecycle_registry().await,
        Err(AdmissionError::Unavailable)
    ));
    assert_local_read(&replacement, &workspace).await;
}

#[tokio::test]
async fn repeated_installation_and_reopen_cannot_settle_an_unknown_original_writer() {
    let f = Fixture::new().await;
    let original = f.services.repository_lifecycle_registry().await.unwrap();
    assert!(f
        .services
        .store()
        .insert_workspace(&f.workspace)
        .await
        .is_err());
    f.assert_creation_unavailable(&original);
    let reopened = Store::open(&f.dir.path().join("store.db")).await.unwrap();
    let observer: Arc<dyn RepositoryLifecycleObserver> = original.clone();
    assert!(reopened.has_repository_lifecycle_observer(&observer));
    let repeated = f.services.repository_lifecycle_registry().await.unwrap();
    assert!(Arc::ptr_eq(&original, &repeated));
    f.assert_creation_unavailable(&repeated);
    let replacement = Services::new(reopened);
    assert!(matches!(
        replacement.repository_lifecycle_registry().await,
        Err(AdmissionError::Unavailable)
    ));
    assert_local_read(&replacement, &f.workspace).await;
}
