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

// Actual Services and strict Store-confirmed physical owners. Only the original
// ACP producer completion is a fixture; no native/read permission is supplied.
mod read_owner {
    use intent_acp::mcp_server::request_context::{McpRequestContext, McpRequestScope};
    use intent_core::caller::{with_wire_credential, WireCredential};
    use intent_core::PrincipalId;
    use intent_store::RepositoryLifecycleKey;

    use super::*;
    use crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalOwner;
    use crate::repository_admission::read_request::RepositoryReadRequest;
    use crate::repository_admission::request_context::{
        current_read_request, current_source_lifetime, retire_current_request_on_denial,
        RepositoryCallbackContext,
    };

    fn caller(f: &Fixture) -> Caller {
        Caller::Agent {
            agent_id: f.agent.clone(),
        }
    }

    async fn creator(f: &Fixture) -> RepositoryCreationOwner {
        let registry = f.services.repository_lifecycle_registry().await.unwrap();
        RepositoryCreationOwner::allocate(
            &registry,
            f.services.store(),
            f.workspace.id.clone(),
            f.agent.clone(),
            RepositoryCreationIntent::FirstSet,
        )
        .unwrap()
    }

    async fn confirmed(f: &Fixture) -> RepositoryPhysicalOwner {
        creator(f)
            .await
            .initialize(f.services.store(), || async {
                Ok("original read fixture".into())
            })
            .await
            .unwrap()
    }

    fn anchored(f: &Fixture, owner: &RepositoryPhysicalOwner) -> RepositoryCallbackContext {
        owner
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(Arc::new(f.services.clone())))
    }

    #[tokio::test]
    async fn original_installed_factory_keeps_policy_but_never_repairs_an_unavailable_anchor() {
        let f = Fixture::new().await;
        let original = Arc::new(f.services.clone());
        let failed = RepositoryReadOwner::capture(original.clone());
        assert!(failed.is_err());
        let owner = confirmed(&f).await;
        let unavailable = owner.callback().with_read_owner(failed);
        assert!(McpRequestContext::capture(&unavailable)
            .private_result_policy()
            .is_none());
        let callback = owner
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(original.clone()));
        let scope = McpRequestContext::capture(&callback);
        let policy = scope
            .private_result_policy()
            .expect("same original installed factory");
        let escaped = read(&scope, caller(&f)).await.unwrap();
        assert!(escaped.retains(original.as_ref()));
        assert!(!escaped.retains(&f.services));
        drop(scope);
        with_caller(caller(&f), async {
            assert_eq!(escaped.check_current(), Err(AdmissionError::Retired));
        })
        .await;
        drop(policy);
        let fresh = McpRequestContext::capture(&callback);
        assert!(read(&fresh, caller(&f)).await.is_ok());
    }

    async fn read(
        scope: &Arc<dyn McpRequestScope>,
        original_caller: Caller,
    ) -> AdmissionResult<Arc<RepositoryReadRequest>> {
        let mut result = Err(AdmissionError::Unavailable);
        with_caller(
            original_caller,
            scope.scope(Box::pin(async {
                result = current_read_request();
            })),
        )
        .await;
        result
    }

    #[tokio::test]
    async fn capture_retains_exact_services_without_installation_or_gate_wait() {
        let f = Fixture::new().await;
        let services = Arc::new(f.services.clone());
        let original = Arc::downgrade(&services);
        let observer: Arc<dyn RepositoryLifecycleObserver> =
            services.repository_lifecycle_registry.clone();
        assert!(matches!(
            RepositoryReadOwner::capture(services.clone()),
            Err(AdmissionError::Unavailable)
        ));
        assert!(!services
            .store()
            .has_repository_lifecycle_observer(&observer));
        services.repository_lifecycle_registry().await.unwrap();
        let directory = services.repository_connection_directory();
        let daemon = services.daemon_boot_id.clone();
        let gate = services.gitlab_credential_gate.lock().await;
        let retained = services
            .worktree_locks
            .with_lock(f.dir.path(), || async {
                RepositoryReadOwner::capture(services.clone()).unwrap()
            })
            .await;
        drop(gate);
        drop(services);
        let same = original
            .upgrade()
            .expect("the exact supplied Arc remains retained");
        assert!(Arc::ptr_eq(
            &directory,
            &same.repository_connection_directory()
        ));
        assert_eq!(same.daemon_boot_id, daemon);
        assert!(same
            .store()
            .shares_repository_lifecycle_domain(f.services.store()));
        drop(same);
        drop(retained);
        assert!(original.upgrade().is_none());
    }

    #[tokio::test]
    async fn final_scope_drop_retires_escaped_read_and_source_after_successful_preparation() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let callback = anchored(&f, &owner);
        let committed = serde_json::to_value(
            f.services
                .store()
                .get_agent_session(&f.agent)
                .await
                .unwrap(),
        )
        .unwrap();
        for retain_clone in [false, true] {
            let scope = McpRequestContext::capture(&callback);
            let clone = scope.clone();
            let sibling = McpRequestContext::capture(&callback);
            let sibling_read = read(&sibling, caller(&f)).await.unwrap();
            let mut escaped = None;
            let mut result = None;
            with_wire_credential(
                None,
                with_caller(
                    caller(&f),
                    scope.scope(Box::pin(async {
                        escaped = Some((
                            current_read_request().unwrap(),
                            current_source_lifetime().unwrap(),
                        ));
                        result = Some("original completed body");
                    })),
                ),
            )
            .await;
            let (original_read, original_source) = escaped.unwrap();
            let remaining = if retain_clone {
                drop(scope);
                clone
            } else {
                drop(clone);
                scope
            };
            let mut prepared = None;
            with_wire_credential(
                None,
                with_caller(
                    caller(&f),
                    remaining.scope(Box::pin(async {
                        assert!(Arc::ptr_eq(
                            &original_read,
                            &current_read_request().unwrap()
                        ));
                        assert!(original_read.check_current().is_ok());
                        assert!(original_source.retirement().check_current().is_ok());
                        let source = current_source_lifetime().unwrap();
                        let subscription = source
                            .subscribe(
                                f.services.store(),
                                &caller(&f),
                                &[RepositoryLifecycleKey::Database],
                            )
                            .unwrap();
                        drop(subscription);
                        assert!(original_read.check_current().is_ok());
                        prepared = Some("original completed preparation");
                    })),
                ),
            )
            .await;
            drop(remaining);
            with_wire_credential(
                None,
                with_caller(caller(&f), async {
                    assert_eq!(
                        (
                            original_read.check_current(),
                            original_source.retirement().check_current(),
                        ),
                        (Err(AdmissionError::Retired), Err(AdmissionError::Retired)),
                        "escaped metadata must not outlive the final MCP scope"
                    );
                    assert!(matches!(
                        original_source.subscribe(
                            f.services.store(),
                            &caller(&f),
                            &[RepositoryLifecycleKey::Database],
                        ),
                        Err(AdmissionError::Retired)
                    ));
                    assert!(sibling_read.check_current().is_ok());
                    assert!(read(&sibling, caller(&f)).await.is_ok());
                    let fresh = McpRequestContext::capture(&callback);
                    assert!(read(&fresh, caller(&f)).await.is_ok());
                    assert_eq!(original_read.check_current(), Err(AdmissionError::Retired));
                }),
            )
            .await;
            assert_eq!(result, Some("original completed body"));
            assert_eq!(prepared, Some("original completed preparation"));
            assert_eq!(
                serde_json::to_value(
                    f.services
                        .store()
                        .get_agent_session(&f.agent)
                        .await
                        .unwrap()
                )
                .unwrap(),
                committed
            );
        }
    }

    #[tokio::test]
    async fn same_capture_survives_normal_source_cleanup_and_retires_with_original_request() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let callback = anchored(&f, &owner);
        let scope = McpRequestContext::capture(&callback);
        assert!(matches!(
            current_read_request(),
            Err(AdmissionError::Unavailable)
        ));
        let first = read(&scope, caller(&f)).await.unwrap();
        let other_scope = McpRequestContext::capture(&callback);
        let other = read(&other_scope, caller(&f)).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &other));
        with_caller(
            caller(&f),
            scope.scope(Box::pin(async {
                let lifetime = current_source_lifetime().unwrap();
                let subscribed = lifetime
                    .subscribe(
                        f.services.store(),
                        &caller(&f),
                        &[
                            RepositoryLifecycleKey::Database,
                            RepositoryLifecycleKey::Workspace(f.workspace.id.clone()),
                        ],
                    )
                    .unwrap();
                drop(subscribed);
                assert!(Arc::ptr_eq(&first, &current_read_request().unwrap()));
                assert!(first.check_current().is_ok());
            })),
        )
        .await;
        let later = read(&scope, caller(&f)).await.unwrap();
        assert!(Arc::ptr_eq(&first, &later));
        with_caller(
            caller(&f),
            scope.scope(Box::pin(async {
                retire_current_request_on_denial(AdmissionError::Denied);
                assert_eq!(first.check_current(), Err(AdmissionError::Retired));
                assert!(other.check_current().is_ok());
            })),
        )
        .await;
        assert!(matches!(
            read(&scope, caller(&f)).await,
            Err(AdmissionError::Retired)
        ));
        assert!(read(&McpRequestContext::capture(&callback), caller(&f))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn missing_and_failed_read_anchors_preserve_ordinary_body_and_valid_source() {
        let f = Fixture::new().await;
        let unavailable = RepositoryReadOwner::capture(Arc::new(f.services.clone()));
        assert!(matches!(unavailable, Err(AdmissionError::Unavailable)));
        let owner = confirmed(&f).await;
        let available = RepositoryReadOwner::capture(Arc::new(f.services.clone()));
        let callbacks = [
            owner.callback(),
            owner
                .callback()
                .with_read_owner(unavailable)
                .with_read_owner(available),
        ];
        for callback in callbacks {
            let scope = McpRequestContext::capture(&callback);
            let mut bodies = 0;
            let mut result = Ok(());
            for _ in 0..2 {
                with_caller(
                    caller(&f),
                    scope.scope(Box::pin(async {
                        assert!(matches!(
                            current_read_request(),
                            Err(AdmissionError::Unavailable)
                        ));
                        let lifetime = current_source_lifetime().unwrap();
                        let _subscription = lifetime
                            .subscribe(
                                f.services.store(),
                                &caller(&f),
                                &[RepositoryLifecycleKey::Database],
                            )
                            .unwrap();
                        bodies += 1;
                        result = Err("ordinary provider error remains original");
                    })),
                )
                .await;
            }
            assert_eq!(bodies, 2);
            assert_eq!(result, Err("ordinary provider error remains original"));
        }
    }

    #[tokio::test]
    async fn authentic_caller_is_captured_before_restore_and_wire_cannot_be_borrowed() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let callback = anchored(&f, &owner);
        let wire = WireCredential::Principal {
            principal_id: PrincipalId::new(),
            token_hash: "fixture wire provenance".into(),
        };
        let scope = with_wire_credential(
            Some(wire.clone()),
            with_caller(Caller::Daemon, async {
                McpRequestContext::capture(&callback)
            }),
        )
        .await;
        let original = read(&scope, caller(&f)).await.unwrap();
        assert_eq!(original.check_current(), Err(AdmissionError::Denied));
        for wrong in [
            Caller::Daemon,
            Caller::Agent {
                agent_id: AgentId::new(),
            },
        ] {
            assert!(matches!(
                read(&scope, wrong).await,
                Err(AdmissionError::Denied)
            ));
        }
        assert!(matches!(
            with_wire_credential(Some(wire), read(&scope, caller(&f))).await,
            Err(AdmissionError::Denied)
        ));
        assert!(Arc::ptr_eq(
            &original,
            &read(&scope, caller(&f)).await.unwrap()
        ));
    }

    #[tokio::test]
    async fn equal_ids_and_same_observer_on_foreign_store_cannot_supply_original_anchor() {
        let f = Fixture::new().await;
        let registry = f.services.repository_lifecycle_registry().await.unwrap();
        let foreign = Store::open(&f.dir.path().join("foreign.db")).await.unwrap();
        foreign.insert_workspace(&f.workspace).await.unwrap();
        foreign
            .insert_agent_session(
                &f.services
                    .store()
                    .get_agent_session(&f.agent)
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        registry.install(&foreign).await.unwrap();
        let observer: Arc<dyn RepositoryLifecycleObserver> = registry.clone();
        assert!(foreign.has_repository_lifecycle_observer(&observer));
        assert!(!foreign.shares_repository_lifecycle_domain(f.services.store()));
        let owner = RepositoryCreationOwner::allocate(
            &registry,
            &foreign,
            f.workspace.id.clone(),
            f.agent.clone(),
            RepositoryCreationIntent::FirstSet,
        )
        .unwrap()
        .initialize(&foreign, || async { Ok("foreign original".into()) })
        .await
        .unwrap();
        let scope = McpRequestContext::capture(&anchored(&f, &owner));
        assert!(matches!(
            read(&scope, caller(&f)).await,
            Err(AdmissionError::Denied)
        ));
        with_caller(
            caller(&f),
            scope.scope(Box::pin(async {
                let lifetime = current_source_lifetime().unwrap();
                let _subscription = lifetime
                    .subscribe(&foreign, &caller(&f), &[RepositoryLifecycleKey::Database])
                    .unwrap();
            })),
        )
        .await;
    }

    #[tokio::test]
    async fn independent_services_registry_and_directory_cannot_replace_original_anchor() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let other = Services::new(f.services.store().clone());
        assert!(!Arc::ptr_eq(
            &f.services.repository_lifecycle_registry,
            &other.repository_lifecycle_registry
        ));
        assert!(!Arc::ptr_eq(
            &f.services.repository_connection_directory(),
            &other.repository_connection_directory()
        ));
        let scope = McpRequestContext::capture(
            &owner
                .callback()
                .with_read_owner(RepositoryReadOwner::capture(Arc::new(other))),
        );
        assert!(matches!(
            read(&scope, caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        assert!(read(
            &McpRequestContext::capture(&anchored(&f, &owner)),
            caller(&f)
        )
        .await
        .is_ok());

        let foreign = Fixture::new().await;
        foreign
            .services
            .repository_lifecycle_registry()
            .await
            .unwrap();
        let available_foreign = RepositoryReadOwner::capture(Arc::new(foreign.services.clone()));
        assert!(available_foreign.is_ok());
        let wrong =
            McpRequestContext::capture(&owner.callback().with_read_owner(available_foreign));
        assert!(matches!(
            read(&wrong, caller(&f)).await,
            Err(AdmissionError::Denied)
        ));
    }

    #[tokio::test]
    async fn pending_projection_and_preconfirmation_scope_never_gain_read_ownership() {
        let f = Fixture::new().await;
        let creator = creator(&f).await;
        let pending = creator
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(Arc::new(f.services.clone())));
        let scope = McpRequestContext::capture(&pending);
        assert!(matches!(
            read(&scope, caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        let owner = creator
            .initialize(f.services.store(), || async { Ok("confirmed".into()) })
            .await
            .unwrap();
        assert!(matches!(
            read(&scope, caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        assert!(matches!(
            read(&McpRequestContext::capture(&pending), caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        assert!(read(
            &McpRequestContext::capture(&anchored(&f, &owner)),
            caller(&f)
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn retained_anchor_cannot_repair_unknown_barrier_or_blocked_capture() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let callback = anchored(&f, &owner);
        let before = McpRequestContext::capture(&callback);
        let key = RepositoryLifecycleKey::Workspace(f.workspace.id.clone());
        let known = f
            .services
            .store()
            .begin_repository_pending_delete(std::slice::from_ref(&key))
            .await
            .unwrap();
        let blocked = McpRequestContext::capture(&callback);
        assert!(matches!(
            read(&blocked, caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        known.settle_confirmed();
        assert!(matches!(
            read(&blocked, caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
        assert!(matches!(
            read(&before, caller(&f)).await,
            Err(AdmissionError::Retired)
        ));
        assert!(read(&McpRequestContext::capture(&callback), caller(&f))
            .await
            .is_ok());
        drop(
            f.services
                .store()
                .begin_repository_pending_delete(std::slice::from_ref(&key))
                .await
                .unwrap(),
        );
        f.services
            .store()
            .begin_repository_pending_delete(&[key])
            .await
            .unwrap()
            .settle_confirmed();
        assert!(
            RepositoryReadOwner::capture(Arc::new(f.services.clone())).is_ok(),
            "retention alone is not a usable request"
        );
        assert!(matches!(
            read(&McpRequestContext::capture(&callback), caller(&f)).await,
            Err(AdmissionError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn invalidated_domain_identity_never_supplies_usable_read_evidence() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let callback = anchored(&f, &owner);
        let scope = McpRequestContext::capture(&callback);
        let read_owner = read(&scope, caller(&f)).await.unwrap();
        let clone = f.services.store().clone();
        clone.close().await;
        let path = f.dir.path().join("store.db");
        let previous = f.dir.path().join("previous.db");
        std::fs::rename(&path, &previous).unwrap();
        std::fs::copy(&previous, &path).unwrap();
        assert!(Store::open(&path).await.is_err());
        assert!(clone.shares_repository_lifecycle_domain(f.services.store()));
        assert!(matches!(
            RepositoryReadOwner::capture(Arc::new(f.services.clone())),
            Err(AdmissionError::Unavailable)
        ));
        assert_eq!(
            with_caller(caller(&f), async { read_owner.check_current() }).await,
            Err(AdmissionError::Unavailable)
        );
        let mut ran = false;
        with_caller(
            caller(&f),
            scope.scope(Box::pin(async {
                assert!(current_read_request().is_err());
                ran = true;
            })),
        )
        .await;
        assert!(ran);
    }

    #[tokio::test]
    async fn services_and_store_retention_never_keep_the_original_physical_owner_alive() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let services = Arc::new(f.services.clone());
        let weak_services = Arc::downgrade(&services);
        let callback = owner
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(services));
        let scope = McpRequestContext::capture(&callback);
        let original = read(&scope, caller(&f)).await.unwrap();
        let retirement = owner.retirement();
        drop(owner);
        assert!(weak_services.upgrade().is_some());
        retirement.retire();
        assert_eq!(
            with_caller(caller(&f), async { original.check_current() }).await,
            Err(AdmissionError::Retired)
        );
        assert!(matches!(
            read(&McpRequestContext::capture(&callback), caller(&f)).await,
            Err(AdmissionError::Retired)
        ));
    }

    #[tokio::test]
    async fn unpolled_and_pending_scope_cancellation_retire_the_same_read_sidecar() {
        for polled in [false, true] {
            let f = Fixture::new().await;
            let owner = confirmed(&f).await;
            let callback = anchored(&f, &owner);
            let scope = McpRequestContext::capture(&callback);
            let original = read(&scope, caller(&f)).await.unwrap();
            let mut starts = 0;
            let mut future = scope.scope(Box::pin(async {
                starts += 1;
                std::future::pending::<()>().await;
            }));
            if polled {
                std::future::poll_fn(|context| {
                    assert!(std::future::Future::poll(future.as_mut(), context).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
            }
            drop(future);
            assert_eq!(starts, usize::from(polled));
            assert_eq!(
                with_caller(caller(&f), async { original.check_current() }).await,
                Err(AdmissionError::Retired)
            );
            assert!(read(&McpRequestContext::capture(&callback), caller(&f))
                .await
                .is_ok());
        }
    }
    #[tokio::test]
    async fn optional_capture_keeps_actual_services_and_never_extends_physical_owner() {
        let f = Fixture::new().await;
        let owner = confirmed(&f).await;
        let original = Arc::new(f.services.clone());
        let context = owner
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(original.clone()));
        let scope = McpRequestContext::capture(&context);
        let mut ready = None;
        with_caller(
            caller(&f),
            scope.scope(Box::pin(async {
                let read = current_read_request().unwrap();
                assert!(read.retains(original.as_ref()));
                assert!(!read.retains(&original.as_ref().clone()));
                ready = Some(
                    read.capture_optional()
                        .unwrap()
                        .run_optional(|local| async move {
                            local.subscribe_metadata(&[RepositoryLifecycleKey::Database])?;
                            Ok(())
                        })
                        .unwrap()
                        .await
                        .unwrap(),
                );
            })),
        )
        .await;
        drop(owner);
        with_caller(caller(&f), async {
            assert_eq!(
                ready.as_ref().unwrap().metadata().check_current(),
                Err(AdmissionError::Retired)
            );
        })
        .await;
        assert_local_read(&f.services, &f.workspace).await;
        assert!(McpRequestContext::capture(&context)
            .private_result_policy()
            .is_none());
    }
}
