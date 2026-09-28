//! The shared module also compiles in the standalone admission harness.
use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner,
};
use crate::repository_admission::request_context::current_read_request;
use intent_acp::mcp_server::request_context::McpRequestContext;
use intent_core::caller::{with_caller, Caller};
use intent_core::{chief_workspace, AgentId, AgentSession, WorkspaceId};

#[tokio::test]
async fn read_child_cleanup_and_last_scope_drop_preserve_only_original_siblings() {
    let dir = tempfile::Builder::new()
        .prefix("original-read-child-")
        .tempdir()
        .unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let mut workspace = chief_workspace();
    workspace.id = WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(serde_json::json!({
        "id":agent,"workspaceId":workspace.id,"name":"read child", "status":"active",
        "createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"
    }))
    .unwrap();
    store.insert_agent_session(&row).await.unwrap();
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    registry.install(&store).await.unwrap();
    let physical = RepositoryCreationOwner::allocate(
        &registry,
        &store,
        workspace.id.clone(),
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap()
    .initialize(&store, || async {
        Ok("original fixture completion".into())
    })
    .await
    .unwrap();
    let retained = Arc::new(17_u8);
    let owner = RepositoryReadOwner::retain_original(retained.clone(), store, registry).unwrap();
    let context = physical.callback().with_read_owner(Ok(owner));
    let scope = McpRequestContext::capture(&context);
    let sibling = McpRequestContext::capture(&context);
    let caller = Caller::Agent { agent_id: agent };
    let mut escaped = None;
    with_caller(
        caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert!(read.retains(retained.as_ref()));
            assert!(!read.retains(&17_u8));
            let mut child = read.child().unwrap();
            child
                .subscribe(&[
                    RepositoryLifecycleKey::Database,
                    RepositoryLifecycleKey::Workspace(workspace.id.clone()),
                ])
                .unwrap();
            assert_eq!(child.transfer(|| Ok(23)), Ok(23));
            let ended = child.retirement();
            drop(child);
            assert_eq!(ended.check_current(), Err(AdmissionError::Retired));
            let fresh = read.child().unwrap();
            assert_eq!(fresh.transfer(|| Ok(29)), Ok(29));
            escaped = Some((read, fresh));
        })),
    )
    .await;
    with_caller(
        caller.clone(),
        scope.scope(Box::pin(async {
            assert!(current_read_request().is_ok());
        })),
    )
    .await;
    let intermediate = scope.clone();
    drop(intermediate);
    with_caller(caller.clone(), async {
        assert!(escaped.as_ref().unwrap().0.check_current().is_ok());
    })
    .await;
    drop(scope);
    with_caller(caller.clone(), async {
        let (read, child) = escaped.as_ref().unwrap();
        assert_eq!(read.check_current(), Err(AdmissionError::Retired));
        assert_eq!(child.transfer(|| Ok(())), Err(AdmissionError::Retired));
    })
    .await;
    with_caller(
        caller,
        sibling.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert_ne!(
                read.correlation(),
                escaped.as_ref().unwrap().0.correlation()
            );
            assert!(read.child().unwrap().transfer(|| Ok(())).is_ok());
        })),
    )
    .await;
}

struct OptionalFixture {
    _dir: tempfile::TempDir,
    store: Store,
    registry: Arc<RepositoryLifecycleRegistry>,
    physical: crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalOwner,
    caller: Caller,
    context: crate::repository_admission::request_context::RepositoryCallbackContext,
}

impl OptionalFixture {
    async fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("optional-original-")
            .tempdir()
            .unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let mut workspace = chief_workspace();
        workspace.id = WorkspaceId::new();
        store.insert_workspace(&workspace).await.unwrap();
        let agent = AgentId::new();
        let row: AgentSession = serde_json::from_value(serde_json::json!({
            "id":agent,"workspaceId":workspace.id,"name":"optional original","status":"active",
            "createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"
        }))
        .unwrap();
        store.insert_agent_session(&row).await.unwrap();
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&store).await.unwrap();
        let physical = RepositoryCreationOwner::allocate(
            &registry,
            &store,
            workspace.id,
            agent.clone(),
            RepositoryCreationIntent::FirstSet,
        )
        .unwrap()
        .initialize(&store, || async {
            Ok("scripted original completion".into())
        })
        .await
        .unwrap();
        let owner =
            RepositoryReadOwner::retain_original(Arc::new(23_u8), store.clone(), registry.clone())
                .unwrap();
        let context = physical.callback().with_read_owner(Ok(owner));
        Self {
            _dir: dir,
            store,
            registry,
            physical,
            caller: Caller::Agent { agent_id: agent },
            context,
        }
    }
}

const OPTIONAL_TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn optional_local_key_retires_only_its_prepared_owner_and_required_coverage_stays_required() {
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    let sibling = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let mut required = read.child().unwrap();
            let key = RepositoryLifecycleKey::GitRoot(intent_core::WorkspaceGitRootId::new());
            let optional = read.capture_optional().unwrap();
            let metadata = optional.metadata();
            metadata
                .subscribe_metadata(&[RepositoryLifecycleKey::Database, key.clone()])
                .unwrap();
            let ready = optional
                .run_optional(|_| async { Ok("prebuilt guidance") })
                .unwrap()
                .await
                .unwrap();
            assert_eq!(ready.value(), &"prebuilt guidance");
            assert_eq!(
                required.transfer_with_optional(Some(ready.metadata()), Ok),
                Ok(true)
            );
            let ticket = f
                .registry
                .begin_mutation(std::slice::from_ref(&key))
                .unwrap();
            assert_eq!(
                required.transfer_with_optional(Some(&metadata), Ok),
                Ok(false)
            );
            assert!(read.check_current().is_ok());
            assert!(read
                .capture_optional()
                .unwrap()
                .metadata()
                .check_current()
                .is_ok());
            // Only the original ticket may clear this exact pending key.
            assert_eq!(
                read.capture_optional()
                    .unwrap()
                    .metadata()
                    .subscribe_metadata(&[RepositoryLifecycleKey::Database, key.clone()]),
                Err(AdmissionError::Unavailable)
            );
            ticket.settle_confirmed();
            required
                .subscribe(&[RepositoryLifecycleKey::Database, key.clone()])
                .unwrap();
            let next = read.capture_optional().unwrap();
            let next_metadata = next.metadata();
            let ticket = f.registry.begin_mutation(&[key]).unwrap();
            assert_eq!(read.check_current(), Err(AdmissionError::Retired));
            assert_eq!(next_metadata.check_current(), Err(AdmissionError::Retired));
            ticket.settle_confirmed();
        })),
    )
    .await;
    with_caller(
        f.caller.clone(),
        sibling.scope(Box::pin(async {
            assert!(current_read_request()
                .unwrap()
                .child()
                .unwrap()
                .transfer(|| Ok(()))
                .is_ok());
        })),
    )
    .await;
    assert!(f.store.list_workspaces(false).await.is_ok());
}

#[tokio::test]
async fn optional_execution_restores_original_identity_and_cannot_grow_required_coverage() {
    use crate::repository_admission::request_context::{
        current_source_lifetime, retire_current_request_on_denial,
    };
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let source = current_source_lifetime().unwrap();
            let mut child = read.child().unwrap();
            let optional = read.capture_optional().unwrap();
            let escaped = optional.metadata();
            let original = read.clone();
            let caller = f.caller.clone();
            let future = optional
                .run_optional(move |local| async move {
                    assert!(Arc::ptr_eq(&current_read_request().unwrap(), &original));
                    assert_eq!(current_caller(), Some(caller.clone()));
                    assert!(current_source_lifetime().is_err());
                    assert!(original.child().is_err());
                    assert!(original.capture_optional().is_err());
                    assert!(child
                        .subscribe(&[RepositoryLifecycleKey::Database])
                        .is_err());
                    assert!(child.transfer(|| Ok(())).is_err());
                    assert!(source
                        .subscribe(
                            &local.0.request.owner.store,
                            &caller,
                            &[RepositoryLifecycleKey::Database]
                        )
                        .is_err());
                    local
                        .subscribe_metadata(&[RepositoryLifecycleKey::Database])
                        .unwrap();
                    retire_current_request_on_denial(AdmissionError::Denied);
                    Ok(())
                })
                .unwrap();
            // The authenticated constructor already captured its entry. Polling on a
            // different ambient task restores only that previously checked caller.
            let result = with_caller(
                Caller::Agent {
                    agent_id: AgentId::new(),
                },
                future,
            )
            .await;
            assert!(matches!(result, Err(AdmissionError::Retired)));
            assert_eq!(escaped.check_current(), Err(AdmissionError::Retired));
            assert!(read.child().unwrap().transfer(|| Ok(())).is_ok());
            assert!(read.capture_optional().is_ok());
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_owner_closes_on_constructor_panic_error_unpolled_and_pending_drop() {
    use std::sync::atomic::AtomicUsize;
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let future = optional
                .run_optional(move |_| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                })
                .unwrap();
            drop(future);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            let result = tokio::spawn(
                optional
                    .run_optional(|_| -> std::future::Ready<AdmissionResult<()>> {
                        panic!("optional constructor fixture");
                    })
                    .unwrap(),
            )
            .await;
            assert!(result.is_err());
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            assert!(optional
                .run_optional(|_| async { Err::<(), _>(AdmissionError::Unavailable) })
                .unwrap()
                .await
                .is_err());
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            let mut future = optional
                .run_optional(|_| std::future::pending::<AdmissionResult<()>>())
                .unwrap();
            std::future::poll_fn(|context| {
                assert!(std::future::Future::poll(future.as_mut(), context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(future);
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(5),
                optional
                    .run_optional(|_| std::future::pending::<AdmissionResult<()>>())
                    .unwrap()
            )
            .await
            .is_err());
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            assert!(read.check_current().is_ok());
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_capture_and_run_refuse_foreign_entry_without_restoration_laundering() {
    use intent_core::caller::{with_wire_credential, WireCredential};
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert!(with_caller(
                Caller::Agent {
                    agent_id: AgentId::new()
                },
                async { read.capture_optional() }
            )
            .await
            .is_err());
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            assert!(with_caller(
                Caller::Agent {
                    agent_id: AgentId::new()
                },
                async { optional.run_optional(|_| async { Ok(()) }) }
            )
            .await
            .is_err());
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            let wire = WireCredential::Principal {
                principal_id: intent_core::PrincipalId::new(),
                token_hash: "opaque optional test".into(),
            };
            let optional = read.capture_optional().unwrap();
            let local = optional.metadata();
            assert!(with_wire_credential(Some(wire.clone()), async {
                optional.run_optional(|_| async { Ok(()) })
            })
            .await
            .is_err());
            assert_eq!(local.check_current(), Err(AdmissionError::Retired));
            assert!(
                with_wire_credential(Some(wire), async { read.capture_optional() })
                    .await
                    .is_err()
            );
            assert!(read.capture_optional().is_ok());
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_final_scope_drop_retires_escaped_metadata_after_successful_preparation() {
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    let sibling = McpRequestContext::capture(&f.context);
    let mut ready = None;
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            ready = Some(
                current_read_request()
                    .unwrap()
                    .capture_optional()
                    .unwrap()
                    .run_optional(|_| async { Ok(31) })
                    .unwrap()
                    .await
                    .unwrap(),
            );
        })),
    )
    .await;
    let clone = scope.clone();
    drop(clone);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            assert_eq!(ready.as_ref().unwrap().value(), &31);
            assert!(ready.as_ref().unwrap().metadata().check_current().is_ok());
        })),
    )
    .await;
    drop(scope);
    with_caller(f.caller.clone(), async {
        assert_eq!(
            ready.as_ref().unwrap().metadata().check_current(),
            Err(AdmissionError::Retired)
        );
    })
    .await;
    with_caller(
        f.caller.clone(),
        sibling.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(ready.as_ref().unwrap().metadata()), Ok),
                Err(AdmissionError::Denied)
            );
            let optional = read.capture_optional().unwrap();
            let metadata = optional.metadata();
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(&metadata), Ok),
                Ok(false)
            );
            let ready = optional
                .run_optional(|_| async { Ok(()) })
                .unwrap()
                .await
                .unwrap();
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(ready.metadata()), Ok),
                Ok(true)
            );
            drop(ready);
            assert_eq!(metadata.check_current(), Err(AdmissionError::Retired));
            assert!(read.check_current().is_ok());
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_busy_or_poisoned_local_fence_omits_without_waiting_or_poisoning_required_parent()
{
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let ready = read
                .capture_optional()
                .unwrap()
                .run_optional(|_| async { Ok(()) })
                .unwrap()
                .await
                .unwrap();
            let local = ready.metadata().0.lifetime.retirement();
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let held = local.clone();
            let thread = std::thread::spawn(move || {
                held.dispatch(|| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
                    Ok(())
                })
            });
            entered_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
            // Transfer must finish before releasing the optional-only fence.
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(ready.metadata()), Ok),
                Ok(false)
            );
            release_tx.send(()).unwrap();
            thread.join().unwrap().unwrap();
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(ready.metadata()), Ok),
                Ok(true)
            );
            let poisoned = local.clone();
            assert!(std::thread::spawn(
                move || poisoned.dispatch::<()>(|| panic!("optional fence poison"))
            )
            .join()
            .is_err());
            assert_eq!(
                read.child()
                    .unwrap()
                    .transfer_with_optional(Some(ready.metadata()), Ok),
                Ok(false)
            );
            assert!(read.check_current().is_ok());
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_parent_retirement_joins_every_concurrent_retire_before_scope_cleanup() {
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let ready = read
                .capture_optional()
                .unwrap()
                .run_optional(|_| async { Ok(()) })
                .unwrap()
                .await
                .unwrap();
            let local = ready.metadata().0.lifetime.retirement();
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let held = local.clone();
            let action = std::thread::spawn(move || {
                held.dispatch(|| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
                    Ok(())
                })
            });
            entered_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let mut waiters = Vec::new();
            for _ in 0..2 {
                let parent = read.original.retirement();
                let done = done_tx.clone();
                waiters.push(std::thread::spawn(move || {
                    parent.retire();
                    done.send(()).unwrap();
                }));
            }
            tokio::time::timeout(OPTIONAL_TEST_BUDGET, local.cancelled())
                .await
                .unwrap();
            assert!(done_rx.try_recv().is_err());
            // The registry is not held by the parent-to-local join.
            f.registry
                .begin_mutation(&[RepositoryLifecycleKey::GitRoot(
                    intent_core::WorkspaceGitRootId::new(),
                )])
                .unwrap()
                .settle_confirmed();
            release_tx.send(()).unwrap();
            action.join().unwrap().unwrap();
            for waiter in waiters {
                waiter.join().unwrap();
            }
            done_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
            done_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
            assert_eq!(read.child().err(), Some(AdmissionError::Retired));
            assert_eq!(
                ready.metadata().check_current(),
                Err(AdmissionError::Retired)
            );
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_parent_signal_drops_inline_future_but_never_claims_to_stop_a_detached_worker() {
    use std::sync::atomic::AtomicUsize;
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let dropped = Arc::new(AtomicUsize::new(0));
            let observed = dropped.clone();
            let optional = read.capture_optional().unwrap();
            let metadata = optional.metadata();
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let lease = Arc::new(());
            let weak = Arc::downgrade(&lease);
            let worker = tokio::task::spawn_blocking(move || {
                release_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
                drop(lease);
                19
            });
            let mut future = optional
                .run_optional(move |_| async move {
                    let _guard = Dropped(observed);
                    std::future::pending::<AdmissionResult<()>>().await
                })
                .unwrap();
            std::future::poll_fn(|context| {
                assert!(std::future::Future::poll(future.as_mut(), context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            f.physical.interrupt_requests();
            assert!(matches!(
                tokio::time::timeout(OPTIONAL_TEST_BUDGET, future)
                    .await
                    .unwrap(),
                Err(AdmissionError::Retired)
            ));
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(weak.upgrade().is_some());
            release_tx.send(()).unwrap();
            assert_eq!(worker.await.unwrap(), 19);
            assert!(weak.upgrade().is_none());
            assert_eq!(metadata.check_current(), Err(AdmissionError::Retired));
            // Worker completion supplied no fresh scope or evidence.
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_attach_racing_original_parent_retirement_cannot_escape_and_closes_registration() {
    let f = OptionalFixture::new().await;
    for _ in 0..12 {
        let scope = McpRequestContext::capture(&f.context);
        with_caller(
            f.caller.clone(),
            scope.scope(Box::pin(async {
                let read = current_read_request().unwrap();
                let barrier = Arc::new(std::sync::Barrier::new(2));
                let release = barrier.clone();
                let parent = read.original.retirement();
                let retire = std::thread::spawn(move || {
                    release.wait();
                    parent.retire();
                });
                barrier.wait();
                let captured = read.capture_optional();
                retire.join().unwrap();
                if let Ok(optional) = captured {
                    assert_eq!(
                        optional.metadata().check_current(),
                        Err(AdmissionError::Retired)
                    );
                }
                assert!(read.capture_optional().is_err());
            })),
        )
        .await;
    }
}

#[tokio::test]
async fn optional_shared_required_fence_waits_and_retirement_wins_before_later_transfer() {
    let f = OptionalFixture::new().await;
    let scope = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            let ready = read
                .capture_optional()
                .unwrap()
                .run_optional(|_| async { Ok(()) })
                .unwrap()
                .await
                .unwrap();
            let parent = read.original.retirement();
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let held = parent.clone();
            let action = std::thread::spawn(move || {
                held.dispatch(|| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
                    Ok(())
                })
            });
            entered_rx.recv_timeout(OPTIONAL_TEST_BUDGET).unwrap();
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
            let retiring = parent.clone();
            let waiter = std::thread::spawn(move || {
                retiring.retire();
                done_tx.send(()).unwrap();
            });
            tokio::time::timeout(OPTIONAL_TEST_BUDGET, parent.cancelled())
                .await
                .unwrap();
            assert!(done_rx.try_recv().is_err());
            release_tx.send(()).unwrap();
            action.join().unwrap().unwrap();
            waiter.join().unwrap();
            assert!(read.child().is_err());
            assert_eq!(
                ready.metadata().check_current(),
                Err(AdmissionError::Retired)
            );
        })),
    )
    .await;
}

#[tokio::test]
async fn optional_pending_workspace_barrier_retires_original_but_preserves_fresh_physical_capture()
{
    let f = OptionalFixture::new().await;
    let workspace = f.store.list_workspaces(false).await.unwrap().remove(0).id;
    let scope = McpRequestContext::capture(&f.context);
    let mut ready = None;
    with_caller(
        f.caller.clone(),
        scope.scope(Box::pin(async {
            ready = Some(
                current_read_request()
                    .unwrap()
                    .capture_optional()
                    .unwrap()
                    .run_optional(|_| async { Ok(()) })
                    .unwrap()
                    .await
                    .unwrap(),
            );
        })),
    )
    .await;
    // Real observer pending semantics; public deletion timers are unchanged.
    let pending = f
        .registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(workspace)])
        .unwrap();
    with_caller(f.caller.clone(), async {
        assert_eq!(
            ready.as_ref().unwrap().metadata().check_current(),
            Err(AdmissionError::Retired)
        );
    })
    .await;
    let blocked = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        blocked.scope(Box::pin(async {
            assert!(current_read_request().is_err());
        })),
    )
    .await;
    pending.settle_confirmed();
    let fresh = McpRequestContext::capture(&f.context);
    with_caller(
        f.caller.clone(),
        fresh.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert!(read.capture_optional().is_ok());
            assert!(read.child().unwrap().transfer(|| Ok(())).is_ok());
        })),
    )
    .await;
}
