//! These captures use the existing fixture origin. Actual initialization and
//! transport integration require their separate immutable owner dependencies.

use intent_core::caller::with_caller;
use intent_core::AgentId;
use intent_store::{RepositoryLifecycleKey, RepositoryLifecycleObserver};

use super::*;
use crate::repository_admission::lifecycle::FixtureOriginOwner;

fn fixture() -> (Arc<RepositoryLifecycleRegistry>, FixtureOriginOwner, Caller) {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let caller = Caller::Agent {
        agent_id: AgentId::new(),
    };
    let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
    (registry, owner, caller)
}

#[tokio::test]
async fn failed_read_attachment_keeps_original_body_source_and_preparation() {
    let (registry, owner, caller) = fixture();
    let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()))
        .with_read_owner(Err(AdmissionError::Denied))
        .with_read_owner(Err(AdmissionError::Retired));
    let scope = McpRequestContext::capture(&callback);
    let mut bodies = 0;
    for _ in 0..2 {
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                assert!(matches!(
                    current_read_request(),
                    Err(AdmissionError::Denied)
                ));
                assert!(current_source_lifetime().is_ok());
                bodies += 1;
            })),
        )
        .await;
    }
    assert_eq!(bodies, 2);
    assert!(matches!(
        current_read_request(),
        Err(AdmissionError::Unavailable)
    ));
}

#[tokio::test]
async fn absent_projection_and_its_captures_never_gain_a_later_origin() {
    let (registry, owner, caller) = fixture();
    let absent = RepositoryCallbackContext::new(&registry, None);
    let old = absent.capture();
    let fresh = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
    with_caller(caller, async {
        assert!(matches!(
            old.source_lifetime(),
            Err(AdmissionError::Unavailable)
        ));
        assert!(matches!(
            absent.capture().source_lifetime(),
            Err(AdmissionError::Unavailable)
        ));
        assert!(fresh.capture().source_lifetime().is_ok());
    })
    .await;
}

#[tokio::test]
async fn interrupt_retires_original_queued_requests_without_retiring_the_physical_owner() {
    let (registry, owner, caller) = fixture();
    let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
    let original = callback.capture();
    let other = callback.capture();
    registry.cancel_origin_requests(&owner.origin());
    with_caller(caller, async {
        assert!(matches!(
            original.source_lifetime(),
            Err(AdmissionError::Retired)
        ));
        assert!(matches!(
            other.source_lifetime(),
            Err(AdmissionError::Retired)
        ));
        let next = callback.capture();
        assert!(next.source_lifetime().is_ok());
        assert!(matches!(
            original.source_lifetime(),
            Err(AdmissionError::Retired)
        ));
    })
    .await;
}

#[tokio::test]
async fn request_completion_retires_escaped_lifetime_after_the_last_captured_scope_drops() {
    let (registry, owner, caller) = fixture();
    let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
    let captured = callback.capture();
    let guidance = captured.clone();
    let lifetime = with_caller(caller.clone(), async {
        captured.source_lifetime().unwrap()
    })
    .await;
    drop(captured);
    assert!(lifetime.retirement().check_current().is_ok());
    drop(guidance);
    assert_eq!(
        lifetime.retirement().check_current(),
        Err(AdmissionError::Retired)
    );
    with_caller(caller, async {
        assert!(callback.capture().source_lifetime().is_ok());
    })
    .await;
}

#[tokio::test]
async fn physical_retirement_before_dispatch_cannot_be_repaired_by_a_new_owner_with_the_same_id() {
    let (registry, owner, caller) = fixture();
    let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
    let queued = callback.capture();
    let Caller::Agent { agent_id } = &caller else {
        unreachable!()
    };
    registry
        .begin_mutation(&[RepositoryLifecycleKey::Agent(agent_id.clone())])
        .unwrap()
        .settle_confirmed();
    let replacement = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
    with_caller(caller, async {
        assert!(matches!(
            queued.source_lifetime(),
            Err(AdmissionError::Retired)
        ));
        assert!(matches!(
            callback.capture().source_lifetime(),
            Err(AdmissionError::Retired)
        ));
        assert!(
            RepositoryCallbackContext::new(&registry, Some(replacement.origin()))
                .capture()
                .source_lifetime()
                .is_ok()
        );
    })
    .await;
}

#[tokio::test]
async fn captured_physical_identity_does_not_replace_the_authentic_transport_caller() {
    let (registry, owner, caller) = fixture();
    let captured = RepositoryCallbackContext::new(&registry, Some(owner.origin())).capture();
    assert!(matches!(
        captured.source_lifetime(),
        Err(AdmissionError::Denied)
    ));
    with_caller(
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        async {
            assert!(matches!(
                captured.source_lifetime(),
                Err(AdmissionError::Denied)
            ));
        },
    )
    .await;
    with_caller(caller, async {
        assert!(captured.source_lifetime().is_ok());
    })
    .await;
    drop(owner);
    assert_eq!(
        captured.retirement.check_current(),
        Err(AdmissionError::Retired)
    );
}

#[tokio::test]
async fn acp_scope_retains_the_original_capture_through_operation_and_preparation() {
    let (registry, owner, caller) = fixture();
    let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
    let scope = McpRequestContext::capture(&callback);
    let mut original = None;
    with_caller(caller, async {
        scope
            .scope(Box::pin(async {
                original = Some(CAPTURED_REQUEST.with(Arc::clone));
                assert!(current_source_lifetime().is_ok());
            }))
            .await;
        let replacement = McpRequestContext::capture(&callback);
        scope
            .scope(Box::pin(async {
                assert!(CAPTURED_REQUEST
                    .with(|current| Arc::ptr_eq(current, original.as_ref().unwrap())));
                assert!(current_source_lifetime().is_ok());
            }))
            .await;
        drop(replacement);
    })
    .await;
    assert!(matches!(
        current_source_lifetime(),
        Err(AdmissionError::Unavailable)
    ));
    drop(scope);
    assert!(original
        .as_ref()
        .unwrap()
        .retirement
        .check_current()
        .is_ok());
    let leaf = original.take().unwrap().retirement.clone();
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
}

#[tokio::test]
async fn acp_scope_preserves_the_body_result_even_when_the_original_origin_is_gone() {
    let (registry, owner, caller) = fixture();
    let scope = McpRequestContext::capture(&RepositoryCallbackContext::new(
        &registry,
        Some(owner.origin()),
    ));
    drop(owner);
    let mut output = None;
    with_caller(
        caller,
        scope.scope(Box::pin(async {
            assert!(matches!(
                current_source_lifetime(),
                Err(AdmissionError::Retired)
            ));
            output = Some(serde_json::json!({"ordinary": "completed"}));
        })),
    )
    .await;
    assert_eq!(output, Some(serde_json::json!({"ordinary": "completed"})));
}

#[tokio::test]
async fn dropping_an_unpolled_or_pending_acp_scope_retires_escaped_request_clones() {
    for polled in [false, true] {
        let (registry, owner, caller) = fixture();
        let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
        let captured = callback.capture();
        let scope = RepositoryRequestScope {
            original: captured.clone(),
            read: Err(AdmissionError::Unavailable),
        };
        let mut body_started = false;
        let mut future = scope.scope(Box::pin(async {
            body_started = true;
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
        assert_eq!(body_started, polled);
        with_caller(caller, async {
            assert!(matches!(
                captured.source_lifetime(),
                Err(AdmissionError::Retired)
            ));
            assert!(callback.capture().source_lifetime().is_ok());
        })
        .await;
    }
}

#[tokio::test]
async fn source_cleanup_retires_only_its_child_and_parent_retirement_fences_every_child() {
    let (registry, owner, caller) = fixture();
    let captured = RepositoryCallbackContext::new(&registry, Some(owner.origin())).capture();
    with_caller(caller, async {
        let first = captured.source_lifetime().unwrap();
        let sibling = captured.source_lifetime().unwrap();
        first.retirement().end_scope();
        assert!(sibling.retirement().check_current().is_ok());
        assert!(captured.source_lifetime().is_ok());
        captured.retirement.retire();
        assert_eq!(
            sibling
                .retirement()
                .dispatch(|| panic!("retired request dispatched a child")),
            Err::<(), _>(AdmissionError::Retired)
        );
        assert!(matches!(
            captured.source_lifetime(),
            Err(AdmissionError::Retired)
        ));
    })
    .await;
}

#[tokio::test]
async fn request_retirement_waits_for_its_original_child_consuming_fence() {
    let (registry, owner, caller) = fixture();
    let captured = RepositoryCallbackContext::new(&registry, Some(owner.origin())).capture();
    let child = with_caller(caller, async {
        captured.source_lifetime().unwrap().retirement()
    })
    .await;
    let (entered, inside) = std::sync::mpsc::channel();
    let (release, hold) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        child.dispatch(|| {
            entered.send(()).unwrap();
            hold.recv().unwrap();
            Ok(())
        })
    });
    inside.recv().unwrap();
    let parent = captured.retirement.clone();
    let (started, waiting) = std::sync::mpsc::channel();
    let (done, retired) = std::sync::mpsc::channel();
    let retirement = std::thread::spawn(move || {
        started.send(()).unwrap();
        parent.retire();
        done.send(()).unwrap();
    });
    waiting.recv().unwrap();
    assert!(retired
        .recv_timeout(std::time::Duration::from_millis(40))
        .is_err());
    release.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), Ok(()));
    retirement.join().unwrap();
    retired.recv().unwrap();
}
