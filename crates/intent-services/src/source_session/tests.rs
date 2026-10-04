//! Real indexed Store and membership, with explicitly local delivery retirement.
use super::*;
use crate::{
    prepared_source_bootstrap::{Context, Mode},
    Services,
};
use intent_core::{
    note_artifact::request::Primitive,
    note_source_session::{Binding, Control, Descriptor, Operation, Read, Reason, SessionError},
    Caller, NoteId, WorkspaceId,
};
use intent_store::CanonicalSourceBinding;
use serde_json::{json, Value};
use std::sync::Arc;
async fn page(
    services: &Services,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
    value: Value,
) -> Value {
    services
        .store
        .read_note_page(
            &ws.0,
            &note.0,
            principal,
            serde_json::from_value(value).unwrap(),
            &json!("source-fixture"),
        )
        .await
        .unwrap()
}

async fn binding_as(
    services: &Services,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
) -> CanonicalSourceBinding {
    let first = page(
        services,
        ws,
        note,
        principal,
        json!({"kind":"source","maxSourceBytes":128,"maxWireBytes":8192}),
    )
    .await;
    let context = page(services, ws, note, principal, json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let owner = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["nodeType"] == "diffBlock" || v["nodeType"] == "mermaidBlock")
        .unwrap();
    let root = page(
        services,
        ws,
        note,
        principal,
        json!({"kind":"metadata","ref":owner["attributesRef"],"maxItems":1,"maxWireBytes":8192}),
    )
    .await;
    let fields = page(services, ws, note, principal, json!({"kind":"metadata","ref":root["items"][0]["childrenRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let code = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "code")
        .unwrap();
    CanonicalSourceBinding {
        scope: serde_json::from_value(first["scope"].clone()).unwrap(),
        snapshot_id: first["snapshotId"].as_str().unwrap().into(),
        source_revision: first["sourceRevision"].as_str().unwrap().into(),
        primitive: if owner["nodeType"] == "diffBlock" {
            Primitive::Diff
        } else {
            Primitive::Mermaid
        },
        owner_ref: owner["nativeRef"].as_str().unwrap().into(),
        source_ref: code["valueRef"].as_str().unwrap().into(),
    }
}
async fn member(
    services: &Services,
    workspace: &WorkspaceId,
) -> (Caller, intent_core::PrincipalId) {
    let id = intent_core::PrincipalId::new();
    services
        .store
        .upsert_principal(&intent_core::Principal {
            id: id.clone(),
            github_user_id: None,
            login: Some("artifact-recovery-guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
            identity: None,
        })
        .await
        .unwrap();
    services
        .store
        .add_workspace_member(workspace, &id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    (
        Caller::Wire {
            principal_id: id.clone(),
            host_role: intent_core::HostRole::Guest,
        },
        id,
    )
}

fn connection(
    services: &Services,
    caller: Caller,
    mode: Mode,
    epoch: u8,
) -> (Context, SourceConnection) {
    let root = services.prepared_source_contexts();
    let mut context = root.try_admit(mode).unwrap();
    assert!(context.bind(&caller.principal_id().unwrap().0, [epoch; 16], None));
    context.phase(5);
    let connection = services
        .prepared_source_connection(&context, caller)
        .unwrap();
    (context, connection)
}
fn operation(
    services: &Services,
    binding: &CanonicalSourceBinding,
    expiry: &str,
    nonce: u8,
) -> Operation {
    let descriptor = Descriptor {
        nonce: format!("{nonce:032x}"),
        daemon_incarnation: services.prepared_source_contexts().incarnation().into(),
        workspace_id: binding.scope.workspace_id.clone(),
        binding: Binding {
            scope: binding.scope.clone(),
            snapshot_id: binding.snapshot_id.clone(),
            source_revision: binding.source_revision.clone(),
            primitive: binding.primitive,
            owner_ref: binding.owner_ref.clone(),
            source_ref: binding.source_ref.clone(),
        },
        accept_until: expiry.into(),
    };
    let operation_id = intent_core::note_artifact::canonical::digest(
        &json!({"domain":"note.sourceSession.open.v1","descriptor":descriptor}).to_string(),
    )
    .unwrap();
    Operation {
        descriptor,
        operation_id,
    }
}
async fn deliver(mut delivery: Delivery, _epoch: [u8; 16]) -> Value {
    delivery.authorize().await.unwrap();
    let frame = delivery.frame().unwrap();
    assert!(frame.len() <= 8192);
    let value: Value = serde_json::from_str(&frame).unwrap();
    let mut sink = LocalSink::default();
    delivery.enqueue(&mut sink).unwrap();
    assert_eq!(sink.0.as_deref(), Some(frame.as_str()));
    // Service-only control: no socket exists here. Explicit local sink retirement
    // is modeled; separate real WSS controls must prove transport integration.
    delivery.flushed().unwrap();
    value
}
async fn settled(connection: &SourceConnection, operation: &Operation) -> Control {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let result = connection.close(operation.clone()).unwrap();
            if !matches!(result, Control::Closing { .. }) {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn real_open_duplicate_delivery_and_replacement_cleanup_do_not_rebind() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff title\n+é😀\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 1);
    drop(hold);
    let (context, mut original) = connection(&services, caller.clone(), Mode::Read, 1);
    let (replacement_context, replacement) =
        connection(&services, caller.clone(), Mode::Cleanup, 2);
    assert!(Arc::ptr_eq(&original.registry, &replacement.registry));
    // The workers must bind their admitted Wire caller even when invoked by a daemon task.
    let Open::Pending(open) = intent_core::with_caller(Caller::Daemon, async {
        original.open(op.clone(), json!("actual\"id")).unwrap()
    })
    .await
    else {
        panic!("new owner required")
    };
    assert!(matches!(
        original.open(op.clone(), json!("actual\"id")).unwrap(),
        Open::Existing(Control::AlreadyRegistered { .. })
    ));
    let opened = deliver(open.await.unwrap(), original.epoch()).await;
    assert_eq!(opened["result"]["kind"], "sourceSessionOpened");
    assert_eq!(
        opened["result"]["sourceExpiresAt"],
        op.descriptor.accept_until
    );
    let read:Read=serde_json::from_value(json!({"workspaceId":ws.0,"operationId":op.operation_id,"sequence":0,"request":{"kind":"context","contextRef":binding.owner_ref,"maxItems":1,"maxWireBytes":8192}})).unwrap();
    #[expect(
        clippy::async_yields_async,
        reason = "Admit under the ambient daemon scope, then await outside it to verify the worker's original caller"
    )]
    let pending = intent_core::with_caller(Caller::Daemon, async {
        original.read(read, json!(1)).unwrap()
    })
    .await;
    let page = deliver(pending.await.unwrap(), original.epoch()).await;
    assert_eq!(page["result"]["items"][0]["nodeType"], "diffBlock");
    services
        .store
        .remove_workspace_member(&ws, &id)
        .await
        .unwrap();
    assert!(matches!(
        settled(&replacement, &op).await,
        Control::Settled {
            reason: Reason::Closed,
            ..
        }
    ));
    assert_eq!(services.canonical_source_admission.available_permits(), 256);
    drop(original);
    drop(replacement);
    context.retire();
    replacement_context.retire();
    services.store.close().await;
}
#[tokio::test]
async fn cancelled_before_open_remains_cancelled_and_signed_deadline_is_exact() {
    let (_tmp, services, ws, note) = crate::tests::setup("```mermaid\ngraph TD\n A-->B\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let raw = hold.expires_at().to_string();
    let grant = services
        .store
        .authorize_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .await
        .unwrap();
    assert_eq!(raw, grant.expires_at);
    drop(grant);
    drop(hold);
    let op = operation(&services, &binding, &raw, 2);
    let (context, mut connection) = connection(&services, caller, Mode::Read, 3);
    assert!(matches!(
        connection.close(op.clone()).unwrap(),
        Control::Settled {
            reason: Reason::Cancelled,
            ..
        }
    ));
    assert!(matches!(
        connection.open(op.clone(), json!("actual\"id")).unwrap(),
        Open::Existing(Control::AlreadyRegistered { .. })
    ));
    assert!(matches!(
        connection.close(op).unwrap(),
        Control::Settled {
            reason: Reason::Cancelled,
            ..
        }
    ));
    let late = intent_core::parse_iso(&raw).unwrap() + time::Duration::nanoseconds(1);
    let late = late
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    let over = operation(&services, &binding, &late, 3);
    assert!(matches!(
        connection.open(over, json!(1)),
        Err(SessionError::Expired)
    ));
    assert_eq!(services.canonical_source_admission.available_permits(), 256);
    drop(connection);
    context.retire();
    services.store.close().await;
}
#[tokio::test]
async fn unpolled_open_cancellation_keeps_exact_owner_until_real_settlement() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 4);
    drop(hold);
    let (context, mut connection) = connection(&services, caller, Mode::Read, 4);
    let Open::Pending(wait) = connection.open(op.clone(), json!("actual\"id")).unwrap() else {
        panic!("new owner")
    };
    let owner = connection.owner.as_ref().unwrap().clone();
    drop(wait);
    assert!(owner.closing.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        settled(&connection, &op).await,
        Control::Settled { .. }
    ));
    assert_eq!(services.canonical_source_admission.available_permits(), 256);
    drop(owner);
    drop(connection);
    context.retire();
    services.store.close().await;
}

#[derive(Default)]
struct LocalSink(Option<String>);
impl SourceWriter for LocalSink {
    fn start_send(&mut self, frame: String) -> std::result::Result<(), ()> {
        assert!(self.0.is_none());
        self.0 = Some(frame);
        Ok(())
    }
}

#[tokio::test]
async fn actual_context_claim_is_unique_parallel_and_never_reset_by_wrapper_drop() {
    let (_tmp, services, ws, _note) = crate::tests::setup("plain").await;
    let (caller, _) = member(&services, &ws).await;
    let root = services.prepared_source_contexts();
    let mut context = root.try_admit(Mode::Read).unwrap();
    assert!(context.bind(&caller.principal_id().unwrap().0, [42; 16], None));
    context.phase(5);
    let mut wrong = caller.clone();
    if let Caller::Wire { principal_id, .. } = &mut wrong {
        *principal_id = intent_core::PrincipalId::new();
    }
    assert!(services
        .prepared_source_connection(&context, wrong)
        .is_err());
    let barrier = std::sync::Barrier::new(2);
    let claims = std::thread::scope(|threads| {
        let first = threads.spawn(|| {
            barrier.wait();
            services.prepared_source_connection(&context, caller.clone())
        });
        let second = threads.spawn(|| {
            barrier.wait();
            services.prepared_source_connection(&context, caller.clone())
        });
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(claims.iter().filter(|v| v.is_ok()).count(), 1);
    drop(claims);
    assert!(services
        .prepared_source_connection(&context, caller.clone())
        .is_err());
    assert_eq!(root.counts().read, 1);
    context.retire();
    let mut next = root.try_admit(Mode::Read).unwrap();
    assert!(next.bind(&caller.principal_id().unwrap().0, [42; 16], None));
    next.phase(5);
    assert!(services.prepared_source_connection(&next, caller).is_ok());
    next.retire();
    services.store.close().await;
}

#[tokio::test]
async fn actual_id_and_context_invalidation_survive_completed_authorization() {
    for invalidation in [0, 1, 2] {
        let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        let (caller, id) = member(&services, &ws).await;
        let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
        let hold = services
            .store
            .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
            .unwrap();
        let op = operation(&services, &binding, hold.expires_at(), 8 + invalidation);
        drop(hold);
        let (context, mut connection) = connection(&services, caller.clone(), Mode::Read, 9);
        let id = json!("\0".repeat(64));
        let Open::Pending(wait) = connection.open(op.clone(), id.clone()).unwrap() else {
            panic!("new owner")
        };
        let mut delivery = wait.await.unwrap();
        delivery.authorize().await.unwrap();
        let frame: Value = serde_json::from_str(&delivery.frame().unwrap()).unwrap();
        assert_eq!(frame["id"], id);
        // No replacement-ID argument exists on frame/enqueue; the read/open ID
        // was validated and captured before the actual Store operation began.
        let mut context = Some(context);
        match invalidation {
            0 => context.as_mut().unwrap().phase(6),
            1 => drop(context.take()),
            _ => context.take().unwrap().retire(),
        }
        let mut sink = LocalSink::default();
        assert!(delivery.enqueue(&mut sink).is_err());
        assert!(sink.0.is_none());
        delivery.discard().unwrap();
        let (cleanup_context, cleanup) = connection_for_cleanup(&services, caller);
        assert!(matches!(
            settled(&cleanup, &op).await,
            Control::Settled { .. }
        ));
        drop(connection);
        drop(cleanup);
        cleanup_context.retire();
        if let Some(context) = context {
            context.retire();
        }
        services.store.close().await;
    }
}
fn connection_for_cleanup(services: &Services, caller: Caller) -> (Context, SourceConnection) {
    connection(services, caller, Mode::Cleanup, 99)
}

#[tokio::test]
async fn replacement_close_before_enqueue_suppresses_the_admitted_frame() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 12);
    drop(hold);
    let (context, mut original) = connection(&services, caller.clone(), Mode::Read, 12);
    let (cleanup_context, cleanup) = connection_for_cleanup(&services, caller);
    let Open::Pending(wait) = original.open(op.clone(), json!(1)).unwrap() else {
        panic!("new owner")
    };
    let mut delivery = wait.await.unwrap();
    delivery.authorize().await.unwrap();
    assert!(matches!(
        cleanup.close(op.clone()).unwrap(),
        Control::Closing { .. }
    ));
    let mut sink = LocalSink::default();
    assert!(delivery.enqueue(&mut sink).is_err());
    assert!(sink.0.is_none());
    delivery.discard().unwrap();
    assert!(matches!(
        settled(&cleanup, &op).await,
        Control::Settled { .. }
    ));
    drop(original);
    drop(cleanup);
    context.retire();
    cleanup_context.retire();
    services.store.close().await;
}

#[tokio::test]
async fn entered_enqueue_serializes_replacement_close_and_retains_write_until_flush() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 14);
    drop(hold);
    let (context, mut original) = connection(&services, caller.clone(), Mode::Read, 14);
    let (cleanup_context, cleanup) = connection_for_cleanup(&services, caller);
    let Open::Pending(wait) = original.open(op.clone(), json!(1)).unwrap() else {
        panic!("new owner")
    };
    let mut delivery = wait.await.unwrap();
    delivery.authorize().await.unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(1);
    *original.registry.close_blocked.lock().unwrap() = Some(blocked_tx);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let runtime = tokio::runtime::Handle::current();
    let (delivery, closed) = std::thread::scope(|threads| {
        let writer = threads.spawn(move || {
            let mut sink = HeldSink {
                entered: entered_tx,
                release: release_rx,
                frame: None,
            };
            delivery.enqueue(&mut sink).unwrap();
            assert!(sink.frame.is_some());
            delivery
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let closer = threads.spawn(|| {
            let _entered = runtime.enter();
            let result = cleanup.close(op.clone()).unwrap();
            done_tx.send(()).unwrap();
            result
        });
        blocked_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("actual first Context authority mutex WouldBlock");
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        (writer.join().unwrap(), closer.join().unwrap())
    });
    assert!(matches!(closed, Control::Closing { .. }));
    assert_eq!(services.canonical_source_admission.available_permits(), 255);
    // Local sink acceptance alone has not retired the frame/write owner.
    assert!(matches!(
        cleanup.close(op.clone()).unwrap(),
        Control::Closing { .. }
    ));
    delivery.flushed().unwrap();
    assert!(matches!(
        settled(&cleanup, &op).await,
        Control::Settled { .. }
    ));
    drop(original);
    drop(cleanup);
    context.retire();
    cleanup_context.retire();
    services.store.close().await;
}
#[tokio::test]
async fn entered_enqueue_serializes_actual_context_phase_barrier() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 15);
    drop(hold);
    let (mut context, mut original) = connection(&services, caller.clone(), Mode::Read, 15);
    let (cleanup_context, cleanup) = connection_for_cleanup(&services, caller);
    let Open::Pending(wait) = original.open(op.clone(), json!(1)).unwrap() else {
        panic!("new owner")
    };
    let mut delivery = wait.await.unwrap();
    delivery.authorize().await.unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    context.observe_phase_lock(blocked_tx);
    let delivery = std::thread::scope(|threads| {
        let writer = threads.spawn(move || {
            let mut sink = HeldSink {
                entered: entered_tx,
                release: release_rx,
                frame: None,
            };
            delivery.enqueue(&mut sink).unwrap();
            assert!(sink.frame.is_some());
            delivery
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let invalidator = threads.spawn(|| {
            context.phase(6);
            done_tx.send(()).unwrap();
        });
        blocked_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("actual context mutex WouldBlock");
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        let delivery = writer.join().unwrap();
        invalidator.join().unwrap();
        delivery
    });
    assert_eq!(services.canonical_source_admission.available_permits(), 255);
    assert!(matches!(
        cleanup.close(op.clone()).unwrap(),
        Control::Closing { .. }
    ));
    delivery.flushed().unwrap();
    assert!(matches!(
        settled(&cleanup, &op).await,
        Control::Settled { .. }
    ));
    drop(original);
    drop(cleanup);
    context.retire();
    cleanup_context.retire();
    services.store.close().await;
}

struct HeldSink {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
    frame: Option<String>,
}
impl SourceWriter for HeldSink {
    fn start_send(&mut self, frame: String) -> std::result::Result<(), ()> {
        // Deliberate TEST barrier inside the local sink to order the competing
        // close. Production adapter must never block or reenter Services here.
        self.entered.send(()).unwrap();
        self.release
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("explicit release, no fallback acceptance");
        self.frame = Some(frame);
        Ok(())
    }
}

#[tokio::test]
async fn terminal_receipt_waits_for_original_transport_retirement() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 16);
    drop(hold);
    let (context, mut original) = connection(&services, caller.clone(), Mode::Read, 16);
    original.attach_transport().unwrap();
    let Open::Pending(wait) = original.open(op.clone(), json!(1)).unwrap() else {
        panic!("new owner")
    };
    deliver(wait.await.unwrap(), original.epoch()).await;
    let owner = original.owner.as_ref().unwrap().clone();
    owner
        .registry
        .close_state(owner.slot, Reason::Closed)
        .unwrap();
    // Retire actual source work/consumer, while explicitly retaining the attached
    // transport owner. Direct private publication probe must still refuse.
    let (delivery_hold, session) = {
        let mut state = owner.state.lock().unwrap();
        (state.delivered.take(), state.session.clone().unwrap())
    };
    if let Some(delivery_hold) = delivery_hold {
        delivery_hold.retire().unwrap();
        owner.registry.consume(owner.slot).unwrap();
    }
    session.close().await.unwrap();
    assert!(
        owner.registry.settle(owner.slot).is_err(),
        "unretired original transport must bar receipt publication"
    );
    assert!(matches!(
        original.close(op.clone()).unwrap(),
        Control::Closing { .. }
    ));
    original.transport_retired().unwrap();
    assert!(original.close(op.clone()).is_err());
    let (cleanup_context, cleanup) = connection(&services, caller, Mode::Cleanup, 17);
    assert!(matches!(
        settled(&cleanup, &op).await,
        Control::Settled { .. }
    ));
    drop(cleanup);
    cleanup_context.retire();
    drop(owner);
    drop(original);
    context.retire();
    services.store.close().await;
}

#[tokio::test]
async fn lost_original_transport_preserves_uncertain_history() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let (caller, id) = member(&services, &ws).await;
    let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
    let hold = services
        .store
        .hold_canonical_source(&ws.0, &format!("principal:{}", id.0), &binding)
        .unwrap();
    let op = operation(&services, &binding, hold.expires_at(), 18);
    drop(hold);
    let (context, mut original) = connection(&services, caller.clone(), Mode::Read, 18);
    original.attach_transport().unwrap();
    assert!(original.attach_transport().is_err());
    let Open::Pending(wait) = original.open(op.clone(), json!(1)).unwrap() else {
        panic!("new owner")
    };
    deliver(wait.await.unwrap(), original.epoch()).await;
    let slot = original.owner.as_ref().unwrap().slot;
    let registry = original.registry.clone();
    drop(original);
    let (cleanup_context, cleanup) = connection(&services, caller, Mode::Cleanup, 19);
    assert!(matches!(
        cleanup.close(op).unwrap(),
        Control::Uncertain { .. }
    ));
    assert!(registry.settle(slot).is_err());
    {
        let directory = registry.state.lock().unwrap();
        let entry = directory.get(slot).unwrap();
        assert!(entry.transport_pending);
        assert!(entry.hold.is_some());
        assert!(entry.receipt.payload().is_err());
    }
    drop(cleanup);
    context.retire();
    cleanup_context.retire();
    services.store.close().await;
}
