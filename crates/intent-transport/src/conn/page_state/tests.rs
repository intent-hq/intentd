use super::*;
use intent_core::{ActorType, BoxFuture, Caller, Error, EventActor, Result};
use intent_store::NewEvent;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};

fn state(generation: u64, deleted: bool) -> Value {
    json!({"kind":"notePageState","scope":{"backendId":"db","workspaceId":"ws","noteId":"note","noteInstanceId":"original"},"stateGeneration":generation.to_string(),"sourceRevision":"r:1","attributionGeneration":"a:1","attributionState":"ready","commentRevision":"c:1","deleted":deleted,"invalidation":"all"})
}

struct StateApi {
    current: Mutex<Value>,
    reads: AtomicUsize,
    denied: AtomicBool,
    incarnations: Mutex<Vec<Option<String>>>,
}

impl WorkspaceApi for StateApi {
    fn get_note_page_state(
        &self,
        workspace: WorkspaceId,
        note: NoteId,
        incarnation: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            assert_eq!(workspace.as_str(), "ws");
            assert_eq!(note.as_str(), "note");
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.incarnations.lock().unwrap().push(incarnation);
            if self.denied.load(Ordering::SeqCst) {
                return Err(Error::NotFound("workspace".into()));
            }
            Ok(self.current.lock().unwrap().clone())
        })
    }

    fn list_notes<'a>(
        &'a self,
        _: &'a WorkspaceId,
    ) -> BoxFuture<'a, Result<Vec<intent_core::Note>>> {
        Box::pin(async { panic!("pageState must never hydrate the legacy note collection") })
    }
}

fn event() -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from("ws"),
        timestamp: intent_core::now_iso(),
        event_type: NOTE_UPDATED.into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"noteId":"note"}),
    }
}

#[tokio::test]
async fn page_state_backpressure_reads_latest_tuple_and_retires_original_incarnation() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&directory.path().join("state.db"))
            .await
            .unwrap();
        let bus = EventBus::new(store.clone());
        let concrete = Arc::new(StateApi {
            current: Mutex::new(state(1, false)),
            reads: AtomicUsize::new(0),
            denied: AtomicBool::new(false),
            incarnations: Mutex::new(Vec::new()),
        });
        let api: Arc<dyn WorkspaceApi> = concrete.clone();
        let (sender, mut receiver) = super::super::outbound_channel();
        for _ in 0..super::super::BULK_CAPACITY {
            sender.bulk_sender().try_send("occupied".into()).unwrap();
        }
        let mut registry = ConnSubs::default();
        assert!(
            crate::context::with_caller(
                Caller::Daemon,
                subscribe(
                    events::IdInfo {
                        present: true,
                        echo: json!(1)
                    },
                    Channel::Note,
                    PageStateSubscription {
                        workspace_id: "ws".into(),
                        note_id: "note".into(),
                        replace_group: None
                    },
                    &api,
                    &bus,
                    &sender,
                    &mut registry,
                )
            )
            .await
        );
        let ack: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
        let id = ack["result"]["subscriptionId"].as_str().unwrap().to_owned();
        // Each publication completes while the bulk lane is still full.
        // None may cause a state read or accumulate serialized snapshots.
        for generation in 2..=30 {
            *concrete.current.lock().unwrap() = state(generation, false);
            bus.publish(&event()).await.unwrap();
        }
        assert_eq!(concrete.reads.load(Ordering::SeqCst), 1);
        for _ in 0..super::super::BULK_CAPACITY {
            assert_eq!(receiver.recv().await.unwrap(), "occupied");
        }
        let frame = receiver.recv().await.unwrap();
        assert!(frame.len() <= 4096);
        let first: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(first["params"]["seq"], 0);
        assert_eq!(first["params"]["snapshot"], state(30, false));
        *concrete.current.lock().unwrap() = state(31, true);
        bus.publish(&event()).await.unwrap();
        let deleted: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(deleted["params"]["seq"], 1);
        assert_eq!(deleted["params"]["snapshot"], state(31, true));
        let subscription = registry.subs.remove(&id).unwrap();
        // Observe the actual forwarder return, rather than aborting to imply it.
        while !subscription.handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let scopes = concrete.incarnations.lock().unwrap().clone();
        assert_eq!(scopes[0], None);
        assert!(scopes[1..]
            .iter()
            .all(|scope| scope.as_deref() == Some("original")));
        drop(subscription);
        bus.shutdown().await.unwrap();
        store.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn page_state_refused_admission_has_no_registered_subscription() {
    struct Denied;
    impl WorkspaceApi for Denied {
        fn get_note_page_state(
            &self,
            _: WorkspaceId,
            _: NoteId,
            _: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async { Err(Error::NotFound("workspace".into())) })
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&directory.path().join("state.db"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(Denied);
    let (sender, mut receiver) = super::super::outbound_channel();
    let mut registry = ConnSubs::default();
    assert!(
        crate::context::with_caller(
            Caller::Daemon,
            subscribe(
                events::IdInfo {
                    present: true,
                    echo: json!(1)
                },
                Channel::Comment,
                PageStateSubscription {
                    workspace_id: "ws".into(),
                    note_id: "note".into(),
                    replace_group: None
                },
                &api,
                &bus,
                &sender,
                &mut registry,
            )
        )
        .await
    );
    let error: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
    assert!(error.get("error").is_some());
    assert!(registry.subs.is_empty());
    bus.shutdown().await.unwrap();
    store.close().await;
}

#[tokio::test]
async fn page_state_actual_broadcast_lag_refreshes_without_matching_retained_events() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&directory.path().join("lag.db"))
            .await
            .unwrap();
        let bus = EventBus::new(store.clone());
        let concrete = Arc::new(StateApi {
            current: Mutex::new(state(1, false)),
            reads: AtomicUsize::new(0),
            incarnations: Mutex::new(Vec::new()),
            denied: AtomicBool::new(false),
        });
        let api: Arc<dyn WorkspaceApi> = concrete.clone();
        let (sender, mut receiver) = super::super::outbound_channel();
        let mut registry = ConnSubs::default();
        let id = start(&api, &bus, &sender, &mut receiver, &mut registry).await;
        let first: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(first["params"]["snapshot"], state(1, false));
        // Current-thread execution, no await: both subscribed broadcast receivers
        // necessarily lag beyond the actual 1024-slot ring. The only matching
        // event is overwritten; every retained event is deliberately irrelevant.
        *concrete.current.lock().unwrap() = state(2, false);
        let _ = bus.publish_transient(&event());
        let mut irrelevant = event();
        irrelevant.event_type = "unrelated:event".into();
        for _ in 0..4096 {
            let _ = bus.publish_transient(&irrelevant);
        }
        let next: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(next["params"]["snapshot"], state(2, false));
        assert_eq!(next["params"]["seq"], 1);
        let abort = registry.subs[&id].handle.abort_handle();
        assert!(registry.remove(&api, &id).await);
        while !abort.is_finished() {
            tokio::task::yield_now().await;
        }
        bus.shutdown().await.unwrap();
        store.close().await;
    })
    .await
    .unwrap();
}

async fn start(
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    sender: &OutboundSender,
    receiver: &mut super::super::OutboundReceiver,
    registry: &mut ConnSubs,
) -> String {
    assert!(
        crate::context::with_caller(
            Caller::Wire {
                principal_id: intent_core::PrincipalId::from("guest"),
                host_role: intent_core::HostRole::Guest
            },
            subscribe(
                events::IdInfo {
                    present: true,
                    echo: json!(1)
                },
                Channel::Note,
                PageStateSubscription {
                    workspace_id: "ws".into(),
                    note_id: "note".into(),
                    replace_group: None
                },
                api,
                bus,
                sender,
                registry,
            )
        )
        .await
    );
    let ack: Value = serde_json::from_str(&receiver.recv().await.unwrap()).unwrap();
    ack["result"]["subscriptionId"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn page_state_full_bulk_revocation_and_unsubscribe_settle_forwarder() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&directory.path().join("retire.db"))
            .await
            .unwrap();
        let bus = EventBus::new(store.clone());
        for revoke in [true, false] {
            let concrete = Arc::new(StateApi {
                current: Mutex::new(state(1, false)),
                reads: AtomicUsize::new(0),
                incarnations: Mutex::new(Vec::new()),
                denied: AtomicBool::new(false),
            });
            let api: Arc<dyn WorkspaceApi> = concrete.clone();
            let (sender, mut receiver) = super::super::outbound_channel();
            for _ in 0..super::super::BULK_CAPACITY {
                sender.bulk_sender().try_send("occupied".into()).unwrap();
            }
            let mut registry = ConnSubs::default();
            let id = start(&api, &bus, &sender, &mut receiver, &mut registry).await;
            let abort = registry.subs[&id].handle.abort_handle();
            assert_eq!(concrete.reads.load(Ordering::SeqCst), 1);
            if revoke {
                concrete.denied.store(true, Ordering::SeqCst);
                let mut unshare = event();
                unshare.event_type = WORKSPACE_UPDATED.into();
                unshare.data = json!({"changeType":"unshared","principalId":"guest"});
                bus.publish(&unshare).await.unwrap();
            } else {
                assert!(registry.remove(&api, &id).await);
            }
            // Nothing is drained to make room. Retirement must proceed solely
            // from authoritative revocation or explicit subscription cancellation.
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
            if revoke {
                assert!(concrete.reads.load(Ordering::SeqCst) >= 2);
                assert!(registry.remove(&api, &id).await);
            }
            for _ in 0..super::super::BULK_CAPACITY {
                assert_eq!(receiver.recv().await.unwrap(), "occupied");
            }
            assert_eq!(sender.bulk_sender().capacity(), super::super::BULK_CAPACITY);
        }
        bus.shutdown().await.unwrap();
        store.close().await;
    })
    .await
    .unwrap();
}
