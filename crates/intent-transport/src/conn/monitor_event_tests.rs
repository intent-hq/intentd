//! Real Store/Services/hello/request/forwarder path, with the final writer held.
use super::*;
use crate::context::{with_credential_context, ReadConnectionGuard, RetirementForwarder};
use crate::reverse::{PrimaryReverseRegistry, ReverseTransport};
use intent_core::repository_request::RepositoryWireEntry;
use intent_core::{now_iso, AgentId, Caller, HostRole, PrMonitor, PrMonitorId, PrMonitorState};
use intent_services::Services;
use intent_store::Store;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::AsyncWrite;

struct Document {
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    caller: Caller,
    connection: ReadConnectionGuard,
    _retirements: RetirementForwarder,
    tx: OutboundSender,
    rx: OutboundReceiver,
    reverse: ReverseChannel,
    guard: PrimaryReverseGuard,
    subs: ConnSubs,
    client: Option<ClientId>,
    id: u64,
}
impl Document {
    async fn new(
        services: Arc<Services>,
        bus: EventBus,
        registry: &Arc<PrimaryReverseRegistry>,
        caller: Caller,
    ) -> Self {
        let api: Arc<dyn WorkspaceApi> = services;
        let connection = with_credential_context(false, Some(caller.clone()), None, async {
            ReadConnectionGuard::bind(api.as_ref(), RepositoryWireEntry::AdmittedLocal)
        })
        .await;
        let (tx, rx) = outbound_channel();
        let retirements = connection.forward_retirements(tx.bulk_sender());
        let reverse = ReverseChannel::new(tx.priority_sender());
        let guard = registry.register(reverse.clone(), ReverseTransport::Uds);
        Self {
            api,
            bus,
            caller,
            connection,
            _retirements: retirements,
            tx,
            rx,
            reverse,
            guard,
            subs: ConnSubs::default(),
            client: None,
            id: 0,
        }
    }
    fn rpc<'a>(&'a mut self, method: &'a str, params: Value) -> intent_core::BoxFuture<'a, Value> {
        Box::pin(async move {
            self.id += 1;
            let raw =
                json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params}).to_string();
            let limiter = RpcLimiter::new(4);
            assert!(
                with_credential_context(
                    false,
                    Some(self.caller.clone()),
                    None,
                    self.connection.run(async {
                        process_frame(
                            &raw,
                            &self.api,
                            &self.bus,
                            &self.tx,
                            &mut self.subs,
                            &self.reverse,
                            &self.guard,
                            None,
                            None,
                            &mut self.client,
                            true,
                            &limiter,
                        )
                        .await
                    })
                )
                .await
            );
            let frame = tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_secs(10)),
                self.rx.recv_priority(),
            )
            .await
            .unwrap()
            .unwrap();
            let mut bytes = Vec::new();
            frame.write_uds(&mut bytes).await.unwrap();
            let response: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(response.get("error").is_none(), "{response}");
            response["result"].clone()
        })
    }
    async fn subscribe(&mut self, workspace: &intent_core::WorkspaceId) -> String {
        self.rpc(
            "events.subscribe",
            json!({"workspaceId":workspace,"eventTypes":["prMonitor:*","client:updated"],"replaceGroup":"monitor-document"}),
        )
        .await["subscriptionId"]
            .as_str()
            .unwrap()
            .into()
    }
    async fn event(&mut self) -> crate::OutboundFrame {
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            self.rx.bulk.recv(),
        )
        .await
        .unwrap()
        .unwrap()
    }
}
impl Drop for Document {
    fn drop(&mut self) {
        self.connection.retire();
    }
}

struct HeldWriter {
    released: Arc<AtomicBool>,
    bytes: Vec<u8>,
}
impl AsyncWrite for HeldWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.released.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        self.bytes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn check_retirement(action: &str, wss: bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("events.db")).await.unwrap();
    let mut workspace = intent_core::chief_workspace();
    workspace.id = intent_core::WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let session = serde_json::from_value(json!({"id":agent,"workspaceId":workspace.id,"name":"monitor owner","status":"idle","createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
    store.insert_agent_session(&session).await.unwrap();
    let row = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: workspace.id.clone(),
        agent_id: agent,
        repo_owner: "original".into(),
        repo_name: "unknown".into(),
        pr_number: 42,
        state: PrMonitorState::Active,
        last_snapshot: None,
        baseline_snapshot: None,
        pending_changes: vec!["private old observation".into()],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    assert!(store.insert_pr_monitor(&row).await.unwrap());
    let bus = EventBus::new(store.clone());
    let services = Arc::new(Services::new(store.clone()).with_event_bus(bus.clone()));
    services.initialize_repository_wire().await.unwrap();
    let caller = Caller::Wire {
        principal_id: store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    };
    let registry = Arc::new(PrimaryReverseRegistry::new());
    let mut ordinary =
        Document::new(services.clone(), bus.clone(), &registry, caller.clone()).await;
    let hello = ordinary
        .rpc("client.hello", json!({"clientId":"monitor-source"}))
        .await;
    let mut document =
        Document::new(services.clone(), bus.clone(), &registry, caller.clone()).await;
    let mut sibling = Document::new(services, bus.clone(), &registry, caller).await;
    for doc in [&mut document, &mut sibling] {
        let response = doc.rpc("client.hello", json!({"clientId":hello["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await;
        assert_eq!(response["connectionRole"], "review-monitor");
    }
    let id = document.subscribe(&workspace.id).await;
    let sibling_id = sibling.subscribe(&workspace.id).await;
    ordinary.subscribe(&workspace.id).await;
    sibling
        .rpc(
            "prMonitor.cancel",
            json!({"workspaceId":workspace.id,"monitorId":row.monitor_id}),
        )
        .await;
    let frame = document.event().await;
    assert!(frame.is_private());
    let sibling_frame = sibling.event().await;
    assert!(sibling_frame.is_private());
    // Poll the actual final writer into Pending, then acknowledge unsubscribe
    // without giving an aborted forwarder a scheduling turn to drop its owner.
    let released = Arc::new(AtomicBool::new(false));
    let mut writer = HeldWriter {
        released: released.clone(),
        bytes: Vec::new(),
    };
    let pending = document.tx.recovery.prepare_frame(&frame);
    let mut write: Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> = Box::pin(async {
        pending
            .finish(async {
                if wss {
                    frame.write_wss(&mut writer).await.is_ok()
                } else {
                    frame.write_uds(&mut writer).await.is_ok()
                }
            })
            .await
            .then_some(())
            .ok_or(())
    });
    assert!(futures::poll!(write.as_mut()).is_pending());
    match action {
        "unsubscribe" => {
            let response = document
                .rpc("events.unsubscribe", json!({"subscriptionId":id}))
                .await;
            assert_eq!(response["success"], true);
        }
        "replace" => {
            assert_ne!(document.subscribe(&workspace.id).await, id);
        }
        "recovery" => document.tx.recovery.close(),
        "document" => {
            // The actual transport exit retires its read owner and registry.
            document.connection.retire();
            drop(std::mem::take(&mut document.subs));
        }
        _ => unreachable!(),
    }
    released.store(true, Ordering::Release);
    assert_eq!(
        futures::poll!(write.as_mut()),
        Poll::Ready(Err(())),
        "unsubscribe reply must join queued private output retirement"
    );
    drop(write);
    assert!(writer.bytes.is_empty());
    let mut live_writer = HeldWriter {
        released: Arc::new(AtomicBool::new(true)),
        bytes: Vec::new(),
    };
    assert!(
        sibling
            .tx
            .recovery
            .prepare_frame(&sibling_frame)
            .finish(async {
                if wss {
                    sibling_frame.write_wss(&mut live_writer).await.is_ok()
                } else {
                    sibling_frame.write_uds(&mut live_writer).await.is_ok()
                }
            })
            .await
    );
    let live: Value = serde_json::from_slice(&live_writer.bytes).unwrap();
    assert_eq!(live["params"]["subscriptionId"], sibling_id);
    assert_eq!(live["params"]["event"]["type"], "prMonitor:cancelled");
    assert!(
        ordinary.rx.bulk.try_recv().is_err(),
        "legacy withheld qualified envelope"
    );
    bus.publish(&intent_store::NewEvent {
        workspace_id: workspace.id,
        timestamp: now_iso(),
        event_type: intent_core::events::CLIENT_UPDATED.into(),
        actor: intent_core::EventActor {
            actor_type: intent_core::ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"clientId":"ordinary"}),
    })
    .await
    .unwrap();
    assert!(!ordinary.event().await.is_private());
    assert!(!sibling.event().await.is_private());
}

impl futures::Sink<tokio_tungstenite::tungstenite::Message> for HeldWriter {
    type Error = std::io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.released.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(
        mut self: Pin<&mut Self>,
        message: tokio_tungstenite::tungstenite::Message,
    ) -> std::io::Result<()> {
        let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
            panic!("expected event text")
        };
        self.bytes.extend_from_slice(text.as_bytes());
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn native_monitor_unsubscribe_retires_queued_event_before_reply() {
    for wss in [false, true] {
        Box::pin(check_retirement("unsubscribe", wss)).await;
    }
}
#[tokio::test]
async fn native_monitor_replacement_retires_queued_event_before_reply() {
    for wss in [false, true] {
        Box::pin(check_retirement("replace", wss)).await;
    }
}
#[tokio::test]
async fn native_monitor_document_retirement_fences_queued_event() {
    for wss in [false, true] {
        Box::pin(check_retirement("document", wss)).await;
    }
}

// Actual subscription/forwarder/final-write lifetime proof. These ordinary
// messages do not stand in for provider-authorized private history evidence.
async fn check_chat_retirement(action: &str, wss: bool, progressive: bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("chat.db")).await.unwrap();
    let mut workspace = intent_core::chief_workspace();
    workspace.id = intent_core::WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let session = serde_json::from_value(json!({"id":agent,"workspaceId":workspace.id,"name":"history owner","status":"idle","createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
    store.insert_agent_session(&session).await.unwrap();
    let message = store
        .append_agent_message(
            &agent,
            "user",
            &json!([{ "type":"text", "text":"ordinary history control" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let services = Arc::new(Services::new(store.clone()).with_event_bus(bus.clone()));
    services.initialize_repository_wire().await.unwrap();
    let caller = Caller::Wire {
        principal_id: store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    };
    let registry = Arc::new(PrimaryReverseRegistry::new());
    let mut ordinary =
        Document::new(services.clone(), bus.clone(), &registry, caller.clone()).await;
    let hello = ordinary
        .rpc("client.hello", json!({"clientId":"history-source"}))
        .await;
    let mut document =
        Document::new(services.clone(), bus.clone(), &registry, caller.clone()).await;
    let mut sibling = Document::new(services, bus, &registry, caller).await;
    for doc in [&mut document, &mut sibling] {
        doc.rpc("client.hello", json!({"clientId":hello["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await;
    }
    let params = json!({"agentId":agent,"replaceGroup":"history-document","historyDelivery":if progressive {"progressive"} else {"snapshot"}});
    let id = document.rpc("chat.subscribe", params.clone()).await["subscriptionId"].clone();
    let sibling_id = sibling.rpc("chat.subscribe", params.clone()).await["subscriptionId"].clone();
    ordinary.rpc("chat.subscribe", params.clone()).await;
    let frame = document.event().await;
    assert!(
        frame.is_private(),
        "supported chat must retain its original history recipient"
    );
    let sibling_frame = sibling.event().await;
    assert!(sibling_frame.is_private());
    let released = Arc::new(AtomicBool::new(false));
    let mut writer = HeldWriter {
        released: released.clone(),
        bytes: Vec::new(),
    };
    let pending = document.tx.recovery.prepare_frame(&frame);
    let mut write: Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> = Box::pin(async {
        pending
            .finish(async {
                if wss {
                    frame.write_wss(&mut writer).await.is_ok()
                } else {
                    frame.write_uds(&mut writer).await.is_ok()
                }
            })
            .await
            .then_some(())
            .ok_or(())
    });
    assert!(futures::poll!(write.as_mut()).is_pending());
    match action {
        "unsubscribe" => {
            assert_eq!(
                document
                    .rpc("chat.unsubscribe", json!({"subscriptionId":id}))
                    .await["success"],
                true
            );
        }
        "replace" => {
            assert_ne!(
                document.rpc("chat.subscribe", params).await["subscriptionId"],
                id
            );
        }
        "document" => {
            document.connection.retire();
            drop(std::mem::take(&mut document.subs));
        }
        _ => unreachable!(),
    }
    released.store(true, Ordering::Release);
    assert_eq!(
        futures::poll!(write.as_mut()),
        Poll::Ready(Err(())),
        "chat retirement must join final private write before reply"
    );
    drop(write);
    assert!(writer.bytes.is_empty());
    let mut live_writer = HeldWriter {
        released: Arc::new(AtomicBool::new(true)),
        bytes: Vec::new(),
    };
    if wss {
        sibling_frame.write_wss(&mut live_writer).await.unwrap();
    } else {
        sibling_frame.write_uds(&mut live_writer).await.unwrap();
    }
    let live: Value = serde_json::from_slice(&live_writer.bytes).unwrap();
    assert_eq!(live["method"], "subscription.push");
    assert_eq!(live["params"]["subscriptionId"], sibling_id);
    assert!(live.to_string().contains(&message.id));
    let legacy_frame = ordinary.event().await;
    assert!(!legacy_frame.is_private());
    let mut legacy_bytes = Vec::new();
    legacy_frame.write_uds(&mut legacy_bytes).await.unwrap();
    let legacy: Value = serde_json::from_slice(&legacy_bytes).unwrap();
    assert!(legacy.to_string().contains(&message.id));
}

#[tokio::test]
async fn native_monitor_chat_unsubscribe_retires_queued_history_before_reply() {
    for wss in [false, true] {
        for progressive in [false, true] {
            Box::pin(check_chat_retirement("unsubscribe", wss, progressive)).await;
        }
    }
}
#[tokio::test]
async fn native_monitor_chat_replacement_retires_queued_history_before_reply() {
    for wss in [false, true] {
        Box::pin(check_chat_retirement("replace", wss, false)).await;
    }
}
#[tokio::test]
async fn native_monitor_chat_document_retirement_fences_queued_history() {
    for wss in [false, true] {
        Box::pin(check_chat_retirement("document", wss, false)).await;
    }
}

#[tokio::test]
async fn recovery_monitor_output_cancellation_keeps_sibling_authority_isolated() {
    for wss in [false, true] {
        check_retirement("recovery", wss).await;
    }
}
