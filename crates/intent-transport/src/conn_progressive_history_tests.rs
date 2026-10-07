//! Controlled history reads prove cancellation and generation invalidation.
use super::*;
use intent_core::{ActorType, BoxFuture, EventActor};
use intent_store::{NewEvent, Store};
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

struct HeldHistory {
    entered: Notify,
    release: Notify,
}
impl WorkspaceApi for HeldHistory {
    fn agent_get(
        &self,
        _: AgentId,
        _: Option<WorkspaceId>,
    ) -> BoxFuture<'_, intent_core::Result<intent_core::AgentLite>> {
        Box::pin(async {
            Ok(serde_json::from_value(json!({"id":"a","workspaceId":"w","name":"Agent","status":"idle","createdAt":intent_core::now_iso(),"updatedAt":intent_core::now_iso(),"messageCount":3,"metadata":{"isBackground":false}})).unwrap())
        })
    }
    fn agent_history_batch(
        &self,
        _: AgentId,
        before: Option<i64>,
        _: usize,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            if before.is_some() {
                self.entered.notify_one();
                self.release.notified().await;
                return Ok(
                    json!({"messages":[{"id":"old-1","seq":1,"contentBlocks":[]}],"totalMessages":3}),
                );
            }
            Ok(json!({"messages":[{"id":"old-2","seq":2,"contentBlocks":[]}],"totalMessages":3}))
        })
    }
    fn agent_get_conversation(
        &self,
        _: AgentId,
        _: Option<i64>,
        _: Option<WorkspaceId>,
        _: Option<String>,
        _: Option<String>,
        _: Option<i64>,
        _: Option<intent_core::ConversationProjection>,
        _: bool,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async {
            Ok(
                json!({"agentId":"a","messages":[{"id":"replacement","seq":0,"contentBlocks":[]}],"totalMessages":1,"truncated":false,"nextToken":null}),
            )
        })
    }
}

#[tokio::test]
async fn progressive_history_invalidation_cancels_held_read_and_completes_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let api = Arc::new(HeldHistory {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let (tx, mut rx) = outbound_channel();
    let handle = tokio::spawn(chat_subscription_loop(
        api.clone(),
        AgentId::from("a"),
        None,
        subscriptions::DeltaEncoding::Incremental,
        Some(intent_core::ConversationProjection::Slim),
        sub,
        None,
        "s".into(),
        tx,
        subscriptions::SnapshotTimer::start(Channel::Chat, "a"),
        3,
        true,
    ));
    let first: Value = serde_json::from_str(
        &timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        first["params"]["snapshot"]["initialHistory"]["complete"],
        false
    );
    timeout(Duration::from_secs(10), api.entered.notified())
        .await
        .unwrap();
    let _ = bus.publish_transient(&NewEvent {
        workspace_id: WorkspaceId::from("w"),
        timestamp: intent_core::now_iso(),
        event_type: AGENT_UPDATED.into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: Some("a".into()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"replacedCount":3}),
    });
    let recovery: Value = serde_json::from_str(
        &timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(recovery["params"]["seq"], 1);
    assert_eq!(recovery["params"]["kind"], "snapshot");
    assert_eq!(
        recovery["params"]["snapshot"]["messages"][0]["id"],
        "replacement"
    );
    assert_eq!(
        recovery["params"]["snapshot"]["initialHistory"]["complete"],
        true
    );
    assert_eq!(
        recovery["params"]["snapshot"]["historyDelivery"],
        "progressive"
    );
    api.release.notify_one();
    drop(bus);
    assert_eq!(
        timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap(),
        "bus_closed"
    );
    assert!(
        rx.recv().await.is_none(),
        "cancelled history must never emit a trailing row or completion"
    );
}

#[tokio::test]
async fn progressive_history_closed_client_cancels_held_read() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let api = Arc::new(HeldHistory {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let (tx, mut rx) = outbound_channel();
    let handle = tokio::spawn(chat_subscription_loop(
        api.clone(),
        AgentId::from("a"),
        None,
        subscriptions::DeltaEncoding::Full,
        Some(intent_core::ConversationProjection::Slim),
        sub,
        None,
        "s".into(),
        tx,
        subscriptions::SnapshotTimer::start(Channel::Chat, "a"),
        3,
        true,
    ));
    timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(10), api.entered.notified())
        .await
        .unwrap();
    drop(rx);
    assert_eq!(
        timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap(),
        "client_closed"
    );
}

#[tokio::test]
async fn progressive_history_membership_revocation_cancels_held_read() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let membership_events = bus.subscribe(SubscriptionFilter::default());
    let api = Arc::new(HeldHistory {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let dyn_api: Arc<dyn WorkspaceApi> = api.clone();
    let principal_id = intent_core::PrincipalId::new();
    let caller = intent_core::Caller::Wire {
        principal_id: principal_id.clone(),
        host_role: intent_core::HostRole::Guest,
    };
    let gate = crate::context::with_request_context(true, Some(caller), async {
        events::MembershipGate::for_current_caller(&dyn_api).unwrap()
    })
    .await;
    let (tx, mut rx) = outbound_channel();
    let handle = tokio::spawn(chat_subscription_loop(
        dyn_api,
        AgentId::from("a"),
        None,
        subscriptions::DeltaEncoding::Full,
        Some(intent_core::ConversationProjection::Slim),
        sub,
        Some((gate, membership_events)),
        "s".into(),
        tx,
        subscriptions::SnapshotTimer::start(Channel::Chat, "a"),
        3,
        true,
    ));
    timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(10), api.entered.notified())
        .await
        .unwrap();
    let _ = bus.publish_transient(&NewEvent {
        workspace_id: WorkspaceId::from("w"),
        timestamp: intent_core::now_iso(),
        event_type: WORKSPACE_UPDATED.into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"changes":{"members":true,"removedPrincipalId":principal_id}}),
    });
    assert_eq!(
        timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap(),
        "membership_revoked"
    );
    assert!(rx.recv().await.is_none());
}

/// A pending database read must keep its future while live events arrive;
/// restarting it for every event can starve history into overflow recovery.
#[tokio::test]
async fn progressive_history_finishes_during_sustained_live_traffic() {
    struct FlowingHistory;
    impl WorkspaceApi for FlowingHistory {
        fn agent_history_batch(
            &self,
            _: AgentId,
            before: Option<i64>,
            _: usize,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                let seq = before.unwrap_or(20) - 1;
                Ok(
                    json!({"messages":[{"id":format!("m{seq}"),"seq":seq,"contentBlocks":[]}],"totalMessages":20}),
                )
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let (tx, mut rx) = outbound_channel();
    let handle = tokio::spawn(chat_subscription_loop(
        Arc::new(FlowingHistory),
        AgentId::from("a"),
        None,
        subscriptions::DeltaEncoding::Full,
        Some(intent_core::ConversationProjection::Slim),
        sub,
        None,
        "s".into(),
        tx,
        subscriptions::SnapshotTimer::start(Channel::Chat, "a"),
        20,
        true,
    ));
    timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let traffic_bus = bus.clone();
    let traffic = tokio::spawn(async move {
        loop {
            let _ = traffic_bus.publish_transient(&NewEvent {
                workspace_id: WorkspaceId::from("w"),
                timestamp: intent_core::now_iso(),
                event_type: AGENT_UPDATED.into(),
                actor: EventActor {
                    actor_type: ActorType::System,
                    ..Default::default()
                },
                session_id: Some("a".into()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({"name":"traffic"}),
            });
            tokio::task::yield_now().await;
        }
    });
    for seq in 1..=20 {
        let frame: Value = serde_json::from_str(
            &timeout(Duration::from_secs(10), rx.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(frame["params"]["kind"], "history", "{frame}");
        assert_eq!(frame["params"]["seq"], seq);
        assert_eq!(frame["params"]["history"]["complete"], seq == 20);
    }
    traffic.abort();
    let _ = traffic.await;
    drop(rx);
    assert_eq!(
        timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap(),
        "client_closed"
    );
}
