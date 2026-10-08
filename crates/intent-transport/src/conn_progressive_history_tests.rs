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

#[derive(Clone, Copy)]
enum WorkspaceLookup {
    Ready,
    Failed,
    Held,
}

struct HeldFirstHistory(HeldHistory, WorkspaceLookup);
impl WorkspaceApi for HeldFirstHistory {
    fn agent_get(
        &self,
        id: AgentId,
        workspace: Option<WorkspaceId>,
    ) -> BoxFuture<'_, intent_core::Result<intent_core::AgentLite>> {
        Box::pin(async move {
            match self.1 {
                WorkspaceLookup::Ready => self.0.agent_get(id, workspace).await,
                WorkspaceLookup::Failed => {
                    Err(intent_core::Error::Internal("lookup failed".into()))
                }
                WorkspaceLookup::Held => {
                    self.0.entered.notify_one();
                    self.0.release.notified().await;
                    self.0.agent_get(id, workspace).await
                }
            }
        })
    }
    fn agent_history_batch(
        &self,
        _: AgentId,
        before: Option<i64>,
        _: usize,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            if before.is_none() {
                self.0.entered.notify_one();
                self.0.release.notified().await;
                Ok(
                    json!({"messages":[{"id":"captured-before-unshare","seq":2,"contentBlocks":[]}],"totalMessages":3}),
                )
            } else {
                Ok(json!({"messages":[],"totalMessages":3}))
            }
        })
    }
}

#[tokio::test]
async fn progressive_history_first_read_cancels_on_closed_client() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let api = Arc::new(HeldFirstHistory(
        HeldHistory {
            entered: Notify::new(),
            release: Notify::new(),
        },
        WorkspaceLookup::Ready,
    ));
    let (tx, rx) = outbound_channel();
    let mut handle = tokio::spawn(chat_subscription_loop(
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
    timeout(Duration::from_secs(10), api.0.entered.notified())
        .await
        .unwrap();
    drop(rx);
    let result = timeout(Duration::from_secs(1), &mut handle).await;
    if result.is_err() {
        handle.abort();
    }
    assert!(result.is_ok(),"First read survives closed outbound lane; continuation-read cancellation does not cover seq-0");
    assert_eq!(result.unwrap().unwrap(), "client_closed");
}

#[tokio::test]
async fn progressive_history_first_snapshot_suppressed_after_membership_revocation() {
    assert_first_snapshot_revoked(WorkspaceLookup::Ready).await;
}

#[tokio::test]
async fn progressive_history_first_snapshot_revoked_after_failed_workspace_lookup() {
    assert_first_snapshot_revoked(WorkspaceLookup::Failed).await;
}

#[tokio::test]
async fn progressive_history_first_workspace_lookup_cancels_on_revocation() {
    assert_first_snapshot_revoked(WorkspaceLookup::Held).await;
}

async fn assert_first_snapshot_revoked(lookup: WorkspaceLookup) {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let membership_events = bus.subscribe(SubscriptionFilter::default());
    let api = Arc::new(HeldFirstHistory(
        HeldHistory {
            entered: Notify::new(),
            release: Notify::new(),
        },
        lookup,
    ));
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
    let mut handle = tokio::spawn(chat_subscription_loop(
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
    timeout(Duration::from_secs(10), api.0.entered.notified())
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
    if !matches!(lookup, WorkspaceLookup::Held) {
        api.0.release.notify_one();
    }
    let result = timeout(Duration::from_secs(2), &mut handle).await;
    if result.is_err() {
        handle.abort();
    }
    assert_eq!(result.unwrap().unwrap(), "membership_revoked");
    let frame = timeout(Duration::from_secs(10), rx.recv()).await.unwrap();
    assert!(
        frame.is_none(),
        "Snapshot escaped after own membership revocation: {frame:?}"
    );
}

#[tokio::test]
async fn progressive_history_first_enqueue_prioritizes_revocation_over_ready_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let membership_events = bus.subscribe(SubscriptionFilter::default());
    let api: Arc<dyn WorkspaceApi> = Arc::new(HeldHistory {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let principal_id = intent_core::PrincipalId::new();
    let caller = intent_core::Caller::Wire {
        principal_id: principal_id.clone(),
        host_role: intent_core::HostRole::Guest,
    };
    let gate = crate::context::with_request_context(true, Some(caller), async {
        events::MembershipGate::for_current_caller(&api).unwrap()
    })
    .await;
    let (tx, mut rx) = outbound_channel();
    while tx.bulk_sender().try_send("occupied".into()).is_ok() {}
    let mut forwarder = Box::pin(chat_subscription_loop(
        api,
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
    // Every initial mock read is immediately ready, so the first Pending
    // is the seq-0 outbound reservation, not a timing-dependent sleep.
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(forwarder.as_mut().poll(cx)))
            .await
            .is_pending()
    );
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
    // Publishing feeds a separate subscription delivery task. Let it queue
    // the removal before making the outbound reservation ready, so this
    // exercises two ready branches rather than racing event delivery itself.
    tokio::task::yield_now().await;
    assert_eq!(rx.bulk.try_recv().unwrap(), "occupied");
    assert_eq!(
        timeout(Duration::from_secs(10), &mut forwarder)
            .await
            .unwrap(),
        "membership_revoked"
    );
    drop(forwarder);
    while let Some(frame) = rx.recv().await {
        assert_eq!(
            frame, "occupied",
            "private seq-0 must not be enqueued after revocation"
        );
    }
}

struct HeldInitialOverlay {
    held: HeldHistory,
    fail_read: bool,
}

impl WorkspaceApi for HeldInitialOverlay {
    fn agent_get(
        &self,
        id: AgentId,
        workspace: Option<WorkspaceId>,
    ) -> BoxFuture<'_, intent_core::Result<intent_core::AgentLite>> {
        self.held.agent_get(id, workspace)
    }

    fn agent_history_batch(
        &self,
        id: AgentId,
        before: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        if self.fail_read {
            Box::pin(async move {
                self.held.entered.notify_one();
                Err(intent_core::Error::Internal(
                    "transient history read".into(),
                ))
            })
        } else {
            self.held.agent_history_batch(id, before, limit)
        }
    }

    fn agent_activity_flags(&self, _: AgentId) -> BoxFuture<'_, Value> {
        Box::pin(async move {
            self.held.entered.notify_one();
            self.held.release.notified().await;
            json!({})
        })
    }
}

#[tokio::test]
async fn progressive_history_initial_overlay_and_retry_are_cancellable() {
    for fail_read in [false, true] {
        for close_client in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
            let sub = bus.subscribe(SubscriptionFilter::default());
            let membership_events = bus.subscribe(SubscriptionFilter::default());
            let api = Arc::new(HeldInitialOverlay {
                held: HeldHistory {
                    entered: Notify::new(),
                    release: Notify::new(),
                },
                fail_read,
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
            let (tx, rx) = outbound_channel();
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
            timeout(Duration::from_secs(10), api.held.entered.notified())
                .await
                .unwrap();
            let mut rx = Some(rx);
            let expected = if close_client {
                drop(rx.take());
                "client_closed"
            } else {
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
                "membership_revoked"
            };
            assert_eq!(
                timeout(Duration::from_secs(10), handle)
                    .await
                    .unwrap()
                    .unwrap(),
                expected
            );
            if let Some(mut rx) = rx {
                assert!(rx.recv().await.is_none());
            }
        }
    }
}

#[tokio::test]
async fn progressive_history_first_enqueue_cancels_on_closed_client() {
    let dir = tempfile::tempdir().unwrap();
    let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
    let sub = bus.subscribe(SubscriptionFilter::default());
    let api: Arc<dyn WorkspaceApi> = Arc::new(HeldHistory {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let (tx, rx) = outbound_channel();
    while tx.bulk_sender().try_send("occupied".into()).is_ok() {}
    let mut forwarder = Box::pin(chat_subscription_loop(
        api,
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
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(forwarder.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(rx);
    assert_eq!(
        timeout(Duration::from_secs(10), forwarder).await.unwrap(),
        "client_closed"
    );
}
