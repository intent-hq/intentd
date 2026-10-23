use super::*;
use crate::conn::{handle_fast_path, outbound_channel, OutboundReceiver, BULK_CAPACITY};
use intent_core::{
    events::WORKSPACE_UPDATED, ActorType, EventActor, PrincipalId, WorkspaceApi, WorkspaceId,
};
use intent_services::{EventBus, SubscriptionFilter};
use intent_store::{NewEvent, Store};

fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../events/fixtures/event-stream-recovery.json"
    ))
    .unwrap()
}
fn scenario(id: &str) -> Value {
    corpus()["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == id)
        .unwrap()["expect"]
        .clone()
}
struct Api;
impl WorkspaceApi for Api {}
struct Harness {
    #[cfg_attr(
        not(unix),
        expect(
            dead_code,
            reason = "keeps the store directory alive; only Unix reads its socket path"
        )
    )]
    dir: tempfile::TempDir,
    bus: EventBus,
    api: Arc<dyn WorkspaceApi>,
    out: OutboundSender,
    rx: OutboundReceiver,
    subs: ConnSubs,
}
impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("events.db")).await.unwrap();
        let (out, rx) = outbound_channel();
        Self {
            dir,
            bus: EventBus::new(store),
            api: Arc::new(Api),
            out,
            rx,
            subs: ConnSubs::default(),
        }
    }
    async fn request(&mut self, request: Value) {
        assert!(
            handle_fast_path(
                events::classify(&request).unwrap(),
                &self.api,
                &self.bus,
                &self.out,
                &mut self.subs
            )
            .await
        );
    }
    async fn subscribe(&mut self, params: Value) -> String {
        self.request(json!({"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":params}))
            .await;
        let frame = self.rx.recv().await.unwrap();
        serde_json::from_str::<Value>(&frame).unwrap()["result"]["subscriptionId"]
            .as_str()
            .unwrap()
            .to_owned()
    }
    async fn written(&mut self) -> Value {
        let frame = self.rx.recv().await.unwrap();
        assert!(self.out.recovery.write(&frame, async { true }).await);
        serde_json::from_str(&frame).unwrap()
    }
    async fn fence(&mut self, ids: &[String]) {
        self.request(json!({"jsonrpc":"2.0","id":62,"method":"events.fence","params":{"subscriptionIds":ids}})).await;
    }
}
fn event(workspace: &str, kind: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(workspace),
        timestamp: "2026-10-13T00:00:00Z".into(),
        event_type: kind.into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}
fn forwarder(out: OutboundSender) -> Forwarder {
    Forwarder {
        api: Arc::new(Api),
        gate: None,
        membership: None,
        scope: Some("ws-a".into()),
        id: "ws-sub-1".into(),
        out,
        buffer: ConflationBuffer::new(),
        generation: 0,
        recovery: true,
    }
}
fn file_item(value: &str) -> EventItem {
    let event = Event {
        id: value.into(),
        workspace_id: WorkspaceId::from("ws-a"),
        timestamp: "now".into(),
        event_type: "file:changed".into(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"path":"same-file","value":value}),
    };
    EventItem::new(&conflate::event_key(&event).unwrap(), event)
}

#[tokio::test]
async fn recovery_canonical_request_validation_and_wire_acks() {
    let mut h = Harness::new().await;
    for case in corpus()["requestValidation"].as_array().unwrap() {
        let before = h.subs.subs.len();
        let mut request =
            json!({"jsonrpc":"2.0","method":"events.subscribe","params":case["params"]});
        if case["hasRequestId"] == true {
            request["id"] = json!(60);
        }
        h.request(request).await;
        if case["hasRequestId"] == false {
            assert_eq!(h.subs.subs.len(), before, "{}", case["id"]);
            assert!(h.rx.priority.try_recv().is_err());
        } else {
            let reply = h.written().await;
            if case["accepted"] == true {
                assert!(reply["result"]["subscriptionId"].is_string());
                assert_eq!(
                    reply["result"].get("lossRecovery"),
                    case["params"].get("lossRecovery")
                );
            } else {
                assert_eq!(reply["error"]["code"], case["errorCode"]);
                assert_eq!(h.subs.subs.len(), before);
            }
        }
    }
    for (req, ack) in [
        ("scopedRequest", "scopedAck"),
        ("legacyRequest", "legacyAck"),
    ] {
        h.request(corpus()["wire"][req].clone()).await;
        let reply = h.written().await;
        let mut expected = corpus()["wire"][ack].clone();
        expected["result"]["subscriptionId"] = reply["result"]["subscriptionId"].clone();
        assert_eq!(reply, expected);
    }
}

#[tokio::test]
async fn recovery_ack_loss_and_quiet_raw_fence_are_ordered() {
    let mut h = Harness::new().await;
    // No await after subscribe admission: overflow before the ack is consumed.
    h.request(corpus()["wire"]["scopedRequest"].clone()).await;
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({"secret":1})));
    }
    let ack = h.written().await;
    let sid = ack["result"]["subscriptionId"].as_str().unwrap().to_owned();
    h.fence(std::slice::from_ref(&sid)).await;
    let lost = h.written().await;
    assert_eq!(
        lost,
        json!({"jsonrpc":"2.0","method":"events.lost","params":{
        "subscriptionId":sid,"generation":"1","scope":{"workspaceId":"ws-a"}}})
    );
    let reply = h.written().await;
    assert_eq!(
        reply,
        json!({"jsonrpc":"2.0","id":62,"result":{"subscriptionIds":[sid]}})
    );
    assert!(h.rx.bulk.try_recv().is_err());
}

#[tokio::test]
async fn recovery_fence_validation_uses_entire_same_connection_set() {
    for case in corpus()["fenceValidation"].as_array().unwrap() {
        let mut h = Harness::new().await;
        let a = h
            .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
            .await;
        let b = h
            .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
            .await;
        let legacy = h.subscribe(json!({"eventTypes":["file:*"]})).await;
        let ids: Vec<_> = case["subscriptionIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| match id.as_str().unwrap() {
                "ws-sub-1" => a.clone(),
                "ws-sub-global" => b.clone(),
                "legacy" => legacy.clone(),
                other => other.to_owned(),
            })
            .collect();
        h.fence(&ids).await;
        let reply = h.written().await;
        if case["accepted"] == true {
            assert_eq!(reply["result"], json!({"subscriptionIds":ids}));
        } else {
            assert_eq!(reply["error"]["code"], -32602, "{}", case["id"]);
            assert_eq!(reply["error"]["message"], INVALID_SET);
        }
    }
}

#[tokio::test]
async fn recovery_fence_is_nonblocking_and_orders_preceding_bus_and_bulk_items() {
    let mut h = Harness::new().await;
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    for _ in 0..BULK_CAPACITY {
        h.out.bulk.try_send("occupied".into()).unwrap();
    }
    let _ = h.bus.publish_transient(&event(
        "ws-a",
        "file:changed",
        json!({"path":"a","state":"S1"}),
    ));
    let _ = h
        .bus
        .publish_transient(&event("foreign", "note:updated", json!({})));
    // Workers have not run. Handler returns despite a full writer lane.
    h.fence(std::slice::from_ref(&sid)).await;
    assert!(
        h.rx.priority.try_recv().is_err(),
        "no priority-lane success"
    );
    for _ in 0..BULK_CAPACITY {
        assert_eq!(h.rx.recv().await.unwrap(), "occupied");
    }
    assert_eq!(h.written().await["params"]["event"]["data"]["state"], "S1");
    assert_eq!(h.written().await["result"]["subscriptionIds"], json!([sid]));
}

#[tokio::test]
async fn recovery_retired_fence_set_never_succeeds_partially() {
    let mut h = Harness::new().await;
    let a = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    let b = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.fence(&[a, b.clone()]).await;
    h.subs.remove(&h.api, &b).await;
    let reply = h.written().await;
    assert_eq!(
        reply["error"]["code"],
        scenario("fence-retired-set")["errorCode"]
    );
    assert!(reply.get("result").is_none());
}

#[tokio::test]
async fn recovery_replacement_resets_generation_and_discards_receipts() {
    let mut h = Harness::new().await;
    let params = json!({"eventTypes":["file:*"],"replaceGroup":"files","lossRecovery":1});
    let old = h.subscribe(params.clone()).await;
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    let old_loss = h.rx.recv().await.unwrap(); // queued, intentionally not written
    let new = h.subscribe(params).await;
    assert_ne!(old, new);
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    let lost = h.written().await;
    assert_eq!(lost["params"]["subscriptionId"], new);
    assert_eq!(lost["params"]["generation"], "1");
    assert_eq!(
        serde_json::from_str::<Value>(&old_loss).unwrap()["params"]["subscriptionId"],
        old
    );
    // A fence waits until the new loss has physically flushed and its task progresses.
    h.fence(&[new]).await;
    assert!(h.written().await.get("result").is_some());
    assert!(h.out.recovery.receipts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_conflation_loss_separates_equal_keys() {
    let (out, mut rx) = outbound_channel();
    let mut f = forwarder(out.clone());
    let key = conflate::Key::File("ws-a".into(), "same-file".into());
    f.buffer.push(key.clone(), file_item("pre-loss-file"));
    let task = tokio::spawn(async move {
        assert!(f.loss().await);
        f
    });
    let first = rx.recv().await.unwrap();
    let lost = rx.recv().await.unwrap();
    assert!(out.recovery.write(&lost, async { true }).await);
    let mut f = task.await.unwrap();
    f.buffer.push(key, file_item("post-loss-file"));
    assert!(f.drain().await);
    let after = rx.recv().await.unwrap();
    let first: Value = serde_json::from_str(&first).unwrap();
    let after: Value = serde_json::from_str(&after).unwrap();
    assert_eq!(
        json!([
            first["params"]["event"]["data"]["value"],
            "lost",
            after["params"]["event"]["data"]["value"]
        ]),
        scenario("conflation-barrier")["wireOrder"]
    );
}

#[tokio::test(start_paused = true)]
async fn recovery_writer_timeout_covers_full_queue_and_queued_active_write() {
    for queued in [false, true] {
        let (out, mut rx) = outbound_channel();
        let mut f = forwarder(out.clone());
        if !queued {
            for _ in 0..BULK_CAPACITY {
                out.bulk.try_send("occupied".into()).unwrap();
            }
        }
        let loss = tokio::spawn(async move { f.loss().await });
        let writer = if queued {
            let frame = rx.recv().await.unwrap();
            let output = out.recovery.clone();
            let (started, ready) = oneshot::channel();
            let task = tokio::spawn(async move {
                output
                    .write(&frame, async {
                        let _ = started.send(());
                        std::future::pending().await
                    })
                    .await
            });
            ready.await.unwrap();
            Some(task)
        } else {
            tokio::task::yield_now().await;
            None
        };
        tokio::time::advance(Duration::from_millis(
            corpus()["policy"]["lossBarrierTimeoutMs"].as_u64().unwrap(),
        ))
        .await;
        assert!(!loss.await.unwrap());
        if let Some(writer) = writer {
            assert!(!writer.await.unwrap());
        }
        assert!(out.recovery.is_closed());
        assert!(out.recovery.receipts.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn recovery_generation_exhaustion_closes_without_wrapping() {
    let (out, mut rx) = outbound_channel();
    let mut f = forwarder(out.clone());
    f.generation = u64::MAX;
    assert!(!f.loss().await);
    assert!(out.recovery.is_closed());
    assert_eq!(f.generation, u64::MAX);
    assert!(rx.bulk.try_recv().is_err());
}

#[tokio::test]
async fn recovery_membership_only_loss_closes_legacy_and_modern_before_buffer_drain() {
    for modern in [false, true] {
        let h = Harness::new().await;
        let membership = h.bus.subscribe(SubscriptionFilter {
            event_types: vec![WORKSPACE_UPDATED.into()],
            ..Default::default()
        });
        let caller = intent_core::Caller::Wire {
            principal_id: PrincipalId::from("guest"),
            host_role: intent_core::HostRole::Guest,
        };
        let gate = crate::context::with_request_context(true, Some(caller), async {
            events::MembershipGate::for_current_caller(&h.api).unwrap()
        })
        .await;
        let mut f = forwarder(h.out.clone());
        f.recovery = modern;
        f.gate = Some(gate);
        f.membership = Some(membership);
        f.buffer.push(
            conflate::Key::File("ws-a".into(), "same-file".into()),
            file_item("protected"),
        );
        for _ in 0..2048 {
            let _ = h
                .bus
                .publish_transient(&event("foreign", "note:updated", json!({})));
        }
        // Await the raw worker receipt on an unrelated subscriber is insufficient;
        // instead hold output capacity so the membership receiver wins deterministically.
        for _ in 0..BULK_CAPACITY {
            h.out.bulk.try_send("occupied".into()).unwrap();
        }
        assert!(!f.drain().await);
        assert!(f.buffer.is_empty());
        assert!(h.out.recovery.is_closed());
    }
}

struct MemberApi(std::sync::atomic::AtomicBool);
impl WorkspaceApi for MemberApi {
    fn principal_host_role(
        &self,
        _: PrincipalId,
    ) -> futures::future::BoxFuture<'_, intent_core::Result<intent_core::HostRole>> {
        Box::pin(async { Ok(intent_core::HostRole::Guest) })
    }
    fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> futures::future::BoxFuture<'_, intent_core::Result<intent_core::Workspace>> {
        let allowed = self.0.load(std::sync::atomic::Ordering::SeqCst) && id.as_str() == "ws-a";
        Box::pin(async move {
            if allowed {
                Ok(intent_core::Workspace {
                    id,
                    ..intent_core::chief_workspace()
                })
            } else {
                Err(intent_core::Error::NotFound("workspace".into()))
            }
        })
    }
}

#[tokio::test]
async fn recovery_foreign_activity_cannot_expand_guest_marker_scope_or_disclose_payload() {
    let h = Harness::new().await;
    // Independently controlled primary/membership sources isolate primary lag.
    let membership_bus = EventBus::new(h.bus.store().clone());
    let membership = membership_bus.subscribe(SubscriptionFilter {
        event_types: vec![WORKSPACE_UPDATED.into()],
        ..Default::default()
    });
    let sub = h.bus.subscribe(SubscriptionFilter {
        event_types: vec!["note:*".into()],
        workspace_id: Some("ws-a".into()),
        ..Default::default()
    });
    let control = sub.fence_handle();
    let api: Arc<dyn WorkspaceApi> = Arc::new(MemberApi(std::sync::atomic::AtomicBool::new(true)));
    let caller = intent_core::Caller::Wire {
        principal_id: PrincipalId::from("guest"),
        host_role: intent_core::HostRole::Guest,
    };
    let output = h.out.clone();
    let (handle,) = crate::context::with_request_context(true, Some(caller), async {
        let gate = events::MembershipGate::for_current_caller(&api).unwrap();
        (spawn_forwarder(forward(
            Arc::clone(&api),
            sub,
            Some((gate, membership)),
            Some("ws-a".into()),
            "guest-lease".into(),
            output,
            false,
            true,
            Lifetime::new(),
        )),)
    })
    .await;
    for _ in 0..2048 {
        let _ = h.bus.publish_transient(&event(
            "foreign",
            "note:updated",
            json!({"secret":"must not escape"}),
        ));
    }
    let receipt = control.request().unwrap();
    let mut h = h;
    assert_eq!(
        h.written().await,
        json!({"jsonrpc":"2.0","method":"events.lost","params":{
        "subscriptionId":"guest-lease","generation":"1","scope":{"workspaceId":"ws-a"}}})
    );
    receipt.await.unwrap();
    assert!(h.rx.bulk.try_recv().is_err());
    handle.abort();
}

#[tokio::test]
async fn recovery_membership_loss_clears_a_previously_allowed_guest_cache() {
    let mut h = Harness::new().await;
    let api = Arc::new(MemberApi(std::sync::atomic::AtomicBool::new(true)));
    h.api = api.clone();
    let caller = intent_core::Caller::Wire {
        principal_id: PrincipalId::from("guest"),
        host_role: intent_core::HostRole::Guest,
    };
    let gate = crate::context::with_request_context(true, Some(caller), async {
        events::MembershipGate::for_current_caller(&h.api).unwrap()
    })
    .await;
    let mut f = forwarder(h.out.clone());
    f.gate = Some(gate);
    f.membership = Some(h.bus.subscribe(SubscriptionFilter {
        event_types: vec![WORKSPACE_UPDATED.into()],
        ..Default::default()
    }));
    let item = file_item("protected");
    assert_eq!(f.allowed(item.event()).await, Some(true));
    api.0.store(false, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    for _ in 0..BULK_CAPACITY {
        h.out.bulk.try_send("occupied".into()).unwrap();
    }
    assert!(!f.send_item(item).await);
    assert!(h.out.recovery.is_closed());
    assert!(
        !f.gate
            .as_mut()
            .unwrap()
            .allows(file_item("probe").event())
            .await,
        "cached allow was discarded"
    );
    assert_eq!(
        f.allowed(file_item("probe").event()).await,
        None,
        "closed connection cannot admit another frame"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_uds_loss_and_fence_follow_physical_write_order() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let h = Harness::new().await;
    let path = h.dir.path().join("recovery.sock");
    let (stop, stopped) = oneshot::channel();
    let api = h.api.clone();
    let bus = h.bus.clone();
    let server_path = path.clone();
    let server = tokio::spawn(async move {
        crate::listener::serve_uds(api, bus, &server_path, None, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        async {
            while !path.exists() {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .unwrap();
    let socket = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = socket.into_split();
    let mut lines = tokio::io::BufReader::new(read).lines();
    write
        .write_all(format!("{}\n", corpus()["wire"]["scopedRequest"]).as_bytes())
        .await
        .unwrap();
    let ack: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(ack["result"]["lossRecovery"], 1);
    let sid = ack["result"]["subscriptionId"].clone();
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    write.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":62,"method":"events.fence","params":{"subscriptionIds":[sid]}})).as_bytes()).await.unwrap();
    let lost: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let fence: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(lost["method"], "events.lost");
    assert_eq!(lost["params"]["generation"], "1");
    assert_eq!(
        fence,
        json!({"jsonrpc":"2.0","id":62,"result":{"subscriptionIds":[sid]}})
    );
    drop(write);
    drop(lines);
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn recovery_legacy_omission_stays_silent_and_fences_do_not_create_loss() {
    let mut h = Harness::new().await;
    let legacy = h.subscribe(json!({"eventTypes":["file:*"]})).await;
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    let _ = h
        .bus
        .publish_transient(&event("ws-a", "file:changed", json!({"path":"last"})));
    assert_eq!(h.written().await["method"], "events.event");
    h.fence(&[legacy]).await;
    assert_eq!(h.written().await["error"]["code"], -32602);
    let modern = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    for _ in 0..3 {
        h.fence(std::slice::from_ref(&modern)).await;
        assert!(h.written().await.get("result").is_some());
    }
    assert!(h.rx.bulk.try_recv().is_err());
}

#[tokio::test]
async fn recovery_loss_between_fences_precedes_end_reply() {
    let mut h = Harness::new().await;
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.fence(std::slice::from_ref(&sid)).await;
    assert!(h.written().await.get("result").is_some());
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({})));
    }
    h.fence(std::slice::from_ref(&sid)).await;
    assert_eq!(h.written().await["method"], "events.lost");
    assert!(h.written().await.get("result").is_some());
}

#[tokio::test]
async fn recovery_overflow_before_ack_enqueue_still_follows_ack() {
    let mut h = Harness::new().await;
    h.bus = EventBus::with_broadcast_capacity(h.bus.store().clone(), 1);
    for _ in 0..super::super::PRIORITY_CAPACITY {
        h.out.priority.try_send("occupied".into()).unwrap();
    }
    let mut admission = Box::pin(handle_fast_path(
        events::classify(&corpus()["wire"]["scopedRequest"]).unwrap(),
        &h.api,
        &h.bus,
        &h.out,
        &mut h.subs,
    ));
    assert!(futures::poll!(&mut admission).is_pending());
    let _ = h
        .bus
        .publish_transient(&event("ws-a", "file:changed", json!({"path":"lost"})));
    let _ = h
        .bus
        .publish_transient(&event("foreign", "note:updated", json!({})));
    assert_eq!(h.rx.priority.recv().await.unwrap(), "occupied");
    assert!(admission.await);
    for _ in 1..super::super::PRIORITY_CAPACITY {
        assert_eq!(h.rx.recv().await.unwrap(), "occupied");
    }
    let ack = h.written().await;
    let marker = h.written().await;
    assert_eq!(ack["id"], 60);
    assert_eq!(marker["method"], "events.lost");
    assert_eq!(
        marker["params"]["subscriptionId"],
        ack["result"]["subscriptionId"]
    );
}

#[tokio::test]
async fn recovery_fence_rejects_bad_shapes_and_notification_creates_no_task() {
    let mut h = Harness::new().await;
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.request(json!({"jsonrpc":"2.0","method":"events.fence","params":{"subscriptionIds":[sid]}}))
        .await;
    assert!(h.subs.fences.is_empty());
    assert!(h.rx.priority.try_recv().is_err());
    for params in [
        json!({}),
        json!({"subscriptionIds":null}),
        json!({"subscriptionIds":"x"}),
        json!({"subscriptionIds":[""]}),
        json!({"subscriptionIds":[1]}),
    ] {
        h.request(json!({"jsonrpc":"2.0","id":62,"method":"events.fence","params":params}))
            .await;
        let reply = h.written().await;
        assert_eq!(reply["error"]["code"], -32602);
        assert_eq!(reply["error"]["message"], INVALID_SET);
    }
}

#[tokio::test]
async fn recovery_fence_deadline_aborts_active_writer_and_releases_receipts() {
    let mut h = Harness::new().await;
    tokio::time::pause();
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.fence(&[sid]).await;
    let frame = h.rx.recv().await.unwrap();
    let output = h.out.recovery.clone();
    let (started, ready) = oneshot::channel();
    let writer = tokio::spawn(async move {
        output
            .write(&frame, async {
                let _ = started.send(());
                std::future::pending().await
            })
            .await
    });
    ready.await.unwrap();
    tokio::time::advance(DEADLINE).await;
    assert!(!writer.await.unwrap());
    assert!(h.out.recovery.is_closed());
    assert!(h.out.recovery.receipts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_retiring_lease_aborts_an_already_active_fence_write() {
    let mut h = Harness::new().await;
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.fence(std::slice::from_ref(&sid)).await;
    let frame = h.rx.recv().await.unwrap();
    let output = h.out.recovery.clone();
    let (started, ready) = oneshot::channel();
    let writer = tokio::spawn(async move {
        output
            .write(&frame, async {
                let _ = started.send(());
                std::future::pending().await
            })
            .await
    });
    ready.await.unwrap();
    h.subs.remove(&h.api, &sid).await;
    assert!(!writer.await.unwrap());
    assert!(h.out.recovery.is_closed());
    assert!(h.out.recovery.receipts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_membership_closure_discards_buffer_and_cancels_connection() {
    let h = Harness::new().await;
    let caller = intent_core::Caller::Wire {
        principal_id: PrincipalId::from("guest"),
        host_role: intent_core::HostRole::Guest,
    };
    let gate = crate::context::with_request_context(true, Some(caller), async {
        events::MembershipGate::for_current_caller(&h.api).unwrap()
    })
    .await;
    let mut f = forwarder(h.out.clone());
    f.gate = Some(gate);
    f.buffer.push(
        conflate::Key::File("ws-a".into(), "same-file".into()),
        file_item("protected"),
    );
    assert!(!f.membership(None));
    assert!(f.buffer.is_empty());
    assert!(h.out.recovery.is_closed());
}

#[tokio::test]
async fn recovery_aborted_coordinator_cannot_release_queued_fence_as_ordinary_frame() {
    let mut h = Harness::new().await;
    let sid = h
        .subscribe(json!({"eventTypes":["file:*"],"lossRecovery":1}))
        .await;
    h.fence(&[sid]).await;
    let frame = h.rx.recv().await.unwrap();
    let tasks = std::mem::take(&mut h.subs.fences);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let mut sent = false;
    assert!(
        !h.out
            .recovery
            .write(&frame, async {
                sent = true;
                true
            })
            .await
    );
    assert!(!sent, "an orphaned fence reply must never reach the socket");
    assert!(h.out.recovery.is_closed());
}

#[tokio::test]
async fn recovery_unscoped_loss_marker_contains_explicit_null_scope() {
    let mut h = Harness::new().await;
    h.request(json!({"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["file:*"],"lossRecovery":1}})).await;
    for _ in 0..2048 {
        let _ = h
            .bus
            .publish_transient(&event("foreign", "note:updated", json!({"secret":1})));
    }
    let ack = h.written().await;
    let sid = ack["result"]["subscriptionId"].as_str().unwrap().to_owned();
    h.fence(std::slice::from_ref(&sid)).await;
    assert_eq!(
        h.written().await,
        json!({"jsonrpc":"2.0","method":"events.lost","params":{
        "subscriptionId":sid,"generation":"1","scope":{"workspaceId":null}}})
    );
    assert_eq!(
        h.written().await["result"],
        json!({"subscriptionIds":[sid]})
    );
    assert!(h.rx.bulk.try_recv().is_err());
}

#[tokio::test]
async fn recovery_retired_loss_receipt_does_not_close_replacement_connection() {
    let output = Output::default();
    let frame = "retired loss marker".to_owned();
    let (_, guard) = output.register(frame.clone(), vec![]).unwrap();
    drop(guard);
    assert!(!output.is_closed());
    assert!(output.write(&frame, async { true }).await);
    assert!(output.receipts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovery_completed_fence_guard_cannot_cancel_reused_request_id() {
    let output = Output::default();
    let frame = events::success_frame(&json!(62), &json!({"subscriptionIds":["lease"]}));
    let lease = Lifetime::new();
    let (first, first_guard) = output.register(frame.clone(), vec![lease.clone()]).unwrap();
    assert!(output.write(&frame, async { true }).await);
    first.await.unwrap();
    let (second, second_guard) = output.register(frame.clone(), vec![lease]).unwrap();
    drop(first_guard);
    assert!(
        !output.is_closed(),
        "a completed receipt cannot cancel a later identical response"
    );
    assert!(output.write(&frame, async { true }).await);
    second.await.unwrap();
    drop(second_guard);
    assert!(!output.is_closed());
}

struct DiscussionProjectionApi {
    mode: std::sync::atomic::AtomicU8,
    calls: std::sync::atomic::AtomicUsize,
}
impl WorkspaceApi for DiscussionProjectionApi {
    fn discussion_project_event(
        &self,
        mut event: Event,
    ) -> futures::future::BoxFuture<'_, intent_core::Result<Option<Event>>> {
        use std::sync::atomic::Ordering::SeqCst;
        self.calls.fetch_add(1, SeqCst);
        Box::pin(async move {
            match self.mode.load(SeqCst) {
                0 => Ok(None),
                1 => {
                    event.data = json!({"visibleToRecipient":true});
                    Ok(Some(event))
                }
                _ => Err(intent_core::Error::Internal(
                    "projection unavailable".into(),
                )),
            }
        })
    }
}
fn private_item(kind: &str) -> EventItem {
    let mut event = file_item("private").event().clone();
    event.event_type = kind.into();
    event.data = json!({"unprojectedSecret":"must not escape"});
    EventItem::Latest(event)
}

#[tokio::test]
async fn recovery_discussion_and_typing_projection_fail_closed_without_a_guest_gate() {
    use std::sync::atomic::{AtomicU8, AtomicUsize};
    for kind in [
        intent_core::events::DISCUSSION_MESSAGE,
        intent_core::events::DISCUSSION_REACTION,
        intent_core::events::PRESENCE_CHANGED,
    ] {
        for mode in [0, 1, 2] {
            let (out, mut rx) = outbound_channel();
            let mut f = forwarder(out);
            f.api = Arc::new(DiscussionProjectionApi {
                mode: AtomicU8::new(mode),
                calls: AtomicUsize::new(0),
            });
            assert!(f.send_item(private_item(kind)).await);
            if mode == 1 {
                let frame = rx.bulk.try_recv().expect("projected event delivered");
                let frame: Value = serde_json::from_str(&frame).unwrap();
                assert_eq!(
                    frame["params"]["event"]["data"],
                    json!({"visibleToRecipient":true})
                );
            }
            assert!(
                rx.bulk.try_recv().is_err(),
                "raw or denied payload escaped for {kind}"
            );
        }
    }
}

#[tokio::test]
async fn recovery_discussion_access_is_projected_after_backpressure_clears() {
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::SeqCst};
    let (out, mut rx) = outbound_channel();
    for _ in 0..BULK_CAPACITY {
        out.bulk.try_send("occupied".into()).unwrap();
    }
    let api = Arc::new(DiscussionProjectionApi {
        mode: AtomicU8::new(1),
        calls: AtomicUsize::new(0),
    });
    let mut f = forwarder(out);
    f.api = api.clone();
    let mut send = Box::pin(f.send_item(private_item(intent_core::events::DISCUSSION_MESSAGE)));
    assert!(futures::poll!(&mut send).is_pending());
    assert_eq!(api.calls.load(SeqCst), 0);
    api.mode.store(0, SeqCst);
    assert_eq!(rx.bulk.recv().await.unwrap(), "occupied");
    assert!(send.await);
    assert_eq!(api.calls.load(SeqCst), 1);
    for _ in 1..BULK_CAPACITY {
        assert_eq!(rx.bulk.try_recv().unwrap(), "occupied");
    }
    assert!(
        rx.bulk.try_recv().is_err(),
        "revoked discussion escaped after capacity became available"
    );
}

#[tokio::test]
async fn desktop_authority_survives_recovery_forwarder_and_fence() {
    let mut h = Harness::new().await;
    let caller = intent_core::Caller::Wire {
        principal_id: PrincipalId::from("desktop-owner"),
        host_role: intent_core::HostRole::Owner,
    };
    let connection = intent_core::desktop::DesktopConnection {
        client_id: intent_core::ClientId::from("desktop-client"),
        principal_id: PrincipalId::from("desktop-owner"),
        connection_epoch: "current".into(),
    };
    let sid = crate::context::with_credential_context(
        true,
        Some(caller),
        None,
        Some(connection),
        h.subscribe(json!({"eventTypes":["desktop:*"],"lossRecovery":1})),
    )
    .await;
    for (principal, epoch, marker) in [
        ("foreign-owner", "current", "foreign"),
        ("desktop-owner", "retired", "stale"),
        ("desktop-owner", "current", "authorized"),
    ] {
        let mut e = event(
            "ws-a",
            intent_core::events::DESKTOP_SESSION_CHANGED,
            json!({"marker":marker}),
        );
        e.metadata = Some(json!({"desktopPrincipalId":principal,"desktopConnectionEpoch":epoch}));
        let _ = h.bus.publish_transient(&e);
    }
    h.fence(std::slice::from_ref(&sid)).await;
    let delivered = h.written().await;
    assert_eq!(delivered["method"], "events.event");
    assert!(delivered.to_string().contains("authorized"));
    assert!(!delivered.to_string().contains("foreign"));
    assert!(!delivered.to_string().contains("stale"));
    let receipt = h.written().await;
    assert_eq!(receipt["id"], 62);
    assert!(h.rx.bulk.try_recv().is_err());
}
