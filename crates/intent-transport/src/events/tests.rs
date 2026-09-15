//! Unit tests for the pure `events.` fast-path helpers.

use super::*;
use intent_core::{ActorType, Event, EventActor, WorkspaceId};

fn parse(s: &str) -> Value {
    serde_json::from_str(s).unwrap()
}

#[test]
fn classify_routes_subscribe_and_unsubscribe() {
    let sub = parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["note:*"]}}"#,
    );
    assert!(matches!(classify(&sub), Some(FastPath::Subscribe { .. })));

    let unsub = parse(
        r#"{"jsonrpc":"2.0","id":2,"method":"events.unsubscribe","params":{"subscriptionId":"ws-sub-1"}}"#,
    );
    assert!(matches!(
        classify(&unsub),
        Some(FastPath::Unsubscribe { .. })
    ));
}

#[test]
fn classify_falls_through_for_non_events_and_bad_envelope() {
    // Non-events method.
    assert!(classify(&parse(r#"{"jsonrpc":"2.0","id":1,"method":"note.list"}"#)).is_none());
    // Wrong jsonrpc version.
    assert!(classify(&parse(
        r#"{"jsonrpc":"1.0","id":1,"method":"events.subscribe"}"#
    ))
    .is_none());
    // Bad id type (object) → fall through so the dispatcher returns -32600.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":{},"method":"events.subscribe"}"#
    ))
    .is_none());
    // Not an object.
    assert!(classify(&parse("[]")).is_none());
}

#[test]
fn classify_notification_has_no_id() {
    let notif =
        parse(r#"{"jsonrpc":"2.0","method":"events.subscribe","params":{"eventTypes":["a"]}}"#);
    match classify(&notif) {
        Some(FastPath::Subscribe { id, .. }) => {
            assert!(!id.present);
            assert_eq!(id.echo, Value::Null);
        }
        _ => panic!("expected subscribe"),
    }

    // Explicit null id is "present" and echoes null.
    let null_id = parse(
        r#"{"jsonrpc":"2.0","id":null,"method":"events.subscribe","params":{"eventTypes":["a"]}}"#,
    );
    match classify(&null_id) {
        Some(FastPath::Subscribe { id, .. }) => {
            assert!(id.present);
            assert_eq!(id.echo, Value::Null);
        }
        _ => panic!("expected subscribe"),
    }
}

#[test]
fn subscribe_params_validation() {
    let ok =
        parse(r#"{"eventTypes":["note:*","agent:idle"],"workspaceId":"ws-1","replaceGroup":"g"}"#);
    let p = parse_subscribe_params(ok.as_object().unwrap()).unwrap();
    assert_eq!(p.event_types, vec!["note:*", "agent:idle"]);
    assert_eq!(p.workspace_id.as_deref(), Some("ws-1"));
    assert_eq!(p.replace_group.as_deref(), Some("g"));

    // Empty / missing / non-array → error.
    for bad in [r#"{"eventTypes":[]}"#, r"{}", r#"{"eventTypes":"note:*"}"#] {
        let v = parse(bad);
        let err = parse_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("non-empty array"));
    }
}

#[test]
fn unsubscribe_id_validation() {
    let ok = parse(r#"{"subscriptionId":"ws-sub-3"}"#);
    assert_eq!(
        parse_unsubscribe_id(ok.as_object().unwrap()).unwrap(),
        "ws-sub-3"
    );
    for bad in [r"{}", r#"{"subscriptionId":""}"#] {
        let v = parse(bad);
        let err = parse_unsubscribe_id(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("subscriptionId is required"));
    }
}

#[test]
fn next_subscription_id_is_monotonic_and_prefixed() {
    let a = next_subscription_id();
    let b = next_subscription_id();
    assert!(a.starts_with("ws-sub-"));
    assert!(b.starts_with("ws-sub-"));
    let na: u64 = a.trim_start_matches("ws-sub-").parse().unwrap();
    let nb: u64 = b.trim_start_matches("ws-sub-").parse().unwrap();
    assert_eq!(nb, na + 1);
}

#[test]
fn error_frame_tags_invalid_params_with_data_code() {
    // -32602 carries the machine-readable discriminator (PROTOCOL §3.3).
    let v = parse(&error_frame(
        &json!(7),
        -32602,
        "eventTypes must be a non-empty array",
    ));
    assert_eq!(v["error"]["code"], json!(-32602));
    assert_eq!(
        v["error"]["message"],
        "eventTypes must be a non-empty array"
    );
    assert_eq!(v["error"]["data"]["code"], "invalid-params");

    // Other codes stay data-less.
    for code in [-32600, -32601, -32603, -32001] {
        let v = parse(&error_frame(&json!(1), code, "boom"));
        assert_eq!(v["error"]["code"], json!(code));
        assert!(v["error"].get("data").is_none(), "no data for {code}: {v}");
    }
}

#[test]
fn event_notification_envelope_matches_protocol() {
    let event = Event {
        id: "evt-789".to_string(),
        workspace_id: WorkspaceId::from("ws-abc"),
        timestamp: "2026-06-17T04:35:04.055Z".to_string(),
        event_type: "note:updated".to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some("agent-123".to_string()),
            name: Some("Coordinator".to_string()),
            ..Default::default()
        },
        session_id: Some("sess-ignored".to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "noteId": "spec", "action": "update" }),
    };
    let frame = build_event_notification("ws-sub-1", &event);
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["method"], "events.event");
    assert_eq!(v["params"]["subscriptionId"], "ws-sub-1");
    let ev = &v["params"]["event"];
    assert_eq!(ev["type"], "note:updated");
    assert_eq!(ev["workspaceId"], "ws-abc");
    assert_eq!(ev["id"], "evt-789");
    assert_eq!(ev["timestamp"], "2026-06-17T04:35:04.055Z");
    assert_eq!(ev["actor"]["type"], "agent");
    assert_eq!(ev["actor"]["id"], "agent-123");
    assert_eq!(ev["data"]["noteId"], "spec");
    // §6.3: the event object carries exactly type/workspaceId/id/timestamp/actor/data.
    assert!(ev.get("sessionId").is_none());
    let keys: Vec<&String> = ev.as_object().unwrap().keys().collect();
    assert_eq!(keys.len(), 6);
}

/// Multiplayer w3 fan-out: a REAL `events.subscribe` through
/// [`crate::conn::handle_fast_path`] under a non-administrator caller may
/// name owner-only patterns, but the forwarder only ever emits allowlisted
/// types — including the `client:*` pair the transport's reverse registry
/// publishes, which is why those two live in the taxonomy. The same frame
/// under the administrator delivers everything it named.
mod collaborator_fan_out {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use intent_core::events::{CLIENT_CONNECTED, NOTE_UPDATED, TERMINAL_DATA, WORKSPACE_UPDATED};
    use intent_core::{
        ActorType, Caller, Error, EventActor, PrincipalId, Workspace, WorkspaceApi, WorkspaceId,
    };
    use intent_services::EventBus;
    use intent_store::{NewEvent, Store};
    use serde_json::{json, Value};

    use crate::conn::{handle_fast_path, outbound_channel, ConnSubs, OutboundReceiver};
    use crate::events::classify;

    /// `workspace.get` stand-in: `Ok` for the workspaces in `members`,
    /// `NotFound` otherwise — the shape the service layer answers a
    /// collaborator with. Shared so a test can revoke membership mid-stream.
    struct MembershipApi {
        members: Arc<Mutex<HashSet<String>>>,
    }

    impl WorkspaceApi for MembershipApi {
        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, intent_core::Result<Workspace>> {
            let allowed = self.members.lock().unwrap().contains(id.as_str());
            Box::pin(async move {
                if allowed {
                    Ok(Workspace {
                        id,
                        ..intent_core::chief_workspace()
                    })
                } else {
                    Err(Error::NotFound(format!("workspace {id}")))
                }
            })
        }
    }

    fn event(event_type: &str, workspace_id: &str) -> NewEvent {
        event_with(event_type, workspace_id, json!({}))
    }

    fn event_with(event_type: &str, workspace_id: &str, data: Value) -> NewEvent {
        NewEvent {
            workspace_id: WorkspaceId::from(workspace_id),
            timestamp: intent_core::now_iso(),
            event_type: event_type.to_string(),
            actor: EventActor {
                actor_type: ActorType::System,
                id: Some("system".to_string()),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        }
    }

    /// Collect `(type, workspaceId)` of every `events.event` frame on the
    /// bulk lane until it stays quiet for a short window.
    async fn delivered(rx: &mut OutboundReceiver) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Ok(Some(frame)) =
            tokio::time::timeout(Duration::from_millis(300), rx.bulk.recv()).await
        {
            let v: Value = serde_json::from_str(&frame).unwrap();
            assert_eq!(v["method"], "events.event");
            let ev = &v["params"]["event"];
            out.push((
                ev["type"].as_str().unwrap().to_string(),
                ev["workspaceId"].as_str().unwrap_or_default().to_string(),
            ));
        }
        out
    }

    async fn delivered_types(rx: &mut OutboundReceiver) -> Vec<String> {
        delivered(rx).await.into_iter().map(|(t, _)| t).collect()
    }

    struct Harness {
        bus: EventBus,
        rx: OutboundReceiver,
        subs: ConnSubs,
        members: Arc<Mutex<HashSet<String>>>,
        subscription_id: String,
        _dir: tempfile::TempDir,
    }

    /// Subscribe through the real fast path under `caller`, with `members`
    /// as the caller's member workspaces.
    async fn subscribe(caller: Caller, members: &[&str], params: Value) -> Harness {
        let dir = tempfile::Builder::new()
            .prefix("intent-transport-collab-fanout-")
            .tempdir()
            .unwrap();
        let store = Store::open(&dir.path().join("bus.db")).await.unwrap();
        let bus = EventBus::new(store);
        let members = Arc::new(Mutex::new(
            members
                .iter()
                .map(|s| (*s).to_string())
                .collect::<HashSet<_>>(),
        ));
        let api: Arc<dyn WorkspaceApi> = Arc::new(MembershipApi {
            members: Arc::clone(&members),
        });
        let (out_tx, mut rx) = outbound_channel();
        let mut subs = ConnSubs::default();

        let frame = json!({"jsonrpc":"2.0","id":1,"method":"events.subscribe", "params": params});
        let fast = classify(&frame).expect("classifies as events.subscribe");
        let bus_ref = &bus;
        let out_ref = &out_tx;
        let subs_ref = &mut subs;
        let api_ref = &api;
        let accepted = crate::context::with_request_context(true, Some(caller), async move {
            handle_fast_path(fast, api_ref, bus_ref, out_ref, subs_ref).await
        })
        .await;
        assert!(accepted);
        let reply: Value = serde_json::from_str(&rx.priority.recv().await.unwrap()).unwrap();
        let subscription_id = reply["result"]["subscriptionId"]
            .as_str()
            .unwrap_or_else(|| {
                panic!("a subscription id is returned whatever the patterns: {reply}")
            })
            .to_string();
        Harness {
            bus,
            rx,
            subs,
            members,
            subscription_id,
            _dir: dir,
        }
    }

    fn unshare(workspace_id: &str, principal_id: &str) -> NewEvent {
        event_with(
            WORKSPACE_UPDATED,
            workspace_id,
            json!({ "changes": { "members": true, "removedPrincipalId": principal_id } }),
        )
    }

    async fn subscribe_and_publish(caller: Caller) -> Vec<String> {
        let mut h = subscribe(
            caller,
            &["ws-1"],
            json!({"eventTypes":["client:*","terminal:data","note:*"]}),
        )
        .await;
        // The transport's own `client:*` emit is a transient global event;
        // the other two are persisted through the writer task.
        let _ = h.bus.publish_transient(&event(CLIENT_CONNECTED, ""));
        h.bus.publish(&event(TERMINAL_DATA, "ws-1")).await.unwrap();
        h.bus.publish(&event(NOTE_UPDATED, "ws-1")).await.unwrap();
        let types = delivered_types(&mut h.rx).await;
        drop(h.subs);
        types
    }

    /// Delivery-time membership: an unscoped `note:*` subscription under a
    /// guest who is a member of `ws-1` only never carries `ws-2` events; a
    /// mid-stream unshare of `ws-1` stops delivery even though the
    /// subscription's own patterns exclude `workspace:updated`.
    #[tokio::test]
    async fn non_member_workspaces_are_filtered_at_delivery_and_removal_tears_down() {
        let principal_id = PrincipalId::new();
        let guest = Caller::Wire {
            principal_id: principal_id.clone(),
            is_administrator: false,
        };
        let mut h = subscribe(guest, &["ws-1"], json!({"eventTypes":["note:*"]})).await;
        h.bus.publish(&event(NOTE_UPDATED, "ws-2")).await.unwrap();
        h.bus.publish(&event(NOTE_UPDATED, "ws-1")).await.unwrap();
        h.bus.publish(&event(NOTE_UPDATED, "ws-2")).await.unwrap();
        assert_eq!(
            delivered(&mut h.rx).await,
            vec![(NOTE_UPDATED.to_string(), "ws-1".to_string())]
        );

        // Owner removes the guest: the service layer drops the membership
        // row and publishes the unshare marker (`workspace_members_remove_op`).
        h.members.lock().unwrap().remove("ws-1");
        h.bus
            .publish(&event_with(
                WORKSPACE_UPDATED,
                "ws-1",
                json!({ "changes": { "members": true, "removedPrincipalId": principal_id.as_str() } }),
            ))
            .await
            .unwrap();
        // Let the side subscription observe the unshare before the next
        // matched event (the cached `ws-1` verdict would otherwise still be
        // within its TTL).
        tokio::time::sleep(Duration::from_millis(200)).await;
        h.bus.publish(&event(NOTE_UPDATED, "ws-1")).await.unwrap();
        assert!(
            delivered(&mut h.rx).await.is_empty(),
            "no delivery after removal"
        );
        drop(h.subs);
    }

    /// Own unshare ends a subscription scoped to that `workspaceId` (like
    /// the scoped collection channels), while a global subscription stays
    /// alive for the subscriber's other member workspaces.
    #[tokio::test]
    async fn own_unshare_ends_a_scoped_subscription_but_not_a_global_one() {
        let principal_id = PrincipalId::new();
        let guest = Caller::Wire {
            principal_id: principal_id.clone(),
            is_administrator: false,
        };
        let mut scoped = subscribe(
            guest.clone(),
            &["ws-1", "ws-2"],
            json!({"eventTypes":["note:*"], "workspaceId":"ws-1"}),
        )
        .await;
        scoped
            .bus
            .publish(&event(NOTE_UPDATED, "ws-1"))
            .await
            .unwrap();
        assert_eq!(
            delivered(&mut scoped.rx).await,
            vec![(NOTE_UPDATED.to_string(), "ws-1".to_string())]
        );
        scoped
            .bus
            .publish(&unshare("ws-1", "someone-else"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            scoped.subs.forwarder_finished(&scoped.subscription_id),
            Some(false),
            "another member's unshare leaves the scoped stream live"
        );
        scoped.members.lock().unwrap().remove("ws-1");
        scoped
            .bus
            .publish(&unshare("ws-1", principal_id.as_str()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            scoped.subs.forwarder_finished(&scoped.subscription_id),
            Some(true),
            "own unshare ends the scoped stream"
        );
        drop(scoped.subs);

        let mut global =
            subscribe(guest, &["ws-1", "ws-2"], json!({"eventTypes":["note:*"]})).await;
        global.members.lock().unwrap().remove("ws-1");
        global
            .bus
            .publish(&unshare("ws-1", principal_id.as_str()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        global
            .bus
            .publish(&event(NOTE_UPDATED, "ws-1"))
            .await
            .unwrap();
        global
            .bus
            .publish(&event(NOTE_UPDATED, "ws-2"))
            .await
            .unwrap();
        assert_eq!(
            delivered(&mut global.rx).await,
            vec![(NOTE_UPDATED.to_string(), "ws-2".to_string())],
            "the global stream keeps delivering the other member workspace"
        );
        assert_eq!(
            global.subs.forwarder_finished(&global.subscription_id),
            Some(false)
        );
        drop(global.subs);
    }

    /// The removed member's own unshare event is its final notification when
    /// its patterns include `workspace:updated`; another member's unshare
    /// of the same workspace is an ordinary update to a still-member.
    #[tokio::test]
    async fn unshare_event_is_the_removed_members_final_frame() {
        let principal_id = PrincipalId::new();
        let guest = Caller::Wire {
            principal_id: principal_id.clone(),
            is_administrator: false,
        };
        let mut h = subscribe(
            guest,
            &["ws-1"],
            json!({"eventTypes":["note:*", "workspace:updated"]}),
        )
        .await;
        h.bus
            .publish(&event_with(
                WORKSPACE_UPDATED,
                "ws-1",
                json!({ "changes": { "members": true, "removedPrincipalId": "someone-else" } }),
            ))
            .await
            .unwrap();
        h.members.lock().unwrap().remove("ws-1");
        h.bus
            .publish(&event_with(
                WORKSPACE_UPDATED,
                "ws-1",
                json!({ "changes": { "members": true, "removedPrincipalId": principal_id.as_str() } }),
            ))
            .await
            .unwrap();
        h.bus.publish(&event(NOTE_UPDATED, "ws-1")).await.unwrap();
        assert_eq!(
            delivered(&mut h.rx).await,
            vec![
                (WORKSPACE_UPDATED.to_string(), "ws-1".to_string()),
                (WORKSPACE_UPDATED.to_string(), "ws-1".to_string()),
            ]
        );
        drop(h.subs);
    }

    #[tokio::test]
    async fn non_administrator_receives_only_allowlisted_types() {
        let guest = Caller::Wire {
            principal_id: PrincipalId::new(),
            is_administrator: false,
        };
        assert_eq!(
            subscribe_and_publish(guest).await,
            vec![NOTE_UPDATED.to_string()]
        );
    }

    #[tokio::test]
    async fn administrator_receives_everything_named() {
        let owner = Caller::Wire {
            principal_id: PrincipalId::new(),
            is_administrator: true,
        };
        assert_eq!(
            subscribe_and_publish(owner).await,
            vec![
                CLIENT_CONNECTED.to_string(),
                TERMINAL_DATA.to_string(),
                NOTE_UPDATED.to_string()
            ]
        );
    }

    /// The reverse registry's transition → event-type mapping resolves to
    /// taxonomy members that the allowlist refuses, so the exhaustive golden
    /// in `intent-core/tests/events.rs` covers the transport's own emits.
    #[test]
    fn client_transitions_publish_taxonomy_types_outside_the_allowlist() {
        use crate::reverse::{ClientTransition, ReverseClientIdentity};
        use intent_core::{ClientHostInfo, ClientId};

        let identity = ReverseClientIdentity {
            client_id: ClientId::from_string("c-1"),
            name: None,
            capabilities: json!({}),
            host: ClientHostInfo {
                hostname: None,
                pretty_hostname: None,
                device_kind: None,
            },
        };
        for transition in [
            ClientTransition::Connected(identity.clone()),
            ClientTransition::Disconnected(identity),
        ] {
            let ty = transition.event_type();
            assert!(intent_core::events::is_known_event_type(ty), "{ty}");
            assert!(!intent_core::events::is_collaborator_event_type(ty), "{ty}");
        }
    }
}
