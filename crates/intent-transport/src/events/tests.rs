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

/// Emit-path taxonomy golden (multiplayer w3). The allowlist is default-deny
/// at delivery, so a literal emitted outside the taxonomy is silently
/// owner-only rather than a routing bug — this scan makes the omission fail
/// mechanically instead: every event-shaped string literal
/// (`ns:name[:sub…]`) in the **non-test** source of `intent-services` and
/// `intent-transport` must be a taxonomy member (`ALL_EVENT_TYPES`, which
/// the core golden in `intent-core/tests/events.rs` classifies exactly once
/// as collaborator-visible or owner-only), a frozen pre-taxonomy emit, or a
/// frozen non-event literal that merely shares the shape.
///
/// Test code is excluded by construction: files named `tests.rs`, modules
/// declared as `#[cfg(test)] mod name;` (their file / directory), and every
/// `#[cfg(test)]` item (attribute through the `;` or the `}` closing its
/// body). Comments and char literals never count.
mod emit_path_taxonomy {
    use std::collections::HashSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    use intent_core::events::{is_collaborator_event_type, is_known_event_type};

    /// Emitted through the bus before the taxonomy existed and deliberately
    /// left off `ALL_EVENT_TYPES` (owner-only by default-deny; see the core
    /// golden's `collaborator_predicate_is_default_deny`). Adding a new
    /// entry here is the wrong fix for a new emit — add it to the taxonomy.
    const PRE_TAXONOMY_EMITS: &[&str] = &["sandbox:cow:created", "sandbox:cow:merged"];

    /// Production literals that have the `ns:name` shape but are not event
    /// types (each listed with the file that carries it). Frozen: a new
    /// entry needs the same review as a new emit.
    const NOT_EVENT_TYPES: &[&str] = &[
        // agent_ops.rs — `agent.delegate` batch dispositions.
        "held:blocked-on-deps",
        "held:conflict",
        // agent_ops.rs — pseudo-type of the wake metadata `reportToParent`
        // delivers to the parent agent; never published on the bus.
        "agent:reportToParent",
        // model_catalog.rs — models-cache version key.
        "antigravity-executable-v1:missing",
    ];

    /// `ns:name`, `ns:name:sub`, … — segments of `[A-Za-z0-9._-]`, each
    /// led by an alphanumeric and the first by a letter. Deliberately wider
    /// than the taxonomy's own spelling (`gitRoot:`, `prMonitor:`,
    /// `mcp.servers:`, `displayStatus-changed`): an emitted typo such as
    /// `note:new_event` or `Note:Updated` must reach the classifier and
    /// fail there, not be filtered out here as "not an event".
    fn emit_shaped(lit: &str) -> bool {
        let mut segments = lit.split(':');
        let Some(first) = segments.next() else {
            return false;
        };
        let segment_ok = |s: &str| {
            s.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
        };
        let mut rest = 0;
        for s in segments {
            if !segment_ok(s) {
                return false;
            }
            rest += 1;
        }
        rest > 0 && segment_ok(first) && first.starts_with(|c: char| c.is_ascii_alphabetic())
    }

    /// `Some(hashes)` when a raw string literal starts at `i`.
    fn raw_string_hashes(chars: &[char], i: usize) -> Option<usize> {
        if i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_') {
            return None;
        }
        let mut j = i;
        if matches!(chars.get(j), Some('b' | 'c')) {
            j += 1;
        }
        if chars.get(j) != Some(&'r') {
            return None;
        }
        j += 1;
        let mut hashes = 0;
        while chars.get(j) == Some(&'#') {
            hashes += 1;
            j += 1;
        }
        (chars.get(j) == Some(&'"')).then_some(hashes)
    }

    /// One source file, lexed: the string literals outside `#[cfg(test)]`
    /// items (with their line) and the module names declared test-only by a
    /// `#[cfg(test)] mod name;` item.
    struct Scanned {
        literals: Vec<(usize, String)>,
        test_only_mods: Vec<String>,
    }

    fn scan_source(src: &str) -> Scanned {
        let chars: Vec<char> = src.chars().collect();
        let len = chars.len();
        let mut literals = Vec::new();
        let mut test_only_mods = Vec::new();
        let mut line = 1usize;
        let mut i = 0usize;
        // Recent code text (cleared at `;` / `{` / `}` / `]`) so the
        // attribute is matched whole, whitespace-insensitively.
        let mut code = String::new();
        // `#[cfg(test)]` item skipping: ends at a `;` before any `{`, else
        // at the `}` that brings the depth back to zero.
        let mut skipping = false;
        let mut skip_depth = 0usize;
        let mut skip_opened = false;
        let mut skipped = String::new();
        while i < len {
            let c = chars[i];
            if c == '/' && chars.get(i + 1) == Some(&'/') {
                while i < len && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            if c == '/' && chars.get(i + 1) == Some(&'*') {
                let mut depth = 0usize;
                while i < len {
                    if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                        depth += 1;
                        i += 2;
                    } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        if chars[i] == '\n' {
                            line += 1;
                        }
                        i += 1;
                    }
                }
                continue;
            }
            if let Some(hashes) = raw_string_hashes(&chars, i) {
                let start_line = line;
                while chars[i] != '"' {
                    i += 1;
                }
                i += 1;
                let mut text = String::new();
                loop {
                    if i >= len {
                        break;
                    }
                    if chars[i] == '"' && (1..=hashes).all(|k| chars.get(i + k) == Some(&'#')) {
                        i += 1 + hashes;
                        break;
                    }
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    text.push(chars[i]);
                    i += 1;
                }
                if !skipping {
                    literals.push((start_line, text));
                }
                continue;
            }
            if c == '"' {
                let start_line = line;
                i += 1;
                let mut text = String::new();
                while i < len && chars[i] != '"' {
                    if chars[i] == '\\' {
                        text.push(chars[i]);
                        i += 1;
                        if i >= len {
                            break;
                        }
                    }
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    text.push(chars[i]);
                    i += 1;
                }
                i += 1;
                if !skipping {
                    literals.push((start_line, text));
                }
                continue;
            }
            if c == '\'' {
                if chars.get(i + 1) == Some(&'\\') {
                    i += 2;
                    while i < len && chars[i] != '\'' {
                        i += 1;
                    }
                    i += 1;
                } else if chars.get(i + 2) == Some(&'\'') {
                    i += 3;
                } else {
                    i += 1;
                }
                continue;
            }
            if c == '\n' {
                line += 1;
            }
            if skipping {
                skipped.push(c);
                match c {
                    '{' => {
                        skip_depth += 1;
                        skip_opened = true;
                    }
                    '}' => {
                        skip_depth = skip_depth.saturating_sub(1);
                        if skip_opened && skip_depth == 0 {
                            skipping = false;
                        }
                    }
                    ';' if !skip_opened => {
                        skipping = false;
                        let mut words = skipped.split_whitespace().peekable();
                        while let Some(w) = words.next() {
                            if w == "mod" {
                                if let Some(name) = words.next() {
                                    test_only_mods.push(name.trim_end_matches(';').to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
                i += 1;
                continue;
            }
            code.push(c);
            match c {
                ']' => {
                    let compact: String = code.chars().filter(|c| !c.is_whitespace()).collect();
                    if compact.ends_with("#[cfg(test)]") {
                        skipping = true;
                        skip_depth = 0;
                        skip_opened = false;
                        skipped.clear();
                    }
                    code.clear();
                }
                ';' | '{' | '}' => code.clear(),
                _ => {}
            }
            i += 1;
        }
        Scanned {
            literals,
            test_only_mods,
        }
    }

    /// Where `mod name;` in `declaring` resolves: `dir/name.rs` or
    /// `dir/name/` for a `lib.rs` / `mod.rs` / `main.rs`, else
    /// `dir/<stem>/name.rs` or `dir/<stem>/name/`.
    fn module_base(declaring: &Path) -> PathBuf {
        let dir = declaring.parent().unwrap_or(Path::new("."));
        match declaring.file_stem().and_then(|s| s.to_str()) {
            Some("lib" | "mod" | "main") | None => dir.to_path_buf(),
            Some(stem) => dir.join(stem),
        }
    }

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        let mut entries: Vec<_> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    /// Every emit-shaped literal in the crate's non-test source, as
    /// `(file, line, literal)`.
    fn emit_shaped_literals(src_dir: &Path) -> Vec<(PathBuf, usize, String)> {
        let mut files = Vec::new();
        rust_files(src_dir, &mut files);
        let scanned: Vec<(PathBuf, Scanned)> = files
            .into_iter()
            .filter(|p| p.file_name().is_some_and(|n| n != "tests.rs"))
            .map(|p| {
                let src =
                    fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
                (p, scan_source(&src))
            })
            .collect();
        let mut test_only: Vec<PathBuf> = Vec::new();
        for (path, s) in &scanned {
            let base = module_base(path);
            for name in &s.test_only_mods {
                test_only.push(base.join(format!("{name}.rs")));
                test_only.push(base.join(name));
            }
        }
        let is_test_only = |p: &Path| {
            test_only
                .iter()
                .any(|t| p == t || (t.extension().is_none() && p.starts_with(t)))
        };
        let mut out = Vec::new();
        for (path, s) in scanned {
            if is_test_only(&path) {
                continue;
            }
            for (line, lit) in s.literals {
                if emit_shaped(&lit) {
                    out.push((path.clone(), line, lit));
                }
            }
        }
        out
    }

    /// The literals the golden refuses: emit-shaped, not in the taxonomy,
    /// not a frozen exception.
    fn unclassified<'a>(literals: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
        literals
            .into_iter()
            .filter(|lit| {
                !is_known_event_type(lit)
                    && !PRE_TAXONOMY_EMITS.contains(lit)
                    && !NOT_EVENT_TYPES.contains(lit)
            })
            .collect()
    }

    fn crate_src(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .join(name)
            .join("src")
    }

    #[test]
    fn every_emitted_literal_is_classified_in_the_taxonomy() {
        let mut seen: HashSet<String> = HashSet::new();
        let mut failures = Vec::new();
        for krate in ["intent-services", "intent-transport"] {
            for (path, line, lit) in emit_shaped_literals(&crate_src(krate)) {
                seen.insert(lit.clone());
                if !unclassified([lit.as_str()]).is_empty() {
                    failures.push(format!("{}:{line}: \"{lit}\"", path.display()));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "event-type literals emitted outside the taxonomy — add each to \
             `intent_core::events::ALL_EVENT_TYPES` and classify it in \
             `COLLABORATOR_EVENT_TYPES` or the owner-only golden \
             (crates/intent-core/tests/events.rs):\n  {}",
            failures.join("\n  ")
        );
        // The scan reaches real emit paths (a silently empty scan would pass
        // vacuously), and the frozen exceptions are still live: a retired
        // entry must leave the lists, not linger.
        assert!(
            seen.iter().any(|lit| is_known_event_type(lit)),
            "the scan found no taxonomy literal at all: {seen:?}"
        );
        for lit in PRE_TAXONOMY_EMITS.iter().chain(NOT_EVENT_TYPES) {
            assert!(
                seen.contains(*lit),
                "`{lit}` is listed as an exception but no longer emitted"
            );
        }
        for lit in PRE_TAXONOMY_EMITS {
            assert!(
                !is_known_event_type(lit) && !is_collaborator_event_type(lit),
                "`{lit}` joined the taxonomy — drop it from PRE_TAXONOMY_EMITS"
            );
        }
    }

    /// Most emit paths name their type through an `intent_core::events`
    /// constant rather than a literal, so the constant table is the other
    /// half of the emit surface: every event-shaped `pub const … : &str`
    /// in `intent-core/src/events.rs` must be a taxonomy member too (the
    /// `*_PREFIX` constants end in `:` and are not event-shaped).
    #[test]
    fn every_event_constant_is_in_the_taxonomy() {
        let path = crate_src("intent-core").join("events.rs");
        let src =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let mut constants = Vec::new();
        for (idx, line) in src.lines().enumerate() {
            let Some(rest) = line.trim_start().strip_prefix("pub const ") else {
                continue;
            };
            let Some((_, value)) = rest.split_once(": &str = \"") else {
                continue;
            };
            let Some((lit, _)) = value.split_once('"') else {
                continue;
            };
            if emit_shaped(lit) {
                constants.push((idx + 1, lit.to_string()));
            }
        }
        assert!(
            constants.len() > 50,
            "expected the constant table, got {constants:?}"
        );
        let missing: Vec<String> = constants
            .iter()
            .filter(|(_, lit)| !is_known_event_type(lit))
            .map(|(line, lit)| format!("{}:{line}: \"{lit}\"", path.display()))
            .collect();
        assert!(
            missing.is_empty(),
            "event constants missing from ALL_EVENT_TYPES:\n  {}",
            missing.join("\n  ")
        );
    }

    /// Negative control: a new literal on an emit path that is not in the
    /// taxonomy is what the golden refuses — whether it is a new name in an
    /// existing family, a new family altogether, or a misspelling of a
    /// known type (underscore, upper case) that would otherwise be
    /// owner-only by default-deny — while the same literal inside a
    /// `#[cfg(test)]` module, a comment, or a non-event shape is ignored.
    #[test]
    fn unclassified_literal_fails_the_golden() {
        let src = r#"
            use intent_core::events::NOTE_UPDATED;
            // "note:in-a-comment" never counts.
            /* nor "agent:in-a-block" */
            pub fn emit(bus: &Bus) {
                bus.publish("note:updated");
                bus.publish("note:bogus-thing");
                bus.publish("brandnew:emitted");
                bus.publish(Event { event_type: "note:new_event".to_string() });
                bus.publish("Note:Updated");
                bus.publish("AGENT:IDLE");
                let _ = (
                    '"', "not an event", "a:", ":b", "x:{y}", "note:",
                    "HEAD:.gitmodules", "127.0.0.1:0", "_ns:name", "note:-x",
                    "note:up dated", "http://x", "a::b",
                );
            }
            #[cfg(test)]
            mod tests;
            #[cfg(test)]
            mod inline {
                fn f() { publish("note:only-in-tests"); }
            }
            #[cfg(test)]
            fn helper() -> &'static str { "task:only-in-tests" }
            pub fn after() -> &'static str { "agent:idle" }
        "#;
        let scanned = scan_source(src);
        let shaped: Vec<&str> = scanned
            .literals
            .iter()
            .map(|(_, l)| l.as_str())
            .filter(|l| emit_shaped(l))
            .collect();
        assert_eq!(
            shaped,
            vec![
                "note:updated",
                "note:bogus-thing",
                "brandnew:emitted",
                "note:new_event",
                "Note:Updated",
                "AGENT:IDLE",
                "agent:idle"
            ]
        );
        assert_eq!(
            unclassified(shaped.iter().copied()),
            vec![
                "note:bogus-thing",
                "brandnew:emitted",
                "note:new_event",
                "Note:Updated",
                "AGENT:IDLE"
            ]
        );
        assert_eq!(scanned.test_only_mods, vec!["tests".to_string()]);
        assert!(unclassified(["note:updated", "agent:idle"]).is_empty());

        // The frozen exceptions pass the classifier but nothing else does.
        assert!(unclassified(PRE_TAXONOMY_EMITS.iter().copied()).is_empty());
        assert!(unclassified(NOT_EVENT_TYPES.iter().copied()).is_empty());
        assert_eq!(
            unclassified(["sandbox:cow:discarded"]),
            vec!["sandbox:cow:discarded"]
        );
    }

    /// A `#[cfg(test)] mod name;` declaration excludes the module's file (or
    /// directory) from the crate scan, so test-only sibling files such as
    /// `v1_goldens.rs` never count as emit paths.
    #[test]
    fn test_only_module_files_are_excluded() {
        let dir = tempfile::Builder::new()
            .prefix("emit-path-golden-")
            .tempdir()
            .expect("tempdir");
        fs::write(
            dir.path().join("lib.rs"),
            "mod real;\n#[cfg(test)]\nmod goldens;\n#[cfg(test)]\nmod nested;\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("real.rs"),
            "pub fn f() -> &'static str { \"note:real-emit\" }\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("goldens.rs"),
            "fn g() -> &'static str { \"note:golden-only\" }\n",
        )
        .unwrap();
        fs::create_dir(dir.path().join("nested")).unwrap();
        fs::write(
            dir.path().join("nested").join("mod.rs"),
            "fn n() -> &'static str { \"note:nested-only\" }\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("tests.rs"),
            "fn t() -> &'static str { \"note:tests-rs-only\" }\n",
        )
        .unwrap();
        let found: Vec<String> = emit_shaped_literals(dir.path())
            .into_iter()
            .map(|(_, _, lit)| lit)
            .collect();
        assert_eq!(found, vec!["note:real-emit".to_string()]);
    }
}
