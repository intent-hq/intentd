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
        json!(7),
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
        let v = parse(&error_frame(json!(1), code, "boom"));
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
