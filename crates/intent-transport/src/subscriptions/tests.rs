//! Unit tests for the pure `subscription.push` fast-path helpers.

use super::*;

fn parse(s: &str) -> Value {
    serde_json::from_str(s).unwrap()
}

#[test]
fn classify_routes_note_subscribe_and_unsubscribe() {
    let sub = parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.subscribe","params":{"workspaceId":"ws-1"}}"#,
    );
    assert!(matches!(
        classify(&sub),
        Some(SubFastPath::Subscribe {
            channel: Channel::Note,
            ..
        })
    ));

    let unsub = parse(
        r#"{"jsonrpc":"2.0","id":2,"method":"note.unsubscribe","params":{"subscriptionId":"ws-sub-1"}}"#,
    );
    assert!(matches!(
        classify(&unsub),
        Some(SubFastPath::Unsubscribe { .. })
    ));
}

#[test]
fn classify_routes_tb5_channels() {
    let cases = [
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"task.subscribe","params":{"workspaceId":"w"}}"#,
            Channel::Task,
        ),
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"workspace.subscribe","params":{}}"#,
            Channel::Workspace,
        ),
        (
            r#"{"jsonrpc":"2.0","id":1,"method":"comment.subscribe","params":{"workspaceId":"w","noteId":"n"}}"#,
            Channel::Comment,
        ),
    ];
    for (frame, want) in cases {
        match classify(&parse(frame)) {
            Some(SubFastPath::Subscribe { channel, .. }) => assert_eq!(channel, want),
            _ => panic!("expected Subscribe({want:?}) for {frame}"),
        }
    }
    for method in [
        "task.unsubscribe",
        "workspace.unsubscribe",
        "comment.unsubscribe",
    ] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"subscriptionId":"s"}}}}"#
        );
        assert!(matches!(
            classify(&parse(&frame)),
            Some(SubFastPath::Unsubscribe { .. })
        ));
    }
}

#[test]
fn classify_disambiguates_agent_channel_from_deprecated_alias() {
    // No `eventTypes` → the new collection channel fast-path.
    match classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.subscribe","params":{"workspaceId":"w"}}"#,
    )) {
        Some(SubFastPath::Subscribe {
            channel: Channel::Agent,
            ..
        }) => {}
        _ => panic!("expected Subscribe(Agent)"),
    }
    // `eventTypes` present → the deprecated service alias; fall through to router.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.subscribe","params":{"workspaceId":"w","eventTypes":["agent:*"]}}"#
    ))
    .is_none());
    // Bare `{ subscriptionId }` → our fast-path unsubscribe.
    assert!(matches!(
        classify(&parse(
            r#"{"jsonrpc":"2.0","id":1,"method":"agent.unsubscribe","params":{"subscriptionId":"s"}}"#
        )),
        Some(SubFastPath::Unsubscribe { .. })
    ));
    // `workspaceId` present → the deprecated alias; fall through to router.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.unsubscribe","params":{"workspaceId":"w","subscriptionId":"s"}}"#
    ))
    .is_none());
}

#[test]
fn classify_routes_chat_channel() {
    // `chat.subscribe` is a distinct method name; it routes to the chat channel
    // without colliding with the `agent.*` alias/deprecated paths.
    match classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"chat.subscribe","params":{"agentId":"agent-1"}}"#,
    )) {
        Some(SubFastPath::Subscribe {
            channel: Channel::Chat,
            ..
        }) => {}
        _ => panic!("expected Subscribe(Chat)"),
    }
    assert!(matches!(
        classify(&parse(
            r#"{"jsonrpc":"2.0","id":2,"method":"chat.unsubscribe","params":{"subscriptionId":"ws-sub-1"}}"#
        )),
        Some(SubFastPath::Unsubscribe { .. })
    ));
}

#[test]
fn chat_params_require_agent_id() {
    let ok = parse(r#"{"agentId":"agent-1","replaceGroup":"chat:agent-1"}"#);
    let p = parse_chat_subscribe_params(ok.as_object().unwrap()).unwrap();
    assert_eq!(p.agent_id, "agent-1");
    assert_eq!(p.replace_group.as_deref(), Some("chat:agent-1"));

    for bad in [r#"{}"#, r#"{"agentId":""}"#, r#"{"agentId":5}"#] {
        let v = parse(bad);
        let err = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("agentId is required"));
    }
}

#[test]
fn chat_channel_tails_only_stream_family() {
    let chat = channel_event_types(Channel::Chat);
    assert_eq!(
        chat,
        vec![
            "agent:stream:chunk".to_string(),
            "agent:tool:call".to_string(),
            "agent:stream:end".to_string(),
        ]
    );
    assert!(!channel_is_global(Channel::Chat));
}

#[test]
fn comment_params_require_workspace_and_note() {
    let ok = parse(r#"{"workspaceId":"w","noteId":"n","replaceGroup":"comment:n"}"#);
    let p = parse_comment_subscribe_params(ok.as_object().unwrap()).unwrap();
    assert_eq!(p.workspace_id, "w");
    assert_eq!(p.note_id, "n");
    assert_eq!(p.replace_group.as_deref(), Some("comment:n"));

    for bad in [r#"{}"#, r#"{"workspaceId":"w"}"#, r#"{"noteId":"n"}"#] {
        let v = parse(bad);
        assert!(parse_comment_subscribe_params(v.as_object().unwrap()).is_err());
    }
}

#[test]
fn workspace_params_are_global() {
    let v = parse(r#"{"replaceGroup":"workspaces"}"#);
    let p = parse_workspace_subscribe_params(v.as_object().unwrap()).unwrap();
    assert_eq!(p.replace_group.as_deref(), Some("workspaces"));
    // No workspaceId needed.
    let empty = parse(r#"{}"#);
    assert!(parse_workspace_subscribe_params(empty.as_object().unwrap()).is_ok());
}

#[test]
fn channel_event_types_exclude_chat_stream() {
    let agent = channel_event_types(Channel::Agent);
    assert!(agent.iter().any(|t| t == "agent:deleted"));
    assert!(
        !agent.iter().any(|t| t.starts_with("agent:stream:")),
        "chat-stream family must be excluded (design R7)"
    );
    assert!(channel_is_global(Channel::Workspace));
    assert!(!channel_is_global(Channel::Task));
}

#[test]
fn classify_falls_through_for_other_methods_and_bad_envelope() {
    // The legacy firehose method is NOT a subscription channel.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["note:*"]}}"#
    ))
    .is_none());
    // A request/response note method falls through to the dispatcher.
    assert!(classify(&parse(r#"{"jsonrpc":"2.0","id":1,"method":"note.list"}"#)).is_none());
    // Wrong jsonrpc version.
    assert!(classify(&parse(
        r#"{"jsonrpc":"1.0","id":1,"method":"note.subscribe"}"#
    ))
    .is_none());
    // Bad id type (object) → fall through so the dispatcher returns -32600.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":{},"method":"note.subscribe"}"#
    ))
    .is_none());
    // Not an object.
    assert!(classify(&parse("[]")).is_none());
}

#[test]
fn classify_notification_has_no_id() {
    let notif =
        parse(r#"{"jsonrpc":"2.0","method":"note.subscribe","params":{"workspaceId":"w"}}"#);
    match classify(&notif) {
        Some(SubFastPath::Subscribe { id, .. }) => {
            assert!(!id.present);
            assert_eq!(id.echo, Value::Null);
        }
        _ => panic!("expected subscribe"),
    }
}

#[test]
fn subscribe_params_require_workspace_id() {
    let ok = parse(r#"{"workspaceId":"ws-1","replaceGroup":"note:ws-1","sinceSeq":null}"#);
    let p = parse_subscribe_params(ok.as_object().unwrap()).unwrap();
    assert_eq!(p.workspace_id, "ws-1");
    assert_eq!(p.replace_group.as_deref(), Some("note:ws-1"));

    for bad in [r#"{}"#, r#"{"workspaceId":""}"#, r#"{"workspaceId":5}"#] {
        let v = parse(bad);
        let err = parse_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("workspaceId is required"));
    }
}

#[test]
fn snapshot_push_matches_protocol() {
    let snapshot = json!([{ "id": "spec", "title": "Spec" }]);
    let frame = build_snapshot_push("ws-sub-1", 0, &snapshot);
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["method"], "subscription.push");
    assert_eq!(v["params"]["subscriptionId"], "ws-sub-1");
    assert_eq!(v["params"]["kind"], "snapshot");
    assert_eq!(v["params"]["seq"], 0);
    assert_eq!(v["params"]["snapshot"][0]["id"], "spec");
    assert!(v["params"].get("delta").is_none());
}

#[test]
fn delta_push_matches_protocol() {
    let delta = json!({ "updated": [{ "id": "spec", "title": "Spec v2" }], "removedIds": ["n2"] });
    let frame = build_delta_push("ws-sub-7", 3, &delta);
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["method"], "subscription.push");
    assert_eq!(v["params"]["subscriptionId"], "ws-sub-7");
    assert_eq!(v["params"]["kind"], "delta");
    assert_eq!(v["params"]["seq"], 3);
    assert_eq!(v["params"]["delta"]["updated"][0]["id"], "spec");
    assert_eq!(v["params"]["delta"]["removedIds"][0], "n2");
    assert!(v["params"].get("snapshot").is_none());
}
