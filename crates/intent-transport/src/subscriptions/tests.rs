//! Unit tests for the pure `subscription.push` fast-path helpers.

use super::*;
use intent_core::{ActorType, EventActor};

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

// --- channel filter / predicate matrix -------------------------------------

#[test]
fn channel_event_types_full_matrix() {
    // Note channel — only the three note-CUD types.
    assert_eq!(
        channel_event_types(Channel::Note),
        vec![
            "note:created".to_string(),
            "note:updated".to_string(),
            "note:deleted".to_string(),
        ]
    );
    // Task channel — the note triad plus task:status-changed.
    let task = channel_event_types(Channel::Task);
    assert!(task.contains(&"note:created".to_string()));
    assert!(task.contains(&"note:updated".to_string()));
    assert!(task.contains(&"note:deleted".to_string()));
    assert!(task.contains(&"task:status-changed".to_string()));
    assert_eq!(task.len(), 4);
    // Workspace channel — global, includes the workspace lifecycle + PR family.
    let ws = channel_event_types(Channel::Workspace);
    for t in [
        "workspace:created",
        "workspace:updated",
        "workspace:deleted",
        "workspace:activity-changed",
        "workspace:attention-changed",
        "pr:linked",
        "pr:updated",
        "pr:unlinked",
    ] {
        assert!(ws.iter().any(|s| s == t), "workspace missing {t}");
    }
    assert_eq!(ws.len(), 8);
    // Comment channel — single type.
    assert_eq!(
        channel_event_types(Channel::Comment),
        vec!["comment:added".to_string()]
    );
}

#[test]
fn channel_is_global_only_workspace() {
    assert!(channel_is_global(Channel::Workspace));
    assert!(!channel_is_global(Channel::Note));
    assert!(!channel_is_global(Channel::Task));
    assert!(!channel_is_global(Channel::Agent));
    assert!(!channel_is_global(Channel::Comment));
    assert!(!channel_is_global(Channel::Chat));
}

// --- classify falls-through edges -----------------------------------------

#[test]
fn classify_rejects_missing_or_wrong_method() {
    // No method at all.
    assert!(classify(&parse(r#"{"jsonrpc":"2.0","id":1}"#)).is_none());
    // Method is not a string.
    assert!(classify(&parse(r#"{"jsonrpc":"2.0","id":1,"method":5}"#)).is_none());
    // Unknown channel method.
    assert!(classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"banana.subscribe"}"#
    ))
    .is_none());
}

#[test]
fn classify_carries_id_echo_for_string_and_number() {
    match classify(&parse(
        r#"{"jsonrpc":"2.0","id":"abc","method":"note.subscribe","params":{"workspaceId":"w"}}"#,
    )) {
        Some(SubFastPath::Subscribe { id, .. }) => {
            assert!(id.present);
            assert_eq!(id.echo, Value::String("abc".to_string()));
        }
        _ => panic!("expected Subscribe"),
    }
    match classify(&parse(
        r#"{"jsonrpc":"2.0","id":null,"method":"note.unsubscribe","params":{"subscriptionId":"s"}}"#,
    )) {
        Some(SubFastPath::Unsubscribe { id, .. }) => {
            assert!(id.present);
            assert_eq!(id.echo, Value::Null);
        }
        _ => panic!("expected Unsubscribe"),
    }
}

#[test]
fn classify_drops_non_object_params() {
    // `params: 5` is not an object → treated as empty map. The classifier still
    // routes the method; later param parsing will reject the missing fields.
    match classify(&parse(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.subscribe","params":5}"#,
    )) {
        Some(SubFastPath::Subscribe { params, .. }) => assert!(params.is_empty()),
        _ => panic!("expected Subscribe with empty params"),
    }
}

// --- next_block_id ---------------------------------------------------------

#[test]
fn next_block_id_increments_trailing_index() {
    assert_eq!(
        next_block_id("msg-1:5").as_deref(),
        Some("msg-1:6"),
        "increments after the last `:`",
    );
    assert_eq!(next_block_id("foo:bar:0").as_deref(), Some("foo:bar:1"));
}

#[test]
fn next_block_id_rejects_missing_colon_or_non_numeric() {
    assert_eq!(next_block_id("no-colon"), None);
    assert_eq!(next_block_id("msg-1:abc"), None);
    assert_eq!(
        next_block_id("msg-1:-1"),
        None,
        "negative reject by parse()"
    );
}

// --- ChatDeltaState — pure (sync) paths -----------------------------------

fn agent() -> AgentId {
    AgentId::from("agent-1")
}

fn chunk_event(message_id: &str, block_id: &str, block_type: &str, content: Value) -> Event {
    Event {
        id: "evt-1".into(),
        event_type: AGENT_STREAM_CHUNK.to_string(),
        timestamp: now_iso(),
        workspace_id: WorkspaceId::from("w"),
        session_id: Some(message_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        data: json!({
            "messageId": message_id,
            "blockId": block_id,
            "blockType": block_type,
            "content": content,
        }),
    }
}

fn tool_event(
    message_id: &str,
    block_id: &str,
    tool_call_id: &str,
    status: &str,
    output: Option<Value>,
) -> Event {
    let mut data = serde_json::Map::new();
    data.insert("messageId".into(), Value::String(message_id.into()));
    data.insert("blockId".into(), Value::String(block_id.into()));
    data.insert("toolCallId".into(), Value::String(tool_call_id.into()));
    data.insert("status".into(), Value::String(status.into()));
    data.insert("toolName".into(), Value::String("shell".into()));
    data.insert("input".into(), json!({ "cmd": "ls" }));
    data.insert("toolKind".into(), Value::String("execute".into()));
    if let Some(out) = output {
        data.insert("output".into(), out);
    }
    Event {
        id: "evt-2".into(),
        event_type: AGENT_TOOL_CALL.to_string(),
        timestamp: now_iso(),
        workspace_id: WorkspaceId::from("w"),
        session_id: Some(message_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        data: Value::Object(data),
    }
}

#[test]
fn chat_chunk_delta_accumulates_text_and_flips_added_to_updated() {
    let mut s = ChatDeltaState::new(&agent());
    // First text chunk for blk-0 → `added`, with the full text so far.
    let d1 = s
        .chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
        .expect("text chunk delta");
    assert_eq!(d1["added"][0]["block"]["text"], "Hello");
    assert_eq!(d1["added"][0]["block"]["type"], "text");
    assert_eq!(d1["added"][0]["messageId"], "msg-1");
    assert_eq!(d1["added"][0]["agentId"], "agent-1");
    assert!(d1["updated"].as_array().unwrap().is_empty());
    // Second text chunk for same block → `updated`, with the FULL accumulated text.
    let d2 = s
        .chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
        .expect("text chunk delta 2");
    assert!(d2["added"].as_array().unwrap().is_empty());
    assert_eq!(d2["updated"][0]["block"]["text"], "Hello, world");
}

#[test]
fn chat_chunk_delta_handles_non_text_block() {
    let mut s = ChatDeltaState::new(&agent());
    let d = s
        .chunk_delta(&chunk_event(
            "msg-2",
            "msg-2:0",
            "image",
            json!({ "url": "data:image/png;base64,..." }),
        ))
        .expect("non-text chunk delta");
    let block = &d["added"][0]["block"];
    assert_eq!(block["id"], "msg-2:0");
    assert_eq!(block["url"], "data:image/png;base64,...");
    assert!(
        block.get("type").is_none(),
        "non-text content passes through verbatim"
    );
}

#[test]
fn chat_chunk_delta_returns_none_for_missing_required_fields() {
    let mut s = ChatDeltaState::new(&agent());
    // Missing blockId.
    let mut e = chunk_event("msg-3", "ignored", "text", json!("x"));
    e.data.as_object_mut().unwrap().remove("blockId");
    assert!(s.chunk_delta(&e).is_none());
    // Missing messageId.
    let mut e = chunk_event("msg-3", "msg-3:0", "text", json!("x"));
    e.data.as_object_mut().unwrap().remove("messageId");
    assert!(s.chunk_delta(&e).is_none());
    // Missing content.
    let mut e = chunk_event("msg-3", "msg-3:0", "text", json!("x"));
    e.data.as_object_mut().unwrap().remove("content");
    assert!(s.chunk_delta(&e).is_none());
}

#[test]
fn chat_tool_delta_started_emits_only_use_block() {
    let mut s = ChatDeltaState::new(&agent());
    let d = s
        .tool_delta(&tool_event("msg-4", "msg-4:0", "tc-1", "started", None))
        .expect("tool started delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 1, "no result block while still started");
    assert_eq!(added[0]["block"]["type"], "tool_use");
    assert_eq!(added[0]["block"]["toolCallId"], "tc-1");
    assert_eq!(added[0]["block"]["metadata"]["status"], "started");
    assert_eq!(added[0]["block"]["metadata"]["toolKind"], "execute");
    assert!(d["removedIds"].as_array().unwrap().is_empty());
}

#[test]
fn chat_tool_delta_completed_emits_use_and_result_blocks() {
    let mut s = ChatDeltaState::new(&agent());
    let d = s
        .tool_delta(&tool_event(
            "msg-5",
            "msg-5:0",
            "tc-9",
            "completed",
            Some(json!("hello\nworld")),
        ))
        .expect("tool completed delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 2, "tool_use + tool_result");
    assert_eq!(added[0]["block"]["type"], "tool_use");
    assert_eq!(added[0]["block"]["id"], "msg-5:0");
    assert_eq!(added[1]["block"]["type"], "tool_result");
    assert_eq!(
        added[1]["block"]["id"], "msg-5:1",
        "result follows use by +1"
    );
    assert_eq!(added[1]["block"]["tool_use_id"], "tc-9");
    assert_eq!(added[1]["block"]["is_error"], false);
}

#[test]
fn chat_tool_delta_error_status_marks_is_error_true() {
    let mut s = ChatDeltaState::new(&agent());
    let d = s
        .tool_delta(&tool_event(
            "msg-6",
            "msg-6:2",
            "tc-7",
            "error",
            Some(json!({ "message": "boom" })),
        ))
        .expect("tool error delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 2);
    assert_eq!(added[1]["block"]["type"], "tool_result");
    assert_eq!(added[1]["block"]["id"], "msg-6:3");
    assert_eq!(added[1]["block"]["is_error"], true);
}

#[test]
fn chat_tool_delta_completed_without_output_only_emits_use_block() {
    let mut s = ChatDeltaState::new(&agent());
    let d = s
        .tool_delta(&tool_event("msg-7", "msg-7:0", "tc-x", "completed", None))
        .expect("completed-no-output delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 1, "no output → no synthetic result block");
    assert_eq!(added[0]["block"]["type"], "tool_use");
}

#[test]
fn chat_tool_delta_returns_none_for_missing_required_fields() {
    let mut s = ChatDeltaState::new(&agent());
    let base = || tool_event("msg-8", "msg-8:0", "tc-1", "started", None);
    for key in ["blockId", "messageId", "toolCallId"] {
        let mut e = base();
        e.data.as_object_mut().unwrap().remove(key);
        assert!(s.tool_delta(&e).is_none(), "missing {key} must yield None");
    }
}

#[test]
fn chat_tool_delta_use_then_completed_marks_use_as_updated() {
    // Tool started, then completes: the second event re-emits the `tool_use` block
    // which is now a known id → goes into `updated`, while the `tool_result` block
    // is brand new → goes into `added`.
    let mut s = ChatDeltaState::new(&agent());
    let _ = s.tool_delta(&tool_event("msg-9", "msg-9:0", "tc-1", "started", None));
    let d = s
        .tool_delta(&tool_event(
            "msg-9",
            "msg-9:0",
            "tc-1",
            "completed",
            Some(json!("ok")),
        ))
        .unwrap();
    let added = d["added"].as_array().unwrap();
    let updated = d["updated"].as_array().unwrap();
    assert_eq!(added.len(), 1, "only the new tool_result is added");
    assert_eq!(added[0]["block"]["type"], "tool_result");
    assert_eq!(updated.len(), 1, "tool_use is re-emitted as updated");
    assert_eq!(updated[0]["block"]["type"], "tool_use");
}

#[test]
fn chat_seed_from_snapshot_primes_in_flight_message_state() {
    let mut s = ChatDeltaState::new(&agent());
    let snapshot = json!({
        "agentId": "agent-1",
        "messages": [
            { "id": "old", "role": "user", "isStreaming": false },
            {
                "id": "msg-live",
                "role": "assistant",
                "isStreaming": true,
                "contentBlocks": [
                    { "id": "msg-live:0", "type": "text", "text": "Hello" },
                    { "id": "msg-live:1", "type": "tool_use", "name": "shell" }
                ]
            }
        ],
        "totalMessages": 2,
    });
    s.seed_from_snapshot(&snapshot);
    // After seeding, the next text chunk for an already-seen block id is `updated`
    // and carries the FULL accumulated text (seeded prefix + new fragment).
    let d = s
        .chunk_delta(&chunk_event(
            "msg-live",
            "msg-live:0",
            "text",
            json!(", world"),
        ))
        .expect("post-seed chunk");
    assert!(d["added"].as_array().unwrap().is_empty());
    assert_eq!(d["updated"][0]["block"]["text"], "Hello, world");
}

#[test]
fn chat_seed_from_snapshot_is_noop_without_streaming_message() {
    let mut s = ChatDeltaState::new(&agent());
    // No `messages` at all.
    s.seed_from_snapshot(&json!({ "agentId": "agent-1" }));
    // `messages` present but no streaming entry.
    s.seed_from_snapshot(&json!({
        "messages": [{ "id": "x", "isStreaming": false }]
    }));
    // Streaming entry missing id.
    s.seed_from_snapshot(&json!({
        "messages": [{ "isStreaming": true, "contentBlocks": [] }]
    }));
    // After all of the above, a fresh chunk is still `added` (state untouched).
    let d = s
        .chunk_delta(&chunk_event("msg-x", "msg-x:0", "text", json!("hi")))
        .unwrap();
    assert_eq!(d["added"][0]["block"]["text"], "hi");
    assert!(d["updated"].as_array().unwrap().is_empty());
}

#[test]
fn merge_live_turn_appends_in_flight_message_idempotently() {
    let mut snapshot = json!({
        "agentId": "agent-1",
        "messages": [],
        "totalMessages": 0,
    });
    let live = json!({
        "messageId": "msg-live",
        "contentBlocks": [{ "id": "msg-live:0", "type": "text", "text": "partial" }],
    });
    merge_live_turn(&mut snapshot, &agent(), &live);
    let messages = snapshot["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["id"], "msg-live");
    assert_eq!(messages[0]["seq"], 0);
    assert_eq!(messages[0]["isStreaming"], true);
    assert_eq!(messages[0]["role"], "assistant");
    assert_eq!(snapshot["totalMessages"], 1);
    // Idempotent re-merge: same message id already present → no duplicate, no seq bump.
    merge_live_turn(&mut snapshot, &agent(), &live);
    assert_eq!(snapshot["messages"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["totalMessages"], 1);
}

#[test]
fn merge_live_turn_noop_when_message_id_missing() {
    let mut snapshot = json!({ "messages": [], "totalMessages": 0 });
    merge_live_turn(&mut snapshot, &agent(), &json!({ "contentBlocks": [] }));
    assert!(snapshot["messages"].as_array().unwrap().is_empty());
    assert_eq!(snapshot["totalMessages"], 0);
}

#[test]
fn merge_live_turn_noop_when_snapshot_is_not_object() {
    let mut snapshot = json!([]);
    merge_live_turn(
        &mut snapshot,
        &agent(),
        &json!({ "messageId": "m", "contentBlocks": [] }),
    );
    assert_eq!(snapshot, json!([]));
}
