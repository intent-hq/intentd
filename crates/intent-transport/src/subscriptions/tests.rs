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
    assert_eq!(p.since_message_id, None);

    for bad in [r#"{}"#, r#"{"agentId":""}"#, r#"{"agentId":5}"#] {
        let v = parse(bad);
        let err = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("agentId is required"));
    }
}

#[test]
fn chat_params_since_message_id() {
    // A non-empty string is captured for the §7.1 resume path.
    let ok = parse(r#"{"agentId":"agent-1","sinceMessageId":"msg-42"}"#);
    let p = parse_chat_subscribe_params(ok.as_object().unwrap()).unwrap();
    assert_eq!(p.since_message_id.as_deref(), Some("msg-42"));

    // Absent / null / empty string all mean "no resume".
    for none in [
        r#"{"agentId":"a"}"#,
        r#"{"agentId":"a","sinceMessageId":null}"#,
        r#"{"agentId":"a","sinceMessageId":""}"#,
    ] {
        let v = parse(none);
        let p = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap();
        assert_eq!(p.since_message_id, None, "no resume for {none}");
    }

    // A present non-string value is a -32602 error.
    for bad in [
        r#"{"agentId":"a","sinceMessageId":5}"#,
        r#"{"agentId":"a","sinceMessageId":["m"]}"#,
    ] {
        let v = parse(bad);
        let err = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("sinceMessageId must be a string"), "{bad}");
    }
}

#[test]
fn chat_params_delta_encoding() {
    // "incremental" opts into append-only text deltas (monorepo#2675).
    let inc = parse(r#"{"agentId":"a","deltaEncoding":"incremental"}"#);
    let p = parse_chat_subscribe_params(inc.as_object().unwrap()).unwrap();
    assert_eq!(p.delta_encoding, DeltaEncoding::Incremental);

    // Absent / null / "full" all select the default full-text encoding.
    for full in [
        r#"{"agentId":"a"}"#,
        r#"{"agentId":"a","deltaEncoding":null}"#,
        r#"{"agentId":"a","deltaEncoding":"full"}"#,
    ] {
        let v = parse(full);
        let p = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap();
        assert_eq!(p.delta_encoding, DeltaEncoding::Full, "full for {full}");
    }

    // Any other value is a -32602 error, never silently ignored.
    for bad in [
        r#"{"agentId":"a","deltaEncoding":"Incremental"}"#,
        r#"{"agentId":"a","deltaEncoding":""}"#,
        r#"{"agentId":"a","deltaEncoding":5}"#,
        r#"{"agentId":"a","deltaEncoding":["incremental"]}"#,
    ] {
        let v = parse(bad);
        let err = parse_chat_subscribe_params(v.as_object().unwrap()).unwrap_err();
        assert!(err.contains("deltaEncoding"), "{bad}: {err}");
    }
}

#[test]
fn stamp_delta_encoding_echoes_only_incremental() {
    // Incremental snapshots carry the echo (seq-0 and lag recovery alike).
    let mut snapshot = json!({ "agentId": "a", "messages": [] });
    stamp_delta_encoding(&mut snapshot, DeltaEncoding::Incremental);
    assert_eq!(snapshot["deltaEncoding"], "incremental");

    // Full-mode snapshots stay byte-identical to the pre-#2675 shape.
    let mut snapshot = json!({ "agentId": "a", "messages": [] });
    stamp_delta_encoding(&mut snapshot, DeltaEncoding::Full);
    assert!(snapshot.get("deltaEncoding").is_none());
}

#[test]
fn chat_channel_tails_stream_family_and_message() {
    let chat = channel_event_types(Channel::Chat);
    assert_eq!(
        chat,
        vec![
            "chat:stream:delta".to_string(),
            "agent:tool:call".to_string(),
            "agent:stream:end".to_string(),
            "agent:message".to_string(),
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
        "workspace:displayStatus-changed",
        "workspace:waiting-changed",
        "pr:linked",
        "pr:updated",
        "pr:unlinked",
    ] {
        assert!(ws.iter().any(|s| s == t), "workspace missing {t}");
    }
    assert_eq!(ws.len(), 10);
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

// --- ChatDeltaState — pure (sync) paths -----------------------------------

fn agent() -> AgentId {
    AgentId::from("agent-1")
}

fn chunk_event(message_id: &str, block_id: &str, block_type: &str, content: Value) -> Event {
    Event {
        id: "evt-1".into(),
        event_type: CHAT_STREAM_DELTA.to_string(),
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

/// An `agent:tool:call` in the SEQUENTIAL layout — the result / proposal block
/// ids follow the `tool_use` id by one and two, the shape `record_tool`
/// produces when nothing interleaves between the call and its completion. Use
/// [`tool_event_with_ids`] for the interleaved / parallel layouts where they do
/// not follow.
fn tool_event(
    message_id: &str,
    block_id: &str,
    tool_call_id: &str,
    status: &str,
    output: Option<Value>,
) -> Event {
    let (prefix, idx) = block_id.rsplit_once(':').expect("block id `{msg}:{index}`");
    let n: usize = idx.parse().expect("numeric block index");
    tool_event_with_ids(
        message_id,
        block_id,
        tool_call_id,
        status,
        output,
        Some(format!("{prefix}:{}", n + 1)),
        vec![format!("{prefix}:{}", n + 2), format!("{prefix}:{}", n + 3)],
    )
}

/// An `agent:tool:call` with the real (`record_tool`-assigned) result and
/// proposal block ids stated explicitly.
fn tool_event_with_ids(
    message_id: &str,
    block_id: &str,
    tool_call_id: &str,
    status: &str,
    output: Option<Value>,
    result_block_id: Option<String>,
    proposal_block_ids: Vec<String>,
) -> Event {
    let mut data = serde_json::Map::new();
    data.insert("messageId".into(), Value::String(message_id.into()));
    data.insert("blockId".into(), Value::String(block_id.into()));
    if let Some(rid) = result_block_id {
        data.insert("resultBlockId".into(), Value::String(rid));
    }
    if !proposal_block_ids.is_empty() {
        data.insert(
            "proposalBlockIds".into(),
            Value::Array(proposal_block_ids.into_iter().map(Value::String).collect()),
        );
    }
    data.insert("toolCallId".into(), Value::String(tool_call_id.into()));
    data.insert("status".into(), Value::String(status.into()));
    data.insert("toolName".into(), Value::String("shell".into()));
    data.insert("title".into(), Value::String("shell: run ls".into()));
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
fn chat_tool_delta_synthesizes_name_and_acp_title_from_event() {
    // The delta stream must produce the same shape `record_tool` persists so
    // seq-0 snapshot and live deltas agree. `toolName` is the real derived
    // tool name on the wire and the raw ACP title travels separately as
    // `title`; the block's `name` is the toolName verbatim and the title is
    // echoed as `input._acpTitle`.
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d = s
        .tool_delta(&tool_event("msg-9", "msg-9:0", "tc-1", "started", None))
        .expect("tool started delta");
    let block = &d["added"][0]["block"];
    assert_eq!(block["name"], "shell", "toolName used verbatim");
    assert_eq!(block["input"]["cmd"], "ls", "raw args preserved");
    assert_eq!(
        block["input"]["_acpTitle"], "shell: run ls",
        "ACP title echoed under `_acpTitle` when present"
    );
}

fn proposal_output_item() -> Value {
    json!({
        "type": "resource",
        "resource": {
            "uri": "intent-proposal://settings-change/Update",
            "name": "Update",
            "mimeType": "application/vnd.intent.proposal+json",
            "text": "{\"kind\":\"settings-change\"}",
        }
    })
}

#[test]
fn chat_tool_delta_proposal_output_emits_standalone_resource_block() {
    // A completed tool whose output carries a proposal-MIME resource item emits
    // the standalone proposal block right after the tool_result (§7.1), with
    // the resource left in `tool_result.output` untouched.
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let output = json!([{ "type": "text", "text": "shown" }, proposal_output_item()]);
    let d = s
        .tool_delta(&tool_event(
            "msg-p",
            "msg-p:0",
            "tc-p",
            "completed",
            Some(output.clone()),
        ))
        .expect("tool completed delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 3, "tool_use + tool_result + proposal resource");
    assert_eq!(added[1]["block"]["type"], "tool_result");
    assert_eq!(added[1]["block"]["output"], output, "output unchanged");
    let proposal = &added[2]["block"];
    assert_eq!(proposal["type"], "resource");
    assert_eq!(proposal["id"], "msg-p:2", "proposal follows result by +1");
    assert_eq!(
        proposal["resource"]["mimeType"],
        "application/vnd.intent.proposal+json"
    );
    assert_eq!(proposal["resource"], proposal_output_item()["resource"]);
}

#[test]
fn chat_tool_delta_collapsed_proposal_output_emits_standalone_resource_block() {
    // Provider-collapsed output (auggie flattens the MCP content items into
    // `{ "output": "<stringified {ok, proposal}>" }`, dropping the resource
    // item): the fallback lift still emits the standalone proposal block.
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let proposal = json!({
        "kind": "settings-change",
        "preview": { "title": "Update" },
        "payload": { "key": "k" },
    });
    let text = serde_json::to_string_pretty(&json!({ "ok": true, "proposal": proposal }))
        .expect("serialize");
    let output = json!({ "output": text });
    let d = s
        .tool_delta(&tool_event(
            "msg-c",
            "msg-c:0",
            "tc-c",
            "completed",
            Some(output.clone()),
        ))
        .expect("tool completed delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 3, "tool_use + tool_result + proposal resource");
    assert_eq!(added[1]["block"]["output"], output, "output unchanged");
    let block = &added[2]["block"];
    assert_eq!(block["type"], "resource");
    assert_eq!(block["id"], "msg-c:2", "proposal follows result by +1");
    assert_eq!(
        block["resource"]["mimeType"],
        "application/vnd.intent.proposal+json"
    );
    assert_eq!(
        block["resource"]["uri"],
        "intent-proposal://settings-change/Update"
    );
}

#[test]
fn chat_tool_delta_errored_tool_with_proposal_output_emits_no_extra_block() {
    // An errored tool must not surface an actionable ProposalCard, even when
    // its output still carries a proposal-MIME resource item.
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d = s
        .tool_delta(&tool_event(
            "msg-e",
            "msg-e:0",
            "tc-e",
            "error",
            Some(json!([proposal_output_item()])),
        ))
        .expect("tool errored delta");
    assert_eq!(d["added"].as_array().unwrap().len(), 2, "use + result only");
}

#[test]
fn chat_tool_delta_no_proposal_in_output_emits_no_extra_block() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d = s
        .tool_delta(&tool_event(
            "msg-q",
            "msg-q:0",
            "tc-q",
            "completed",
            Some(json!([{ "type": "text", "text": "plain" }])),
        ))
        .expect("tool completed delta");
    assert_eq!(d["added"].as_array().unwrap().len(), 2, "use + result only");
}

#[test]
fn chat_tool_delta_malformed_proposal_resource_emits_no_extra_block() {
    // Wrong MIME / missing text → not a proposal resource; no standalone block.
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let mut wrong_mime = proposal_output_item();
    wrong_mime["resource"]["mimeType"] = json!("text/plain");
    let mut no_text = proposal_output_item();
    no_text["resource"].as_object_mut().unwrap().remove("text");
    let d = s
        .tool_delta(&tool_event(
            "msg-r",
            "msg-r:0",
            "tc-r",
            "completed",
            Some(json!([wrong_mime, no_text])),
        ))
        .expect("tool completed delta");
    assert_eq!(d["added"].as_array().unwrap().len(), 2, "use + result only");
}

#[test]
fn chat_tool_delta_completed_without_output_only_emits_use_block() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d = s
        .tool_delta(&tool_event("msg-7", "msg-7:0", "tc-x", "completed", None))
        .expect("completed-no-output delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 1, "no output → no synthetic result block");
    assert_eq!(added[0]["block"]["type"], "tool_use");
}

#[test]
fn chat_tool_delta_returns_none_for_missing_required_fields() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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

/// An id-keyed client's view of the live stream: every delta's entities folded
/// into an ordered `(blockId, type)` list, `added` appending and `updated`
/// replacing in place — exactly what `ChatTranscriptReconciler.upsertBlock`
/// does on the FE.
fn reduce_deltas(deltas: &[Value]) -> Vec<(String, String)> {
    let mut blocks: Vec<(String, String)> = Vec::new();
    for delta in deltas {
        for key in ["added", "updated"] {
            for entity in delta[key].as_array().into_iter().flatten() {
                let block = &entity["block"];
                let id = block["id"].as_str().expect("block id").to_string();
                let ty = block["type"].as_str().unwrap_or("").to_string();
                match blocks.iter_mut().find(|(bid, _)| bid == &id) {
                    Some(slot) => slot.1 = ty,
                    None => blocks.push((id, ty)),
                }
            }
        }
    }
    blocks
}

/// monorepo#2029, shape (a): text INTERLEAVES between a tool call and its
/// completion. The durable transcript flushes that text into `{mid}:2` and
/// puts the real `tool_result` at `{mid}:3`; the mapper stamps the event's
/// `resultBlockId`, so the live ids equal the durable ones and the interleaved
/// text block — the one that can carry a `<group:Name>` opener — is never
/// overwritten. Predicting `tool_use + 1` clobbered it for the rest of the turn.
#[test]
fn chat_tool_delta_uses_the_event_result_id_when_text_interleaves() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let deltas = vec![
        s.chunk_delta(&chunk_event(
            "msg-i",
            "msg-i:0",
            "text",
            json!("I'll run it. "),
        ))
        .expect("opening text"),
        s.tool_delta(&tool_event_with_ids(
            "msg-i",
            "msg-i:1",
            "tc-i",
            "started",
            None,
            None,
            Vec::new(),
        ))
        .expect("tool started"),
        s.chunk_delta(&chunk_event(
            "msg-i",
            "msg-i:2",
            "text",
            json!("<group:Setup>\nChecking output. "),
        ))
        .expect("interleaved text"),
        s.tool_delta(&tool_event_with_ids(
            "msg-i",
            "msg-i:1",
            "tc-i",
            "completed",
            Some(json!("12 passed")),
            Some("msg-i:3".to_string()),
            Vec::new(),
        ))
        .expect("tool completed"),
    ];
    // The durable layout `record_tool` writes for this turn, verbatim.
    assert_eq!(
        reduce_deltas(&deltas),
        vec![
            ("msg-i:0".to_string(), "text".to_string()),
            ("msg-i:1".to_string(), "tool_use".to_string()),
            ("msg-i:2".to_string(), "text".to_string()),
            ("msg-i:3".to_string(), "tool_result".to_string()),
        ],
        "the live ids match the durable transcript; msg-i:2 stays the text block"
    );
}

/// monorepo#2029, shape (b): two PARALLEL calls completing in order. Both
/// `tool_use` blocks precede either result, so `tool_use + 1` named the second
/// call's `tool_use` — live, t2's tool row was replaced by t1's result until
/// `stream:end`. With the real ids on the event both rows survive.
#[test]
fn chat_tool_delta_uses_the_event_result_id_for_parallel_completions() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let started = |block_id: &str, tc: &str| {
        tool_event_with_ids("msg-p2", block_id, tc, "started", None, None, Vec::new())
    };
    let done = |block_id: &str, tc: &str, result_id: &str| {
        tool_event_with_ids(
            "msg-p2",
            block_id,
            tc,
            "completed",
            Some(json!("ok")),
            Some(result_id.to_string()),
            Vec::new(),
        )
    };
    let deltas = vec![
        s.chunk_delta(&chunk_event("msg-p2", "msg-p2:0", "text", json!("Both. ")))
            .expect("opening text"),
        s.tool_delta(&started("msg-p2:1", "t1"))
            .expect("t1 started"),
        s.tool_delta(&started("msg-p2:2", "t2"))
            .expect("t2 started"),
        s.tool_delta(&done("msg-p2:1", "t1", "msg-p2:3"))
            .expect("t1 done"),
        s.tool_delta(&done("msg-p2:2", "t2", "msg-p2:4"))
            .expect("t2 done"),
    ];
    assert_eq!(
        reduce_deltas(&deltas),
        vec![
            ("msg-p2:0".to_string(), "text".to_string()),
            ("msg-p2:1".to_string(), "tool_use".to_string()),
            ("msg-p2:2".to_string(), "tool_use".to_string()),
            ("msg-p2:3".to_string(), "tool_result".to_string()),
            ("msg-p2:4".to_string(), "tool_result".to_string()),
        ],
        "t2's tool_use at msg-p2:2 survives t1's completion"
    );
}

/// The proposal-resource block ids come off the event too — they are NOT
/// chained from the `tool_result` id, which is wrong for the same reasons.
#[test]
fn chat_tool_delta_proposal_ids_come_from_the_event() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let output = json!([proposal_output_item()]);
    let d = s
        .tool_delta(&tool_event_with_ids(
            "msg-pi",
            "msg-pi:1",
            "tc-pi",
            "completed",
            Some(output),
            Some("msg-pi:4".to_string()),
            vec!["msg-pi:5".to_string()],
        ))
        .expect("tool completed delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 3, "tool_use + tool_result + proposal resource");
    assert_eq!(added[1]["block"]["id"], "msg-pi:4");
    assert_eq!(added[2]["block"]["type"], "resource");
    assert_eq!(
        added[2]["block"]["id"], "msg-pi:5",
        "the proposal block carries the id record_tool gave it"
    );
}

/// Defensive: an event carrying output but no `resultBlockId` (no result block
/// was materialized) synthesizes no result block live rather than inventing an
/// id — the terminal reconcile delivers whatever the turn actually persisted.
#[test]
fn chat_tool_delta_without_result_id_emits_only_the_use_block() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d = s
        .tool_delta(&tool_event_with_ids(
            "msg-nr",
            "msg-nr:1",
            "tc-nr",
            "completed",
            Some(json!("ok")),
            None,
            Vec::new(),
        ))
        .expect("tool completed delta");
    let added = d["added"].as_array().unwrap();
    assert_eq!(added.len(), 1, "no result block without a real id");
    assert_eq!(added[0]["block"]["type"], "tool_use");
}

#[test]
fn chat_seed_from_snapshot_primes_in_flight_message_state() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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

/// `thinking` chunks accumulate like text ones: one block id, `added` then
/// `updated` with the full reasoning so far, and the block keeps its
/// `thinking` type.
#[test]
fn chat_chunk_delta_accumulates_thinking_blocks() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    let d1 = s
        .chunk_delta(&chunk_event(
            "msg-6",
            "msg-6:0",
            "thinking",
            json!("Let me "),
        ))
        .expect("thinking chunk delta");
    assert_eq!(d1["added"][0]["block"]["type"], "thinking");
    assert_eq!(d1["added"][0]["block"]["text"], "Let me ");
    let d2 = s
        .chunk_delta(&chunk_event(
            "msg-6",
            "msg-6:0",
            "thinking",
            json!("think."),
        ))
        .expect("thinking chunk delta 2");
    assert!(d2["added"].as_array().unwrap().is_empty());
    assert_eq!(d2["updated"][0]["block"]["text"], "Let me think.");
    assert_eq!(d2["updated"][0]["block"]["type"], "thinking");
    // The text block that follows the reasoning is a separate accumulator.
    let d3 = s
        .chunk_delta(&chunk_event("msg-6", "msg-6:1", "text", json!("42.")))
        .expect("text chunk delta");
    assert_eq!(d3["added"][0]["block"]["type"], "text");
    assert_eq!(d3["added"][0]["block"]["text"], "42.");
}

/// A mid-turn `chat.subscribe` seeds the accumulator from `thinking` blocks
/// too, so the resumed reasoning stream is not truncated to its tail.
#[test]
fn chat_seed_from_snapshot_primes_thinking_blocks() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
    s.seed_from_snapshot(&json!({
        "agentId": "agent-1",
        "messages": [{
            "id": "msg-live",
            "role": "assistant",
            "isStreaming": true,
            "contentBlocks": [
                { "id": "msg-live:0", "type": "thinking", "text": "Let me " }
            ]
        }],
    }));
    let d = s
        .chunk_delta(&chunk_event(
            "msg-live",
            "msg-live:0",
            "thinking",
            json!("think."),
        ))
        .expect("post-seed thinking chunk");
    assert!(d["added"].as_array().unwrap().is_empty());
    assert_eq!(d["updated"][0]["block"]["text"], "Let me think.");
    assert_eq!(d["updated"][0]["block"]["type"], "thinking");
}

#[test]
fn chat_seed_from_snapshot_is_noop_without_streaming_message() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
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

/// Incremental encoding (monorepo#2675): each text/thinking chunk delta
/// carries only the new fragment as `textDelta` (never the accumulated
/// `text`), with the same added→updated bucket flip as full mode.
#[test]
fn incremental_chunk_delta_carries_only_the_fragment() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Incremental, None);
    let d1 = s
        .chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
        .expect("first chunk");
    assert_eq!(d1["added"][0]["block"]["textDelta"], "Hello");
    assert_eq!(d1["added"][0]["block"]["type"], "text");
    assert_eq!(d1["added"][0]["block"]["id"], "msg-1:0");
    assert!(
        d1["added"][0]["block"].get("text").is_none(),
        "incremental deltas never carry the accumulated text: {d1}"
    );
    let d2 = s
        .chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
        .expect("second chunk");
    assert!(d2["added"].as_array().unwrap().is_empty());
    assert_eq!(
        d2["updated"][0]["block"]["textDelta"], ", world",
        "only the NEW fragment travels, not the accumulation: {d2}"
    );
    // Thinking chunks take the same shape.
    let d3 = s
        .chunk_delta(&chunk_event("msg-1", "msg-1:1", "thinking", json!("Hmm")))
        .expect("thinking chunk");
    assert_eq!(d3["added"][0]["block"]["textDelta"], "Hmm");
    assert_eq!(d3["added"][0]["block"]["type"], "thinking");
}

/// Non-text chunks are encoding-independent: they pass through as the full
/// block in incremental mode exactly as in full mode.
#[test]
fn incremental_chunk_delta_passes_non_text_blocks_through() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Incremental, None);
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
    assert!(block.get("textDelta").is_none());
}

/// D5 mid-turn resume in incremental mode: the snapshot already carries the
/// accumulated text, so the post-seed chunk is an `updated` fragment the
/// client appends — the daemon must NOT replay the seeded prefix.
#[test]
fn incremental_seed_from_snapshot_appends_fragments_after_the_snapshot_text() {
    let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Incremental, None);
    s.seed_from_snapshot(&json!({
        "agentId": "agent-1",
        "messages": [{
            "id": "msg-live",
            "role": "assistant",
            "isStreaming": true,
            "contentBlocks": [
                { "id": "msg-live:0", "type": "text", "text": "Hello" }
            ]
        }],
    }));
    let d = s
        .chunk_delta(&chunk_event(
            "msg-live",
            "msg-live:0",
            "text",
            json!(", world"),
        ))
        .expect("post-seed chunk");
    assert!(d["added"].as_array().unwrap().is_empty());
    assert_eq!(
        d["updated"][0]["block"]["textDelta"], ", world",
        "the seeded prefix lives in the snapshot; only the fragment travels: {d}"
    );
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
    merge_live_turn(&mut snapshot, &agent(), &live, true);
    let messages = snapshot["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["id"], "msg-live");
    assert_eq!(messages[0]["seq"], 0);
    assert_eq!(messages[0]["isStreaming"], true);
    assert_eq!(messages[0]["role"], "assistant");
    assert_eq!(snapshot["totalMessages"], 1);
    // Idempotent re-merge: same message id already present → no duplicate, no seq bump.
    merge_live_turn(&mut snapshot, &agent(), &live, true);
    assert_eq!(snapshot["messages"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["totalMessages"], 1);
}

/// monorepo#2104 — an orphaned (not-busy) slot's content is merged, flagged
/// `isStreaming: false`: real output, but nothing is coming for it.
#[test]
fn merge_live_turn_merges_an_orphan_slot_as_not_streaming() {
    let mut snapshot = json!({
        "agentId": "agent-1",
        "messages": [],
        "totalMessages": 0,
    });
    let live = json!({
        "messageId": "msg-orphan",
        "contentBlocks": [{ "id": "msg-orphan:0", "type": "text", "text": "partial" }],
    });
    merge_live_turn(&mut snapshot, &agent(), &live, false);
    let messages = snapshot["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["id"], "msg-orphan");
    assert_eq!(
        messages[0]["isStreaming"], false,
        "an orphaned slot must never claim to be streaming: {snapshot}"
    );
    assert_eq!(snapshot["totalMessages"], 1);
}

/// An orphan with nothing streamed is skipped — no blank assistant row — while a
/// still-streaming turn that has not produced blocks yet is still merged, so the
/// client learns the message id to reconcile the terminal event against.
#[test]
fn merge_live_turn_skips_an_empty_orphan_but_not_an_empty_live_turn() {
    let empty = json!({ "messageId": "msg-live", "contentBlocks": [] });

    let mut orphan = json!({ "messages": [], "totalMessages": 0 });
    merge_live_turn(&mut orphan, &agent(), &empty, false);
    assert!(
        orphan["messages"].as_array().unwrap().is_empty(),
        "an empty orphan slot adds no row: {orphan}"
    );
    assert_eq!(orphan["totalMessages"], 0);

    let mut streaming = json!({ "messages": [], "totalMessages": 0 });
    merge_live_turn(&mut streaming, &agent(), &empty, true);
    assert_eq!(streaming["messages"].as_array().unwrap().len(), 1);
    assert_eq!(streaming["messages"][0]["isStreaming"], true);
}

#[test]
fn merge_live_turn_noop_when_message_id_missing() {
    let mut snapshot = json!({ "messages": [], "totalMessages": 0 });
    merge_live_turn(
        &mut snapshot,
        &agent(),
        &json!({ "contentBlocks": [] }),
        true,
    );
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
        true,
    );
    assert_eq!(snapshot, json!([]));
}

// --- task_delta re-read arm (channel-mapping regression) ------------------

mod task_delta_re_read {
    use super::*;
    use intent_core::{
        BoxFuture, ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, Result,
        TaskMetadata, WorkspaceApi, WorkspaceId,
    };
    use std::collections::HashSet;

    /// Minimal `WorkspaceApi` that serves pre-canned [`Note`]s from `get_note`
    /// by id — `spec` for the spec fixture, `other`'s own id for the second
    /// task-note fixture, anything else for the primary task-note fixture
    /// (`NotFound` when the fixture is `None`) — and all fixtures from
    /// `list_notes` (the spec-flip path's one bounded read). Everything else
    /// falls through to the trait defaults, which is fine because
    /// `task_delta` only touches these two.
    struct StaticNoteApi {
        note: Option<Note>,
        spec: Option<Note>,
        other: Option<Note>,
    }

    impl StaticNoteApi {
        fn new(note: Option<Note>) -> Self {
            Self {
                note,
                spec: None,
                other: None,
            }
        }
    }

    impl WorkspaceApi for StaticNoteApi {
        fn get_note(
            &self,
            _workspace_id: WorkspaceId,
            note_id: NoteId,
        ) -> BoxFuture<'_, Result<Note>> {
            let note = if note_id.as_str() == "spec" {
                self.spec.clone()
            } else if self.other.as_ref().is_some_and(|n| n.id == note_id) {
                self.other.clone()
            } else {
                self.note.clone()
            };
            Box::pin(async move { note.ok_or_else(|| Error::NotFound("note".to_string())) })
        }

        fn list_notes<'a>(
            &'a self,
            _workspace_id: &'a WorkspaceId,
        ) -> BoxFuture<'a, Result<Vec<Note>>> {
            let notes: Vec<Note> = self
                .spec
                .iter()
                .chain(self.note.iter())
                .chain(self.other.iter())
                .cloned()
                .collect();
            Box::pin(async move { Ok(notes) })
        }
    }

    fn ws() -> WorkspaceId {
        WorkspaceId::from("w")
    }

    fn note_with(task: Option<TaskMetadata>) -> Note {
        Note {
            id: NoteId::from("n-1"),
            workspace_id: ws(),
            title: "T".to_string(),
            content: String::new(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata { task },
            created_at: "t0".to_string(),
            rev: 0,
            updated_at: "t0".to_string(),
        }
    }

    fn spec_note(content: &str) -> Note {
        Note {
            id: NoteId::from("spec"),
            content: content.to_string(),
            ..note_with(None)
        }
    }

    fn note_event(event_type: &str, note_id: &str) -> Event {
        Event {
            id: "evt-1".into(),
            event_type: event_type.to_string(),
            timestamp: now_iso(),
            workspace_id: ws(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            data: json!({ "noteId": note_id }),
        }
    }

    #[tokio::test]
    async fn note_updated_emits_removed_ids_when_task_was_demoted() {
        // The re-read note no longer projects a task → the delta must remove it
        // from every subscribed task list.
        let api = StaticNoteApi::new(Some(note_with(None)));
        let d = task_delta(
            &api,
            &ws(),
            &note_event(NOTE_UPDATED, "n-1"),
            &mut HashSet::new(),
        )
        .await
        .expect("delta must be emitted for demotion");
        assert_eq!(d["removedIds"][0], "n-1");
        assert!(d.get("updated").is_none());
        assert!(d.get("added").is_none());
    }

    #[tokio::test]
    async fn task_status_changed_also_emits_removed_ids_when_task_missing() {
        // The `task:status-changed` arm shares the re-read path with
        // `note:updated`; a demotion that races a status event must still be
        // reported as a removal, not silently dropped.
        let api = StaticNoteApi::new(Some(note_with(None)));
        let d = task_delta(
            &api,
            &ws(),
            &note_event(TASK_STATUS_CHANGED, "n-1"),
            &mut HashSet::new(),
        )
        .await
        .expect("delta must be emitted");
        assert_eq!(d["removedIds"][0], "n-1");
    }

    #[tokio::test]
    async fn note_updated_still_emits_updated_when_task_present() {
        // Regression guard: the removal path must not swallow legitimate
        // `updated` deltas when the note is still a task. With no spec note
        // readable, `specLinked` degrades to false (matching `task.list`).
        let api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        let d = task_delta(
            &api,
            &ws(),
            &note_event(NOTE_UPDATED, "n-1"),
            &mut HashSet::new(),
        )
        .await
        .expect("delta must be emitted");
        assert_eq!(d["updated"][0]["id"], "n-1");
        assert_eq!(d["updated"][0]["specLinked"], false);
        assert!(d.get("removedIds").is_none());
    }

    #[tokio::test]
    async fn updated_row_carries_spec_linked_true_when_spec_links_the_task() {
        // `specLinked` semantics match `task.list` (§5.4): true iff the task
        // id appears in the spec body's `intent://local/task/{id}` links.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("- [ ] [T](intent://local/task/n-1)"));
        let mut linked = HashSet::new();
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "n-1"), &mut linked)
            .await
            .expect("delta must be emitted");
        assert_eq!(d["updated"][0]["specLinked"], true, "delta: {d}");
        assert!(
            linked.contains("n-1"),
            "the per-task spec read must refresh the tracked link set"
        );
    }

    #[tokio::test]
    async fn added_row_carries_spec_linked_flag() {
        // The `note:created` arm stamps the flag too — false when the spec
        // links a different task.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("- [ ] [Other](intent://local/task/n-2)"));
        let d = task_delta(
            &api,
            &ws(),
            &note_event(NOTE_CREATED, "n-1"),
            &mut HashSet::new(),
        )
        .await
        .expect("delta must be emitted");
        assert_eq!(d["added"][0]["specLinked"], false, "delta: {d}");
    }

    // --- spec-body edits refresh flipped `specLinked` flags (monorepo#2407) --

    #[tokio::test]
    async fn spec_edit_adding_a_link_emits_updated_row_with_spec_linked_true() {
        // A spec `note:updated` no longer maps to the junk
        // `removedIds: ["spec"]`: the link set is diffed against the tracked
        // one and the newly linked task's row is re-emitted with
        // `specLinked: true`.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("- [ ] [T](intent://local/task/n-1)"));
        let mut linked = HashSet::new();
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked)
            .await
            .expect("flip delta must be emitted");
        assert_eq!(d["updated"].as_array().map(Vec::len), Some(1), "delta: {d}");
        assert_eq!(d["updated"][0]["id"], "n-1");
        assert_eq!(d["updated"][0]["specLinked"], true, "delta: {d}");
        assert!(d.get("removedIds").is_none(), "delta: {d}");
        assert!(d.get("added").is_none(), "delta: {d}");
    }

    #[tokio::test]
    async fn spec_edit_removing_a_link_emits_updated_row_with_spec_linked_false() {
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("no links left"));
        let mut linked = HashSet::from(["n-1".to_string()]);
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked)
            .await
            .expect("flip delta must be emitted");
        assert_eq!(d["updated"][0]["id"], "n-1");
        assert_eq!(d["updated"][0]["specLinked"], false, "delta: {d}");
    }

    #[tokio::test]
    async fn spec_edit_not_touching_links_emits_no_delta() {
        // Same link set before and after → nothing flipped → no rows at all
        // (unrelated tasks must not be re-emitted on every spec keystroke).
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("edited body\n- [ ] [T](intent://local/task/n-1)"));
        let mut linked = HashSet::from(["n-1".to_string()]);
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked).await;
        assert!(d.is_none(), "no delta expected: {d:?}");
    }

    #[tokio::test]
    async fn spec_edit_flip_is_emitted_once_then_settles() {
        // The tracked set is replaced by the flip, so replaying the same spec
        // state emits nothing further.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("- [ ] [T](intent://local/task/n-1)"));
        let mut linked = HashSet::new();
        task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked)
            .await
            .expect("first flip delta");
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked).await;
        assert!(d.is_none(), "replay must settle: {d:?}");
    }

    #[tokio::test]
    async fn spec_link_to_a_non_task_id_emits_no_row() {
        // A dangling `intent://local/task/{id}` link (no such task note)
        // flips silently — there is no row to refresh.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.spec = Some(spec_note("- [ ] [Ghost](intent://local/task/ghost)"));
        let mut linked = HashSet::new();
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked).await;
        assert!(d.is_none(), "no delta expected: {d:?}");
        assert!(linked.contains("ghost"), "the tracked set still advances");
    }

    #[tokio::test]
    async fn per_task_stamp_must_not_adopt_other_tasks_link_changes() {
        // Interleaving race (PR #1224 review, r3784216992): a spec write that
        // links n-2 commits, then n-1's own event is processed BEFORE the
        // spec's `note:updated` reaches the forwarder. The per-task arm's
        // fresh spec read already sees n-2's link, but must update only n-1
        // in the tracked set — adopting n-2's membership without re-emitting
        // its row would make the later flip diff see no difference, leaving
        // the subscriber's n-2 flag permanently stale.
        let mut api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        api.other = Some(Note {
            id: NoteId::from("n-2"),
            ..note_with(Some(TaskMetadata::default()))
        });
        api.spec = Some(spec_note("- [ ] [Other](intent://local/task/n-2)"));
        let mut linked = HashSet::new();
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "n-1"), &mut linked)
            .await
            .expect("delta must be emitted");
        assert_eq!(d["updated"][0]["specLinked"], false, "delta: {d}");
        assert!(
            !linked.contains("n-2"),
            "per-task stamp must not adopt n-2's membership: {linked:?}"
        );
        // The spec event arrives next: its flip diff still sees n-2 as new
        // and re-emits the row the subscriber has never seen linked.
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "spec"), &mut linked)
            .await
            .expect("flip delta must be emitted");
        assert_eq!(d["updated"][0]["id"], "n-2", "delta: {d}");
        assert_eq!(d["updated"][0]["specLinked"], true, "delta: {d}");
    }

    #[tokio::test]
    async fn spec_deletion_emits_updated_rows_unlinking_tracked_tasks() {
        // Spec `note:deleted` routes through the same flip diff against the
        // now-empty link set (PR #1224 review, r3784217143): every tracked
        // task is re-emitted `specLinked: false` instead of the subscriber
        // holding stale flags forever.
        let api = StaticNoteApi::new(Some(note_with(Some(TaskMetadata::default()))));
        let mut linked = HashSet::from(["n-1".to_string()]);
        let d = task_delta(&api, &ws(), &note_event(NOTE_DELETED, "spec"), &mut linked)
            .await
            .expect("flip delta must be emitted");
        assert_eq!(d["updated"][0]["id"], "n-1");
        assert_eq!(d["updated"][0]["specLinked"], false, "delta: {d}");
        assert!(d.get("removedIds").is_none(), "delta: {d}");
        assert!(linked.is_empty(), "tracked set must drain: {linked:?}");
    }
}

// --- chat_snapshot bounded seq-0 read (monorepo#958 regression) ------------

mod chat_snapshot_bounded {
    use super::*;
    use intent_core::{BoxFuture, Result, WorkspaceApi};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal `WorkspaceApi` standing in for the SQL-paginated
    /// `agent.getConversation` over a large (120-message) transcript: it
    /// serves the bounded newest page (`truncated: true` + a `nextToken`)
    /// and counts calls. The bounded-snapshot contract (monorepo#958) is
    /// that `chat_snapshot` performs exactly ONE read — no token follow-up
    /// to re-hydrate older pages — and forwards the page verbatim.
    struct BoundedPageApi {
        calls: AtomicUsize,
        busy: bool,
    }

    impl BoundedPageApi {
        fn new(busy: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                busy,
            }
        }
    }

    impl WorkspaceApi for BoundedPageApi {
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // The snapshot must ask for the newest page (no cursor) and must
            // not override the server-clamped bounded limit ([1,200]).
            assert!(page_token.is_none(), "snapshot must not walk older pages");
            assert!(
                limit.is_none_or(|l| (1..=200).contains(&l)),
                "snapshot limit must stay within the server clamp, got {limit:?}"
            );
            Box::pin(async move {
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": [
                        { "id": "m-118", "role": "user", "seq": 118 },
                        { "id": "m-119", "role": "assistant", "seq": 119 },
                    ],
                    "truncated": true,
                    "totalMessages": 120,
                    "nextToken": "tok-older",
                    "turnInFlight": false,
                    "lastStreamActivityAt": Value::Null,
                }))
            })
        }

        fn agent_is_busy(&self, _agent_id: AgentId) -> bool {
            self.busy
        }

        fn agent_live_turn(&self, _agent_id: AgentId) -> Option<Value> {
            self.busy.then(|| {
                json!({
                    "messageId": "msg-live",
                    "contentBlocks": [
                        { "id": "msg-live:0", "type": "text", "text": "partial" }
                    ],
                })
            })
        }
    }

    #[tokio::test]
    async fn chat_snapshot_reads_exactly_one_bounded_page_for_large_transcript() {
        let api = BoundedPageApi::new(false);
        let snap = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(
            api.calls.load(Ordering::SeqCst),
            1,
            "seq-0 snapshot must issue exactly one bounded conversation read"
        );
        // The bounded page is forwarded verbatim: pagination fields intact,
        // no attempt to inline the older history.
        assert_eq!(snap["messages"].as_array().unwrap().len(), 2);
        assert_eq!(snap["truncated"], true);
        assert_eq!(snap["totalMessages"], 120);
        assert_eq!(snap["nextToken"], "tok-older");
        // No `sinceMessageId` → no `resumed` key at all (§7.1).
        assert!(
            snap.get("resumed").is_none(),
            "standard snapshot carries no resumed key: {snap}"
        );
        // Activity flags are overlaid (all-false via the trait default here).
        assert_eq!(snap["isResponding"], false);
        assert_eq!(snap["waitingForAgentIds"], json!([]));
    }

    #[tokio::test]
    async fn chat_snapshot_merges_live_turn_on_truncated_page() {
        let api = BoundedPageApi::new(true);
        let snap = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        // The in-flight message is appended after the bounded page with the
        // next monotonic seq (CS-0 D5) — truncation does not disable the merge.
        let messages = snap["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[2]["id"], "msg-live");
        assert_eq!(messages[2]["seq"], 120);
        assert_eq!(messages[2]["isStreaming"], true);
        assert_eq!(snap["totalMessages"], 121);
        assert_eq!(snap["truncated"], true);
        assert_eq!(snap["nextToken"], "tok-older");
    }

    #[tokio::test]
    async fn chat_snapshot_resume_serves_only_messages_after_since_id() {
        let api = BoundedPageApi::new(false);
        let snap = chat_snapshot(&api, &agent(), Some("m-118"), None).await;
        // Resume is a post-filter, never a second fetch.
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        let messages = snap["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1, "only rows after m-118: {snap}");
        assert_eq!(messages[0]["id"], "m-119");
        assert_eq!(snap["resumed"], true);
        // No gap toward older history: the client already holds it.
        assert_eq!(snap["truncated"], false);
        assert_eq!(snap["nextToken"], Value::Null);
        // totalMessages stays the transcript-wide count.
        assert_eq!(snap["totalMessages"], 120);
    }

    #[tokio::test]
    async fn chat_snapshot_resume_at_newest_id_yields_empty_page() {
        let api = BoundedPageApi::new(false);
        let snap = chat_snapshot(&api, &agent(), Some("m-119"), None).await;
        assert_eq!(snap["messages"].as_array().unwrap().len(), 0);
        assert_eq!(snap["resumed"], true);
        assert_eq!(snap["truncated"], false);
        assert_eq!(snap["nextToken"], Value::Null);
    }

    #[tokio::test]
    async fn chat_snapshot_resume_unknown_id_falls_back_to_full_page() {
        let api = BoundedPageApi::new(false);
        let snap = chat_snapshot(&api, &agent(), Some("msg-nope"), None).await;
        // Still exactly one bounded read — no lookup follow-up.
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        // The standard page is served intact; `resumed: false` tells the
        // client to discard its cache and rehydrate.
        let messages = snap["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(snap["resumed"], false);
        assert_eq!(snap["truncated"], true);
        assert_eq!(snap["nextToken"], "tok-older");
        assert_eq!(snap["totalMessages"], 120);
    }

    #[tokio::test]
    async fn chat_snapshot_resume_keeps_live_turn_merge() {
        let api = BoundedPageApi::new(true);
        let snap = chat_snapshot(&api, &agent(), Some("m-119"), None).await;
        // The filter trims the persisted page to empty, then the in-flight
        // message is merged AFTER the filter, so it is never trimmed away.
        let messages = snap["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["id"], "msg-live");
        assert_eq!(messages[0]["isStreaming"], true);
        assert_eq!(snap["resumed"], true);
    }

    /// A `WorkspaceApi` that records the `projection` each conversation read
    /// was asked for — the snapshot paths must forward the subscription's
    /// projection so slim subscribers never receive a full-fidelity page.
    struct ProjectionRecordingApi {
        seen: std::sync::Mutex<Vec<Option<intent_core::ConversationProjection>>>,
    }

    impl ProjectionRecordingApi {
        fn new() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl WorkspaceApi for ProjectionRecordingApi {
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.seen.lock().unwrap().push(projection);
            Box::pin(async move {
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": [],
                    "truncated": false,
                    "totalMessages": 0,
                    "nextToken": Value::Null,
                }))
            })
        }
    }

    /// Both the seq-0 snapshot and the lag-recovery snapshot forward the
    /// subscription's projection to the conversation read (and absent stays
    /// absent — back-compat).
    #[tokio::test]
    async fn snapshots_forward_subscription_projection() {
        use intent_core::ConversationProjection;
        let api = ProjectionRecordingApi::new();
        chat_snapshot(&api, &agent(), None, Some(ConversationProjection::Slim)).await;
        chat_snapshot(&api, &agent(), None, None).await;
        chat_recovery_snapshot(&api, &agent(), Some(ConversationProjection::Slim)).await;
        assert_eq!(
            *api.seen.lock().unwrap(),
            vec![
                Some(ConversationProjection::Slim),
                None,
                Some(ConversationProjection::Slim)
            ],
        );
    }
}

// --- ChatDeltaState — agent:message user-row deltas ------------------------

mod chat_message_delta {
    use super::*;
    use intent_core::events::AGENT_MESSAGE;
    use intent_core::{BoxFuture, Result, WorkspaceApi};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal `WorkspaceApi` serving a pre-canned `agent.getConversation`
    /// page and counting reads, standing in for the persisted transcript the
    /// `agent:message` re-read path consults.
    struct ConvApi {
        calls: AtomicUsize,
        conversation: Value,
    }

    impl ConvApi {
        fn new(conversation: Value) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                conversation,
            }
        }
    }

    impl WorkspaceApi for ConvApi {
        fn agent_get_conversation(
            &self,
            _agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let conv = self.conversation.clone();
            Box::pin(async move { Ok(conv) })
        }
    }

    /// An `agent:message` bus event as `publish_agent_mutation_event` emits it
    /// (payload from `agent_message_event_payload`; `session_id == agentId`).
    fn message_event(agent_id: &str, message_id: &str, role: &str) -> Event {
        Event {
            id: "evt-3".into(),
            event_type: AGENT_MESSAGE.to_string(),
            timestamp: now_iso(),
            workspace_id: WorkspaceId::from("w"),
            session_id: Some(agent_id.to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            data: json!({
                "agentId": agent_id,
                "messageId": message_id,
                "role": role,
            }),
        }
    }

    fn user_row_conversation() -> Value {
        json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "user-msg-1",
                    "role": "user",
                    "seq": 4,
                    "timestamp": "2026-07-30T00:00:00Z",
                    "contentBlocks": [
                        { "type": "text", "text": "queued message one" },
                        { "type": "image", "data": "abc", "mimeType": "image/png" }
                    ]
                }
            ],
            "truncated": false,
            "totalMessages": 5,
            "nextToken": Value::Null,
        })
    }

    #[tokio::test]
    async fn user_row_message_emits_added_entities_with_real_role() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row agent:message must map to a delta");
        assert_eq!(api.calls.load(Ordering::SeqCst), 1, "one bounded re-read");
        let added = d["added"].as_array().unwrap();
        assert_eq!(added.len(), 2, "one entity per persisted block: {d}");
        for e in added {
            assert_eq!(e["agentId"], "agent-1");
            assert_eq!(e["messageId"], "user-msg-1");
            assert_eq!(e["role"], "user", "entity carries the row's REAL role");
            assert_eq!(e["messageSeq"], 4);
            assert_eq!(e["timestamp"], "2026-07-30T00:00:00Z");
            assert_eq!(e["streamingComplete"], true);
        }
        // Stable synthetic ids `{messageId}:{index}` stamped on blocks that
        // persisted without one, so re-delivery upserts by id.
        assert_eq!(added[0]["block"]["id"], "user-msg-1:0");
        assert_eq!(added[0]["block"]["text"], "queued message one");
        assert_eq!(added[1]["block"]["id"], "user-msg-1:1");
        assert!(d["updated"].as_array().unwrap().is_empty());
        assert!(d["removedIds"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn user_row_message_entities_carry_row_metadata() {
        // A child→coordinator send persists the user row with
        // `metadata: { type: "agent_message", fromAgentId, fromAgentName }`
        // (sender attribution). The live delta entities must lift that row
        // metadata so subscribers render the sender chip with no refetch —
        // rows without metadata keep the pre-existing entity shape (additive).
        let metadata = json!({
            "type": "agent_message",
            "fromAgentId": "agent-child",
            "fromAgentName": "Child Agent",
        });
        let conv = json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "user-msg-1",
                    "role": "user",
                    "seq": 4,
                    "timestamp": "2026-07-30T00:00:00Z",
                    "metadata": metadata,
                    "contentBlocks": [ { "type": "text", "text": "child report" } ]
                }
            ],
            "truncated": false,
            "totalMessages": 5,
            "nextToken": Value::Null,
        });
        let api = ConvApi::new(conv);
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row agent:message must map to a delta");
        let added = d["added"].as_array().unwrap();
        assert_eq!(added.len(), 1);
        assert_eq!(
            added[0]["metadata"], metadata,
            "entity must carry the persisted row metadata: {d}"
        );
    }

    #[tokio::test]
    async fn user_row_message_without_metadata_omits_the_field() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row agent:message must map to a delta");
        for e in d["added"].as_array().unwrap() {
            assert!(
                e.get("metadata").is_none(),
                "rows without metadata keep the lean entity shape: {e}"
            );
        }
    }

    #[tokio::test]
    async fn user_row_message_entities_carry_app_message_id() {
        // monorepo#1157: a user row persisted with a client-minted
        // `userAppMessageId` serves it top-level as `appMessageId` on the
        // re-read; the live delta entities must lift it so subscribers can
        // dedup optimistic user rows by id on the delta path — every entity
        // of the row carries it.
        let conv = json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "user-msg-1",
                    "role": "user",
                    "seq": 4,
                    "timestamp": "2026-07-30T00:00:00Z",
                    "appMessageId": "app-msg-1",
                    "contentBlocks": [
                        { "type": "text", "text": "optimistic send" },
                        { "type": "image", "data": "abc", "mimeType": "image/png" }
                    ]
                }
            ],
            "truncated": false,
            "totalMessages": 5,
            "nextToken": Value::Null,
        });
        let api = ConvApi::new(conv);
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row agent:message must map to a delta");
        let added = d["added"].as_array().unwrap();
        assert_eq!(added.len(), 2);
        for e in added {
            assert_eq!(
                e["appMessageId"], "app-msg-1",
                "entity must carry the row's appMessageId: {d}"
            );
        }
    }

    #[tokio::test]
    async fn user_row_message_without_app_message_id_omits_the_field() {
        // Rows without a client id keep the lean entity shape — the field is
        // omitted entirely, never serialized as null.
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row agent:message must map to a delta");
        for e in d["added"].as_array().unwrap() {
            assert!(
                e.get("appMessageId").is_none(),
                "rows without a client id omit appMessageId: {e}"
            );
        }
    }

    #[tokio::test]
    async fn assistant_row_message_maps_to_none_without_re_read() {
        // Assistant rows are owned by the stream + terminal reconcile; an
        // `agent:message` echo for one must NOT emit (double-emission guard)
        // and must not even cost a conversation read.
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "asst-msg-1", "assistant"))
            .await;
        assert!(d.is_none());
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_message_id_maps_to_none() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-GONE", "user"))
            .await;
        assert!(d.is_none(), "re-read miss must emit no frame");
    }

    #[tokio::test]
    async fn redelivery_upserts_known_blocks_as_updated() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        let e = message_event("agent-1", "user-msg-1", "user");
        let first = s.delta(&api, &e).await.expect("first delivery");
        assert_eq!(first["added"].as_array().unwrap().len(), 2);
        let second = s.delta(&api, &e).await.expect("re-delivery");
        assert!(
            second["added"].as_array().unwrap().is_empty(),
            "known block ids re-route to updated: {second}"
        );
        assert_eq!(second["updated"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn mid_turn_user_row_does_not_corrupt_assistant_accumulation() {
        // In-flight assistant text is accumulating when an interrupt-priority
        // user row lands. The user-row delta must not disturb the text
        // accumulator, and the terminal reconcile must NOT remove the user
        // blocks (they were never part of the assistant turn's emitted set).
        let conv = json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "user-msg-1",
                    "role": "user",
                    "seq": 4,
                    "timestamp": "2026-07-30T00:00:00Z",
                    "contentBlocks": [ { "type": "text", "text": "interrupt!" } ]
                },
                {
                    "id": "msg-1",
                    "role": "assistant",
                    "seq": 5,
                    "timestamp": "2026-07-30T00:00:01Z",
                    "contentBlocks": [
                        { "type": "text", "id": "msg-1:0", "text": "Hello, world" }
                    ]
                }
            ],
            "truncated": false,
            "totalMessages": 6,
            "nextToken": Value::Null,
        });
        let api = ConvApi::new(conv);
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        // Turn in flight: first assistant chunk accumulated.
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("chunk 1");
        // User row lands mid-turn.
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-1", "user"))
            .await
            .expect("user-row delta");
        assert_eq!(d["added"][0]["role"], "user");
        // The next chunk still carries the FULL accumulated assistant text.
        let d2 = s
            .chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
            .expect("chunk 2");
        assert_eq!(
            d2["updated"][0]["block"]["text"], "Hello, world",
            "user-row emission must not reset the text accumulator"
        );
        // Terminal reconcile: user block ids are NOT orphan-removed.
        let end = Event {
            event_type: intent_core::events::AGENT_STREAM_END.to_string(),
            ..message_event("agent-1", "msg-1", "assistant")
        };
        let terminal = s.delta(&api, &end).await.expect("terminal reconcile");
        assert_eq!(
            terminal["removedIds"],
            json!([]),
            "user-row block ids must never surface as orphans: {terminal}"
        );
        assert_eq!(terminal["updated"][0]["block"]["text"], "Hello, world");
        assert_eq!(terminal["updated"][0]["role"], "assistant");
    }
}

// --- chat_snapshot — the interrupt-flush drop window (monorepo#2056) -------

mod chat_snapshot_interrupt_window {
    use super::*;
    use intent_core::{BoxFuture, Result, WorkspaceApi};

    /// The three states an interrupted turn passes through, in order, as
    /// `AgentManager::interrupt_inner` / `detach_with_redelivery` tear it down.
    /// The busy claim is held across ALL of them — it is released by `end_turn`
    /// only after the flush — so `agent_is_busy` is `true` throughout.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Phase {
        /// Mid-turn: the live-turn slot carries the partial content, nothing
        /// is persisted yet.
        Streaming,
        /// The window: `worker.abort()` dropped the `LiveTurnGuard` and
        /// `flush_partial_turn_on_interruption` has not yet written the
        /// interrupted assistant row. The teardown path pinned the slot before
        /// the abort (monorepo#2056), so it is still published here.
        PinnedRowNotYetPersisted,
        /// The flush landed: the interrupted row is in the conversation page
        /// and the flush cleared the slot (releasing the pin).
        Flushed,
    }

    /// Minimal `WorkspaceApi` that replays the teardown sequence above
    /// deterministically — no sleeps, no scheduler races: the test steps the
    /// phase by hand and takes a snapshot at each step.
    struct InterruptWindowApi {
        phase: std::sync::Mutex<Phase>,
    }

    impl InterruptWindowApi {
        fn new() -> Self {
            Self {
                phase: std::sync::Mutex::new(Phase::Streaming),
            }
        }

        fn set(&self, phase: Phase) {
            *self.phase.lock().unwrap() = phase;
        }

        fn phase(&self) -> Phase {
            *self.phase.lock().unwrap()
        }
    }

    impl WorkspaceApi for InterruptWindowApi {
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            let flushed = self.phase() == Phase::Flushed;
            Box::pin(async move {
                let mut messages = vec![json!({
                    "id": "m-0",
                    "role": "user",
                    "seq": 0,
                    "contentBlocks": [{ "id": "m-0:0", "type": "text", "text": "Run the tests" }],
                })];
                if flushed {
                    messages.push(json!({
                        "id": "msg-live",
                        "role": "assistant",
                        "seq": 1,
                        "contentBlocks": [
                            { "id": "msg-live:0", "type": "text", "text": "I'll run " }
                        ],
                        "metadata": { "interrupted": true, "interruptReason": "user_stop" },
                    }));
                }
                let total = messages.len();
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": messages,
                    "truncated": false,
                    "totalMessages": total,
                    "nextToken": Value::Null,
                }))
            })
        }

        /// Held across the whole teardown: `end_turn` runs only after the
        /// flush, so the busy gate never opens inside the window.
        fn agent_is_busy(&self, _agent_id: AgentId) -> bool {
            true
        }

        /// Published until the flush clears it: mid-turn by the streaming
        /// path, and across the abort→flush window by the teardown path's pin
        /// (`Services::pin_live_turn`, which makes the slot survive the
        /// `LiveTurnGuard` drop).
        fn agent_live_turn(&self, _agent_id: AgentId) -> Option<Value> {
            (self.phase() != Phase::Flushed).then(|| {
                json!({
                    "messageId": "msg-live",
                    "contentBlocks": [
                        { "id": "msg-live:0", "type": "text", "text": "I'll run " }
                    ],
                })
            })
        }
    }

    /// A genuinely orphaned slot: content is still published but no worker is
    /// in flight (the turn died with no flush behind it, and the busy claim is
    /// gone). Its content is real and is served — just never as "streaming".
    struct OrphanSlotApi {
        /// `false` for a turn that died before streaming anything.
        populated: bool,
    }

    impl WorkspaceApi for OrphanSlotApi {
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": [{
                        "id": "m-0",
                        "role": "user",
                        "seq": 0,
                        "contentBlocks": [{ "id": "m-0:0", "type": "text", "text": "Run the tests" }],
                    }],
                    "truncated": false,
                    "totalMessages": 1,
                    "nextToken": Value::Null,
                }))
            })
        }

        fn agent_is_busy(&self, _agent_id: AgentId) -> bool {
            false
        }

        fn agent_live_turn(&self, _agent_id: AgentId) -> Option<Value> {
            let blocks = if self.populated {
                json!([{ "id": "msg-orphan:0", "type": "text", "text": "I'll run " }])
            } else {
                json!([])
            };
            Some(json!({
                "messageId": "msg-orphan",
                "contentBlocks": blocks,
            }))
        }
    }

    /// The permanent variant of the same hole: `flush_partial_turn_on_interruption`
    /// hit a non-UNIQUE store error, so it warned and deliberately KEPT the slot
    /// as the only copy of the content — then `end_turn` released the busy claim.
    /// The conversation page never gains the row, and the slot never goes away
    /// on its own.
    struct FlushFailureApi {
        /// `true` once `end_turn` has released the busy claim (post-flush-failure).
        flush_failed: std::sync::atomic::AtomicBool,
    }

    impl WorkspaceApi for FlushFailureApi {
        /// The interrupted assistant row is NEVER written — the store rejected it.
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": [{
                        "id": "m-0",
                        "role": "user",
                        "seq": 0,
                        "contentBlocks": [{ "id": "m-0:0", "type": "text", "text": "Run the tests" }],
                    }],
                    "truncated": false,
                    "totalMessages": 1,
                    "nextToken": Value::Null,
                }))
            })
        }

        fn agent_is_busy(&self, _agent_id: AgentId) -> bool {
            !self.flush_failed.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Kept forever: on a store error the flush leaves the slot in place
        /// precisely because it is the only copy of the streamed content.
        fn agent_live_turn(&self, _agent_id: AgentId) -> Option<Value> {
            Some(json!({
                "messageId": "msg-live",
                "contentBlocks": [
                    { "id": "msg-live:0", "type": "text", "text": "I'll run " }
                ],
            }))
        }
    }

    /// Replays the exact interleaving from the #1161 review: a following turn
    /// claims `busy` in the instant BETWEEN the snapshot's two reads, while the
    /// slot still holds the PREVIOUS turn's flush-failure orphan (its worker
    /// replaces the slot only in `begin_live_turn`, many awaits later).
    ///
    /// The claim is modelled as a side effect of the SLOT read: whichever read
    /// `chat_snapshot` performs first, the claim lands immediately after it. So
    /// the order is what the assertion actually pins — read the slot first and
    /// the busy read that follows returns the new turn's claim, labelling the old
    /// turn's content `isStreaming: true`; read busy first and it cannot be
    /// influenced by the claim at all.
    struct ClaimsBusyBetweenReadsApi {
        busy: std::sync::atomic::AtomicBool,
    }

    impl WorkspaceApi for ClaimsBusyBetweenReadsApi {
        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "agentId": agent_id.as_str(),
                    "messages": [{
                        "id": "m-0",
                        "role": "user",
                        "seq": 0,
                        "contentBlocks": [{ "id": "m-0:0", "type": "text", "text": "Run the tests" }],
                    }],
                    "truncated": false,
                    "totalMessages": 1,
                    "nextToken": Value::Null,
                }))
            })
        }

        fn agent_is_busy(&self, _agent_id: AgentId) -> bool {
            self.busy.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Reading the slot lets the next turn win the claim right afterwards.
        fn agent_live_turn(&self, _agent_id: AgentId) -> Option<Value> {
            self.busy.store(true, std::sync::atomic::Ordering::SeqCst);
            Some(json!({
                "messageId": "msg-previous-turn",
                "contentBlocks": [
                    { "id": "msg-previous-turn:0", "type": "text", "text": "I'll run " }
                ],
            }))
        }
    }

    fn assistant_ids(snap: &Value) -> Vec<String> {
        snap["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["role"] == "assistant")
            .map(|m| m["id"].as_str().unwrap().to_string())
            .collect()
    }

    /// monorepo#2056 — a `chat.subscribe` landing anywhere in an interrupt
    /// teardown serves the partial turn, with no gap: mid-turn from the live
    /// slot, across the abort→flush window from the SAME slot (kept published
    /// by the teardown path's pin, since the interrupted row is not durable
    /// yet), and after the flush from the persisted row. Before the pin the
    /// middle snapshot carried no in-flight message at all, so a
    /// wholesale-rebuilding reconciler dropped the whole partial turn — and
    /// never healed, because a subscription that merged nothing learns no
    /// `messageId` to reconcile the terminal `agent:stream:end` against.
    ///
    /// Note which gate is load-bearing: `agent_is_busy` is `true` in all three
    /// phases (the busy slot is released by `end_turn` only AFTER the flush),
    /// so the hypothesized "busy cleared before the row persisted" ordering was
    /// never what opened the window — the live-turn slot vanishing before the
    /// row landed was.
    #[tokio::test]
    async fn chat_snapshot_keeps_the_partial_turn_across_the_abort_to_flush_window() {
        let api = InterruptWindowApi::new();

        // Phase 1 — mid-turn: the partial turn is served from the live slot.
        let mid = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(assistant_ids(&mid), vec!["msg-live".to_string()]);
        assert_eq!(mid["messages"][1]["isStreaming"], true);

        // Phase 2 — the window: the guard dropped on abort and the row is not
        // yet written, but the pinned slot is still published, so the snapshot
        // carries the same in-flight message.
        api.set(Phase::PinnedRowNotYetPersisted);
        let gap = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(
            assistant_ids(&gap),
            vec!["msg-live".to_string()],
            "monorepo#2056: the partial turn survives the abort→flush window: {gap}"
        );
        assert_eq!(
            gap["messages"][1]["contentBlocks"],
            json!([{ "id": "msg-live:0", "type": "text", "text": "I'll run " }]),
            "…with the streamed-so-far blocks intact: {gap}"
        );
        assert_eq!(
            gap["messages"][1]["isStreaming"], true,
            "…still flagged in-flight (the turn's busy claim is still held): {gap}"
        );
        assert_eq!(
            gap["totalMessages"], 2,
            "…and counted, so the client's seq stays contiguous: {gap}"
        );

        // Phase 3 — the flush landed: the row is durable and the flush cleared
        // the slot, so the content is served ONCE, as a persisted,
        // NON-streaming row.
        api.set(Phase::Flushed);
        let after = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(assistant_ids(&after), vec!["msg-live".to_string()]);
        assert_eq!(
            after["totalMessages"], 2,
            "the persisted row replaces the overlay rather than duplicating it: {after}"
        );
        assert!(
            after["messages"][1].get("isStreaming").is_none(),
            "the persisted interrupted row carries no streaming hint: {after}"
        );
    }

    /// monorepo#2104 — deliberately supersedes #1150's
    /// `chat_snapshot_never_merges_an_orphaned_slot_from_an_idle_agent`, which
    /// asserted an orphaned slot is not merged AT ALL. The objection it encoded
    /// was to labelling orphan content `isStreaming`, not to showing it: a
    /// crashed or failed-to-flush turn's partial output is real content the user
    /// watched arrive. So the merge now always happens and `agent_is_busy` only
    /// decides the flag — the invariant that survives, asserted below, is that an
    /// orphaned slot never claims to be streaming.
    #[tokio::test]
    async fn chat_snapshot_serves_an_orphaned_slot_as_a_non_streaming_message() {
        let snap = chat_snapshot(&OrphanSlotApi { populated: true }, &agent(), None, None).await;
        assert_eq!(
            assistant_ids(&snap),
            vec!["msg-orphan".to_string()],
            "an orphan slot's streamed content is real and must be served: {snap}"
        );
        assert_eq!(
            snap["messages"][1]["isStreaming"], false,
            "…but never as a phantom streaming message: {snap}"
        );
        assert_eq!(
            snap["messages"][1]["contentBlocks"],
            json!([{ "id": "msg-orphan:0", "type": "text", "text": "I'll run " }]),
            "…with the streamed-so-far blocks intact: {snap}"
        );
        assert_eq!(
            snap["totalMessages"], 2,
            "…and counted, so the client's seq stays contiguous: {snap}"
        );
    }

    /// The other half of that invariant: an orphaned slot with nothing streamed
    /// adds no blank assistant row (there is no content to rescue, and an empty
    /// bubble is strictly worse than nothing).
    #[tokio::test]
    async fn chat_snapshot_skips_an_empty_orphaned_slot() {
        let snap = chat_snapshot(&OrphanSlotApi { populated: false }, &agent(), None, None).await;
        assert!(
            assistant_ids(&snap).is_empty(),
            "an empty orphan slot must not surface at all: {snap}"
        );
        assert_eq!(snap["totalMessages"], 1, "…and does not inflate the count");
    }

    /// #1161 review, medium: the busy read must come BEFORE the slot read.
    ///
    /// Both reads take their own lock, so a following turn can claim `busy`
    /// between them. In the reversed order the snapshot would pair the previous
    /// turn's orphaned content with the new turn's claim and serve it as
    /// `isStreaming: true` — exactly the phantom-streaming state the merge rule
    /// promises to prevent, and the one case where showing orphan content could
    /// be worse than hiding it. Reading busy first makes that pairing
    /// unrepresentable, and `try_begin` clears a stale slot under the busy lock
    /// before publishing the claim, so `busy == true` implies the stale slot is
    /// already gone.
    #[tokio::test]
    async fn chat_snapshot_reads_busy_before_the_slot_so_a_new_claim_cannot_gild_stale_content() {
        let api = ClaimsBusyBetweenReadsApi {
            busy: std::sync::atomic::AtomicBool::new(false),
        };

        let snap = chat_snapshot(&api, &agent(), None, None).await;

        assert_eq!(
            assistant_ids(&snap),
            vec!["msg-previous-turn".to_string()],
            "the orphaned content is still served: {snap}"
        );
        assert_eq!(
            snap["messages"][1]["isStreaming"], false,
            "a turn claiming busy after the snapshot's busy read must NOT relabel \
             the previous turn's content as streaming: {snap}"
        );
    }

    /// monorepo#2104, the case that makes this worth changing: the flush hit a
    /// non-UNIQUE store error, kept the slot as the only copy of the content, and
    /// `end_turn` then cleared busy. Under the old busy-as-merge-gate the daemon
    /// held real streamed output that NO snapshot could ever show — permanently,
    /// not for a millisecond. Now it is visible, as a non-streaming message.
    #[tokio::test]
    async fn chat_snapshot_shows_content_the_failed_flush_left_in_the_slot() {
        let api = FlushFailureApi {
            flush_failed: std::sync::atomic::AtomicBool::new(false),
        };

        // Mid-turn: served from the slot as usual, flagged in-flight.
        let mid = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(assistant_ids(&mid), vec!["msg-live".to_string()]);
        assert_eq!(mid["messages"][1]["isStreaming"], true);

        // The flush failed and `end_turn` released the busy claim. The row is
        // not in the page and never will be; the slot is the only copy.
        api.flush_failed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let after = chat_snapshot(&api, &agent(), None, None).await;
        assert_eq!(
            assistant_ids(&after),
            vec!["msg-live".to_string()],
            "content kept in the slot by a failed flush must stay visible: {after}"
        );
        assert_eq!(
            after["messages"][1]["contentBlocks"],
            json!([{ "id": "msg-live:0", "type": "text", "text": "I'll run " }]),
            "…with the streamed-so-far blocks intact: {after}"
        );
        assert_eq!(
            after["messages"][1]["isStreaming"], false,
            "…and not as a phantom streaming message — the turn is over: {after}"
        );
        assert_eq!(after["totalMessages"], 2);
    }
}

/// monorepo#2105 — the terminal reconcile falls back to the `stream:end`
/// payload's `messageId` when the turn's id was never learned live, so a
/// subscription whose seq-0 snapshot missed the in-flight message self-heals at
/// turn end instead of staying short a whole turn until the client resubscribes.
mod chat_terminal_message_id_fallback {
    use super::*;
    use intent_core::events::AGENT_STREAM_END;
    use intent_core::{BoxFuture, Result, WorkspaceApi};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal `WorkspaceApi` serving the persisted transcript the terminal
    /// reconcile re-reads, counting reads.
    struct ConvApi {
        calls: AtomicUsize,
        conversation: Value,
    }

    impl ConvApi {
        fn new(conversation: Value) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                conversation,
            }
        }
    }

    impl WorkspaceApi for ConvApi {
        fn agent_get_conversation(
            &self,
            _agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let conv = self.conversation.clone();
            Box::pin(async move { Ok(conv) })
        }
    }

    /// The turn's persisted assistant row (plus the user row that prompted it).
    fn conversation() -> Value {
        json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "user-msg-1",
                    "role": "user",
                    "seq": 7,
                    "timestamp": "2026-08-11T00:00:00Z",
                    "contentBlocks": [ { "type": "text", "id": "user-msg-1:0", "text": "Run it" } ]
                },
                {
                    "id": "msg-1",
                    "role": "assistant",
                    "seq": 8,
                    "timestamp": "2026-08-11T00:00:01Z",
                    "contentBlocks": [
                        { "type": "text", "id": "msg-1:0", "text": "Working on it" },
                        { "type": "text", "id": "msg-1:1", "text": "Interrupted" }
                    ]
                }
            ],
            "truncated": false,
            "totalMessages": 9,
            "nextToken": Value::Null,
        })
    }

    /// The terminal `agent:stream:end` as `run_prompt_turn` /
    /// `flush_partial_turn_on_interruption` emit it: `messageId` present when
    /// the turn persisted an assistant row, absent when it did not.
    fn end_event(message_id: Option<&str>) -> Event {
        let mut data = json!({ "agentId": "agent-1" });
        if let Some(id) = message_id {
            data["messageId"] = json!(id);
        }
        Event {
            id: "evt-end".into(),
            event_type: AGENT_STREAM_END.to_string(),
            timestamp: now_iso(),
            workspace_id: WorkspaceId::from("w"),
            session_id: Some("agent-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            data,
        }
    }

    #[tokio::test]
    async fn snapshot_miss_recovers_the_whole_turn_at_stream_end() {
        // The subscription opened inside the interrupt window (monorepo#2056):
        // the seq-0 snapshot carried no in-flight message, so `seed_from_snapshot`
        // learned no id, and no further chunk arrives for the already-dead turn.
        // Pre-fix the terminal event mapped to `None` and the partial turn stayed
        // missing for the rest of the subscription's life.
        let api = ConvApi::new(conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.seed_from_snapshot(&conversation()); // no `isStreaming` row → no id learned
        let d = s
            .delta(&api, &end_event(Some("msg-1")))
            .await
            .expect("stream:end must reconcile against the event's messageId");
        assert_eq!(api.calls.load(Ordering::SeqCst), 1, "one bounded re-read");
        let added = d["added"].as_array().unwrap();
        assert_eq!(added.len(), 2, "every persisted block is delivered: {d}");
        for e in added {
            assert_eq!(e["agentId"], "agent-1");
            assert_eq!(e["messageId"], "msg-1");
            assert_eq!(e["role"], "assistant");
            assert_eq!(e["messageSeq"], 8);
            assert_eq!(e["timestamp"], "2026-08-11T00:00:01Z");
            assert_eq!(e["streamingComplete"], true);
        }
        assert_eq!(added[0]["block"]["id"], "msg-1:0");
        assert_eq!(added[1]["block"]["text"], "Interrupted");
        assert!(
            d["updated"].as_array().unwrap().is_empty(),
            "nothing was emitted live, so nothing is an update: {d}"
        );
        assert_eq!(
            d["removedIds"],
            json!([]),
            "no live-emitted ids means no orphans: {d}"
        );
    }

    #[tokio::test]
    async fn the_learned_id_wins_and_the_turn_is_delivered_once() {
        // The normal case is untouched: the id learned from the live stream is
        // used (not the event's), the blocks the client already saw come back as
        // `updated` — one terminal frame, no re-added duplicates.
        let api = ConvApi::new(conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Working")))
            .expect("first chunk");
        let d = s
            .delta(&api, &end_event(Some("other-msg")))
            .await
            .expect("terminal reconcile");
        assert_eq!(
            d["updated"][0]["messageId"], "msg-1",
            "the live-learned id must win over the event payload: {d}"
        );
        assert_eq!(
            d["updated"].as_array().unwrap().len(),
            1,
            "the live-emitted block returns as an update, not an add: {d}"
        );
        assert_eq!(
            d["added"].as_array().unwrap().len(),
            1,
            "msg-1:1 is new: {d}"
        );
        assert_eq!(d["added"][0]["block"]["id"], "msg-1:1");
    }

    #[tokio::test]
    async fn an_end_event_without_a_message_id_still_emits_nothing() {
        // A turn that persisted no assistant row (e.g. an empty interrupted
        // turn) omits `messageId` — there is nothing to reconcile against.
        let api = ConvApi::new(conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        assert!(s.delta(&api, &end_event(None)).await.is_none());
        assert_eq!(
            api.calls.load(Ordering::SeqCst),
            0,
            "no id, no conversation read"
        );
    }

    #[tokio::test]
    async fn the_fallback_id_does_not_leak_into_the_next_turn() {
        // `finalize` resets the per-turn accumulation whichever way the id was
        // resolved, so the recovered id cannot reconcile a later turn.
        let api = ConvApi::new(conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.delta(&api, &end_event(Some("msg-1")))
            .await
            .expect("first turn recovered");
        assert!(
            s.delta(&api, &end_event(None)).await.is_none(),
            "the next turn starts with no id"
        );
    }
}

/// The terminal reconcile must never silently skip the terminal frame: when
/// the `agent.getConversation` re-read fails at turn end (transient store
/// failure), it is retried once, and a persistent failure still emits a
/// best-effort terminal frame — the accumulated live state stamped
/// `streamingComplete: true` — plus a WARN, so the transcript converges
/// instead of staying silently stale while the turn looks ended.
mod chat_terminal_reconcile_failure {
    use super::*;
    use crate::protocol::test_capture::Capture;
    use intent_core::events::AGENT_STREAM_END;
    use intent_core::{BoxFuture, Error, Result, WorkspaceApi};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal `WorkspaceApi` whose conversation read fails the first
    /// `fail_first` calls, then serves `conversation` (a `Value::Null`
    /// conversation keeps failing forever). Counts the attempts.
    struct FailingConvApi {
        calls: AtomicUsize,
        fail_first: usize,
        conversation: Value,
    }

    impl FailingConvApi {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first: usize::MAX,
                conversation: Value::Null,
            }
        }

        fn failing_once_then(conversation: Value) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_first: 1,
                conversation,
            }
        }
    }

    impl WorkspaceApi for FailingConvApi {
        fn agent_get_conversation(
            &self,
            _agent_id: AgentId,
            _limit: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let conv = self.conversation.clone();
            let fail = call < self.fail_first;
            Box::pin(async move {
                if fail {
                    Err(Error::Internal("store read failed".to_string()))
                } else {
                    Ok(conv)
                }
            })
        }
    }

    /// The turn's persisted assistant row, as served once a read succeeds.
    fn conversation() -> Value {
        json!({
            "agentId": "agent-1",
            "messages": [
                {
                    "id": "msg-1",
                    "role": "assistant",
                    "seq": 8,
                    "timestamp": "2026-08-11T00:00:01Z",
                    "contentBlocks": [
                        { "type": "text", "id": "msg-1:0", "text": "Hello, world" }
                    ]
                }
            ],
            "truncated": false,
            "totalMessages": 9,
            "nextToken": Value::Null,
        })
    }

    fn end_event(message_id: &str) -> Event {
        Event {
            id: "evt-end".into(),
            event_type: AGENT_STREAM_END.to_string(),
            timestamp: now_iso(),
            workspace_id: WorkspaceId::from("w"),
            session_id: Some("agent-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            data: json!({ "agentId": "agent-1", "messageId": message_id }),
        }
    }

    #[tokio::test]
    async fn a_failed_re_read_retries_once_then_emits_the_best_effort_frame() {
        let api = FailingConvApi::new();
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("first chunk");
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
            .expect("second chunk");
        s.tool_delta(&tool_event("msg-1", "msg-1:1", "call-1", "started", None))
            .expect("tool call");

        let capture = Capture::default();
        let d = {
            let _guard = tracing::subscriber::set_default(capture.clone());
            s.delta(&api, &end_event("msg-1"))
                .await
                .expect("a failed reconcile must still emit a terminal frame")
        };

        assert_eq!(
            api.calls.load(Ordering::SeqCst),
            2,
            "the re-read is retried exactly once"
        );
        let updated = d["updated"].as_array().unwrap();
        assert_eq!(
            updated.len(),
            2,
            "every live-accumulated block returns in the frame: {d}"
        );
        for e in updated {
            assert_eq!(e["messageId"], "msg-1");
            assert_eq!(e["role"], "assistant");
            assert_eq!(
                e["streamingComplete"], true,
                "the frame must flip streamingComplete: {d}"
            );
            assert!(
                e.get("messageSeq").is_none(),
                "no authoritative seq without the store read: {d}"
            );
        }
        assert_eq!(
            updated[0]["block"]["text"], "Hello, world",
            "text blocks carry the FULL accumulated text: {d}"
        );
        assert_eq!(updated[1]["block"]["type"], "tool_use");
        assert_eq!(d["added"], json!([]));
        assert_eq!(
            d["removedIds"],
            json!([]),
            "no orphan is provable without the persisted message: {d}"
        );
        assert!(
            capture
                .lines()
                .iter()
                .any(|(level, line)| *level == tracing::Level::WARN
                    && line.contains("terminal reconcile")),
            "the persistent failure logs a WARN: {:?}",
            capture.lines()
        );
    }

    #[tokio::test]
    async fn the_per_turn_state_resets_after_the_best_effort_frame() {
        let api = FailingConvApi::new();
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("first chunk");
        s.delta(&api, &end_event("msg-1"))
            .await
            .expect("terminal frame");

        // The next turn starts clean: its first chunk arrives as `added` with
        // the text restarted from the fragment…
        let d = s
            .chunk_delta(&chunk_event("msg-2", "msg-2:0", "text", json!("Next")))
            .expect("next turn chunk");
        assert_eq!(d["added"][0]["block"]["text"], "Next");
        // …and its own failed reconcile carries only ITS accumulated blocks.
        let d = s
            .delta(&api, &end_event("msg-2"))
            .await
            .expect("terminal frame");
        let updated = d["updated"].as_array().unwrap();
        assert_eq!(
            updated.len(),
            1,
            "the previous turn's live state was cleared: {d}"
        );
        assert_eq!(updated[0]["block"]["id"], "msg-2:0");
        assert_eq!(updated[0]["messageId"], "msg-2");
    }

    /// The terminal reconcile is encoding-independent (monorepo#2675): it
    /// re-reads the persisted message and emits authoritative FULL blocks
    /// (`text`, never `textDelta`), converging the client whatever the live
    /// encoding was (§7.1 / CS-3).
    #[tokio::test]
    async fn incremental_terminal_reconcile_emits_full_authoritative_blocks() {
        let api = FailingConvApi::failing_once_then(conversation());
        api.calls.store(1, Ordering::SeqCst); // consume the failing call
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Incremental, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("first chunk");
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
            .expect("second chunk");
        let d = s
            .delta(&api, &end_event("msg-1"))
            .await
            .expect("terminal reconcile");
        let updated = d["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 1);
        assert_eq!(
            updated[0]["block"]["text"], "Hello, world",
            "the terminal frame carries the persisted FULL text: {d}"
        );
        assert!(
            updated[0]["block"].get("textDelta").is_none(),
            "the terminal frame is never incremental: {d}"
        );
        assert_eq!(updated[0]["streamingComplete"], true);
    }

    /// The degraded best-effort terminal frame (re-read failed twice) also
    /// carries authoritative FULL text in incremental mode — a fragment there
    /// would clobber the client's accumulation. This is why the mapper keeps
    /// `text_acc` up to date in incremental mode too.
    #[tokio::test]
    async fn incremental_best_effort_terminal_carries_the_full_accumulated_text() {
        let api = FailingConvApi::new();
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Incremental, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("first chunk");
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!(", world")))
            .expect("second chunk");
        let d = s
            .delta(&api, &end_event("msg-1"))
            .await
            .expect("a failed reconcile must still emit a terminal frame");
        let updated = d["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 1);
        assert_eq!(
            updated[0]["block"]["text"], "Hello, world",
            "the degraded frame rebuilds the FULL text from text_acc: {d}"
        );
        assert!(updated[0]["block"].get("textDelta").is_none());
        assert_eq!(updated[0]["streamingComplete"], true);
    }

    #[tokio::test]
    async fn a_transient_failure_recovers_on_the_retry_with_the_authoritative_frame() {
        // First read fails, the retry succeeds → the AUTHORITATIVE terminal
        // frame is emitted (persisted seq/timestamp), not the degraded one.
        let api = FailingConvApi::failing_once_then(conversation());
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.chunk_delta(&chunk_event("msg-1", "msg-1:0", "text", json!("Hello")))
            .expect("first chunk");
        let d = s
            .delta(&api, &end_event("msg-1"))
            .await
            .expect("terminal frame");

        assert_eq!(
            api.calls.load(Ordering::SeqCst),
            2,
            "the transient failure consumed the one retry"
        );
        let updated = d["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 1, "the persisted block, reconciled: {d}");
        assert_eq!(updated[0]["block"]["text"], "Hello, world");
        assert_eq!(
            updated[0]["messageSeq"], 8,
            "the authoritative frame carries the persisted seq: {d}"
        );
        assert_eq!(updated[0]["timestamp"], "2026-08-11T00:00:01Z");
        assert_eq!(updated[0]["streamingComplete"], true);
    }

    #[tokio::test]
    async fn a_blank_streaming_snapshot_still_gets_a_terminal_entity_on_failure() {
        // monorepo#2105-adjacent: the seq-0 snapshot carried a just-started
        // streaming row with NO blocks yet. If the terminal re-read then fails
        // twice, the degraded frame must still carry one `streamingComplete`
        // entity for that row — an empty frame would leave the blank row
        // permanently streaming.
        let api = FailingConvApi::new();
        let mut s = ChatDeltaState::new(&agent(), DeltaEncoding::Full, None);
        s.seed_from_snapshot(&json!({
            "agentId": "agent-1",
            "messages": [{
                "id": "msg-1",
                "role": "assistant",
                "isStreaming": true,
                "contentBlocks": []
            }],
        }));
        let d = s
            .delta(&api, &end_event("msg-1"))
            .await
            .expect("terminal frame");

        let updated = d["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 1, "one placeholder entity for the row: {d}");
        assert_eq!(updated[0]["block"]["id"], "msg-1:0");
        assert_eq!(updated[0]["block"]["type"], "text");
        assert_eq!(updated[0]["block"]["text"], "");
        assert_eq!(
            updated[0]["streamingComplete"], true,
            "the blank row flips out of streaming: {d}"
        );
    }

    /// The lag-recovery snapshot must never degrade to an empty page: it is
    /// emitted at a LATER seq than content the client already rendered, so an
    /// empty value would rebuild the transcript as blank. It retries the read
    /// once and reports failure (`None`) instead of degrading.
    mod chat_recovery_snapshot_fallible {
        use super::*;

        #[tokio::test]
        async fn a_persistent_read_failure_returns_none_after_one_retry() {
            let api = FailingConvApi::new();
            let snapshot = chat_recovery_snapshot(&api, &agent(), None).await;
            assert!(
                snapshot.is_none(),
                "a persistent failure must NOT degrade to an empty page"
            );
            assert_eq!(
                api.calls.load(Ordering::SeqCst),
                2,
                "the read is retried exactly once"
            );
        }

        #[tokio::test]
        async fn a_transient_failure_recovers_on_the_retry() {
            let api = FailingConvApi::failing_once_then(conversation());
            let snapshot = chat_recovery_snapshot(&api, &agent(), None)
                .await
                .expect("the retry served the page");
            assert_eq!(api.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                snapshot["messages"][0]["contentBlocks"][0]["text"], "Hello, world",
                "the recovered snapshot carries the persisted page: {snapshot}"
            );
        }

        #[tokio::test]
        async fn a_healthy_read_serves_the_page_first_try() {
            let api = FailingConvApi::failing_once_then(conversation());
            api.calls.store(1, Ordering::SeqCst); // consume the failing call
            let before = api.calls.load(Ordering::SeqCst);
            let snapshot = chat_recovery_snapshot(&api, &agent(), None)
                .await
                .expect("healthy read");
            assert_eq!(
                api.calls.load(Ordering::SeqCst) - before,
                1,
                "exactly one bounded page read on the happy path"
            );
            assert_eq!(snapshot["messages"][0]["id"], "msg-1");
        }
    }
}
