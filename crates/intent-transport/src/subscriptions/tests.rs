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
        "pr:linked",
        "pr:updated",
        "pr:unlinked",
    ] {
        assert!(ws.iter().any(|s| s == t), "workspace missing {t}");
    }
    assert_eq!(ws.len(), 9);
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
fn chat_tool_delta_synthesizes_name_and_acp_title_from_event() {
    // The delta stream must produce the same shape `record_tool` persists so
    // seq-0 snapshot and live deltas agree. `toolName` is the real derived
    // tool name on the wire and the raw ACP title travels separately as
    // `title`; the block's `name` is the toolName verbatim and the title is
    // echoed as `input._acpTitle`.
    let mut s = ChatDeltaState::new(&agent());
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
    let mut s = ChatDeltaState::new(&agent());
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
    let mut s = ChatDeltaState::new(&agent());
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
    let mut s = ChatDeltaState::new(&agent());
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
    let mut s = ChatDeltaState::new(&agent());
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
    let mut s = ChatDeltaState::new(&agent());
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

// --- task_delta re-read arm (channel-mapping regression) ------------------

mod task_delta_re_read {
    use super::*;
    use intent_core::{
        BoxFuture, ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, Result,
        TaskMetadata, WorkspaceApi, WorkspaceId,
    };

    /// Minimal `WorkspaceApi` that returns a pre-canned [`Note`] from `get_note`
    /// (or `NotFound` when the fixture is `None`). Everything else falls
    /// through to the trait defaults, which is fine because `task_delta` only
    /// touches `get_note`.
    struct StaticNoteApi(Option<Note>);

    impl WorkspaceApi for StaticNoteApi {
        fn get_note(
            &self,
            _workspace_id: WorkspaceId,
            _note_id: NoteId,
        ) -> BoxFuture<'_, Result<Note>> {
            let note = self.0.clone();
            Box::pin(async move { note.ok_or_else(|| Error::NotFound("note".to_string())) })
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
        let api = StaticNoteApi(Some(note_with(None)));
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "n-1"))
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
        let api = StaticNoteApi(Some(note_with(None)));
        let d = task_delta(&api, &ws(), &note_event(TASK_STATUS_CHANGED, "n-1"))
            .await
            .expect("delta must be emitted");
        assert_eq!(d["removedIds"][0], "n-1");
    }

    #[tokio::test]
    async fn note_updated_still_emits_updated_when_task_present() {
        // Regression guard: the removal path must not swallow legitimate
        // `updated` deltas when the note is still a task.
        let api = StaticNoteApi(Some(note_with(Some(TaskMetadata::default()))));
        let d = task_delta(&api, &ws(), &note_event(NOTE_UPDATED, "n-1"))
            .await
            .expect("delta must be emitted");
        assert_eq!(d["updated"][0]["id"], "n-1");
        assert!(d.get("removedIds").is_none());
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
        let snap = chat_snapshot(&api, &agent()).await;
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
        // Activity flags are overlaid (all-false via the trait default here).
        assert_eq!(snap["isResponding"], false);
        assert_eq!(snap["waitingForAgentIds"], json!([]));
    }

    #[tokio::test]
    async fn chat_snapshot_merges_live_turn_on_truncated_page() {
        let api = BoundedPageApi::new(true);
        let snap = chat_snapshot(&api, &agent()).await;
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
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
        let d = s
            .delta(&api, &message_event("agent-1", "asst-msg-1", "assistant"))
            .await;
        assert!(d.is_none());
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_message_id_maps_to_none() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent());
        let d = s
            .delta(&api, &message_event("agent-1", "user-msg-GONE", "user"))
            .await;
        assert!(d.is_none(), "re-read miss must emit no frame");
    }

    #[tokio::test]
    async fn redelivery_upserts_known_blocks_as_updated() {
        let api = ConvApi::new(user_row_conversation());
        let mut s = ChatDeltaState::new(&agent());
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
        let mut s = ChatDeltaState::new(&agent());
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
