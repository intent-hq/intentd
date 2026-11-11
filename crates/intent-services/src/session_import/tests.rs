use super::*;
use intent_acp::transport::IncomingNotification;

fn update(update: &Value) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".into(),
        params: json!({ "sessionId": "s", "update": update }),
    }
}

#[test]
fn slug_matches_claude_for_short_paths() {
    assert_eq!(
        claude_project_slug("/Users/sam/Projects/ember-cloudflare"),
        "-Users-sam-Projects-ember-cloudflare"
    );
    assert_eq!(claude_project_slug("/a/b_c.d"), "-a-b-c-d");
}

#[test]
fn slug_truncates_long_paths_with_the_sdk_hash() {
    let long = format!("/{}", "a".repeat(250));
    let slug = claude_project_slug(&long);
    let (head, hash) = slug.rsplit_once('-').unwrap();
    assert_eq!(head.len(), 200);
    assert!(head.starts_with("-aaa"));
    // JS: Math.abs(h).toString(36) of the 32-bit `(h << 5) - h + c | 0` hash.
    let mut h: i32 = 0;
    for c in long.chars() {
        h = h.wrapping_shl(5).wrapping_sub(h).wrapping_add(c as i32);
    }
    assert_eq!(hash, base36(h.unsigned_abs()));
}

#[test]
fn rebase_only_rewrites_whole_path_segments() {
    assert_eq!(
        rebase("/src/app", "/src/app", "/wt").as_deref(),
        Some("/wt")
    );
    assert_eq!(
        rebase("/src/app/web", "/src/app", "/wt").as_deref(),
        Some("/wt/web")
    );
    assert_eq!(rebase("/src/application", "/src/app", "/wt"), None);
    assert_eq!(rebase("/other", "/src/app", "/wt"), None);
}

#[test]
fn fork_rewrites_ids_and_cwds_and_keeps_the_source() {
    let dir = crate::test_support::test_tempdir("session-import-");
    let source = dir.path().join("src.jsonl");
    let original = concat!(
        r#"{"type":"user","sessionId":"old","cwd":"/src/app","message":{"content":"/src/app stays"}}"#,
        "\n",
        r#"{"type":"assistant","sessionId":"old","cwd":"/src/app/web"}"#,
        "\n",
        "not json\n",
        r#"{"type":"ai-title","aiTitle":"T","sessionId":"old"}"#,
        "\n",
    );
    std::fs::write(&source, original).unwrap();
    let target = dir.path().join("proj").join("new.jsonl");
    fork_claude_transcript(&source, &target, "new", "/src/app", "/wt").unwrap();
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);
    let rows: Vec<String> = std::fs::read_to_string(&target)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let first: Value = serde_json::from_str(&rows[0]).unwrap();
    assert_eq!(first["sessionId"], "new");
    assert_eq!(first["cwd"], "/wt");
    assert_eq!(first["message"]["content"], "/src/app stays");
    let second: Value = serde_json::from_str(&rows[1]).unwrap();
    assert_eq!(second["cwd"], "/wt/web");
    assert_eq!(rows[2], "not json");
    let third: Value = serde_json::from_str(&rows[3]).unwrap();
    assert_eq!(third["sessionId"], "new");
    assert_eq!(rows.len(), 4);
    // Never overwrites an existing target.
    assert!(fork_claude_transcript(&source, &target, "new", "/src/app", "/wt").is_err());
}

#[test]
fn finds_a_session_in_any_project_folder() {
    let dir = crate::test_support::test_tempdir("session-import-");
    let project = dir.path().join("-p");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("abc-1.jsonl"), "{\"cwd\":\"/p\"}\n").unwrap();
    let found = find_claude_session(dir.path(), "abc-1").unwrap();
    assert_eq!(claude_session_cwd(&found).as_deref(), Some("/p"));
    assert!(find_claude_session(dir.path(), "missing").is_none());
    assert!(!safe_session_id("../etc"));
    assert!(safe_session_id("6eca79e3-7963-44ad-b89e-e781d28df2b9"));
}

#[test]
fn wire_row_fills_title_and_optional_meta() {
    let row = wire_row(
        "claude-code",
        json!({ "sessionId": "s1", "cwd": "/p", "updatedAt": "2026-01-01T00:00:00Z",
                "_meta": { "gitBranch": "main", "messageCount": 4 } }),
    );
    assert_eq!(
        row,
        json!({ "provider": "claude-code", "sessionId": "s1", "title": "s1", "cwd": "/p",
                "updatedAt": "2026-01-01T00:00:00Z", "gitBranch": "main", "messageCount": 4 })
    );
}

#[test]
fn cursor_round_trips_and_rejects_foreign_values() {
    assert_eq!(decode_cursor(None).unwrap(), 0);
    assert_eq!(decode_cursor(Some("o:50")).unwrap(), 50);
    assert!(decode_cursor(Some("offset:50")).is_err());
}

#[test]
fn replay_becomes_user_and_assistant_messages() {
    let notes = vec![
        update(&json!({ "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": "Fix the " } })),
        update(&json!({ "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": "bug" } })),
        update(&json!({ "sessionUpdate": "agent_thought_chunk",
                        "content": { "type": "text", "text": "thinking" } })),
        update(&json!({ "sessionUpdate": "agent_message_chunk",
                        "content": { "type": "text", "text": "Looking." } })),
        update(&json!({ "sessionUpdate": "tool_call", "toolCallId": "t1",
                        "title": "Read", "kind": "read", "status": "pending",
                        "rawInput": { "path": "a.rs" } })),
        update(
            &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                        "status": "completed",
                        "content": [{ "type": "content", "content": { "type": "text", "text": "ok" } }] }),
        ),
        update(&json!({ "sessionUpdate": "agent_message_chunk",
                        "content": { "type": "text", "text": "Done." } })),
        update(&json!({ "sessionUpdate": "plan", "entries": [] })),
        update(&json!({ "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": "Thanks" } })),
    ];
    let messages = crate::agent_session::replay_to_messages(&notes);
    assert_eq!(messages.len(), 3, "{messages:#?}");
    assert_eq!(messages[0].role, "user");
    assert_eq!(
        messages[0].blocks,
        vec![json!({ "type": "text", "text": "Fix the bug" })]
    );
    assert_eq!(messages[1].role, "assistant");
    let types: Vec<&str> = messages[1]
        .blocks
        .iter()
        .map(|b| b["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        ["thinking", "text", "tool_use", "tool_result", "text"]
    );
    assert!(messages[1].blocks.iter().all(|b| b.get("id").is_none()));
    assert_eq!(messages[1].blocks[2]["toolCallId"], "t1");
    assert_eq!(messages[1].blocks[2]["metadata"]["status"], "completed");
    assert_eq!(messages[1].blocks[3]["tool_use_id"], "t1");
    assert_eq!(messages[2].role, "user");
}

#[test]
fn handoff_note_maps_the_folder_and_names_the_workspace() {
    let note = import_handoff_note(
        "Anthropic Claude Code",
        Some("/src/app"),
        Some("/wt"),
        Some("Demo"),
    );
    assert!(note.contains("started in the Anthropic Claude Code CLI"));
    assert!(note.contains("(\"Demo\")"));
    assert!(note.contains("under /src/app now live under /wt"));
    assert!(note.contains("ws.* tools"));
    assert!(note.contains("Uncommitted changes"));
    let same = import_handoff_note("Codex", Some("/wt"), Some("/wt"), None);
    assert!(same.contains("Work in /wt."));
    assert!(!same.contains("now live under"));
}
