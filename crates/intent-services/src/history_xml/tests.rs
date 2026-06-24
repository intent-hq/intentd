//! Unit tests for the `<supervisor>` history XML formatter (parity with the TS
//! `formatHistoryAsXml` / `sanitizeMessagesForHistory`).

use intent_core::{AgentId, AgentMessage};
use serde_json::{json, Value};

use super::{format_history_as_xml, MAX_HISTORY_CHARS};

fn msg(role: &str, content: Value) -> AgentMessage {
    AgentMessage {
        id: format!("m-{role}"),
        agent_id: AgentId::from("agent-1"),
        seq: 0,
        role: role.to_string(),
        content,
        created_at: "2024-01-01T00:00:00Z".to_string(),
    }
}

#[test]
fn empty_input_renders_empty_string() {
    assert_eq!(format_history_as_xml(&[], MAX_HISTORY_CHARS), "");
}

#[test]
fn wraps_exchanges_in_supervisor_with_text_blocks() {
    let messages = vec![
        msg("user", json!([{ "type": "text", "text": "hello" }])),
        msg("assistant", json!([{ "type": "text", "text": "hi there" }])),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.starts_with("<supervisor>\n"));
    assert!(xml.ends_with("</supervisor>"));
    assert!(xml.contains("<exchange>\n"));
    assert!(xml.contains("  <user_request_or_tool_results>\n    <text>hello</text>\n"));
    assert!(xml.contains("  <agent_response_or_tool_uses>\n    <text>hi there</text>\n"));
}

#[test]
fn escapes_xml_special_characters() {
    let messages = vec![msg(
        "user",
        json!([{ "type": "text", "text": "a < b & c > d \"q\"" }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<text>a &lt; b &amp; c &gt; d &quot;q&quot;</text>"));
}

#[test]
fn renders_tool_use_and_tool_result_blocks() {
    let messages = vec![
        msg(
            "assistant",
            json!([{
                "type": "tool_use", "name": "edit", "tool_use_id": "t1",
                "input": { "path": "src/lib.rs" }
            }]),
        ),
        msg(
            "user",
            json!([{
                "type": "tool_result", "tool_use_id": "t1",
                "output": "ok", "is_error": false
            }]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<tool_use name=\"edit\" tool_use_id=\"t1\">"));
    // The JSON input is rendered then XML-escaped (quotes → &quot;).
    assert!(xml.contains("{&quot;path&quot;:&quot;src/lib.rs&quot;}"));
    assert!(xml.contains("<tool_result tool_use_id=\"t1\" is_error=\"false\">"));
    assert!(xml.contains("ok"));
}

#[test]
fn sanitizes_malformed_tool_results_and_empty_assistants() {
    let messages = vec![
        // Empty assistant turn → dropped entirely.
        msg("assistant", json!([])),
        msg("user", json!([{ "type": "text", "text": "go" }])),
        msg(
            "assistant",
            json!([
                { "type": "tool_use", "name": "run", "tool_use_id": "a", "input": {} },
                // Missing tool_use_id → dropped.
                { "type": "tool_result", "tool_use_id": "", "output": "x" },
                // Valid.
                { "type": "tool_result", "tool_use_id": "a", "output": "done" },
                // Duplicate id → dropped.
                { "type": "tool_result", "tool_use_id": "a", "output": "dup" },
                // Empty non-error result → dropped.
                { "type": "tool_result", "tool_use_id": "b", "output": "" }
            ]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<tool_result tool_use_id=\"a\" is_error=\"false\">"));
    assert!(xml.contains("done"));
    assert!(!xml.contains("dup"));
    // Exactly one tool_result survived sanitation.
    assert_eq!(xml.matches("<tool_result ").count(), 1);
}

#[test]
fn truncates_oversized_tool_content_in_the_middle() {
    let big = "x".repeat(10_000);
    let messages = vec![msg(
        "user",
        json!([{ "type": "tool_result", "tool_use_id": "t", "output": big }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("characters truncated"));
    assert!(xml.len() < 10_000);
}

#[test]
fn budget_omits_older_exchanges_newest_first() {
    let messages = vec![
        msg("user", json!([{ "type": "text", "text": "OLDEST" }])),
        msg("assistant", json!([{ "type": "text", "text": "a1" }])),
        msg("user", json!([{ "type": "text", "text": "NEWEST" }])),
        msg("assistant", json!([{ "type": "text", "text": "a2" }])),
    ];
    // Budget large enough for the wrapper + one exchange (plus the reserved
    // omission-comment overhead) but not both exchanges.
    let one = format_history_as_xml(&messages[2..], MAX_HISTORY_CHARS);
    let xml = format_history_as_xml(&messages, one.len() + 80);
    assert!(xml.contains("NEWEST"));
    assert!(!xml.contains("OLDEST"));
    assert!(xml.contains("earlier exchanges omitted due to size limits"));
}
