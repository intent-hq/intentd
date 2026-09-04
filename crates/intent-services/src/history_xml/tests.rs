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
        metadata: None,
        app_message_id: None,
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

/// Regression test for STAB-108: dangling `tool_use` blocks (no corresponding
/// `tool_result`) should be removed during sanitization to prevent provider
/// rejection on session resume.
#[test]
fn sanitizes_dangling_tool_use_blocks() {
    let messages = vec![
        msg("user", json!([{ "type": "text", "text": "edit file" }])),
        msg(
            "assistant",
            json!([
                // This tool_use has a result → should be kept.
                { "type": "tool_use", "name": "edit", "tool_use_id": "t1", "input": {} },
                // This tool_use has NO result → should be dropped.
                { "type": "tool_use", "name": "view", "tool_use_id": "t2", "input": {} },
            ]),
        ),
        msg(
            "user",
            json!([
                // Result for t1 → keeps t1 alive.
                { "type": "tool_result", "tool_use_id": "t1", "output": "done" },
                // No result for t2 → t2 should be dropped.
            ]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // t1 with result should appear.
    assert!(xml.contains("<tool_use name=\"edit\" tool_use_id=\"t1\">"));
    assert!(xml.contains("<tool_result tool_use_id=\"t1\" is_error=\"false\">"));
    // t2 without result should NOT appear.
    assert!(!xml.contains("tool_use_id=\"t2\""));
    assert!(!xml.contains("<tool_use name=\"view\""));
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

/// intent#3696: the preamble tells the model that abbreviated tool blocks are
/// a replay artefact (not failed/empty tools) and how to recover one output.
#[test]
fn preamble_carries_truncation_hint_paragraph() {
    let messages = vec![msg("user", json!([{ "type": "text", "text": "hello" }]))];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    let (preamble, _) = xml
        .split_once("<exchange>")
        .expect("preamble before exchanges");
    assert!(preamble.contains("The previous ACP session was lost."));
    assert!(preamble.contains(
        "some tool inputs and tool outputs below are abbreviated by the recovery replay"
    ));
    assert!(preamble.contains("longer than 4000 characters is middle-truncated"));
    assert!(preamble.contains("blocks without that attribute are complete"));
    assert!(preamble.contains("Older exchanges may be omitted"));
    assert!(preamble.contains("does NOT mean the tool failed or returned empty output"));
    assert!(preamble.contains("re-run that ONE call once"));
    assert!(preamble.contains("Do not re-fetch the same inputs repeatedly"));
}

/// intent#3696: an over-cap `tool_result` is marked at the element level
/// (`truncated="true" original_chars="N"`) in addition to the inline marker.
#[test]
fn oversized_tool_result_carries_truncated_attribute_and_marker() {
    let big = "x".repeat(10_000);
    let messages = vec![msg(
        "user",
        json!([{ "type": "tool_result", "tool_use_id": "t", "output": big }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains(
        "<tool_result tool_use_id=\"t\" is_error=\"false\" truncated=\"true\" original_chars=\"10000\">"
    ));
    assert!(xml.contains("\n... [6060 characters truncated] ...\n"));
}

/// intent#3696: `original_chars` and the inline marker count chars, not bytes
/// (a 2-byte `é` repeated 5000 times is 10000 bytes but 5000 chars).
#[test]
fn truncated_attribute_and_marker_count_chars_not_bytes() {
    let multibyte = "é".repeat(5_000);
    assert_eq!(multibyte.len(), 10_000);
    let messages = vec![msg(
        "user",
        json!([{ "type": "tool_result", "tool_use_id": "t", "output": multibyte }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains(
        "<tool_result tool_use_id=\"t\" is_error=\"false\" truncated=\"true\" original_chars=\"5000\">"
    ));
    // 5000 - 2 * 1970 kept chars.
    assert!(xml.contains("\n... [1060 characters truncated] ...\n"));
}

/// intent#3696: an under-cap `tool_result` renders exactly as before — no
/// `truncated` attribute, no marker.
#[test]
fn under_cap_tool_result_has_no_truncated_attribute() {
    let exact = "x".repeat(4_000);
    let messages = vec![msg(
        "user",
        json!([{ "type": "tool_result", "tool_use_id": "t", "output": exact }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // The preamble mentions the attribute/marker by name; only the exchange
    // body must be free of them.
    let (_, body) = xml.split_once("<exchange>").expect("exchange body");
    assert!(body.contains("<tool_result tool_use_id=\"t\" is_error=\"false\">"));
    assert!(!body.contains("truncated="));
    assert!(!body.contains("original_chars="));
    assert!(!body.contains("characters truncated] ..."));
}

/// intent#3696: same element-level marking for an over-cap `tool_use` input,
/// and none for an under-cap one.
#[test]
fn tool_use_input_truncated_attribute_tracks_cap() {
    let big_input = json!({ "path": "x".repeat(10_000) });
    let small_input = json!({ "path": "y".repeat(100) });
    let messages = vec![
        msg(
            "assistant",
            json!([
                { "type": "tool_use", "name": "view", "tool_use_id": "big", "input": big_input },
                { "type": "tool_use", "name": "view", "tool_use_id": "small", "input": small_input },
            ]),
        ),
        msg(
            "user",
            json!([
                { "type": "tool_result", "tool_use_id": "big", "output": "ok" },
                { "type": "tool_result", "tool_use_id": "small", "output": "ok" },
            ]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    let (_, body) = xml.split_once("<exchange>").expect("exchange body");
    // `{"path":"` + 10000 + `"}` = 10011 chars of stringified JSON input.
    assert!(body.contains(
        "<tool_use name=\"view\" tool_use_id=\"big\" truncated=\"true\" original_chars=\"10011\">"
    ));
    assert!(body.contains("<tool_use name=\"view\" tool_use_id=\"small\">"));
    assert_eq!(body.matches("truncated=\"true\"").count(), 1);
    assert_eq!(body.matches("characters truncated] ...").count(), 1);
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

#[test]
fn renders_thinking_block_as_thinking_tag() {
    let messages = vec![msg(
        "assistant",
        json!([{ "type": "thinking", "text": "pondering <x>" }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // Thinking blocks are escaped just like text blocks (TS `escapeXml`).
    assert!(xml.contains("<thinking>pondering &lt;x&gt;</thinking>"));
}

#[test]
fn unknown_block_type_and_unknown_role_are_dropped_silently() {
    let messages = vec![
        msg(
            "user",
            json!([
                { "type": "text", "text": "ok" },
                { "type": "image", "data": "ignored" },
            ]),
        ),
        // Unknown roles fall through the `_ => {}` arm in the exchange grouper.
        msg("system", json!([{ "type": "text", "text": "sys" }])),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<text>ok</text>"));
    assert!(!xml.contains("ignored"));
    assert!(!xml.contains("sys"));
}

#[test]
fn empty_user_and_null_error_content_render_as_empty_wrappers() {
    // Non-assistant turns whose blocks are empty/missing keep their wrapper
    // (sanitize pushes a Msg with empty blocks); the assistant variant is
    // dropped entirely (already covered).
    let messages = vec![msg("user", json!([])), msg("error", Value::Null)];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<user_request_or_tool_results>\n  </user_request_or_tool_results>"));
    assert!(xml.contains("<error>\n  </error>"));
}

#[test]
fn assistant_whose_blocks_all_get_sanitized_is_dropped() {
    // tool_result with empty tool_use_id is dropped → assistant turn ends up
    // with zero clean blocks → entire assistant message is dropped (line 161).
    let messages = vec![msg(
        "assistant",
        json!([{ "type": "tool_result", "tool_use_id": "", "output": "x" }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // The supervisor wrapper still appears, but the (now-empty) exchange list
    // contains no assistant tag and no tool_result element (the preamble's
    // prose mentions `tool_result` by name, so match the element form).
    assert!(xml.starts_with("<supervisor>\n"));
    assert!(!xml.contains("<tool_result"));
    assert!(!xml.contains("agent_response_or_tool_uses"));
}

#[test]
fn tool_use_without_name_or_input_renders_defaults() {
    // Missing `name`/`toolName` → str_field returns "" (the `String::new()`
    // tail); missing/falsy `input` → defaults to `{}` (the `_` match arm).
    let messages = vec![
        msg(
            "assistant",
            json!([{ "type": "tool_use", "tool_use_id": "t1" }]),
        ),
        msg(
            "user",
            json!([{ "type": "tool_result", "tool_use_id": "t1", "output": "ok" }]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<tool_use name=\"\" tool_use_id=\"t1\">"));
    // Empty input object stringifies to `{}` (then XML-escapes to `{}`).
    assert!(xml.contains("  {}\n"));
}

#[test]
fn long_tool_name_is_truncated_with_ellipsis() {
    // tool_name > MAX_TOOL_NAME_CHARS (200) → truncated head + "...".
    let long = "n".repeat(300);
    let messages = vec![
        msg(
            "assistant",
            json!([{ "type": "tool_use", "name": long, "tool_use_id": "t" }]),
        ),
        msg(
            "user",
            json!([{ "type": "tool_result", "tool_use_id": "t", "output": "ok" }]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // 200 cap → 197 chars of head + "..." (3 chars).
    let head: String = "n".repeat(197);
    assert!(xml.contains(&format!("<tool_use name=\"{head}...\" tool_use_id=\"t\">")));
}

#[test]
fn tool_result_falls_back_to_content_when_output_is_null_or_missing() {
    // Output null → sanitize falls through the `_` arm to read `content`
    // (line 143/144); first_truthy on render must then skip the null `output`
    // (is_truthy=false) before returning the truthy `content`.
    let messages = vec![
        msg(
            "assistant",
            json!([
                { "type": "tool_use", "name": "x", "tool_use_id": "n" },
                { "type": "tool_use", "name": "y", "tool_use_id": "m" },
            ]),
        ),
        msg(
            "user",
            json!([
                { "type": "tool_result", "tool_use_id": "n", "output": null, "content": "from-content" },
                { "type": "tool_result", "tool_use_id": "m", "content": "bare-content" },
            ]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("from-content"));
    assert!(xml.contains("bare-content"));
}

#[test]
fn tool_result_is_error_true_with_no_output_renders_empty_content() {
    // is_error=true keeps the block past sanitize even with no output/content;
    // first_truthy then returns None → content defaults to "" (line 101).
    let messages = vec![msg(
        "assistant",
        json!([{ "type": "tool_result", "tool_use_id": "e", "is_error": true }]),
    )];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<tool_result tool_use_id=\"e\" is_error=\"true\">"));
}

#[test]
fn numeric_output_renders_as_json_and_zero_falls_back_to_content() {
    // Number output: is_truthy(42) → true → safe_stringify yields "42" (covers
    // the Number arm of is_truthy and the JSON-encoding arm of safe_stringify).
    // Sanitize's `has_output` check only keeps String/Object/Array outputs, so
    // these blocks ride past it on the `is_error` flag (matching the TS rule).
    let messages = vec![
        msg(
            "assistant",
            json!([
                { "type": "tool_use", "name": "f", "tool_use_id": "n42" },
                { "type": "tool_use", "name": "g", "tool_use_id": "n0" },
            ]),
        ),
        msg(
            "user",
            json!([
                { "type": "tool_result", "tool_use_id": "n42", "output": 42, "is_error": true },
                // Number 0 is falsy → first_truthy skips it and returns `content`.
                { "type": "tool_result", "tool_use_id": "n0", "output": 0, "content": "fallback", "is_error": true },
            ]),
        ),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(xml.contains("<tool_result tool_use_id=\"n42\" is_error=\"true\">"));
    // Indent inside the assistant wrapper is 4 spaces; tool body adds 2 more.
    assert!(xml.contains("\n      42\n"));
    assert!(xml.contains("fallback"));
}

#[test]
fn consecutive_assistants_without_user_split_into_exchanges() {
    // Two assistants in a row with no leading user: the second triggers the
    // "current.user.is_none() && !current.assistants.is_empty()" branch that
    // closes out the prior exchange and starts a fresh one.
    let messages = vec![
        msg("assistant", json!([{ "type": "text", "text": "first" }])),
        msg("assistant", json!([{ "type": "text", "text": "second" }])),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert_eq!(
        xml.matches("<exchange>\n").count(),
        2,
        "two consecutive userless assistants must split into two exchanges"
    );
    assert!(xml.contains("first"));
    assert!(xml.contains("second"));
}

#[test]
fn error_role_renders_as_error_tag_within_current_exchange() {
    let messages = vec![
        msg("user", json!([{ "type": "text", "text": "go" }])),
        msg("error", json!([{ "type": "text", "text": "boom" }])),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    // Error messages render with the `<error>` tag (not `agent_response_…`).
    assert!(xml.contains("  <error>\n    <text>boom</text>\n  </error>\n"));
}

#[test]
fn system_model_changed_notice_is_excluded_from_replay() {
    // The `model_changed` informational row is persisted as `role: "system"`
    // — the formatter only renders user/assistant/error, so the notice must
    // never leak into a provider prompt via supervisor-XML replay.
    let messages = vec![
        msg("user", json!([{ "type": "text", "text": "hello" }])),
        msg("assistant", json!([{ "type": "text", "text": "hi" }])),
        msg(
            "system",
            json!([{ "type": "text", "text": "Model changed from auggie (default model) to claude-code:sonnet." }]),
        ),
        msg("user", json!([{ "type": "text", "text": "again" }])),
        msg("assistant", json!([{ "type": "text", "text": "sure" }])),
    ];
    let xml = format_history_as_xml(&messages, MAX_HISTORY_CHARS);
    assert!(!xml.contains("Model changed"), "notice must not render");
    assert!(
        xml.contains("hello") && xml.contains("again"),
        "real turns render"
    );
}
