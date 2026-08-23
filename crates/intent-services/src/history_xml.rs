//! Conversation-history → `<supervisor>` XML formatter for session recovery
//! (faithful port of `acp-provider.ts` `formatHistoryAsXml` +
//! `sanitizeMessagesForHistory`).
//!
//! When the resume-impossible fallback creates a fresh `session/new`, the new
//! ACP session has no prior context. This renders the persisted `agent_message`
//! log into the same `<supervisor>`-wrapped exchange XML the TS provider sends so
//! the agent continues seamlessly. Operates on the stored JSON content blocks
//! (`serde_json::Value`) rather than typed blocks, mirroring the persisted shape.

use std::collections::HashSet;
use std::fmt::Write as _;

use intent_core::AgentMessage;
use serde_json::{json, Value};

/// Max characters of history to include (TS `MAX_HISTORY_CHARS`).
pub(crate) const MAX_HISTORY_CHARS: usize = 200_000;
/// Max characters per tool input/output block (TS `MAX_TOOL_CONTENT_CHARS`).
const MAX_TOOL_CONTENT_CHARS: usize = 4_000;
/// Max characters for a tool name (TS `MAX_TOOL_NAME_CHARS`).
const MAX_TOOL_NAME_CHARS: usize = 200;

const SUPERVISOR_PREAMBLE: &str = "<supervisor>\nThe previous ACP session was lost. Below is the full conversation history from the prior session so you can continue seamlessly.\nDo NOT mention session recovery to the user. Just continue naturally as if nothing happened.\n\n";
const SUPERVISOR_CLOSING: &str =
    "Continue the conversation from this point. Do not mention session recovery or interruption.\n</supervisor>";

/// Escape XML special characters (TS `escapeXml`): `&` first, then `<`, `>`, `"`.
/// Also used by the restart-resume tail recap (monorepo#2539) so replayed
/// user/partial text cannot escape its quoting tags.
pub(crate) fn escape_xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Middle-truncate `text` to `max_chars`, keeping the head and tail (TS
/// `truncateMiddleContent`). Operates on chars to stay on UTF-8 boundaries.
/// Also used by the restart-resume tail recap (monorepo#2539) to bound the
/// replayed user/partial-response text.
pub(crate) fn truncate_middle_content(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    if len <= max_chars {
        return text.to_string();
    }
    // 60 chars reserved for the "... [N characters truncated] ..." marker.
    let half_budget = max_chars.saturating_sub(60) / 2;
    if half_budget == 0 {
        return chars[..max_chars.min(len)].iter().collect();
    }
    let start: String = chars[..half_budget].iter().collect();
    let end: String = chars[len - half_budget..].iter().collect();
    let omitted = len - half_budget * 2;
    format!("{start}\n... [{omitted} characters truncated] ...\n{end}")
}

/// Stringify a value (TS `safeStringify`): strings pass through; everything else
/// is JSON-encoded.
fn safe_stringify(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| value.to_string()),
    }
}

/// JS truthiness for a JSON value (null/false/0/"" are falsy).
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::String(s) => !s.is_empty(),
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// First non-empty string among `keys` (the `a || b || ''` chain for strings).
fn str_field(block: &Value, keys: &[&str]) -> String {
    for k in keys {
        if let Some(s) = block.get(*k).and_then(Value::as_str) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}

/// Whether any of `keys` is truthy (the `a || b || false` boolean chain).
fn bool_field(block: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|k| block.get(*k).is_some_and(is_truthy))
}

/// First truthy value among `keys` (the `a || b` chain for values).
fn first_truthy(block: &Value, keys: &[&str]) -> Option<Value> {
    for k in keys {
        if let Some(v) = block.get(*k) {
            if is_truthy(v) {
                return Some(v.clone());
            }
        }
    }
    None
}

/// A sanitized message reduced to its role + retained content blocks.
struct Msg {
    role: String,
    blocks: Vec<Value>,
}

/// Drop malformed persisted blocks before rendering (TS
/// `sanitizeMessagesForHistory`): empty assistant turns, `tool_results` with
/// missing/duplicate ids, empty non-error `tool_results`, and dangling `tool_use`
/// blocks (STAB-108: `tool_use` without a corresponding `tool_result` causes
/// provider rejection on session resume).
fn sanitize_messages_for_history(messages: &[AgentMessage]) -> Vec<Msg> {
    // First pass: collect valid tool_result IDs to identify dangling tool_use blocks.
    let mut valid_tool_result_ids: HashSet<String> = HashSet::new();

    for m in messages {
        if let Some(blocks) = m.content.as_array() {
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                    let tool_use_id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !tool_use_id.is_empty() {
                        // Check if this tool_result is valid (has output or is_error).
                        let output = match block.get("output") {
                            Some(v) if !v.is_null() => Some(v),
                            _ => block.get("content"),
                        };
                        let has_output =
                            matches!(
                                output,
                                Some(Value::String(s)) if !s.is_empty()
                            ) || matches!(output, Some(Value::Object(_) | Value::Array(_)));
                        if has_output || bool_field(block, &["is_error", "isError"]) {
                            valid_tool_result_ids.insert(tool_use_id);
                        }
                    }
                }
            }
        }
    }

    // Second pass: sanitize messages, dropping dangling tool_use blocks.
    let mut seen_tool_result_ids: HashSet<String> = HashSet::new();
    let mut sanitized = Vec::new();
    for m in messages {
        let blocks = m.content.as_array().filter(|b| !b.is_empty());
        let Some(blocks) = blocks else {
            // Empty/no blocks: drop empty assistant turns, keep user/error wrappers.
            if m.role == "assistant" {
                continue;
            }
            sanitized.push(Msg {
                role: m.role.clone(),
                blocks: Vec::new(),
            });
            continue;
        };
        let mut clean: Vec<Value> = Vec::new();
        for block in blocks {
            match block.get("type").and_then(Value::as_str) {
                Some("tool_use") => {
                    // Drop tool_use blocks that don't have a valid tool_result.
                    let id = str_field(block, &["tool_use_id", "id"]);
                    if !id.is_empty() && valid_tool_result_ids.contains(&id) {
                        clean.push(block.clone());
                    }
                }
                Some("tool_result") => {
                    let tool_use_id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if tool_use_id.is_empty() || seen_tool_result_ids.contains(&tool_use_id) {
                        continue;
                    }
                    // `??` (nullish): "" output is kept, not coalesced to content.
                    let output = match block.get("output") {
                        Some(v) if !v.is_null() => Some(v),
                        _ => block.get("content"),
                    };
                    let has_output =
                        matches!(
                            output,
                            Some(Value::String(s)) if !s.is_empty()
                        ) || matches!(output, Some(Value::Object(_) | Value::Array(_)));
                    if !has_output && !bool_field(block, &["is_error", "isError"]) {
                        continue;
                    }
                    seen_tool_result_ids.insert(tool_use_id);
                    clean.push(block.clone());
                }
                _ => {
                    clean.push(block.clone());
                }
            }
        }
        if clean.is_empty() && m.role == "assistant" {
            continue;
        }
        sanitized.push(Msg {
            role: m.role.clone(),
            blocks: clean,
        });
    }
    sanitized
}

/// Render a message's content blocks into XML fragments (TS
/// `renderContentBlocks`): `text/thinking/tool_use/tool_result`, each escaped and
/// (for tool blocks) middle-truncated.
fn render_content_blocks(blocks: &[Value], indent: &str) -> String {
    let mut xml = String::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => {
                let text = str_field(block, &["text", "content"]);
                let _ = writeln!(xml, "{indent}<text>{}</text>", escape_xml(&text));
            }
            "thinking" => {
                let thinking = str_field(block, &["text", "content"]);
                let _ = writeln!(
                    xml,
                    "{indent}<thinking>{}</thinking>",
                    escape_xml(&thinking)
                );
            }
            "tool_use" => {
                let raw_name = str_field(block, &["name", "toolName"]);
                let name = if raw_name.chars().count() > MAX_TOOL_NAME_CHARS {
                    let head: String = raw_name.chars().take(MAX_TOOL_NAME_CHARS - 3).collect();
                    format!("{head}...")
                } else {
                    raw_name
                };
                let tool_name = escape_xml(&name);
                let tool_use_id = escape_xml(&str_field(block, &["tool_use_id", "id"]));
                let raw_input = match block.get("input") {
                    Some(v) if is_truthy(v) => v.clone(),
                    _ => json!({}),
                };
                let input_str = escape_xml(&truncate_middle_content(
                    &safe_stringify(&raw_input),
                    MAX_TOOL_CONTENT_CHARS,
                ));
                let _ = writeln!(
                    xml,
                    "{indent}<tool_use name=\"{tool_name}\" tool_use_id=\"{tool_use_id}\">"
                );
                let _ = writeln!(xml, "{indent}  {input_str}");
                let _ = writeln!(xml, "{indent}</tool_use>");
            }
            "tool_result" => {
                let tool_use_id = escape_xml(&str_field(block, &["tool_use_id"]));
                let is_error = bool_field(block, &["is_error", "isError"]);
                let content = first_truthy(block, &["output", "content"])
                    .unwrap_or(Value::String(String::new()));
                let content_str = escape_xml(&truncate_middle_content(
                    &safe_stringify(&content),
                    MAX_TOOL_CONTENT_CHARS,
                ));
                let _ = writeln!(
                    xml,
                    "{indent}<tool_result tool_use_id=\"{tool_use_id}\" is_error=\"{is_error}\">"
                );
                let _ = writeln!(xml, "{indent}  {content_str}");
                let _ = writeln!(xml, "{indent}</tool_result>");
            }
            _ => {}
        }
    }
    xml
}

/// One grouped exchange: an optional user turn and the assistant/error turns
/// that follow it.
struct Exchange {
    user: Option<Msg>,
    assistants: Vec<Msg>,
}

/// Format conversation `messages` as `<supervisor>`-wrapped exchange XML for
/// session recovery (TS `formatHistoryAsXml`). Groups user→assistant pairs,
/// renders newest-first within a `max_chars` budget, and emits an omission
/// comment for older exchanges that did not fit. Returns `""` for empty input.
pub(crate) fn format_history_as_xml(messages: &[AgentMessage], max_chars: usize) -> String {
    if messages.is_empty() {
        return String::new();
    }
    let sanitized = sanitize_messages_for_history(messages);

    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut current = Exchange {
        user: None,
        assistants: Vec::new(),
    };
    for msg in sanitized {
        match msg.role.as_str() {
            "user" => {
                if current.user.is_some() || !current.assistants.is_empty() {
                    exchanges.push(current);
                    current = Exchange {
                        user: None,
                        assistants: Vec::new(),
                    };
                }
                current.user = Some(msg);
            }
            "assistant" => {
                if current.user.is_none() && !current.assistants.is_empty() {
                    exchanges.push(current);
                    current = Exchange {
                        user: None,
                        assistants: Vec::new(),
                    };
                }
                current.assistants.push(msg);
            }
            "error" => current.assistants.push(msg),
            _ => {}
        }
    }
    if current.user.is_some() || !current.assistants.is_empty() {
        exchanges.push(current);
    }

    let exchange_xml_strings: Vec<String> = exchanges
        .iter()
        .map(|ex| {
            let mut s = String::from("<exchange>\n");
            if let Some(user) = &ex.user {
                s.push_str("  <user_request_or_tool_results>\n");
                s.push_str(&render_content_blocks(&user.blocks, "    "));
                s.push_str("  </user_request_or_tool_results>\n");
            }
            for assistant in &ex.assistants {
                let tag = if assistant.role == "error" {
                    "error"
                } else {
                    "agent_response_or_tool_uses"
                };
                let _ = writeln!(s, "  <{tag}>");
                s.push_str(&render_content_blocks(&assistant.blocks, "    "));
                let _ = writeln!(s, "  </{tag}>");
            }
            s.push_str("</exchange>\n");
            s
        })
        .collect();

    let wrapper_overhead = SUPERVISOR_PREAMBLE.len() + SUPERVISOR_CLOSING.len();
    let max_omission_comment = format!(
        "<!-- {} earlier exchanges omitted due to size limits -->\n",
        exchange_xml_strings.len()
    );

    // Include newest-first, stopping at the first exchange that does not fit so
    // the retained history stays contiguous.
    let mut cumulative = wrapper_overhead + max_omission_comment.len();
    let mut included: Vec<String> = Vec::new();
    let mut omitted_count = 0;
    for i in (0..exchange_xml_strings.len()).rev() {
        let ex = &exchange_xml_strings[i];
        if cumulative + ex.len() <= max_chars {
            included.insert(0, ex.clone());
            cumulative += ex.len();
        } else {
            omitted_count = i + 1;
            break;
        }
    }

    let mut exchanges_xml = String::new();
    if omitted_count > 0 {
        let _ = writeln!(
            exchanges_xml,
            "<!-- {omitted_count} earlier exchanges omitted due to size limits -->"
        );
    }
    exchanges_xml.push_str(&included.concat());

    format!("{SUPERVISOR_PREAMBLE}{exchanges_xml}{SUPERVISOR_CLOSING}")
}

#[cfg(test)]
mod tests;
