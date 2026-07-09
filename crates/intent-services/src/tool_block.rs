//! Synthesized `tool_use` block factory (§7.1).
//!
//! Both the persisted transcript (`record_tool` in `agent_session`) and the
//! live `chat.subscribe` delta stream (`tool_delta` in `intent-transport`)
//! synthesize `tool_use` blocks from the same `agent:tool:call` signal (§6.6).
//! Keeping the derivation in one place ensures the seq-0 snapshot and every
//! live delta agree byte-for-byte — the invariant `chat.subscribe` depends on.
//!
//! ACP providers (auggie, codex, …) deliver a **human-readable** `title`
//! (e.g. `"sub-agent-explore: Explore the AI agent system…"`) rather than the
//! raw tool name the model actually invoked. The real name is derived once,
//! at `session/update` mapping time, by `intent_acp::session::derive_tool_name`
//! and carried on the `agent:tool:call` payload as `toolName` (with the raw
//! title alongside as `title`, §6.6). The FE classifier needs the raw name in
//! `block.name` to route icons/labels, and the title alongside it as
//! `_acpTitle` on the input for fallback rendering when raw args are missing
//! (auggie frequently sends `raw_input: null`). Mirrors the reference
//! electron behavior at `acp-provider-streaming.ts:917` / `:1821`.
//!
//! The ACP title is always echoed under `input._acpTitle` when non-empty.

use serde_json::{json, Map, Value};

/// Return `input` augmented with `_acpTitle = title` when `title` is non-empty.
/// A `Null` input is coerced to `{}` so the marker can attach; non-object,
/// non-null inputs (arrays / scalars) pass through verbatim.
pub fn attach_acp_title(input: Value, title: &str) -> Value {
    if title.is_empty() {
        return match input {
            Value::Null => Value::Object(Map::new()),
            other => other,
        };
    }
    let mut obj = match input {
        Value::Object(m) => m,
        Value::Null => Map::new(),
        other => return other,
    };
    obj.insert("_acpTitle".to_string(), Value::String(title.to_string()));
    Value::Object(obj)
}

/// Build the full `tool_use` block matching PROTOCOL.md §7.1's persisted shape:
/// `{ type, id, name, input, toolCallId, metadata:{ toolKind, status } }`.
/// `name` is the real tool name (already derived, §6.6 `toolName`); `title`
/// is the raw ACP title echoed as `input._acpTitle`.
pub fn build_tool_use_block(
    block_id: &str,
    name: &str,
    title: &str,
    input: Value,
    tool_call_id: &str,
    tool_kind: &str,
    status: &str,
) -> Value {
    json!({
        "type": "tool_use",
        "id": block_id,
        "name": name,
        "input": attach_acp_title(input, title),
        "toolCallId": tool_call_id,
        "metadata": { "toolKind": tool_kind, "status": status },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_title_land_in_block_and_marker() {
        // The real name goes to `block.name` verbatim; the whole ACP title
        // is echoed as `_acpTitle` alongside the raw input.
        let block = build_tool_use_block(
            "m:1",
            "sub-agent-explore",
            "sub-agent-explore: Explore the AI agent system",
            json!({ "topic": "acp" }),
            "tc-1",
            "other",
            "started",
        );
        assert_eq!(block["name"], "sub-agent-explore");
        assert_eq!(
            block["input"]["_acpTitle"],
            "sub-agent-explore: Explore the AI agent system"
        );
        assert_eq!(block["input"]["topic"], "acp");
        assert_eq!(block["toolCallId"], "tc-1");
        assert_eq!(block["metadata"]["status"], "started");
    }

    #[test]
    fn title_absent_still_produces_object_input_and_no_marker() {
        // A missing / empty title cannot be echoed as `_acpTitle`, and a
        // `Null` input is coerced to `{}` so downstream renderers see an
        // object regardless of whether the provider sent raw args.
        let block = build_tool_use_block("m:1", "", "", Value::Null, "tc-2", "file", "started");
        assert_eq!(block["name"], "");
        assert!(block["input"].is_object());
        assert!(block["input"].get("_acpTitle").is_none());
    }

    #[test]
    fn null_input_with_title_gets_marker_only() {
        // Reflects the observed auggie payload: `raw_input: null` + title.
        let block = build_tool_use_block(
            "m:1",
            "sub-agent-explore",
            "sub-agent-explore: go",
            Value::Null,
            "tc-3",
            "other",
            "started",
        );
        assert_eq!(block["name"], "sub-agent-explore");
        assert_eq!(
            block["input"],
            json!({ "_acpTitle": "sub-agent-explore: go" })
        );
    }

    #[test]
    fn non_object_input_passes_through_verbatim() {
        // Arrays / scalars have no place to attach a marker; the input is
        // preserved as-is (the FE classifier still has `title` in the event).
        let block = build_tool_use_block(
            "m:1",
            "shell",
            "shell: run",
            json!(["ls", "-la"]),
            "tc-6",
            "terminal",
            "started",
        );
        assert!(block["input"].is_array());
    }
}
