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
//! raw tool name the model actually invoked. The FE classifier needs the raw
//! name in `block.name` to route icons/labels, and the title alongside it as
//! `_acpTitle` on the input for fallback rendering when raw args are missing
//! (auggie frequently sends `raw_input: null`). Mirrors the reference
//! electron behavior at `acp-provider-streaming.ts:917` / `:1821`.
//!
//! Derivation rules for the "real" tool name:
//!  - A title of the form `<name>: <description>` (`<name>` a bare identifier
//!    of `[A-Za-z0-9_-]+`, followed by `": "` or `":\t"`) is split; the
//!    prefix becomes the name.
//!  - Repeated `_workspace-mcp` suffixes are collapsed to a single one — a
//!    known auggie convention artifact when the MCP server name equals the
//!    tool-name suffix (§18.4).
//!  - Otherwise the title passes through as-is.
//!
//! The ACP title is always echoed under `input._acpTitle` when non-empty.

use serde_json::{json, Map, Value};

/// Derive a synthesized `tool_use` block's `name` from the ACP `title`.
pub fn derive_tool_name(title: &str) -> String {
    let base = split_name_prefix(title).unwrap_or(title);
    collapse_workspace_mcp_suffix(base)
}

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
pub fn build_tool_use_block(
    block_id: &str,
    title: &str,
    input: Value,
    tool_call_id: &str,
    tool_kind: &str,
    status: &str,
) -> Value {
    json!({
        "type": "tool_use",
        "id": block_id,
        "name": derive_tool_name(title),
        "input": attach_acp_title(input, title),
        "toolCallId": tool_call_id,
        "metadata": { "toolKind": tool_kind, "status": status },
    })
}

fn split_name_prefix(title: &str) -> Option<&str> {
    let colon = title.find(':')?;
    let name = &title[..colon];
    if name.is_empty() {
        return None;
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    let after = title[colon + 1..].chars().next()?;
    if after != ' ' && after != '\t' {
        return None;
    }
    Some(name)
}

fn collapse_workspace_mcp_suffix(name: &str) -> String {
    const SUFFIX: &str = "_workspace-mcp";
    const DOUBLE: &str = "_workspace-mcp_workspace-mcp";
    let mut cur = name.to_string();
    while cur.ends_with(DOUBLE) {
        cur.truncate(cur.len() - SUFFIX.len());
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn title_present_with_name_colon_description_splits_into_name() {
        // Auggie sub-agent title shape → raw tool name is the identifier
        // before the colon, and the whole title is echoed as `_acpTitle`.
        let block = build_tool_use_block(
            "m:1",
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
        let block = build_tool_use_block("m:1", "", Value::Null, "tc-2", "file", "started");
        assert_eq!(block["name"], "");
        assert!(block["input"].is_object());
        assert!(block["input"].get("_acpTitle").is_none());
    }

    #[test]
    fn null_input_with_title_gets_marker_only() {
        // Reflects the observed auggie payload: `raw_input: null` + title.
        let block = build_tool_use_block(
            "m:1",
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
    fn double_workspace_mcp_suffix_collapses_to_single() {
        // Auggie's `<tool>_<server>` convention double-suffixes our
        // `*_workspace-mcp` tools because the server itself is
        // `workspace-mcp`. The synthesized block strips the extra copy.
        assert_eq!(
            derive_tool_name("list_notes_workspace-mcp_workspace-mcp"),
            "list_notes_workspace-mcp"
        );
        assert_eq!(
            derive_tool_name("list_notes_workspace-mcp_workspace-mcp_workspace-mcp"),
            "list_notes_workspace-mcp"
        );
        // A single suffix is preserved (the canonical shape).
        assert_eq!(
            derive_tool_name("list_notes_workspace-mcp"),
            "list_notes_workspace-mcp"
        );
    }

    #[test]
    fn split_and_collapse_compose() {
        // `<name>_workspace-mcp_workspace-mcp: List all notes` → split then
        // collapse yields the single-suffix canonical name.
        let block = build_tool_use_block(
            "m:1",
            "list_notes_workspace-mcp_workspace-mcp: List all notes",
            json!({ "workspaceId": "w1" }),
            "tc-4",
            "note",
            "started",
        );
        assert_eq!(block["name"], "list_notes_workspace-mcp");
        assert_eq!(
            block["input"]["_acpTitle"],
            "list_notes_workspace-mcp_workspace-mcp: List all notes"
        );
        assert_eq!(block["input"]["workspaceId"], "w1");
    }

    #[test]
    fn title_without_colon_passes_through_as_name() {
        // Titles like `Edit src/lib.rs` have no identifier prefix; the whole
        // string is the block name and also lands under `_acpTitle`.
        let block = build_tool_use_block(
            "m:1",
            "Edit src/lib.rs",
            json!({ "path": "src/lib.rs" }),
            "tc-5",
            "file",
            "started",
        );
        assert_eq!(block["name"], "Edit src/lib.rs");
        assert_eq!(block["input"]["_acpTitle"], "Edit src/lib.rs");
    }

    #[test]
    fn urlish_and_time_prefixes_do_not_split() {
        // Colons that aren't followed by whitespace (URLs, times) are left
        // alone so we don't invent a fake tool name.
        assert_eq!(
            derive_tool_name("https://example.com/x"),
            "https://example.com/x"
        );
        assert_eq!(derive_tool_name("10:15 sync"), "10:15 sync");
    }

    #[test]
    fn non_object_input_passes_through_verbatim() {
        // Arrays / scalars have no place to attach a marker; the input is
        // preserved as-is (the FE classifier still has `title` in the event).
        let block = build_tool_use_block(
            "m:1",
            "shell: run",
            json!(["ls", "-la"]),
            "tc-6",
            "terminal",
            "started",
        );
        assert!(block["input"].is_array());
    }
}
