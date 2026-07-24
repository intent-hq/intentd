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

/// MIME type identifying a proposal resource content item (§7.1). Aliases the
/// bindings' canonical constant (parity with the FE contract in
/// `cloudlands-fe/src/shared/types/proposal-resource.ts`).
pub const PROPOSAL_RESOURCE_MIME: &str = intent_acp::mcp_server::PROPOSAL_RESOURCE_MIME_TYPE;

/// Find the first well-formed proposal resource item in a `tool_result` output
/// array: `{ type: "resource", resource: { mimeType: <proposal MIME>, text } }`
/// with `text` a string (the JSON the FE parses into a `Proposal`). Returns
/// `None` for a non-array output, no matching item, or a malformed resource
/// (wrong MIME, missing/non-string `text`).
pub fn find_proposal_resource(output: &Value) -> Option<&Value> {
    output.as_array()?.iter().find(|item| {
        item.get("type").and_then(Value::as_str) == Some("resource")
            && item.get("resource").is_some_and(|r| {
                r.get("mimeType").and_then(Value::as_str) == Some(PROPOSAL_RESOURCE_MIME)
                    && r.get("text").is_some_and(Value::is_string)
            })
    })
}

/// Build the standalone proposal-resource block (§7.1): the output item echoed
/// verbatim (`{ type: "resource", resource: {…} }`) with the stable block id
/// stamped on. Shared by `record_tool` (persisted) and `tool_delta` (live) so
/// the shapes agree byte-for-byte.
pub fn build_proposal_resource_block(block_id: &str, item: &Value) -> Value {
    let mut block = item.clone();
    if let Some(obj) = block.as_object_mut() {
        obj.insert("id".to_string(), Value::String(block_id.to_string()));
    }
    block
}

/// Size cap for the collapsed-output fallback parse: a stringified
/// `{ok, proposal}` payload larger than this is never a real proposal echo
/// (proposals are preview+payload sized), so skip the parse entirely.
const COLLAPSED_PROPOSAL_MAX_BYTES: usize = 256 * 1024;

/// Find or reconstruct the proposal resource item for a completed tool's
/// output (§7.1). Tries [`find_proposal_resource`] first (the provider echoed
/// the MCP content-item array intact); when the output is not an array,
/// falls back to recovering the daemon's own `{ok: true, proposal}` text-item
/// payload from a provider-collapsed output — auggie flattens the dual
/// text+resource MCP content items into `{ "output": "<stringified {ok,
/// proposal}>" }`, dropping the resource item entirely
/// (intent-hq/monorepo#511 regression class). The fallback is guarded: the
/// candidate string is size-capped, must parse as JSON carrying `ok: true`
/// plus a proposal passing the bindings' own canonical validation
/// (`intent_acp::mcp_server::is_valid_proposal`), and the resource item is
/// rebuilt with the bindings' own URI builder, so downstream rendering is
/// identical to the array path. Note the guards verify *shape*, not
/// *provenance*: a collapsed output byte-identical to a `ws.app.proposal.show`
/// echo is indistinguishable from one and will be lifted.
pub fn lift_proposal_resource(output: &Value) -> Option<Value> {
    if let Some(item) = find_proposal_resource(output) {
        return Some(item.clone());
    }
    rebuild_collapsed_proposal_resource(output)
}

/// Extract the candidate stringified payload from a provider-collapsed tool
/// output: `{ "output": "<string>" }` (auggie's shape) or a bare string.
fn collapsed_output_text(output: &Value) -> Option<&str> {
    match output {
        Value::String(s) => Some(s),
        Value::Object(m) => m.get("output")?.as_str(),
        _ => None,
    }
}

/// Parse a collapsed output back into the `{ok: true, proposal}` payload the
/// daemon's own `ws.app.*` bindings emitted as their MCP text item, and
/// rebuild the proposal resource item from it. `None` unless every guard
/// passes: string present, within the size cap, valid JSON object with
/// `ok: true`, and a proposal that passes the bindings' canonical
/// `is_valid_proposal` (the SAME function `ws.app.proposal.show` validated
/// with before emitting it).
fn rebuild_collapsed_proposal_resource(output: &Value) -> Option<Value> {
    let text = collapsed_output_text(output)?;
    if text.len() > COLLAPSED_PROPOSAL_MAX_BYTES || !text.trim_start().starts_with('{') {
        return None;
    }
    let parsed: Value = serde_json::from_str(text).ok()?;
    let obj = parsed.as_object()?;
    if obj.get("ok") != Some(&Value::Bool(true)) {
        return None;
    }
    let proposal = obj.get("proposal")?;
    if !intent_acp::mcp_server::is_valid_proposal(proposal) {
        return None;
    }
    Some(build_proposal_resource_item(proposal))
}

/// Rebuild the MCP proposal resource item exactly as the `ws.app.*` bindings
/// construct it (`proposal.rs::show`), reusing the bindings' own
/// `proposal_resource_uri`: uri from kind + applyToolCallId (or
/// preview.title), name from preview.title, compact proposal JSON as `text`.
fn build_proposal_resource_item(proposal: &Value) -> Value {
    let name = proposal
        .get("preview")
        .and_then(|p| p.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("Proposal");
    json!({
        "type": "resource",
        "resource": {
            "uri": intent_acp::mcp_server::proposal_resource_uri(proposal),
            "name": name,
            "mimeType": PROPOSAL_RESOURCE_MIME,
            "text": serde_json::to_string(proposal).unwrap_or_else(|_| "{}".to_string()),
        }
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

    fn proposal_item() -> Value {
        json!({
            "type": "resource",
            "resource": {
                "uri": "intent-proposal://settings-change/Update",
                "name": "Update",
                "mimeType": PROPOSAL_RESOURCE_MIME,
                "text": "{\"kind\":\"settings-change\"}",
            }
        })
    }

    #[test]
    fn find_proposal_resource_matches_well_formed_item() {
        let output = json!([{ "type": "text", "text": "shown" }, proposal_item()]);
        let found = find_proposal_resource(&output).expect("proposal item found");
        assert_eq!(found["resource"]["mimeType"], PROPOSAL_RESOURCE_MIME);
    }

    #[test]
    fn find_proposal_resource_absent_or_non_array_yields_none() {
        assert!(find_proposal_resource(&json!([{ "type": "text", "text": "hi" }])).is_none());
        assert!(find_proposal_resource(&json!("plain string output")).is_none());
        assert!(find_proposal_resource(&json!({ "summary": "ok" })).is_none());
    }

    #[test]
    fn find_proposal_resource_rejects_malformed_items() {
        // Wrong MIME.
        let mut wrong_mime = proposal_item();
        wrong_mime["resource"]["mimeType"] = json!("text/plain");
        assert!(find_proposal_resource(&json!([wrong_mime])).is_none());
        // Missing `text`.
        let mut no_text = proposal_item();
        no_text["resource"].as_object_mut().unwrap().remove("text");
        assert!(find_proposal_resource(&json!([no_text])).is_none());
        // Non-string `text`.
        let mut bad_text = proposal_item();
        bad_text["resource"]["text"] = json!({ "kind": "settings-change" });
        assert!(find_proposal_resource(&json!([bad_text])).is_none());
        // `resource` not an object with the expected fields.
        assert!(find_proposal_resource(&json!([{ "type": "resource" }])).is_none());
    }

    #[test]
    fn build_proposal_resource_block_echoes_item_and_stamps_id() {
        let block = build_proposal_resource_block("m:3", &proposal_item());
        assert_eq!(block["id"], "m:3");
        assert_eq!(block["type"], "resource");
        assert_eq!(block["resource"], proposal_item()["resource"]);
    }

    fn valid_proposal() -> Value {
        json!({
            "kind": "settings-change",
            "preview": { "title": "Update Test Setting" },
            "payload": { "key": "test.setting", "value": "new-value" },
        })
    }

    /// The auggie-collapsed shape: the daemon's own `{ok, proposal}` text-item
    /// payload pretty-printed into `raw_output.output` (intent-hq/monorepo#511).
    fn collapsed_output(proposal: &Value) -> Value {
        let text = serde_json::to_string_pretty(&json!({ "ok": true, "proposal": proposal }))
            .expect("serialize");
        json!({ "output": text })
    }

    #[test]
    fn lift_proposal_resource_prefers_array_item() {
        let output = json!([{ "type": "text", "text": "shown" }, proposal_item()]);
        let item = lift_proposal_resource(&output).expect("array item lifted");
        assert_eq!(item, proposal_item());
    }

    #[test]
    fn lift_proposal_resource_rebuilds_from_collapsed_object_output() {
        let item =
            lift_proposal_resource(&collapsed_output(&valid_proposal())).expect("fallback lifted");
        assert_eq!(item["type"], "resource");
        let resource = &item["resource"];
        assert_eq!(resource["mimeType"], PROPOSAL_RESOURCE_MIME);
        assert_eq!(resource["name"], "Update Test Setting");
        assert_eq!(
            resource["uri"],
            "intent-proposal://settings-change/Update%20Test%20Setting"
        );
        // The rebuilt `text` round-trips to the original proposal.
        let text = resource["text"].as_str().expect("text is a string");
        let parsed: Value = serde_json::from_str(text).expect("text parses");
        assert_eq!(parsed, valid_proposal());
    }

    #[test]
    fn lift_proposal_resource_rebuilds_from_plain_string_output() {
        let text =
            serde_json::to_string_pretty(&json!({ "ok": true, "proposal": valid_proposal() }))
                .unwrap();
        let item = lift_proposal_resource(&json!(text)).expect("string fallback lifted");
        assert_eq!(item["resource"]["mimeType"], PROPOSAL_RESOURCE_MIME);
    }

    #[test]
    fn lift_proposal_resource_uri_uses_apply_tool_call_id_when_present() {
        let mut proposal = valid_proposal();
        proposal["applyToolCallId"] = json!("tc-apply-1");
        let item = lift_proposal_resource(&collapsed_output(&proposal)).expect("lifted");
        assert_eq!(
            item["resource"]["uri"],
            "intent-proposal://settings-change/tc-apply-1"
        );
    }

    #[test]
    fn lift_proposal_resource_rejects_non_proposal_collapsed_outputs() {
        // `ok: false` — the bindings only echo `ok: true` alongside a proposal.
        let not_ok =
            serde_json::to_string(&json!({ "ok": false, "proposal": valid_proposal() })).unwrap();
        assert!(lift_proposal_resource(&json!({ "output": not_ok })).is_none());
        // No `proposal` field.
        assert!(lift_proposal_resource(&json!({ "output": "{\"ok\": true}" })).is_none());
        // Structurally invalid proposal (unknown kind).
        let mut bad_kind = valid_proposal();
        bad_kind["kind"] = json!("not-a-kind");
        assert!(lift_proposal_resource(&collapsed_output(&bad_kind)).is_none());
        // Missing preview.title.
        let mut no_title = valid_proposal();
        no_title["preview"] = json!({});
        assert!(lift_proposal_resource(&collapsed_output(&no_title)).is_none());
        // Non-object payload.
        let mut bad_payload = valid_proposal();
        bad_payload["payload"] = json!("nope");
        assert!(lift_proposal_resource(&collapsed_output(&bad_payload)).is_none());
        // Non-JSON text and ordinary tool outputs.
        assert!(lift_proposal_resource(&json!({ "output": "12 tests passed" })).is_none());
        assert!(lift_proposal_resource(&json!({ "output": "(no return value)" })).is_none());
        assert!(lift_proposal_resource(&json!({ "summary": "ok" })).is_none());
        assert!(lift_proposal_resource(&json!(42)).is_none());
        assert!(lift_proposal_resource(&Value::Null).is_none());
    }

    #[test]
    fn lift_proposal_resource_rejects_oversized_collapsed_output() {
        let mut proposal = valid_proposal();
        proposal["payload"]["filler"] = json!("x".repeat(COLLAPSED_PROPOSAL_MAX_BYTES));
        assert!(lift_proposal_resource(&collapsed_output(&proposal)).is_none());
    }
}
