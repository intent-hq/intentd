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
//!
//! This module is also home to the slim-projection block bounding (§5.5):
//! [`slim_message_blocks`] / [`slim_tool_block`] are shared by the persisted
//! read path (`agent_ops`) and the live `chat.subscribe` stream
//! (`intent-transport`'s tool deltas and live-turn snapshot merge), so a slim
//! subscriber's accumulated state stays byte-identical to a fresh slim
//! snapshot — the same invariant the factory above upholds for block shape.

use intent_core::SLIM_PROJECTION_BUDGET_BYTES;
use serde_json::{json, Map, Value};

/// Apply the slim conversation projection (PROTOCOL §5.5, the wire default
/// since v8.0) to one served message's content blocks: oversized
/// `tool_use.input` / `tool_result.output` bodies are replaced by a
/// structure-preserving preview bounded by [`SLIM_PROJECTION_BUDGET_BYTES`]
/// with additive `inputTruncated`/`inputBytes` (resp.
/// `outputTruncated`/`outputBytes`) flags; oversized `image.data` is replaced
/// by the write-time thumbnail from `thumbnails` (keyed by the block's image
/// ordinal) with `dataTruncated`/`dataBytes`/`dataIsThumbnail`, or omitted
/// entirely when no thumbnail was persisted (legacy rows, failed generation,
/// or an in-flight turn — pass `None`). Blocks at or under budget pass
/// through byte-identical with no flags. `name`, block ids, `tool_use_id`
/// pairing, and `is_error` are never touched. Serve-time only — stored rows
/// are untouched.
pub fn slim_message_blocks(blocks: &mut [Value], thumbnails: Option<&Value>) {
    let mut image_ordinal: usize = 0;
    for block in blocks.iter_mut() {
        if block.get("type").and_then(Value::as_str) == Some("image") {
            let ordinal = image_ordinal;
            image_ordinal += 1;
            slim_image(block, ordinal, thumbnails);
        } else {
            slim_tool_block(block);
        }
    }
}

/// Slim one `tool_use` / `tool_result` block in place (no-op for any other
/// block type). Split out of [`slim_message_blocks`] for the live delta
/// stream, which synthesizes tool blocks one at a time from `agent:tool:call`
/// payloads and must bound them identically to the persisted read path.
pub fn slim_tool_block(block: &mut Value) {
    match block.get("type").and_then(Value::as_str) {
        Some("tool_use") => slim_body(block, "input", "inputTruncated", "inputBytes"),
        Some("tool_result") => slim_body(block, "output", "outputTruncated", "outputBytes"),
        _ => {}
    }
}

/// Slim one `tool_use.input` / `tool_result.output` body in place: measured
/// against [`SLIM_PROJECTION_BUDGET_BYTES`] (string bodies by length, JSON
/// bodies by serialized length), an over-budget body is replaced by the
/// bounded preview plus the additive truncation flags. Delegates to
/// [`intent_core::slim_heavy_body`] — the SAME transform the write-time
/// heavy-payload extraction persists (intent-store `message_payload`), so a
/// stored placeholder served without hydration is byte-identical to slimming
/// the full body here; that sharing is also why an already-flagged block
/// (a persisted write-time preview) passes through untouched instead of
/// being re-capped. The extracted original body is dropped (serve side).
fn slim_body(block: &mut Value, field: &str, truncated_flag: &str, bytes_flag: &str) {
    drop(intent_core::slim_heavy_body(
        block,
        field,
        truncated_flag,
        bytes_flag,
        SLIM_PROJECTION_BUDGET_BYTES,
    ));
}

/// Slim one `image` block in place: an over-budget base64 `data` is replaced
/// by the persisted write-time thumbnail (`dataIsThumbnail: true`, `mimeType`
/// switched to the thumbnail's encoding) when one exists for this image
/// ordinal, otherwise `data` is omitted entirely (a truncated base64 fragment
/// is unrenderable). `dataTruncated`/`dataBytes` are stamped in both cases.
fn slim_image(block: &mut Value, ordinal: usize, thumbnails: Option<&Value>) {
    let Some(data_len) = block.get("data").and_then(Value::as_str).map(str::len) else {
        return;
    };
    if data_len <= SLIM_PROJECTION_BUDGET_BYTES {
        return;
    }
    let thumb = thumbnails
        .and_then(|t| t.get(ordinal.to_string()))
        .and_then(|t| {
            let data = t.get("data").and_then(Value::as_str)?;
            let mime = t.get("mimeType").and_then(Value::as_str)?;
            Some((data.to_string(), mime.to_string()))
        });
    let Some(obj) = block.as_object_mut() else {
        return;
    };
    match thumb {
        Some((data, mime)) => {
            obj.insert("data".to_string(), Value::String(data));
            obj.insert("mimeType".to_string(), Value::String(mime));
            obj.insert("dataIsThumbnail".to_string(), json!(true));
        }
        None => {
            obj.remove("data");
        }
    }
    obj.insert("dataTruncated".to_string(), json!(true));
    obj.insert("dataBytes".to_string(), json!(data_len));
}

/// Return `input` augmented with `_acpTitle = title` when `title` is non-empty.
/// A `Null` input is coerced to `{}` so the marker can attach; non-object,
/// non-null inputs (arrays / scalars) pass through verbatim.
pub(crate) fn attach_acp_title(input: Value, title: &str) -> Value {
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

/// Build the full `tool_use` block matching docs/protocol/07-agent-streaming.md §7.1's persisted shape:
/// `{ type, id, name, input, toolCallId, metadata:{ toolKind, status } }`.
/// `name` is the real tool name (already derived, §6.6 `toolName`); `title`
/// is the raw ACP title echoed as `input._acpTitle`.
#[must_use]
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
pub(crate) const PROPOSAL_RESOURCE_MIME: &str = intent_acp::mcp_server::PROPOSAL_RESOURCE_MIME_TYPE;

/// Find the first well-formed proposal resource item in a `tool_result` output
/// array: `{ type: "resource", resource: { mimeType: <proposal MIME>, text } }`
/// with `text` a string (the JSON the FE parses into a `Proposal`). Returns
/// `None` for a non-array output, no matching item, or a malformed resource
/// (wrong MIME, missing/non-string `text`).
pub(crate) fn find_proposal_resource(output: &Value) -> Option<&Value> {
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
#[must_use]
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
#[must_use]
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
///
/// When the initial parse fails, a wrap-repair pass strips raw control
/// characters from inside JSON string literals and re-parses: auggie
/// hard-wraps the collapsed echo at a 1000-char column, injecting raw `\n`
/// mid-string (even mid-word), which strict JSON rejects. See
/// [`repair_wrapped_json`].
fn rebuild_collapsed_proposal_resource(output: &Value) -> Option<Value> {
    let text = collapsed_output_text(output)?;
    if text.len() > COLLAPSED_PROPOSAL_MAX_BYTES || !text.trim_start().starts_with('{') {
        return None;
    }
    let parsed: Value = serde_json::from_str(text)
        .ok()
        .or_else(|| serde_json::from_str(&repair_wrapped_json(text)?).ok())?;
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

/// Repair a provider-column-wrapped JSON text by removing raw C0 control
/// characters (`< U+0020`; DEL U+007F is kept) occurring **inside string
/// literals**, tracking in/out-of-string state and honoring backslash
/// escapes. Raw control characters in that range are never valid inside JSON
/// strings (RFC 8259 §7) — real newlines in content arrive escaped as `\n` —
/// so the removal is unambiguous. A wrap that splits an escape sequence
/// (`\` + raw newline + `n`) is reassembled by the same removal. Characters
/// outside string literals are left untouched (whitespace there is legal, and
/// structural corruption should still fail the re-parse). Returns `None` when
/// nothing was removed, so the caller skips the pointless re-parse. Parity
/// with the FE mirror's `charCodeAt < 0x20` check
/// (`daemon-events-bridge.client.ts`, cloudlands-fe PR #347).
fn repair_wrapped_json(text: &str) -> Option<String> {
    let mut repaired = String::with_capacity(text.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut removed = false;
    for c in text.chars() {
        if in_string && c < '\u{20}' {
            removed = true;
            continue;
        }
        match c {
            _ if escaped => escaped = false,
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            _ => {}
        }
        repaired.push(c);
    }
    removed.then_some(repaired)
}

/// The proposal identity of one lifted proposal-resource block:
/// `applyToolCallId ?? preview.title` parsed from the block's embedded
/// proposal JSON (`resource.text`) — the SAME identity
/// `intent_acp::mcp_server::proposal_resource_uri` encodes into the resource
/// URI, so pending-tracking and rendering agree on which proposal is which.
/// `None` for non-proposal blocks (wrong type/MIME), unparseable `text`, or
/// a proposal carrying neither identity field (no fabricated fallback: an
/// identity-less proposal cannot be deduped or resolved, so it is not
/// tracked).
pub(crate) fn proposal_block_id(block: &Value) -> Option<String> {
    if block.get("type").and_then(Value::as_str) != Some("resource") {
        return None;
    }
    let resource = block.get("resource")?;
    if resource.get("mimeType").and_then(Value::as_str) != Some(PROPOSAL_RESOURCE_MIME) {
        return None;
    }
    let proposal: Value = serde_json::from_str(resource.get("text")?.as_str()?).ok()?;
    proposal
        .get("applyToolCallId")
        .and_then(Value::as_str)
        .or_else(|| {
            proposal
                .get("preview")
                .and_then(|p| p.get("title"))
                .and_then(Value::as_str)
        })
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

/// The human-readable `preview.title` of the proposal-resource block whose
/// identity ([`proposal_block_id`]) equals `proposal_id`, scanning `blocks`
/// in order (last match wins, consistent with [`proposal_ids_in`] dedupe).
/// `None` when no block carries the id or the matching proposal has no
/// title — callers fall back to the id itself.
pub(crate) fn proposal_title_in(blocks: &[Value], proposal_id: &str) -> Option<String> {
    let mut title = None;
    for block in blocks {
        if proposal_block_id(block).as_deref() == Some(proposal_id) {
            title = block
                .get("resource")
                .and_then(|r| r.get("text"))
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .and_then(|proposal| {
                    proposal
                        .get("preview")
                        .and_then(|p| p.get("title"))
                        .and_then(Value::as_str)
                        .filter(|t| !t.is_empty())
                        .map(str::to_string)
                });
        }
    }
    title
}

/// Proposal ids carried by a message's content blocks, in block order,
/// deduped within the slice (a later duplicate wins its position — last
/// occurrence order). Backs the turn-end pending-proposals recording: both
/// the registry/array path and the wrapped-echo path in `record_tool` land
/// as lifted proposal-resource blocks in the persisted array, so one scan
/// covers both.
pub(crate) fn proposal_ids_in(blocks: &[Value]) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for block in blocks {
        if let Some(id) = proposal_block_id(block) {
            ids.retain(|existing| existing != &id);
            ids.push(id);
        }
    }
    ids
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

    /// The unbounded-keys regression (PR #1304 review): a `tool_use.input`
    /// with thousands of keys must still serialize within a small constant
    /// factor of the budget — entry admission stops when the budget runs out,
    /// it never emits every key.
    #[test]
    fn slim_tool_block_bounds_key_heavy_inputs() {
        let mut input = Map::new();
        for i in 0..10_000 {
            input.insert(format!("key_number_{i:05}"), json!(i));
        }
        let mut block = json!({
            "type": "tool_use",
            "id": "m:1",
            "name": "big",
            "input": Value::Object(input),
            "toolCallId": "tc-1",
        });
        slim_tool_block(&mut block);
        assert_eq!(block["inputTruncated"], true);
        let served = serde_json::to_string(&block["input"]).unwrap();
        assert!(
            served.len() <= SLIM_PROJECTION_BUDGET_BYTES * 2,
            "served input must stay near the budget, got {} bytes",
            served.len()
        );
    }

    /// Small scalar entries (the fields `classifyTool` reads) survive a giant
    /// blob sibling: entries are admitted smallest-value-first, so the blob
    /// burns the budget LAST instead of starving the small keys.
    #[test]
    fn slim_tool_block_keeps_small_keys_over_giant_sibling() {
        let mut block = json!({
            "type": "tool_use",
            "id": "m:1",
            "name": "write_file",
            "input": {
                "path": "/tmp/a.txt",
                "mode": "overwrite",
                "content": "x".repeat(SLIM_PROJECTION_BUDGET_BYTES * 8),
            },
            "toolCallId": "tc-1",
        });
        slim_tool_block(&mut block);
        assert_eq!(block["inputTruncated"], true);
        assert_eq!(block["input"]["path"], "/tmp/a.txt");
        assert_eq!(block["input"]["mode"], "overwrite");
        let content = block["input"]["content"].as_str().unwrap();
        assert!(content.len() < SLIM_PROJECTION_BUDGET_BYTES * 8);
    }

    /// Under-budget tool blocks and non-tool blocks pass through untouched —
    /// no flags, byte-identical.
    #[test]
    fn slim_tool_block_passes_small_and_foreign_blocks_through() {
        let mut small = json!({
            "type": "tool_result",
            "id": "m:2",
            "tool_use_id": "tc-1",
            "output": "12 tests passed",
            "is_error": false,
        });
        let before = small.clone();
        slim_tool_block(&mut small);
        assert_eq!(small, before);

        let mut text = json!({ "type": "text", "id": "m:0", "text": "hi" });
        let before = text.clone();
        slim_tool_block(&mut text);
        assert_eq!(text, before);
    }

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

    /// Hard-wrap every line of `text` at `col` characters by inserting raw
    /// newlines, reproducing auggie's column-wrapped echo of collapsed tool
    /// output (wraps land mid-word, inside JSON string literals). Chunks by
    /// chars so non-ASCII fixture data can't split a multi-byte sequence.
    fn column_wrap(text: &str, col: usize) -> String {
        text.lines()
            .flat_map(|line| {
                line.chars()
                    .collect::<Vec<_>>()
                    .chunks(col)
                    .map(|c| c.iter().collect::<String>())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A proposal whose pretty-printed lines exceed the provider's 1000-char
    /// wrap column, mirroring the observed corrupt payload
    /// (`workspace-create` with a long `initialPrompt`).
    fn long_proposal() -> Value {
        let prompt = "Fix these 4 open bugs in parallel. ".repeat(40) + "End of the long prompt.";
        json!({
            "kind": "workspace-create",
            "preview": { "title": "Create workspace: Parallel bug batch" },
            "payload": {
                "operation": "workspace.create",
                "params": { "initialPrompt": prompt },
            },
        })
    }

    /// The provider-wrapped shape observed in the wild: the daemon's
    /// `{ok, proposal}` text-item payload pretty-printed, then hard-wrapped at
    /// a 1000-char column with raw newlines injected inside string literals.
    fn wrapped_collapsed_text(proposal: &Value) -> String {
        let pretty = serde_json::to_string_pretty(&json!({ "ok": true, "proposal": proposal }))
            .expect("serialize");
        let wrapped = column_wrap(&pretty, 1000);
        // The wrap must actually corrupt the payload for the test to mean
        // anything: raw newlines inside string literals break strict JSON.
        assert!(serde_json::from_str::<Value>(&wrapped).is_err());
        wrapped
    }

    #[test]
    fn lift_proposal_resource_repairs_provider_wrapped_object_output() {
        let proposal = long_proposal();
        let output = json!({ "output": wrapped_collapsed_text(&proposal) });
        let item = lift_proposal_resource(&output).expect("wrapped fallback lifted");
        assert_eq!(item["resource"]["mimeType"], PROPOSAL_RESOURCE_MIME);
        assert_eq!(
            item["resource"]["uri"],
            "intent-proposal://workspace-create/Create%20workspace%3A%20Parallel%20bug%20batch"
        );
        // The repaired `text` round-trips to the original proposal — the raw
        // wrap newlines are gone, the content is otherwise untouched.
        let text = item["resource"]["text"].as_str().expect("text is a string");
        let parsed: Value = serde_json::from_str(text).expect("text parses");
        assert_eq!(parsed, proposal);
    }

    #[test]
    fn lift_proposal_resource_repairs_provider_wrapped_string_output() {
        let proposal = long_proposal();
        let item = lift_proposal_resource(&json!(wrapped_collapsed_text(&proposal)))
            .expect("wrapped bare-string fallback lifted");
        let text = item["resource"]["text"].as_str().expect("text is a string");
        let parsed: Value = serde_json::from_str(text).expect("text parses");
        assert_eq!(parsed, proposal);
    }

    #[test]
    fn lift_repair_preserves_escaped_newlines_in_content() {
        // Real newlines in proposal content arrive escaped (`\n` in the JSON
        // text); only the RAW wrap newlines are removed by the repair.
        let mut proposal = long_proposal();
        proposal["payload"]["params"]["initialPrompt"] =
            json!("line one\nline two\n".repeat(60) + "tail");
        let output = json!({ "output": wrapped_collapsed_text(&proposal) });
        let item = lift_proposal_resource(&output).expect("wrapped fallback lifted");
        let text = item["resource"]["text"].as_str().expect("text is a string");
        let parsed: Value = serde_json::from_str(text).expect("text parses");
        assert_eq!(parsed, proposal);
    }

    #[test]
    fn lift_repair_handles_wrap_splitting_an_escape_sequence() {
        // A wrap column can land between the backslash and the letter of an
        // escape sequence (`\` + raw newline + `n`); removing the raw newline
        // must reassemble the escape.
        let text = "{\"ok\": true, \"proposal\": {\"kind\": \"settings-change\", \"preview\": {\"title\": \"T\\\nnT\"}, \"payload\": {}}}";
        assert!(serde_json::from_str::<Value>(text).is_err());
        let item = lift_proposal_resource(&json!({ "output": text })).expect("lifted");
        let lifted: Value =
            serde_json::from_str(item["resource"]["text"].as_str().unwrap()).unwrap();
        assert_eq!(lifted["preview"]["title"], "T\nT");
    }

    #[test]
    fn lift_repair_strips_raw_tab_inside_string_literal() {
        // The repair covers the full C0 control range (< U+0020), not just
        // newlines: a raw TAB inside a string literal is equally invalid JSON
        // and is stripped (parity with the FE mirror's `charCodeAt < 0x20`).
        let text = "{\"ok\": true, \"proposal\": {\"kind\": \"settings-change\", \"preview\": {\"title\": \"Ta\tb\"}, \"payload\": {}}}";
        assert!(serde_json::from_str::<Value>(text).is_err());
        let item = lift_proposal_resource(&json!({ "output": text })).expect("lifted");
        let lifted: Value =
            serde_json::from_str(item["resource"]["text"].as_str().unwrap()).unwrap();
        assert_eq!(lifted["preview"]["title"], "Tab");
    }

    /// A persisted proposal-resource block carrying `proposal_json` as its
    /// embedded `resource.text`, id-stamped like the turn-end drain leaves it.
    fn proposal_block(proposal: &Value) -> Value {
        json!({
            "type": "resource",
            "id": "m:5",
            "resource": {
                "uri": intent_acp::mcp_server::proposal_resource_uri(proposal),
                "name": "P",
                "mimeType": PROPOSAL_RESOURCE_MIME,
                "text": serde_json::to_string(proposal).unwrap(),
            }
        })
    }

    #[test]
    fn proposal_block_id_prefers_apply_tool_call_id() {
        let mut proposal = valid_proposal();
        proposal["applyToolCallId"] = json!("tc-apply-9");
        assert_eq!(
            proposal_block_id(&proposal_block(&proposal)).as_deref(),
            Some("tc-apply-9")
        );
    }

    #[test]
    fn proposal_block_id_falls_back_to_preview_title() {
        assert_eq!(
            proposal_block_id(&proposal_block(&valid_proposal())).as_deref(),
            Some("Update Test Setting")
        );
    }

    #[test]
    fn proposal_block_id_rejects_non_proposal_and_malformed_blocks() {
        // Wrong type / MIME.
        assert!(proposal_block_id(&json!({ "type": "text", "text": "hi" })).is_none());
        let mut wrong_mime = proposal_block(&valid_proposal());
        wrong_mime["resource"]["mimeType"] = json!("text/plain");
        assert!(proposal_block_id(&wrong_mime).is_none());
        // Unparseable embedded text.
        let mut bad_text = proposal_block(&valid_proposal());
        bad_text["resource"]["text"] = json!("not json");
        assert!(proposal_block_id(&bad_text).is_none());
        // No identity: neither applyToolCallId nor preview.title.
        let mut no_identity = valid_proposal();
        no_identity["preview"] = json!({});
        assert!(proposal_block_id(&proposal_block(&no_identity)).is_none());
    }

    #[test]
    fn proposal_ids_in_collects_in_order_and_dedupes_last_wins() {
        let mut a = valid_proposal();
        a["applyToolCallId"] = json!("tc-a");
        let mut b = valid_proposal();
        b["applyToolCallId"] = json!("tc-b");
        let blocks = vec![
            json!({ "type": "text", "text": "two proposals" }),
            proposal_block(&a),
            proposal_block(&b),
            proposal_block(&a),
        ];
        assert_eq!(proposal_ids_in(&blocks), vec!["tc-b", "tc-a"]);
        assert!(proposal_ids_in(&[json!({ "type": "text", "text": "none" })]).is_empty());
    }

    #[test]
    fn lift_repair_does_not_rescue_genuinely_invalid_json() {
        // Truncated payload: no amount of control-char stripping makes this
        // parse; the lift must still decline.
        let proposal = long_proposal();
        let wrapped = wrapped_collapsed_text(&proposal);
        let cut = wrapped.char_indices().rev().nth(39).map_or(0, |(i, _)| i);
        let truncated = &wrapped[..cut];
        assert!(lift_proposal_resource(&json!({ "output": truncated })).is_none());
        // Structural corruption outside strings (unquoted garbage) as well.
        let garbage = "{\"ok\": true, \"proposal\": not json at\nall}";
        assert!(lift_proposal_resource(&json!({ "output": garbage })).is_none());
    }
}
