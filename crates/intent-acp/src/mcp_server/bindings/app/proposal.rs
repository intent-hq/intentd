//! `ws.app.proposal.*` bindings (chief-gated).
//!
//! Exposes proposal-rendering methods (`show`) exclusively to Chief-of-Staff
//! workspace agents. Non-chief agents receive a clear gating error. Shape parity
//! with the TS reference `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-proposal-content.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.proposal = {
        show: (proposal) => host({ method: 'app.proposal.show', args: { proposal } }),
    };
";

/// MCP resource MIME type for proposals (parity with FE `proposal-resource.ts`).
pub const PROPOSAL_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.proposal+json";

/// Valid proposal kinds (parity with TS `PROPOSAL_KINDS`).
pub(crate) const PROPOSAL_KINDS: &[&str] = &[
    "workspace-create",
    "settings-change",
    "specialist-edit",
    "bulk-op",
];

pub(crate) async fn dispatch(
    _api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    match method {
        "show" => show(args),
        other => Err(format!("host: unknown method `app.proposal.{other}`")),
    }
}

/// Port of TS `isProposal` validation from `proposal.ts`.
///
/// Public (re-exported at `intent_acp::mcp_server`) so the §7.1
/// collapsed-output proposal lift in `intent-services::tool_block` validates
/// against the SAME canonical rules — one source, no drift.
pub fn is_valid_proposal(value: &Value) -> bool {
    if let Some(obj) = value.as_object() {
        // Check kind
        let kind_valid = obj
            .get("kind")
            .and_then(Value::as_str)
            .is_some_and(|k| PROPOSAL_KINDS.contains(&k));

        // Check preview exists and has title
        let preview_valid = obj
            .get("preview")
            .and_then(Value::as_object)
            .and_then(|p| p.get("title"))
            .and_then(Value::as_str)
            .is_some_and(|t| !t.is_empty());

        // Check payload exists and is an object
        let payload_valid = obj.get("payload").and_then(Value::as_object).is_some();

        kind_valid && preview_valid && payload_valid
    } else {
        false
    }
}

/// Build proposal resource URI (parity with TS `proposalResourceId` + `createProposalResource`).
///
/// Public (re-exported at `intent_acp::mcp_server`) so the §7.1
/// collapsed-output lift rebuilds the identical URI.
pub fn proposal_resource_uri(proposal: &Value) -> String {
    let kind = proposal
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    // Use applyToolCallId if present, otherwise use preview.title
    let id = proposal
        .get("applyToolCallId")
        .and_then(Value::as_str)
        .or_else(|| {
            proposal
                .get("preview")
                .and_then(|p| p.get("title"))
                .and_then(Value::as_str)
        })
        .unwrap_or("untitled");

    // RFC3986 percent-encode the id portion for URI path segment use
    let encoded_id = percent_encode_path_segment(id);
    format!("intent-proposal://{kind}/{encoded_id}")
}

/// RFC3986 percent-encoding for URI path segments.
/// Encodes all characters except unreserved (A-Z a-z 0-9 - _ . ~).
pub(super) fn percent_encode_path_segment(s: &str) -> String {
    s.as_bytes()
        .iter()
        .flat_map(|&b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect()
}

/// `ws.app.proposal.show(proposal)` — validate and return dual text+resource content items.
fn show(args: &Value) -> Result<Value, String> {
    let proposal = args
        .get("proposal")
        .ok_or_else(|| "`proposal` is required".to_string())?;

    if !is_valid_proposal(proposal) {
        return Err("Invalid proposal: must have `kind` (one of workspace-create, settings-change, specialist-edit, bulk-op), `preview.title`, and `payload`".to_string());
    }

    // Build resource name from preview.title
    let name = proposal
        .get("preview")
        .and_then(|p| p.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("Proposal");

    // Build MCP content items: text item with {ok, proposal} + resource item
    let text_item = json!({
        "type": "text",
        "text": serde_json::to_string_pretty(&json!({
            "ok": true,
            "proposal": proposal
        })).unwrap_or_else(|_| "{}".to_string())
    });

    let resource_item = json!({
        "type": "resource",
        "resource": {
            "uri": proposal_resource_uri(proposal),
            "name": name,
            "mimeType": PROPOSAL_RESOURCE_MIME_TYPE,
            "text": serde_json::to_string(proposal).unwrap_or_else(|_| "{}".to_string())
        }
    });

    // Return with __mcpContentItems marker (dispatch.rs will extract this)
    Ok(json!({
        "ok": true,
        "proposal": proposal,
        "__mcpContentItems": [text_item, resource_item]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_proposal() -> Value {
        json!({
            "kind": "workspace-create",
            "preview": {
                "title": "Test Proposal"
            },
            "payload": {
                "operation": "workspace.create"
            }
        })
    }

    #[test]
    fn is_valid_proposal_accepts_valid_proposal() {
        assert!(is_valid_proposal(&valid_proposal()));
    }

    #[test]
    fn is_valid_proposal_rejects_missing_kind() {
        let mut p = valid_proposal();
        p.as_object_mut().unwrap().remove("kind");
        assert!(!is_valid_proposal(&p));
    }

    #[test]
    fn is_valid_proposal_rejects_invalid_kind() {
        let mut p = valid_proposal();
        p["kind"] = json!("invalid-kind");
        assert!(!is_valid_proposal(&p));
    }

    #[test]
    fn is_valid_proposal_rejects_missing_preview_title() {
        let mut p = valid_proposal();
        p["preview"] = json!({});
        assert!(!is_valid_proposal(&p));
    }

    #[test]
    fn is_valid_proposal_rejects_missing_payload() {
        let mut p = valid_proposal();
        p.as_object_mut().unwrap().remove("payload");
        assert!(!is_valid_proposal(&p));
    }

    #[test]
    fn show_returns_dual_content_items() {
        let proposal = valid_proposal();
        let args = json!({"proposal": proposal});
        let result = show(&args).expect("show should succeed");

        // Should have __mcpContentItems
        let items = result["__mcpContentItems"]
            .as_array()
            .expect("should have content items");
        assert_eq!(items.len(), 2);

        // First item is text
        assert_eq!(items[0]["type"], "text");
        let text = items[0]["text"].as_str().expect("text should be string");
        assert!(text.contains("\"ok\": true"));
        assert!(text.contains("\"proposal\""));

        // Second item is resource
        assert_eq!(items[1]["type"], "resource");
        let resource = &items[1]["resource"];
        assert_eq!(resource["mimeType"], "application/vnd.intent.proposal+json");
        assert!(resource["uri"]
            .as_str()
            .expect("uri")
            .starts_with("intent-proposal://"));
        assert_eq!(resource["name"], "Test Proposal");
    }

    #[test]
    fn show_rejects_invalid_proposal() {
        let invalid = json!({"kind": "invalid"});
        let args = json!({"proposal": invalid});
        let result = show(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid proposal"));
    }

    #[test]
    fn show_rejects_missing_proposal() {
        let args = json!({});
        let result = show(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("`proposal` is required"));
    }

    #[test]
    fn proposal_resource_uri_uses_apply_tool_call_id_when_present() {
        let proposal = json!({
            "kind": "settings-change",
            "applyToolCallId": "tool-123",
            "preview": {"title": "Change Setting"},
            "payload": {}
        });
        let uri = proposal_resource_uri(&proposal);
        assert_eq!(uri, "intent-proposal://settings-change/tool-123");
    }

    #[test]
    fn proposal_resource_uri_encodes_special_characters() {
        let proposal = json!({
            "kind": "workspace-create",
            "preview": {"title": "Test / Workspace : New?"},
            "payload": {}
        });
        let uri = proposal_resource_uri(&proposal);
        // Verify that special characters (/, :, ?, space) are percent-encoded
        assert_eq!(
            uri,
            "intent-proposal://workspace-create/Test%20%2F%20Workspace%20%3A%20New%3F"
        );
    }

    #[test]
    fn percent_encode_path_segment_handles_multibyte_utf8() {
        assert_eq!(percent_encode_path_segment("héllo"), "h%C3%A9llo");
        assert_eq!(percent_encode_path_segment("日本"), "%E6%97%A5%E6%9C%AC");
        assert_eq!(percent_encode_path_segment("a-b_c.~"), "a-b_c.~");
    }

    #[test]
    fn proposal_resource_uri_falls_back_to_title() {
        let proposal = json!({
            "kind": "bulk-op",
            "preview": {"title": "Bulk Delete"},
            "payload": {}
        });
        let uri = proposal_resource_uri(&proposal);
        assert_eq!(uri, "intent-proposal://bulk-op/Bulk%20Delete");
    }
}
