//! `ws.app.specialists.*` bindings (chief-gated).
//!
//! Exposes specialist read methods (`list`, `get`) exclusively to Chief-of-Staff
//! workspace agents. Non-chief agents receive a clear gating error. Uses the
//! existing 3-tier specialist loader in intent-services. Shape parity with the
//! TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-specialists-api.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.specialists = {
        list: () => host({ method: 'app.specialists.list', args: {} }),
        get: (id) => host({ method: 'app.specialists.get', args: { id } }),
        propose: (input) => host({ method: 'app.specialists.propose', args: input }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating: all ws.app.* methods require the caller to be
    // in the Chief workspace.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    match method {
        "list" => list(api, args).await,
        "get" => get(api, args).await,
        "propose" => propose(api, args).await,
        other => Err(format!("host: unknown method `app.specialists.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, _args: &Value) -> Result<Value, String> {
    // Fetch all specialists from the 3-tier loader (no workspace_path for chief)
    let result = api
        .specialist_list(None, None)
        .await
        .map_err(|e| format!("specialist.list failed: {e}"))?;

    // The daemon returns { specialists: SpecialistDef[] }
    let specialists = result
        .get("specialists")
        .and_then(Value::as_array)
        .ok_or_else(|| "specialists.list returned invalid shape".to_string())?;

    // The wire shape already matches the TS reference (id, name, description,
    // model, prompt, behaviorPrompt, source, isCustomized, etc.)
    Ok(Value::Array(specialists.clone()))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "Specialist id is required".to_string())?;

    // Fetch the specialist from the 3-tier loader
    let result = api
        .specialist_get(id.to_string(), None, None)
        .await
        .map_err(|e| match e {
            intent_core::Error::NotFound(_) => format!("Specialist not found: {id}"),
            _ => format!("specialist.get failed: {e}"),
        })?;

    // The daemon returns { specialist: SpecialistDef }
    let specialist = result
        .get("specialist")
        .ok_or_else(|| "specialists.get returned invalid shape".to_string())?;

    Ok(specialist.clone())
}

/// MCP resource MIME type for proposals (parity with FE `proposal-resource.ts`).
const PROPOSAL_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.proposal+json";

/// Build proposal resource URI (parity with TS `proposalResourceId` + `createProposalResource`).
fn proposal_resource_uri(proposal: &Value) -> String {
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
    let encoded_id = super::proposal::percent_encode_path_segment(id);
    format!("intent-proposal://{kind}/{encoded_id}")
}

/// Return a proposal with dual text+resource content items.
#[allow(clippy::unnecessary_wraps)] // dispatch arm helper; keeps the uniform Result shape
fn proposal_result(proposal: &Value) -> Result<Value, String> {
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
            "text": serde_json::to_string(&proposal).unwrap_or_else(|_| "{}".to_string())
        }
    });

    // Return with __mcpContentItems marker (dispatch.rs will extract this)
    Ok(json!({
        "ok": true,
        "proposal": proposal,
        "__mcpContentItems": [text_item, resource_item]
    }))
}

fn non_empty_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
}

async fn propose(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    if !args.is_object() {
        return Err("propose() requires a proposal object".to_string());
    }

    // Determine operation (create, edit, or delete)
    let operation = args
        .get("action")
        .or_else(|| args.get("operation"))
        .and_then(Value::as_str)
        .or_else(|| {
            if args.get("create").is_some() {
                Some("create")
            } else if args.get("edit").is_some() {
                Some("edit")
            } else if args.get("delete").is_some() {
                Some("delete")
            } else {
                None
            }
        })
        .ok_or_else(|| {
            "propose() requires action/operation to be create, edit, or delete".to_string()
        })?;

    // Extract the payload for the operation
    let mut payload = args.clone();
    if let Some(op_obj) = args.get(operation) {
        if let Some(op_map) = op_obj.as_object() {
            if let Some(payload_map) = payload.as_object_mut() {
                for (k, v) in op_map {
                    payload_map.insert(k.clone(), v.clone());
                }
            }
        }
    }

    // Extract and validate id/name/prompt
    let id = payload.get("id").and_then(non_empty_string);
    let name = payload.get("name").and_then(non_empty_string);
    let prompt = payload
        .get("prompt")
        .or_else(|| payload.get("behaviorPrompt"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);

    // For edit/delete, validate specialist exists
    let current = if operation == "edit" || operation == "delete" {
        let specialist_id = id
            .as_ref()
            .ok_or_else(|| "Specialist not found: (missing id)".to_string())?;

        let result = api
            .specialist_get(specialist_id.clone(), None, None)
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    format!("Specialist not found: {specialist_id}")
                } else {
                    format!("specialist.get failed: {e}")
                }
            })?;

        result
            .get("specialist")
            .ok_or_else(|| "specialists.get returned invalid shape".to_string())?
            .clone()
    } else {
        json!({})
    };

    // Build draft combining current + payload
    let draft_id = id
        .or_else(|| current.get("id").and_then(non_empty_string))
        .ok_or_else(|| "Specialist id is required".to_string())?;

    let draft_name = name
        .or_else(|| current.get("name").and_then(non_empty_string))
        .unwrap_or_default();

    let draft_prompt = prompt
        .or_else(|| {
            current
                .get("behaviorPrompt")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .unwrap_or_default();

    // Validate for create/edit
    if operation != "delete" {
        if draft_name.is_empty() {
            return Err("Specialist name is required".to_string());
        }
        if draft_prompt.trim().is_empty() {
            return Err("Specialist prompt is required".to_string());
        }
    }

    let draft_description = payload
        .get("description")
        .and_then(non_empty_string)
        .or_else(|| current.get("description").and_then(non_empty_string))
        .unwrap_or_else(|| "Custom specialist".to_string());

    let draft_model = payload
        .get("model")
        .and_then(non_empty_string)
        .or_else(|| current.get("model").and_then(non_empty_string))
        .unwrap_or_default();

    // Build fields for preview
    let editable = operation != "delete";
    let mut fields = vec![
        json!({
            "key": "name",
            "label": "Name",
            "value": draft_name,
            "before": current.get("name"),
            "after": draft_name,
            "editable": editable
        }),
        json!({
            "key": "description",
            "label": "Description",
            "value": draft_description,
            "before": current.get("description"),
            "after": draft_description,
            "editable": editable,
            "multiline": true
        }),
        json!({
            "key": "model",
            "label": "Model",
            "value": draft_model,
            "before": current.get("model"),
            "after": draft_model,
            "editable": editable
        }),
        json!({
            "key": "prompt",
            "label": "Prompt",
            "value": draft_prompt,
            "before": current.get("behaviorPrompt"),
            "after": draft_prompt,
            "editable": editable,
            "multiline": true
        }),
    ];

    // Remove undefined before/after fields
    for field in &mut fields {
        if let Some(obj) = field.as_object_mut() {
            if obj.get("before").and_then(Value::as_str).is_none() {
                obj.remove("before");
                obj.remove("after");
            } else {
                obj.remove("value");
            }
        }
    }

    let title_verb = match operation {
        "create" => "Create",
        "edit" => "Edit",
        "delete" => "Delete",
        _ => "Modify",
    };

    let summary = if operation == "delete" {
        "Deletes the file-based specialist or removes a user override for a built-in specialist."
            .to_string()
    } else {
        "Review and edit the specialist fields before applying.".to_string()
    };

    let proposal = json!({
        "kind": "specialist-edit",
        "payload": {
            "operation": operation,
            "id": draft_id,
            "name": draft_name,
            "description": draft_description,
            "model": draft_model,
            "prompt": draft_prompt,
            "behaviorPrompt": draft_prompt
        },
        "preview": {
            "title": format!("{} specialist: {}", title_verb, if draft_name.is_empty() { &draft_id } else { &draft_name }),
            "summary": summary,
            "fields": fields,
            "warnings": if operation == "delete" {
                Some(vec!["Applying this proposal dispatches the same delete action used by the specialist editor."])
            } else {
                None
            }
        }
    });

    proposal_result(&proposal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Error, Result};
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Default)]
    struct FakeApi {}

    impl WorkspaceApi for FakeApi {
        fn specialist_list(
            &self,
            _workspace_path: Option<String>,
            _provider: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "specialists": [
                        {
                            "id": "implementor",
                            "name": "Implementor",
                            "description": "Implements tasks",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are an implementor",
                            "behaviorPrompt": "Focus on implementation",
                            "source": "builtin",
                            "isCustomized": false
                        },
                        {
                            "id": "verifier",
                            "name": "Verifier",
                            "description": "Verifies work",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are a verifier",
                            "behaviorPrompt": "Focus on verification",
                            "source": "builtin",
                            "isCustomized": false
                        }
                    ]
                }))
            })
        }

        fn specialist_get(
            &self,
            id: String,
            _workspace_path: Option<String>,
            _provider: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                match id.as_str() {
                    "implementor" => Ok(json!({
                        "specialist": {
                            "id": "implementor",
                            "name": "Implementor",
                            "description": "Implements tasks",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are an implementor",
                            "behaviorPrompt": "Focus on implementation",
                            "source": "builtin",
                            "isCustomized": false
                        }
                    })),
                    _ => Err(Error::NotFound(format!("Specialist not found: {id}"))),
                }
            })
        }
    }

    #[tokio::test]
    async fn test_dispatch_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(&api, &non_chief_id, "list", &json!({})).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_list_returns_expected_shape() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();

        let specialists = result.as_array().unwrap();
        assert_eq!(specialists.len(), 2);

        // Check expected fields are present
        for specialist in specialists {
            assert!(specialist.get("id").is_some());
            assert!(specialist.get("name").is_some());
            assert!(specialist.get("description").is_some());
            assert!(specialist.get("model").is_some());
            assert!(specialist.get("prompt").is_some());
            assert!(specialist.get("behaviorPrompt").is_some());
            assert!(specialist.get("source").is_some());
            assert!(specialist.get("isCustomized").is_some());
        }
    }

    #[tokio::test]
    async fn test_get_returns_expected_shape() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "implementor" }))
            .await
            .unwrap();

        // Check expected fields are present
        assert_eq!(result.get("id").unwrap().as_str().unwrap(), "implementor");
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "Implementor");
        assert!(result.get("description").is_some());
        assert!(result.get("model").is_some());
        assert!(result.get("prompt").is_some());
        assert!(result.get("behaviorPrompt").is_some());
        assert!(result.get("source").is_some());
        assert!(result.get("isCustomized").is_some());
    }

    #[tokio::test]
    async fn test_get_missing_specialist_returns_error() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "nonexistent" })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Specialist not found: nonexistent");
    }

    // Proposal tests

    #[tokio::test]
    async fn test_propose_rejects_non_chief() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(
            &api,
            &non_chief_id,
            "propose",
            &json!({ "action": "create", "id": "new-spec", "name": "New", "prompt": "Test" }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_propose_requires_object() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "propose", &json!("invalid")).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("propose() requires a proposal object"));
    }

    #[tokio::test]
    async fn test_propose_requires_operation() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "propose", &json!({ "id": "test" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("propose() requires action/operation to be create, edit, or delete"));
    }

    #[tokio::test]
    async fn test_propose_create_requires_name() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "action": "create", "id": "test", "prompt": "Test prompt" }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Specialist name is required");
    }

    #[tokio::test]
    async fn test_propose_create_requires_prompt() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "action": "create", "id": "test", "name": "Test" }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Specialist prompt is required");
    }

    #[tokio::test]
    async fn test_propose_create_returns_proposal() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({
                "action": "create",
                "id": "new-spec",
                "name": "New Specialist",
                "prompt": "You are a new specialist"
            }),
        )
        .await
        .unwrap();

        // Should have proposal and content items
        assert!(result.get("ok").unwrap().as_bool().unwrap());
        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "specialist-edit"
        );

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "create"
        );
        assert_eq!(payload.get("id").unwrap().as_str().unwrap(), "new-spec");

        let preview = proposal.get("preview").unwrap();
        assert!(preview
            .get("title")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("Create specialist:"));
    }

    #[tokio::test]
    async fn test_propose_edit_requires_existing_specialist() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "action": "edit", "id": "nonexistent", "name": "Updated" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Specialist not found: nonexistent"));
    }

    #[tokio::test]
    async fn test_propose_edit_returns_proposal() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({
                "action": "edit",
                "id": "implementor",
                "name": "Updated Implementor"
            }),
        )
        .await
        .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "specialist-edit"
        );

        let payload = proposal.get("payload").unwrap();
        assert_eq!(payload.get("operation").unwrap().as_str().unwrap(), "edit");
        assert_eq!(payload.get("id").unwrap().as_str().unwrap(), "implementor");

        let preview = proposal.get("preview").unwrap();
        assert!(preview
            .get("title")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("Edit specialist:"));
    }

    #[tokio::test]
    async fn test_propose_delete_requires_existing_specialist() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "action": "delete", "id": "nonexistent" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Specialist not found: nonexistent"));
    }

    #[tokio::test]
    async fn test_propose_delete_returns_proposal_with_warnings() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "action": "delete", "id": "implementor" }),
        )
        .await
        .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "specialist-edit"
        );

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "delete"
        );

        let preview = proposal.get("preview").unwrap();
        assert!(preview
            .get("title")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("Delete specialist:"));
        assert!(preview.get("warnings").is_some());
    }

    #[tokio::test]
    async fn test_propose_has_mcp_content_items() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({
                "action": "create",
                "id": "test",
                "name": "Test",
                "prompt": "Test prompt"
            }),
        )
        .await
        .unwrap();

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);

        // Text item
        assert_eq!(items[0].get("type").unwrap().as_str().unwrap(), "text");
        let text = items[0].get("text").unwrap().as_str().unwrap();
        assert!(text.contains("\"ok\": true"));

        // Resource item
        assert_eq!(items[1].get("type").unwrap().as_str().unwrap(), "resource");
        let resource = items[1].get("resource").unwrap();
        assert_eq!(
            resource.get("mimeType").unwrap().as_str().unwrap(),
            "application/vnd.intent.proposal+json"
        );
        assert!(resource
            .get("uri")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("intent-proposal://"));
    }
}
