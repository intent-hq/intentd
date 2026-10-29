//! `ws.secret.*` bindings — agent secret requests (PROTOCOL §5.56).
//!
//! `request` records a pending request in the daemon's memory-only vault and
//! registers ONE `AtTurnEnd` turn attachment, so the card lands as a trailing
//! `application/vnd.intent.secret-request+json` resource block on the turn's
//! final assistant message — the same drain as `ws.app.question.ask`. The
//! user answers through `secret.provide` / `secret.dismiss` on the wire, never
//! through the chat, so the value never reaches the model.
//!
//! `list` and `forget` act on the calling agent's own entry only. Secrets are
//! used through the agent-only `secretEnv` option of `ws.host.exec` (see the
//! `host` binding). Availability is decided by the caller in
//! [`super::try_dispatch`]: the effective `agentFeatures.secretRequests`
//! toggle with `hostExec` on, a top-level caller and a non-chief workspace.

use std::sync::Arc;

use intent_core::{
    new_attachment_id, AgentId, AttachmentPolicy, TurnAttachment, TurnAttachmentRegistry,
    WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};

use super::map_err;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.secret = {
        request: (spec) => host({ method: 'secret.request', args: spec || {} }),
        list: () => host({ method: 'secret.list', args: {} }),
        forget: (name) => host({ method: 'secret.forget', args: { name } }),
    };
";

/// MCP resource MIME type of a secret-request card (PROTOCOL §5.56, §7).
pub const SECRET_REQUEST_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.secret-request+json";

/// The error a frame gets when the feature is not effective for this caller.
pub(crate) const SECRET_REQUESTS_UNAVAILABLE: &str =
    "ws.secret is not available to this agent (agentFeatures.secretRequests and agentFeatures.hostExec must be on, for a top-level agent outside the Assistant workspace)";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller: Option<&AgentId>,
    registry: Option<&Arc<TurnAttachmentRegistry>>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    let Some(caller) = caller else {
        return Err(
            "ws.secret is only available to an agent (no caller agent is wired)".to_string(),
        );
    };
    match method {
        "request" => request(api, workspace_id, caller, registry, args).await,
        "list" => {
            let mut listing = api
                .secret_list(workspace_id.clone(), caller.clone())
                .await
                .map_err(map_err)?;
            // The agent view is `{ secrets, pendingRequests }`; answered
            // outcomes serve the app's card state only.
            if let Some(fields) = listing.as_object_mut() {
                fields.remove("answeredRequests");
            }
            Ok(listing)
        }
        "forget" => {
            let name = args
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| "name is required".to_string())?;
            api.secret_forget(workspace_id.clone(), caller.clone(), name.to_string())
                .await
                .map_err(map_err)
        }
        other => Err(format!("host: unknown method `secret.{other}`")),
    }
}

/// `ws.secret.request({ name, reason })` — validate and record the request in
/// the daemon, then queue its card for turn end. A repeat request for a name
/// that is already pending reuses that request and queues no second card.
async fn request(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller: &AgentId,
    registry: Option<&Arc<TurnAttachmentRegistry>>,
    args: &Value,
) -> Result<Value, String> {
    let Some(registry) = registry else {
        return Err(
            "ws.secret.request is only available from a live agent turn, not from a background hook; return { dispatch: true, message } from the hook so the agent asks the user instead"
                .to_string(),
        );
    };
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "`name` is required and must be a string".to_string())?;
    let reason = args
        .get("reason")
        .and_then(Value::as_str)
        .ok_or_else(|| "`reason` is required and must be a string".to_string())?;
    let ticket = api
        .secret_request(
            workspace_id.clone(),
            caller.clone(),
            name.to_string(),
            reason.to_string(),
        )
        .await
        .map_err(map_err)?;
    let request_id = ticket
        .get("requestId")
        .and_then(Value::as_str)
        .ok_or_else(|| "secret request returned no requestId".to_string())?
        .to_string();
    let created = ticket.get("created").and_then(Value::as_bool) == Some(true);
    if created {
        let payload = json!({
            "requestId": request_id,
            "agentId": caller.0,
            "name": ticket.get("name").cloned().unwrap_or(Value::Null),
            "reason": ticket.get("reason").cloned().unwrap_or(Value::Null),
        });
        let text = serde_json::to_string(&payload)
            .map_err(|e| format!("failed to serialize secret request: {e}"))?;
        registry.register(
            caller,
            TurnAttachment {
                id: new_attachment_id(),
                policy: AttachmentPolicy::AtTurnEnd,
                mime_type: SECRET_REQUEST_RESOURCE_MIME_TYPE.to_string(),
                uri: format!("intent-secret-request://{request_id}"),
                name: name.to_string(),
                text,
            },
        );
    }
    Ok(json!({
        "ok": true,
        "requestId": request_id,
        "message": if created {
            "Secret request queued: the user sees a masked input card when this turn ends. End your turn now. You will get a message when the user provides or declines it; never ask for the value in chat."
        } else {
            "A request for this secret is already waiting for the user; no new card was added. End your turn and wait for the user's answer."
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result};

    struct FakeApi;

    impl WorkspaceApi for FakeApi {
        fn secret_request(
            &self,
            _workspace_id: WorkspaceId,
            agent_id: AgentId,
            name: String,
            reason: String,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "requestId": "sec-1",
                    "agentId": agent_id.0,
                    "name": name,
                    "reason": reason.trim(),
                    "created": name != "PENDING",
                }))
            })
        }
    }

    fn api() -> Arc<dyn WorkspaceApi> {
        Arc::new(FakeApi)
    }

    fn agent() -> AgentId {
        AgentId::from_string("agent-a")
    }

    #[tokio::test]
    async fn request_registers_one_turn_end_card_without_a_value() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let out = dispatch(
            &api(),
            &WorkspaceId::from_string("w"),
            Some(&agent()),
            Some(&registry),
            "request",
            &json!({ "name": "KEY", "reason": " run tests " }),
        )
        .await
        .unwrap();
        assert_eq!(out["ok"], json!(true));
        assert_eq!(out["requestId"], json!("sec-1"));
        let drained = registry.finish_turn(&agent());
        assert_eq!(drained.len(), 1);
        let card = &drained[0];
        assert_eq!(card.mime_type, SECRET_REQUEST_RESOURCE_MIME_TYPE);
        assert_eq!(card.uri, "intent-secret-request://sec-1");
        assert_eq!(card.name, "KEY");
        let payload: Value = serde_json::from_str(&card.text).unwrap();
        assert_eq!(
            payload,
            json!({ "requestId": "sec-1", "agentId": "agent-a", "name": "KEY", "reason": "run tests" })
        );
    }

    #[tokio::test]
    async fn reused_request_adds_no_second_card() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let out = dispatch(
            &api(),
            &WorkspaceId::from_string("w"),
            Some(&agent()),
            Some(&registry),
            "request",
            &json!({ "name": "PENDING", "reason": "r" }),
        )
        .await
        .unwrap();
        assert_eq!(out["requestId"], json!("sec-1"));
        assert!(registry.finish_turn(&agent()).is_empty());
    }

    #[tokio::test]
    async fn request_needs_a_live_turn_and_a_caller() {
        let err = dispatch(
            &api(),
            &WorkspaceId::from_string("w"),
            Some(&agent()),
            None,
            "request",
            &json!({ "name": "KEY", "reason": "r" }),
        )
        .await
        .unwrap_err();
        assert!(err.contains("background hook"), "{err}");
        let err = dispatch(
            &api(),
            &WorkspaceId::from_string("w"),
            None,
            None,
            "list",
            &json!({}),
        )
        .await
        .unwrap_err();
        assert!(err.contains("only available to an agent"), "{err}");
    }
}
