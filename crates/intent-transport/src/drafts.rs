//! `drafts.*` fast-path: BE-persisted, per-client chat drafts (§5.16, §15).
//!
//! Drafts are keyed by `(workspaceId, agentId, clientId)` where `clientId` is
//! the connection's logical client (resolved from `client.hello`, §5.17) — it
//! is NEVER a wire parameter. Like `client.hello`, these methods are a transport
//! concern (they consume the per-connection `client_id` binding) and are
//! intercepted before the JSON-RPC dispatcher. A connection that never said
//! hello is an anonymous, connection-scoped client: a `clientId` is minted (and
//! its `client` row created) lazily on first write, so its drafts round-trip
//! within the connection but do not survive reconnect.

use intent_core::{AgentId, ClientId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};

/// Cap on the serialized `attachments` payload of a `drafts.set` (rejected
/// with `-32602` above this) to keep `SQLite` rows bounded (PROTOCOL §5.16).
pub(crate) const MAX_ATTACHMENTS_BYTES: usize = 25 * 1024 * 1024;

/// The three `drafts.*` methods, once classified.
pub(crate) enum DraftMethod {
    Get,
    Set {
        text: Option<String>,
        attachments: Option<Value>,
    },
    Clear,
}

/// A classified `drafts.*` request awaiting handling by the connection task.
pub(crate) struct DraftRequest {
    pub method: DraftMethod,
    pub workspace_id: Option<String>,
    pub agent_id: Option<String>,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `drafts.*` request, or `None` to fall through.
/// Mirrors the `forward`/`host` fast-path pre-check.
pub(crate) fn classify(value: &Value) -> Option<DraftRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method_name = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let params = obj.get("params").and_then(Value::as_object);
    let opt_str = |name: &str| {
        params
            .and_then(|p| p.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let method = match method_name {
        "drafts.get" => DraftMethod::Get,
        "drafts.set" => DraftMethod::Set {
            text: opt_str("text"),
            attachments: params
                .and_then(|p| p.get("attachments"))
                .filter(|v| !v.is_null())
                .cloned(),
        },
        "drafts.clear" => DraftMethod::Clear,
        _ => return None,
    };
    Some(DraftRequest {
        method,
        workspace_id: opt_str("workspaceId"),
        agent_id: opt_str("agentId"),
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Resolve the connection's effective `clientId` for a draft mutation, minting a
/// connection-scoped one (and persisting its `client` row to satisfy the draft
/// FK) when the connection never completed `client.hello`.
async fn resolve_for_write(
    api: &dyn WorkspaceApi,
    client_id: &mut Option<ClientId>,
) -> Result<ClientId, (i32, String)> {
    if let Some(id) = client_id.as_ref() {
        return Ok(id.clone());
    }
    let minted = ClientId::new();
    api.upsert_client(minted.clone(), None, None)
        .await
        .map_err(|e| (-32603, e.to_string()))?;
    *client_id = Some(minted.clone());
    Ok(minted)
}

/// Validate the optional `attachments` param of a `drafts.set`: it must be a
/// JSON array, an empty array is normalized to `None` (nothing stored), and a
/// serialized payload above [`MAX_ATTACHMENTS_BYTES`] is rejected (`-32602`).
fn validate_attachments(attachments: Option<Value>) -> Result<Option<Value>, (i32, String)> {
    let Some(value) = attachments else {
        return Ok(None);
    };
    let Some(items) = value.as_array() else {
        return Err((
            -32602,
            "Invalid parameter: attachments must be an array".to_string(),
        ));
    };
    if items.is_empty() {
        return Ok(None);
    }
    struct CountingWriter(usize);
    impl std::io::Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = CountingWriter(0);
    serde_json::to_writer(&mut counter, &value).map_err(|e| (-32603, e.to_string()))?;
    let size = counter.0;
    if size > MAX_ATTACHMENTS_BYTES {
        return Err((
            -32602,
            format!("Invalid parameter: attachments exceeds {MAX_ATTACHMENTS_BYTES} bytes"),
        ));
    }
    Ok(Some(value))
}

/// Handle a classified `drafts.*` request against the connection's `client_id`
/// binding. Missing `workspaceId`/`agentId` (or `text` on `set`) and invalid
/// or oversized `attachments` are `-32602`; persistence failures are `-32603`
/// (PROTOCOL §5.16, §9).
pub(crate) async fn handle(
    req: DraftRequest,
    api: &dyn WorkspaceApi,
    client_id: &mut Option<ClientId>,
) -> Option<String> {
    let (Some(ws), Some(agent)) = (
        req.workspace_id.filter(|s| !s.is_empty()),
        req.agent_id.filter(|s| !s.is_empty()),
    ) else {
        return frame(
            req.id_present,
            req.id_echo,
            Err((
                -32602,
                "Missing required parameter: workspaceId/agentId".to_string(),
            )),
        );
    };
    let ws = WorkspaceId::from(ws);
    let agent = AgentId::from(agent);
    let result: Result<Value, (i32, String)> = match req.method {
        DraftMethod::Get => match client_id.clone() {
            // An anonymous connection cannot have a stored draft yet → null,
            // without minting a client row for a pure read.
            None => Ok(Value::Null),
            Some(cid) => match api.draft_get(ws, agent, cid).await {
                Ok(Some(draft)) => {
                    let mut result = json!({ "text": draft.text, "updatedAt": draft.updated_at });
                    if let Some(attachments) = draft.attachments {
                        result["attachments"] = attachments;
                    }
                    Ok(result)
                }
                Ok(None) => Ok(Value::Null),
                Err(e) => Err((-32603, e.to_string())),
            },
        },
        DraftMethod::Set { text, attachments } => match text {
            None => Err((-32602, "Missing required parameter: text".to_string())),
            Some(text) => match validate_attachments(attachments) {
                Err(e) => Err(e),
                Ok(attachments) => match resolve_for_write(api, client_id).await {
                    Err(e) => Err(e),
                    Ok(cid) => match api.draft_set(ws, agent, cid, text, attachments).await {
                        Ok(Some(updated_at)) => Ok(json!({ "ok": true, "updatedAt": updated_at })),
                        Ok(None) => Ok(json!({ "ok": true })),
                        Err(e) => Err((-32603, e.to_string())),
                    },
                },
            },
        },
        DraftMethod::Clear => match resolve_for_write(api, client_id).await {
            Err(e) => Err(e),
            Ok(cid) => match api.draft_clear(ws, agent, cid).await {
                Ok(()) => Ok(json!({ "ok": true })),
                Err(e) => Err((-32603, e.to_string())),
            },
        },
    };
    frame(req.id_present, req.id_echo, result)
}

/// Build the response frame for a `drafts.*` result, or `None` for a
/// notification (no `id`).
fn frame(id_present: bool, id_echo: Value, result: Result<Value, (i32, String)>) -> Option<String> {
    if !id_present {
        return None;
    }
    Some(match result {
        Ok(value) => success_frame(id_echo, value),
        Err((code, message)) => error_frame(id_echo, code, &message),
    })
}

#[cfg(test)]
mod tests;
