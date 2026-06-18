//! Transport-agnostic JSON-RPC 2.0 router (PROTOCOL §3, §9).
//!
//! [`handle_message`] takes a single request string and returns the response
//! string, or `None` for notifications (a request without an `id` member).
//! Envelope validation, the notification-vs-request distinction, and the
//! `-32700/-32600/-32601/-32602/-32603` error matrix all live here so every
//! transport (UDS today, WS/TLS later) shares one code path.

use intent_core::{Error, WorkspaceApi, WorkspaceCreate, WorkspaceId, WorkspaceUpdate};
use serde_json::{json, Map, Value};

const PARSE_ERROR: i32 = -32700;
const INVALID_REQUEST: i32 = -32600;
const METHOD_NOT_FOUND: i32 = -32601;
const INVALID_PARAMS: i32 = -32602;

/// A JSON-RPC error to surface to the client.
struct RpcErr {
    code: i32,
    message: String,
    data: Option<Value>,
}

fn rpc(code: i32, message: impl Into<String>) -> RpcErr {
    RpcErr {
        code,
        message: message.into(),
        data: None,
    }
}

/// Map a domain [`Error`] to its JSON-RPC representation (§9). Internal errors
/// surface as `-32603 "Internal error"` carrying the original cause in `data`.
fn domain_to_rpc(e: Error) -> RpcErr {
    match e {
        Error::Internal(msg) => RpcErr {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(Value::String(msg)),
        },
        other => RpcErr {
            code: other.code(),
            message: other.to_string(),
            data: None,
        },
    }
}

/// Handle one JSON-RPC frame. Returns `Some(response)` for requests and `None`
/// for notifications (including unknown / failed ones, per §3.4).
pub async fn handle_message(api: &dyn WorkspaceApi, message: &str) -> Option<String> {
    let value: Value = match serde_json::from_str(message) {
        Ok(v) => v,
        // Parse errors are always answered with id null (§9), even for
        // would-be notifications — notification status is not yet known.
        Err(_) => return Some(error_string(Value::Null, PARSE_ERROR, "Parse error", None)),
    };

    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            return Some(error_string(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request: expected an object",
                None,
            ))
        }
    };

    let id_member = obj.get("id");
    let has_id = id_member.is_some();
    let id_type_ok = match id_member {
        None => true,
        Some(v) => v.is_string() || v.is_number() || v.is_null(),
    };
    let echo_id = match id_member {
        Some(v) if id_type_ok => v.clone(),
        _ => Value::Null,
    };

    // Envelope validation (-32600). Answered even for notification-shaped
    // frames: notification status is not trusted until the envelope is valid.
    let jsonrpc_ok = obj.get("jsonrpc").and_then(Value::as_str) == Some("2.0");
    let method = obj.get("method").and_then(Value::as_str);
    let method_ok = method.map(|m| !m.is_empty()).unwrap_or(false);
    if !jsonrpc_ok || !method_ok || !id_type_ok {
        let msg = if !jsonrpc_ok {
            "Invalid Request: jsonrpc must be \"2.0\""
        } else if !method_ok {
            "Invalid Request: method must be a non-empty string"
        } else {
            "Invalid Request: id must be a string, number, or null"
        };
        return Some(error_string(echo_id, INVALID_REQUEST, msg, None));
    }
    let method = method.unwrap();
    let is_notification = !has_id;

    // params: object kept as-is; positional array coerced to {}; absent/null
    // treated as empty; any other scalar is invalid (§3.1).
    let params: Map<String, Value> = match obj.get("params") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(Value::Array(_)) => Map::new(),
        Some(_) => {
            if is_notification {
                return None;
            }
            return Some(error_string(
                echo_id,
                INVALID_PARAMS,
                "Invalid params",
                None,
            ));
        }
    };

    let result = dispatch(api, method, &params).await;

    // Notifications never get a response, even on error / unknown method (§3.4).
    if is_notification {
        return None;
    }
    Some(match result {
        Ok(v) => success_string(echo_id, v),
        Err(e) => error_string(echo_id, e.code, &e.message, e.data),
    })
}

/// Dispatch a validated request to the injected [`WorkspaceApi`].
async fn dispatch(
    api: &dyn WorkspaceApi,
    method: &str,
    params: &Map<String, Value>,
) -> Result<Value, RpcErr> {
    match method {
        "workspace.list" => {
            let include_archived = params
                .get("includeArchived")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let workspaces = api
                .list_workspaces(include_archived)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "workspaces": workspaces }))
        }
        "workspace.get" => {
            let id = require_workspace_id(params)?;
            let ws = api.get_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.create" => {
            let input: WorkspaceCreate = serde_json::from_value(Value::Object(params.clone()))
                .map_err(|e| rpc(INVALID_PARAMS, format!("invalid params: {e}")))?;
            let ws = api.create_workspace(input).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.update" => {
            let id = require_workspace_id(params)?;
            let mut rest = params.clone();
            rest.remove("workspaceId");
            let update: WorkspaceUpdate = serde_json::from_value(Value::Object(rest))
                .map_err(|e| rpc(INVALID_PARAMS, format!("invalid params: {e}")))?;
            let ws = api
                .update_workspace(id, update)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.delete" => {
            let id = require_workspace_id(params)?;
            api.delete_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.archive" => {
            let id = require_workspace_id(params)?;
            api.archive_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.unarchive" => {
            let id = require_workspace_id(params)?;
            api.unarchive_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.dismissAttention" => {
            let id = require_workspace_id(params)?;
            let ws = api.dismiss_attention(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.markSeen" => {
            let id = require_workspace_id(params)?;
            let ws = api.mark_seen(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "note.list" => {
            let ws_id = match params.get("workspaceId").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => WorkspaceId::from(s),
                _ => return Err(rpc(INVALID_PARAMS, "workspaceId is required")),
            };
            let notes = api.list_notes(&ws_id).await.map_err(domain_to_rpc)?;
            Ok(json!({ "notes": notes }))
        }
        _ => Err(rpc(METHOD_NOT_FOUND, "Method not found")),
    }
}

/// Extract a required `workspaceId` string param, or `-32602` with the exact
/// message the TS handler emits via `requireParam` (PROTOCOL §5.1).
fn require_workspace_id(params: &Map<String, Value>) -> Result<WorkspaceId, RpcErr> {
    match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(WorkspaceId::from(s)),
        _ => Err(rpc(
            INVALID_PARAMS,
            "Missing required parameter: workspaceId",
        )),
    }
}

/// Map a domain [`Error`] for `workspace.*` methods: a missing workspace surfaces
/// as `-32602 "Workspace not found"`, matching the TS handler (PROTOCOL §5.1).
fn workspace_err(e: Error) -> RpcErr {
    match e {
        Error::NotFound(_) => rpc(INVALID_PARAMS, "Workspace not found"),
        other => domain_to_rpc(other),
    }
}

/// Serialize a success envelope. `result` is always a JSON object (§3.2).
fn success_string(id: Value, result: Value) -> String {
    let resp = json!({ "jsonrpc": "2.0", "result": result, "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Serialize an error envelope, optionally carrying `data`.
fn error_string(id: Value, code: i32, message: &str, data: Option<Value>) -> String {
    let mut err = Map::new();
    err.insert("code".to_string(), json!(code));
    err.insert("message".to_string(), json!(message));
    if let Some(d) = data {
        err.insert("data".to_string(), d);
    }
    let resp = json!({ "jsonrpc": "2.0", "error": Value::Object(err), "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Last-resort response if serialization itself fails (should never happen).
fn internal_fallback() -> String {
    r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null}"#.to_string()
}

#[cfg(test)]
mod tests;
