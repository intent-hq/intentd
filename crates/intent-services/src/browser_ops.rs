//! Envelope validation + result-shaping for `browser.exec` (PROTOCOL §5.14,
//! §12.4).
//!
//! `browser.exec` is a **client-callable trigger** whose real work happens on
//! the connected frontend: the daemon validates the payload, forwards it as an
//! FE-served reverse RPC — method name unchanged (`browser.exec`), with a
//! `rev-<n>` request id (mirroring `host.openInEditor` /
//! `host.pickApplication`) — and echoes the FE's result back to the caller.
//! No CDP logic runs in Rust — the daemon is a thin proxy.
//!
//! This module owns the two pure pieces of that flow: (1) parsing/validating
//! the params into [`BrowserExecArgs`] and (2) reshaping the FE's raw
//! `{ success, results, error? }` envelope into the wire result the caller
//! expects (single action → one result, multiple → `results[]` — reference
//! parity with the FE MCP tool). The transport layer handles the reverse-RPC
//! wire hop and is the only piece that needs the per-connection reverse channel.

use serde_json::{json, Map, Value};

/// Wire error codes surfaced by `browser.exec` (PROTOCOL §9: `-32602` for
/// invalid params, `-32603` for internal / proxy failures).
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

/// A `browser.exec` failure with the error code the transport should surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserExecError {
    pub code: i32,
    pub message: String,
}

impl BrowserExecError {
    fn invalid(msg: impl Into<String>) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: msg.into(),
        }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: INTERNAL_ERROR,
            message: msg.into(),
        }
    }
}

/// Parsed `browser.exec` params. `actions` is the validated (non-empty) action
/// batch; the trailing optional fields carry the reverse-intent envelope so
/// the FE handler can attribute the batch to a caller / workspace without a
/// second round-trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserExecArgs {
    /// Ordered action sequence, opaque to the daemon (validated shape only).
    pub actions: Vec<Value>,
    /// Optional default tab id the FE applies to actions that omit their own.
    pub tab_id: Option<String>,
    /// Attribution: agent id of the caller (from the ws.browser.exec binding).
    pub agent_id: Option<String>,
    /// Attribution: workspace id of the caller (from the ws.browser.exec binding).
    pub workspace_id: Option<String>,
}

/// Parse a JSON-RPC params object into [`BrowserExecArgs`]. Enforces the two
/// invariants the daemon can check without touching the FE:
///   * `actions` is present, an array, and non-empty (`-32602` otherwise);
///   * `tabId` / `agentId` / `workspaceId`, when supplied, are strings.
///
/// # Errors
///
/// Returns an invalid-params `BrowserExecError` when required parameters are missing or malformed.
pub fn parse_args(params: &Map<String, Value>) -> Result<BrowserExecArgs, BrowserExecError> {
    let actions = match params.get("actions") {
        Some(Value::Array(a)) => a.clone(),
        Some(Value::Null) | None => {
            return Err(BrowserExecError::invalid(
                "Missing required parameter: actions",
            ))
        }
        Some(_) => {
            return Err(BrowserExecError::invalid(
                "Invalid parameter: actions must be an array",
            ))
        }
    };
    if actions.is_empty() {
        return Err(BrowserExecError::invalid(
            "Invalid parameter: actions must not be empty",
        ));
    }
    let tab_id = optional_string(params, "tabId")?;
    let agent_id = optional_string(params, "agentId")?;
    let workspace_id = optional_string(params, "workspaceId")?;
    Ok(BrowserExecArgs {
        actions,
        tab_id,
        agent_id,
        workspace_id,
    })
}

/// Build the params object the daemon dispatches on the FE-served reverse
/// intent (`browser.exec`). Optional envelope fields are omitted when absent
/// so the wire payload stays minimal.
pub fn build_forward_params(args: &BrowserExecArgs) -> Value {
    let mut out = Map::new();
    out.insert("actions".to_string(), Value::Array(args.actions.clone()));
    if let Some(tab_id) = &args.tab_id {
        out.insert("tabId".to_string(), Value::String(tab_id.clone()));
    }
    if let Some(agent_id) = &args.agent_id {
        out.insert("agentId".to_string(), Value::String(agent_id.clone()));
    }
    if let Some(workspace_id) = &args.workspace_id {
        out.insert(
            "workspaceId".to_string(),
            Value::String(workspace_id.clone()),
        );
    }
    Value::Object(out)
}

/// Reshape the FE's raw `{ success, results, error? }` envelope into the wire
/// result the caller expects. Reference parity (`BrowserExecTool.execute`):
/// a single-action batch yields the lone action's envelope unchanged (see
/// [`single_result_payload`] — the caller unwraps `.result` / `.error`
/// itself); a multi-action batch yields `{ "results": [...] }`. A
/// `success: false` envelope with a top-level `error` string surfaces as
/// `-32603` so the caller sees the FE's context. Missing / malformed `results`
/// also surfaces as `-32603` — the daemon cannot invent a shape it did not
/// receive.
///
/// # Errors
///
/// Returns an internal `BrowserExecError` when the frontend response is not an object, reports failure, or carries no results.
pub fn shape_result(fe_response: Value) -> Result<Value, BrowserExecError> {
    let obj = fe_response.as_object().ok_or_else(|| {
        BrowserExecError::internal("browser.exec: frontend returned a non-object response")
    })?;
    // Explicit failure envelope: surface the FE's `error` string verbatim.
    if obj.get("success").and_then(Value::as_bool) == Some(false) {
        let message = obj
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("browser.exec: frontend reported failure")
            .to_string();
        return Err(BrowserExecError::internal(format!(
            "browser.exec failed: {message}"
        )));
    }
    let results = obj
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            BrowserExecError::internal("browser.exec: frontend response missing results array")
        })?;
    match results.len() {
        // A validated non-empty batch that came back with zero results is a
        // protocol violation on the FE side — surface it rather than fabricate.
        0 => Err(BrowserExecError::internal(
            "browser.exec: frontend returned an empty results array",
        )),
        1 => Ok(single_result_payload(&results[0])),
        _ => Ok(json!({ "results": results })),
    }
}

/// Reshape the FE envelope for the **agent-JS surface** (`ws.browser.exec`,
/// WSAPI-6). Same shaping as [`shape_result`] with one deliberate difference:
/// a `success: false` envelope that still carries a non-empty `results` array
/// is returned as data, not collapsed into a top-level error
/// (intent-hq/monorepo#3042). The FE emits structured per-action failures —
/// `{ action, success: false, errorCode: "not-owner" | "already-claimed",
/// ownerAgentId, error }` — and the browser docs promise agents these surface
/// inside the per-action envelope, "never as a top-level failure"; flattening
/// them into thrown prose loses `errorCode` / `ownerAgentId`, which agents
/// need to react (claim the tab, name the owner).
///
/// `requested_actions` is the size of the *request* batch, not the reply: the
/// FE aborts a batch on the first failing action and returns the partial
/// `results` collected so far, so a 3-action batch failing at action 1 comes
/// back with one result. Shaping by reply arity would hand a multi-action
/// caller an unpredictable shape (bare envelope vs `results[]` depending on
/// where the batch failed); keying on the request keeps the contract stable —
/// single-action requests yield the lone action envelope (structured failure
/// intact), multi-action requests yield the FE envelope
/// (`success: false` / `results` / `error`, with `results` possibly shorter
/// than the request on abort). A failure envelope *without* per-action
/// results (transport/CDP-level breakage) still surfaces as `-32603` — there
/// is no structure to preserve.
///
/// The wire `browser.exec` client path keeps [`shape_result`] unchanged
/// (PROTOCOL §5.14 maps envelope failure to `-32603`).
///
/// # Errors
///
/// Returns an internal `BrowserExecError` when the frontend response is not an object, reports failure without results, or carries no results.
pub fn shape_agent_result(
    fe_response: Value,
    requested_actions: usize,
) -> Result<Value, BrowserExecError> {
    if let Some(obj) = fe_response.as_object() {
        if obj.get("success").and_then(Value::as_bool) == Some(false) {
            if let Some(results) = obj.get("results").and_then(Value::as_array) {
                if !results.is_empty() {
                    return if requested_actions == 1 {
                        Ok(single_result_payload(&results[0]))
                    } else {
                        Ok(json!({
                            "success": false,
                            "results": results,
                            "error": obj.get("error").cloned().unwrap_or(Value::Null),
                        }))
                    };
                }
            }
        }
    }
    shape_result(fe_response)
}

/// Build the wire payload for the single-action case: pass the FE's action
/// envelope through unchanged. The daemon neither unwraps `result` nor
/// re-maps action-level `error`s — it forwards exactly what the FE gave us.
/// We keep the shape self-describing so the FE binding (WSAPI-6) can render
/// either the success or the failure side.
fn single_result_payload(action: &Value) -> Value {
    // Preserve `action` (the tool name), `success`, and `result` / `error` as
    // the FE emitted them. Consumers that expect just the payload can read
    // `.result`; consumers that need the failure reason read `.error`.
    action.clone()
}

/// Extract an optional string param; rejects non-string, non-null values so a
/// typo (`tabId: 42`) surfaces as `-32602` instead of being silently dropped.
fn optional_string(
    params: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, BrowserExecError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(_) => Err(BrowserExecError::invalid(format!(
            "Invalid parameter: {key} must be a string"
        ))),
    }
}

#[cfg(test)]
mod tests;
