//! `browser.*` client-callable trigger fast-path: `browser.exec` (§5.14, §12.4).
//!
//! `browser.exec` is a **client-callable trigger** whose real work happens on
//! the connected frontend (Chrome `DevTools` Protocol against embedded browser
//! tabs — no CDP logic runs in Rust). The daemon validates the envelope, then
//! dispatches an FE-served reverse RPC — method name unchanged (`browser.exec`),
//! with a `rev-<n>` request id (mirroring `host.openInEditor` /
//! `host.pickApplication`) — and echoes the FE's result back to the caller.
//!
//! Wire shape (reference parity with the FE MCP tool `browser_exec`): a
//! validated non-empty `actions` batch forwards `{ actions, tabId?, agentId?,
//! workspaceId? }` to the FE; the reply is reduced to a single action's
//! result envelope for a one-action batch and to `{ results: [...] }` for a
//! multi-action batch. `-32602` on missing / empty / non-array `actions`;
//! `-32603` when the FE reverse RPC fails, times out, no client is connected,
//! or the FE surfaces its own error.

use std::fmt;

use intent_services::browser_ops;
use serde_json::{Map, Value};

use crate::events::{error_frame, success_frame};
use crate::reverse::{ReverseChannel, DEFAULT_REVERSE_TIMEOUT};

/// The `browser.*` methods, once classified. Kept as an enum for parity with
/// `host::HostMethod` and to leave room for future additions without changing
/// the classify/handle signature.
pub(crate) enum BrowserMethod {
    /// `browser.exec` client-callable trigger → FE-served reverse RPC.
    Exec,
}

/// A classified `browser.*` request awaiting handling by the connection task.
pub(crate) struct BrowserRequest {
    pub method: BrowserMethod,
    pub id_present: bool,
    pub id_echo: Value,
    pub params: Map<String, Value>,
}

/// Classify a parsed frame as a `browser.*` request, or `None` to fall through
/// to the next fast-path / JSON-RPC dispatcher. Mirrors `host::classify`: a
/// JSON-RPC 2.0 object with a string `method` and an `id` (if present) that is
/// a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<BrowserRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let method = match method {
        "browser.exec" => BrowserMethod::Exec,
        _ => return None,
    };
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Some(BrowserRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
        params,
    })
}

/// Handle a classified `browser.*` request: validate the envelope, forward
/// via the reverse channel, and shape the FE's reply. Returns `None` for a
/// notification (no `id`), which gets no response.
pub(crate) async fn handle(req: BrowserRequest, reverse: &ReverseChannel) -> Option<String> {
    let BrowserRequest {
        method,
        id_present,
        id_echo,
        params,
    } = req;
    let frame = match method {
        BrowserMethod::Exec => match exec(&params, reverse).await {
            Ok(v) => success_frame(&id_echo, &v),
            Err(e) => error_frame(&id_echo, e.code(), &e.to_string()),
        },
    };
    if !id_present {
        return None;
    }
    Some(frame)
}

/// Why a [`exec`] call could not be satisfied. `code()` maps each to a
/// standard JSON-RPC error code (PROTOCOL §9: `-32602` for invalid params,
/// `-32603` for internal / proxy failures).
#[derive(Debug)]
pub enum BrowserExecError {
    /// Envelope validation rejected the payload (missing / empty / non-array
    /// `actions`, or a non-string envelope field).
    InvalidParams(String),
    /// The FE-served reverse RPC failed / timed out / no client is connected,
    /// or the FE surfaced a failure envelope.
    Proxy(String),
}

impl BrowserExecError {
    /// JSON-RPC 2.0 numeric error code for this condition.
    pub fn code(&self) -> i32 {
        match self {
            BrowserExecError::InvalidParams(_) => browser_ops::INVALID_PARAMS,
            BrowserExecError::Proxy(_) => browser_ops::INTERNAL_ERROR,
        }
    }
}

impl fmt::Display for BrowserExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrowserExecError::InvalidParams(m) | BrowserExecError::Proxy(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for BrowserExecError {}

/// Execute one `browser.exec` request: parse + validate the envelope, forward
/// it to the FE reverse intent, then reshape the FE's reply into the wire
/// result (single action → one result envelope, multiple → `{ results: [...]
/// }`). A closed outbound channel ("no frontend connected") and a reverse-RPC
/// timeout both surface as `-32603` with the underlying context so the caller
/// can distinguish them from a validation failure.
pub(crate) async fn exec(
    params: &Map<String, Value>,
    reverse: &ReverseChannel,
) -> Result<Value, BrowserExecError> {
    let args = browser_ops::parse_args(params).map_err(|e| {
        // Envelope validation errors are always `-32602`; the service module
        // pre-tagged them, so no need to re-classify here.
        BrowserExecError::InvalidParams(e.message)
    })?;
    let forwarded = browser_ops::build_forward_params(&args);
    let fe_response = reverse
        .request("browser.exec", forwarded, DEFAULT_REVERSE_TIMEOUT)
        .await
        .map_err(|e| BrowserExecError::Proxy(format!("browser.exec: {}", e.message)))?;
    browser_ops::shape_result(&fe_response).map_err(|e| BrowserExecError::Proxy(e.message))
}

#[cfg(test)]
mod tests;
