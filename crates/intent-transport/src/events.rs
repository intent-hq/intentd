//! Server-initiated event fast-path (PROTOCOL §6).
//!
//! Pure, transport-agnostic helpers that mirror the `events.subscribe` /
//! `events.unsubscribe` fast-path in `~/src/intent/src/main/`:
//! `websocket-api-server.ts` (the JSON-RPC-shape pre-check that routes the two
//! `events.` methods before the dispatcher) and `websocket-event-bridge.ts`
//! (`handleSubscribe` / `handleUnsubscribe` param validation, the global
//! `ws-sub-<n>` id counter, and the `events.event` notification envelope). The
//! connection orchestration that consumes these lives in [`crate::listener`].

use std::sync::atomic::{AtomicU64, Ordering};

use intent_core::Event;
use serde_json::{json, Map, Value};

/// The `id` member of a fast-path request: whether it was present (a response is
/// only sent for requests, not notifications) and the value to echo (`id ?? null`).
pub(crate) struct IdInfo {
    pub present: bool,
    pub echo: Value,
}

/// A classified fast-path request awaiting handling by the connection task.
pub(crate) enum FastPath {
    Subscribe {
        id: IdInfo,
        params: Map<String, Value>,
    },
    Unsubscribe {
        id: IdInfo,
        params: Map<String, Value>,
    },
}

/// Parsed `events.subscribe` params (`handleSubscribe`).
#[derive(Debug)]
pub(crate) struct SubscribeParams {
    pub event_types: Vec<String>,
    pub workspace_id: Option<String>,
    pub replace_group: Option<String>,
}

/// Global, monotonic subscription id counter (`ws-sub-<n>`). Mirrors the TS
/// module-level `subCounter`, which is shared across all connections.
static SUB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Mint the next `ws-sub-<n>` subscription id.
pub(crate) fn next_subscription_id() -> String {
    let n = SUB_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    format!("ws-sub-{n}")
}

/// Classify a parsed frame as a fast-path `events.` request, or `None` to fall
/// through to the JSON-RPC dispatcher. Mirrors the `websocket-api-server.ts`
/// pre-check: the frame must be a JSON-RPC 2.0 object with a string `method`,
/// and any present `id` must be a string, number, or null (otherwise it falls
/// through so the dispatcher returns the `-32600` invalid-request error).
pub(crate) fn classify(value: &Value) -> Option<FastPath> {
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
    let id = IdInfo {
        present: id_member.is_some(),
        echo: id_member.cloned().unwrap_or(Value::Null),
    };
    // `parsed.params || {}`: a non-object (absent/null/array/scalar) yields `{}`,
    // which then fails the same required-param checks the TS handlers apply.
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    match method {
        "events.subscribe" => Some(FastPath::Subscribe { id, params }),
        "events.unsubscribe" => Some(FastPath::Unsubscribe { id, params }),
        _ => None,
    }
}

/// Validate `events.subscribe` params. `eventTypes` must be a non-empty array
/// (TS throws otherwise → `-32602`); `workspaceId` / `replaceGroup` are optional.
pub(crate) fn parse_subscribe_params(
    params: &Map<String, Value>,
) -> Result<SubscribeParams, String> {
    let event_types = match params.get("eventTypes") {
        Some(Value::Array(arr)) if !arr.is_empty() => arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => return Err("eventTypes must be a non-empty array of event type strings".to_string()),
    };
    Ok(SubscribeParams {
        event_types,
        workspace_id: opt_str(params, "workspaceId"),
        replace_group: opt_str(params, "replaceGroup"),
    })
}

/// Validate `events.unsubscribe` params. A missing/empty `subscriptionId` throws
/// (`-32602`); a present-but-unknown id is handled by the caller as `success:false`.
pub(crate) fn parse_unsubscribe_id(params: &Map<String, Value>) -> Result<String, String> {
    match params.get("subscriptionId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(s.to_string()),
        _ => Err("subscriptionId is required".to_string()),
    }
}

fn opt_str(params: &Map<String, Value>, name: &str) -> Option<String> {
    params.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Build the `events.event` notification (PROTOCOL §6.3). The `event` object
/// carries exactly `type`, `workspaceId`, `id`, `timestamp`, `actor`, `data` —
/// matching `websocket-event-bridge.ts` (session/correlation ids are omitted).
pub(crate) fn build_event_notification(subscription_id: &str, event: &Event) -> String {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "events.event",
        "params": {
            "subscriptionId": subscription_id,
            "event": {
                "type": &event.event_type,
                "workspaceId": &event.workspace_id,
                "id": &event.id,
                "timestamp": &event.timestamp,
                "actor": &event.actor,
                "data": &event.data,
            }
        }
    });
    serde_json::to_string(&frame).unwrap_or_default()
}

/// Serialize a JSON-RPC success response for a fast-path request.
pub(crate) fn success_frame(id: &Value, result: &Value) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "result": result }))
        .unwrap_or_default()
}

/// Serialize a JSON-RPC error response for a fast-path request. A `-32602`
/// carries the machine-readable discriminator `error.data.code =
/// "invalid-params"` (PROTOCOL §3.3, monorepo#1364), mirroring the dispatcher's
/// `invalid_params` helper: every fast-path `-32602` is a parameter-validation
/// failure (the only entity-absent fast-path case — an unknown
/// `host.execStream` `requestId` — is `-32603`), so the discriminator is
/// attached centrally here rather than at each call site. A future fast-path
/// site addressing a missing entity must emit `"not-found"` via a dedicated
/// variant instead.
pub(crate) fn error_frame(id: &Value, code: i32, message: &str) -> String {
    let error = if code == -32602 {
        json!({ "code": code, "message": message, "data": { "code": "invalid-params" } })
    } else {
        json!({ "code": code, "message": message })
    };
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "error": error }))
        .unwrap_or_default()
}

/// Serialize a JSON-RPC error response carrying an explicit `error.data`
/// payload — for fast-path sites that attach a machine-readable
/// discriminator beyond the centralized `-32602` tagging in [`error_frame`]
/// (e.g. the pairing listener-down `{ "code": "listener-down" }`,
/// monorepo#1822).
pub(crate) fn error_frame_with_data(id: &Value, code: i32, message: &str, data: &Value) -> String {
    // §3.3 invariant: every -32602 carries an `error.data.code` discriminator
    // — a caller bypassing the centralized tagging must supply one itself.
    debug_assert!(
        code != -32602 || data.get("code").is_some(),
        "-32602 error.data must carry a `code` discriminator (PROTOCOL §3.3)"
    );
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message, "data": data }
    }))
    .unwrap_or_default()
}

#[cfg(test)]
mod tests;
