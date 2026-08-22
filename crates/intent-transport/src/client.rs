//! `client.hello` handshake fast-path: stable client identity (§5.17, §16).
//!
//! `client.hello` is a transport concern: it establishes the connection's
//! logical `clientId` (the disambiguation key for `drafts.*`, §5.16) and
//! advertises the daemon host's nature in a `server` block, exactly like the
//! `host.status` probe (§5.14). So, like the `system.*`/`host.status`/`forward.*`
//! fast-paths, every listener intercepts it before the JSON-RPC dispatcher and
//! threads the resolved id through the per-connection `client_id` binding.

use intent_core::{ClientId, WorkspaceApi};
use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};
use crate::host_env::detect_has_display;
use crate::protocol::PROTOCOL_VERSION;

/// A classified `client.hello` request awaiting handling by the connection task.
pub(crate) struct ClientRequest {
    /// The re-presented `clientId`, when a valid string was supplied.
    pub client_id: Option<String>,
    /// A `clientId` member was present but not a string ⇒ `-32602`.
    pub client_id_invalid: bool,
    pub name: Option<String>,
    pub capabilities: Option<Value>,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `client.hello` request, or `None` to fall
/// through. Mirrors the `host`/`forward` fast-path pre-check: a JSON-RPC 2.0
/// object with a string `method` and an `id` (if present) that is a string,
/// number, or null.
pub(crate) fn classify(value: &Value) -> Option<ClientRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    if obj.get("method").and_then(Value::as_str)? != "client.hello" {
        return None;
    }
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let params = obj.get("params").and_then(Value::as_object);
    let client_id_member = params.and_then(|p| p.get("clientId"));
    let (client_id, client_id_invalid) = match client_id_member {
        None | Some(Value::Null) => (None, false),
        Some(Value::String(s)) => (Some(s.clone()), false),
        Some(_) => (None, true),
    };
    let name = params
        .and_then(|p| p.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let capabilities = params.and_then(|p| p.get("capabilities")).cloned();
    Some(ClientRequest {
        client_id,
        client_id_invalid,
        name,
        capabilities,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Build the `server` capability block (§5.17): `{ locality, hasDisplay, osArch,
/// version, protocolVersion, capabilities }`. `osArch` is the daemon host's
/// `os/arch` (e.g. `darwin/arm64`). `protocolVersion` is the frozen JSON-RPC
/// surface version (v2.0). `capabilities.liveState` advertises the live-state
/// push surface (D+E) so the FE can feature-detect it without version-sniffing.
/// Pure (inputs injected) so it is unit-testable.
pub(crate) fn server_json(
    has_display: bool,
    os: &str,
    arch: &str,
    version: &str,
    is_local: bool,
) -> Value {
    json!({
        "locality": if is_local { "local" } else { "remote" },
        "hasDisplay": has_display,
        "osArch": format!("{os}/{arch}"),
        "version": version,
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "liveState": true },
    })
}

/// Handle a classified `client.hello`: resolve (or mint) the `clientId`, persist
/// the logical `client` row, set the connection's `client_id` binding, and reply
/// with `{ clientId, protocolVersion, server }`. The top-level `protocolVersion`
/// is an explicit copy of `server.protocolVersion` so clients can version-check
/// without digging into the `server` block. A non-string `clientId` is `-32602`;
/// a persistence failure is `-32603`. Idempotent: re-sending updates name /
/// capabilities and re-returns the same `server` block (PROTOCOL §5.17).
pub(crate) async fn handle(
    req: ClientRequest,
    api: &dyn WorkspaceApi,
    client_id: &mut Option<ClientId>,
    is_local: bool,
) -> Option<String> {
    if req.client_id_invalid {
        return frame(
            req.id_present,
            &req.id_echo,
            Err((-32602, "clientId must be a string".to_string())),
        );
    }
    let resolved = req.client_id.map(ClientId::from_string).unwrap_or_default();
    if let Err(e) = api
        .upsert_client(resolved.clone(), req.name, req.capabilities)
        .await
    {
        return frame(req.id_present, &req.id_echo, Err((-32603, e.to_string())));
    }
    *client_id = Some(resolved.clone());
    let server = server_json(
        detect_has_display(),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("CARGO_PKG_VERSION"),
        is_local,
    );
    frame(
        req.id_present,
        &req.id_echo,
        Ok(json!({
            "clientId": resolved.as_str(),
            "protocolVersion": PROTOCOL_VERSION,
            "server": server,
        })),
    )
}

/// Build the response frame for a `client.hello` result, or `None` for a
/// notification (no `id`).
fn frame(
    id_present: bool,
    id_echo: &Value,
    result: Result<Value, (i32, String)>,
) -> Option<String> {
    if !id_present {
        return None;
    }
    Some(match result {
        Ok(value) => success_frame(id_echo, &value),
        Err((code, message)) => error_frame(id_echo, code, &message),
    })
}

#[cfg(test)]
mod tests;
