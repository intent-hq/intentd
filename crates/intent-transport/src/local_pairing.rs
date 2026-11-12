//! Local code pairing fast paths (§5.58, §2.7).
//!
//! `pairing.createLocalCode`, `pairing.cancelLocalCode`,
//! `pairing.listLocalCredentials` and `pairing.revokeLocalCredential` are
//! local-only owner administration, like `pairing.getInfo`: gated on the real
//! connection origin, so every TCP/WSS caller — including a connection
//! admitted with a local pairing credential — gets `-32001`.
//! `pairing.redeemLocalCode` is served only on the anonymous `/local-pair`
//! endpoint, which calls [`redeem_grant`]; on any other connection it is
//! `-32001`.
//!
//! The code slot and credential table live behind
//! [`intent_core::WorkspaceApi::local_pairing`]; this module renders frames.

use std::net::IpAddr;
use std::sync::Arc;

use intent_core::local_pairing::{LocalPairingError, LocalPairingService};
use intent_core::WorkspaceApi;
use serde_json::{json, Map, Value};

use crate::events::{error_frame, error_frame_with_data, success_frame};
use crate::server::ServerPairingInfo;

/// Version of the `LocalPairingGrant` shape.
pub const GRANT_VERSION: u32 = 1;

/// The redemption method, served only on `/local-pair`.
pub const REDEEM_METHOD: &str = "pairing.redeemLocalCode";

/// The `-32001` message for `pairing.redeemLocalCode` outside `/local-pair`.
pub const REDEEM_ELSEWHERE_MESSAGE: &str = "pairing.redeemLocalCode is served only on /local-pair";

/// Whether a TCP peer is on this computer: `127.0.0.0/8`, `::1`, or an
/// IPv4-mapped `::ffff:127.0.0.0/104`.
#[must_use]
pub fn is_loopback_peer(peer: IpAddr) -> bool {
    peer.to_canonical().is_loopback()
}

/// The local pairing methods, once classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalPairingMethod {
    CreateCode,
    CancelCode,
    ListCredentials,
    RevokeCredential,
    Redeem,
}

impl LocalPairingMethod {
    fn name(self) -> &'static str {
        match self {
            Self::CreateCode => "pairing.createLocalCode",
            Self::CancelCode => "pairing.cancelLocalCode",
            Self::ListCredentials => "pairing.listLocalCredentials",
            Self::RevokeCredential => "pairing.revokeLocalCredential",
            Self::Redeem => REDEEM_METHOD,
        }
    }
}

/// A classified local pairing request.
pub(crate) struct LocalPairingRequest {
    pub(crate) method: LocalPairingMethod,
    id_present: bool,
    id_echo: Value,
    params: Option<Value>,
}

/// Classify a parsed frame as a local pairing request, or `None` to fall
/// through. Mirrors `pairing::classify`.
pub(crate) fn classify(value: &Value) -> Option<LocalPairingRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method = match obj.get("method").and_then(Value::as_str)? {
        "pairing.createLocalCode" => LocalPairingMethod::CreateCode,
        "pairing.cancelLocalCode" => LocalPairingMethod::CancelCode,
        "pairing.listLocalCredentials" => LocalPairingMethod::ListCredentials,
        "pairing.revokeLocalCredential" => LocalPairingMethod::RevokeCredential,
        "pairing.redeemLocalCode" => LocalPairingMethod::Redeem,
        _ => return None,
    };
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    Some(LocalPairingRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
        params: obj.get("params").cloned(),
    })
}

/// Render a typed local pairing failure as a JSON-RPC error frame.
#[must_use]
pub fn error_response(id: &Value, error: &LocalPairingError) -> String {
    match error.data() {
        Some(data) => error_frame_with_data(id, error.code(), &error.message(), &data),
        None => error_frame(id, error.code(), &error.message()),
    }
}

/// Params as an object; absent / `null` / `[]` read as empty.
fn params_object(params: Option<&Value>) -> Result<Map<String, Value>, LocalPairingError> {
    match params {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(map)) => Ok(map.clone()),
        Some(Value::Array(items)) if items.is_empty() => Ok(Map::new()),
        Some(_) => Err(LocalPairingError::InvalidParams(
            "params must be an object".into(),
        )),
    }
}

fn optional_string(
    params: &Map<String, Value>,
    name: &str,
) -> Result<Option<String>, LocalPairingError> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
        Some(_) => Err(LocalPairingError::InvalidParams(format!(
            "{name} must be a non-empty string"
        ))),
    }
}

fn required_string(params: &Map<String, Value>, name: &str) -> Result<String, LocalPairingError> {
    optional_string(params, name)?
        .ok_or_else(|| LocalPairingError::InvalidParams(format!("{name} is required")))
}

fn service(api: &dyn WorkspaceApi) -> Result<Arc<dyn LocalPairingService>, LocalPairingError> {
    api.local_pairing()
        .ok_or_else(|| LocalPairingError::Internal("local pairing is not available".into()))
}

/// Handle a classified request on an authenticated or local connection.
/// `None` for a notification.
pub(crate) async fn handle(
    req: LocalPairingRequest,
    api: &dyn WorkspaceApi,
    provider: &dyn ServerPairingInfo,
) -> Option<String> {
    let method = req.method;
    let result = if method == LocalPairingMethod::Redeem {
        Err((-32001, REDEEM_ELSEWHERE_MESSAGE.to_owned()))
    } else if crate::context::is_tcp_connection() {
        Err((-32001, format!("{} is local-only", method.name())))
    } else {
        Ok(admin(method, req.params.as_ref(), api, provider).await)
    };
    if !req.id_present {
        return None;
    }
    Some(match result {
        Err((code, message)) => error_frame(&req.id_echo, code, &message),
        Ok(Ok(value)) => success_frame(&req.id_echo, &value),
        Ok(Err(error)) => error_response(&req.id_echo, &error),
    })
}

async fn admin(
    method: LocalPairingMethod,
    params: Option<&Value>,
    api: &dyn WorkspaceApi,
    provider: &dyn ServerPairingInfo,
) -> Result<Value, LocalPairingError> {
    let params = params_object(params)?;
    let pairing = service(api)?;
    match method {
        LocalPairingMethod::CreateCode => {
            if provider.pairing_snapshot().await.port.is_none() {
                return Err(LocalPairingError::ListenerDown);
            }
            let created = pairing.create_code().await?;
            Ok(json!({ "code": created.code, "localCode": created.local_code }))
        }
        LocalPairingMethod::CancelCode => {
            let code_id = optional_string(&params, "codeId")?;
            Ok(json!({ "cancelled": pairing.cancel_code(code_id).await }))
        }
        LocalPairingMethod::ListCredentials => {
            Ok(json!({ "credentials": pairing.list_credentials().await? }))
        }
        LocalPairingMethod::RevokeCredential => {
            let credential_id = required_string(&params, "credentialId")?;
            Ok(json!({ "revoked": pairing.revoke_credential(credential_id).await? }))
        }
        LocalPairingMethod::Redeem => Err(LocalPairingError::Internal(
            "redeem is not an administration method".into(),
        )),
    }
}

/// `pairing.redeemLocalCode` for the `/local-pair` endpoint: check the peer,
/// spend the code, and build the `LocalPairingGrant`. `params` are the raw
/// request params (`{ code, appName }`); `peer` is the TCP peer address
/// (re-checked here as defence in depth); `display_name` is the host's
/// discovery-record display name.
///
/// # Errors
///
/// The §5.58 redemption errors, in contract order: peer, params, code state,
/// comparison. `ListenerDown` / `Internal` when the listener or certificate
/// cannot be described; these are checked before the code is touched, so
/// the code stays active and no try is counted.
pub async fn redeem_grant(
    api: &dyn WorkspaceApi,
    provider: &dyn ServerPairingInfo,
    params: Option<&Value>,
    peer: IpAddr,
    display_name: &str,
) -> Result<Value, LocalPairingError> {
    if !is_loopback_peer(peer) {
        return Err(LocalPairingError::NotLoopback);
    }
    let params = params_object(params)?;
    let code = match params.get("code") {
        Some(Value::String(code)) => code.clone(),
        _ => return Err(LocalPairingError::InvalidParams("code is required".into())),
    };
    let app_name = match params.get("appName") {
        Some(Value::String(name)) => name.clone(),
        _ => {
            return Err(LocalPairingError::InvalidParams(
                "appName is required".into(),
            ))
        }
    };
    // Contract order: params are fully validated (a malformed code or name
    // is `invalid-params` and never a try) before anything else is checked.
    if intent_core::local_pairing::normalize_code(&code).is_none() {
        return Err(LocalPairingError::InvalidParams(
            "code must be 6 characters from the pairing alphabet".into(),
        ));
    }
    intent_core::local_pairing::validate_app_name(&app_name)?;
    // Describe the listener before spending the code, so a down listener or
    // unreadable certificate never burns it.
    let port = provider
        .pairing_snapshot()
        .await
        .port
        .ok_or(LocalPairingError::ListenerDown)?;
    let fingerprint = crate::ensure_tls_certificate(provider.data_dir())
        .map_err(|e| LocalPairingError::Internal(e.to_string()))?
        .fingerprint256;
    let redeemed = service(api)?.redeem(code, app_name).await?;
    let host = if peer.to_canonical().is_ipv6() {
        "::1"
    } else {
        "127.0.0.1"
    };
    Ok(json!({
        "version": GRANT_VERSION,
        "host": host,
        "port": port,
        "fingerprint": fingerprint,
        "credentialId": redeemed.credential_id,
        "token": redeemed.token,
        "displayName": display_name,
        "prettyHostname": provider.host_environment().pretty_hostname,
    }))
}

#[cfg(test)]
mod tests;
