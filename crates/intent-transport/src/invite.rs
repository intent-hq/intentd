//! Invite links and the identity-only join (multiplayer w4).
//!
//! Two fast paths share this module:
//!
//! - `workspace.invite.create` — served on every authenticated connection
//!   (UDS and `/ws`). The service half mints the invite; this half wraps it
//!   into the `intent://invite?…` link, which is the `intent://pair` envelope
//!   (hosts / port / fingerprint / optional `tc`) **minus the bearer token**,
//!   plus `inviteId` and `secret`. It needs the listener's own pairing
//!   snapshot ([`ServerPairingInfo`]), which the JSON-RPC router has no
//!   access to — hence a fast path, like `pairing.getInfo`.
//! - `invite.redeem` — the ONLY method served on the unauthenticated
//!   `/invite` endpoint ([`crate::ws`]). Two phases on one method name:
//!   `{ inviteId, secret }` starts the identity-only GitHub device flow and
//!   returns the user code; `{ flowId }` waits for the grant and returns the
//!   collaborator credential exactly once.

use std::fmt::Write as _;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::events::{error_frame, error_frame_with_data, success_frame};
use crate::pairing::encode_query_value;
use crate::server::{pairing_hosts, ServerPairingInfo};
use intent_core::{Error, InviteErrorKind, Result, WorkspaceApi, WorkspaceId};

/// Version of the `intent://invite` payload format (`v` query parameter and
/// the `version` field of the `workspace.invite.create` result).
pub(crate) const INVITE_PAYLOAD_VERSION: u32 = 1;

/// Human message paired with the `-32001` an `/invite` connection gets for
/// any method other than `invite.redeem`.
pub(crate) const INVITE_ENDPOINT_ONLY_MESSAGE: &str =
    "the /invite endpoint serves invite.redeem only";

/// Build the invite link:
/// `intent://invite?v=1&host=<ip[,ip...]>&port=<p>&fp=<sha256>&inviteId=<id>&secret=<s>[&tc=<addr>]`.
/// Same encoding rules as [`crate::pairing::build_pairing_uri`]; never
/// carries the daemon bearer token.
pub(crate) fn build_invite_uri(
    hosts: &[String],
    port: u16,
    fingerprint: &str,
    invite_id: &str,
    secret: &str,
    tc_address: Option<&str>,
) -> String {
    let hosts = hosts
        .iter()
        .map(|h| encode_query_value(h))
        .collect::<Vec<_>>()
        .join(",");
    let mut uri = format!(
        "intent://invite?v={INVITE_PAYLOAD_VERSION}&host={hosts}&port={port}&fp={}&inviteId={}&secret={}",
        encode_query_value(fingerprint),
        encode_query_value(invite_id),
        encode_query_value(secret)
    );
    if let Some(tc) = tc_address {
        let _ = write!(uri, "&tc={}", encode_query_value(tc));
    }
    uri
}

/// Which of the two invite fast paths a frame names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InviteMethod {
    Create,
    Redeem,
}

/// A classified invite request awaiting handling by the connection task.
pub(crate) struct InviteRequest {
    pub method: InviteMethod,
    pub id_present: bool,
    pub id_echo: Value,
    pub params: Value,
}

/// Classify a parsed frame as an invite fast-path request, or `None` to fall
/// through. Mirrors `pairing::classify`.
pub(crate) fn classify(value: &Value) -> Option<InviteRequest> {
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
        "workspace.invite.create" => InviteMethod::Create,
        "invite.redeem" => InviteMethod::Redeem,
        _ => return None,
    };
    Some(InviteRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
        params: obj.get("params").cloned().unwrap_or(Value::Null),
    })
}

/// Frame a handler outcome for the request (`None` for a notification).
/// Invite refusals carry `error.data.code = <InviteErrorKind>` so a client
/// routes "expired" / "pin mismatch" / "denied" without matching on prose.
fn respond(req: &InviteRequest, result: Result<Value>) -> Option<String> {
    if !req.id_present {
        return None;
    }
    Some(match result {
        Ok(v) => success_frame(&req.id_echo, &v),
        Err(e @ Error::Invite(kind)) => error_frame_with_data(
            &req.id_echo,
            e.code(),
            &e.to_string(),
            &json!({ "code": kind.as_str() }),
        ),
        Err(e @ Error::ListenerDown) => error_frame_with_data(
            &req.id_echo,
            e.code(),
            &e.to_string(),
            &json!({ "code": "listener-down" }),
        ),
        Err(e) => error_frame(&req.id_echo, e.code(), &e.to_string()),
    })
}

/// Refuse an `invite.redeem` the connection will not run because it already
/// has its per-connection quota of requests in flight (`flow-busy`, same
/// code the daemon-wide flow cap answers with). `None` for a notification.
pub(crate) fn refuse_busy(req: &InviteRequest) -> Option<String> {
    respond(req, Err(Error::Invite(InviteErrorKind::FlowBusy)))
}

fn str_param(params: &Value, key: &str) -> Result<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| Error::InvalidParams(format!("missing or invalid `{key}`")))
}

fn opt_str_param(params: &Value, key: &str) -> Result<Option<String>> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(Error::InvalidParams(format!("`{key}` must be a string"))),
    }
}

fn opt_u64_param(params: &Value, key: &str) -> Result<Option<u64>> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| Error::InvalidParams(format!("`{key}` must be a non-negative integer"))),
    }
}

/// The link envelope of this listener: `(hosts, port, fingerprint, tc)`.
/// Resolved BEFORE the invite is minted so a daemon nobody can dial never
/// stores an invite that cannot be redeemed: no TCP listener is
/// [`Error::ListenerDown`] (`error.data.code = "listener-down"`, like
/// `pairing.getInfo`), and no dialable route at all (loopback-only bind and
/// no tunnel) is `Unsupported`.
async fn link_envelope(
    provider: Option<&Arc<dyn ServerPairingInfo>>,
) -> Result<(Vec<String>, u16, String, Option<String>)> {
    let provider = provider.ok_or_else(|| {
        Error::Unsupported("invite links are unavailable on this listener".to_string())
    })?;
    let snapshot = provider.pairing_snapshot().await;
    let port = snapshot.port.ok_or(Error::ListenerDown)?;
    let cert = crate::ensure_tls_certificate(provider.data_dir())?;
    let hosts = pairing_hosts(&snapshot);
    if hosts.is_empty() && snapshot.tc_address.is_none() {
        return Err(Error::Unsupported(
            "no dialable route for an invite link: set server.bindAddress to a LAN \
             address or enable the tunnel (server.tunnel.enabled) before inviting"
                .to_string(),
        ));
    }
    Ok((hosts, port, cert.fingerprint256, snapshot.tc_address))
}

/// Handle a classified `workspace.invite.create`: params
/// `{ workspaceId, pinLogin?, expiresInSecs? }` → the service result
/// (`{ invite, secret }`) extended with `url`, `hosts`, `port`,
/// `fingerprint`, `version` and the additive `tcAddress`. Owner-only in the
/// service layer (`-32003` otherwise); the secret appears exactly once, here.
pub(crate) async fn handle_create(
    req: InviteRequest,
    api: &Arc<dyn WorkspaceApi>,
    provider: Option<&Arc<dyn ServerPairingInfo>>,
) -> Option<String> {
    let result = create_json(&req.params, api, provider).await;
    respond(&req, result)
}

async fn create_json(
    params: &Value,
    api: &Arc<dyn WorkspaceApi>,
    provider: Option<&Arc<dyn ServerPairingInfo>>,
) -> Result<Value> {
    let workspace_id = WorkspaceId::from(str_param(params, "workspaceId")?.as_str());
    let pin_login = opt_str_param(params, "pinLogin")?;
    let expires_in_secs = opt_u64_param(params, "expiresInSecs")?;
    let (hosts, port, fingerprint, tc_address) = link_envelope(provider).await?;
    let mut result = api
        .workspace_invite_create(workspace_id, pin_login, expires_in_secs)
        .await?;
    let invite_id = result
        .pointer("/invite/id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Internal("invite result carries no invite.id".to_string()))?
        .to_string();
    let secret = result
        .get("secret")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Internal("invite result carries no secret".to_string()))?
        .to_string();
    let url = build_invite_uri(
        &hosts,
        port,
        &fingerprint,
        &invite_id,
        &secret,
        tc_address.as_deref(),
    );
    let obj = result
        .as_object_mut()
        .ok_or_else(|| Error::Internal("invite result is not an object".to_string()))?;
    obj.insert("url".into(), url.into());
    obj.insert("hosts".into(), json!(hosts));
    obj.insert("port".into(), port.into());
    obj.insert("fingerprint".into(), fingerprint.into());
    obj.insert("version".into(), INVITE_PAYLOAD_VERSION.into());
    if let Some(tc) = tc_address {
        obj.insert("tcAddress".into(), tc.into());
    }
    Ok(result)
}

/// Handle a classified `invite.redeem` on the `/invite` endpoint. Phase 1
/// (`{ inviteId, secret }`) starts the identity-only device flow; phase 2
/// (`{ flowId }`) blocks until the grant settles (the caller runs this on a
/// detached task so heartbeats keep flowing) and yields the credential once.
pub(crate) async fn handle_redeem(
    req: InviteRequest,
    api: &Arc<dyn WorkspaceApi>,
) -> Option<String> {
    let result = redeem_json(&req.params, api).await;
    respond(&req, result)
}

async fn redeem_json(params: &Value, api: &Arc<dyn WorkspaceApi>) -> Result<Value> {
    match opt_str_param(params, "flowId")? {
        Some(flow_id) if !flow_id.trim().is_empty() => {
            api.invite_redeem_wait(flow_id.trim().to_string()).await
        }
        _ => {
            let invite_id = str_param(params, "inviteId")?;
            let secret = str_param(params, "secret")?;
            api.invite_redeem_start(invite_id, secret).await
        }
    }
}

/// The frame an `/invite` connection gets for any method other than
/// `invite.redeem`: `-32001` (the endpoint is unauthenticated, so nothing
/// else is reachable through it). `None` for a notification.
pub(crate) fn refuse_non_invite(value: &Value) -> Option<String> {
    let obj = value.as_object()?;
    let id = obj.get("id")?;
    Some(error_frame(id, -32001, INVITE_ENDPOINT_ONLY_MESSAGE))
}

#[cfg(test)]
mod tests;
