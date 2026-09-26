//! `pairing.getInfo` fast-path: the structured QR pairing payload (§5.2).
//!
//! Returns the `intent://pair?…` payload URI plus its component fields so GUI
//! clients can render their own QR code and the `intentd pair` CLI can print
//! one in the terminal. Reuses the exact host/fingerprint/token sources as
//! `server.pairingInfo` via the shared [`ServerPairingInfo`] provider wired by
//! the composition root. LOCAL-ONLY, mirroring `server.*`: the payload embeds
//! the long-lived bearer token (Decision 4), so remote (TCP) callers get a
//! -32001 auth error regardless of the `--mode` locality flag.

use std::fmt::Write as _;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::events::{error_frame, error_frame_with_data, success_frame};
use crate::server::{pairing_hosts, ServerPairingInfo};
use intent_core::{current_caller, Caller, Error, Result, WorkspaceApi};

/// Private connection state; never persisted, logged, or exposed to agents.
#[derive(Default)]
pub(crate) struct ConnectionPairing {
    pub(crate) admitted: Option<crate::auth::AdmittedCredential>,
    pub(crate) revoked: bool,
}

pub(crate) struct SelfPairingRequest {
    request: PairingRequest,
    valid_params: bool,
}

pub(crate) fn classify_self(value: &Value) -> Option<SelfPairingRequest> {
    Some(SelfPairingRequest {
        request: classify_method(value, "pairing.getSelfInfo")?,
        valid_params: value.get("params").is_none_or(|p| {
            p.is_null()
                || p.as_object().is_some_and(serde_json::Map::is_empty)
                || p.as_array().is_some_and(Vec::is_empty)
        }),
    })
}

pub(crate) async fn handle_self(
    req: SelfPairingRequest,
    provider: &Arc<dyn ServerPairingInfo>,
    api: &Arc<dyn WorkspaceApi>,
    connection: &mut ConnectionPairing,
) -> Option<String> {
    let request = req.request;
    let result = if req.valid_params {
        get_self_info(
            provider.as_ref(),
            api.as_ref(),
            connection.admitted.as_ref(),
        )
        .await
    } else {
        Err(Error::InvalidParams(
            "pairing.getSelfInfo takes no parameters".into(),
        ))
    };
    connection.revoked = matches!(result, Err(Error::Forbidden(_)));
    if !request.id_present {
        return None;
    }
    Some(match result {
        Ok(value) => success_frame(&request.id_echo, &value),
        Err(error) => {
            let code = match error {
                Error::InvalidParams(_) => Some("invalid-params"),
                Error::Forbidden(_) => Some("access-revoked"),
                Error::ListenerDown => Some("listener-down"),
                _ => None,
            };
            if let Some(code) = code {
                error_frame_with_data(
                    &request.id_echo,
                    error.code(),
                    &error.to_string(),
                    &json!({"code":code}),
                )
            } else {
                error_frame(&request.id_echo, error.code(), &error.to_string())
            }
        }
    })
}

async fn get_self_info(
    provider: &dyn ServerPairingInfo,
    api: &dyn WorkspaceApi,
    admitted: Option<&crate::auth::AdmittedCredential>,
) -> Result<Value> {
    let denied = || Error::Forbidden("pairing access revoked".into());
    let caller = current_caller().ok_or_else(denied)?;
    let Caller::Wire { principal_id, .. } = &caller else {
        return Err(denied());
    };
    let local = !crate::context::is_tcp_connection();
    // Locality overrides and caller-supplied labels never select a credential.
    let local_token;
    let token = if local {
        if api.primary_principal_id().await.ok().as_ref() != Some(principal_id) {
            return Err(denied());
        }
        local_token = provider
            .token_store()
            .load_token()
            .await
            .ok_or_else(denied)?;
        local_token.as_str()
    } else {
        let admitted = admitted.ok_or_else(denied)?;
        if !admitted
            .valid_for(provider.token_store(), api, &caller)
            .await
        {
            return Err(denied());
        }
        admitted.token()
    };
    let snapshot = provider.pairing_snapshot().await;
    let mut result = pairing_json(provider, &snapshot, token)?;
    let principal = api.principal_me().await.map_err(|_| denied())?;
    if principal["id"].as_str() != Some(principal_id.0.as_str()) {
        return Err(denied());
    }
    // All route/profile awaits precede this final credential check. A removal
    // that wins this ordering returns no token; one after it invalidates reuse.
    let valid = if local {
        provider.token_store().load_token().await.as_deref() == Some(token)
    } else {
        admitted
            .expect("remote admission checked above")
            .valid_for(provider.token_store(), api, &caller)
            .await
    };
    if !valid {
        return Err(denied());
    }
    result["principal"] = principal;
    Ok(result)
}

/// Version of the `intent://pair` payload format (the `v` query parameter and
/// the `version` field of the `pairing.getInfo` result).
pub(crate) const PAIRING_PAYLOAD_VERSION: u32 = 1;

/// Build the pairing payload URI:
/// `intent://pair?v=1&host=<ip[,ip...]>&port=<p>&fp=<sha256>&token=<t>[&tc=<addr>]`.
///
/// The `tc=` parameter is additive (appended only when the tailcat tunnel is
/// running); clients that don't know it tolerate the unknown query param.
///
/// Query values are percent-encoded defensively. Generated values (dotted-quad
/// IPv4 hosts, colon-separated hex fingerprints, 64-char hex tokens) pass
/// through unchanged, but a token injected via `INTENTD_AUTH_TOKEN` may contain
/// reserved characters (`&`, `=`, `%`, …) that would otherwise make the query
/// string ambiguous.
pub(crate) fn build_pairing_uri(
    hosts: &[String],
    port: u16,
    fingerprint: &str,
    token: &str,
    tc_address: Option<&str>,
) -> String {
    let hosts = hosts
        .iter()
        .map(|h| encode_query_value(h))
        .collect::<Vec<_>>()
        .join(",");
    let mut uri = format!(
        "intent://pair?v={PAIRING_PAYLOAD_VERSION}&host={hosts}&port={port}&fp={}&token={}",
        encode_query_value(fingerprint),
        encode_query_value(token)
    );
    if let Some(tc) = tc_address {
        let _ = write!(uri, "&tc={}", encode_query_value(tc));
    }
    uri
}

/// Percent-encode a query value, passing through unreserved characters
/// (RFC 3986) plus `:` (valid in query strings; keeps fingerprints readable).
pub(crate) fn encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b':' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// A classified `pairing.getInfo` request awaiting handling by the connection task.
pub(crate) struct PairingRequest {
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `pairing.getInfo` request, or `None` to fall
/// through to the JSON-RPC dispatcher. Mirrors `server::classify`.
pub(crate) fn classify(value: &Value) -> Option<PairingRequest> {
    classify_method(value, "pairing.getInfo")
}

fn classify_method(value: &Value, expected: &str) -> Option<PairingRequest> {
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
    if method != expected {
        return None;
    }
    Some(PairingRequest {
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Handle a classified `pairing.getInfo`: build the response frame (or `None`
/// for a notification). LOCAL-ONLY: remote connections get -32001, mirroring
/// `server.*` (the payload embeds the bearer token).
pub(crate) async fn handle(
    req: PairingRequest,
    provider: &Arc<dyn ServerPairingInfo>,
    is_local: bool,
) -> Option<String> {
    if !is_local {
        if !req.id_present {
            return None;
        }
        return Some(error_frame(
            &req.id_echo,
            -32001,
            "pairing.getInfo is local-only",
        ));
    }
    let result = get_info_json(provider.as_ref()).await;
    if !req.id_present {
        return None;
    }
    match result {
        Ok(v) => Some(success_frame(&req.id_echo, &v)),
        // Listener-down carries the machine-readable discriminator
        // `error.data.code = "listener-down"` so `intentd pair` auto-enable
        // stops depending on message prose (monorepo#1822).
        Err(e @ Error::ListenerDown) => Some(error_frame_with_data(
            &req.id_echo,
            e.code(),
            &e.to_string(),
            &json!({ "code": "listener-down" }),
        )),
        Err(e) => Some(error_frame(&req.id_echo, e.code(), &e.to_string())),
    }
}

/// Build the `pairing.getInfo` result JSON: `{ uri, hosts, port, fingerprint,
/// token, version }`. Errors clearly when the TCP listener is not running —
/// there is no port to embed in the payload, so pairing is impossible
/// ([`Error::ListenerDown`], surfaced with `error.data.code = "listener-down"`)
/// — and when there is no dialable route at all (no non-loopback host and no
/// tunnel): a payload without a route would report pairing as successful while
/// no other device can connect through it.
async fn get_info_json(provider: &dyn ServerPairingInfo) -> Result<Value> {
    let snapshot = provider.pairing_snapshot().await;
    snapshot.port.ok_or(Error::ListenerDown)?;
    let token = crate::get_or_create_token(provider.token_store()).await?;
    pairing_json(provider, &snapshot, &token)
}

fn pairing_json(
    provider: &dyn ServerPairingInfo,
    snapshot: &crate::server::PairingSnapshot,
    token: &str,
) -> Result<Value> {
    let port = snapshot.port.ok_or(Error::ListenerDown)?;
    let cert = crate::ensure_tls_certificate(provider.data_dir())?;
    // Bind-aware hosts: a specific bind advertises exactly its non-loopback
    // addresses (loopback is never dialable from another device, so it is
    // filtered even when bound); only an unspecified/unknown bind enumerates
    // local IPs. Pairing needs at least one dialable route — a direct host or
    // the tunnel — so an empty set with no tunnel errors instead of minting a
    // payload no other device can connect through. With the tunnel up, an
    // empty host list still pairs: the tc= route carries it.
    let hosts = pairing_hosts(snapshot);
    if hosts.is_empty() && snapshot.tc_address.is_none() {
        let enumerated = snapshot
            .bind_addresses
            .as_deref()
            .is_none_or(|a| a.is_empty() || a.iter().any(std::net::IpAddr::is_unspecified));
        let msg = if enumerated {
            "no non-loopback IPv4 address found — connect this machine to a network before pairing"
        } else {
            "listener is bound to loopback only and no tunnel is active — set \
             server.bindAddress to a LAN address or enable the tunnel \
             (server.tunnel.enabled) before pairing"
        };
        return Err(Error::Unsupported(msg.to_string()));
    }
    let uri = build_pairing_uri(
        &hosts,
        port,
        &cert.fingerprint256,
        token,
        snapshot.tc_address.as_deref(),
    );
    let mut result = json!({
        "uri": uri,
        "hosts": hosts,
        "port": port,
        "fingerprint": cert.fingerprint256,
        "token": token,
        "version": PAIRING_PAYLOAD_VERSION,
    });
    // Additive tunnel route (presence-detected): omitted when the tunnel is
    // disabled or down, so older clients are unaffected.
    if let Some(tc) = &snapshot.tc_address {
        result
            .as_object_mut()
            .expect("get_info_json literal is an object")
            .insert("tcAddress".into(), tc.clone().into());
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
