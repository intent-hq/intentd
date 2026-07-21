//! `pairing.getInfo` fast-path: the structured QR pairing payload (§5.2).
//!
//! Returns the `intent://pair?…` payload URI plus its component fields so GUI
//! clients can render their own QR code and the `intentd pair` CLI can print
//! one in the terminal. Reuses the exact host/fingerprint/token sources as
//! `server.pairingInfo` via the shared [`ServerPairingInfo`] provider wired by
//! the composition root. LOCAL-ONLY, mirroring `server.*`: the payload embeds
//! the long-lived bearer token (Decision 4), so remote (TCP) callers get a
//! -32001 auth error regardless of the `--mode` locality flag.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};
use crate::server::{collect_local_ips, ServerPairingInfo};
use intent_core::{Error, Result};

/// Version of the `intent://pair` payload format (the `v` query parameter and
/// the `version` field of the `pairing.getInfo` result).
pub const PAIRING_PAYLOAD_VERSION: u32 = 1;

/// Build the pairing payload URI:
/// `intent://pair?v=1&host=<ip[,ip...]>&port=<p>&fp=<sha256>&token=<t>`.
///
/// All values are URI-safe by construction — hosts are dotted-quad IPv4
/// literals, the fingerprint is colon-separated hex, and the token is 64-char
/// lowercase hex — so no percent-encoding is required.
pub fn build_pairing_uri(hosts: &[String], port: u16, fingerprint: &str, token: &str) -> String {
    format!(
        "intent://pair?v={PAIRING_PAYLOAD_VERSION}&host={}&port={port}&fp={fingerprint}&token={token}",
        hosts.join(",")
    )
}

/// A classified `pairing.getInfo` request awaiting handling by the connection task.
pub(crate) struct PairingRequest {
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `pairing.getInfo` request, or `None` to fall
/// through to the JSON-RPC dispatcher. Mirrors `server::classify`.
pub(crate) fn classify(value: &Value) -> Option<PairingRequest> {
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
    match method {
        "pairing.getInfo" => {}
        _ => return None,
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
            req.id_echo,
            -32001,
            "pairing.getInfo is local-only",
        ));
    }
    let result = get_info_json(provider.as_ref()).await;
    if !req.id_present {
        return None;
    }
    match result {
        Ok(v) => Some(success_frame(req.id_echo, v)),
        Err(e) => Some(error_frame(req.id_echo, e.code(), &e.to_string())),
    }
}

/// Build the `pairing.getInfo` result JSON: `{ uri, hosts, port, fingerprint,
/// token, version }`. Errors clearly when the TCP listener is not running —
/// there is no port to embed in the payload, so pairing is impossible.
async fn get_info_json(provider: &dyn ServerPairingInfo) -> Result<Value> {
    let snapshot = provider.pairing_snapshot().await;
    let port = snapshot.port.ok_or_else(|| {
        Error::Unsupported(
            "TCP listener is not running — start the daemon with `intentd serve --listen both` \
             (or enable the WSS listener) before pairing"
                .to_string(),
        )
    })?;
    let token = crate::get_or_create_token(provider.token_store()).await?;
    let cert = crate::ensure_tls_certificate(provider.data_dir())?;
    let hosts = collect_local_ips();
    if hosts.is_empty() {
        return Err(Error::Unsupported(
            "no non-loopback IPv4 address found — connect this machine to a network before pairing"
                .to_string(),
        ));
    }
    let uri = build_pairing_uri(&hosts, port, &cert.fingerprint256, &token);
    Ok(json!({
        "uri": uri,
        "hosts": hosts,
        "port": port,
        "fingerprint": cert.fingerprint256,
        "token": token,
        "version": PAIRING_PAYLOAD_VERSION,
    }))
}

#[cfg(test)]
mod tests;
