//! Server pairing fast-path: `server.pairingInfo` + `server.rotateToken`
//! (docs/protocol/05-method-catalog.md §5 fast-path catalog).
//!
//! These two methods expose pairing credentials (token + fingerprint + port + local IPs + hostname)
//! and rotate the bearer token. They are LOCAL-ONLY: gated on the real connection origin (UDS vs TCP)
//! via the task-local context set by the transport layer. WSS connections are ALWAYS remote (TCP),
//! regardless of the `--mode local` locality flag. UDS connections are ALWAYS local. Remote callers
//! receive a -32001 auth error. This mirrors the `control::` fast-path pattern and sits one layer
//! above the domain [`WorkspaceApi`] router, intercepted before JSON-RPC dispatch.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};
use intent_core::{Error, Result};

/// Pairing information provider: implemented by the composition root (`intentd`).
/// Provides access to token store, data dir, and port snapshot for pairing RPCs.
pub trait ServerPairingInfo: Send + Sync {
    /// Get the current bound WSS port, or `None` if the listener is stopped.
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>>;
    /// Data directory for TLS cert access.
    fn data_dir(&self) -> &std::path::Path;
    /// Token store for get/generate operations.
    fn token_store(&self) -> &crate::AsyncTokenStore;
}

/// Point-in-time snapshot of pairing-relevant server state.
#[derive(Debug, Clone)]
pub struct PairingSnapshot {
    /// The bound WSS port, when the TCP listener is running.
    pub port: Option<u16>,
    /// The address the running listener is bound to (`server.bindAddress`),
    /// when known — drives which hosts the pairing payload advertises
    /// ([`pairing_hosts`]).
    pub bind_address: Option<std::net::IpAddr>,
}

/// The two server methods, once classified.
#[derive(Debug)]
pub(crate) enum ServerMethod {
    PairingInfo,
    RotateToken,
}

/// A classified `server.*` request awaiting handling by the connection task.
pub(crate) struct ServerRequest {
    pub method: ServerMethod,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `server.pairingInfo` / `server.rotateToken` request, or
/// `None` to fall through to the JSON-RPC dispatcher. Mirrors `control::classify`.
pub(crate) fn classify(value: &Value) -> Option<ServerRequest> {
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
        "server.pairingInfo" => ServerMethod::PairingInfo,
        "server.rotateToken" => ServerMethod::RotateToken,
        _ => return None,
    };
    Some(ServerRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Handle a classified `server.*` request: build the response frame (or `None`
/// for a notification). LOCAL-ONLY: remote connections get an auth error.
pub(crate) async fn handle(
    req: ServerRequest,
    provider: &Arc<dyn ServerPairingInfo>,
    is_local: bool,
) -> Option<String> {
    if !is_local {
        // Remote connections are not allowed to access pairing info or rotate tokens.
        if !req.id_present {
            return None;
        }
        return Some(error_frame(
            req.id_echo,
            -32001,
            "server.* methods are local-only",
        ));
    }

    let result: std::result::Result<Value, (i32, String)> = match req.method {
        ServerMethod::PairingInfo => pairing_info_json(provider.as_ref())
            .await
            .map_err(|e| (e.code(), e.to_string())),
        ServerMethod::RotateToken => rotate_token_json(provider.as_ref())
            .await
            .map_err(|e| (e.code(), e.to_string())),
    };

    if !req.id_present {
        return None;
    }
    match result {
        Ok(v) => Some(success_frame(req.id_echo, v)),
        Err((code, msg)) => Some(error_frame(req.id_echo, code, &msg)),
    }
}

/// Build the `server.pairingInfo` result JSON.
async fn pairing_info_json(provider: &dyn ServerPairingInfo) -> Result<Value> {
    let snapshot = provider.pairing_snapshot().await;
    let token = crate::get_or_create_token(provider.token_store()).await?;
    let cert = crate::ensure_tls_certificate(provider.data_dir())?;
    let local_ips = pairing_hosts(&snapshot);
    let hostname = crate::host_env::local_hostname();

    Ok(json!({
        "token": token,
        "certFingerprint": cert.fingerprint256,
        "port": snapshot.port,
        "path": "/ws",
        "localIps": local_ips,
        "hostname": hostname,
    }))
}

/// Build the `server.rotateToken` result JSON. Returns an error when INTENTD_AUTH_TOKEN is set.
async fn rotate_token_json(provider: &dyn ServerPairingInfo) -> Result<Value> {
    // Check if INTENTD_AUTH_TOKEN is set (token is fixed by env).
    if std::env::var("INTENTD_AUTH_TOKEN")
        .map(|t| !t.is_empty())
        .unwrap_or(false)
    {
        return Err(Error::InvalidParams(
            "cannot rotate token: INTENTD_AUTH_TOKEN is set (token is fixed by env)".to_string(),
        ));
    }

    crate::generate_token(provider.token_store()).await?;
    pairing_info_json(provider).await
}

/// Hosts the pairing payload should advertise for `snapshot`: a listener
/// bound to a specific address (loopback included) is reachable only there,
/// so advertise exactly that address; an unspecified bind (`0.0.0.0` / `::`)
/// or an unknown one falls back to enumerating the machine's local IPs.
/// An IPv6-unspecified bind (`::`) also accepts native IPv6 connections
/// (v4-mapped sockets cover the IPv4 side), so its enumeration additionally
/// carries the machine's global IPv6 addresses.
pub fn pairing_hosts(snapshot: &PairingSnapshot) -> Vec<String> {
    match snapshot.bind_address {
        Some(addr) if !addr.is_unspecified() => vec![addr.to_string()],
        Some(std::net::IpAddr::V6(_)) => {
            let mut hosts = collect_local_ips();
            hosts.extend(collect_local_ipv6s());
            hosts
        }
        _ => collect_local_ips(),
    }
}

/// Collect local non-loopback, non-link-local IPv6 addresses. Companion to
/// [`collect_local_ips`] for advertising hosts of an IPv6-unspecified (`::`)
/// bind; link-local (`fe80::/10`) addresses are skipped because they are not
/// usable without a zone index.
fn collect_local_ipv6s() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if ["docker", "veth", "br-", "bridge", "vboxnet", "vmnet"]
                .iter()
                .any(|p| iface.name.starts_with(p))
            {
                continue;
            }
            if iface.is_loopback() {
                continue;
            }
            if let std::net::IpAddr::V6(v6) = iface.ip() {
                if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                    continue;
                }
                let addr = v6.to_string();
                if !ips.contains(&addr) {
                    ips.push(addr);
                }
            }
        }
    }
    ips
}

/// Enumerate `(interface name, IPv4)` candidates for the `intentd pair`
/// bind-address picker: loopback included (listed first), virtual/container
/// interfaces skipped (same prefixes as [`collect_local_ips`]), one entry per
/// distinct address.
pub fn collect_bind_interfaces() -> Vec<(String, std::net::Ipv4Addr)> {
    let mut out: Vec<(String, std::net::Ipv4Addr)> = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if ["docker", "veth", "br-", "bridge", "vboxnet", "vmnet"]
                .iter()
                .any(|p| iface.name.starts_with(p))
            {
                continue;
            }
            if let std::net::IpAddr::V4(v4) = iface.ip() {
                if !out.iter().any(|(_, ip)| *ip == v4) {
                    out.push((iface.name.clone(), v4));
                }
            }
        }
    }
    out.sort_by_key(|(_, ip)| !ip.is_loopback());
    out
}

/// Collect local IP addresses (non-loopback IPv4) for pairing. Mirrors the logic
/// from `tls::collect_san` but returns only the local IPs (no localhost/loopback).
/// Shared with the `pairing.getInfo` fast-path and the `system.status` snapshot
/// (composition root) so all surfaces report the same hosts.
pub fn collect_local_ips() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            // Skip virtual/container interfaces (same prefixes as tls::collect_san).
            if ["docker", "veth", "br-", "bridge", "vboxnet", "vmnet"]
                .iter()
                .any(|p| iface.name.starts_with(p))
            {
                continue;
            }
            if iface.is_loopback() {
                continue;
            }
            if let std::net::IpAddr::V4(v4) = iface.ip() {
                let addr = v4.to_string();
                if !ips.contains(&addr) {
                    ips.push(addr);
                }
            }
        }
    }
    ips
}

#[cfg(test)]
mod tests;
