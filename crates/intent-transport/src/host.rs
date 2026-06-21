//! `host.*` capability probe fast-path: `host.status` (§5.14, §12.3).
//!
//! `host.status` is a transport/host concern — its result depends on the
//! serving transport's locality and on host-level probes (OS/arch, display
//! availability) rather than on the domain [`crate::router`]/`WorkspaceApi`. So,
//! like the `system.*` (§5.7) and `events.` fast-paths, every listener
//! intercepts it before the JSON-RPC dispatcher. Unlike `system.*` (UDS-only
//! control), `host.status` is answered on BOTH transports so a remote WSS client
//! can probe the daemon host's nature and gate GUI/forwarding UI accordingly.

use serde_json::{json, Value};

use crate::discovery::{detect_display_server, detect_has_display, local_hostname};
use crate::events::success_frame;

/// Resolve the effective locality for a connection (§5.14): the transport
/// default (`true`/local for UDS, `false`/remote for TCP/WSS) unless forced by
/// `--mode local|remote` / the `server.locality` setting. Pure so the
/// per-transport + override matrix is unit-testable.
pub fn resolve_is_local(transport_local: bool, override_local: Option<bool>) -> bool {
    override_local.unwrap_or(transport_local)
}

/// A classified `host.status` request awaiting handling by the connection task.
pub(crate) struct HostRequest {
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `host.status` request, or `None` to fall through
/// to the events fast-path / JSON-RPC dispatcher. Mirrors `control::classify`:
/// a JSON-RPC 2.0 object with a string `method` and an `id` (if present) that is
/// a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<HostRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    if obj.get("method").and_then(Value::as_str)? != "host.status" {
        return None;
    }
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    Some(HostRequest {
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Build the `host.status` result JSON (§5.14): `{ os, arch, hostname,
/// hasDisplay, locality, displayServer? }`. `displayServer` is omitted when no
/// display server is detected. Pure (inputs injected) so it is unit-testable.
pub(crate) fn host_status_json(
    os: &str,
    arch: &str,
    hostname: &str,
    has_display: bool,
    display_server: Option<&str>,
    is_local: bool,
) -> Value {
    let mut result = json!({
        "os": os,
        "arch": arch,
        "hostname": hostname,
        "hasDisplay": has_display,
        "locality": if is_local { "local" } else { "remote" },
    });
    if let Some(ds) = display_server {
        result["displayServer"] = json!(ds);
    }
    result
}

/// Handle a classified `host.status` request: probe the host and render the
/// result frame (or `None` for a notification, which gets no reply). `is_local`
/// is the resolved locality of the serving connection (§5.14).
pub(crate) fn handle(req: HostRequest, is_local: bool) -> Option<String> {
    let result = host_status_json(
        std::env::consts::OS,
        std::env::consts::ARCH,
        &local_hostname(),
        detect_has_display(),
        detect_display_server().as_deref(),
        is_local,
    );
    if !req.id_present {
        return None;
    }
    Some(success_frame(req.id_echo, result))
}

#[cfg(test)]
mod tests;
