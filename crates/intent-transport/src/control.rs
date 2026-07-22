//! System control fast-path: `system.status` + `system.shutdown` (§5.7).
//!
//! These two control methods surface live daemon state and request a graceful
//! shutdown. They sit one layer above the domain [`WorkspaceApi`] router because
//! the data they expose (bound port, connected clients, active agents, the TLS
//! fingerprint) and the action they take (tear down the listener) are
//! transport/process concerns, not domain operations. The composition root
//! (`intentd`) implements [`SystemControl`]; each listener intercepts the two
//! methods before the JSON-RPC dispatcher, mirroring the `events.` fast-path.

use serde_json::{json, Value};

use crate::events::success_frame;
use crate::protocol::PROTOCOL_VERSION;

/// A point-in-time snapshot of daemon state for `system.status` (§5.7, §12.3).
/// `locality` is derived per-connection (UDS ⇒ `local`, WSS ⇒ `remote`) and so
/// is not stored here; it is applied when the snapshot is rendered to JSON.
#[derive(Debug, Clone)]
pub struct SystemStatus {
    /// Derived listen mode: `both` while the WSS listener is up, else `uds`
    /// (the UDS listener always serves).
    pub listen_mode: String,
    /// Whether the UDS listener is active (always true in practice).
    pub uds: bool,
    /// Whether the TCP/WSS listener is currently active (runtime state, not a
    /// boot-time flag).
    pub tcp: bool,
    /// The bound WSS port, when the TCP listener is running.
    pub port: Option<u16>,
    /// Currently-connected WebSocket clients (UDS connections are not tracked).
    pub clients: usize,
    /// Live agent processes registered with the manager.
    pub agents: usize,
    /// The pinned SHA-256 certificate fingerprint, when a cert exists.
    pub fingerprint: Option<String>,
    /// Host OS identifier (`std::env::consts::OS`).
    pub os: String,
    /// Host CPU architecture (`std::env::consts::ARCH`).
    pub arch: String,
    /// Whether a GUI/display is available on the host (§12.3).
    pub has_display: bool,
    /// The AgentManager concurrency cap (resolved maxConcurrent incl. auto-detection).
    pub max_agents: usize,
    /// The daemon crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Uptime in seconds since daemon start.
    pub uptime_seconds: u64,
    /// CPU usage of the daemon process, raw `sysinfo` convention: 100 = one
    /// full core, so values may exceed 100 on multi-core hosts. The first
    /// sample after startup may legitimately read 0.
    pub cpu_percent: f32,
    /// Resident memory of the daemon process, in bytes.
    pub memory_bytes: u64,
}

/// Live daemon control surface implemented by the composition root (`intentd`).
/// Lets a listener answer `system.status` from real state and trigger a
/// graceful shutdown for `system.shutdown` without reaching into domain code.
pub trait SystemControl: Send + Sync {
    /// Snapshot the current daemon state.
    fn status(&self) -> SystemStatus;
    /// Request a graceful shutdown (idempotent). Returns immediately; the daemon
    /// tears the listeners down asynchronously.
    fn request_shutdown(&self);
}

/// The two control methods, once classified.
pub(crate) enum SystemMethod {
    Status,
    Shutdown,
}

/// A classified `system.*` request awaiting handling by the connection task.
pub(crate) struct SystemRequest {
    pub method: SystemMethod,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `system.status` / `system.shutdown` request, or
/// `None` to fall through to the events fast-path / JSON-RPC dispatcher. Mirrors
/// the `events::classify` pre-check: a JSON-RPC 2.0 object with a string
/// `method` and an `id` (if present) that is a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<SystemRequest> {
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
        "system.status" => SystemMethod::Status,
        "system.shutdown" => SystemMethod::Shutdown,
        _ => return None,
    };
    Some(SystemRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Render a [`SystemStatus`] to the `system.status` result JSON. `is_local`
/// reflects the serving transport (UDS ⇒ `local`, WSS ⇒ `remote`, §12.3).
pub(crate) fn status_json(status: &SystemStatus, is_local: bool) -> Value {
    let mut transports = Vec::new();
    if status.uds {
        transports.push("uds");
    }
    if status.tcp {
        transports.push("tcp");
    }
    json!({
        "running": true,
        "listenMode": status.listen_mode,
        "transports": transports,
        "port": status.port,
        "clients": status.clients,
        "agents": status.agents,
        "maxAgents": status.max_agents,
        "version": status.version,
        "uptimeSeconds": status.uptime_seconds,
        "cpuPercent": status.cpu_percent,
        "memoryBytes": status.memory_bytes,
        "fingerprint": status.fingerprint,
        "protocolVersion": PROTOCOL_VERSION,
        "host": {
            "os": status.os,
            "arch": status.arch,
            "hasDisplay": status.has_display,
            "locality": if is_local { "local" } else { "remote" },
        },
    })
}

/// Handle a classified `system.*` request: build the response frame (or `None`
/// for a notification, which gets no reply). `system.shutdown` triggers the
/// graceful teardown before acknowledging.
pub(crate) fn handle(
    req: SystemRequest,
    control: &dyn SystemControl,
    is_local: bool,
) -> Option<String> {
    let result = match req.method {
        SystemMethod::Status => status_json(&control.status(), is_local),
        SystemMethod::Shutdown => {
            control.request_shutdown();
            json!({ "ok": true, "stopping": true })
        }
    };
    if !req.id_present {
        return None;
    }
    Some(success_frame(req.id_echo, result))
}

#[cfg(test)]
mod tests;
