//! `host.*` capability probe fast-path: `host.status` (§5.14, §12.3).
//!
//! `host.status` is a transport/host concern — its result depends on the
//! serving transport's locality and on host-level probes (OS/arch, display
//! availability) rather than on the domain [`crate::router`]/`WorkspaceApi`. So,
//! like the `system.*` (§5.7) and `events.` fast-paths, every listener
//! intercepts it before the JSON-RPC dispatcher. Unlike `system.*` (UDS-only
//! control), `host.status` is answered on BOTH transports so a remote WSS client
//! can probe the daemon host's nature and gate GUI/forwarding UI accordingly.

use std::fmt;

use serde_json::{json, Value};

use crate::discovery::{detect_display_server, detect_has_display, local_hostname};
use crate::events::success_frame;
use crate::reverse::{ReverseChannel, DEFAULT_REVERSE_TIMEOUT};

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

/// Opens a URL/file on the daemon host's GUI. Injected so the local path is
/// unit-testable without launching a real browser.
pub trait ExternalOpener: Send + Sync {
    /// Open `url` with the host's default handler. Returns a descriptive error
    /// when the platform opener cannot be spawned.
    fn open(&self, url: &str) -> Result<(), String>;
}

/// Default opener: the platform handler (`open` on macOS, `cmd /C start` on
/// Windows, `xdg-open` elsewhere), detached from this process's stdio.
pub struct OsOpener;

impl ExternalOpener for OsOpener {
    fn open(&self, url: &str) -> Result<(), String> {
        #[cfg(target_os = "macos")]
        let mut cmd = {
            let mut c = std::process::Command::new("open");
            c.arg(url);
            c
        };
        #[cfg(target_os = "windows")]
        let mut cmd = {
            let mut c = std::process::Command::new("cmd");
            c.args(["/C", "start", "", url]);
            c
        };
        #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
        let mut cmd = {
            let mut c = std::process::Command::new("xdg-open");
            c.arg(url);
            c
        };
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        cmd.spawn()
            .map(|_| ())
            .map_err(|e| format!("open external failed: {e}"))
    }
}

/// Why a [`open_external`] call could not be satisfied. `code()` maps each to a
/// standard JSON-RPC error code (PROTOCOL §9: no custom codes — server-side
/// conditions are `-32602`/`-32603` with a descriptive message).
#[derive(Debug)]
pub enum OpenExternalError {
    /// `hasDisplay=false` on the daemon host and the op needs a display (§12.4).
    Headless(String),
    /// The host OS opener failed (the local path).
    Opener(String),
    /// The FE-served reverse RPC failed/timed out (the remote path).
    Proxy(String),
    /// The `url` parameter was missing or empty.
    InvalidUrl(String),
}

impl OpenExternalError {
    /// JSON-RPC 2.0 numeric error code for this condition (PROTOCOL §9).
    pub fn code(&self) -> i32 {
        match self {
            OpenExternalError::InvalidUrl(_) => -32602,
            OpenExternalError::Headless(_)
            | OpenExternalError::Opener(_)
            | OpenExternalError::Proxy(_) => -32603,
        }
    }
}

impl fmt::Display for OpenExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenExternalError::Headless(m)
            | OpenExternalError::Opener(m)
            | OpenExternalError::Proxy(m)
            | OpenExternalError::InvalidUrl(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for OpenExternalError {}

/// Open `url` on the *user's* machine (§12.4). When the connection is local the
/// daemon resolves it directly via `opener`; if the local host is headless
/// (`has_display=false`) this is a clear headless warning instead of a silent
/// failure. When the connection is remote the intent is dispatched to the
/// connected frontend as an FE-served reverse RPC (`host.openExternal`) so it
/// opens on the user's machine (mirroring the ACP client-served pattern, §6.7).
pub async fn open_external(
    url: &str,
    is_local: bool,
    has_display: bool,
    opener: &dyn ExternalOpener,
    reverse: &ReverseChannel,
) -> Result<(), OpenExternalError> {
    if url.is_empty() {
        return Err(OpenExternalError::InvalidUrl(
            "Missing required parameter: url".to_string(),
        ));
    }
    if is_local {
        if !has_display {
            return Err(OpenExternalError::Headless(format!(
                "host is headless (hasDisplay=false); cannot open {url} on the daemon host — connect a client with a display"
            )));
        }
        return opener.open(url).map_err(OpenExternalError::Opener);
    }
    reverse
        .request(
            "host.openExternal",
            json!({ "url": url }),
            DEFAULT_REVERSE_TIMEOUT,
        )
        .await
        .map(|_| ())
        .map_err(|e| OpenExternalError::Proxy(e.message))
}

#[cfg(test)]
mod tests;
