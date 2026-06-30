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

use intent_core::WorkspaceApi;
use serde_json::{json, Map, Value};

use crate::discovery::{detect_display_server, detect_has_display, local_hostname};
use crate::events::{error_frame, success_frame};
use crate::host_ops;
use crate::reverse::{ReverseChannel, DEFAULT_REVERSE_TIMEOUT};

/// Resolve the effective locality for a connection (§5.14): the transport
/// default (`true`/local for UDS, `false`/remote for TCP/WSS) unless forced by
/// `--mode local|remote` / the `server.locality` setting. Pure so the
/// per-transport + override matrix is unit-testable.
pub fn resolve_is_local(transport_local: bool, override_local: Option<bool>) -> bool {
    override_local.unwrap_or(transport_local)
}

/// The `host.*` capability-probe methods, once classified. `Status` is the
/// original host probe (§5.14); `CheckGit`/`ListDirectory`/`DirectoryStatus`/
/// `CheckAuggie`/`FindBinary`/`ToolAvailability`/`Env`/`FindApp`/
/// `ListInstalledEditors` are additive host-services that let the FE delegate
/// Git detection, repo-folder browsing, auggie-binary discovery, generic binary
/// resolution, a batch tool-availability probe, the daemon's PATH/environment,
/// macOS `.app` bundle lookup, and the cross-platform editor catalog to the
/// daemon host (cross-transport, like the rest of `host.*`).
pub(crate) enum HostMethod {
    Status,
    CheckGit,
    ListDirectory,
    DirectoryStatus,
    CheckAuggie,
    FindBinary,
    ToolAvailability,
    Env,
    FindApp,
    ListInstalledEditors,
}

/// A classified `host.*` request awaiting handling by the connection task.
/// `params` is the raw params object (already coerced to an empty map when the
/// frame had no params or non-object params), consumed by the methods that
/// take input (`ListDirectory`/`DirectoryStatus`).
pub(crate) struct HostRequest {
    pub method: HostMethod,
    pub id_present: bool,
    pub id_echo: Value,
    pub params: Map<String, Value>,
}

/// Classify a parsed frame as a `host.*` request, or `None` to fall through to
/// the next fast-path / JSON-RPC dispatcher. Mirrors `control::classify`: a
/// JSON-RPC 2.0 object with a string `method` and an `id` (if present) that is
/// a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<HostRequest> {
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
        "host.status" => HostMethod::Status,
        "host.checkGit" => HostMethod::CheckGit,
        "host.listDirectory" => HostMethod::ListDirectory,
        "host.directoryStatus" => HostMethod::DirectoryStatus,
        "host.checkAuggie" => HostMethod::CheckAuggie,
        "host.findBinary" => HostMethod::FindBinary,
        "host.toolAvailability" => HostMethod::ToolAvailability,
        "host.env" => HostMethod::Env,
        "host.findApp" => HostMethod::FindApp,
        "host.listInstalledEditors" => HostMethod::ListInstalledEditors,
        _ => return None,
    };
    // `parsed.params || {}`: a non-object (absent/null/array/scalar) yields `{}`,
    // matching the events fast-path classifier so handlers run the same
    // required-param checks regardless of the wire shape.
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Some(HostRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
        params,
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

/// Handle a classified `host.*` request: build the response frame (or `None`
/// for a notification, which gets no reply). `is_local` is the resolved
/// locality of the serving connection (§5.14). The host-services methods
/// (`checkGit`/`listDirectory`/`directoryStatus`/`checkAuggie`/`findBinary`/
/// `toolAvailability`/`env`/`findApp`/`listInstalledEditors`) run their
/// filesystem / subprocess work on a blocking thread so the async runtime stays
/// free; `checkAuggie` consults `api.settings_get` for `context.auggiePath` and
/// `providers.paths.auggie` before falling back to the canonical resolver in
/// `intent_services::auggie_discovery`. `findBinary` / `findApp` require a
/// `name` param (`-32602` when absent); `env` is secret-safe (names only, no
/// values); `findApp` / `listInstalledEditors` return only app names + paths.
pub(crate) async fn handle(
    req: HostRequest,
    api: &dyn WorkspaceApi,
    is_local: bool,
) -> Option<String> {
    let HostRequest {
        method,
        id_present,
        id_echo,
        params,
    } = req;
    let frame = match method {
        HostMethod::Status => {
            let result = host_status_json(
                std::env::consts::OS,
                std::env::consts::ARCH,
                &local_hostname(),
                detect_has_display(),
                detect_display_server().as_deref(),
                is_local,
            );
            success_frame(id_echo, result)
        }
        HostMethod::CheckGit => {
            let result = tokio::task::spawn_blocking(host_ops::check_git)
                .await
                .unwrap_or_else(|_| json!({ "available": false }));
            success_frame(id_echo, result)
        }
        HostMethod::ListDirectory => {
            let path = params
                .get("path")
                .and_then(Value::as_str)
                .map(str::to_string);
            let join =
                tokio::task::spawn_blocking(move || host_ops::list_directory(path.as_deref()))
                    .await;
            match join {
                Ok(Ok(v)) => success_frame(id_echo, v),
                Ok(Err(msg)) => error_frame(id_echo, -32603, &msg),
                Err(e) => error_frame(id_echo, -32603, &format!("listDirectory join error: {e}")),
            }
        }
        HostMethod::DirectoryStatus => {
            let path = match params.get("path").and_then(Value::as_str) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        id_echo,
                        -32602,
                        "Missing required parameter: path",
                    ));
                }
            };
            let join = tokio::task::spawn_blocking(move || host_ops::directory_status(&path)).await;
            match join {
                Ok(v) => success_frame(id_echo, v),
                Err(e) => error_frame(id_echo, -32603, &format!("directoryStatus join error: {e}")),
            }
        }
        HostMethod::CheckAuggie => {
            let configured = configured_auggie_path(api).await;
            let join =
                tokio::task::spawn_blocking(move || host_ops::check_auggie(configured.as_deref()))
                    .await;
            let result = join.unwrap_or_else(|_| json!({ "available": false }));
            success_frame(id_echo, result)
        }
        HostMethod::FindBinary => {
            let name = match params.get("name").and_then(Value::as_str) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        id_echo,
                        -32602,
                        "Missing required parameter: name",
                    ));
                }
            };
            let common_paths: Vec<String> = params
                .get("commonPaths")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let result =
                tokio::task::spawn_blocking(move || host_ops::find_binary_op(&name, &common_paths))
                    .await
                    .unwrap_or_else(|_| json!({ "available": false }));
            success_frame(id_echo, result)
        }
        HostMethod::ToolAvailability => {
            let tools: Option<Vec<String>> =
                params.get("tools").and_then(Value::as_array).map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                });
            let result = tokio::task::spawn_blocking(move || host_ops::tool_availability_op(tools))
                .await
                .unwrap_or_else(|_| json!({ "tools": {} }));
            success_frame(id_echo, result)
        }
        HostMethod::Env => {
            let result = tokio::task::spawn_blocking(host_ops::env_probe)
                .await
                .unwrap_or_else(|_| json!({ "path": "", "pathEntries": [], "varNames": [] }));
            success_frame(id_echo, result)
        }
        HostMethod::FindApp => {
            let name = match params.get("name").and_then(Value::as_str) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        id_echo,
                        -32602,
                        "Missing required parameter: name",
                    ));
                }
            };
            let result = tokio::task::spawn_blocking(move || host_ops::find_app_op(&name))
                .await
                .unwrap_or_else(|_| json!({ "installed": false }));
            success_frame(id_echo, result)
        }
        HostMethod::ListInstalledEditors => {
            let result = tokio::task::spawn_blocking(host_ops::list_installed_editors_op)
                .await
                .unwrap_or_else(|_| json!({ "editors": [] }));
            success_frame(id_echo, result)
        }
    };
    if !id_present {
        return None;
    }
    Some(frame)
}

/// Read the user-configured auggie path from settings, preferring the explicit
/// `context.auggiePath` and falling back to `providers.paths.auggie`. Returns
/// `None` when neither is set (the caller then uses
/// `intent_services::auggie_discovery::find_auggie`).
async fn configured_auggie_path(api: &dyn WorkspaceApi) -> Option<String> {
    if let Some(v) = read_setting_string(api, "context.auggiePath").await {
        return Some(v);
    }
    if let Ok(payload) = api.settings_get("providers.paths".to_string()).await {
        if let Some(map) = payload.get("value").and_then(Value::as_object) {
            if let Some(s) = map.get("auggie").and_then(Value::as_str) {
                if !s.trim().is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

/// Read a single string-valued setting; returns `None` for missing / null /
/// non-string / blank values, or when the lookup itself fails.
async fn read_setting_string(api: &dyn WorkspaceApi, path: &str) -> Option<String> {
    let payload = api.settings_get(path.to_string()).await.ok()?;
    let s = payload.get("value")?.as_str()?;
    if s.trim().is_empty() {
        None
    } else {
        Some(s.to_string())
    }
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
