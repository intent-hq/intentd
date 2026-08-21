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
use std::fmt::Write as _;

use base64::Engine as _;
use intent_core::WorkspaceApi;
use intent_services::EventBus;
use serde_json::{json, Map, Value};

use crate::events::{error_frame, success_frame};
use crate::host_env::{detect_display_server, detect_has_display, local_hostname};
use crate::host_ops;
use crate::reverse::{ReverseChannel, DEFAULT_REVERSE_TIMEOUT};

/// Resolve the effective locality for a connection (§5.14): the transport
/// default (`true`/local for UDS, `false`/remote for TCP/WSS) unless forced by
/// `--mode local|remote` / the `server.locality` setting. Pure so the
/// per-transport + override matrix is unit-testable.
pub(crate) fn resolve_is_local(transport_local: bool, override_local: Option<bool>) -> bool {
    override_local.unwrap_or(transport_local)
}

/// The `host.*` capability-probe methods, once classified. `Status` is the
/// original host probe (§5.14); `CheckGit`/`CheckNode`/`CheckGh`/
/// `ListDirectory`/`CreateDirectory`/
/// `DirectoryStatus`/`CheckAuggie`/`FindBinary`/`ToolAvailability`/`Env`/
/// `FindApp`/`ListInstalledEditors` are additive host-services that let the FE
/// delegate Git/Node/gh detection, repo-folder browsing/creation, auggie-binary
/// discovery, generic binary
/// resolution, a batch tool-availability probe, the daemon's PATH/environment,
/// macOS `.app` bundle lookup, and the cross-platform editor catalog to the
/// daemon host (cross-transport, like the rest of `host.*`).
pub(crate) enum HostMethod {
    Status,
    CheckGit,
    CheckNode,
    CheckGh,
    ListDirectory,
    CreateDirectory,
    DirectoryStatus,
    CheckAuggie,
    FindBinary,
    ToolAvailability,
    Env,
    FindApp,
    ListInstalledEditors,
    ProviderDiscovery,
    /// Daemon-owned provider auth probes (`host.providerAuthStatus`, §5.14):
    /// `{ providerId?, force? }` → `{ providers: [{ id, authenticated }] }`
    /// with `authenticated: true | false | null`.
    ProviderAuthStatus,
    /// Client-callable editor-open trigger (`host.openInEditor`, §5.14):
    /// dispatched to [`open_in_editor`], which short-circuits locally on a
    /// local connection and re-dispatches to the connected FE as the
    /// FE-served reverse RPC on a remote one.
    OpenInEditor,
    Exec,
    /// Streaming/interactive exec surface (`host.execStream`, §5.14): returns
    /// `{ requestId }` immediately, then streams `host:exec:*` bus frames.
    ExecStream,
    /// Follow-up stdin write to a live `host.execStream` (`{ requestId, stdin?,
    /// stdinBase64?, eof? }` → `{ ok: true }`).
    ExecStreamWrite,
    /// Cancellation for a live `host.execStream` (`{ requestId }` →
    /// `{ ok: true, cancelled: bool }`); idempotent on unknown ids.
    ExecStreamCancel,
}

/// A classified `host.*` request awaiting handling by the connection task.
/// `params` is the raw params object (already coerced to an empty map when the
/// frame had no params or non-object params), consumed by the methods that
/// take input (`ListDirectory`/`CreateDirectory`/`DirectoryStatus`).
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
        "host.checkNode" => HostMethod::CheckNode,
        "host.checkGh" => HostMethod::CheckGh,
        "host.listDirectory" => HostMethod::ListDirectory,
        "host.createDirectory" => HostMethod::CreateDirectory,
        "host.directoryStatus" => HostMethod::DirectoryStatus,
        "host.checkAuggie" => HostMethod::CheckAuggie,
        "host.findBinary" => HostMethod::FindBinary,
        "host.toolAvailability" => HostMethod::ToolAvailability,
        "host.env" => HostMethod::Env,
        "host.findApp" => HostMethod::FindApp,
        "host.listInstalledEditors" => HostMethod::ListInstalledEditors,
        "host.providerDiscovery" => HostMethod::ProviderDiscovery,
        "host.providerAuthStatus" => HostMethod::ProviderAuthStatus,
        "host.openInEditor" => HostMethod::OpenInEditor,
        "host.exec" => HostMethod::Exec,
        "host.execStream" => HostMethod::ExecStream,
        "host.execStream.write" => HostMethod::ExecStreamWrite,
        "host.execStream.cancel" => HostMethod::ExecStreamCancel,
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
/// (`checkGit`/`checkNode`/`checkGh`/`listDirectory`/`createDirectory`/
/// `directoryStatus`/
/// `checkAuggie`/`findBinary`/`toolAvailability`/`env`/`findApp`/
/// `listInstalledEditors`) run their
/// filesystem / subprocess work on a blocking thread so the async runtime stays
/// free; `checkAuggie` consults `api.settings_get` for `context.auggiePath` and
/// `providers.paths.auggie` before falling back to the canonical resolver in
/// `intent_services::auggie_discovery`, and is resolution-only (`{ available,
/// path? }` — no `--version` spawn). `findBinary` / `findApp` require a
/// `name` param (`-32602` when absent); `env` is secret-safe (names only, no
/// values); `findApp` / `listInstalledEditors` return only app names + paths.
/// `reverse` is the connection's reverse-RPC channel, consumed by the
/// client-called `openInEditor` trigger on a remote connection.
pub(crate) async fn handle(
    req: HostRequest,
    api: &dyn WorkspaceApi,
    bus: Option<&EventBus>,
    is_local: bool,
    reverse: &ReverseChannel,
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
            success_frame(&id_echo, &result)
        }
        HostMethod::CheckGit => {
            let result = tokio::task::spawn_blocking(host_ops::check_git)
                .await
                .unwrap_or_else(|_| json!({ "available": false }));
            success_frame(&id_echo, &result)
        }
        HostMethod::CheckNode => {
            let result = tokio::task::spawn_blocking(host_ops::check_node)
                .await
                .unwrap_or_else(|_| json!({ "available": false }));
            success_frame(&id_echo, &result)
        }
        HostMethod::CheckGh => {
            let result = tokio::task::spawn_blocking(host_ops::check_gh)
                .await
                .unwrap_or_else(|_| json!({ "available": false }));
            success_frame(&id_echo, &result)
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
                Ok(Ok(v)) => success_frame(&id_echo, &v),
                Ok(Err(msg)) => error_frame(&id_echo, -32603, &msg),
                Err(e) => error_frame(&id_echo, -32603, &format!("listDirectory join error: {e}")),
            }
        }
        HostMethod::CreateDirectory => {
            let path = match params.get("path").and_then(Value::as_str) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Missing required parameter: path",
                    ));
                }
            };
            let join = tokio::task::spawn_blocking(move || host_ops::create_directory(&path)).await;
            match join {
                Ok(Ok(v)) => success_frame(&id_echo, &v),
                Ok(Err(msg)) => error_frame(&id_echo, -32603, &msg),
                Err(e) => error_frame(
                    &id_echo,
                    -32603,
                    &format!("createDirectory join error: {e}"),
                ),
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
                        &id_echo,
                        -32602,
                        "Missing required parameter: path",
                    ));
                }
            };
            let join = tokio::task::spawn_blocking(move || host_ops::directory_status(&path)).await;
            match join {
                Ok(v) => success_frame(&id_echo, &v),
                Err(e) => error_frame(
                    &id_echo,
                    -32603,
                    &format!("directoryStatus join error: {e}"),
                ),
            }
        }
        HostMethod::CheckAuggie => {
            let configured = configured_auggie_path(api).await;
            let join =
                tokio::task::spawn_blocking(move || host_ops::check_auggie(configured.as_deref()))
                    .await;
            let result = join.unwrap_or_else(|_| json!({ "available": false }));
            success_frame(&id_echo, &result)
        }
        HostMethod::FindBinary => {
            let name = match params.get("name").and_then(Value::as_str) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
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
            success_frame(&id_echo, &result)
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
            success_frame(&id_echo, &result)
        }
        HostMethod::ProviderDiscovery => {
            // `providers.paths` overrides live in settings, above the
            // discovery seam — read them here so `installed` /
            // `secondaryResolved` match what the spawn path would actually
            // resolve (monorepo#1065). resolvedPath/secondaryResolvedPath
            // stay auto-detected.
            let provider_paths = read_provider_paths(api).await;
            let result = tokio::task::spawn_blocking(move || {
                host_ops::provider_discovery_op(&provider_paths)
            })
            .await
            .unwrap_or_else(|_| {
                json!({ "providers": [], "npx": { "resolvedPath": null, "version": null, "versionOk": false } })
            });
            // Default-provider self-heal (monorepo#3044): with the discovery
            // verdicts in hand, backfill unset default provider/model
            // settings from the installed set. Idempotent and no-overwrite;
            // best-effort — a heal failure never fails the discovery RPC.
            let installed: Vec<String> = result["providers"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter(|p| p["installed"].as_bool() == Some(true))
                        .filter_map(|p| p["id"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if !installed.is_empty() {
                if let Err(e) = api.settings_heal_default_provider(installed).await {
                    tracing::warn!(error = %e, "default-provider settings self-heal failed");
                }
            }
            success_frame(&id_echo, &result)
        }
        HostMethod::ProviderAuthStatus => {
            let provider_id = match params.get("providerId") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                Some(_) => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Invalid parameter: providerId must be a non-empty string",
                    ));
                }
            };
            let force = match params.get("force") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Invalid parameter: force must be a boolean",
                    ));
                }
            };
            // Same settings seam as `ProviderDiscovery` (monorepo#1065): the
            // install gate must honor `providers.paths` overrides so a
            // provider reachable only via a valid override is still probed
            // (monorepo#1086). auggie follows the `host.checkAuggie`
            // precedence: `context.auggiePath` wins over
            // `providers.paths.auggie`.
            let mut provider_paths = read_provider_paths(api).await;
            if let Some(p) = read_setting_string(api, "context.auggiePath").await {
                provider_paths.insert("auggie".to_string(), p);
            }
            match intent_services::provider_auth::provider_auth_status(
                provider_id.as_deref(),
                force,
                &provider_paths,
            )
            .await
            {
                Ok(result) => success_frame(&id_echo, &result),
                Err(msg) => error_frame(&id_echo, -32602, &msg),
            }
        }
        HostMethod::Env => {
            let result = tokio::task::spawn_blocking(host_ops::env_probe)
                .await
                .unwrap_or_else(|_| json!({ "path": "", "pathEntries": [], "varNames": [] }));
            success_frame(&id_echo, &result)
        }
        HostMethod::FindApp => {
            let name = match params.get("name").and_then(Value::as_str) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Missing required parameter: name",
                    ));
                }
            };
            let result = tokio::task::spawn_blocking(move || host_ops::find_app_op(&name))
                .await
                .unwrap_or_else(|_| json!({ "installed": false }));
            success_frame(&id_echo, &result)
        }
        HostMethod::ListInstalledEditors => {
            let result = tokio::task::spawn_blocking(host_ops::list_installed_editors_op)
                .await
                .unwrap_or_else(|_| json!({ "editors": [] }));
            success_frame(&id_echo, &result)
        }
        HostMethod::OpenInEditor => {
            let editor_id = params
                .get("editorId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let path = params
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let line = params
                .get("line")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok());
            let column = params
                .get("column")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok());
            // The platform editor catalog only backs the local short-circuit;
            // the remote path forwards the intent to the FE untouched.
            let editors = if is_local {
                tokio::task::spawn_blocking(host_ops::list_installed_editors_op)
                    .await
                    .unwrap_or_else(|_| json!({ "editors": [] }))
            } else {
                json!({ "editors": [] })
            };
            match open_in_editor(
                &editor_id,
                &path,
                line,
                column,
                is_local,
                detect_has_display(),
                &editors,
                &OsEditorLauncher,
                reverse,
            )
            .await
            {
                Ok(()) => success_frame(&id_echo, &json!({ "ok": true })),
                Err(e) => error_frame(&id_echo, e.code(), &e.to_string()),
            }
        }
        HostMethod::Exec => {
            let parsed = match intent_services::host_exec::parse_args(&params) {
                Ok(a) => a,
                Err(e) => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(&id_echo, e.code, &e.message));
                }
            };
            match intent_services::host_exec::run_default(api, parsed).await {
                Ok(v) => success_frame(&id_echo, &v),
                Err(e) => error_frame(&id_echo, e.code, &e.message),
            }
        }
        HostMethod::ExecStream => {
            let bus = if let Some(b) = bus {
                b.clone()
            } else {
                if !id_present {
                    return None;
                }
                return Some(error_frame(
                    &id_echo,
                    -32603,
                    "host.execStream requires an event bus",
                ));
            };
            let parsed = match intent_services::host_exec_stream::parse_args(&params) {
                Ok(a) => a,
                Err(e) => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(&id_echo, e.code, &e.message));
                }
            };
            match intent_services::host_exec_stream::start_stream(api, bus, parsed).await {
                Ok(request_id) => success_frame(&id_echo, &json!({ "requestId": request_id })),
                Err(e) => error_frame(&id_echo, e.code, &e.message),
            }
        }
        HostMethod::ExecStreamWrite => {
            let request_id = match params.get("requestId").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Missing required parameter: requestId",
                    ));
                }
            };
            let data = match parse_write_stdin(&params) {
                Ok(v) => v,
                Err(msg) => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(&id_echo, -32602, &msg));
                }
            };
            let eof = params.get("eof").and_then(Value::as_bool).unwrap_or(false);
            match intent_services::host_exec_stream::registry()
                .write(&request_id, data, eof)
                .await
            {
                Ok(()) => success_frame(&id_echo, &json!({ "ok": true })),
                Err(e) => error_frame(&id_echo, e.code, &e.message),
            }
        }
        HostMethod::ExecStreamCancel => {
            let request_id = match params.get("requestId").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    if !id_present {
                        return None;
                    }
                    return Some(error_frame(
                        &id_echo,
                        -32602,
                        "Missing required parameter: requestId",
                    ));
                }
            };
            let cancelled = intent_services::host_exec_stream::registry().cancel(&request_id);
            success_frame(&id_echo, &json!({ "ok": true, "cancelled": cancelled }))
        }
    };
    if !id_present {
        return None;
    }
    Some(frame)
}

/// Extract the optional stdin payload from a `host.execStream.write` request.
/// Accepts either a plain UTF-8 `stdin` string or a base64 `stdinBase64` blob;
/// returns `None` when neither is supplied, `Err(message)` on a type error or
/// invalid base64 (mapped to `-32602` by the caller).
fn parse_write_stdin(params: &Map<String, Value>) -> Result<Option<Vec<u8>>, String> {
    match (params.get("stdin"), params.get("stdinBase64")) {
        (None | Some(Value::Null), None | Some(Value::Null)) => Ok(None),
        (Some(text), None | Some(Value::Null)) => match text {
            Value::String(s) => Ok(Some(s.as_bytes().to_vec())),
            _ => Err("Invalid parameter: stdin must be a string".to_string()),
        },
        (None | Some(Value::Null), Some(b64)) => match b64 {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map(Some)
                .map_err(|_| "Invalid parameter: stdinBase64 is not valid base64".to_string()),
            _ => Err("Invalid parameter: stdinBase64 must be a string".to_string()),
        },
        (Some(_), Some(_)) => {
            Err("Invalid parameter: pass either stdin or stdinBase64, not both".to_string())
        }
    }
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

/// Read the full `providers.paths` settings map (provider key → configured
/// binary path), skipping blank values. Empty when unset or when the lookup
/// fails — discovery then behaves exactly as before (auto-detection only).
async fn read_provider_paths(api: &dyn WorkspaceApi) -> std::collections::HashMap<String, String> {
    let mut paths = std::collections::HashMap::new();
    if let Ok(payload) = api.settings_get("providers.paths".to_string()).await {
        if let Some(map) = payload.get("value").and_then(Value::as_object) {
            for (key, value) in map {
                if let Some(s) = value.as_str() {
                    if !s.trim().is_empty() {
                        paths.insert(key.clone(), s.to_string());
                    }
                }
            }
        }
    }
    paths
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
#[cfg(test)]
pub(crate) trait ExternalOpener: Send + Sync {
    /// Open `url` with the host's default handler. Returns a descriptive error
    /// when the platform opener cannot be spawned.
    fn open(&self, url: &str) -> Result<(), String>;
}

/// Why a [`open_external`] call could not be satisfied. `code()` maps each to a
/// standard JSON-RPC error code (PROTOCOL §9: no custom codes — server-side
/// conditions are `-32602`/`-32603` with a descriptive message).
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum OpenExternalError {
    /// `hasDisplay=false` on the daemon host and the op needs a display (§12.4).
    Headless(String),
    /// The host OS opener failed (the local path).
    Opener(String),
    /// The FE-served reverse RPC failed/timed out (the remote path).
    Proxy(String),
    /// The `url` parameter was missing or empty.
    InvalidUrl(String),
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
impl std::error::Error for OpenExternalError {}

/// Open `url` on the *user's* machine (§5.14; the daemon→client reverse RPC
/// itself is `host.openExternal`, §12.4 — see `reverse.rs`). When the
/// connection is local the daemon resolves it directly via `opener`; if the
/// local host is headless (`has_display=false`) this is a clear headless
/// warning instead of a silent failure. When the connection is remote the
/// intent is dispatched to the connected frontend as an FE-served reverse RPC
/// (`host.openExternal`) so it opens on the user's machine (mirroring the
/// ACP client-served pattern, §6.7).
#[cfg(test)]
pub(crate) async fn open_external(
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

/// Where the editor should open. Line/column are 1-based hints (best-effort in
/// the local launch path; forwarded verbatim on the reverse RPC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EditorTarget {
    pub path: String,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

/// Resolved detection metadata for a known editor entry, mirroring the
/// per-entry payload emitted by `host.listInstalledEditors`. Consumed by
/// [`EditorLauncher::launch`] so the launcher can pick the right spawn strategy
/// (native binary vs macOS `.app` bundle vs flatpak) without re-doing the
/// resolution work.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedEditor {
    pub id: String,
    pub installed: bool,
    pub path: Option<String>,
    pub source: Option<String>,
    pub flatpak_id: Option<String>,
}

impl ResolvedEditor {
    /// Locate `editor_id` in the `host.listInstalledEditors` payload. Returns
    /// `None` when the id is unknown to the current platform's catalog.
    pub(crate) fn from_editors_payload(editor_id: &str, payload: &Value) -> Option<Self> {
        let entries = payload.get("editors")?.as_array()?;
        entries
            .iter()
            .find(|e| e.get("id").and_then(Value::as_str) == Some(editor_id))
            .map(|e| ResolvedEditor {
                id: editor_id.to_string(),
                installed: e.get("installed").and_then(Value::as_bool).unwrap_or(false),
                path: e.get("path").and_then(Value::as_str).map(str::to_string),
                source: e.get("source").and_then(Value::as_str).map(str::to_string),
                flatpak_id: e
                    .get("flatpakId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
    }
}

/// Launch a resolved editor on the daemon host with the requested target.
/// Injected so the local path is unit-testable without spawning a real editor.
pub(crate) trait EditorLauncher: Send + Sync {
    /// Launch `editor` on `target`. Returns a descriptive error when the
    /// platform launch cannot be spawned.
    fn launch(&self, editor: &ResolvedEditor, target: &EditorTarget) -> Result<(), String>;
}

/// Default editor launcher: mirrors [`OsOpener`], spawning the resolved editor
/// detached from this process's stdio. The launcher picks the right invocation
/// per `source`: a native binary path, a macOS `.app` bundle via `open -a`, or
/// a flatpak application id via `flatpak run`. Line/column are best-effort and
/// only honored for editors that accept a `--goto <file>:<line>[:<col>]` arg;
/// unknown editors receive just the target path.
pub(crate) struct OsEditorLauncher;

impl EditorLauncher for OsEditorLauncher {
    fn launch(&self, editor: &ResolvedEditor, target: &EditorTarget) -> Result<(), String> {
        if !editor.installed {
            return Err(format!("editor '{}' is not installed", editor.id));
        }
        let goto_id = matches!(editor.id.as_str(), "vscode" | "cursor");
        let mut cmd = if let Some(flatpak_id) = editor.flatpak_id.as_deref() {
            let mut c = std::process::Command::new("flatpak");
            c.args(["run", flatpak_id]);
            append_editor_args(&mut c, goto_id, target);
            c
        } else if editor.source.as_deref() == Some("macAppBundle") {
            let app_path = editor
                .path
                .as_deref()
                .ok_or_else(|| format!("editor '{}' is missing an app bundle path", editor.id))?;
            let mut c = std::process::Command::new("open");
            c.args(["-a", app_path, &target.path]);
            c
        } else {
            let bin = editor
                .path
                .as_deref()
                .ok_or_else(|| format!("editor '{}' is missing a launch path", editor.id))?;
            let mut c = std::process::Command::new(bin);
            append_editor_args(&mut c, goto_id, target);
            c
        };
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        cmd.spawn()
            .map(|_| ())
            .map_err(|e| format!("open in editor failed: {e}"))
    }
}

/// Append editor-specific target args. When `goto` is set (VS Code / Cursor)
/// and a line hint is present, use `--goto path:line[:col]`; otherwise pass the
/// target path verbatim.
fn append_editor_args(cmd: &mut std::process::Command, goto: bool, target: &EditorTarget) {
    if goto {
        if let Some(line) = target.line {
            let mut spec = format!("{}:{}", target.path, line);
            if let Some(col) = target.column {
                let _ = write!(spec, ":{col}");
            }
            cmd.args(["--goto", &spec]);
            return;
        }
    }
    cmd.arg(&target.path);
}

/// Why an [`open_in_editor`] call could not be satisfied. `code()` maps each to
/// a standard JSON-RPC error code (PROTOCOL §9: no custom codes — server-side
/// conditions are `-32602`/`-32603` with a descriptive message).
#[derive(Debug)]
pub(crate) enum OpenInEditorError {
    /// `editorId` / `path` was missing or empty, or the editor id is unknown to
    /// the current platform's `host.listInstalledEditors` catalog.
    InvalidParams(String),
    /// The editor is known but not installed on the daemon host (`installed:false`).
    NotInstalled(String),
    /// `hasDisplay=false` on the daemon host and the launch needs a display.
    Headless(String),
    /// The host launcher failed (the local path).
    Launcher(String),
    /// The FE-served reverse RPC failed / timed out (the remote path).
    Proxy(String),
}

impl OpenInEditorError {
    /// JSON-RPC 2.0 numeric error code for this condition (PROTOCOL §9).
    pub fn code(&self) -> i32 {
        match self {
            OpenInEditorError::InvalidParams(_) => -32602,
            OpenInEditorError::NotInstalled(_)
            | OpenInEditorError::Headless(_)
            | OpenInEditorError::Launcher(_)
            | OpenInEditorError::Proxy(_) => -32603,
        }
    }
}

impl fmt::Display for OpenInEditorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenInEditorError::InvalidParams(m)
            | OpenInEditorError::NotInstalled(m)
            | OpenInEditorError::Headless(m)
            | OpenInEditorError::Launcher(m)
            | OpenInEditorError::Proxy(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for OpenInEditorError {}

/// Open `path` in `editor_id` on the *user's* machine (§5.14). On a local
/// connection the daemon resolves the editor against the current platform's
/// `host.listInstalledEditors` catalog (`editors` — the raw payload from
/// [`host_ops::list_installed_editors_op`]) and launches it directly via
/// `launcher`; a headless local host returns a clear warning instead of a
/// silent failure. On a remote connection the intent is dispatched to the
/// connected frontend as an FE-served reverse RPC (`host.openInEditor`) so the
/// editor opens on the user's laptop (mirroring `host.openExternal`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_in_editor(
    editor_id: &str,
    path: &str,
    line: Option<u32>,
    column: Option<u32>,
    is_local: bool,
    has_display: bool,
    editors: &Value,
    launcher: &dyn EditorLauncher,
    reverse: &ReverseChannel,
) -> Result<(), OpenInEditorError> {
    if editor_id.is_empty() {
        return Err(OpenInEditorError::InvalidParams(
            "Missing required parameter: editorId".to_string(),
        ));
    }
    if path.is_empty() {
        return Err(OpenInEditorError::InvalidParams(
            "Missing required parameter: path".to_string(),
        ));
    }
    if is_local {
        if !has_display {
            return Err(OpenInEditorError::Headless(format!(
                "host is headless (hasDisplay=false); cannot launch '{editor_id}' on the daemon host — connect a client with a display"
            )));
        }
        let resolved =
            ResolvedEditor::from_editors_payload(editor_id, editors).ok_or_else(|| {
                OpenInEditorError::InvalidParams(format!("unknown editorId: {editor_id}"))
            })?;
        if !resolved.installed {
            return Err(OpenInEditorError::NotInstalled(format!(
                "editor '{editor_id}' is not installed on the daemon host"
            )));
        }
        let target = EditorTarget {
            path: path.to_string(),
            line,
            column,
        };
        return launcher
            .launch(&resolved, &target)
            .map_err(OpenInEditorError::Launcher);
    }
    let mut params = Map::new();
    params.insert("editorId".to_string(), json!(editor_id));
    params.insert("path".to_string(), json!(path));
    if let Some(l) = line {
        params.insert("line".to_string(), json!(l));
    }
    if let Some(c) = column {
        params.insert("column".to_string(), json!(c));
    }
    reverse
        .request(
            "host.openInEditor",
            Value::Object(params),
            DEFAULT_REVERSE_TIMEOUT,
        )
        .await
        .map(|_| ())
        .map_err(|e| OpenInEditorError::Proxy(e.message))
}

/// Present an "open with…" application chooser for `path` to the user.
/// Injected so the local path is unit-testable without launching a real OS
/// chooser dialog.
#[cfg(test)]
pub(crate) trait AppPicker: Send + Sync {
    /// Prompt the user for the application to open `path` with. Returns
    /// `Ok(Some(applicationId))` on selection, `Ok(None)` when the user
    /// cancelled / the daemon host has no local chooser, or `Err(message)`
    /// when the picker itself failed.
    fn pick(&self, path: &str) -> Result<Option<String>, String>;
}

/// Default local app picker: returns `Ok(None)` because the daemon has no
/// display-less way to present an "open with…" dialog. On a local connection
/// with a display, callers can inject a native picker; otherwise clients
/// gate the UI on `host.status.hasDisplay` and let the FE-served reverse RPC
/// (`host.pickApplication`) present the chooser on the user's machine.
#[cfg(test)]
pub(crate) struct NoopAppPicker;

#[cfg(test)]
impl AppPicker for NoopAppPicker {
    fn pick(&self, _path: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// Why a [`pick_application`] call could not be satisfied. `code()` maps each
/// to a standard JSON-RPC error code (PROTOCOL §9).
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum PickApplicationError {
    /// `path` was missing or empty.
    InvalidPath(String),
    /// The host picker failed (the local path).
    Picker(String),
    /// The FE-served reverse RPC failed / timed out (the remote path).
    Proxy(String),
}

#[cfg(test)]
impl PickApplicationError {
    /// JSON-RPC 2.0 numeric error code for this condition (PROTOCOL §9).
    pub fn code(&self) -> i32 {
        match self {
            PickApplicationError::InvalidPath(_) => -32602,
            PickApplicationError::Picker(_) | PickApplicationError::Proxy(_) => -32603,
        }
    }
}

#[cfg(test)]
impl fmt::Display for PickApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PickApplicationError::InvalidPath(m)
            | PickApplicationError::Picker(m)
            | PickApplicationError::Proxy(m) => f.write_str(m),
        }
    }
}

#[cfg(test)]
impl std::error::Error for PickApplicationError {}

/// Present the "open with…" chooser for `path` on the *user's* machine
/// (§5.14). On a local connection the daemon invokes `picker` directly; the
/// default [`NoopAppPicker`] returns `Ok(None)` because a display-less daemon
/// cannot show a native chooser. On a remote connection the intent is
/// dispatched to the connected frontend as an FE-served reverse RPC
/// (`host.pickApplication`) so the chooser appears on the user's laptop; the
/// client's `{ applicationId? }` reply is echoed back verbatim.
#[cfg(test)]
pub(crate) async fn pick_application(
    path: &str,
    is_local: bool,
    picker: &dyn AppPicker,
    reverse: &ReverseChannel,
) -> Result<Option<String>, PickApplicationError> {
    if path.is_empty() {
        return Err(PickApplicationError::InvalidPath(
            "Missing required parameter: path".to_string(),
        ));
    }
    if is_local {
        return picker.pick(path).map_err(PickApplicationError::Picker);
    }
    let result = reverse
        .request(
            "host.pickApplication",
            json!({ "path": path }),
            DEFAULT_REVERSE_TIMEOUT,
        )
        .await
        .map_err(|e| PickApplicationError::Proxy(e.message))?;
    let app_id = result
        .get("applicationId")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(app_id)
}

#[cfg(test)]
mod tests;
