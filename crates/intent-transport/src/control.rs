//! System control fast-path: `system.status`, `system.shutdown`,
//! `system.importLegacy` (§5.7), `system.gitCredential` (monorepo#884), and
//! `system.requestUpdate` (§5.7).
//!
//! These control methods surface live daemon state, request graceful shutdown,
//! and run legacy import. They sit above the domain [`WorkspaceApi`] router because
//! the data they expose (bound port, connected clients, active agents, the TLS
//! fingerprint) and the action they take (tear down the listener) are
//! transport/process concerns, not domain operations. The composition root
//! (`intentd`) implements [`SystemControl`]; each listener intercepts these
//! methods before the JSON-RPC dispatcher, mirroring the `events.` fast-path.

use std::future::Future;
use std::pin::Pin;

use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};
use crate::protocol::PROTOCOL_VERSION;

/// A point-in-time snapshot of daemon state for `system.status` (§5.7, §12.3).
/// `locality` is derived per-connection (UDS ⇒ `local`, WSS ⇒ `remote`) and so
/// is not stored here; it is applied when the snapshot is rendered to JSON.
// The bools (`uds`, `tcp`, `has_display`, `update_supported`) are independent
// wire-facing status flags, not an encoded state machine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct SystemStatus {
    /// Derived listen mode: `both` while the TCP listener (secure WSS, or
    /// plain-ws under `--insecure`) is up, else `uds` (the UDS listener
    /// always serves).
    pub listen_mode: String,
    /// Whether the UDS listener is active (always true in practice).
    pub uds: bool,
    /// Whether the TCP/WSS listener is currently active (runtime state, not a
    /// boot-time flag).
    pub tcp: bool,
    /// The bound TCP listener port (secure WSS, or plain-ws under
    /// `--insecure`), when the listener is running.
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
    /// The `AgentManager` concurrency cap (resolved maxConcurrent incl. auto-detection).
    pub max_agents: usize,
    /// The daemon crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Source commit embedded at build time. `None` when the build environment
    /// cannot identify a commit (for example, a source archive without Git metadata).
    pub build_commit: Option<String>,
    /// Uptime in seconds since daemon start.
    pub uptime_seconds: u64,
    /// Non-loopback IPv4 addresses the host is reachable on (same source as
    /// `server.pairingInfo` — `collect_local_ips`), so an authenticated remote
    /// client can discover alternative routes to the daemon.
    pub local_ips: Vec<String>,
    /// The tailcat tunnel's stable `tc...` address (`server.tunnel.*`), when
    /// the sidecar is running — served alongside `localIps` so connected
    /// clients can refresh their stored tunnel route from `system.status`
    /// alone. Presence-detected on the wire: omitted when `None`, never null.
    pub tc_address: Option<String>,
    /// Local OS hostname (same source as `server.pairingInfo`).
    pub hostname: String,
    /// OS "pretty" device name (macOS Computer Name), falling back to the
    /// hostname when unavailable (same source as `server.pairingInfo` /
    /// `host.status`).
    pub pretty_hostname: String,
    /// Detected device category, omitted from the host block when unknown.
    pub device_kind: Option<String>,
    /// Raw hardware product/model name, omitted from the host block when unknown.
    pub hardware_model: Option<String>,
    /// CPU usage of the daemon process, raw `sysinfo` convention: 100 = one
    /// full core, so values may exceed 100 on multi-core hosts. The first
    /// sample after startup may legitimately read 0.
    pub cpu_percent: f32,
    /// Resident memory of the daemon process, in bytes.
    pub memory_bytes: u64,
    /// Number of OS processes in the daemon's descendant tree — every agent
    /// provider CLI plus whatever it spawns (ACP adapters, MCP bridges, the
    /// tool children an agent runs), the Unsloth server, and `host.exec`
    /// children. `None` until the background sampler has taken its first
    /// sample, or on platforms where the tree cannot be walked.
    pub child_processes: Option<usize>,
    /// Aggregate resident memory of [`Self::child_processes`], in bytes. This
    /// is the daemon's *real* memory footprint from the OS's point of view —
    /// [`Self::memory_bytes`] covers only the daemon binary itself, which is a
    /// small fraction of it once agents are live. `None` alongside
    /// `child_processes`.
    pub child_memory_bytes: Option<u64>,
    /// High-water mark of the daemon's descendant-tree memory since daemon
    /// start. The instantaneous value is not enough on its own: quick-action
    /// and model-probe adapters live for seconds, and by the time a debug
    /// bundle is captured any overshoot has drained back to baseline. `None`
    /// alongside `child_processes`.
    ///
    /// Sampled more often than [`Self::child_memory_bytes`], so it can exceed
    /// any value that field ever published: the tree is swept every 500 ms
    /// while an ephemeral adapter chain is live, against the 5 s baseline the
    /// published sample keeps (monorepo#2107). Bursts are short and steep
    /// enough that the baseline alone missed them almost entirely — measured,
    /// a 16-chain burst peaking at 6.97 GB reported 0.01 GB.
    pub child_memory_peak_bytes: Option<u64>,
    /// The installed aggregate agent memory budget in bytes
    /// (`agents.memoryBudgetMb`, monorepo#2063). `None` when the budget is
    /// off. The three budget fields are presence-detected on the wire —
    /// omitted when `None`, never null — unlike the child-tree sample fields
    /// above, which are null until the first sample.
    pub agent_memory_budget_bytes: Option<u64>,
    /// The bytes admission actually compares against the budget: the last
    /// descendant-tree sample plus the provisional correction for spawns
    /// admitted / processes released since it was taken. `None` when the
    /// budget is off, and also while the budget is on but no sample has
    /// landed yet (the budget is inert until then).
    pub agent_memory_charged_bytes: Option<u64>,
    /// Spawns currently queued behind the admission gate (slot cap or memory
    /// budget). `None` when the budget is off.
    pub queued_spawns: Option<u64>,
    /// Available bytes on the volume containing the daemon's resolved
    /// workspaces root. `None` until the background disk sampler lands its
    /// first sample, or when no mounted volume matches the root (e.g. an
    /// empty disks list in a locked-down container, or an unmounted drive
    /// letter on Windows — a merely not-yet-created root still matches its
    /// would-be volume). Presence-detected on the wire — omitted when `None`,
    /// never null or 0.
    pub workspaces_disk_available_bytes: Option<u64>,
    /// Total bytes of the volume containing the workspaces root. `None`
    /// alongside `workspaces_disk_available_bytes`.
    pub workspaces_disk_total_bytes: Option<u64>,
    /// File-watch coverage snapshot (intent-hq/intent#3708): `None` until the
    /// backgrounded watcher registry has started (and again after it is torn
    /// down), so the whole `fileWatch` object is presence-detected on the
    /// wire — absent when `None`, never null.
    pub file_watch: Option<FileWatchStatus>,
    /// Open file descriptors held by the daemon process, from the background
    /// own-process sampler (intent-hq/intent#4390). `None` until the first
    /// sample lands or where the count is unavailable (non-Linux/macOS).
    /// Presence-detected on the wire — omitted when `None`, never null.
    pub fd_count: Option<u64>,
    /// Soft `RLIMIT_NOFILE` in effect after the startup raise. `None` where
    /// the limit could not be read (non-Unix). Presence-detected on the wire.
    pub fd_limit: Option<u64>,
    /// Whether `system.requestUpdate` can currently succeed
    /// (intent-hq/intent#3875): true exactly when the daemon is
    /// sitter-supervised, per the same pidfile + parent/name verification
    /// that method performs — evaluated signal-free at read time. Always
    /// `false` on platforms without unix signals, where
    /// `system.requestUpdate` is unsupported.
    pub update_supported: bool,
}

/// Live file-watch coverage for `system.status` (intent-hq/intent#3708):
/// whether the roots the daemon *wants* watched are actually registered with
/// the OS. `failed_roots > 0` means lost coverage — file events under those
/// roots are silently missed until a retry recovers them (e.g. inotify
/// instance exhaustion, `fseventsd` load).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileWatchStatus {
    /// Shared OS watch streams whose watcher is actually created (one `notify`
    /// watcher each). A stream stuck retrying watcher creation is not counted,
    /// so this reads 0 while `total_roots > 0` under creation failure.
    pub active_streams: usize,
    /// Watch roots currently requested, whatever their registration state.
    pub total_roots: usize,
    /// Roots whose OS registration failed; 0 when coverage is healthy.
    pub failed_roots: usize,
}

/// A `(username, password)` pair resolved for `system.gitCredential`.
pub type GitCredential = (String, String);

/// Live daemon control surface implemented by the composition root (`intentd`).
/// Lets a listener answer `system.status` from real state and trigger a
/// graceful shutdown for `system.shutdown` without reaching into domain code.
pub trait SystemControl: Send + Sync {
    /// Snapshot the current daemon state.
    fn status(&self) -> SystemStatus;
    /// Cached host identity, refreshed by the composition root off the RPC path.
    fn host_environment(&self) -> crate::host_env::HostEnvironment;
    /// Request a graceful shutdown (idempotent). Returns immediately; the daemon
    /// tears the listeners down asynchronously.
    fn request_shutdown(&self);
    /// Ask the supervising `intentd-sitter` to run an update check now
    /// (`system.requestUpdate`): locate the sitter pidfile, verify the pid is
    /// live, and send it SIGUSR1.
    ///
    /// # Errors
    ///
    /// Returns a human-readable reason when the daemon is not
    /// sitter-supervised (or signaling is unsupported on this platform);
    /// the handler maps it to `-32603`.
    fn request_update(&self) -> Result<(), String>;
    /// Import legacy workspaces into the daemon's live store.
    fn import_legacy(
        &self,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>>;
    /// Resolve the daemon-managed GitHub credential for the git-credential
    /// helper (monorepo#884): `Some((username, password))` when the
    /// `exposeGitCredentialToChildren` setting is on and a usable token
    /// resolves, else `None`. `client_pid` is the helper's self-reported pid,
    /// used only for audit logging — implementations must never log the token.
    fn git_credential(
        &self,
        client_pid: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Option<GitCredential>> + Send + '_>>;
}

/// The control methods, once classified.
pub(crate) enum SystemMethod {
    Status,
    Shutdown,
    RequestUpdate,
    ImportLegacy {
        force: Result<bool, ()>,
    },
    GitCredential {
        pid: Option<u64>,
        protocol: Option<String>,
        host: Option<String>,
    },
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
        "system.requestUpdate" => SystemMethod::RequestUpdate,
        "system.importLegacy" => {
            let force = match obj.get("params") {
                None | Some(Value::Null) => Ok(false),
                Some(Value::Object(params)) => match params.get("force") {
                    None | Some(Value::Null) => Ok(false),
                    Some(Value::Bool(force)) => Ok(*force),
                    Some(_) => Err(()),
                },
                Some(_) => Err(()),
            };
            SystemMethod::ImportLegacy { force }
        }
        "system.gitCredential" => {
            // Lenient pid extraction: it is audit-only metadata, so a missing
            // or non-numeric value degrades to `None` rather than erroring.
            // `protocol`/`host` feed the server-side scope gate in `handle`;
            // a missing or non-string value simply fails that gate.
            let params = obj.get("params").and_then(Value::as_object);
            let pid = params.and_then(|p| p.get("pid")).and_then(Value::as_u64);
            let text = |key: &str| {
                params
                    .and_then(|p| p.get(key))
                    .and_then(Value::as_str)
                    .map(String::from)
            };
            SystemMethod::GitCredential {
                pid,
                protocol: text("protocol"),
                host: text("host"),
            }
        }
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
///
/// The aggregate-budget fields (`agentMemoryBudgetBytes`,
/// `agentMemoryChargedBytes`, `queuedSpawns`; monorepo#2063) are
/// presence-detected: omitted when the budget is off, never null — unlike the
/// child-tree sample fields, which are always present and null until sampled.
pub(crate) fn status_json(status: &SystemStatus, is_local: bool) -> Value {
    let mut transports = Vec::new();
    if status.uds {
        transports.push("uds");
    }
    if status.tcp {
        transports.push("tcp");
    }
    let mut v = json!({
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
        "childProcesses": status.child_processes,
        "childMemoryBytes": status.child_memory_bytes,
        "childMemoryPeakBytes": status.child_memory_peak_bytes,
        "fingerprint": status.fingerprint,
        "localIps": status.local_ips,
        "hostname": status.hostname,
        "prettyHostname": status.pretty_hostname,
        "protocolVersion": PROTOCOL_VERSION,
        "updateSupported": status.update_supported,
        "host": {
            "os": status.os,
            "arch": status.arch,
            "hasDisplay": status.has_display,
            "locality": if is_local { "local" } else { "remote" },
        },
    });
    let obj = v.as_object_mut().expect("status_json literal is an object");
    let host = obj
        .get_mut("host")
        .and_then(Value::as_object_mut)
        .expect("status_json host literal is an object");
    if let Some(device_kind) = &status.device_kind {
        host.insert("deviceKind".into(), device_kind.clone().into());
    }
    if let Some(hardware_model) = &status.hardware_model {
        host.insert("hardwareModel".into(), hardware_model.clone().into());
    }
    if let Some(tc) = &status.tc_address {
        obj.insert("tcAddress".into(), tc.clone().into());
    }
    if let Some(build_commit) = &status.build_commit {
        obj.insert("buildCommit".into(), build_commit.clone().into());
    }
    if let Some(budget) = status.agent_memory_budget_bytes {
        obj.insert("agentMemoryBudgetBytes".into(), budget.into());
    }
    if let Some(charged) = status.agent_memory_charged_bytes {
        obj.insert("agentMemoryChargedBytes".into(), charged.into());
    }
    if let Some(queued) = status.queued_spawns {
        obj.insert("queuedSpawns".into(), queued.into());
    }
    if let Some(avail) = status.workspaces_disk_available_bytes {
        obj.insert("workspacesDiskAvailableBytes".into(), avail.into());
    }
    if let Some(total) = status.workspaces_disk_total_bytes {
        obj.insert("workspacesDiskTotalBytes".into(), total.into());
    }
    if let Some(fw) = &status.file_watch {
        obj.insert(
            "fileWatch".into(),
            json!({
                "activeStreams": fw.active_streams,
                "totalRoots": fw.total_roots,
                "failedRoots": fw.failed_roots,
            }),
        );
    }
    if let Some(count) = status.fd_count {
        obj.insert("fdCount".into(), count.into());
    }
    if let Some(limit) = status.fd_limit {
        obj.insert("fdLimit".into(), limit.into());
    }
    v
}

/// The daemon-side scope gate for `system.gitCredential` (monorepo#884): only
/// `protocol=https` + `host=github.com` (case-insensitive, exact host) may
/// receive the credential. Mirrors the helper's own client-side gate.
pub(crate) fn git_credential_scope_ok(protocol: Option<&str>, host: Option<&str>) -> bool {
    protocol.is_some_and(|p| p.eq_ignore_ascii_case("https"))
        && host.is_some_and(|h| h.eq_ignore_ascii_case("github.com"))
}

/// Handle a classified `system.*` request: build the response frame (or `None`
/// for a notification, which gets no reply). `system.shutdown` triggers the
/// graceful teardown before acknowledging; like `system.importLegacy` and
/// `system.gitCredential` it is UDS-only, so remote (TCP/WSS) callers get
/// -32001 and remote shutdown notifications are ignored. `system.gitCredential`
/// returns `{ credential: { username, password } }` when a credential is
/// available and `{ credential: null }` otherwise (scope gate failed / setting
/// off / no token) — the distinction between those cases is never surfaced on
/// the wire. `system.requestUpdate` is served on BOTH transports (a remote
/// client is exactly who needs to trigger an update): success returns
/// `{ ok: true }`, and a daemon that is not sitter-supervised gets `-32603`
/// with the reason.
pub(crate) async fn handle(
    req: SystemRequest,
    control: &dyn SystemControl,
    is_local: bool,
    is_uds: bool,
) -> Option<String> {
    let result: Result<Value, (i32, String)> = match req.method {
        SystemMethod::Status => Ok(status_json(&control.status(), is_local)),
        SystemMethod::Shutdown if !is_uds => Err((
            -32001,
            "system.shutdown is available over UDS only".to_string(),
        )),
        SystemMethod::Shutdown => {
            control.request_shutdown();
            Ok(json!({ "ok": true, "stopping": true }))
        }
        SystemMethod::RequestUpdate => control
            .request_update()
            .map(|()| json!({ "ok": true }))
            .map_err(|message| (-32603, message)),
        SystemMethod::ImportLegacy { .. } if !is_uds => Err((
            -32001,
            "system.importLegacy is available over UDS only".to_string(),
        )),
        SystemMethod::ImportLegacy { force: Err(()) } => {
            Err((-32602, "force must be a boolean".to_string()))
        }
        SystemMethod::ImportLegacy { force: Ok(force) } => control
            .import_legacy(force)
            .await
            .map_err(|message| (-32603, message)),
        SystemMethod::GitCredential { .. } if !is_uds => Err((
            -32001,
            "system.gitCredential is available over UDS only".to_string(),
        )),
        SystemMethod::GitCredential {
            pid,
            protocol,
            host,
        } => {
            // Defense in depth (monorepo#884): the daemon re-checks the
            // helper's scope gate, so an arbitrary local UDS caller cannot
            // obtain the credential for anything but https://github.com. A
            // scope miss is indistinguishable from "no token" on the wire.
            if git_credential_scope_ok(protocol.as_deref(), host.as_deref()) {
                let credential = control.git_credential(pid).await.map(
                    |(username, password)| json!({ "username": username, "password": password }),
                );
                Ok(json!({ "credential": credential }))
            } else {
                tracing::debug!(
                    client_pid = pid,
                    "git credential request denied (scope is not https://github.com)"
                );
                Ok(json!({ "credential": Value::Null }))
            }
        }
    };
    if !req.id_present {
        return None;
    }
    Some(match result {
        Ok(value) => success_frame(&req.id_echo, &value),
        Err((code, message)) => error_frame(&req.id_echo, code, &message),
    })
}

#[cfg(test)]
mod tests;
