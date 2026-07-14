//! mDNS / DNS-SD advertisement of the WSS listener (§5.4).
//!
//! Ports `src/main/websocket-discovery.ts` (which uses `bonjour-service`) onto
//! the `mdns-sd` crate. Publishes a `_intent-ws._tcp` service named
//! `Intent on <hostname>` on the bound WSS port so mobile/LAN clients can
//! auto-discover the daemon and pin its TLS fingerprint (TOFU). The TXT record
//! carries the TS keys (`version`, `path`, `hostname`, `fp`) plus the §12.3
//! host-capability fields (`os`, `arch`, `hasDisplay`, `locality`).
//!
//! Advertising is started post-bind from [`crate::lifecycle`] (so it uses the
//! real, post-backoff port) and destroyed in the graceful-shutdown ordering;
//! it is gated behind the discovery-enable flag (`is_discovery_enabled`,
//! default false) and only runs for the TCP/both listen modes, since the WSS
//! listener itself only exists then.
//!
//! ## Ownership and Lifecycle Invariant
//!
//! Discovery handles are owned by the [`WsApiServer`](crate::ws::WsApiServer)
//! and stored in [`crate::lifecycle::RunningHandles`]. This coupling enforces
//! the **product invariant** that mDNS advertisement **requires an active
//! listener**: advertising a service (port + TLS fingerprint) that clients
//! cannot connect to violates the mDNS service contract. The coupling is
//! intentional and correct:
//!
//! 1. **Service contract**: Clients expect advertised services to be reachable.
//! 2. **Settings ordering**: The batch-update system (§5.12) enforces that
//!    `server.wsApi.enabled` runs before `server.discovery.enabled`, ensuring
//!    the listener starts before discovery and stops after discovery.
//! 3. **Runtime guards**: [`crate::lifecycle::WsInner::start_discovery`] returns
//!    an error if the listener is not running.
//! 4. **Secure mode requirement**: Discovery advertises the TLS fingerprint;
//!    insecure mode has no fingerprint and forces `discovery_enabled = false`.
//!
//! Independent runtime control (`start_discovery` / `stop_discovery` /
//! `is_discovery_active`) allows toggling discovery on/off while the listener
//! runs, without restarting the listener itself.

use mdns_sd::{ServiceDaemon, ServiceInfo};

use intent_core::{Error, Result};

/// DNS-SD service type (with the `.local.` domain label `mdns-sd` expects).
pub const SERVICE_TYPE: &str = "_intent-ws._tcp.local.";
/// TXT `version` value — matches the TS source's `version: '1'`.
const TXT_VERSION: &str = "1";
/// TXT `path` value — the WSS endpoint path, matching the TS source.
const TXT_PATH: &str = "/ws";

/// Host-capability fields advertised in the TXT record (§12.3). `os`/`arch`
/// are Rust's `std::env::consts` identifiers (e.g. `macos`/`aarch64`); the TS
/// source did not advertise these, so there is no exact key-value parity to
/// match — only the four TS keys (`version`/`path`/`hostname`/`fp`) do.
#[derive(Debug, Clone)]
pub(crate) struct HostCapabilities {
    pub os: String,
    pub arch: String,
    pub has_display: bool,
    /// `local` or `remote` (§12.3).
    pub locality: &'static str,
}

impl HostCapabilities {
    /// Best-effort detection (§12.3). `locality` is the resolved value for the
    /// listener being advertised (`local` when forced/UDS, `remote` for TCP/WSS,
    /// §5.14); `has_display` is inferred from platform env
    /// (`DISPLAY`/`WAYLAND_DISPLAY`, or absence of `SSH_*`).
    pub fn detect(is_local: bool) -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            has_display: detect_has_display(),
            locality: if is_local { "local" } else { "remote" },
        }
    }
}

/// Best-effort `hasDisplay` (§12.3): X11/Wayland env on Unix, otherwise assume
/// a display unless the process was reached over SSH (no local console). Public
/// so the CLI `status`/`doctor` surfaces (§5.7) report the same value the mDNS
/// TXT record advertises.
pub fn detect_has_display() -> bool {
    has_display(
        std::env::var_os("DISPLAY").is_some(),
        std::env::var_os("WAYLAND_DISPLAY").is_some(),
        std::env::var_os("SSH_CONNECTION").is_some() || std::env::var_os("SSH_TTY").is_some(),
        cfg!(target_os = "macos") || cfg!(target_os = "windows"),
    )
}

/// Pure `hasDisplay` policy (testable without touching the process env): an
/// explicit X11/Wayland display always wins; otherwise GUI platforms
/// (macOS/Windows) assume a console unless reached over SSH; headless Unix
/// without a display server has none.
fn has_display(display: bool, wayland: bool, over_ssh: bool, gui_platform: bool) -> bool {
    if display || wayland {
        return true;
    }
    if gui_platform {
        return !over_ssh;
    }
    false
}

/// Best-effort display-server identifier for `host.status` `displayServer`
/// (§5.14). Reports `wayland`/`x11` when their env is present; `None`
/// otherwise. Cross-platform clean — returns `None` on hosts without those
/// Unix display-server env vars.
pub fn detect_display_server() -> Option<String> {
    if std::env::var_os("WAYLAND_DISPLAY").is_some() {
        return Some("wayland".to_string());
    }
    if std::env::var_os("DISPLAY").is_some() {
        return Some("x11".to_string());
    }
    None
}

/// Local OS hostname (`os.hostname()` equivalent), or `intent` on failure.
pub(crate) fn local_hostname() -> String {
    whoami::fallible::hostname()
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "intent".to_string())
}

/// Strip a trailing `.local` label (matches the TS `replace(/\.local$/, '')`).
fn strip_local(hostname: &str) -> &str {
    hostname.strip_suffix(".local").unwrap_or(hostname)
}

/// DNS-SD instance name: `Intent on <hostname stripped of .local>`.
pub(crate) fn instance_name(hostname: &str) -> String {
    format!("Intent on {}", strip_local(hostname))
}

/// Build the ordered TXT key/value pairs. Deterministic for a given input so
/// the builder can be unit-tested without touching the network.
pub(crate) fn txt_records(
    fingerprint: &str,
    hostname: &str,
    caps: &HostCapabilities,
) -> Vec<(String, String)> {
    vec![
        ("version".to_string(), TXT_VERSION.to_string()),
        ("path".to_string(), TXT_PATH.to_string()),
        ("hostname".to_string(), hostname.to_string()),
        ("fp".to_string(), fingerprint.to_string()),
        ("os".to_string(), caps.os.clone()),
        ("arch".to_string(), caps.arch.clone()),
        ("hasDisplay".to_string(), caps.has_display.to_string()),
        ("locality".to_string(), caps.locality.to_string()),
    ]
}

/// Build a [`ServiceInfo`] for the advertisement. With `explicit_ip = None` the
/// host's addresses are filled in automatically; tests pass a loopback address
/// for a hermetic, network-free round-trip.
pub(crate) fn build_service_info(
    port: u16,
    fingerprint: &str,
    hostname: &str,
    caps: &HostCapabilities,
    explicit_ip: Option<&str>,
) -> Result<ServiceInfo> {
    let instance = instance_name(hostname);
    let host_label = format!("{}.local.", strip_local(hostname));
    let props = txt_records(fingerprint, hostname, caps);
    let ip = explicit_ip.unwrap_or("");
    let info = ServiceInfo::new(SERVICE_TYPE, &instance, &host_label, ip, port, &props[..])
        .map_err(|e| Error::Internal(format!("mdns service info: {e}")))?;
    Ok(if explicit_ip.is_none() {
        info.enable_addr_auto()
    } else {
        info
    })
}

/// A live mDNS advertisement. Dropped/`stop`ped on graceful shutdown so no
/// stale record lingers; re-`start` replaces a prior advert (idempotent).
pub struct Discovery {
    daemon: ServiceDaemon,
    fullname: String,
}

impl Discovery {
    /// Publish the service on the host's real interfaces. `is_local` is the
    /// resolved locality advertised in the TXT record (§5.14): TCP/WSS listeners
    /// are `remote` by default, unless forced local via `--mode`/`server.locality`.
    pub fn start(port: u16, fingerprint: &str, is_local: bool) -> Result<Self> {
        let info = build_service_info(
            port,
            fingerprint,
            &local_hostname(),
            &HostCapabilities::detect(is_local),
            None,
        )?;
        Self::register(info)
    }

    /// Register an already-built [`ServiceInfo`] with a fresh daemon.
    pub(crate) fn register(info: ServiceInfo) -> Result<Self> {
        let daemon =
            ServiceDaemon::new().map_err(|e| Error::Internal(format!("mdns daemon: {e}")))?;
        let fullname = info.get_fullname().to_string();
        daemon
            .register(info)
            .map_err(|e| Error::Internal(format!("mdns register: {e}")))?;
        tracing::info!(service = %fullname, "mDNS service published");
        Ok(Self { daemon, fullname })
    }

    /// Unregister the service and shut the daemon down (best-effort).
    pub fn stop(self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
        tracing::info!(service = %self.fullname, "mDNS service unpublished");
    }
}

/// Advertise only when `enabled`; returns `None` (publishing nothing) when the
/// discovery flag is off. A registration failure is logged and yields `None`
/// rather than failing the listener (best-effort, matching the TS try/catch).
/// `is_local` is the resolved locality advertised in the TXT record (§5.14).
pub(crate) fn advertise_if_enabled(
    enabled: bool,
    port: u16,
    fingerprint: &str,
    is_local: bool,
) -> Option<Discovery> {
    if !enabled {
        return None;
    }
    match Discovery::start(port, fingerprint, is_local) {
        Ok(discovery) => Some(discovery),
        Err(e) => {
            tracing::warn!(error = %e, "failed to publish mDNS service");
            None
        }
    }
}

#[cfg(test)]
mod tests;
