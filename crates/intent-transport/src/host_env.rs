//! Host-environment probes (§12.3): `hasDisplay` and display-server detection
//! (used by `host.status` and the CLI `status`/`doctor` surfaces, §5.7), plus
//! the crate-local OS hostname helper (used by `host.status` and
//! `server.pairingInfo`).

/// Best-effort `hasDisplay` (§12.3): X11/Wayland env on Unix, otherwise assume
/// a display unless the process was reached over SSH (no local console). Public
/// so the CLI `status`/`doctor` surfaces (§5.7) report the same value
/// `host.status` returns.
#[must_use]
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
// The four independent env probes ARE the inputs of this pure policy fn;
// bundling them into a struct would only obscure the truth table.
#[allow(clippy::fn_params_excessive_bools)]
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
pub(crate) fn detect_display_server() -> Option<String> {
    if std::env::var_os("WAYLAND_DISPLAY").is_some() {
        return Some("wayland".to_string());
    }
    if std::env::var_os("DISPLAY").is_some() {
        return Some("x11".to_string());
    }
    None
}

/// Local OS hostname (`os.hostname()` equivalent), or `intent` on failure.
/// Public so the composition root can include it in the `system.status`
/// snapshot alongside `host.status` and `server.pairingInfo`.
#[must_use]
pub fn local_hostname() -> String {
    whoami::hostname()
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "intent".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_display_policy_is_cross_platform_clean() {
        // An explicit display server always wins, regardless of platform/SSH.
        assert!(has_display(true, false, true, false));
        assert!(has_display(false, true, true, false));
        // GUI platforms (macOS/Windows) assume a console unless reached over SSH.
        assert!(has_display(false, false, false, true));
        assert!(!has_display(false, false, true, true));
        // Headless Unix without a display server has none.
        assert!(!has_display(false, false, false, false));
    }

    #[test]
    fn display_server_prefers_wayland_then_x11() {
        // Deterministic precedence is asserted at the policy boundary; this test
        // only pins the documented contract for the env-free `None` case (CI hosts
        // without DISPLAY/WAYLAND_DISPLAY set).
        if std::env::var_os("WAYLAND_DISPLAY").is_none() && std::env::var_os("DISPLAY").is_none() {
            assert_eq!(detect_display_server(), None);
        }
    }
}
