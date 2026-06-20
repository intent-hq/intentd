//! Daemonization: launchd (macOS) + systemd (Linux) unit generation (§5.8).
//!
//! The daemon never forks/double-forks itself; supervision is delegated to the
//! platform service manager (the modern, recommended approach). This module
//! generates the platform unit, installs/uninstalls it under the user's service
//! directory, and validates an installed unit (the `intentd service` subcommand).
//! `intentd serve --foreground` remains the only non-supervised mode.

use std::path::{Path, PathBuf};

use intent_core::Config;

/// launchd `Label` / plist basename stem (`ai.intent.intentd.plist`, §5.8).
pub const LAUNCHD_LABEL: &str = "ai.intent.intentd";
/// systemd user-unit filename (`~/.config/systemd/user/intentd.service`, §5.8).
pub const SYSTEMD_UNIT_NAME: &str = "intentd.service";

/// Render the macOS LaunchAgent plist (§5.8): `RunAtLoad`, `KeepAlive`
/// (`Crashed=true`, `SuccessfulExit=false` so a clean `stop` does not relaunch),
/// `ProgramArguments = [intentd, serve, --listen, uds]`, and the log paths.
/// Lines are joined explicitly so indentation is preserved (a `\`-continued
/// string literal would strip the leading whitespace).
pub fn launchd_plist(exe: &str, out_log: &str, err_log: &str) -> String {
    let lines = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".to_string(),
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">".to_string(),
        "<plist version=\"1.0\">".to_string(),
        "<dict>".to_string(),
        "    <key>Label</key>".to_string(),
        format!("    <string>{LAUNCHD_LABEL}</string>"),
        "    <key>ProgramArguments</key>".to_string(),
        "    <array>".to_string(),
        format!("        <string>{exe}</string>"),
        "        <string>serve</string>".to_string(),
        "        <string>--listen</string>".to_string(),
        "        <string>uds</string>".to_string(),
        "    </array>".to_string(),
        "    <key>RunAtLoad</key>".to_string(),
        "    <true/>".to_string(),
        "    <key>KeepAlive</key>".to_string(),
        "    <dict>".to_string(),
        "        <key>Crashed</key>".to_string(),
        "        <true/>".to_string(),
        "        <key>SuccessfulExit</key>".to_string(),
        "        <false/>".to_string(),
        "    </dict>".to_string(),
        "    <key>StandardOutPath</key>".to_string(),
        format!("    <string>{out_log}</string>"),
        "    <key>StandardErrorPath</key>".to_string(),
        format!("    <string>{err_log}</string>"),
        "</dict>".to_string(),
        "</plist>".to_string(),
    ];
    format!("{}\n", lines.join("\n"))
}

/// Render the Linux systemd user unit (§5.8): `Type=simple`,
/// `ExecStart=intentd serve`, `ExecStop=intentd stop`, `Restart=on-failure`.
pub fn systemd_unit(exe: &str) -> String {
    let lines = [
        "[Unit]".to_string(),
        "Description=Intent backend daemon (intentd)".to_string(),
        "After=network.target".to_string(),
        String::new(),
        "[Service]".to_string(),
        "Type=simple".to_string(),
        format!("ExecStart={exe} serve"),
        format!("ExecStop={exe} stop"),
        "Restart=on-failure".to_string(),
        String::new(),
        "[Install]".to_string(),
        "WantedBy=default.target".to_string(),
    ];
    format!("{}\n", lines.join("\n"))
}

/// The resolved install target for the current platform.
pub struct ServiceTarget {
    /// Absolute path the unit is written to.
    pub path: PathBuf,
    /// The exact unit contents that should be on disk.
    pub content: String,
    /// `launchd` (macOS) or `systemd` (Linux) — used in user-facing messages.
    pub kind: &'static str,
    /// The platform-appropriate enable hint printed after install.
    pub enable_hint: String,
}

/// Resolve where the service unit lives and what it should contain on the
/// current platform. Errors on an unsupported OS (no double-fork fallback).
pub fn plan(config: &Config) -> anyhow::Result<ServiceTarget> {
    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("cannot resolve the intentd executable path: {e}"))?;
    let exe = exe.to_string_lossy().into_owned();
    let home = home_dir()?;

    if cfg!(target_os = "macos") {
        let log_dir = config.data_dir.join("logs");
        let out_log = log_dir.join("intentd.out.log");
        let err_log = log_dir.join("intentd.err.log");
        let content = launchd_plist(&exe, &out_log.to_string_lossy(), &err_log.to_string_lossy());
        let path = home
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{LAUNCHD_LABEL}.plist"));
        Ok(ServiceTarget {
            path,
            content,
            kind: "launchd",
            enable_hint: format!(
                "launchctl load -w {}",
                display_path(&home, "Library/LaunchAgents")
            ),
        })
    } else if cfg!(target_os = "linux") {
        let content = systemd_unit(&exe);
        let path = home
            .join(".config")
            .join("systemd")
            .join("user")
            .join(SYSTEMD_UNIT_NAME);
        Ok(ServiceTarget {
            path,
            content,
            kind: "systemd",
            enable_hint: "systemctl --user enable --now intentd".to_string(),
        })
    } else {
        anyhow::bail!(
            "daemonization is only supported on macOS (launchd) and Linux (systemd); \
             use `intentd serve --foreground` on this platform"
        )
    }
}

/// Resolve the user's home directory from `HOME` (set on macOS/Linux).
fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("HOME is not set; cannot locate the user service directory"))
}

fn display_path(home: &Path, rel: &str) -> String {
    home.join(rel).to_string_lossy().into_owned()
}

mod ops;
pub use ops::{install, status, uninstall};

#[cfg(test)]
mod tests;
