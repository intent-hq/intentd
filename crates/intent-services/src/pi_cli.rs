//! Shared `pi` CLI resolution + version probe (monorepo#1662).
//!
//! The pinned pi-acp adapter requires the `pi` CLI at
//! [`intent_providers::PI_CLI_MIN_VERSION`] or newer. This module owns the
//! one resolution + probe path shared by provider discovery
//! (`host.providerDiscovery`'s pi row), `intentd doctor`, and the spawn-time
//! fail-fast in `agent_manager` — so every surface reports the same verdict
//! for the same binary the spawned child would actually exec.

use std::path::PathBuf;

use intent_providers::{pi_cli_gate, PiCliGate, PiCliProbe};

/// Env var pi-acp (0.0.33) reads to override the `pi` binary it spawns
/// (`PiRpcProcess.spawn({ piCommand: process.env.PI_ACP_PI_COMMAND })`).
/// `create_agent` points it at the generated wrapper script; the wrapper
/// itself execs [`resolve_real_pi_command`]'s result.
pub(crate) const PI_ACP_PI_COMMAND_ENV: &str = "PI_ACP_PI_COMMAND";

/// The pi binary the wrapper execs: a pre-existing `PI_ACP_PI_COMMAND` in the
/// daemon env (the value pi-acp itself would have used, which the wrapper
/// override shadows) or `pi` from the child's PATH — pi-acp's own fallback.
pub(crate) fn resolve_real_pi_command() -> String {
    std::env::var(PI_ACP_PI_COMMAND_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "pi".to_string())
}

/// One resolved-and-probed `pi` CLI snapshot: the command that would be
/// exec'd, where it resolved (if anywhere), the raw `--version` first line
/// (if the probe produced one), and the pure gate decision over the probe.
#[derive(Debug, Clone)]
pub struct PiCliStatus {
    /// The command probed — the `PI_ACP_PI_COMMAND` override or bare `pi`.
    pub command: String,
    /// Absolute path the command resolved to, when found.
    pub resolved_path: Option<PathBuf>,
    /// First line of `pi --version` output, when the probe succeeded.
    pub version_output: Option<String>,
    /// The version-gate decision ([`intent_providers::pi_cli_gate`]).
    pub gate: PiCliGate,
}

/// Resolve and probe the `pi` CLI: resolve [`resolve_real_pi_command`]'s
/// result via [`intent_providers::find_pi_cli`] (spawn-time enhanced-PATH
/// scan mirroring the pi-acp child's PATH, or direct validation for explicit
/// paths), then run `--version` with a short timeout. Blocking (subprocess
/// wait) — call from a blocking context.
#[must_use]
pub fn probe_pi_cli() -> PiCliStatus {
    let command = resolve_real_pi_command();
    let resolved_path = intent_providers::find_pi_cli(&command);
    let (version_output, probe) = match &resolved_path {
        // A RELATIVE separator-carrying override (e.g. `./bin/pi`) validates
        // against the daemon's CWD, but the wrapper/child may exec it from a
        // different one — a child-resolvable override could look unresolvable
        // here. Missing is the only probe-side verdict that hard-gates, so
        // degrade that case to Failed (→ Unknown, permissive WARN) and
        // confine hard-gating to the bare-name PATH scan and absolute paths.
        None if is_relative_with_separator(&command) => (None, PiCliProbe::Failed),
        None => (None, PiCliProbe::Missing),
        Some(path) => match run_version_probe(path) {
            Some(line) => (Some(line.clone()), PiCliProbe::Output(line)),
            None => (None, PiCliProbe::Failed),
        },
    };
    let gate = pi_cli_gate(&probe);
    PiCliStatus {
        command,
        resolved_path,
        version_output,
        gate,
    }
}

/// Whether `command` is a relative path that carries a path separator — the
/// override shape whose resolution is CWD-dependent (see [`probe_pi_cli`]).
fn is_relative_with_separator(command: &str) -> bool {
    !PathBuf::from(command).is_absolute() && command.contains(std::path::MAIN_SEPARATOR)
}

/// Spawn-time fail-fast decision over a probed [`PiCliStatus`]: a gating
/// verdict (Missing / known-too-old) is a clear, user-facing error naming
/// the found version, the requirement, and the pi-acp pin
/// ([`intent_providers::pi_gate_reason`]); Unknown proceeds with a WARN
/// (`version_gate.rs` policy). Pure over the status — unit-testable without a
/// real probe.
pub(crate) fn check_pi_cli_for_spawn(status: &PiCliStatus) -> crate::Result<()> {
    if let Some(reason) = intent_providers::pi_gate_reason(&status.gate) {
        // InvalidInput (not Internal): environment misconfiguration whose
        // Display survives the JSON-RPC envelope (`domain_to_rpc` masks
        // Internal messages behind a literal "Internal error").
        return Err(crate::Error::InvalidInput(format!(
            "cannot start Pi agent: {reason}"
        )));
    }
    if status.gate == PiCliGate::Unknown {
        tracing::warn!(
            command = %status.command,
            version_output = status.version_output.as_deref().unwrap_or(""),
            "pi CLI version probe inconclusive; proceeding with spawn"
        );
    }
    Ok(())
}

/// Run `<path> --version` with a 3s budget and return the trimmed first
/// stdout line, or `None` on spawn failure, nonzero exit, timeout, or empty
/// output (the same shape as the npx version probe in `lib.rs`).
fn run_version_probe(path: &std::path::Path) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let mut child = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                let mut stdout_handle = child.stdout.take()?;
                let mut output = Vec::new();
                stdout_handle.read_to_end(&mut output).ok()?;
                let stdout = String::from_utf8_lossy(&output);
                let first_line = stdout.lines().next()?.trim();
                if first_line.is_empty() {
                    return None;
                }
                return Some(first_line.to_string());
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_with_gate(gate: PiCliGate) -> PiCliStatus {
        PiCliStatus {
            command: "pi".to_string(),
            resolved_path: None,
            version_output: None,
            gate,
        }
    }

    #[test]
    fn spawn_check_fails_fast_on_missing_cli() {
        let err = check_pi_cli_for_spawn(&status_with_gate(PiCliGate::Missing)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot start Pi agent"), "{msg}");
        assert!(msg.contains(intent_providers::PI_CLI_REQUIREMENT), "{msg}");
        assert!(msg.contains(intent_providers::PI_ACP_NPX_PACKAGE), "{msg}");
    }

    #[test]
    fn spawn_check_fails_fast_on_too_old_cli_naming_found_version() {
        let err = check_pi_cli_for_spawn(&status_with_gate(PiCliGate::TooOld("0.79.0".into())))
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("0.79.0"), "{msg}");
        assert!(msg.contains(intent_providers::PI_CLI_REQUIREMENT), "{msg}");
        assert!(msg.contains(intent_providers::PI_ACP_NPX_PACKAGE), "{msg}");
        // InvalidInput so the message survives the JSON-RPC envelope.
        assert!(matches!(err, crate::Error::InvalidInput(_)));
    }

    #[test]
    fn spawn_check_is_permissive_on_ok_and_unknown() {
        assert!(check_pi_cli_for_spawn(&status_with_gate(PiCliGate::Ok)).is_ok());
        assert!(check_pi_cli_for_spawn(&status_with_gate(PiCliGate::Unknown)).is_ok());
    }

    #[test]
    fn resolve_real_pi_command_defaults_to_pi() {
        // The env override branch is inherently process-global; only the
        // default arm is asserted here (parallel tests must not mutate env).
        if std::env::var(PI_ACP_PI_COMMAND_ENV).is_err() {
            assert_eq!(resolve_real_pi_command(), "pi");
        }
    }

    /// The probe resolves and gates a real fake `pi` on an explicit path —
    /// exercising resolve → `--version` → gate end-to-end without PATH
    /// mutation (the explicit-path arm of `find_pi_cli`).
    #[cfg(unix)]
    #[test]
    fn probe_pipeline_gates_a_fake_pi_binary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::Builder::new()
            .prefix("intent-pi-cli-probe-")
            .tempdir()
            .expect("tempdir");
        let fake_pi = dir.path().join("pi");
        std::fs::write(&fake_pi, "#!/bin/sh\necho 0.79.0\n").unwrap();
        std::fs::set_permissions(&fake_pi, std::fs::Permissions::from_mode(0o755)).unwrap();

        let resolved = intent_providers::find_pi_cli(fake_pi.to_str().unwrap())
            .expect("explicit path resolves");
        let line = run_version_probe(&resolved).expect("probe output");
        assert_eq!(line, "0.79.0");
        assert_eq!(
            pi_cli_gate(&PiCliProbe::Output(line)),
            PiCliGate::TooOld("0.79.0".into())
        );
    }

    /// An unresolvable RELATIVE separator-carrying override is CWD-dependent
    /// (the child may resolve it where the daemon cannot), so it must degrade
    /// to Failed → Unknown (permissive), never the hard-gating Missing. A
    /// bare name and an absolute path stay Missing (hard-gate shapes).
    #[test]
    fn relative_override_shape_is_cwd_dependent() {
        let sep = std::path::MAIN_SEPARATOR;
        assert!(is_relative_with_separator(&format!(".{sep}bin{sep}pi")));
        assert!(!is_relative_with_separator("pi"));
        assert!(!is_relative_with_separator(&format!(
            "{sep}usr{sep}local{sep}bin{sep}pi"
        )));
    }
}
