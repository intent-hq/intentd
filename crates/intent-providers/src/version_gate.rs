//! Pure version-gate decision for the pi provider's `pi` CLI.
//!
//! The pinned pi-acp adapter ([`crate::config::PI_ACP_NPX_PACKAGE`]) requires
//! the `pi` CLI at [`crate::config::PI_CLI_MIN_VERSION`] or newer. The gate
//! decision here is pure and injectable — callers probe `pi --version`
//! themselves (subprocess/PATH work stays at the call site) and feed the
//! result in as a [`PiCliProbe`].
//!
//! Policy: only a confirmed-old or missing CLI gates the provider off.
//! [`PiCliGate::Unknown`] (probe failed or output unparseable) is permissive
//! so a changed `--version` format never false-negatives the provider;
//! callers log a warning instead.

use crate::config::PI_CLI_MIN_VERSION;

/// Result of probing the `pi` CLI, as fed to [`pi_cli_gate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PiCliProbe {
    /// The `pi` binary was not found at all.
    Missing,
    /// The probe ran and produced this raw `pi --version` output.
    Output(String),
    /// The binary was found but the probe itself failed (spawn error,
    /// timeout, nonzero exit with no usable output).
    Failed,
}

/// Version-gate decision for the pi provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PiCliGate {
    /// CLI found at [`PI_CLI_MIN_VERSION`] or newer — do not gate.
    Ok,
    /// CLI found but confirmed older than the minimum — gate off. Carries
    /// the version string found, for user-facing messages.
    TooOld(String),
    /// CLI not found — gate off.
    Missing,
    /// Probe failed or output unparseable — permissive: do NOT gate, callers
    /// log a warning.
    Unknown,
}

impl PiCliGate {
    /// Whether this decision gates the pi provider off. Only [`Self::TooOld`]
    /// and [`Self::Missing`] gate; [`Self::Unknown`] is permissive.
    pub fn gates(&self) -> bool {
        matches!(self, Self::TooOld(_) | Self::Missing)
    }
}

/// Decide the pi version gate from a probe result. Pure — no filesystem,
/// PATH, or subprocess access.
pub fn pi_cli_gate(probe: &PiCliProbe) -> PiCliGate {
    match probe {
        PiCliProbe::Missing => PiCliGate::Missing,
        PiCliProbe::Failed => PiCliGate::Unknown,
        PiCliProbe::Output(output) => match (
            parse_cli_version(output),
            parse_cli_version(PI_CLI_MIN_VERSION),
        ) {
            (Some(found), Some(min)) if found < min => PiCliGate::TooOld(format_version(found)),
            (Some(_), Some(_)) => PiCliGate::Ok,
            _ => PiCliGate::Unknown,
        },
    }
}

/// Tolerantly extract a `major.minor.patch` triple from `--version` output:
/// the first whitespace-separated token that looks like a version wins, an
/// optional leading `v` is accepted, and a missing patch defaults to 0
/// (`0.80.4`, `pi 0.80.4`, `v0.80.4`, `pi version 0.80` all parse). At least
/// `major.minor` is required so bare integers are never mistaken for versions.
fn parse_cli_version(output: &str) -> Option<(u64, u64, u64)> {
    output.split_whitespace().find_map(parse_version_token)
}

fn parse_version_token(token: &str) -> Option<(u64, u64, u64)> {
    let token = token.strip_prefix('v').unwrap_or(token);
    // Cut pre-release/build suffixes ("0.80.4-beta.1", "0.80.4+abc").
    let token = token.split(['-', '+']).next().unwrap_or(token);
    let mut parts = token.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next().map_or(Some(0), |p| p.parse().ok())?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn format_version((major, minor, patch): (u64, u64, u64)) -> String {
    format!("{major}.{minor}.{patch}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate_output(output: &str) -> PiCliGate {
        pi_cli_gate(&PiCliProbe::Output(output.to_string()))
    }

    #[test]
    fn min_version_constant_parses() {
        assert_eq!(parse_cli_version(PI_CLI_MIN_VERSION), Some((0, 80, 4)));
    }

    #[test]
    fn older_version_gates_as_too_old() {
        let gate = gate_output("0.80.3");
        assert_eq!(gate, PiCliGate::TooOld("0.80.3".to_string()));
        assert!(gate.gates());
        assert_eq!(gate_output("0.79.9"), PiCliGate::TooOld("0.79.9".into()));
        // Missing patch defaults to 0: 0.80 < 0.80.4.
        assert_eq!(gate_output("pi 0.80"), PiCliGate::TooOld("0.80.0".into()));
    }

    #[test]
    fn exact_minimum_version_is_ok() {
        let gate = gate_output("0.80.4");
        assert_eq!(gate, PiCliGate::Ok);
        assert!(!gate.gates());
    }

    #[test]
    fn newer_versions_are_ok() {
        assert_eq!(gate_output("0.80.5"), PiCliGate::Ok);
        assert_eq!(gate_output("0.81.0"), PiCliGate::Ok);
        assert_eq!(gate_output("1.0.0"), PiCliGate::Ok);
    }

    #[test]
    fn tolerant_parsing_accepts_common_version_formats() {
        assert_eq!(gate_output("v0.80.4"), PiCliGate::Ok);
        assert_eq!(gate_output("pi 0.80.4"), PiCliGate::Ok);
        assert_eq!(gate_output("pi version v1.2.3"), PiCliGate::Ok);
        assert_eq!(gate_output("0.80.4-beta.1"), PiCliGate::Ok);
        assert_eq!(gate_output("  0.80.4\n"), PiCliGate::Ok);
    }

    #[test]
    fn unparseable_output_is_permissive_unknown() {
        for output in ["", "garbage", "pi build 20260101", "a.b.c", "1.2.3.4"] {
            let gate = gate_output(output);
            assert_eq!(gate, PiCliGate::Unknown, "output: {output:?}");
            assert!(!gate.gates(), "output: {output:?}");
        }
    }

    #[test]
    fn failed_probe_is_permissive_unknown() {
        let gate = pi_cli_gate(&PiCliProbe::Failed);
        assert_eq!(gate, PiCliGate::Unknown);
        assert!(!gate.gates());
    }

    #[test]
    fn missing_binary_gates() {
        let gate = pi_cli_gate(&PiCliProbe::Missing);
        assert_eq!(gate, PiCliGate::Missing);
        assert!(gate.gates());
    }
}
