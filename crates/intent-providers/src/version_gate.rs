//! Pure version-gate decisions for provider CLIs (`pi`, `auggie`).
//!
//! The pinned pi-acp adapter ([`crate::config::PI_ACP_NPX_PACKAGE`]) requires
//! the `pi` CLI at [`crate::config::PI_CLI_MIN_VERSION`] or newer, and the ACP
//! agent-spawn path requires `auggie` at
//! [`crate::config::AUGGIE_CLI_MIN_VERSION`] or newer (its launch flags landed
//! in 0.7.0). The gate decisions here are pure and injectable — callers probe
//! `<cli> --version` themselves (subprocess/PATH work stays at the call site)
//! and feed the result in as a [`PiCliProbe`].
//!
//! Policy: only a confirmed-old or missing CLI gates the provider off.
//! [`PiCliGate::Unknown`] (probe failed or output unparseable) is permissive
//! so a changed `--version` format never false-negatives the provider;
//! callers log a warning instead. The auggie gate
//! ([`auggie_cli_gate`]) reuses the same probe/gate shapes so the
//! skip-and-continue spawn resolver treats an unparseable version as usable.

use crate::config::{
    AUGGIE_CLI_MIN_VERSION, AUGGIE_CLI_REQUIREMENT, PI_ACP_NPX_PACKAGE, PI_CLI_MIN_VERSION,
    PI_CLI_REQUIREMENT,
};

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

/// Human-readable, actionable reason the pi provider is unavailable, or
/// `None` when the gate is permissive ([`PiCliGate::Ok`] /
/// [`PiCliGate::Unknown`]). Shared by discovery (`unavailableReason`),
/// doctor, and the spawn fail-fast so every surface names the same found
/// version, requirement, and adapter pin.
pub fn pi_gate_reason(gate: &PiCliGate) -> Option<String> {
    match gate {
        PiCliGate::TooOld(found) => Some(format!(
            "pi CLI {found} is too old — {PI_CLI_REQUIREMENT} is required by {PI_ACP_NPX_PACKAGE}"
        )),
        PiCliGate::Missing => Some(format!(
            "pi CLI not found — {PI_CLI_REQUIREMENT} is required by {PI_ACP_NPX_PACKAGE}"
        )),
        PiCliGate::Ok | PiCliGate::Unknown => None,
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

/// Human-readable, actionable reason the resolved `auggie` binary cannot be
/// launched, or `None` when the gate is permissive ([`PiCliGate::Ok`] /
/// [`PiCliGate::Unknown`]). Names the found version, the requirement, and the
/// remedy (record the newer install in `~/.augment/auggie-path` or set
/// `context.auggiePath`) so the spawn fail-fast and doctor share one message.
pub fn auggie_gate_reason(gate: &PiCliGate) -> Option<String> {
    match gate {
        PiCliGate::TooOld(found) => Some(format!(
            "auggie {found} is too old — {AUGGIE_CLI_REQUIREMENT} is required to launch an agent \
             (the daemon passes --acp/--allow-indexing/--model/--remove-tool). Update auggie, or \
             point the daemon at a newer install via ~/.augment/auggie-path or the \
             context.auggiePath setting."
        )),
        PiCliGate::Missing => Some(format!(
            "auggie not found — {AUGGIE_CLI_REQUIREMENT} is required to launch an agent"
        )),
        PiCliGate::Ok | PiCliGate::Unknown => None,
    }
}

/// Decide the auggie version gate from a probe result. Pure — no filesystem,
/// PATH, or subprocess access. Shares the [`PiCliProbe`]/[`PiCliGate`] shapes
/// and tolerant parser with the pi gate; only the minimum version differs
/// ([`AUGGIE_CLI_MIN_VERSION`]).
pub fn auggie_cli_gate(probe: &PiCliProbe) -> PiCliGate {
    match probe {
        PiCliProbe::Missing => PiCliGate::Missing,
        PiCliProbe::Failed => PiCliGate::Unknown,
        PiCliProbe::Output(output) => match (
            parse_cli_version(output),
            parse_cli_version(AUGGIE_CLI_MIN_VERSION),
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

    #[test]
    fn gate_reasons_name_version_requirement_and_pin() {
        let too_old = pi_gate_reason(&PiCliGate::TooOld("0.79.0".into())).unwrap();
        assert!(too_old.contains("0.79.0"));
        assert!(too_old.contains(PI_CLI_REQUIREMENT));
        assert!(too_old.contains(PI_ACP_NPX_PACKAGE));
        let missing = pi_gate_reason(&PiCliGate::Missing).unwrap();
        assert!(missing.contains(PI_CLI_REQUIREMENT));
        assert!(missing.contains(PI_ACP_NPX_PACKAGE));
        assert_eq!(pi_gate_reason(&PiCliGate::Ok), None);
        assert_eq!(pi_gate_reason(&PiCliGate::Unknown), None);
    }

    fn auggie_gate_output(output: &str) -> PiCliGate {
        auggie_cli_gate(&PiCliProbe::Output(output.to_string()))
    }

    #[test]
    fn auggie_min_version_constant_parses() {
        assert_eq!(parse_cli_version(AUGGIE_CLI_MIN_VERSION), Some((0, 7, 0)));
    }

    #[test]
    fn auggie_older_versions_gate_as_too_old() {
        // The exact stale binaries from the incident (0.1.0 / 0.4.0) gate off.
        assert_eq!(
            auggie_gate_output("0.1.0"),
            PiCliGate::TooOld("0.1.0".into())
        );
        assert_eq!(
            auggie_gate_output("auggie 0.4.0 (commit abc)"),
            PiCliGate::TooOld("0.4.0".into())
        );
        assert_eq!(
            auggie_gate_output("0.6.9"),
            PiCliGate::TooOld("0.6.9".into())
        );
    }

    #[test]
    fn auggie_minimum_and_newer_are_ok() {
        assert_eq!(auggie_gate_output("0.7.0"), PiCliGate::Ok);
        // The known-good binary from the incident.
        assert_eq!(
            auggie_gate_output("0.35.0 (commit 9a7f3836)"),
            PiCliGate::Ok
        );
        assert_eq!(auggie_gate_output("v1.0.0"), PiCliGate::Ok);
    }

    #[test]
    fn auggie_unparseable_and_failed_are_permissive() {
        assert_eq!(auggie_gate_output("garbage"), PiCliGate::Unknown);
        assert_eq!(auggie_gate_output(""), PiCliGate::Unknown);
        assert_eq!(auggie_cli_gate(&PiCliProbe::Failed), PiCliGate::Unknown);
    }

    #[test]
    fn auggie_missing_gates() {
        assert_eq!(auggie_cli_gate(&PiCliProbe::Missing), PiCliGate::Missing);
    }

    #[test]
    fn auggie_gate_reason_names_version_requirement_and_remedy() {
        let too_old = auggie_gate_reason(&PiCliGate::TooOld("0.1.0".into())).unwrap();
        assert!(too_old.contains("0.1.0"), "{too_old}");
        assert!(too_old.contains(AUGGIE_CLI_REQUIREMENT), "{too_old}");
        assert!(too_old.contains("auggie-path"), "{too_old}");
        assert!(too_old.contains("context.auggiePath"), "{too_old}");
        let missing = auggie_gate_reason(&PiCliGate::Missing).unwrap();
        assert!(missing.contains(AUGGIE_CLI_REQUIREMENT), "{missing}");
        assert_eq!(auggie_gate_reason(&PiCliGate::Ok), None);
        assert_eq!(auggie_gate_reason(&PiCliGate::Unknown), None);
    }
}
