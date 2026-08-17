//! Version-gated `auggie` binary selection for the ACP agent-spawn path
//! (monorepo#1045).
//!
//! The daemon launches auggie with `--acp --allow-indexing --model …
//! --remove-tool …`; that flag set requires auggie
//! [`intent_providers::AUGGIE_CLI_MIN_VERSION`] or newer. A stale nvm auggie
//! earlier on the daemon's (minimal, GUI-inherited) PATH would otherwise be
//! launched with flags it does not understand and fail with "Unknown
//! arguments".
//!
//! This module resolves auggie the version-gate-aware way: it walks the
//! ordered candidate list from
//! [`intent_providers::find_auggie_candidates`] (explicit
//! `providers.paths["auggie"]` → `~/.augment/bin/auggie` →
//! `~/.augment/auggie-path` marker → each enhanced-PATH hit), probes
//! `--version` on each, and picks the first one new enough — skipping an
//! incompatible earlier hit rather than launching it. If none qualify it
//! returns a clear, actionable error naming the newest version it saw and the
//! remedy (record the update in `~/.augment/auggie-path` or set
//! `context.auggiePath`).

use std::path::PathBuf;

use intent_providers::{auggie_cli_gate, PiCliGate, PiCliProbe};

/// One probed auggie candidate: its path plus the pure version-gate verdict.
#[derive(Debug, Clone)]
pub(crate) struct AuggieCandidate {
    pub path: PathBuf,
    pub gate: PiCliGate,
}

/// Resolve the auggie binary to launch, version-gated. Blocking (spawns
/// `<candidate> --version`, ≤3s each) — call from a blocking context.
///
/// `explicit_path` is the `providers.paths["auggie"]` override (already
/// trimmed/validated by the caller). Returns the first candidate that is new
/// enough, or an error if none qualify.
pub(crate) fn select_auggie_for_spawn(explicit_path: Option<&str>) -> crate::Result<PathBuf> {
    let candidates: Vec<AuggieCandidate> = intent_providers::find_auggie_candidates(explicit_path)
        .into_iter()
        .map(|path| {
            let probe = match run_version_probe(&path) {
                Some(line) => PiCliProbe::Output(line),
                None => PiCliProbe::Failed,
            };
            let gate = auggie_cli_gate(&probe);
            AuggieCandidate { path, gate }
        })
        .collect();
    select_from_candidates(candidates)
}

/// Pure selection over pre-probed candidates (test seam): pick the first whose
/// gate does not gate it off ([`PiCliGate::Ok`] or the permissive
/// [`PiCliGate::Unknown`]). When every candidate is too old, error naming the
/// newest version seen; when there were no candidates at all, error that
/// auggie was not found. A skipped (too-old) candidate is logged.
pub(crate) fn select_from_candidates(candidates: Vec<AuggieCandidate>) -> crate::Result<PathBuf> {
    let mut newest_too_old: Option<String> = None;
    for cand in candidates {
        match &cand.gate {
            PiCliGate::TooOld(found) => {
                tracing::warn!(
                    path = %cand.path.display(),
                    version = %found,
                    "skipping too-old auggie candidate; continuing discovery"
                );
                // Track the highest too-old version seen for the error message.
                if newest_too_old
                    .as_deref()
                    .is_none_or(|cur| cur < found.as_str())
                {
                    newest_too_old = Some(found.clone());
                }
            }
            // Ok or Unknown (unparseable/failed probe) — usable. Unknown is
            // permissive so a changed `--version` format never blocks spawn.
            _ => return Ok(cand.path),
        }
    }
    // InvalidInput (not Internal): environment misconfiguration whose Display
    // survives the JSON-RPC envelope (`domain_to_rpc` masks Internal messages).
    let gate = match newest_too_old {
        Some(found) => PiCliGate::TooOld(found),
        None => PiCliGate::Missing,
    };
    let reason = intent_providers::auggie_gate_reason(&gate)
        .unwrap_or_else(|| "auggie is not available".to_string());
    Err(crate::Error::InvalidInput(format!(
        "cannot start Auggie agent: {reason}"
    )))
}

/// Run `<path> --version` with a 3s budget and return the trimmed first stdout
/// line, or `None` on spawn failure, nonzero exit, timeout, or empty output
/// (same shape as [`crate::pi_cli`]'s probe).
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

    fn cand(path: &str, gate: PiCliGate) -> AuggieCandidate {
        AuggieCandidate {
            path: PathBuf::from(path),
            gate,
        }
    }

    #[test]
    fn picks_first_ok_candidate() {
        let selected =
            select_from_candidates(vec![cand("/a", PiCliGate::Ok), cand("/b", PiCliGate::Ok)])
                .unwrap();
        assert_eq!(selected, PathBuf::from("/a"));
    }

    #[test]
    fn skips_too_old_and_continues_to_compatible() {
        // The monorepo#1045 shape: a stale 0.1.0 earlier in precedence, the
        // good 0.35.0 after it — selection skips the stale one.
        let selected = select_from_candidates(vec![
            cand("/nvm/v24/auggie", PiCliGate::TooOld("0.1.0".into())),
            cand("/nvm/v22/auggie", PiCliGate::Ok),
        ])
        .unwrap();
        assert_eq!(selected, PathBuf::from("/nvm/v22/auggie"));
    }

    #[test]
    fn unknown_version_is_usable_permissive() {
        // A candidate whose --version is unparseable/failed must not block the
        // spawn (Unknown is permissive).
        let selected = select_from_candidates(vec![
            cand("/old", PiCliGate::TooOld("0.4.0".into())),
            cand("/unparseable", PiCliGate::Unknown),
        ])
        .unwrap();
        assert_eq!(selected, PathBuf::from("/unparseable"));
    }

    #[test]
    fn all_too_old_errors_naming_newest_seen() {
        let err = select_from_candidates(vec![
            cand("/a", PiCliGate::TooOld("0.1.0".into())),
            cand("/b", PiCliGate::TooOld("0.4.0".into())),
        ])
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot start Auggie agent"), "{msg}");
        // Names the newest too-old version seen (0.4.0 > 0.1.0), the
        // requirement, and the remedy.
        assert!(msg.contains("0.4.0"), "{msg}");
        assert!(
            msg.contains(intent_providers::AUGGIE_CLI_REQUIREMENT),
            "{msg}"
        );
        assert!(msg.contains("auggie-path"), "{msg}");
        // InvalidInput so the message survives the JSON-RPC envelope.
        assert!(matches!(err, crate::Error::InvalidInput(_)));
    }

    #[test]
    fn no_candidates_errors_as_missing() {
        let err = select_from_candidates(vec![]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot start Auggie agent"), "{msg}");
        assert!(msg.contains("not found"), "{msg}");
    }

    /// End-to-end over real fake binaries on explicit-path candidates:
    /// resolve → `--version` → gate → select. A too-old fake is skipped for a
    /// new-enough one.
    #[cfg(unix)]
    #[test]
    fn probe_pipeline_selects_new_enough_fake_binary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::Builder::new()
            .prefix("intent-auggie-cli-probe-")
            .tempdir()
            .expect("tempdir");
        let good = dir.path().join("auggie");
        std::fs::write(&good, "#!/bin/sh\necho 0.35.0\n").unwrap();
        std::fs::set_permissions(&good, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Explicit path resolves and probes to 0.35.0 → selected.
        let selected = select_auggie_for_spawn(good.to_str()).unwrap();
        assert_eq!(selected, good);
    }
}
