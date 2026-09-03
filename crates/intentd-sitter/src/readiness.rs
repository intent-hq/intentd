//! Post-restart readiness wait for `intentd update`.
//!
//! After an update installs a new daemon and SIGHUPs the serve-mode sitter,
//! the old daemon takes a moment to shut down and the new one to recreate
//! its socket — a chained command (`intentd update && intentd pair`) races
//! that window (intent-hq/intent#4276). This module polls the freshly
//! installed daemon binary's `call system.status` until the daemon
//! answering on the socket reports the target version, mirroring
//! install.sh's "waiting for the daemon to respond..." verification.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// How long the update path waits for the restarted daemon before giving
/// up (matches install.sh's verification budget).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Pause between readiness probes.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Result of [`wait_for_version`].
#[derive(Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// A daemon reporting the target version answered on the socket.
    Ready,
    /// The deadline passed without the target version answering;
    /// `last_seen` is the version that did answer (the old daemon still
    /// shutting down), when any probe succeeded at all.
    TimedOut { last_seen: Option<String> },
}

/// Probe the daemon once via the installed binary's `call system.status`
/// fast-path: the version the daemon on the socket reports, or `None`
/// while nothing answers. The spawned binary resolves the socket exactly
/// like any other one-shot CLI invocation (env inherited).
#[must_use]
pub fn probe_daemon_version(daemon_binary: &Path) -> Option<String> {
    let output = Command::new(daemon_binary)
        .args(["call", "system.status"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_status_version(&String::from_utf8_lossy(&output.stdout))
}

/// Extract the `version` field from a `system.status` result JSON.
fn parse_status_version(stdout: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).ok()?;
    Some(value.get("version")?.as_str()?.to_string())
}

/// Poll `probe` until it reports `target` (leading `v` ignored on both
/// sides) or `timeout` elapses. Always probes at least once; sleeps
/// `poll_interval` between probes.
pub fn wait_for_version(
    target: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut probe: impl FnMut() -> Option<String>,
) -> WaitOutcome {
    let deadline = Instant::now() + timeout;
    let mut last_seen = None;
    loop {
        if let Some(version) = probe() {
            if normalize(&version) == normalize(target) {
                return WaitOutcome::Ready;
            }
            last_seen = Some(version);
        }
        if Instant::now() >= deadline {
            return WaitOutcome::TimedOut { last_seen };
        }
        std::thread::sleep(poll_interval);
    }
}

/// Version comparison key: trimmed, optional leading `v` dropped.
fn normalize(version: &str) -> &str {
    version.trim().trim_start_matches('v')
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAST: Duration = Duration::from_millis(5);

    #[test]
    fn parses_version_from_status_json() {
        let stdout = r#"{
  "listenMode": "uds",
  "version": "0.9.10"
}"#;
        assert_eq!(parse_status_version(stdout), Some("0.9.10".to_string()));
    }

    #[test]
    fn parse_rejects_non_json_and_missing_version() {
        assert_eq!(parse_status_version("cannot connect"), None);
        assert_eq!(parse_status_version(r#"{"listenMode": "uds"}"#), None);
    }

    #[test]
    fn ready_immediately_when_target_answers() {
        let outcome = wait_for_version("0.9.10", FAST, FAST, || Some("0.9.10".to_string()));
        assert_eq!(outcome, WaitOutcome::Ready);
    }

    #[test]
    fn keeps_polling_past_the_old_daemon_then_succeeds() {
        let mut answers = vec![
            None,
            Some("0.9.9".to_string()),
            Some("0.9.9".to_string()),
            Some("0.9.10".to_string()),
        ]
        .into_iter();
        let outcome = wait_for_version("0.9.10", Duration::from_secs(5), FAST, || {
            answers.next().flatten()
        });
        assert_eq!(outcome, WaitOutcome::Ready);
    }

    #[test]
    fn times_out_reporting_the_version_that_kept_answering() {
        let outcome = wait_for_version("0.9.10", Duration::from_millis(30), FAST, || {
            Some("0.9.9".to_string())
        });
        assert_eq!(
            outcome,
            WaitOutcome::TimedOut {
                last_seen: Some("0.9.9".to_string())
            }
        );
    }

    #[test]
    fn times_out_with_no_answer_at_all() {
        let outcome = wait_for_version("0.9.10", Duration::from_millis(30), FAST, || None);
        assert_eq!(outcome, WaitOutcome::TimedOut { last_seen: None });
    }

    #[test]
    fn leading_v_is_ignored_on_both_sides() {
        let outcome = wait_for_version("v0.9.10", FAST, FAST, || Some("0.9.10".to_string()));
        assert_eq!(outcome, WaitOutcome::Ready);
    }

    /// Fake daemon binary: a script whose `call system.status` output is
    /// scripted, exercising the real spawn + parse path.
    #[cfg(unix)]
    fn fake_daemon(dir: &Path, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("intentd");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Polls via [`wait_for_version`] rather than asserting a single probe:
    /// a lone spawn can transiently fail with ETXTBSY when another test's
    /// fork briefly holds the just-written script open (the retry loop is
    /// exactly what production runs anyway).
    #[cfg(unix)]
    #[test]
    fn probe_reads_version_from_a_responding_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let bin = fake_daemon(dir.path(), r#"echo '{ "version": "0.9.10" }'"#);
        let outcome = wait_for_version(
            "0.9.10",
            Duration::from_secs(10),
            Duration::from_millis(10),
            || probe_daemon_version(&bin),
        );
        assert_eq!(outcome, WaitOutcome::Ready);
    }

    #[cfg(unix)]
    #[test]
    fn probe_is_none_while_the_daemon_is_down() {
        let dir = tempfile::tempdir().unwrap();
        let failing = fake_daemon(
            dir.path(),
            "echo 'error: cannot connect to daemon' >&2; exit 1",
        );
        assert_eq!(probe_daemon_version(&failing), None);
        assert_eq!(probe_daemon_version(&dir.path().join("missing")), None);
    }
}
