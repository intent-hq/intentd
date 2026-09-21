//! Post-restart readiness wait for `intentd update`.
//!
//! After an update installs a new daemon and SIGHUPs the serve-mode sitter,
//! the old daemon takes a moment to shut down and the new one to recreate
//! its socket — a chained command (`intentd update && intentd pair`) races
//! that window (intent-hq/intent#4276). This module polls the freshly
//! installed daemon binary's `call system.status` until the daemon
//! answering on the socket reports the target version, mirroring
//! install.sh's "waiting for the daemon to respond..." verification.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// How long the update path waits for the restarted daemon before giving
/// up. Deliberately shorter than install.sh's 300s first-install
/// verification budget: an update restarts an already-configured daemon,
/// which answers within seconds when healthy.
///
/// The budget covers the whole restart, so it must stay above the
/// supervisor's [`crate::supervisor::SupervisorConfig::kill_timeout`] (30s
/// — how long a graceful stop waits before force-killing the old daemon)
/// plus new-daemon startup: a SIGTERM-slow old daemon eats that much of it
/// first. Revisit this constant if `kill_timeout` grows.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Test-only env override (integer milliseconds) for [`DEFAULT_TIMEOUT`],
/// matching the `INTENTD_SITTER_*_MS` convention of
/// [`crate::supervisor::SupervisorConfig::from_env`], so integration tests
/// can exercise the timeout arm without waiting a minute. Production never
/// sets it.
pub const TIMEOUT_ENV: &str = "INTENTD_SITTER_READINESS_TIMEOUT_MS";

/// Pause between readiness probes.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Hard ceiling on a single probe subprocess. `intentd call` has no
/// client-side RPC timeout, so a daemon that accepts the connection but
/// stalls before responding would otherwise hang the probe — and with it
/// the whole wait — forever, never reaching [`DEFAULT_TIMEOUT`]. A hung
/// probe is killed and counts as "no answer"; the overall wait can thus
/// overshoot its deadline by at most this much.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// [`DEFAULT_TIMEOUT`] with any [`TIMEOUT_ENV`] override applied.
#[must_use]
pub fn timeout_from_env() -> Duration {
    timeout_from_lookup(|name| std::env::var(name).ok())
}

/// [`timeout_from_env`] with an injectable lookup so tests never mutate
/// process state. Unset, empty, or unparseable values keep the default.
fn timeout_from_lookup(get: impl Fn(&str) -> Option<String>) -> Duration {
    get(TIMEOUT_ENV)
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(DEFAULT_TIMEOUT, Duration::from_millis)
}

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
/// while nothing answers (including a probe killed at [`PROBE_TIMEOUT`]).
/// The spawned binary resolves the socket exactly like any other one-shot
/// CLI invocation (env inherited).
#[must_use]
pub fn probe_daemon_version(daemon_binary: &Path) -> Option<String> {
    probe_daemon_version_with_timeout(daemon_binary, PROBE_TIMEOUT)
}

/// [`probe_daemon_version`] with an explicit per-probe deadline (split out
/// so tests can exercise the kill path without waiting the real ceiling).
fn probe_daemon_version_with_timeout(daemon_binary: &Path, timeout: Duration) -> Option<String> {
    let mut child = Command::new(daemon_binary)
        .args(["call", "system.status"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Drain stdout on a helper thread so a chatty child can never fill the
    // pipe and deadlock against our exit-polling loop. On the kill paths
    // the reader is dropped (detached), not joined: a grandchild holding
    // the inherited write end would keep the pipe open past the kill, and
    // joining would trade the subprocess hang for a thread hang.
    let mut pipe = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = pipe.read_to_string(&mut buf);
        buf
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    let stdout = reader.join().ok()?;
    if !status.success() {
        return None;
    }
    parse_status_version(&stdout)
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
    fn timeout_override_applies_only_for_a_parseable_value() {
        assert_eq!(
            timeout_from_lookup(|_| Some("250".to_string())),
            Duration::from_millis(250)
        );
        assert_eq!(timeout_from_lookup(|_| None), DEFAULT_TIMEOUT);
        assert_eq!(
            timeout_from_lookup(|_| Some(String::new())),
            DEFAULT_TIMEOUT
        );
        assert_eq!(
            timeout_from_lookup(|_| Some("soon".to_string())),
            DEFAULT_TIMEOUT
        );
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

    /// Regression: `intentd call` has no client-side RPC timeout, so a
    /// daemon that accepts the connection but never responds used to hang
    /// the probe (and the whole wait) forever. The probe now kills the
    /// subprocess at its deadline and reports "no answer".
    #[cfg(unix)]
    #[test]
    fn probe_kills_a_hung_subprocess_at_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        // Writes a valid status then stalls: only the timeout path returns.
        // The exec'd sleep IS the child (no intermediate shell), so the
        // kill reaches it and nothing outlives the test.
        let hung = fake_daemon(
            dir.path(),
            r#"echo '{ "version": "0.9.10" }'; exec sleep 600"#,
        );
        let started = Instant::now();
        let probed = probe_daemon_version_with_timeout(&hung, Duration::from_millis(200));
        assert_eq!(probed, None);
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "probe must not wait for the hung child"
        );
    }
}
