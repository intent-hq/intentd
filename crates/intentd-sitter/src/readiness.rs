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
/// up without a fresh starting reply. Deliberately shorter than install.sh's 300s first-install
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
    /// The daemon explicitly reported terminal startup failure.
    Failed,
    /// The deadline passed without the target version answering;
    /// `last_seen` is the version that did answer (the old daemon still
    /// shutting down), when any probe succeeded at all.
    TimedOut { last_seen: Option<String> },
}

/// Result of a fresh protected `system.status` request. A startup reply is
/// liveness only; only normal status can establish readiness and version.
#[derive(Debug, PartialEq, Eq)]
pub enum ProbeStatus {
    Ready(String),
    Starting { attempt_id: String },
    Failed,
    Unavailable,
}

/// Fresh protected startup replies renew the silence budget, including a new
/// attempt during an explicit restart. Child/PID ownership and restart nonces
/// remain the caller's authority; no cached snapshot renews this budget.
pub(crate) struct StartupBudget {
    pub(crate) deadline: Instant,
    timeout: Duration,
    attempt: Option<String>,
}

impl StartupBudget {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            timeout,
            attempt: None,
        }
    }

    pub(crate) fn observe(&mut self, status: &ProbeStatus) {
        if let ProbeStatus::Starting { attempt_id } = status {
            if Instant::now() < self.deadline {
                self.attempt = Some(attempt_id.clone());
                self.deadline = Instant::now() + self.timeout;
            }
        }
    }

    pub(crate) fn has_started(&self) -> bool {
        self.attempt.is_some()
    }

    pub(crate) fn probe_timeout(&self) -> Duration {
        self.deadline
            .saturating_duration_since(Instant::now())
            .min(PROBE_TIMEOUT)
    }
}

/// Compatibility helper for callers interested only in ordinary readiness.
#[must_use]
pub fn probe_daemon_version(daemon_binary: &Path) -> Option<String> {
    probe_daemon_version_with_timeout(daemon_binary, PROBE_TIMEOUT)
}

pub(crate) fn probe_daemon_version_with_timeout(
    binary: &Path,
    timeout: Duration,
) -> Option<String> {
    match probe_status(binary, timeout) {
        ProbeStatus::Ready(version) => Some(version),
        _ => None,
    }
}

const MAX_PROBE_OUTPUT: usize = 64 * 1024;

/// Probe through the installed CLI and its normal protected local transport.
/// `call` prints result JSON on stdout, but error JSON on stderr followed by
/// `Error: rpc error` and a nonzero exit. Never discard that structured error.
#[must_use]
pub fn probe_status(binary: &Path, timeout: Duration) -> ProbeStatus {
    fn read_pipe(pipe: impl Read + Send + 'static) -> std::sync::mpsc::Receiver<String> {
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut buf = String::new();
            if pipe
                .take((MAX_PROBE_OUTPUT + 1) as u64)
                .read_to_string(&mut buf)
                .is_ok()
                && buf.len() <= MAX_PROBE_OUTPUT
            {
                let _ = send.send(buf);
            }
        });
        receive
    }
    let Ok(mut child) = Command::new(binary)
        .args(["call", "system.status"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    else {
        return ProbeStatus::Unavailable;
    };
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        let _ = child.kill();
        let _ = child.wait();
        return ProbeStatus::Unavailable;
    };
    let stdout = read_pipe(stdout);
    let stderr = read_pipe(stderr);
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(25)),
            ),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return ProbeStatus::Unavailable;
            }
        }
    };
    // Bounded waits also cover a descendant that inherited either pipe.
    let Ok(stdout) = stdout.recv_timeout(deadline.saturating_duration_since(Instant::now())) else {
        return ProbeStatus::Unavailable;
    };
    let Ok(stderr) = stderr.recv_timeout(deadline.saturating_duration_since(Instant::now())) else {
        return ProbeStatus::Unavailable;
    };
    parse_probe(status.success(), &stdout, &stderr)
}

/// Dropping this future on stop, child exit or deadline kills the probe.
pub(crate) async fn probe_status_async(binary: &Path) -> ProbeStatus {
    use tokio::io::{AsyncRead, AsyncReadExt};
    async fn read_pipe(pipe: impl AsyncRead + Unpin) -> std::io::Result<String> {
        let mut buf = String::new();
        pipe.take((MAX_PROBE_OUTPUT + 1) as u64)
            .read_to_string(&mut buf)
            .await?;
        if buf.len() > MAX_PROBE_OUTPUT {
            return Err(std::io::Error::other("oversized readiness response"));
        }
        Ok(buf)
    }
    let Ok(mut child) = tokio::process::Command::new(binary)
        .args(["call", "system.status"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    else {
        return ProbeStatus::Unavailable;
    };
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    match tokio::time::timeout(PROBE_TIMEOUT, async {
        tokio::try_join!(child.wait(), read_pipe(stdout), read_pipe(stderr))
    })
    .await
    {
        Ok(Ok((status, stdout, stderr))) => parse_probe(status.success(), &stdout, &stderr),
        _ => ProbeStatus::Unavailable,
    }
}

fn parse_probe(success: bool, stdout: &str, stderr: &str) -> ProbeStatus {
    if success {
        let Ok(result) = serde_json::from_str::<serde_json::Value>(stdout) else {
            return ProbeStatus::Unavailable;
        };
        if let Some(ready) = result.get("ready") {
            if ready == false {
                // New CLI compatibility output deliberately succeeds for old
                // sitters. Explicit not-ready MUST precede the version fallback.
                let snapshot = &result["startup"];
                let error = serde_json::json!({"code": -32012, "data": {
                    "code": "daemon-not-ready", "retryable": true,
                    "retryAfterMs": snapshot["retryAfterMs"], "startup": snapshot
                }});
                return parse_probe(false, "", &error.to_string());
            }
            if ready != true {
                return ProbeStatus::Unavailable;
            }
        }
        return parse_status_version(stdout).map_or(ProbeStatus::Unavailable, ProbeStatus::Ready);
    }
    // Decode exactly the first JSON object, allowing the CLI's trailing error
    // diagnostic. Arbitrary log text, auth errors and unsupported schemas fail closed.
    let Some(Ok(error)) = serde_json::Deserializer::from_str(stderr.trim())
        .into_iter::<serde_json::Value>()
        .next()
    else {
        return ProbeStatus::Unavailable;
    };
    let data = &error["data"];
    let snapshot = &data["startup"];
    if error["code"] != -32012 || snapshot["startupStatus"].as_u64() != Some(1) {
        return ProbeStatus::Unavailable;
    }
    let Some(attempt) = snapshot["attemptId"].as_str().filter(|s| !s.is_empty()) else {
        return ProbeStatus::Unavailable;
    };
    if ["elapsedMs", "phaseElapsedMs", "progressAgeMs"]
        .iter()
        .any(|key| {
            snapshot[key]
                .as_u64()
                .is_none_or(|n| n > 9_007_199_254_740_991)
        })
    {
        return ProbeStatus::Unavailable;
    }
    match (
        data["code"].as_str(),
        data["retryable"].as_bool(),
        snapshot["phase"].as_str(),
    ) {
        (Some("daemon-not-ready"), Some(true), Some("preparing" | "indexing" | "finalizing"))
            if snapshot["retryAfterMs"]
                .as_u64()
                .is_some_and(|n| (1000..=30000).contains(&n))
                && data["retryAfterMs"] == snapshot["retryAfterMs"] =>
        {
            ProbeStatus::Starting {
                attempt_id: attempt.to_owned(),
            }
        }
        (Some("daemon-startup-failed"), Some(false), Some("failed")) => ProbeStatus::Failed,
        _ => ProbeStatus::Unavailable,
    }
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
    wait_for_startup_version(target, timeout, poll_interval, |_| {
        probe().map_or(ProbeStatus::Unavailable, ProbeStatus::Ready)
    })
}

/// Wait for normal status from the target version, renewing only on fresh
/// starting responses. The per-probe budget never extends beyond the deadline.
pub fn wait_for_startup_version(
    target: &str,
    timeout: Duration,
    poll_interval: Duration,
    mut probe: impl FnMut(Duration) -> ProbeStatus,
) -> WaitOutcome {
    let mut budget = StartupBudget::new(timeout);
    let mut last_seen = None;
    loop {
        let status = probe(budget.probe_timeout());
        budget.observe(&status);
        match status {
            ProbeStatus::Ready(version) => {
                if versions_match(&version, target) {
                    return WaitOutcome::Ready;
                }
                last_seen = Some(version);
            }
            ProbeStatus::Failed => return WaitOutcome::Failed,
            _ => {}
        }
        if Instant::now() >= budget.deadline {
            return WaitOutcome::TimedOut { last_seen };
        }
        std::thread::sleep(
            poll_interval.min(budget.deadline.saturating_duration_since(Instant::now())),
        );
    }
}

pub(crate) fn versions_match(version: &str, target: &str) -> bool {
    normalize(version) == normalize(target)
}

/// Version comparison key: trimmed, optional leading `v` dropped.
fn normalize(version: &str) -> &str {
    version.trim().trim_start_matches('v')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn starting_error() -> serde_json::Value {
        serde_json::json!({"code":-32012,"data":{"code":"daemon-not-ready","retryable":true,"retryAfterMs":1000,
            "startup":{"startupStatus":1,"attemptId":"boot-a","phase":"indexing","elapsedMs":10,"phaseElapsedMs":10,"progressAgeMs":10,"retryAfterMs":1000}}})
    }

    #[test]
    fn cli_not_ready_takes_precedence_over_matching_version() {
        let error = starting_error();
        let result = serde_json::json!({"version":"0.9.10","ready":false,"startup":error["data"]["startup"]});
        assert_eq!(
            parse_probe(true, &result.to_string(), ""),
            ProbeStatus::Starting {
                attempt_id: "boot-a".into()
            }
        );
        assert_eq!(
            parse_probe(true, r#"{"version":"0.9.10","ready":false}"#, ""),
            ProbeStatus::Unavailable
        );
        assert_eq!(
            parse_probe(true, r#"{"version":"0.9.10","ready":"false"}"#, ""),
            ProbeStatus::Unavailable
        );
    }

    #[test]
    fn cli_error_json_allows_trailing_diagnostic_but_rejects_untrusted_shapes() {
        let error = starting_error();
        assert_eq!(
            parse_probe(false, "", &format!("{error:#}\nError: rpc error\n")),
            ProbeStatus::Starting {
                attempt_id: "boot-a".into()
            }
        );
        for pointer in [
            "/code",
            "/data/code",
            "/data/retryable",
            "/data/startup/startupStatus",
            "/data/startup/attemptId",
            "/data/startup/phase",
            "/data/startup/elapsedMs",
            "/data/startup/retryAfterMs",
        ] {
            let mut invalid = error.clone();
            *invalid.pointer_mut(pointer).unwrap() = serde_json::Value::Null;
            assert_eq!(
                parse_probe(false, "", &invalid.to_string()),
                ProbeStatus::Unavailable,
                "{pointer}"
            );
        }
        assert_eq!(
            parse_probe(false, "", &format!("untrusted log\n{error}")),
            ProbeStatus::Unavailable
        );
        assert_eq!(
            parse_probe(false, r#"{"version":"0.9.10"}"#, "auth denied"),
            ProbeStatus::Unavailable
        );
    }

    #[test]
    fn fresh_replies_renew_across_restart_but_missing_or_late_replies_do_not() {
        let mut budget = StartupBudget::new(Duration::from_secs(1));
        budget.observe(&ProbeStatus::Starting {
            attempt_id: "a".into(),
        });
        budget.deadline = Instant::now() + Duration::from_millis(100);
        let before_restart = budget.deadline;
        budget.observe(&ProbeStatus::Starting {
            attempt_id: "b".into(),
        });
        assert!(budget.deadline > before_restart + Duration::from_millis(500));
        let deadline = budget.deadline;
        budget.observe(&ProbeStatus::Unavailable);
        budget.observe(&ProbeStatus::Ready("0.1".into()));
        assert_eq!(budget.deadline, deadline);
        budget.deadline = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let expired = budget.deadline;
        budget.observe(&ProbeStatus::Starting {
            attempt_id: "a".into(),
        });
        assert_eq!(
            budget.deadline, expired,
            "late result cannot revive expired startup"
        );
    }

    #[test]
    fn terminal_failure_does_not_wait_for_silence_timeout() {
        assert_eq!(
            wait_for_startup_version("0.9.10", Duration::from_secs(60), FAST, |_| {
                ProbeStatus::Failed
            }),
            WaitOutcome::Failed
        );
    }

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
