//! Shared Cursor CLI resolution + identity probe.
//!
//! One path for `host.providerDiscovery`'s cursor row and the spawn-time
//! fail-fast in `agent_manager`, so both judge the binary the spawned child
//! would actually exec. Path-level Grok agent host rejection happens in
//! discovery ([`intent_providers::cursor`]); this probe adds the
//! `--version` check for a Grok host that is not recognisable by path.

use std::path::PathBuf;

use intent_providers::cursor::{
    classify_cursor_version, grok_agent_host_reason, CursorCliIdentity,
};

/// One resolved-and-probed `cursor-agent` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorCliStatus {
    /// Where `cursor-agent` resolved (override, PATH, or versioned install).
    pub resolved_path: Option<PathBuf>,
    /// The `--version` identity, when the probe produced output.
    pub identity: Option<CursorCliIdentity>,
}

impl CursorCliStatus {
    /// The version string to report: the Cursor date-hash version, or the
    /// raw line for an unrecognized or Grok host answer.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        match self.identity.as_ref()? {
            CursorCliIdentity::Cursor(v)
            | CursorCliIdentity::GrokAgentHost(v)
            | CursorCliIdentity::Unrecognized(v) => Some(v),
        }
    }

    /// The reason the resolved binary is refused: only the Grok agent host.
    /// A failed or unrecognized probe stays permissive (a changed version
    /// format must not take the provider away).
    #[must_use]
    pub fn unavailable_reason(&self) -> Option<String> {
        match self.identity.as_ref()? {
            CursorCliIdentity::GrokAgentHost(line) => Some(grok_agent_host_reason(line)),
            CursorCliIdentity::Cursor(_) | CursorCliIdentity::Unrecognized(_) => None,
        }
    }
}

/// Resolve `cursor-agent` (honouring `providers.paths.cursor`) and run
/// `--version` with a 3s budget. Blocking — call from a blocking context.
#[must_use]
pub fn probe_cursor_cli(override_path: Option<&str>) -> CursorCliStatus {
    probe_cursor_cli_at(intent_providers::find_provider_binary(
        "cursor",
        intent_providers::cursor::CURSOR_COMMAND,
        override_path,
    ))
}

/// [`probe_cursor_cli`] on an already resolved launch binary (the spawn
/// path's `provider_binary`). Blocking.
#[must_use]
pub fn probe_cursor_cli_at(resolved_path: Option<PathBuf>) -> CursorCliStatus {
    let identity = resolved_path
        .as_deref()
        .and_then(run_cursor_version_probe)
        .map(|line| classify_cursor_version(&line));
    if let Some(CursorCliIdentity::Unrecognized(line)) = &identity {
        tracing::warn!(
            version_output = %line,
            "cursor-agent --version output is not a Cursor version; not gating the cursor provider"
        );
    }
    CursorCliStatus {
        resolved_path,
        identity,
    }
}

/// Kill every process left in a Cursor CLI's process group (the child was
/// spawned with `process_group(0)`, so its pid is the group id). The CLI's
/// own children may outlive it and `cursor-agent` ignores stdin EOF, so
/// every run ends with this, on success, error and timeout alike.
fn kill_cursor_group(pgid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pgid) = pgid.and_then(|p| i32::try_from(p).ok()) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(not(unix))]
    let _ = pgid;
}

/// Run one short Cursor CLI command (`models`, `status --format json`) in
/// its own process group with plain-output env, bounded by `timeout`; the
/// whole group is killed afterwards on every outcome. `None` on spawn
/// failure or timeout.
pub(crate) async fn run_cursor_cli(
    mut cmd: tokio::process::Command,
    timeout: std::time::Duration,
) -> Option<std::process::Output> {
    cmd.envs(
        intent_providers::cursor::CURSOR_PLAIN_OUTPUT_ENV
            .iter()
            .copied(),
    )
    .stdin(std::process::Stdio::null())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd.spawn().ok()?;
    let pgid = child.id();
    // Read the pipes concurrently and wait for the CLI itself, not for pipe
    // EOF: a child the CLI leaves behind would keep the pipes open.
    let read = |pipe: Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>| {
        // caller-binding: allow — only drains a child pipe into a buffer; never reaches a capability gate
        tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = tokio::io::AsyncReadExt::read_to_end(&mut pipe, &mut buf).await;
            }
            buf
        })
    };
    let stdout = read(child.stdout.take().map(|p| Box::new(p) as _));
    let stderr = read(child.stderr.take().map(|p| Box::new(p) as _));
    let status = tokio::time::timeout(timeout, child.wait()).await;
    kill_cursor_group(pgid);
    let Ok(Ok(status)) = status else {
        let _ = child.kill().await;
        return None;
    };
    let collect = |task: tokio::task::JoinHandle<Vec<u8>>| async move {
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default()
    };
    Some(std::process::Output {
        status,
        stdout: collect(stdout).await,
        stderr: collect(stderr).await,
    })
}

/// `cursor-agent --version` with a 3s budget, in its own process group that
/// is killed afterwards (the shared pi probe kills only the leader).
fn run_cursor_version_probe(path: &std::path::Path) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let mut cmd = Command::new(path);
    cmd.arg("--version")
        .envs(
            intent_providers::cursor::CURSOR_PLAIN_OUTPUT_ENV
                .iter()
                .copied(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().ok()?;
    let pgid = Some(child.id());
    let start = Instant::now();
    let line = loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                // Kill the group first: a child left behind would hold the
                // stdout pipe open and block the read.
                kill_cursor_group(pgid);
                let mut out = Vec::new();
                child.stdout.take()?.read_to_end(&mut out).ok()?;
                break String::from_utf8_lossy(&out)
                    .lines()
                    .next()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(str::to_owned);
            }
            Ok(Some(_)) | Err(_) => break None,
            Ok(None) if start.elapsed() >= Duration::from_secs(3) => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    kill_cursor_group(pgid);
    line
}

/// The refusal reason when `providers.paths.cursor` points at the Grok
/// agent host (see [`intent_providers::cursor::grok_host_override_reason`]).
/// Shared by discovery and spawn so a refused override is never silently
/// replaced by another `cursor-agent`.
#[must_use]
pub fn grok_host_override_reason(override_path: Option<&str>) -> Option<String> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    override_path.and_then(|path| {
        intent_providers::cursor::grok_host_override_reason(
            path,
            &intent_providers::cursor::CursorHostDirs::from_env(home.as_deref()),
        )
    })
}

/// Spawn-time refusal of a Grok agent host override (the same actionable
/// reason the discovery row shows).
pub(crate) fn check_cursor_override_for_spawn(override_path: Option<&str>) -> crate::Result<()> {
    match grok_host_override_reason(override_path) {
        Some(reason) => Err(crate::Error::InvalidInput(format!(
            "cannot start Cursor agent: {reason}"
        ))),
        None => Ok(()),
    }
}

/// Spawn-time fail-fast: refuse to launch the Grok agent host as Cursor.
pub(crate) fn check_cursor_cli_for_spawn(status: &CursorCliStatus) -> crate::Result<()> {
    match status.unavailable_reason() {
        // InvalidInput so the message survives the JSON-RPC envelope.
        Some(reason) => Err(crate::Error::InvalidInput(format!(
            "cannot start Cursor agent: {reason}"
        ))),
        None => Ok(()),
    }
}

/// Fold a probed status into the discovery row: `cliCommand`,
/// `cliResolved`, optional `cliResolvedPath` / `cliVersion`, and — only for
/// the Grok agent host — `installed: false` with `unavailableReason`.
pub(crate) fn apply_cursor_cli_fields(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    installed: &mut bool,
    status: &CursorCliStatus,
) {
    obj.insert(
        "cliCommand".into(),
        serde_json::json!(intent_providers::cursor::CURSOR_COMMAND),
    );
    obj.insert(
        "cliResolved".into(),
        serde_json::json!(status.resolved_path.is_some()),
    );
    if let Some(path) = &status.resolved_path {
        obj.insert(
            "cliResolvedPath".into(),
            serde_json::json!(path.display().to_string()),
        );
    }
    if let Some(version) = status.version() {
        obj.insert("cliVersion".into(), serde_json::json!(version));
    }
    if let Some(reason) = status.unavailable_reason() {
        *installed = false;
        obj.insert("unavailableReason".into(), serde_json::json!(reason));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(identity: Option<CursorCliIdentity>) -> CursorCliStatus {
        CursorCliStatus {
            resolved_path: Some(PathBuf::from("/home/u/.local/bin/cursor-agent")),
            identity,
        }
    }

    #[test]
    fn cursor_version_is_reported_and_allowed() {
        let s = status(Some(CursorCliIdentity::Cursor("2026.11.05-86047df".into())));
        let mut obj = serde_json::Map::new();
        let mut installed = true;
        apply_cursor_cli_fields(&mut obj, &mut installed, &s);
        assert!(installed);
        assert_eq!(obj["cliCommand"], "cursor-agent");
        assert_eq!(obj["cliResolved"], true);
        assert_eq!(obj["cliResolvedPath"], "/home/u/.local/bin/cursor-agent");
        assert_eq!(obj["cliVersion"], "2026.11.05-86047df");
        assert!(!obj.contains_key("unavailableReason"));
        assert!(check_cursor_cli_for_spawn(&s).is_ok());
    }

    #[test]
    fn grok_agent_host_is_unavailable_and_refused_at_spawn() {
        let s = status(Some(CursorCliIdentity::GrokAgentHost(
            "grok 0.3.1 (abc) [unified]".into(),
        )));
        let mut obj = serde_json::Map::new();
        let mut installed = true;
        apply_cursor_cli_fields(&mut obj, &mut installed, &s);
        assert!(!installed);
        let reason = obj["unavailableReason"].as_str().unwrap();
        assert!(reason.contains("Grok agent host"), "{reason}");
        let err = check_cursor_cli_for_spawn(&s).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidInput(_)));
        assert!(err.to_string().contains("cannot start Cursor agent"));
    }

    #[test]
    fn failed_or_unrecognized_probe_is_permissive() {
        for identity in [None, Some(CursorCliIdentity::Unrecognized("v9".into()))] {
            let s = status(identity);
            let mut obj = serde_json::Map::new();
            let mut installed = true;
            apply_cursor_cli_fields(&mut obj, &mut installed, &s);
            assert!(installed);
            assert!(!obj.contains_key("unavailableReason"));
            assert!(check_cursor_cli_for_spawn(&s).is_ok());
        }
    }

    /// A `providers.paths.cursor` override that resolves to the Grok agent
    /// host makes the discovery row unavailable with an actionable reason,
    /// whatever auto-discovery finds on this host, and the spawn/auth
    /// resolver refuses it instead of falling back.
    #[cfg(unix)]
    #[test]
    fn grok_host_override_is_unavailable_in_discovery() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let host = dir.path().join("agent-host-0.3.1-linux-x64");
        std::fs::write(&host, "#!/bin/sh\necho 'grok 0.3.1'\n").unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o755)).unwrap();
        let paths =
            std::collections::HashMap::from([("cursor".to_string(), host.display().to_string())]);
        let discovery = crate::discover_providers_with_npx_overrides(&paths);
        let row = discovery["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == "cursor")
            .unwrap();
        assert_eq!(row["installed"], false, "{row}");
        let reason = row["unavailableReason"].as_str().unwrap();
        assert!(reason.contains("Grok agent host"), "{reason}");
        assert!(reason.contains("providers.paths.cursor"), "{reason}");
        assert_eq!(
            intent_providers::find_provider_binary("cursor", "cursor-agent", host.to_str()),
            None
        );
    }

    /// Spawn refuses a Grok agent host override with the discovery reason
    /// instead of falling back to another `cursor-agent`.
    #[cfg(unix)]
    #[test]
    fn spawn_refuses_a_grok_host_override() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let host = dir.path().join("agent-host-0.3.1-linux-x64");
        std::fs::write(&host, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = check_cursor_override_for_spawn(host.to_str()).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidInput(_)));
        let msg = err.to_string();
        assert!(msg.contains("cannot start Cursor agent"), "{msg}");
        assert!(msg.contains("Grok agent host"), "{msg}");
        assert!(msg.contains("providers.paths.cursor"), "{msg}");
        assert!(check_cursor_override_for_spawn(None).is_ok());
        let cursor = dir.path().join("cursor-agent");
        std::fs::write(&cursor, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&cursor, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(check_cursor_override_for_spawn(cursor.to_str()).is_ok());
    }

    /// Whether `pid` still runs (a zombie counts as gone).
    #[cfg(unix)]
    fn alive(pid: i32) -> bool {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
            && std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |stat| {
                stat.rsplit_once(") ")
                    .is_none_or(|(_, rest)| !rest.starts_with('Z'))
            })
    }

    /// Wait (bounded) until none of `pids` runs.
    #[cfg(unix)]
    fn assert_all_gone(pids: &[i32], what: &str) {
        let deadline = std::time::Instant::now()
            + intentd_test_support::test_timeout(std::time::Duration::from_secs(5));
        while pids.iter().any(|p| alive(*p)) {
            assert!(
                std::time::Instant::now() < deadline,
                "{what}: a Cursor child survived: {pids:?}"
            );
            // timing-guard: poll interval for killed processes to disappear (bounded above).
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// A fake `cursor-agent` that leaves a SIGTERM-ignoring child behind
    /// (like the real CLI's workers), records its pid, then runs `tail`.
    #[cfg(unix)]
    fn fake_cli(dir: &std::path::Path, name: &str, tail: &str) -> (PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let pid_file = dir.join(format!("{name}.pid"));
        let script = dir.join(name);
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nsh -c 'trap \"\" TERM; sleep 300' &\necho $! > '{}'\n{tail}\n",
                pid_file.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        (script, pid_file)
    }

    #[cfg(unix)]
    fn recorded_pid(pid_file: &std::path::Path) -> i32 {
        let deadline = std::time::Instant::now()
            + intentd_test_support::test_timeout(std::time::Duration::from_secs(5));
        loop {
            if let Some(pid) = std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|s| s.trim().parse().ok())
            {
                return pid;
            }
            assert!(std::time::Instant::now() < deadline, "no pid recorded");
            // timing-guard: poll interval until the fake CLI records its child (bounded above).
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// `models` / `status` runs leave no process behind on success, error
    /// or timeout, even when the CLI's own child ignores SIGTERM.
    #[cfg(unix)]
    #[tokio::test]
    async fn short_cli_runs_kill_the_whole_group_on_every_exit() {
        let dir = tempfile::tempdir().unwrap();
        for (name, tail, timeout_ms, expect_output) in [
            ("ok", "echo 'auto - Auto'; exit 0", 5_000, true),
            (
                "err",
                "echo 'Error: Authentication required.' >&2; exit 1",
                5_000,
                true,
            ),
            ("hang", "sleep 300", 300, false),
        ] {
            let (script, pid_file) = fake_cli(dir.path(), name, tail);
            let out = run_cursor_cli(
                tokio::process::Command::new(&script),
                std::time::Duration::from_millis(timeout_ms),
            )
            .await;
            assert_eq!(out.is_some(), expect_output, "{name}");
            assert_all_gone(&[recorded_pid(&pid_file)], name);
        }
    }

    #[cfg(unix)]
    #[test]
    fn version_probe_kills_the_whole_group() {
        let dir = tempfile::tempdir().unwrap();
        for (name, tail) in [("v-ok", "echo 2026.11.05-86047df"), ("v-hang", "sleep 300")] {
            let (script, pid_file) = fake_cli(dir.path(), name, tail);
            let line = run_cursor_version_probe(&script);
            assert_eq!(line.is_some(), name == "v-ok", "{name}");
            assert_all_gone(&[recorded_pid(&pid_file)], name);
        }
    }

    /// The session-free model probe (`cursor-agent acp` + list models) kills
    /// the mock CLI and its SIGTERM-ignoring worker on success and on the
    /// signed-out error, although the CLI ignores stdin EOF.
    #[cfg(unix)]
    #[tokio::test]
    async fn model_probe_leaves_no_cursor_process() {
        use std::os::unix::fs::PermissionsExt;
        let Some(node) = intent_providers::resolve_on_path("node") else {
            eprintln!("skipping: node not on PATH");
            return;
        };
        let mock = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../intentd/tests/fixtures/mock-cursor-agent.mjs");
        let dir = tempfile::tempdir().unwrap();
        for signed_out in [false, true] {
            let pid_file = dir.path().join(format!("acp-{signed_out}.pid"));
            let wrapper = dir.path().join(format!("cursor-agent-{signed_out}"));
            std::fs::write(
                &wrapper,
                format!(
                    "#!/bin/sh\nexport MOCK_CURSOR_PID_FILE='{}'\nexport MOCK_CURSOR_SIGNED_OUT='{}'\nexec '{}' '{}' \"$@\"\n",
                    pid_file.display(),
                    if signed_out { "1" } else { "0" },
                    node.display(),
                    mock.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
            let fetch = crate::provider_models::fetch_cursor_models_at(Some(wrapper)).await;
            assert_eq!(fetch.models.is_some(), !signed_out, "{fetch:?}");
            let pids: Vec<i32> = std::fs::read_to_string(&pid_file)
                .unwrap()
                .split_whitespace()
                .map(|p| p.parse().unwrap())
                .collect();
            assert_eq!(pids.len(), 2, "main and worker pids");
            assert_all_gone(&pids, if signed_out { "signed out" } else { "success" });
        }
    }

    /// End to end over a fake `cursor-agent` on an explicit override path:
    /// resolve → `--version` → classify, without touching PATH or HOME.
    #[cfg(unix)]
    #[test]
    fn probe_classifies_a_fake_grok_host_binary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("cursor-agent");
        std::fs::write(&fake, "#!/bin/sh\necho 'grok 0.3.1 (abc) [unified]'\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let s = probe_cursor_cli(fake.to_str());
        assert_eq!(s.resolved_path.as_deref(), Some(fake.as_path()));
        assert!(matches!(
            s.identity,
            Some(CursorCliIdentity::GrokAgentHost(_))
        ));
        std::fs::write(&fake, "#!/bin/sh\necho 2026.11.05-86047df\n").unwrap();
        let s = probe_cursor_cli(fake.to_str());
        assert_eq!(s.version(), Some("2026.11.05-86047df"));
        assert!(s.unavailable_reason().is_none());
    }
}
