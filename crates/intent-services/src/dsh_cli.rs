//! DeepSeek Harness (`dsh`) identity, upload guard, and sign-in checks.
//!
//! One probe for `host.providerDiscovery`'s dsh row and the spawn-time
//! refusal in `agent_manager`, so both judge the binary a launch would exec:
//! `dsh --version` must be a DeepSeek Harness version (another tool named
//! `dsh` exists), and the composed launch profile (`--dump-config` with
//! Intent's overlay) must have every upload row disabled
//! ([`intent_providers::dsh::check_dump_config`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use intent_providers::dsh;

const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
const DUMP_TIMEOUT: Duration = Duration::from_secs(30);

/// One resolved-and-probed `dsh` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DshCliStatus {
    pub resolved_path: Option<PathBuf>,
    /// The DeepSeek Harness version, when `--version` reported one.
    pub version: Option<String>,
    /// The upload guard outcome; `None` when it did not run.
    pub guard: Option<Result<(), String>>,
}

impl DshCliStatus {
    /// Why the resolved binary is refused, if it is.
    #[must_use]
    pub fn unavailable_reason(&self) -> Option<String> {
        self.resolved_path.as_ref()?;
        if self.version.is_none() {
            return Some(
                "`dsh --version` did not report a DeepSeek Harness version. Install DeepSeek \
                 Harness with `npm i -g @deepseek-ai/dsh` (it needs Node.js 24.9 or newer), or \
                 point providers.paths.dsh at it."
                    .to_string(),
            );
        }
        match &self.guard {
            Some(Err(reason)) => Some(reason.clone()),
            Some(Ok(())) | None => None,
        }
    }
}

/// Resolve `dsh` (honouring `providers.paths.dsh`) and probe it. Blocking.
#[must_use]
pub fn probe_dsh_cli(override_path: Option<&str>) -> DshCliStatus {
    probe_dsh_cli_at(crate::provider_models::dsh_binary(override_path))
}

/// [`probe_dsh_cli`] on an already resolved launch binary. The guard result
/// is cached per binary and version for the daemon's lifetime. Blocking.
#[must_use]
pub fn probe_dsh_cli_at(resolved_path: Option<PathBuf>) -> DshCliStatus {
    let Some(path) = resolved_path else {
        return DshCliStatus {
            resolved_path: None,
            version: None,
            guard: None,
        };
    };
    let version = run_bounded(&path, &["--version".to_string()], VERSION_TIMEOUT)
        .filter(|out| out.success)
        .and_then(|out| dsh::parse_version_output(&out.stdout));
    let guard = version.as_ref().map(|v| cached_guard(&path, v));
    DshCliStatus {
        resolved_path: Some(path),
        version,
        guard,
    }
}

fn cached_guard(path: &Path, version: &str) -> Result<(), String> {
    static CACHE: OnceLock<Mutex<HashMap<(PathBuf, String), Result<(), String>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (path.to_path_buf(), version.to_string());
    if let Some(hit) = cache.lock().expect("dsh guard cache").get(&key) {
        return hit.clone();
    }
    let outcome = run_guard(path);
    // A probe that could not run is retried next time; a verdict is kept.
    if outcome.as_ref().err().map(String::as_str) != Some(GUARD_DID_NOT_RUN) {
        cache
            .lock()
            .expect("dsh guard cache")
            .insert(key, outcome.clone());
    }
    outcome
}

const GUARD_DID_NOT_RUN: &str = "Intent could not check this DeepSeek Harness version's profile \
     (`dsh --dump-config` failed or timed out), so it does not start DSH agents. Run `dsh \
     --profile acp --dump-config` in a terminal to see the error.";

fn run_guard(path: &Path) -> Result<(), String> {
    let Some(config) = intent_providers::find_provider(dsh::PROVIDER_ID) else {
        return Err(GUARD_DID_NOT_RUN.to_string());
    };
    let mut args =
        intent_providers::build_provider_args(config, &intent_providers::ArgInputs::default());
    args.push(dsh::DUMP_CONFIG_FLAG.to_string());
    match run_bounded(path, &args, DUMP_TIMEOUT) {
        Some(out) if out.success => dsh::check_dump_config(&out.stdout),
        _ => Err(GUARD_DID_NOT_RUN.to_string()),
    }
}

struct BoundedOutput {
    success: bool,
    stdout: String,
}

/// Run `path args…` with the launch env, stdin closed, in its own process
/// group, bounded by `timeout`. The group is killed on every outcome.
fn run_bounded(path: &Path, args: &[String], timeout: Duration) -> Option<BoundedOutput> {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let config = intent_providers::find_provider(dsh::PROVIDER_ID)?;
    let mut cmd = Command::new(path);
    cmd.args(args)
        .env("PATH", intent_providers::enhanced_path(Some(path)))
        .envs(intent_providers::build_provider_env(
            config, None, None, None,
        ))
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().ok()?;
    let pgid = child.id();
    let mut stdout = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Err(_) => break None,
            Ok(None) if start.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    kill_group(pgid);
    let out = reader.join().unwrap_or_default();
    let status = status?;
    Some(BoundedOutput {
        success: status.success(),
        stdout: String::from_utf8_lossy(&out).into_owned(),
    })
}

fn kill_group(pgid: u32) {
    #[cfg(unix)]
    if let Ok(pgid) = i32::try_from(pgid) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(not(unix))]
    let _ = pgid;
}

/// Spawn-time refusal: a binary that is not DeepSeek Harness, or a profile
/// that would still upload sessions.
pub(crate) fn check_dsh_cli_for_spawn(status: &DshCliStatus) -> crate::Result<()> {
    match status.unavailable_reason() {
        Some(reason) => Err(crate::Error::InvalidInput(format!(
            "cannot start DeepSeek Harness agent: {reason}"
        ))),
        None => Ok(()),
    }
}

/// Fold a probed status into the discovery row: `cliCommand`,
/// `cliResolved`, optional `cliResolvedPath` / `cliVersion`, and
/// `installed: false` with `unavailableReason` when refused.
pub(crate) fn apply_dsh_cli_fields(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    installed: &mut bool,
    status: &DshCliStatus,
) {
    obj.insert("cliCommand".into(), serde_json::json!(dsh::COMMAND));
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
    if let Some(version) = &status.version {
        obj.insert("cliVersion".into(), serde_json::json!(version));
    }
    if let Some(reason) = status.unavailable_reason() {
        *installed = false;
        obj.insert("unavailableReason".into(), serde_json::json!(reason));
    }
}

/// Whether a launch would find a DeepSeek API key: in the daemon's
/// environment, the login-shell capture, or the DSH home
/// (`.credentials.yaml` / `.env`). Presence only.
#[must_use]
pub fn api_key_known() -> bool {
    let captured = intent_core::path_utils::login_shell_credential_env();
    let lookup = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| captured.get(key).cloned())
            .filter(|v| !v.trim().is_empty())
    };
    if lookup(dsh::API_KEY_ENV).is_some() {
        return true;
    }
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return false;
    };
    let dsh_home_env = lookup(dsh::HOME_ENV);
    dsh::home_has_api_key(&dsh::dsh_home_in(&home, dsh_home_env.as_deref()))
}

/// Appended to the not-authenticated message.
pub(crate) const DSH_CREDENTIALS_NOTE: &str =
    " DeepSeek Harness also reads DEEPSEEK_API_KEY from the daemon's environment.";

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const DUMP: &str =
        include_str!("../../intent-providers/tests/fixtures/dsh-dump-config-0.2.2-rc.5.txt");

    fn fake_dsh(dir: &Path, version: &str, dump: &str) -> PathBuf {
        let dump_file = dir.join("dump.txt");
        std::fs::write(&dump_file, dump).unwrap();
        let script = dir.join("dsh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\ncase \"$*\" in\n  --version) echo '{version}' ;;\n  *--dump-config*) cat '{}' ;;\n  *) exit 2 ;;\nesac\n",
                dump_file.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script
    }

    #[test]
    fn deepseek_harness_with_overlay_is_available() {
        let dir = tempfile::tempdir().unwrap();
        let status = probe_dsh_cli_at(Some(fake_dsh(dir.path(), "0.2.2-rc.5", DUMP)));
        assert_eq!(status.version.as_deref(), Some("0.2.2-rc.5"));
        assert_eq!(status.guard, Some(Ok(())));
        assert_eq!(status.unavailable_reason(), None);
        assert!(check_dsh_cli_for_spawn(&status).is_ok());
    }

    #[test]
    fn other_dsh_tool_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let status = probe_dsh_cli_at(Some(fake_dsh(
            dir.path(),
            "Distributed Shell / Dancer's shell. version 0.25.10",
            DUMP,
        )));
        assert_eq!(status.version, None);
        let reason = status.unavailable_reason().unwrap();
        assert!(reason.contains("npm i -g @deepseek-ai/dsh"), "{reason}");
        assert!(check_dsh_cli_for_spawn(&status).is_err());
    }

    #[test]
    fn profile_that_still_uploads_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let dump = DUMP.replace(
            "  name: '@deepseek-ai/dsh-session-log-deepseek'\n  disabled: true",
            "  name: '@deepseek-ai/dsh-session-log-deepseek'",
        );
        let status = probe_dsh_cli_at(Some(fake_dsh(dir.path(), "9.9.9-test.1", &dump)));
        let reason = status.unavailable_reason().unwrap();
        assert!(reason.contains("session-log-deepseek"), "{reason}");
        let err = check_dsh_cli_for_spawn(&status).unwrap_err().to_string();
        assert!(err.contains("cannot start DeepSeek Harness agent"), "{err}");
    }

    #[test]
    fn failed_dump_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("dsh");
        std::fs::write(
            &script,
            "#!/bin/sh\n[ \"$1\" = --version ] && { echo 0.3.0; exit 0; }\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let status = probe_dsh_cli_at(Some(script));
        assert_eq!(
            status.unavailable_reason().as_deref(),
            Some(GUARD_DID_NOT_RUN)
        );
    }

    #[test]
    fn guard_runs_with_the_launch_overlay_and_env() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("argv.txt");
        let script = dir.path().join("dsh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n[ \"$1\" = --version ] && {{ echo 0.4.0; exit 0; }}\necho \"$* telemetry=$DSH_TELEMETRY_DISABLED\" > '{}'\ncat \"$4\"\n",
                log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let status = probe_dsh_cli_at(Some(script));
        let argv = std::fs::read_to_string(&log).unwrap();
        assert!(argv.starts_with("--profile acp --patch "), "{argv}");
        assert!(argv.contains(" --dump-config telemetry=1"), "{argv}");
        assert_eq!(
            status.guard,
            Some(Ok(())),
            "the overlay alone disables every upload row"
        );
    }

    #[test]
    fn missing_binary_is_not_refused() {
        let status = probe_dsh_cli_at(None);
        assert_eq!(status.unavailable_reason(), None);
    }
}
