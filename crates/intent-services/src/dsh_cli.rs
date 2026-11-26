//! DeepSeek Harness (`dsh`) identity, upload guard, and sign-in checks.
//!
//! One probe for `host.providerDiscovery`'s dsh row and the spawn-time
//! refusal in `agent_manager`, so both judge the binary a launch would exec:
//! `dsh --version` must be a DeepSeek Harness version (another tool named
//! `dsh` exists), and the composed launch profile (`--dump-config` with
//! Intent's overlay) must have every upload row disabled
//! ([`intent_providers::dsh::check_dump_config`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
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

/// Resolve `dsh` (honouring `providers.paths.dsh`) and probe it under the
/// Default launch environment. Blocking.
#[must_use]
pub fn probe_dsh_cli(override_path: Option<&str>) -> DshCliStatus {
    probe_dsh_cli_at(crate::provider_models::dsh_binary(override_path), None)
}

/// [`probe_dsh_cli`] on an already resolved launch binary, under the
/// environment a launch with `profile` composes ([`launch_env`]). The guard
/// verdict is cached per binary, version, effective `DSH_*` environment and
/// DSH patch-file state, so a profile edit is checked again. Blocking.
#[must_use]
pub fn probe_dsh_cli_at(
    resolved_path: Option<PathBuf>,
    profile: Option<&intent_acp::ProfileEnv>,
) -> DshCliStatus {
    let Some(path) = resolved_path else {
        return DshCliStatus {
            resolved_path: None,
            version: None,
            guard: None,
        };
    };
    let env = launch_env(profile);
    let version = run_bounded(&path, &["--version".to_string()], &env, VERSION_TIMEOUT)
        .filter(|out| out.success)
        .and_then(|out| dsh::parse_version_output(&out.stdout));
    let guard = version.as_ref().map(|v| cached_guard(&path, v, &env));
    DshCliStatus {
        resolved_path: Some(path),
        version,
        guard,
    }
}

/// The environment changes a DSH launch makes on top of the daemon's own,
/// composed in `intent_acp::spawn` order: the profile overlay, then the
/// provider env, then login-shell captured variables filling only gaps.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DshLaunchEnv {
    sets: BTreeMap<String, String>,
    removes: BTreeSet<String>,
}

impl DshLaunchEnv {
    pub(crate) fn sets(&self) -> impl Iterator<Item = (&str, &str)> {
        self.sets.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub(crate) fn removes(&self) -> impl Iterator<Item = &str> {
        self.removes.iter().map(String::as_str)
    }

    fn value(&self, key: &str) -> Option<String> {
        if self.removes.contains(key) {
            return None;
        }
        self.sets
            .get(key)
            .cloned()
            .or_else(|| std::env::var(key).ok())
    }

    /// Hash of every effective `DSH_*` variable plus the state of the patch
    /// files the composed profile reads. Values stay in memory, unlogged.
    fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut keys: BTreeSet<String> = std::env::vars_os()
            .filter_map(|(k, _)| k.into_string().ok())
            .chain(self.sets.keys().cloned())
            .filter(|k| k.starts_with("DSH_"))
            .collect();
        keys.retain(|k| !self.removes.contains(k));
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for key in keys {
            (key.as_str(), self.value(&key)).hash(&mut hasher);
        }
        if let Some(home) = self.value("HOME").map(PathBuf::from) {
            let dsh_home = dsh::dsh_home_in(&home, self.value(dsh::HOME_ENV).as_deref());
            let profile_dir = dsh_home.join("profiles").join("acp");
            for file in [
                dsh_home.join("cordis.patch.yml"),
                profile_dir.join("cordis.patch.yml"),
                profile_dir.join("cordis.yml"),
                profile_dir.join("package.json"),
            ] {
                let stamp = std::fs::metadata(&file)
                    .ok()
                    .map(|m| (m.len(), m.modified().ok()));
                (file, stamp).hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}

/// [`DshLaunchEnv`] for a launch with `profile`, against this daemon.
pub(crate) fn launch_env(profile: Option<&intent_acp::ProfileEnv>) -> DshLaunchEnv {
    let provider_env = intent_providers::find_provider(dsh::PROVIDER_ID)
        .map(|config| intent_providers::build_provider_env(config, None, None, None))
        .unwrap_or_default();
    compose_launch_env(
        &provider_env,
        profile,
        intent_core::path_utils::login_shell_credential_env(),
        |key| std::env::var_os(key).is_some(),
    )
}

fn compose_launch_env(
    provider_env: &BTreeMap<String, String>,
    profile: Option<&intent_acp::ProfileEnv>,
    captured: &BTreeMap<String, String>,
    daemon_has: impl Fn(&str) -> bool,
) -> DshLaunchEnv {
    let mut env = DshLaunchEnv::default();
    if let Some(profile) = profile {
        for (key, value) in profile.entries() {
            if let Some(value) = value {
                env.removes.remove(key);
                env.sets.insert(key.to_string(), value.to_string());
            } else {
                env.sets.remove(key);
                env.removes.insert(key.to_string());
            }
        }
    }
    for (key, value) in provider_env {
        env.removes.remove(key);
        env.sets.insert(key.clone(), value.clone());
    }
    for (key, value) in captured {
        if provider_env.contains_key(key)
            || profile.is_some_and(|p| p.controls(key))
            || daemon_has(key)
        {
            continue;
        }
        env.sets.insert(key.clone(), value.clone());
    }
    env
}

type GuardKey = (PathBuf, String, u64);

fn cached_guard(path: &Path, version: &str, env: &DshLaunchEnv) -> Result<(), String> {
    static CACHE: OnceLock<Mutex<HashMap<GuardKey, Result<(), String>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (path.to_path_buf(), version.to_string(), env.fingerprint());
    if let Some(hit) = cache.lock().expect("dsh guard cache").get(&key) {
        return hit.clone();
    }
    let outcome = run_guard(path, env);
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

fn run_guard(path: &Path, env: &DshLaunchEnv) -> Result<(), String> {
    let Some(config) = intent_providers::find_provider(dsh::PROVIDER_ID) else {
        return Err(GUARD_DID_NOT_RUN.to_string());
    };
    let mut args =
        intent_providers::build_provider_args(config, &intent_providers::ArgInputs::default());
    args.push(dsh::DUMP_CONFIG_FLAG.to_string());
    match run_bounded(path, &args, env, DUMP_TIMEOUT) {
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
fn run_bounded(
    path: &Path,
    args: &[String],
    env: &DshLaunchEnv,
    timeout: Duration,
) -> Option<BoundedOutput> {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(path);
    cmd.args(args);
    for key in env.removes() {
        cmd.env_remove(key);
    }
    cmd.envs(env.sets())
        .env("PATH", intent_providers::enhanced_path(Some(path)))
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

/// Whether a Default launch would find a DeepSeek API key: in the daemon's
/// environment, the login-shell capture, or the DSH home
/// (`.credentials.yaml` / `.env`). Presence only.
#[must_use]
pub fn api_key_known() -> bool {
    api_key_known_in(&launch_env(None))
}

/// [`api_key_known`] for the environment a launch composes (a key profile's
/// overlay sets the key and its own DSH home).
#[must_use]
pub(crate) fn api_key_known_in(env: &DshLaunchEnv) -> bool {
    if env
        .value(dsh::API_KEY_ENV)
        .is_some_and(|v| !v.trim().is_empty())
    {
        return true;
    }
    let Some(home) = env.value("HOME").map(PathBuf::from) else {
        return false;
    };
    dsh::home_has_api_key(&dsh::dsh_home_in(
        &home,
        env.value(dsh::HOME_ENV).as_deref(),
    ))
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
        let status = probe_dsh_cli_at(Some(fake_dsh(dir.path(), "0.2.2-rc.5", DUMP)), None);
        assert_eq!(status.version.as_deref(), Some("0.2.2-rc.5"));
        assert_eq!(status.guard, Some(Ok(())));
        assert_eq!(status.unavailable_reason(), None);
        assert!(check_dsh_cli_for_spawn(&status).is_ok());
    }

    #[test]
    fn other_dsh_tool_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let status = probe_dsh_cli_at(
            Some(fake_dsh(
                dir.path(),
                "Distributed Shell / Dancer's shell. version 0.25.10",
                DUMP,
            )),
            None,
        );
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
        let status = probe_dsh_cli_at(Some(fake_dsh(dir.path(), "9.9.9-test.1", &dump)), None);
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
        let status = probe_dsh_cli_at(Some(script), None);
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
        let status = probe_dsh_cli_at(Some(script), None);
        let argv = std::fs::read_to_string(&log).unwrap();
        assert!(argv.starts_with("--profile acp --patch "), "{argv}");
        assert!(argv.contains(" --dump-config telemetry=1"), "{argv}");
        assert_eq!(
            status.guard,
            Some(Ok(())),
            "the overlay alone disables every upload row"
        );
    }

    fn uploading_dump() -> String {
        DUMP.replace(
            "  name: '@deepseek-ai/dsh-session-log-deepseek'\n  disabled: true",
            "  name: '@deepseek-ai/dsh-session-log-deepseek'",
        )
    }

    fn write_script(path: &Path, body: &str) {
        std::fs::write(path, format!("#!/bin/sh\n{body}")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn launch_env_follows_spawn_precedence() {
        let provider_env =
            BTreeMap::from([(dsh::TELEMETRY_DISABLED_ENV.to_string(), "1".to_string())]);
        let captured = BTreeMap::from([
            ("DSH_HOME".to_string(), "/captured/dsh".to_string()),
            (dsh::API_KEY_ENV.to_string(), "sk-captured".to_string()),
            ("DSH_EXTRA".to_string(), "captured".to_string()),
        ]);
        let profile = intent_acp::ProfileEnv::new(
            "dsh@work",
            "fp",
            vec![
                ("DSH_EXTRA".to_string(), None),
                (
                    dsh::TELEMETRY_DISABLED_ENV.to_string(),
                    Some("0".to_string()),
                ),
            ],
        );
        let env = compose_launch_env(&provider_env, Some(&profile), &captured, |key| {
            key == dsh::API_KEY_ENV
        });
        assert_eq!(
            env.sets.get("DSH_HOME").map(String::as_str),
            Some("/captured/dsh")
        );
        assert!(
            !env.sets.contains_key(dsh::API_KEY_ENV),
            "the daemon's own key wins"
        );
        assert!(env.removes.contains("DSH_EXTRA"));
        assert!(
            !env.sets.contains_key("DSH_EXTRA"),
            "a profile unset beats the capture"
        );
        assert_eq!(
            env.sets
                .get(dsh::TELEMETRY_DISABLED_ENV)
                .map(String::as_str),
            Some("1"),
            "Intent's opt-out beats the profile"
        );
    }

    #[test]
    fn guard_judges_the_launch_dsh_home_and_rechecks_patch_edits() {
        let dir = tempfile::tempdir().unwrap();
        let clean = dir.path().join("clean");
        let uploading = dir.path().join("uploading");
        std::fs::create_dir_all(&clean).unwrap();
        std::fs::create_dir_all(&uploading).unwrap();
        std::fs::write(uploading.join("cordis.patch.yml"), "upload\n").unwrap();
        let good = dir.path().join("good.txt");
        let bad = dir.path().join("bad.txt");
        std::fs::write(&good, DUMP).unwrap();
        std::fs::write(&bad, uploading_dump()).unwrap();
        let script = dir.path().join("dsh");
        write_script(
            &script,
            &format!(
                "[ \"$1\" = --version ] && {{ echo 9.9.8-env.1; exit 0; }}\n\
                 if grep -q upload \"$DSH_HOME/cordis.patch.yml\" 2>/dev/null; then cat '{}'; else cat '{}'; fi\n",
                bad.display(),
                good.display()
            ),
        );
        let profile_for = |home: &Path| {
            intent_acp::ProfileEnv::new(
                "dsh@p",
                "fp",
                vec![("DSH_HOME".to_string(), Some(home.display().to_string()))],
            )
        };

        let clean_status = probe_dsh_cli_at(Some(script.clone()), Some(&profile_for(&clean)));
        assert_eq!(clean_status.guard, Some(Ok(())));
        let uploading_status =
            probe_dsh_cli_at(Some(script.clone()), Some(&profile_for(&uploading)));
        assert!(
            uploading_status.unavailable_reason().is_some(),
            "same binary and version, but the launch DSH_HOME still uploads"
        );

        std::fs::write(clean.join("cordis.patch.yml"), "upload\n").unwrap();
        let edited = probe_dsh_cli_at(Some(script), Some(&profile_for(&clean)));
        assert!(
            edited.unavailable_reason().is_some(),
            "a patch edit after the first probe is checked again"
        );
    }

    #[tokio::test]
    async fn model_probe_never_starts_a_profile_that_still_uploads() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.txt");
        std::fs::write(&bad, uploading_dump()).unwrap();
        let started = dir.path().join("acp-started");
        let script = dir.path().join("dsh");
        write_script(
            &script,
            &format!(
                "case \"$*\" in\n  --version) echo 9.9.7-probe.1 ;;\n  *--dump-config*) cat '{}' ;;\n  *) touch '{}'; exit 2 ;;\nesac\n",
                bad.display(),
                started.display()
            ),
        );
        let fetch = crate::provider_models::fetch_dsh_models_at(Some(script)).await;
        assert!(!started.exists(), "dsh was never started for a session");
        assert!(fetch.models.is_none());
        let warning = fetch.warning.unwrap_or_default();
        assert!(warning.contains("session-log-deepseek"), "{warning}");
    }

    #[test]
    fn api_key_comes_from_the_launch_profile_or_its_dsh_home() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().display().to_string();
        let keyed = intent_acp::ProfileEnv::new(
            "dsh@p",
            "fp",
            vec![
                (dsh::API_KEY_ENV.to_string(), Some("sk-profile".to_string())),
                (dsh::HOME_ENV.to_string(), Some(home.clone())),
            ],
        );
        assert!(api_key_known_in(&launch_env(Some(&keyed))));
        let unset = intent_acp::ProfileEnv::new(
            "dsh@p",
            "fp",
            vec![
                (dsh::API_KEY_ENV.to_string(), None),
                (dsh::HOME_ENV.to_string(), Some(home)),
            ],
        );
        assert!(!api_key_known_in(&launch_env(Some(&unset))));
        std::fs::write(
            dir.path().join(".credentials.yaml"),
            "version: 1\nrefs:\n  DEEPSEEK_API_KEY: sk-saved\n",
        )
        .unwrap();
        assert!(api_key_known_in(&launch_env(Some(&unset))));
    }

    #[test]
    fn missing_binary_is_not_refused() {
        let status = probe_dsh_cli_at(None, None);
        assert_eq!(status.unavailable_reason(), None);
    }
}
