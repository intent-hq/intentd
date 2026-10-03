//! One installed runtime and environment per operation. Never render this state:
//! it contains credentials. Blocking discovery/config reads stay off the executor.

use crate::codex_diagnostics::process::ProbeDependency;

use std::collections::{hash_map::RandomState, BTreeMap};
use std::ffi::OsString;
use std::hash::{BuildHasher, Hash};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use intent_providers::installed_cli::{InstalledCli, InstalledCliIdentity, InstalledCliRuntime};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

#[derive(Clone)]
pub(crate) struct InstalledContext {
    pub runtime: InstalledCliRuntime,
    env: BTreeMap<OsString, OsString>,
    // Only model catalog preparation inspects auth/configuration files. Normal
    // launches must not inherit the catalog's bounded inspection restrictions.
    context_key: Option<u64>,
    names: intent_core::cli_env::CodexEnvNames,
}

fn private_hash(value: &impl Hash) -> u64 {
    static HASHER: OnceLock<RandomState> = OnceLock::new();
    HASHER.get_or_init(RandomState::new).hash_one(value)
}

impl InstalledContext {
    pub fn resolve(cli: InstalledCli) -> Result<Self, String> {
        let runtime = cli.resolve().map_err(|e| e.to_string())?;
        Self::with_runtime(runtime)
    }

    fn with_runtime(runtime: InstalledCliRuntime) -> Result<Self, String> {
        let captured = intent_core::path_utils::login_shell_credential_env();
        let names = intent_core::path_utils::login_shell_codex_env_names();
        let inherited: BTreeMap<_, _> = std::env::vars_os().collect();
        Self::from_inputs(runtime, captured, inherited, names)
    }

    pub(crate) fn from_inputs(
        runtime: InstalledCliRuntime,
        captured: &BTreeMap<String, String>,
        inherited: BTreeMap<OsString, OsString>,
        names: &intent_core::cli_env::CodexEnvNames,
    ) -> Result<Self, String> {
        let unicode = inherited
            .iter()
            .filter_map(|(k, v)| Some((k.to_str()?.to_owned(), v.to_str()?.to_owned())))
            .collect();
        let mut overlay = runtime
            .environment(captured, &unicode, &BTreeMap::new(), names)
            .map_err(|e| e.to_string())?;
        // A captured String must not replace an inherited opaque OS value.
        overlay.retain(|key, _| {
            inherited
                .get(std::ffi::OsStr::new(key))
                .is_none_or(|v| v.to_str().is_some())
        });
        let mut env = inherited;
        env.extend(overlay.into_iter().map(|(k, v)| (k.into(), v.into())));
        Ok(Self {
            runtime,
            env,
            context_key: None,
            names: names.clone(),
        })
    }

    pub async fn discover(cli: InstalledCli) -> Result<Self, String> {
        tokio::task::spawn_blocking(move || Self::resolve(cli))
            .await
            .map_err(|_| "installed CLI discovery failed".to_owned())?
    }

    pub async fn with_catalog_fingerprint(mut self) -> Result<Self, String> {
        tokio::task::spawn_blocking(move || {
            self.context_key = Some(context_key(self.runtime.cli(), &self.env, &self.names)?);
            Ok(self)
        })
        .await
        .map_err(|_| "installed CLI catalog fingerprint task failed".to_owned())?
    }

    pub fn apply_isolated(&self, command: &mut Command) {
        let mut selected = self.clone();
        let names = &self.names;
        selected.env.retain(|k, _| {
            k.to_str().is_some_and(|k| {
                self.runtime.cli().accepts_env(k) || names.contains(k) || k == "SystemRoot"
            })
        });
        selected.apply(command);
    }

    pub fn secret_values(&self) -> Vec<String> {
        let names = &self.names;
        self.env
            .iter()
            .filter_map(|(k, v)| {
                let key = k.to_str()?;
                ((self.runtime.cli().accepts_env(key) || names.contains(key))
                    && !matches!(
                        key,
                        "PATH"
                            | "HOME"
                            | "USERPROFILE"
                            | "CODEX_HOME"
                            | "CLAUDE_CONFIG_DIR"
                            | "CODEX_PATH"
                            | "CODEX_CONFIG"
                            | "CLAUDE_CODE_EXECUTABLE"
                    ))
                .then(|| v.to_string_lossy().into_owned())
            })
            .collect()
    }

    pub fn codex_home(&self) -> Option<PathBuf> {
        nonempty(&self.env, "CODEX_HOME")
            .map(PathBuf::from)
            .or_else(|| self.home().map(|p| p.join(".codex")))
    }

    fn home(&self) -> Option<PathBuf> {
        nonempty(&self.env, "HOME")
            .or_else(|| nonempty(&self.env, "USERPROFILE"))
            .map(PathBuf::from)
    }

    /// Freeze inheritance, retain trusted command overrides (including isolated
    /// homes and PATH), then enforce the selected executable and Intent policy.
    pub fn apply(&self, command: &mut Command) {
        let overrides: Vec<_> = command
            .as_std()
            .get_envs()
            .map(|(k, v)| (k.to_owned(), v.map(std::ffi::OsStr::to_owned)))
            .collect();
        command.env_clear().envs(&self.env);
        for (k, v) in overrides {
            if let Some(v) = v {
                command.env(k, v);
            } else {
                command.env_remove(k);
            }
        }
        let keys: Vec<_> = command
            .as_std()
            .get_envs()
            .map(|(k, _)| k.to_owned())
            .collect();
        for key in keys {
            let name = key.to_string_lossy();
            if name.eq_ignore_ascii_case("CODEX_PATH")
                || name.eq_ignore_ascii_case("CLAUDE_CODE_EXECUTABLE")
                || self.runtime.cli() == InstalledCli::Codex
                    && name.eq_ignore_ascii_case("CODEX_CONFIG")
            {
                command.env_remove(key);
            }
        }
        command.env(self.runtime.cli().path_env(), self.runtime.path());
        if self.runtime.cli() == InstalledCli::Codex {
            command.env(
                "CODEX_CONFIG",
                intent_providers::CODEX_SUBAGENT_POLICY_CONFIG,
            );
        }
    }

    pub fn key(&self, identity: &InstalledCliIdentity) -> Option<String> {
        let context_key = self.context_key?;
        let adapter = match self.runtime.cli() {
            InstalledCli::Codex => intent_providers::codex::ADAPTER_VERSION.trim(),
            InstalledCli::Claude => intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
        };
        Some(format!(
            "installed-v1:{:016x}",
            private_hash(&(adapter, identity, context_key))
        ))
    }

    /// Re-discovery is validation only: it never redirects an in-flight launch.
    pub async fn still_current(
        &self,
        identity: &InstalledCliIdentity,
        command: &Command,
        dependency: ProbeDependency,
    ) -> bool {
        let Ok(current) = Self::discover(self.runtime.cli()).await else {
            return false;
        };
        let Ok(current) = current.with_catalog_fingerprint().await else {
            return false;
        };
        if current.runtime != self.runtime || current.context_key != self.context_key {
            return false;
        }
        matches!(self.observe_with_dependency(command, Some(dependency)).await, Ok((now,_)) if &now==identity)
    }

    pub async fn observe(
        &self,
        launch: &Command,
    ) -> Result<(InstalledCliIdentity, String), String> {
        self.observe_with_dependency(launch, None).await
    }

    pub async fn observe_with_dependency(
        &self,
        launch: &Command,
        dependency: Option<ProbeDependency>,
    ) -> Result<(InstalledCliIdentity, String), String> {
        let mut command = Command::new(self.runtime.path());
        command.arg("--version").env_clear();
        // apply() has frozen all inheritance on the actual adapter command.
        for (k, v) in launch.as_std().get_envs() {
            if let Some(v) = v {
                command.env(k, v);
            }
        }
        if let Some(cwd) = launch.as_std().get_current_dir() {
            command.current_dir(cwd);
        }
        // The task owns the process and its resource leases even if its caller
        // stops awaiting. Only confirmed cleanup releases the actual resources.
        let bytes =
            intent_core::caller::spawn_with_current_caller(version_output(command, dependency))
                .await
                .map_err(|_| "installed CLI version owner failed")??;
        let version = std::str::from_utf8(&bytes)
            .ok()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
            .ok_or("installed CLI returned an invalid version")?
            .to_owned();
        let runtime = self.runtime.clone();
        let observed = version.clone();
        let identity = tokio::task::spawn_blocking(move || runtime.identity(&observed))
            .await
            .map_err(|_| "installed CLI identity task failed")?
            .map_err(|_| "installed CLI changed or disappeared")?;
        Ok((identity, version))
    }
}

fn nonempty<'a>(env: &'a BTreeMap<OsString, OsString>, key: &str) -> Option<&'a OsString> {
    env.get(std::ffi::OsStr::new(key)).filter(|v| !v.is_empty())
}

fn context_key(
    cli: InstalledCli,
    env: &BTreeMap<OsString, OsString>,
    names: &intent_core::cli_env::CodexEnvNames,
) -> Result<u64, String> {
    let home = nonempty(env, "HOME")
        .or_else(|| nonempty(env, "USERPROFILE"))
        .map(PathBuf::from);
    let mut files = Vec::new();
    match cli {
        InstalledCli::Codex => {
            let root = nonempty(env, "CODEX_HOME")
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|p| p.join(".codex")));
            if let Some(root) = root {
                files.extend([root.join("auth.json"), root.join("config.toml")]);
            }
            files.push(PathBuf::from("/etc/codex/config.toml"));
        }
        InstalledCli::Claude => {
            let root = nonempty(env, "CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .or_else(|| home.as_ref().map(|p| p.join(".claude")));
            if let Some(root) = root {
                files.extend([
                    root.join(".credentials.json"),
                    root.join("settings.json"),
                    root.join(".claude.json"),
                ]);
            }
            if let Some(home) = home {
                files.extend([
                    home.join(".claude.json"),
                    home.join(".aws/credentials"),
                    home.join(".aws/config"),
                    home.join(".config/gcloud/application_default_credentials.json"),
                ]);
            }
        }
    }
    for key in [
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AZURE_FEDERATED_TOKEN_FILE",
        "AZURE_CLIENT_CERTIFICATE_PATH",
        "CLAUDE_CODE_CLIENT_CERT",
        "CLAUDE_CODE_CLIENT_KEY",
    ] {
        if cli.accepts_env(key) || names.contains(key) {
            if let Some(path) = nonempty(env, key) {
                files.push(PathBuf::from(path));
            }
        }
    }
    let mut contents = Vec::new();
    for path in files {
        contents.push((path.clone(), bounded_config(&path)?));
    }
    // Process-private hash: cache files never contain reusable auth fingerprints.
    Ok(private_hash(&(env, contents)))
}

fn bounded_config(path: &Path) -> Result<Option<Vec<u8>>, String> {
    use std::io::Read;
    match std::fs::metadata(path) {
        Ok(meta) if !meta.is_file() => {
            return Err("installed CLI authentication/configuration is not a regular file".into())
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot inspect installed CLI authentication/configuration".into()),
    }
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot read installed CLI authentication/configuration".into()),
    };
    let mut data = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut data)
        .map_err(|_| "cannot read installed CLI authentication/configuration")?;
    if data.len() > 1024 * 1024 {
        return Err("installed CLI configuration exceeds the inspection limit".into());
    }
    Ok(Some(data))
}

// macOS has no equivalent private descendant owner in the current platform
// layer. Preserve bounded CLI launch compatibility there; this best-effort
// reaper cannot guarantee cleanup of detached children after their parent exits.
#[cfg(any(target_os = "macos", all(test, unix)))]
struct VersionChild(Option<(tokio::process::Child, u32)>);
#[cfg(any(target_os = "macos", all(test, unix)))]
impl VersionChild {
    async fn reap(&mut self) {
        if let Some((mut child, pid)) = self.0.take() {
            crate::acp_adapter::reap_child(&mut child, pid).await;
        }
    }
}
#[cfg(any(target_os = "macos", all(test, unix)))]
impl Drop for VersionChild {
    fn drop(&mut self) {
        if let Some((mut child, pid)) = self.0.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    crate::acp_adapter::reap_child(&mut child, pid).await;
                });
            }
        }
    }
}

#[cfg(target_os = "macos")]
async fn version_output(
    command: Command,
    dependency: Option<ProbeDependency>,
) -> Result<Vec<u8>, String> {
    let lease = dependency.map(crate::codex_diagnostics::process::ProbeHome::from);
    let result = version_output_uncontained(command).await;
    // The compatible macOS path retains its documented best-effort reaper.
    if let Some(lease) = lease {
        let _ = lease.remove();
    }
    result
}

#[cfg(any(target_os = "macos", all(test, unix)))]
async fn version_output_uncontained(mut command: Command) -> Result<Vec<u8>, String> {
    use std::process::Stdio;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let child = command
        .spawn()
        .map_err(|_| "installed CLI version check could not start")?;
    let pid = child
        .id()
        .ok_or("installed CLI version check has no process ID")?;
    let mut guard = VersionChild(Some((child, pid)));
    let child = &mut guard.0.as_mut().expect("live child").0;
    let stdout = child
        .stdout
        .take()
        .ok_or("installed CLI version check has no stdout")?;
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        stdout
            .take(4097)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| "installed CLI version read failed")?;
        if bytes.len() > 4096 {
            return Err("installed CLI version output exceeds the limit");
        }
        let status = child
            .wait()
            .await
            .map_err(|_| "installed CLI version check failed")?;
        if !status.success() {
            return Err("installed CLI version check exited unsuccessfully");
        }
        Ok(bytes)
    })
    .await
    .unwrap_or(Err("installed CLI version check timed out"))
    .map_err(str::to_owned);
    guard.reap().await;
    result
}

// Reuse the private Linux subreaper / Windows Job owner. Unlike a snapshot of
// the version leader, it retains descendants after successful exit and sets no
// daemon-wide process flags. The command keeps its exact launch env and cwd.
#[cfg(any(target_os = "linux", windows))]
async fn version_output(
    command: Command,
    dependency: Option<ProbeDependency>,
) -> Result<Vec<u8>, String> {
    use crate::codex_diagnostics::process::ProbeProcess;
    let directory = tokio::task::spawn_blocking(tempfile::tempdir)
        .await
        .map_err(|_| "installed CLI version owner failed")?
        .map_err(|_| "installed CLI version owner failed")?;
    let mut guard = ProbeProcess::spawn_with_dependency(command, directory, dependency)
        .await
        .map_err(|_| "installed CLI version check could not start")?;
    drop(guard.stdin.take());
    let stdout = guard
        .stdout
        .take()
        .ok_or("installed CLI version check has no stdout")?;
    let mut stderr = guard
        .stderr
        .take()
        .ok_or("installed CLI version check has no stderr")?;
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let read = async {
            let mut bytes = Vec::new();
            stdout
                .take(4097)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| "installed CLI version read failed")?;
            if bytes.len() > 4096 {
                return Err("installed CLI version output exceeds the limit");
            }
            Ok(bytes)
        };
        let discard = async {
            tokio::io::copy(&mut stderr, &mut tokio::io::sink())
                .await
                .map_err(|_| "installed CLI version read failed")
        };
        let (bytes, _) = tokio::try_join!(read, discard)?;
        if !guard
            .wait()
            .await
            .map_err(|_| "installed CLI version check failed")?
            .success()
        {
            return Err("installed CLI version check exited unsuccessfully");
        }
        Ok(bytes)
    })
    .await
    .unwrap_or(Err("installed CLI version check timed out"));
    guard
        .cleanup()
        .await
        .map_err(|_| "installed CLI version cleanup failed")?;
    result.map_err(str::to_owned)
}

#[cfg(all(test, unix))]
mod tests;

#[cfg(all(test, unix))]
pub(crate) use tests::context_in as test_context_in;

#[cfg(all(test, target_os = "linux"))]
pub(crate) use tests::{cancel_version_owner, owner_failure_context};
