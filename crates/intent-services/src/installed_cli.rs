//! One installed runtime and environment per operation. Never render this state:
//! it contains credentials. Blocking discovery/config reads stay off the executor.

use crate::codex_diagnostics::process::ProbeDependency;

use std::collections::{hash_map::RandomState, BTreeMap};
use std::ffi::OsString;
use std::hash::{BuildHasher, Hash};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use intent_acp::ProfileEnv;
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
    /// The selected named profile, already applied to `env`. Kept so catalog
    /// keys, redaction and re-discovery use the same profile.
    profile: Option<Arc<ProfileEnv>>,
    /// The confirmed-too-old host CLI this context replaced with Intent's
    /// bundled copy (`runtime`). Re-discovery checks the host again.
    host: Option<InstalledCliRuntime>,
}

fn private_hash(value: &impl Hash) -> u64 {
    static HASHER: OnceLock<RandomState> = OnceLock::new();
    HASHER.get_or_init(RandomState::new).hash_one(value)
}

/// An installed-CLI discovery failure that remembers whether the CLI itself
/// is missing, so the model catalog can label it `warningKind:
/// "cli-missing"` (PROTOCOL §5.30) without matching the message text.
#[derive(Debug, Clone)]
pub struct DiscoveryError {
    /// The human-readable reason, unchanged from the untyped path.
    pub message: String,
    /// `true` when no canonical executable was found on the host.
    pub cli_missing: bool,
}

impl DiscoveryError {
    fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cli_missing: false,
        }
    }
}

impl From<DiscoveryError> for String {
    fn from(err: DiscoveryError) -> Self {
        err.message
    }
}

impl InstalledContext {
    pub fn resolve(cli: InstalledCli) -> Result<Self, String> {
        Self::classify(cli.resolve()).map_err(String::from)
    }

    /// Build a context from a CLI lookup, keeping a missing CLI distinct
    /// from every later failure.
    pub(crate) fn classify(
        found: Result<InstalledCliRuntime, intent_providers::installed_cli::MissingInstalledCli>,
    ) -> Result<Self, DiscoveryError> {
        let runtime = found.map_err(|missing| DiscoveryError {
            message: missing.to_string(),
            cli_missing: true,
        })?;
        Self::with_runtime(runtime).map_err(DiscoveryError::other)
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
            profile: None,
            host: None,
        })
    }

    /// Resolve a named profile's overlay into this context. It replaces
    /// inherited and captured values and removes unset ones, so the account
    /// home, source-home isolation and catalog fingerprint all see the
    /// profile. The selected executable and enforced Codex policy are
    /// re-asserted afterwards: a profile can never override them. `None`
    /// keeps the Default configuration. Call before
    /// [`Self::with_catalog_fingerprint`].
    #[must_use]
    pub fn with_profile(mut self, profile: Option<Arc<ProfileEnv>>) -> Self {
        let Some(profile) = profile else {
            return self;
        };
        profile.apply_to_map(&mut self.env);
        let cli = self.runtime.cli();
        // A profile that selects its own Codex home also selects the custom
        // credential names that home's config declares, so the isolated model
        // probe, fingerprint and redaction use the same names as runs. Bounded
        // read; callers run this off the executor. An unreadable config keeps
        // the existing names, as the login-shell capture does.
        if cli == InstalledCli::Codex && profile.controls("CODEX_HOME") {
            if let Some(home) = self.codex_home() {
                if let Ok(names) = intent_core::cli_env::CodexEnvNames::from_home(&home) {
                    self.names.extend_from(&names);
                } else {
                    tracing::warn!(
                        "could not read Codex config credential references for a provider profile home; using the default names"
                    );
                }
            }
        }
        self.env.retain(|key, _| {
            let key = key.to_string_lossy();
            !(key.eq_ignore_ascii_case("CODEX_PATH")
                || key.eq_ignore_ascii_case("CLAUDE_CODE_EXECUTABLE")
                || cli == InstalledCli::Codex && key.eq_ignore_ascii_case("CODEX_CONFIG"))
        });
        self.env.insert(
            cli.path_env().into(),
            self.runtime.path().as_os_str().to_owned(),
        );
        if cli == InstalledCli::Codex {
            self.env.insert(
                "CODEX_CONFIG".into(),
                intent_providers::CODEX_SUBAGENT_POLICY_CONFIG.into(),
            );
        }
        self.context_key = None;
        self.profile = Some(profile);
        self
    }

    /// The applied profile, if any.
    pub fn profile(&self) -> Option<&Arc<ProfileEnv>> {
        self.profile.as_ref()
    }

    pub async fn discover(cli: InstalledCli) -> Result<Self, String> {
        Self::discover_for(cli, None).await
    }

    /// [`Self::discover`] with a named profile's overlay resolved before any
    /// account lookup.
    pub async fn discover_for(
        cli: InstalledCli,
        profile: Option<Arc<ProfileEnv>>,
    ) -> Result<Self, String> {
        Self::discover_classified(cli, profile)
            .await
            .map_err(String::from)
    }

    /// [`Self::discover_for`] keeping whether the CLI itself is missing.
    pub(crate) async fn discover_classified(
        cli: InstalledCli,
        profile: Option<Arc<ProfileEnv>>,
    ) -> Result<Self, DiscoveryError> {
        tokio::task::spawn_blocking(move || {
            Self::classify(cli.resolve()).map(|c| c.with_profile(profile))
        })
        .await
        .map_err(|_| DiscoveryError::other("installed CLI discovery failed"))?
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
                self.runtime.cli().accepts_env(k)
                    || names.contains(k)
                    || k == "SystemRoot"
                    || self.profile.as_ref().is_some_and(|p| p.controls(k))
            })
        });
        selected.apply(command);
    }

    pub fn secret_values(&self) -> Vec<String> {
        let names = &self.names;
        let mut values = self
            .profile
            .as_ref()
            .map(|p| p.secret_values())
            .unwrap_or_default();
        values.extend(self.env.iter().filter_map(|(k, v)| {
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
        }));
        values
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
        self.apply_selection(command);
    }

    /// Enforce only the selected executable and Intent's Codex policy on an
    /// already prepared (`apply`d or `apply_isolated`) command, leaving the
    /// rest of its environment untouched.
    pub fn apply_selection(&self, command: &mut Command) {
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

    fn provider_id(&self) -> &'static str {
        self.runtime.cli().provider_id()
    }

    pub fn validate_launch_version(&self, version: &str) -> Result<(), String> {
        intent_providers::adapter_cli::validate_launch_version(self.provider_id(), version)
    }

    /// The host CLI this context replaced with Intent's bundled copy, if any.
    pub fn replaced_host(&self) -> Option<&InstalledCliRuntime> {
        self.host.as_ref()
    }

    /// The same environment and profile with `runtime` as the launched CLI.
    /// The original runtime is remembered as the replaced host CLI.
    async fn with_bundled(&self, runtime: InstalledCliRuntime) -> Result<Self, String> {
        let mut next = self.clone();
        next.host = Some(self.host.clone().unwrap_or_else(|| self.runtime.clone()));
        next.env.insert(
            runtime.cli().path_env().into(),
            runtime.path().as_os_str().to_owned(),
        );
        next.runtime = runtime;
        if next.context_key.is_some() {
            return next.with_catalog_fingerprint().await;
        }
        Ok(next)
    }

    /// Observe the selected CLI with the prepared `launch` command and enforce
    /// the launch minimum. A host CLI confirmed below it is replaced by
    /// Intent's bundled copy (installed on first use): `self` switches to it,
    /// and it is observed and validated instead. Unknown versions keep the
    /// host CLI. Returns the identity and version of what will be launched.
    /// Callers must then call [`Self::apply_selection`] on `launch` so the
    /// adapter receives the selected executable.
    ///
    /// The bundled copy lives under the launch environment's home, so a
    /// profile or fixture home selects its own managed directory.
    pub async fn select_launch(
        &mut self,
        launch: &Command,
        dependency: Option<ProbeDependency>,
    ) -> Result<(InstalledCliIdentity, String), LaunchSelectionError> {
        use LaunchSelectionError::{BelowMinimum, BundledUnavailable, Unverified};
        let observed = self
            .observe_with_dependency(launch, dependency.clone())
            .await
            .map_err(Unverified)?;
        if self.host.is_some() {
            self.validate_launch_version(&observed.1)
                .map_err(BelowMinimum)?;
            return Ok(observed);
        }
        let provider = self.provider_id();
        let Some(bundled) = intent_providers::adapter_cli::bundled_fallback(provider, &observed.1)
        else {
            self.validate_launch_version(&observed.1)
                .map_err(BelowMinimum)?;
            return Ok(observed);
        };
        let reason = self
            .validate_launch_version(&observed.1)
            .err()
            .unwrap_or_default();
        let cli = self.runtime.cli();
        let root = self.home().as_deref().and_then(crate::bundled_cli::root_in);
        let installed = match &root {
            Some(root) => crate::bundled_cli::ensure(root, cli, &bundled).await,
            None => Err("the launch environment has no absolute home directory".to_owned()),
        };
        let runtime = match installed {
            Ok(runtime) => runtime,
            // Claude had no launch minimum before the bundled fallback; when
            // it cannot be installed (offline, no npm) keep launching the
            // installed Claude rather than blocking every session.
            Err(error) if cli == InstalledCli::Claude => {
                tracing::warn!(
                    installed = %observed.1,
                    bundled = %bundled.spec(),
                    %error,
                    "could not install Intent's bundled Claude Code CLI; launching the installed CLI"
                );
                return Ok(observed);
            }
            Err(error) => {
                return Err(BundledUnavailable(format!(
                    "{reason} Intent could not install its bundled {} instead: {error}",
                    bundled.spec()
                )))
            }
        };
        warn_bundled_once(cli, &observed.1, &bundled);
        *self = self.with_bundled(runtime).await.map_err(Unverified)?;
        // Probe with the launch's environment and cwd, selecting the bundled
        // executable exactly as the launch will.
        let mut probe = Command::new(self.runtime.path());
        for (key, value) in launch.as_std().get_envs() {
            match value {
                Some(value) => probe.env(key, value),
                None => probe.env_remove(key),
            };
        }
        if let Some(cwd) = launch.as_std().get_current_dir() {
            probe.current_dir(cwd);
        }
        self.apply_selection(&mut probe);
        let observed = self
            .observe_with_dependency(&probe, dependency)
            .await
            .map_err(Unverified)?;
        self.validate_launch_version(&observed.1)
            .map_err(BelowMinimum)?;
        Ok(observed)
    }

    pub fn key(&self, identity: &InstalledCliIdentity) -> Option<String> {
        let context_key = self.context_key?;
        let adapter = match self.runtime.cli() {
            InstalledCli::Codex => intent_providers::CODEX_ACP_NPX_PACKAGE,
            InstalledCli::Claude => intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
        };
        let profile = self.profile.as_ref().map(|p| p.cache_key());
        Some(format!(
            "installed-v1:{:016x}",
            private_hash(&(adapter, identity, context_key, profile))
        ))
    }

    /// Re-discovery is validation only: it never redirects an in-flight launch.
    pub async fn still_current(
        &self,
        identity: &InstalledCliIdentity,
        command: &Command,
        dependency: ProbeDependency,
    ) -> bool {
        let Ok(current) = Self::discover_for(self.runtime.cli(), self.profile.clone()).await else {
            return false;
        };
        let Ok(mut current) = current.with_catalog_fingerprint().await else {
            return false;
        };
        if let Some(host) = &self.host {
            // The bundled copy stays current only while the same host CLI is
            // still confirmed too old; upgrading it switches back.
            if current.runtime != *host {
                return false;
            }
            let Ok((_, version)) = current
                .observe_with_dependency(command, Some(dependency.clone()))
                .await
            else {
                return false;
            };
            if intent_providers::adapter_cli::bundled_fallback(self.provider_id(), &version)
                .is_none()
            {
                return false;
            }
            let Ok(bundled) = current.with_bundled(self.runtime.clone()).await else {
                return false;
            };
            current = bundled;
        }
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

/// Why a launch could not select a runnable CLI.
#[derive(Debug)]
pub(crate) enum LaunchSelectionError {
    /// The CLI that would be launched is confirmed below the minimum.
    BelowMinimum(String),
    /// The installed CLI is too old and Intent's bundled copy could not be
    /// installed; the message names both.
    BundledUnavailable(String),
    /// A version or identity probe failed.
    Unverified(String),
}

impl std::fmt::Display for LaunchSelectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Self::BelowMinimum(reason)
        | Self::BundledUnavailable(reason)
        | Self::Unverified(reason)) = self;
        f.write_str(reason)
    }
}

impl From<LaunchSelectionError> for String {
    fn from(error: LaunchSelectionError) -> Self {
        error.to_string()
    }
}

fn warn_bundled_once(
    cli: InstalledCli,
    version: &str,
    bundled: &intent_providers::adapter_cli::BundledCli,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static CODEX: AtomicBool = AtomicBool::new(false);
    static CLAUDE: AtomicBool = AtomicBool::new(false);
    let warned = match cli {
        InstalledCli::Codex => &CODEX,
        InstalledCli::Claude => &CLAUDE,
    };
    if !warned.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            cli = cli.command(),
            installed = version,
            bundled = %bundled.spec(),
            "installed CLI is below the adapter minimum; launching Intent's bundled CLI until it is upgraded"
        );
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
    let mut claude_state = Vec::new();
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
                files.extend([root.join(".credentials.json"), root.join("settings.json")]);
                claude_state.push(root.join(".claude.json"));
            }
            if let Some(home) = home {
                claude_state.push(home.join(".claude.json"));
                files.extend([
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
    for path in claude_state {
        let data = bounded_config(&path)?.map(|bytes| claude_state_projection(&bytes));
        contents.push((path, data));
    }
    for path in files {
        let data = bounded_config(&path)?;
        contents.push((path, data));
    }
    // Process-private hash: cache files never contain reusable auth fingerprints.
    Ok(private_hash(&(env, contents)))
}

/// Top-level `.claude.json` keys that select the account or API key. The file
/// is also Claude Code's state store, which it rewrites with cache timestamps
/// and counters during ordinary runs, including model discovery itself.
const CLAUDE_STATE_IDENTITY_KEYS: [&str; 3] =
    ["oauthAccount", "primaryApiKey", "customApiKeyResponses"];

/// Unparseable state keeps its raw bytes, so any change still invalidates.
fn claude_state_projection(bytes: &[u8]) -> Vec<u8> {
    let Ok(serde_json::Value::Object(state)) = serde_json::from_slice(bytes) else {
        return bytes.to_owned();
    };
    let mut projection = serde_json::Map::new();
    for key in CLAUDE_STATE_IDENTITY_KEYS {
        let Some(value) = state.get(key) else {
            continue;
        };
        let mut value = value.clone();
        if let serde_json::Value::Object(account) = &mut value {
            account.retain(|k, _| !k.ends_with("FetchedAt"));
        }
        projection.insert(key.to_owned(), canonical_json(value));
    }
    let mut out = b"claude-state-v1:".to_vec();
    out.extend(serde_json::to_vec(&projection).unwrap_or_default());
    out
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, canonical_json(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonical_json).collect())
        }
        other => other,
    }
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
