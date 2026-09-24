//! Bounded, local Codex diagnostics. Selection comes from production discovery
//! and ACP spawn policy; no npm process or model catalog is invoked here.
//!
//! `CodexLaunch::discover` snapshots the selected launch. `inspect_local` is
//! suitable for ordinary doctor output. An opt-in caller that establishes the
//! actual npm entrypoint can pass it to `inspect_materialized` (never a guessed
//! cache hit). Only `CodexRuntimeReport` is intended for rendering/serialization.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_acp::spawn::{build_command, SpawnOptions};
use intent_core::settings_file::SettingsFile;
use intent_providers::config::CODEX_ACP_NPX_PACKAGE;
use intent_providers::discover::{resolve_fallback_launch, ProviderBinarySource, ProviderLaunch};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

pub(crate) mod process;

const LOCAL_TIMEOUT: Duration = Duration::from_secs(3);
const OUTPUT_LIMIT: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LaunchSource {
    SettingsOverride,
    LocalDiscovery,
    ManagedNpm,
    Unresolved,
}

/// Fixed, safe reasons: subprocess messages and account/auth data never enter
/// a report, even on a nonzero exit or malformed output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UnknownReason {
    ManagedPackageNotInspected,
    AdapterNotFound,
    NodeNotFound,
    OpaqueAdapter,
    OpaqueRuntime,
    PackageMismatch,
    PackageUnreadable,
    RuntimeNotFound,
    RelativeRuntimeOverride,
    SpawnFailed,
    TimedOut,
    OutputLimit,
    UnsuccessfulExit,
    InvalidVersion,
    InspectionFailed,
    CleanupFailed,
}

impl UnknownReason {
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::ManagedPackageNotInspected => {
                "selected npm package has not been inspected; no package was installed"
            }
            Self::AdapterNotFound => "adapter and npm fallback are unavailable",
            Self::NodeNotFound => "Node is unavailable for local package inspection",
            Self::OpaqueAdapter => {
                "wrapper or native adapter cannot be inspected without executing unverified code"
            }
            Self::OpaqueRuntime => "runtime entrypoint cannot be verified for offline inspection",
            Self::PackageMismatch => "inspected package does not match the selected npm pin",
            Self::PackageUnreadable => "selected adapter package could not be inspected",
            Self::RuntimeNotFound => "selected adapter's runtime could not be resolved",
            Self::RelativeRuntimeOverride => {
                "relative runtime override depends on the agent working directory"
            }
            Self::SpawnFailed => "diagnostic process could not start safely",
            Self::TimedOut => "local check exceeded its deadline",
            Self::OutputLimit => "local check exceeded its output limit",
            Self::UnsuccessfulExit => "local version process exited unsuccessfully",
            Self::InvalidVersion => "local output was not a recognized version",
            Self::InspectionFailed => "local package inspection failed",
            Self::CleanupFailed => {
                "probe cleanup could not be confirmed; temporary configuration was retained"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "camelCase")]
pub enum VersionMeasurement {
    Measured(String),
    Unknown(UnknownReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeSource {
    AdapterDependency,
    EnvironmentOverride,
    Unknown,
}

/// All free text is sanitized before entering this rendering boundary.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeReport {
    pub launch_source: LaunchSource,
    pub launch_program: String,
    /// Configuration, never a measured version (also shown for local launches).
    pub configured_package: &'static str,
    /// Whether production removes `CODEX_PATH` and `CODEX_CONFIG` for this launch.
    pub removes_codex_overrides: bool,
    pub adapter_path: Option<String>,
    pub adapter_version: VersionMeasurement,
    pub runtime_source: RuntimeSource,
    pub runtime_path: Option<String>,
    pub runtime_version: VersionMeasurement,
}

/// Executable identity for the later raw catalog probe. This is launch data,
/// not rendering data. A dependency uses Node + its exact package entrypoint;
/// it never uses an unrelated `codex` discovered on PATH.
pub struct DiagnosticExecutable {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

impl DiagnosticExecutable {
    #[must_use]
    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        command
    }
}

pub struct CodexInspection {
    pub report: CodexRuntimeReport,
    /// Present only when inspection establishes the selected runtime's identity.
    /// Its version may still fail to measure; callers must inspect both states.
    pub runtime: Option<DiagnosticExecutable>,
}

/// Raw launch and effective environment, deliberately not `Debug`/`Serialize`.
/// Keep this object for both ACP and raw catalog probes so they share selection.
pub struct CodexLaunch {
    selection: ProviderLaunch,
    path: OsString,
    codex_path: Option<OsString>,
}

impl CodexLaunch {
    /// Uses the same settings, discovery, fallback, and environment policy as
    /// production. Discovery may do the existing bounded login-shell capture;
    /// async callers should perform this synchronous step off their executor.
    #[must_use]
    pub fn discover(settings: &SettingsFile) -> Self {
        let provider = intent_providers::provider_config("codex");
        let selection = resolve_fallback_launch(
            provider,
            settings.providers.paths.get("codex").map(String::as_str),
        );
        let command = build_command(&spawn_options(&selection));
        Self {
            selection,
            path: effective_env(&command, "PATH").unwrap_or_default(),
            codex_path: effective_env(&command, "CODEX_PATH"),
        }
    }

    #[must_use]
    pub fn selection(&self) -> &ProviderLaunch {
        &self.selection
    }

    /// The production launch's enriched PATH, also used for local inspection.
    /// Catalog callers can apply this after isolating their probe environment.
    #[must_use]
    pub fn effective_path(&self) -> &OsStr {
        &self.path
    }

    /// Production launch options, including the managed env policy when passed
    /// to `intent_acp::spawn::build_command`. Catalog callers must additionally
    /// isolate config/cwd, bound IO/lifetime, and never send a prompt.
    #[must_use]
    pub fn spawn_options(&self) -> SpawnOptions<'_> {
        spawn_options(&self.selection)
    }

    /// Default local reporting. Never launches npx, scans npm caches, installs
    /// packages, or asks a provider for models/account/auth information.
    pub async fn inspect_local(&self) -> CodexInspection {
        match &self.selection {
            ProviderLaunch::Local(binary) => self.inspect(&binary.path, None).await,
            ProviderLaunch::Managed { .. } => {
                self.unknown(UnknownReason::ManagedPackageNotInspected)
            }
            ProviderLaunch::Bare { .. } => self.unknown(UnknownReason::AdapterNotFound),
        }
    }

    /// Inspect an entrypoint established by the opt-in managed-package launch.
    /// The caller must obtain it from that launch, not a PATH or cache guess.
    /// The inspector also verifies the package name, bin identity, and exact pin.
    /// Local selections ignore this argument and inspect their selected binary.
    pub async fn inspect_materialized(&self, adapter: &Path) -> CodexInspection {
        if let ProviderLaunch::Managed { package, .. } = &self.selection {
            let version = package.rsplit_once('@').map(|(_, version)| version);
            self.inspect(adapter, version).await
        } else {
            self.inspect_local().await
        }
    }

    fn unknown(&self, reason: UnknownReason) -> CodexInspection {
        let (launch_source, program) = match &self.selection {
            ProviderLaunch::Local(binary) => (
                match binary.source {
                    ProviderBinarySource::SettingsOverride => LaunchSource::SettingsOverride,
                    ProviderBinarySource::LocalDiscovery => LaunchSource::LocalDiscovery,
                },
                binary.path.as_os_str(),
            ),
            ProviderLaunch::Managed { npx, .. } => (LaunchSource::ManagedNpm, npx.as_os_str()),
            ProviderLaunch::Bare { command } => (LaunchSource::Unresolved, OsStr::new(command)),
        };
        CodexInspection {
            report: CodexRuntimeReport {
                launch_source,
                launch_program: safe_text(&program.to_string_lossy()),
                configured_package: CODEX_ACP_NPX_PACKAGE,
                removes_codex_overrides: !intent_acp::spawn::codex_managed_env_removals(
                    "codex",
                    self.spawn_options().via_npx(),
                )
                .is_empty(),
                adapter_path: None,
                adapter_version: VersionMeasurement::Unknown(reason),
                runtime_source: RuntimeSource::Unknown,
                runtime_path: None,
                runtime_version: VersionMeasurement::Unknown(reason),
            },
            runtime: None,
        }
    }

    async fn inspect(&self, adapter: &Path, pin: Option<&str>) -> CodexInspection {
        let mut result = self.unknown(UnknownReason::OpaqueAdapter);
        result.report.adapter_path = Some(safe_text(&adapter.to_string_lossy()));
        // Even --version on an opaque wrapper can install packages. Inspect
        // package/bin identity without importing it before executing anything.
        let Some(node) = executable_on_path("node", &self.path) else {
            result.report.runtime_version =
                VersionMeasurement::Unknown(UnknownReason::NodeNotFound);
            result.report.adapter_version = result.report.runtime_version.clone();
            return result;
        };
        let runtime_override = self.runtime_override_path();
        let mut command = Command::new(&node);
        command
            .args(["-e", include_str!("inspect.cjs")])
            .arg(adapter)
            .arg(pin.unwrap_or(""))
            .arg(
                runtime_override
                    .as_ref()
                    .ok()
                    .and_then(Option::as_deref)
                    .unwrap_or_else(|| Path::new("")),
            );
        let package = match local_output(command, &self.path, LOCAL_TIMEOUT)
            .await
            .and_then(|bytes| {
                serde_json::from_slice::<PackageInspection>(&bytes)
                    .map_err(|_| UnknownReason::InspectionFailed)
            }) {
            Ok(package) => package,
            Err(reason) => {
                result.report.runtime_version = VersionMeasurement::Unknown(reason);
                result.report.adapter_version = VersionMeasurement::Unknown(reason);
                return result;
            }
        };
        if let Some(reason) = package.reason {
            let reason = match reason.as_str() {
                "opaque" => UnknownReason::OpaqueAdapter,
                "mismatch" => UnknownReason::PackageMismatch,
                _ => UnknownReason::PackageUnreadable,
            };
            result.report.runtime_version = VersionMeasurement::Unknown(reason);
            result.report.adapter_version = VersionMeasurement::Unknown(reason);
            return result;
        }
        let Some(adapter) = package.adapter else {
            return result;
        };
        let mut command = Command::new(&node);
        command.arg(&adapter);
        result.report.adapter_version = self.version(command, VersionKind::Adapter).await;
        let runtime_override = match runtime_override {
            Ok(path) => path,
            Err(reason) => {
                result.report.runtime_version = VersionMeasurement::Unknown(reason);
                return result;
            }
        };
        let Some(path) = package.runtime else {
            let reason = if package.runtime_reason.as_deref() == Some("opaque") {
                UnknownReason::OpaqueRuntime
            } else {
                UnknownReason::RuntimeNotFound
            };
            result.report.runtime_version = VersionMeasurement::Unknown(reason);
            return result;
        };
        result.report.runtime_source = if runtime_override.is_some() {
            RuntimeSource::EnvironmentOverride
        } else {
            RuntimeSource::AdapterDependency
        };
        result.report.runtime_path = Some(safe_text(&path.to_string_lossy()));
        let runtime = DiagnosticExecutable {
            program: node,
            args: vec![path.into_os_string()],
        };
        result.report.runtime_version = self.version(runtime.command(), VersionKind::Runtime).await;
        result.runtime = Some(runtime);
        result
    }

    fn runtime_override_path(&self) -> Result<Option<PathBuf>, UnknownReason> {
        if let Some(value) = self.codex_path.as_ref().filter(|value| !value.is_empty()) {
            let configured = PathBuf::from(value);
            let program = if configured.is_absolute() {
                configured
            } else if configured.components().count() == 1 {
                executable_on_path(value, &self.path).ok_or(UnknownReason::RuntimeNotFound)?
            } else {
                return Err(UnknownReason::RelativeRuntimeOverride);
            };
            if !intent_core::path_utils::is_executable_file(&program) {
                return Err(UnknownReason::RuntimeNotFound);
            }
            return Ok(Some(program));
        }
        Ok(None)
    }

    async fn version(&self, mut command: Command, kind: VersionKind) -> VersionMeasurement {
        command.arg("--version");
        match local_output(command, &self.path, LOCAL_TIMEOUT)
            .await
            .and_then(|bytes| parse_version(&bytes, kind).ok_or(UnknownReason::InvalidVersion))
        {
            Ok(version) => VersionMeasurement::Measured(version),
            Err(reason) => VersionMeasurement::Unknown(reason),
        }
    }
}

fn spawn_options(selection: &ProviderLaunch) -> SpawnOptions<'_> {
    let mut options = SpawnOptions::new(intent_providers::provider_config("codex"));
    match selection {
        ProviderLaunch::Local(binary) => options.provider_binary = Some(&binary.path),
        ProviderLaunch::Managed { npx, package } => {
            options.npx_fallback_binary = Some(npx);
            options.npx_fallback_package = Some(package);
        }
        ProviderLaunch::Bare { .. } => {}
    }
    options
}

fn effective_env(command: &Command, key: &str) -> Option<OsString> {
    command
        .as_std()
        .get_envs()
        .find(|(name, _)| *name == key)
        .map_or_else(
            || std::env::var_os(key),
            |(_, value)| value.map(OsStr::to_os_string),
        )
}

fn executable_on_path(name: impl AsRef<OsStr>, path: &OsStr) -> Option<PathBuf> {
    std::env::split_paths(path)
        .filter(|dir| dir.is_absolute())
        .find_map(|dir| {
            let file = dir.join(name.as_ref());
            #[cfg(windows)]
            let file = if file.extension().is_none() {
                file.with_extension("exe")
            } else {
                file
            };
            intent_core::path_utils::is_executable_file(&file).then_some(file)
        })
}

#[derive(serde::Deserialize)]
struct PackageInspection {
    reason: Option<String>,
    adapter: Option<PathBuf>,
    runtime: Option<PathBuf>,
    runtime_reason: Option<String>,
}

/// Strip control/bidi formatting and cap path text. Version and reason text
/// use separate strict allowlists; no raw child output is ever rendered.
fn safe_text(value: &str) -> String {
    value
        .chars()
        .take(512)
        .map(|c| {
            if c.is_control() || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
                '?'
            } else {
                c
            }
        })
        .collect()
}

#[derive(Clone, Copy)]
enum VersionKind {
    Adapter,
    Runtime,
}

fn parse_version(bytes: &[u8], kind: VersionKind) -> Option<String> {
    let output = std::str::from_utf8(bytes).ok()?.trim();
    let prefixes: &[&str] = match kind {
        VersionKind::Adapter => &["@agentclientprotocol/codex-acp ", "codex-acp "],
        VersionKind::Runtime => &["codex-cli ", "codex "],
    };
    let version = prefixes
        .iter()
        .find_map(|prefix| output.strip_prefix(prefix))
        .unwrap_or(output);
    let version = version.strip_prefix('v').unwrap_or(version);
    // Accept only numeric release versions and numeric build/prerelease
    // segments (e.g. 0.148.0-alpha.7). Never echo arbitrary suffixes.
    let (release, suffix) = version
        .split_once('-')
        .map_or((version, None), |(r, s)| (r, Some(s)));
    if version.len() > 64
        || release.split('.').count() != 3
        || !release
            .split('.')
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    if let Some(suffix) = suffix {
        let (channel, number) = suffix.split_once('.')?;
        if !["alpha", "beta", "rc"].contains(&channel)
            || number.is_empty()
            || !number.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
    }
    Some(version.to_string())
}

/// A local check with no credentials, user config, or project cwd. The deadline
/// covers both exit and bounded stdout; stderr is discarded rather than logged.
async fn local_output(
    mut command: Command,
    path: &OsStr,
    timeout: Duration,
) -> Result<Vec<u8>, UnknownReason> {
    let directory = tempfile::Builder::new()
        .prefix("intentd-codex-diagnostic-")
        .tempdir()
        .map_err(|_| UnknownReason::InspectionFailed)?;
    command
        .env_clear()
        .env("PATH", path)
        .env("HOME", directory.path())
        .env("USERPROFILE", directory.path())
        .env("CODEX_HOME", directory.path())
        .env("XDG_CONFIG_HOME", directory.path())
        .env("NODE_DISABLE_COMPILE_CACHE", "1")
        .current_dir(directory.path());
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", root);
    }
    let mut guard = process::ProbeProcess::spawn(command, directory).await?;
    drop(guard.stdin.take());
    let stdout = guard.stdout.take().ok_or(UnknownReason::SpawnFailed)?;
    let mut stderr = guard.stderr.take().ok_or(UnknownReason::SpawnFailed)?;
    let result = tokio::time::timeout(timeout, async {
        let read_stdout = async {
            let mut bytes = Vec::new();
            stdout
                .take((OUTPUT_LIMIT + 1) as u64)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| UnknownReason::InspectionFailed)?;
            if bytes.len() > OUTPUT_LIMIT {
                return Err(UnknownReason::OutputLimit);
            }
            Ok(bytes)
        };
        let discard_stderr = async {
            tokio::io::copy(&mut stderr, &mut tokio::io::sink())
                .await
                .map_err(|_| UnknownReason::InspectionFailed)
        };
        let (bytes, _) = tokio::try_join!(read_stdout, discard_stderr)?;
        if !guard.wait().await?.success() {
            return Err(UnknownReason::UnsuccessfulExit);
        }
        Ok(bytes)
    })
    .await
    .unwrap_or(Err(UnknownReason::TimedOut));
    guard.cleanup().await?;
    result
}

#[cfg(test)]
mod tests;
