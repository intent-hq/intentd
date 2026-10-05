//! Production acquisition for the bounded custom-endpoint Claude native path.
//! No account-status inference: Claude 2.1.280 positively excludes remote
//! organization settings for this route before considering login state.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use intent_acp::NormalizedMcpServers;
use intent_providers::installed_cli::{InstalledCli, InstalledCliIdentity};
use serde_json::{json, Value};

use super::{
    auth, policy, staged, AuthModelContext, ConfigurationIdentity, ManagedProviderProfile,
    ProfileDirectory, ProfileError, ProfilePurpose, ProfileResult, RuntimeIdentity,
};
use crate::installed_cli::InstalledContext;
use staged::DeferredReason;

mod local_sources;

pub enum NativeAcquisition {
    Ready(Box<AcquiredNativeProfile>),
    Deferred(DeferredReason),
}

/// Private provenance and source snapshot. Cannot be constructed by callers or
/// from an authentication-status boolean. May contain credentials; never render.
pub struct AcquiredNativeProfile {
    context: InstalledContext,
    runtime_identity: InstalledCliIdentity,
    workspace: PathBuf,
    etc_root: PathBuf,
    environment: BTreeMap<String, String>,
    auth_model: AuthModelContext,
    sources: ConfigurationIdentity,
}

/// Intent-owned inputs, distinct from acquired native auth/policy. The model is
/// an explicit user/service choice, never inferred from an unrelated provider.
pub struct LaunchInputs<'a> {
    pub purpose: ProfilePurpose,
    pub approved_servers: &'a NormalizedMcpServers,
    pub model: Option<&'a str>,
    pub instructions: &'a str,
    pub has_skill_instructions: bool,
    pub intent_policy: &'a [policy::PolicySource],
}

/// Resolve the actual installed CLI, frozen captured/inherited environment and
/// bounded version through the existing installed-provider service. Other paths
/// defer; no directory is rewritten and no auth/helper command is executed.
pub async fn acquire_native(provider: &str, workspace: &Path) -> NativeAcquisition {
    if provider != "claude-code" {
        return NativeAcquisition::Deferred(if provider == "pi" {
            DeferredReason::PolicyAuthority
        } else {
            DeferredReason::ProviderControls
        });
    }
    if std::env::consts::OS != "linux" || std::env::consts::ARCH != "x86_64" {
        return NativeAcquisition::Deferred(DeferredReason::Platform);
    }
    // Native WSL inherits additional Windows policy roots. Do not infer a plain
    // Linux host merely from Rust's target OS or missing WSL environment flags.
    let plain_linux = tokio::task::spawn_blocking(|| {
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .is_ok_and(|s| !s.to_ascii_lowercase().contains("microsoft"))
    })
    .await
    .unwrap_or(false);
    if !plain_linux {
        return NativeAcquisition::Deferred(DeferredReason::Platform);
    }
    let Ok(context) = InstalledContext::discover(InstalledCli::Claude).await else {
        return NativeAcquisition::Deferred(DeferredReason::RuntimeVersion);
    };
    acquire_context(context, workspace.to_owned(), PathBuf::from("/etc")).await
}

async fn acquire_context(
    context: InstalledContext,
    workspace: PathBuf,
    etc_root: PathBuf,
) -> NativeAcquisition {
    let mut command = tokio::process::Command::new(context.runtime.path());
    command.current_dir(&workspace);
    context.apply(&mut command);
    let Ok((runtime_identity, version)) = context.observe(&command).await else {
        return NativeAcquisition::Deferred(DeferredReason::RuntimeVersion);
    };
    if version != "2.1.280 (Claude Code)" {
        return NativeAcquisition::Deferred(DeferredReason::RuntimeVersion);
    }
    let Some(environment) = command
        .as_std()
        .get_envs()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .map(|(k, v)| Some((k.to_str()?.to_owned(), v.to_str()?.to_owned())))
        .collect::<Option<BTreeMap<_, _>>>()
    else {
        return NativeAcquisition::Deferred(DeferredReason::AuthProjection);
    };
    let inspect_env = environment.clone();
    let inspect_workspace = workspace.clone();
    let inspect_etc = etc_root.clone();
    let inspected = tokio::task::spawn_blocking(move || {
        inspect_sources(&inspect_env, &inspect_workspace, &inspect_etc)
    })
    .await;
    let (auth_model, sources) = match inspected {
        Ok(Ok(value)) => value,
        Ok(Err(reason)) => return NativeAcquisition::Deferred(reason),
        Err(_) => return NativeAcquisition::Deferred(DeferredReason::PolicyAcquisition),
    };
    NativeAcquisition::Ready(Box::new(AcquiredNativeProfile {
        context,
        runtime_identity,
        workspace,
        etc_root,
        environment,
        auth_model,
        sources,
    }))
}

impl AcquiredNativeProfile {
    /// The selected native executable; never substitute an ACP adapter or a new
    /// discovery result. ACP acquisition is intentionally not certified here.
    #[must_use]
    pub fn executable(&self) -> &Path {
        self.context.runtime.path()
    }

    /// Freeze the acquired environment before applying profile.environment last.
    /// Use only for this native launch; the caller owns args, cwd and child reap.
    pub fn apply_environment(&self, command: &mut tokio::process::Command) {
        // InstalledContext normally retains trusted per-command overrides. This
        // sealed acquisition must instead retain the exact proven authority route.
        command.env_clear();
        self.context.apply(command);
    }

    /// Build an acquired managed launch. Any changed auth/policy/runtime, denial
    /// or publication failure is an error: never retry through the legacy path.
    /// # Errors
    /// Source/runtime changes require fresh acquisition, and all policy/build
    /// failures remain hard failures after this positive acquisition.
    pub async fn build(
        &self,
        inputs: LaunchInputs<'_>,
        directory: ProfileDirectory,
    ) -> ProfileResult<ManagedProviderProfile> {
        if inputs
            .intent_policy
            .iter()
            .any(|s| !matches!(s.format, policy::PolicyFormat::Intent))
        {
            return Err(changed());
        }
        // The native CLI expands identities before policy matching. Require
        // resolved identities; never let an unexpanded alias bypass a denial.
        if inputs.approved_servers.values().any(|server| match server {
            intent_acp::NormalizedMcpServer::Stdio { command, args, .. } => {
                command.contains("${") || args.iter().any(|s| s.contains("${"))
            }
            intent_acp::NormalizedMcpServer::Http { url, .. }
            | intent_acp::NormalizedMcpServer::Sse { url, .. } => url.contains("${"),
        }) {
            return Err(changed());
        }
        let mut command = tokio::process::Command::new(self.executable());
        command.current_dir(&self.workspace);
        self.context.apply(&mut command);
        let (identity, _) = self
            .context
            .observe(&command)
            .await
            .map_err(|_| changed())?;
        if identity != self.runtime_identity {
            return Err(changed());
        }
        let env = self.environment.clone();
        let workspace = self.workspace.clone();
        let etc = self.etc_root.clone();
        let (_, sources) =
            tokio::task::spawn_blocking(move || inspect_sources(&env, &workspace, &etc))
                .await
                .map_err(|_| changed())?
                .map_err(|_| changed())?;
        if sources != self.sources {
            return Err(changed());
        }
        let mut auth_model = self.auth_model.clone();
        if let Some(model) = inputs.model {
            auth_model.model = Some(model.to_owned());
        }
        let selected = staged::select(staged::SelectionRequest {
            provider: "claude-code",
            runtime: RuntimeIdentity {
                native_version: "2.1.280",
                adapter_version: None,
                os: "linux",
                arch: "x86_64",
            },
            sdk_version: None,
            purpose: inputs.purpose,
            approved_servers: inputs.approved_servers,
            auth_model: &auth_model,
            instructions: inputs.instructions,
            has_skill_instructions: inputs.has_skill_instructions,
            policy: staged::PolicyAcquisition {
                etc_root: &self.etc_root,
                #[cfg(test)]
                authority: staged::PolicyAuthority::LocalFilesOnly,
                additional: inputs.intent_policy,
            },
        });
        match selected {
            staged::ProfileSelection::Managed(plan) => plan.build(directory),
            staged::ProfileSelection::Deferred(_) => Err(changed()),
        }
    }
}

fn changed() -> ProfileError {
    ProfileError::UnsupportedPolicy {
        source: "native acquisition".into(),
        requirement: "acquired sources or runtime changed; reacquire before launching",
    }
}

fn read_json(path: &Path) -> Result<Option<Value>, DeferredReason> {
    use std::io::Read as _;
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(meta) if meta.is_file() => {}
        _ => return Err(DeferredReason::PolicyAcquisition),
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|_| DeferredReason::PolicyAcquisition)?;
    if !file
        .metadata()
        .map_err(|_| DeferredReason::PolicyAcquisition)?
        .is_file()
    {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| DeferredReason::PolicyAcquisition)?;
    if bytes.len() > 1024 * 1024 {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| DeferredReason::PolicyAcquisition)?;
    if !value.is_object() {
        return Err(DeferredReason::PolicyAcquisition);
    }
    Ok(Some(value))
}

fn absent(path: &Path) -> Result<(), DeferredReason> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(DeferredReason::PolicyAcquisition),
    }
}

fn inspect_sources(
    env: &BTreeMap<String, String>,
    workspace: &Path,
    etc: &Path,
) -> Result<(AuthModelContext, ConfigurationIdentity), DeferredReason> {
    // This is positive route evidence, NOT an inference from missing OAuth or
    // org metadata. Native PC/f/ms/hS in 2.1.280 returns custom_base_url here.
    let base = env
        .get("ANTHROPIC_BASE_URL")
        .ok_or(DeferredReason::PolicyAuthority)?;
    let url = reqwest::Url::parse(base).map_err(|_| DeferredReason::PolicyAuthority)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.host_str() == Some("api.anthropic.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DeferredReason::PolicyAuthority);
    }
    if env.get("ANTHROPIC_API_KEY").is_none_or(String::is_empty) {
        return Err(DeferredReason::AuthProjection);
    }
    // Unknown routing/host/policy overrides may change the native eligibility
    // decision or bypass the acquired filesystem tiers. Defer rather than strip.
    for key in env.keys().filter(|key| {
        key.starts_with("CLAUDE_")
            || key.starts_with("_CLAUDE_")
            || key.starts_with("ANTHROPIC_")
            || key.starts_with("WSL_")
    }) {
        if !matches!(
            key.as_str(),
            "CLAUDE_CONFIG_DIR"
                | "CLAUDE_CODE_EXECUTABLE"
                | "ANTHROPIC_BASE_URL"
                | "ANTHROPIC_API_KEY"
                | "ANTHROPIC_MODEL"
                | "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"
        ) {
            return Err(DeferredReason::PolicyAuthority);
        }
    }
    let home = env
        .get("HOME")
        .filter(|v| !v.is_empty())
        .ok_or(DeferredReason::AuthProjection)?;
    let config = env
        .get("CLAUDE_CONFIG_DIR")
        .filter(|v| !v.is_empty())
        .map_or_else(|| Path::new(home).join(".claude"), PathBuf::from);
    if !config.is_absolute() || !workspace.is_absolute() {
        return Err(DeferredReason::AuthProjection);
    }
    // Unresolved helper/cache/profile/drop-in sources are never hidden by the
    // new home. The supported source subset is ordinary local MCP restrictions.
    for path in [
        etc.join("claude-code/managed-mcp.json"),
        etc.join("claude-code/managed-settings.d"),
        config.join("remote-settings.json"),
        config.join("policy-limits.json"),
        config.join(".credentials.json"),
        config.join("state"),
    ] {
        absent(&path)?;
    }
    // Anthropic unified profiles are a separate native auth source, with their
    // own token refresh. Native QH/ue resolves this directory via XDG or HOME.
    let anthropic = env
        .get("XDG_CONFIG_HOME")
        .filter(|s| !s.trim().is_empty())
        .map_or_else(
            || Path::new(home).join(".config/anthropic"),
            |s| Path::new(s.trim()).join("anthropic"),
        );
    absent(&anthropic)?;
    let local = local_sources::resolve(workspace, Path::new(home))?;
    let local_documents = local
        .paths
        .iter()
        .map(|path| read_json(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut documents = Vec::new();
    let user = read_json(&config.join("settings.json"))?.unwrap_or_else(|| json!({}));
    let mut auth = AuthModelContext::default();
    for (key, value) in env {
        if auth::credential_env_allowed("claude-code", key) {
            auth.credential_environment
                .insert(key.clone(), value.clone());
        }
    }
    for document in std::iter::once(user.clone()).chain(
        local_documents
            .iter()
            .map(|value| value.clone().unwrap_or_else(|| json!({}))),
    ) {
        if [
            "policyHelper",
            "policyHelpers",
            "allowedMcpServers",
            "deniedMcpServers",
            "allowManagedMcpServersOnly",
            "managedMcpServers",
        ]
        .iter()
        .any(|key| document.get(*key).is_some())
        {
            return Err(DeferredReason::PolicyAuthority);
        }
        // Settings can inject route/authority overrides even if they are not
        // classified as credentials. Require all relevant values to agree with
        // the already captured launch environment rather than guessing precedence.
        if let Some(settings_env) = document.get("env").and_then(Value::as_object) {
            for (key, value) in settings_env {
                if (key.starts_with("CLAUDE_")
                    || key.starts_with("_CLAUDE_")
                    || key.starts_with("ANTHROPIC_")
                    || key == "HOME"
                    || key.starts_with("XDG_"))
                    && env.get(key).map(String::as_str) != value.as_str()
                {
                    return Err(DeferredReason::PolicyAuthority);
                }
            }
        }
        let projected =
            auth::project_claude_settings(&document).map_err(|_| DeferredReason::AuthProjection)?;
        if let Some(settings_env) = projected["env"].as_object() {
            for (key, value) in settings_env {
                if env.get(key).map(String::as_str) != value.as_str() {
                    return Err(DeferredReason::AuthProjection);
                }
            }
        }
        documents.push(document);
    }
    // Until precedence for project model customizations is proven, don't drop them.
    for document in &documents[1..] {
        if ["model", "modelOverrides", "availableModels"]
            .iter()
            .any(|k| document.get(k).is_some())
        {
            return Err(DeferredReason::AuthProjection);
        }
    }
    for state_path in [
        config.join(".claude.json"),
        Path::new(home).join(".claude.json"),
        config.join(".config.json"),
    ] {
        let state = read_json(&state_path)?.unwrap_or_else(|| json!({}));
        if ["oauthAccount", "primaryApiKey", "profile", "profiles"]
            .iter()
            .any(|key| state.get(key).is_some())
        {
            return Err(DeferredReason::AuthProjection);
        }
        documents.push(state);
    }
    auth.model = env
        .get("ANTHROPIC_MODEL")
        .cloned()
        .or_else(|| user["model"].as_str().map(str::to_owned));
    auth.claude_settings =
        Some(auth::project_claude_settings(&user).map_err(|_| DeferredReason::AuthProjection)?);
    let native =
        read_json(&etc.join("claude-code/managed-settings.json"))?.unwrap_or_else(|| json!({}));
    let policy = policy::read_linux_system_policy("claude-code", etc, &[])
        .map_err(|_| DeferredReason::PolicyAcquisition)?;
    if policy.has_unverified_claude_matchers() {
        return Err(DeferredReason::PolicyAcquisition);
    }
    documents.push(native);
    let sources = ConfigurationIdentity::from_value(
        &json!({"documents":documents,"environment":env,"localSources":{"paths":local.paths,"provenance":local.provenance,"documents":local_documents}}),
    );
    Ok((auth, sources))
}

#[cfg(all(test, unix))]
mod tests;
