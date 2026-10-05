//! Daemon-owned launch configuration for unmodified upstream providers.
//!
//! Apply the result after ordinary provider command construction, retain it
//! until the child/process group has exited, and use its session MCP and meta
//! for new/load/resume. These controls are best effort: diagnostics deliberately
//! distinguish thread controls from native catalogs, policy and authentication.
mod codex;
mod policy;
mod storage;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod upstream_tests;

pub use codex::{NativeMcpEntry, NativeTransport};
pub use policy::{constrain_mcp, load_mcp_policy, McpPolicy, PolicySource};
pub use storage::cleanup_abandoned_profiles;

use intent_acp::mcp_config::{to_auggie_mcp_config, to_opencode_mcp_config, NormalizedMcpServers};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaunchPurpose {
    Persistent,
    Completion,
    PromptTest,
    ModelProbe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityOutcome {
    Verified,
    Partial,
    Unsupported,
    Unverified,
    TestOnly,
}

#[derive(Clone, Debug)]
pub struct ProfileCapabilities {
    pub mcp: CapabilityOutcome,
    pub skills: CapabilityOutcome,
    pub managed_policy: CapabilityOutcome,
    pub authenticated_resume: CapabilityOutcome,
}

/// Inputs are captured by the daemon, not read from mutable process-global env.
/// `policy_sources` MUST include applicable remote/MDM policy: use `Unavailable`
/// when one is known but cannot be reconstructed. Local defaults are checked too.
#[derive(Clone, Copy)]
pub struct ProviderProfileRequest<'a> {
    pub provider_id: &'a str,
    pub detected_version: Option<&'a str>,
    pub purpose: LaunchPurpose,
    pub owned_root: &'a Path,
    pub persistent_identity: Option<&'a str>,
    pub resume: bool,
    pub home: &'a Path,
    /// Resolved `CODEX_HOME` or `OpenCode` configuration directory before isolation.
    pub provider_home: Option<&'a Path>,
    pub workspace_root: &'a Path,
    pub launch_cwd: &'a Path,
    pub owned_mcp: &'a NormalizedMcpServers,
    pub policy_sources: &'a [PolicySource],
    /// Additional native layers discovered by the launcher, suppression only.
    pub native_config_files: &'a [PathBuf],
    pub native_skill_roots: &'a [PathBuf],
    /// Daemon-generated `OpenCode` routing/model/permission config, e.g. Unsloth.
    /// Never pass ambient `OPENCODE_CONFIG_CONTENT` here.
    pub trusted_launch_config: Option<&'a Value>,
}

#[derive(Clone, Debug)]
pub struct ProviderProfileDiagnostic {
    pub provider: String,
    pub detected_version: Option<String>,
    pub purpose: LaunchPurpose,
    pub code: &'static str,
    pub message: &'static str,
}

#[derive(Debug)]
pub struct ProviderProfileError {
    pub code: &'static str,
    pub message: &'static str,
}
impl ProviderProfileError {
    fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}
impl std::fmt::Display for ProviderProfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for ProviderProfileError {}

/// This value owns the profile lease. Do not drop it merely because spawn or
/// handshake returned: temporary files must outlive all child descendants.
/// Deliberately no Debug implementation: env/config may contain credentials.
pub struct ProviderLaunchProfile {
    pub env: BTreeMap<String, String>,
    pub remove_env: BTreeSet<String>,
    pub args: Vec<String>,
    /// Native Pi wrapper arguments; NEVER append these to pi-acp itself.
    pub native_args: Vec<String>,
    pub session_meta: Value,
    pub approved_mcp: NormalizedMcpServers,
    /// Codex must send an empty ACP list to preserve the full `CODEX_CONFIG` map.
    pub session_mcp: NormalizedMcpServers,
    pub mcp_name_mapping: BTreeMap<String, String>,
    pub native_mcp_inventory: Vec<NativeMcpEntry>,
    pub policy: McpPolicy,
    pub diagnostics: Vec<ProviderProfileDiagnostic>,
    pub capabilities: ProfileCapabilities,
    provider: String,
    version: Option<String>,
    purpose: LaunchPurpose,
    storage: storage::ProfileStorage,
}

impl ProviderLaunchProfile {
    #[must_use]
    pub fn path(&self) -> &Path {
        self.storage.path()
    }

    /// Final application, after build_command/extra_env/login-shell merging.
    /// In particular this replaces spawn's default `CODEX_CONFIG` while retaining
    /// both required native-subagent denials in the owned profile config.
    pub fn apply_to_command(&self, command: &mut tokio::process::Command) {
        for key in &self.remove_env {
            command.env_remove(key);
        }
        command.envs(&self.env).args(&self.args);
    }

    /// Merge into the ACP `_meta` object without discarding existing tools:[],
    /// rules, routing or other caller-owned metadata. Profile controls win.
    pub fn merge_session_meta(&self, meta: &mut Value) {
        merge_json(meta, &self.session_meta);
    }

    fn diagnostic(&mut self, code: &'static str, message: &'static str) {
        if !self.diagnostics.iter().any(|d| d.code == code) {
            self.diagnostics.push(ProviderProfileDiagnostic {
                provider: self.provider.clone(),
                detected_version: self.version.clone(),
                purpose: self.purpose,
                code,
                message,
            });
        }
    }
}

fn merge_json(target: &mut Value, overlay: &Value) {
    if let (Some(target), Some(overlay)) = (target.as_object_mut(), overlay.as_object()) {
        for (key, value) in overlay {
            merge_json(target.entry(key).or_insert(Value::Null), value);
        }
    } else {
        *target = overlay.clone();
    }
}

/// Pure orchestration plus bounded local config reads/writes; never executes a
/// credential helper, MCP command, network request or adapter modification.
///
/// # Errors
/// Returns a redacted error for unknown providers, unsafe/missing profile paths,
/// incompatible enforced policy, malformed native config or storage failures.
pub fn prepare_provider_profile(
    request: ProviderProfileRequest<'_>,
) -> Result<ProviderLaunchProfile, ProviderProfileError> {
    if intent_providers::find_provider(request.provider_id).is_none() {
        return Err(ProviderProfileError::new(
            "unknown-provider",
            "No profile strategy exists for this provider.",
        ));
    }
    if !request.launch_cwd.is_absolute() || !request.workspace_root.is_absolute() {
        return Err(ProviderProfileError::new(
            "config-boundary-escape",
            "Provider configuration requires absolute workspace and launch paths.",
        ));
    }
    if request.resume && request.purpose != LaunchPurpose::Persistent {
        return Err(ProviderProfileError::new(
            "resume-profile-missing",
            "Temporary provider profiles cannot resume a persistent session.",
        ));
    }
    let identity = if request.purpose == LaunchPurpose::Persistent {
        Some(
            request
                .persistent_identity
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    ProviderProfileError::new(
                        "profile-identity-missing",
                        "Persistent providers require a stable agent identity.",
                    )
                })?,
        )
    } else {
        None
    };
    let mut sources = local_policy_sources(request.provider_id, request.home);
    if let Some(home) = request.provider_home {
        if matches!(request.provider_id, "codex" | "grok") {
            sources.push(PolicySource::Unsupported(home.join("managed_config.toml")));
            if request.provider_id == "grok" {
                sources.push(PolicySource::Unsupported(home.join("requirements.toml")));
            }
        }
    }
    sources.extend_from_slice(request.policy_sources);
    let policy = load_mcp_policy(request.provider_id, &sources)?;
    let approved_mcp = if request.purpose == LaunchPurpose::Persistent {
        policy.constrain(request.owned_mcp)
    } else {
        NormalizedMcpServers::new()
    };
    if request.purpose == LaunchPurpose::Persistent
        && request.owned_mcp.contains_key("workspace-mcp")
        && !approved_mcp.contains_key("workspace-mcp")
    {
        return Err(ProviderProfileError::new("managed-policy-conflict", "Managed policy denies the workspace MCP bridge; Intent cannot bypass this restriction."));
    }
    let mut profile = ProviderLaunchProfile {
        storage: storage::ProfileStorage::create(
            request.owned_root,
            request.provider_id,
            identity,
            request.resume,
        )?,
        env: BTreeMap::new(),
        remove_env: BTreeSet::new(),
        args: Vec::new(),
        native_args: Vec::new(),
        session_meta: json!({}),
        session_mcp: approved_mcp.clone(),
        approved_mcp,
        mcp_name_mapping: BTreeMap::new(),
        native_mcp_inventory: Vec::new(),
        policy,
        diagnostics: Vec::new(),
        provider: request.provider_id.into(),
        version: request.detected_version.map(str::to_owned),
        purpose: request.purpose,
        capabilities: ProfileCapabilities {
            mcp: CapabilityOutcome::Partial,
            skills: CapabilityOutcome::Partial,
            managed_policy: CapabilityOutcome::Partial,
            authenticated_resume: CapabilityOutcome::Unverified,
        },
    };
    if request.purpose != LaunchPurpose::Persistent && !request.owned_mcp.is_empty() {
        profile.diagnostic("ephemeral-mcp-disabled", "Ephemeral provider calls do not receive MCP servers; existing native tool-removal controls must also be retained.");
    } else if profile.approved_mcp.len() != request.owned_mcp.len() {
        profile.diagnostic(
            "managed-policy-filtered",
            "Managed policy removed prohibited MCP servers from this launch.",
        );
    }
    match request.provider_id {
        "codex" => codex::prepare(&request, &mut profile)?,
        "claude-code" => {
            profile.session_meta = json!({"claudeCode":{"options":{"settingSources":["user"],"strictMcpConfig":true,"extraArgs":{"disable-slash-commands":null}}}});
            profile.diagnostic("native-plugin-connectors-unverified", "Strict MCP and skill-command controls were tested with file-based sources; user plugins, hooks and authenticated connectors remain unverified.");
        }
        "opencode" | "unsloth" => prepare_opencode(&request, &mut profile)?,
        "pi" => {
            profile.native_args = vec![
                "--no-skills".into(),
                "--no-extensions".into(),
                "--no-prompt-templates".into(),
            ];
            if request.purpose != LaunchPurpose::Persistent {
                profile.native_args.push("--no-context-files".into());
            }
            profile.capabilities.skills = if request.detected_version == Some("0.81.0") {
                CapabilityOutcome::Verified
            } else {
                CapabilityOutcome::Unverified
            };
            profile.diagnostic("explicit-extension-required", "Apply native suppression flags inside the Pi wrapper and retain Intent's explicit -e bridge; Pi ACP ignores session MCP lists.");
            profile.diagnostic("native-builtin-extension-residual", "Pi 0.81.0 keeps built-in extensions under ambient extension suppression; actual bridge and authenticated resume need integration verification.");
        }
        "auggie" => {
            profile.storage.write(
                "mcp.json",
                to_auggie_mcp_config(&profile.approved_mcp)
                    .to_string()
                    .as_bytes(),
            )?;
            profile.args = vec![
                "--mcp-config".into(),
                profile.path().join("mcp.json").to_string_lossy().into(),
            ];
            profile.diagnostic("native-skills-residual", "Auggie has no audited global skill-off switch; home/project/plugin skills can duplicate Intent skills. Use clean native skill roots.");
            profile.diagnostic("native-mcp-residual", "Auggie's explicit MCP file replaces settings MCP; registry/plugin composition remains unverified. Auth and session storage stay in their original locations.");
        }
        "droid" => {
            profile.args.push("--disable-builtin-skills".into());
            profile.capabilities.mcp = CapabilityOutcome::Unverified;
            profile.capabilities.skills = CapabilityOutcome::Unverified;
            profile.diagnostic("provider-control-unverified", "Droid's built-in skill flag is documented but this binary was unavailable for testing; user/project MCP and skills can still load. Remove unwanted native entries and restart.");
        }
        "grok" => {
            for key in [
                "GROK_CLAUDE_MCPS_ENABLED",
                "GROK_CURSOR_MCPS_ENABLED",
                "GROK_CLAUDE_SKILLS_ENABLED",
                "GROK_CURSOR_SKILLS_ENABLED",
            ] {
                profile.env.insert(key.into(), "false".into());
            }
            profile.capabilities.mcp = CapabilityOutcome::Unverified;
            profile.capabilities.skills = CapabilityOutcome::Unverified;
            profile.diagnostic("provider-control-unverified", "Grok compatibility switches are source-backed but not runtime-verified for this installation; native .grok/.agents/plugin sources remain. Use clean native configuration.");
        }
        "cortex" => {
            profile.capabilities.mcp = CapabilityOutcome::Unsupported;
            profile.capabilities.skills = CapabilityOutcome::Unverified;
            profile.session_mcp.clear();
            profile.diagnostic("mcp-delivery-unsupported", "The configured legacy Cortex adapter cannot receive Intent MCP servers; native loading is unverified. Newer Cortex CLI flags are not assumed compatible.");
        }
        "antigravity" => {
            profile.capabilities.skills = CapabilityOutcome::Unverified;
            profile.diagnostic("existing-profile-required", "Retain Antigravity's existing private SessionProfile and tool guards; this generic profile does not replace them. Project skills and macOS authentication remain unverified here.");
        }
        "mock" => {
            profile.capabilities.mcp = CapabilityOutcome::TestOnly;
            profile.capabilities.skills = CapabilityOutcome::TestOnly;
            profile.diagnostic(
                "test-provider-only",
                "Mock checks prove Intent plumbing only, never upstream provider isolation.",
            );
        }
        _ => {
            return Err(ProviderProfileError::new(
                "provider-control-unverified",
                "This catalog provider has no audited profile strategy yet.",
            ))
        }
    }
    profile.diagnostic("resume-unverified", "Stable profile storage is retained across stops and restarts; authenticated provider resume and credential refresh are not established by configuration tests.");
    Ok(profile)
}

fn prepare_opencode(
    request: &ProviderProfileRequest<'_>,
    profile: &mut ProviderLaunchProfile,
) -> Result<(), ProviderProfileError> {
    let config_dir = profile.path().join("config");
    storage::private_dir(&config_dir)?;
    profile.env.insert(
        "XDG_CONFIG_HOME".into(),
        config_dir.to_string_lossy().into(),
    );
    profile.env.insert(
        "OPENCODE_CONFIG_DIR".into(),
        config_dir.to_string_lossy().into(),
    );
    profile
        .remove_env
        .extend(["OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT"].map(str::to_owned));
    profile
        .env
        .insert("OPENCODE_DISABLE_PROJECT_CONFIG".into(), "1".into());
    profile
        .env
        .insert("OPENCODE_DISABLE_EXTERNAL_SKILLS".into(), "1".into());
    let original = request
        .provider_home
        .map_or_else(|| request.home.join(".config/opencode"), Path::to_path_buf);
    let mut config = json!({});
    if let Some(bytes) = storage::read_optional(&original.join("opencode.json"))? {
        if let Ok(native) = serde_json::from_slice::<Value>(&bytes) {
            seed_opencode_routing(&native, &mut config);
        } else {
            profile.diagnostic("model-config-unverified", "The original OpenCode routing configuration could not be parsed as JSON; set custom endpoints explicitly in Intent.");
        }
    }
    if original.join("opencode.jsonc").exists() {
        profile.diagnostic(
            "model-config-unverified",
            "OpenCode JSONC routing was not imported; set custom endpoints explicitly in Intent.",
        );
    }
    if let Some(owned) = request.trusted_launch_config {
        merge_json(&mut config, owned);
    }
    config["mcp"] = to_opencode_mcp_config(&profile.approved_mcp);
    // Normalized imports carry explicit credentials, not an OAuth lifecycle.
    // Preserve discovery's oauth:false instead of re-enabling native OAuth.
    if let Some(servers) = config["mcp"].as_object_mut() {
        for server in servers.values_mut() {
            if server["type"] == "remote" {
                server["oauth"] = json!(false);
            }
        }
    }
    // A caller's blanket denial is stronger than the selective default. Never
    // turn a tool-free permission string into a permissive object.
    if config["permission"] != "deny" {
        if let Some(permission) = config["permission"].as_str() {
            config["permission"] = json!({"*":permission});
        } else if config["permission"].is_null() {
            config["permission"] = json!({});
        } else if !config["permission"].is_object() {
            return Err(ProviderProfileError::new(
                "profile-config-invalid",
                "The daemon's OpenCode permission configuration is invalid.",
            ));
        }
        config["permission"]["skill"] = json!("deny");
        // Retain the existing native subagent denial from provider env assembly.
        config["permission"]["task"] = json!("deny");
    }
    profile
        .env
        .insert("OPENCODE_CONFIG_CONTENT".into(), config.to_string());
    profile.diagnostic("native-home-config-residual", "OpenCode still discovers HOME/.opencode and remote/plugin config. Native skill permission is denied, but discovery is not removed; use clean native sources.");
    profile.diagnostic("auth-store-unverified", "OpenCode auth and session data locations are unchanged; custom routing is narrowly seeded and authenticated resume is unverified.");
    Ok(())
}

fn seed_opencode_routing(native: &Value, target: &mut Value) {
    for key in ["model", "small_model"] {
        if native[key].is_string() {
            target[key] = native[key].clone();
        }
    }
    if let Some(providers) = native["provider"].as_object() {
        for (name, provider) in providers {
            let mut seed = json!({});
            for key in ["name", "npm"] {
                if provider[key].is_string() {
                    seed[key] = provider[key].clone();
                }
            }
            for key in ["baseURL", "apiKey", "headers", "timeout"] {
                if let Some(value) = provider["options"].get(key) {
                    seed["options"][key] = value.clone();
                }
            }
            // Model descriptions/capability maps do not load MCP or skills.
            if provider["models"].is_object() {
                seed["models"] = provider["models"].clone();
            }
            target["provider"][name] = seed;
        }
    }
}

/// Known local policy locations. Remote enrollment / MDM discovery remains a
/// launcher obligation; an applicable unreadable policy must be `Unavailable`.
#[must_use]
pub fn local_policy_sources(provider: &str, home: &Path) -> Vec<PolicySource> {
    let mut sources = Vec::new();
    if provider == "codex" {
        #[cfg(unix)]
        {
            sources.push(PolicySource::CodexRequirements(PathBuf::from(
                "/etc/codex/requirements.toml",
            )));
            sources.push(PolicySource::Unsupported(PathBuf::from(
                "/etc/codex/managed_config.toml",
            )));
        }
        sources.push(PolicySource::Unsupported(
            home.join(".codex/managed_config.toml"),
        ));
    }
    if provider == "claude-code" {
        #[cfg(target_os = "linux")]
        let root = PathBuf::from("/etc/claude-code");
        #[cfg(target_os = "macos")]
        let root = PathBuf::from("/Library/Application Support/ClaudeCode");
        #[cfg(windows)]
        let root = PathBuf::from("C:/Program Files/ClaudeCode");
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        {
            sources.push(PolicySource::ClaudeExclusive(root.join("managed-mcp.json")));
            sources.push(PolicySource::ClaudeSettings(
                root.join("managed-settings.json"),
            ));
            // Directory policies require faithful composition; detect rather
            // than silently skip an enforced layer we cannot parse yet.
            if root.join("managed-settings.d").exists() {
                sources.push(PolicySource::Unavailable);
            }
        }
    }
    if matches!(provider, "opencode" | "unsloth") {
        #[cfg(target_os = "linux")]
        let root = PathBuf::from("/etc/opencode");
        #[cfg(target_os = "macos")]
        let root = PathBuf::from("/Library/Application Support/opencode");
        #[cfg(windows)]
        let root = PathBuf::from("C:/ProgramData/opencode");
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        for file in ["opencode.json", "opencode.jsonc"] {
            sources.push(PolicySource::Unsupported(root.join(file)));
        }
    }
    if provider == "grok" {
        for file in ["managed_config.toml", "requirements.toml"] {
            sources.push(PolicySource::Unsupported(home.join(".grok").join(file)));
            #[cfg(unix)]
            sources.push(PolicySource::Unsupported(
                PathBuf::from("/etc/grok").join(file),
            ));
        }
        #[cfg(target_os = "linux")]
        sources.push(PolicySource::Unsupported(PathBuf::from(
            "/etc/claude-code/managed-settings.json",
        )));
        #[cfg(target_os = "macos")]
        sources.push(PolicySource::Unsupported(PathBuf::from(
            "/Library/Application Support/ClaudeCode/managed-settings.json",
        )));
    }
    sources
}
