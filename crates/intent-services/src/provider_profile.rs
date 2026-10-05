//! Intent-owned provider profile construction, separate from runtime activation.
//!
//! Candidates contain useful, tested controls but are not isolation certificates.
//! Integration must call `ensure_launchable` before spawning. Native loader gaps
//! are recorded per provider, never disguised by an isolated HOME or empty MCP.
//! This module does not change any existing production launch path.

use std::collections::BTreeMap;
use std::fmt;

use intent_acp::NormalizedMcpServers;
use intent_providers::launch_overrides::EnvironmentOverrides;
use serde_json::{json, Value};

pub mod auth;
pub mod policy;
mod storage;
pub use storage::{ProfileDirectory, ProfileIdentity};

pub type ProfileResult<T> = std::result::Result<T, ProfileError>;

#[derive(Debug)]
pub enum ProfileError {
    Io,
    InUse,
    InvalidAuth(&'static str),
    PolicyIo {
        source: String,
    },
    UnsupportedPolicy {
        source: String,
        requirement: &'static str,
    },
    PolicyDenied {
        source: String,
        subject: &'static str,
    },
    UnsupportedIsolation {
        provider: String,
        missing: &'static str,
    },
    EphemeralCatalog,
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => f.write_str("cannot create or update private provider profile; verify the owned state directory and permissions"),
            Self::InUse => f.write_str("provider profile is still in use; reap the child process group before deleting session state"),
            Self::InvalidAuth(reason) => write!(f, "cannot preserve provider authentication/model routing: {reason}"),
            Self::PolicyIo { source } => write!(f, "cannot read applicable host policy ({source}); restore access before retrying"),
            Self::UnsupportedPolicy { source, requirement } => write!(f, "unsupported host policy ({source}): {requirement}; a compatible policy adapter is required"),
            Self::PolicyDenied { source, subject } => write!(f, "host policy ({source}) denies {subject}; select an allowed configuration"),
            Self::UnsupportedIsolation { provider, missing } => write!(f, "cannot certify managed {provider} isolation: {missing}"),
            Self::EphemeralCatalog => f.write_str("ephemeral provider profiles require zero MCP servers and no skill/extension injection"),
        }
    }
}
impl std::error::Error for ProfileError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfilePurpose {
    Interactive,
    Ephemeral,
}

/// Auth/model data is explicitly supplied by the owning service, never obtained
/// by copying a native config directory. No Debug or serialization: may be secret.
#[derive(Clone, Default)]
pub struct AuthModelContext {
    pub model: Option<String>,
    pub endpoint: Option<ModelEndpoint>,
    pub credential_environment: BTreeMap<String, String>,
    pub credentials: Vec<auth::CredentialFile>,
    /// Output of `project_codex_config`, validated again before use.
    pub codex_routing_toml: Option<String>,
    /// Native Claude settings; only auth/model fields are projected.
    pub claude_settings: Option<Value>,
    pub approval: Option<String>,
    pub sandbox: Option<String>,
}

/// An explicit OpenAI-compatible model route. No plugin/package/module path or
/// executable helper is accepted. Secrets intentionally have no Debug output.
#[derive(Clone)]
pub struct ModelEndpoint {
    pub provider_id: String,
    pub model_id: String,
    pub base_url: String,
    pub api_key: String,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub compaction_reserved: Option<u64>,
}

/// Controls with source evidence; gaps explicitly distinguish them from actual
/// effective-inventory verification for a selected runtime/version/platform.
#[derive(Debug, Clone)]
pub struct CapabilityEvidence {
    pub source_baseline: &'static str,
    pub controls: &'static [&'static str],
    pub missing: &'static [&'static str],
}

/// A concrete profile to inspect/test. Callers must retain `directory` until all
/// child processes are reaped. Runtime args go to the native CLI (Pi via its
/// existing command wrapper), not blindly to the ACP adapter.
pub struct ManagedProviderProfile {
    pub directory: ProfileDirectory,
    pub environment: EnvironmentOverrides,
    pub runtime_args: Vec<String>,
    pub session_meta: Value,
    pub model: Option<String>,
    pub capability: CapabilityEvidence,
    provider: String,
}

impl ManagedProviderProfile {
    /// Gate activation, not construction. There is no user-supplied "isolated"
    /// boolean that can turn unverified native discovery into certified support.
    /// # Errors
    /// Returns the first concrete missing runtime capability/evidence requirement.
    pub fn ensure_launchable(&self) -> ProfileResult<()> {
        if let Some(missing) = self.capability.missing.first() {
            return Err(ProfileError::UnsupportedIsolation {
                provider: self.provider.clone(),
                missing,
            });
        }
        Ok(())
    }
}

/// Observed native runtime and adapter versions, supplied by the existing
/// bounded executable probe. Never infer the native version from an adapter pin.
pub struct RuntimeIdentity<'a> {
    pub native_version: &'a str,
    pub adapter_version: Option<&'a str>,
    pub os: &'a str,
    pub arch: &'a str,
}

pub struct ProfileRequest<'a> {
    pub provider: &'a str,
    pub runtime: RuntimeIdentity<'a>,
    pub purpose: ProfilePurpose,
    pub directory: ProfileDirectory,
    pub approved_servers: &'a NormalizedMcpServers,
    pub auth_model: &'a AuthModelContext,
    pub policy: &'a policy::HostPolicySnapshot,
}

/// Prepare a profile for an explicitly verified native path. Pi 0.81.0 on Linux
/// `x86_64` has credential-free real loader and CLI startup/resume fixtures. ACP
/// gateway delivery is a separate integration capability, not implied here.
/// # Errors
/// Unsupported versions/platforms or unresolved delivery/isolation capabilities
/// return actionable errors. This never returns an ambient fallback command.
pub fn prepare_provider_profile(
    request: ProfileRequest<'_>,
) -> ProfileResult<ManagedProviderProfile> {
    let mut profile = prepare_profile_candidate(
        request.provider,
        request.purpose,
        request.directory,
        request.approved_servers,
        request.auth_model,
        request.policy,
    )?;
    if request.provider == "pi" {
        if request.runtime.native_version != "0.81.0"
            || request.runtime.os != "linux"
            || request.runtime.arch != "x86_64"
        {
            return Err(ProfileError::UnsupportedIsolation {provider:"pi".into(), missing:"profile controls verified with Pi 0.81.0 on Linux x86_64 only; verify the installed runtime before activation"});
        }
        if request.runtime.adapter_version.is_some() {
            return Err(ProfileError::UnsupportedIsolation {
                provider: "pi".into(),
                missing: "verify ACP wrapper delivery of the owned native arguments and environment before activation",
            });
        }
        if !request.approved_servers.is_empty() {
            return Err(ProfileError::UnsupportedIsolation {provider:"pi".into(), missing:"attach and verify the Intent-owned gateway extension for interactive MCP delivery"});
        }
        profile.capability.source_baseline = "Pi 0.81.0 Linux x86_64 actual resource loader and native RPC CLI startup/resume fixtures";
        profile.capability.missing = &[];
    }
    profile.ensure_launchable()?;
    Ok(profile)
}

/// Build only owned files, authoritative environment operations, and metadata.
/// This deliberately returns inspectable candidates for runtimes with known
/// missing controls; production integration must enforce `ensure_launchable`.
/// Approved MCP input must already have catalog provenance/transport checks.
/// # Errors
/// Denied policy, unsafe auth inputs, nonempty ephemeral catalog, unknown
/// providers, unsupported credential files, or private-file I/O fail explicitly.
pub fn prepare_profile_candidate(
    provider: &str,
    purpose: ProfilePurpose,
    directory: ProfileDirectory,
    approved_servers: &NormalizedMcpServers,
    auth_model: &AuthModelContext,
    policy: &policy::HostPolicySnapshot,
) -> ProfileResult<ManagedProviderProfile> {
    if purpose == ProfilePurpose::Ephemeral && !approved_servers.is_empty() {
        return Err(ProfileError::EphemeralCatalog);
    }
    let capability = capability_evidence(provider)?;
    for (name, server) in approved_servers {
        policy.validate_server(name, server)?;
    }
    let features = if provider == "codex" {
        BTreeMap::from([("multi_agent".into(), false)])
    } else {
        BTreeMap::new()
    };
    policy.validate_launch(
        auth_model.approval.as_deref(),
        auth_model.sandbox.as_deref(),
        &features,
    )?;
    let mut environment = EnvironmentOverrides::default();
    // Prevent runtime injection via inherited launch configuration. PATH/runtime
    // executable selection belongs to the caller; ordinary network env survives.
    for key in [
        "NODE_OPTIONS",
        "NODE_PATH",
        "BUN_OPTIONS",
        "PYTHONPATH",
        "PYTHONSTARTUP",
    ] {
        environment.remove(key);
    }
    let custom_codex_names = auth_model
        .codex_routing_toml
        .as_deref()
        .map(intent_core::cli_env::CodexEnvNames::from_config)
        .transpose()
        .map_err(|_| ProfileError::InvalidAuth("invalid custom Codex credential references"))?
        .unwrap_or_default();
    for (key, value) in &auth_model.credential_environment {
        if !(auth::credential_env_allowed(provider, key)
            || provider == "codex" && custom_codex_names.contains(key))
        {
            return Err(ProfileError::InvalidAuth(
                "unsupported credential environment key",
            ));
        }
        environment.set(key, value);
    }
    let home = directory
        .path()
        .to_str()
        .ok_or(ProfileError::Io)?
        .to_owned();
    let mut runtime_args = Vec::new();
    let mut session_meta = json!({});
    match provider {
        "claude-code" => {
            environment.set("CLAUDE_CONFIG_DIR", &home);
            environment.remove("CLAUDE_MODEL_CONFIG");
            let settings = auth_model
                .claude_settings
                .as_ref()
                .map(auth::project_claude_settings)
                .transpose()?
                .unwrap_or_else(|| json!({}));
            session_meta = json!({"claudeCode":{"options":{
                "strictMcpConfig":true, "settingSources":[], "settings":settings,
                "extraArgs":{"disable-slash-commands":""}
            }}});
            if purpose == ProfilePurpose::Ephemeral {
                session_meta["claudeCode"]["options"]["tools"] = json!([]);
            }
        }
        "codex" => {
            environment.set("CODEX_HOME", &home);
            environment.set("DISABLE_MCP_CONFIG_FILTERING", "true");
            let mut config = auth_model
                .codex_routing_toml
                .as_deref()
                .map(auth::project_codex_config)
                .transpose()?
                .unwrap_or_else(|| json!({}));
            // Preserve both generations of the native subagent denial. Never
            // merge ambient CODEX_CONFIG, whose MCP/plugins may be executable.
            let denial: Value =
                serde_json::from_str(intent_providers::CODEX_SUBAGENT_POLICY_CONFIG)
                    .map_err(|_| ProfileError::InvalidAuth("invalid built-in Codex policy"))?;
            for (key, value) in denial.as_object().into_iter().flatten() {
                config[key] = value.clone();
            }
            config["features"]["multi_agent"] = json!(false);
            if let Some(approval) = &auth_model.approval {
                config["approval_policy"] = json!(approval);
            }
            if let Some(sandbox) = &auth_model.sandbox {
                config["sandbox_mode"] = json!(sandbox);
            }
            if let Some(model) = &auth_model.model {
                config["model"] = json!(model);
            }
            environment.set("CODEX_CONFIG", config.to_string());
        }
        "pi" => {
            environment.set("PI_CODING_AGENT_DIR", &home);
            // Pi migrates JSONL files at the agent-dir root. A dedicated
            // sessions subdirectory preserves IDs across native CLI startup.
            let sessions = directory.private_subdirectory("sessions")?;
            environment.set(
                "PI_CODING_AGENT_SESSION_DIR",
                sessions.to_str().ok_or(ProfileError::Io)?,
            );
            environment.remove("PI_ACP_PI_COMMAND");
            environment.remove("PI_PACKAGE_DIR");
            runtime_args.extend(
                [
                    "--no-skills",
                    "--no-extensions",
                    "--no-prompt-templates",
                    "--no-themes",
                    "--no-approve",
                    "--offline",
                ]
                .map(str::to_owned),
            );
            if purpose == ProfilePurpose::Ephemeral {
                runtime_args.push("--no-tools".into());
            }
            directory.write_private(
                "settings.json",
                b"{\"packages\":[],\"extensions\":[],\"skills\":[],\"prompts\":[]}",
            )?;
            if let Some(endpoint) = &auth_model.endpoint {
                if endpoint.api_key.starts_with('!') || endpoint.base_url.starts_with('!') {
                    return Err(ProfileError::InvalidAuth(
                        "Pi command-expanded model routing is not supported",
                    ));
                }
                let models = json!({"providers": {&endpoint.provider_id: {
                    "baseUrl": endpoint.base_url, "apiKey": endpoint.api_key,
                    "api": "openai-completions", "models": [{"id": endpoint.model_id, "name": endpoint.model_id,
                    "reasoning": false, "input": ["text"], "cost": {"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
                    "contextWindow": endpoint.context_window.unwrap_or(128_000),
                    "maxTokens": endpoint.max_output_tokens.unwrap_or(16_384)}]
                }}});
                directory.write_private("models.json", models.to_string().as_bytes())?;
            }
            // Interactive integration owns the reviewed -e gateway extension.
            // Its MCP credentials and live authorization remain in Intent.
        }
        "opencode" | "unsloth" => {
            for key in ["OPENCODE_CONFIG", "OPENCODE_CONFIG_DIR"] {
                environment.remove(key);
            }
            environment.set("OPENCODE_DISABLE_PROJECT_CONFIG", "true");
            environment.set("OPENCODE_DISABLE_EXTERNAL_SKILLS", "true");
            environment.set("XDG_CONFIG_HOME", &home);
            environment.set("XDG_DATA_HOME", &home);
            let mut config = json!({"permission":{"task":"deny","skill":"deny"},"mcp":intent_acp::to_opencode_mcp_config(approved_servers)});
            if let Some(model) = &auth_model.model {
                config["model"] = json!(model);
            }
            if provider == "unsloth" && auth_model.endpoint.is_none() {
                return Err(ProfileError::InvalidAuth(
                    "Unsloth requires its resolved managed model endpoint",
                ));
            }
            if let Some(endpoint) = &auth_model.endpoint {
                let effective_model = auth_model.model.as_deref().unwrap_or(&endpoint.model_id);
                config["provider"] = json!({&endpoint.provider_id: {
                    "npm": "@ai-sdk/openai-compatible", "name": endpoint.provider_id,
                    "options": {"baseURL": endpoint.base_url, "apiKey": endpoint.api_key},
                    "models": {effective_model: {"name": effective_model}}
                }});
                if let (Some(context), Some(output)) =
                    (endpoint.context_window, endpoint.max_output_tokens)
                {
                    config["provider"][&endpoint.provider_id]["models"][effective_model]["limit"] =
                        json!({"context":context,"output":output});
                }
                config["model"] = json!(format!("{}/{effective_model}", endpoint.provider_id));
                if provider == "unsloth" {
                    config["small_model"] = config["model"].clone();
                    if let Some(reserved) = endpoint.compaction_reserved {
                        config["compaction"] = json!({"auto":true,"reserved":reserved});
                    }
                }
            }
            environment.set("OPENCODE_CONFIG_CONTENT", config.to_string());
            // This does not suppress /etc/managed/account layers or native home
            // resource roots. Evidence below keeps the candidate unlaunchable.
        }
        "auggie" => {
            let path = directory.path().join("mcp.json");
            directory.write_private(
                "mcp.json",
                intent_acp::to_auggie_mcp_config(approved_servers)
                    .to_string()
                    .as_bytes(),
            )?;
            runtime_args.extend([
                "--mcp-config".into(),
                path.to_str().ok_or(ProfileError::Io)?.into(),
            ]);
        }
        "grok" => {
            environment.set("GROK_HOME", &home);
        }
        "droid" => {
            runtime_args.push("--disable-builtin-skills".into());
        }
        // Existing managed Antigravity GEMINI_HOME/hooks/session state are owned
        // by the lifecycle integration. Never replace them with this directory.
        "antigravity" | "cortex" | "mock" => {}
        _ => unreachable!("provider was checked by capability_evidence"),
    }
    for credential in &auth_model.credentials {
        let credential_provider = credential.provider();
        if credential_provider != provider
            && !(provider == "unsloth" && credential_provider == "opencode")
        {
            return Err(ProfileError::InvalidAuth(
                "credential file belongs to another provider",
            ));
        }
        let (name, value) = credential.material()?;
        if matches!(provider, "opencode" | "unsloth") {
            directory.write_private_in("opencode", name, value.to_string().as_bytes())?;
        } else {
            directory.write_private(name, value.to_string().as_bytes())?;
        }
    }
    Ok(ManagedProviderProfile {
        directory,
        environment,
        runtime_args,
        session_meta,
        model: auth_model.model.clone(),
        capability,
        provider: provider.into(),
    })
}

/// Version/platform/source baselines are evidence, not minimum supported runtime
/// versions. New verified runtimes require effective native inventory tests.
/// # Errors
/// Unknown provider IDs have no implicit ambient fallback.
pub fn capability_evidence(provider: &str) -> ProfileResult<CapabilityEvidence> {
    let (source_baseline, controls, missing): (_, &'static [&'static str], &'static [&'static str]) = match provider {
        "claude-code" => ("claude-agent-acp 0.81.1 / SDK 0.3.280", &["strict MCP configuration", "empty native settings sources", "disable slash commands"], &["verify installed CLI connector/plugin/managed-source suppression and effective skills; exclusive managed MCP requires a compatible adapter"]),
        "codex" => ("codex-acp 2.1.0 / CLI 0.160.0 Linux", &["private CODEX_HOME", "typed auth/model projection", "authoritative native subagent denial"], &["native project/admin/plugin MCP and skill source shutoff is missing; require a verified runtime source-filter control; ephemeral also needs bundled-skill suppression"]),
        "pi" => ("pi-acp 0.0.34 / pi source 0.81.0", &["no-skills", "no-extensions", "no-prompt-templates", "private agent directory"], &["verify installed Pi resource/package loaders and reload inventory; interactive MCP delivery requires the Intent gateway extension"]),
        "opencode" | "unsloth" => ("opencode 1.18.18 Linux", &["project config disabled", "external compatibility skills disabled", "owned inline MCP configuration"], &["managed/account/home/plugin loaders need a verified source-filter control; preserve custom model and auth storage before activation"]),
        "auggie" => ("auggie 0.35.0 source", &["owned MCP override file"], &["tenant registry/plugin/skill discovery needs a provider-supported shutoff; --mcp-config alone is not exclusive"]),
        "grok" => ("Grok documentation; runtime unavailable", &["private GROK_HOME"], &["project/admin/compatibility MCP and skill loaders need a verified native source control and enterprise acquisition adapter"]),
        "droid" => ("Droid documentation; runtime unavailable", &["disable bundled skills"], &["user/project/org MCP and skill watchers need a complete discovery shutoff and org policy acquisition"]),
        "antigravity" => ("managed macOS arm64 bundle 1.1.1", &["existing managed profile remains unchanged"], &["verify workspace skill sources and ephemeral zero inventory on the supported macOS bundle"]),
        "cortex" => ("hidden registered provider", &[], &["no verified managed native-source contract; add an explicit profile adapter before activation"]),
        "mock" => ("Intent test provider", &[], &["mock fixture is not runtime isolation evidence"]),
        _ => return Err(ProfileError::UnsupportedIsolation { provider: "unknown provider".into(), missing: "register an explicit profile strategy" }),
    };
    Ok(CapabilityEvidence {
        source_baseline,
        controls,
        missing,
    })
}

#[cfg(test)]
mod tests;
