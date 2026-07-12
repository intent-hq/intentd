//! `ProviderConfig` registry — provider quirks are data, not code (§6.9).
//!
//! Static port of `ACP_PROVIDERS` / `ACPProviderConfig`
//! (`src/shared/config/provider-config.ts`). Each entry is data; adding a
//! provider is a config change, not a code change.

/// Configuration for an ACP provider (port of `ACPProviderConfig`).
///
/// UI-only fields from the TS interface (`ipcChannelPrefix`, `iconPath`) are
/// intentionally omitted — they are Electron IPC / renderer concerns and are
/// not part of the §6.9 field list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Unique identifier (e.g., `auggie`, `opencode`).
    pub id: &'static str,
    /// Display name shown in UI (e.g., `Augment Auggie`).
    pub display_name: &'static str,
    /// CLI command to spawn the agent.
    pub command: &'static str,
    /// Default arguments for ACP mode.
    pub base_args: &'static [&'static str],
    /// Flag for model selection (e.g., `--model`). `None` when the provider
    /// passes model config through other mechanisms (env vars, custom args).
    pub model_flag: Option<&'static str>,
    /// Default agent name for the ACP session (e.g., `build` for OpenCode).
    pub default_agent: Option<&'static str>,
    /// Whether the provider implements the ACP `authenticate` method.
    pub supports_authenticate: bool,
    /// Whether the provider supports `session/set_mode`.
    pub supports_set_mode: bool,
    /// Whether the provider supports MCP server configuration via CLI args.
    pub supports_mcp_config: bool,
    /// Whether the provider supports rules files via CLI args.
    pub supports_rules_file: bool,
    /// Flag for the rules file (e.g., `--rules`).
    pub rules_flag: Option<&'static str>,
    /// Flag for the MCP config file (e.g., `--mcp-config`).
    pub mcp_config_flag: Option<&'static str>,
    /// Flag for quiet mode (e.g., `--quiet`).
    pub quiet_flag: Option<&'static str>,
    /// Flag for removing a provider-native tool at spawn time (e.g.
    /// `--remove-tool`). Repeated once per tool name. `None` when the provider
    /// exposes no equivalent knob — spawn-time tool restrictions are dropped
    /// for that provider (MCP-side filtering, §6.8, still applies).
    pub remove_tool_flag: Option<&'static str>,
    /// Optional provider-specific mode-map overrides (`logical -> provider`).
    pub mode_map: Option<&'static [(&'static str, &'static str)]>,
    /// Optional filter of available models for this provider.
    pub supported_models: Option<&'static [&'static str]>,
    /// Whether this provider is the default/primary provider.
    pub is_default: bool,
    /// Whether this provider can be disabled in settings.
    pub can_be_disabled: bool,
    /// Authentication error patterns used to detect auth failures.
    pub auth_error_patterns: Option<&'static [&'static str]>,
    /// Login command hint surfaced on authentication errors.
    pub login_command_hint: Option<&'static str>,
    /// If set, the provider is only visible when this env var is defined.
    pub requires_env_var: Option<&'static str>,
    /// If set, the provider is only visible when this feature code is active.
    pub requires_feature_code: Option<&'static str>,
    /// CLI args to check auth status (exit 0 == authenticated).
    pub auth_check_args: Option<&'static [&'static str]>,
    /// URL to login/auth docs for this provider.
    pub login_docs_url: Option<&'static str>,
}

impl ProviderConfig {
    const fn empty(id: &'static str, display_name: &'static str, command: &'static str) -> Self {
        Self {
            id,
            display_name,
            command,
            base_args: &[],
            model_flag: None,
            default_agent: None,
            supports_authenticate: false,
            supports_set_mode: false,
            supports_mcp_config: false,
            supports_rules_file: false,
            rules_flag: None,
            mcp_config_flag: None,
            quiet_flag: None,
            remove_tool_flag: None,
            mode_map: None,
            supported_models: None,
            is_default: false,
            can_be_disabled: false,
            auth_error_patterns: None,
            login_command_hint: None,
            requires_env_var: None,
            requires_feature_code: None,
            auth_check_args: None,
            login_docs_url: None,
        }
    }
}

/// All available ACP providers, in the same definition order as
/// `ACP_PROVIDERS` (`provider-config.ts`).
pub static ACP_PROVIDERS: &[ProviderConfig] = &[
    // NOTE (auggie modes): auggie advertises `session/set_mode` support but its
    // `availableModes` today are `default` + `ask` — it does NOT offer a
    // `bypassPermissions` (or otherwise-permissive) mode. We keep
    // `supports_set_mode: true` because the method itself is implemented, but
    // deliberately leave `mode_map: None` so `select_preferred_mode` finds no
    // advertised bypass-equivalent and `try_bypass_permissions_mode` skips the
    // call rather than triggering a `-32602` invalid-params error. Under the
    // shipped `AllowAll` policy the local auto-approve path in
    // `ClientRequestHandler` remains authoritative for auggie sessions.
    ProviderConfig {
        base_args: &["--acp", "--allow-indexing"],
        model_flag: Some("--model"),
        supports_authenticate: true,
        supports_set_mode: true,
        supports_mcp_config: true,
        supports_rules_file: true,
        rules_flag: Some("--rules"),
        mcp_config_flag: Some("--mcp-config"),
        quiet_flag: Some("--quiet"),
        remove_tool_flag: Some("--remove-tool"),
        is_default: true,
        auth_error_patterns: Some(&[
            "authentication required",
            "auggie login",
            "please run `auggie login`",
        ]),
        login_command_hint: Some("auggie login"),
        ..ProviderConfig::empty("auggie", "Augment Auggie", "auggie")
    },
    ProviderConfig {
        can_be_disabled: true,
        auth_check_args: Some(&["auth", "status"]),
        login_docs_url: Some(
            "https://code.claude.com/docs/en/quickstart#step-2-log-in-to-your-account",
        ),
        ..ProviderConfig::empty("claude-code", "Anthropic Claude Code", "claude-agent-acp")
    },
    ProviderConfig {
        can_be_disabled: true,
        auth_check_args: Some(&["login", "status"]),
        login_docs_url: Some("https://developers.openai.com/codex/cli#cli-setup"),
        ..ProviderConfig::empty("codex", "OpenAI Codex", "codex-acp")
    },
    ProviderConfig {
        can_be_disabled: true,
        requires_feature_code: Some("cortex"),
        ..ProviderConfig::empty("cortex", "Snowflake Cortex", "cortex-acp")
    },
    ProviderConfig {
        base_args: &["acp"],
        can_be_disabled: true,
        auth_check_args: Some(&["models"]),
        login_docs_url: Some("https://opencode.ai/docs#configure"),
        ..ProviderConfig::empty("opencode", "OpenCode", "opencode")
    },
    ProviderConfig {
        base_args: &["exec", "--output-format", "acp"],
        model_flag: Some("--model"),
        can_be_disabled: true,
        login_docs_url: Some("https://docs.factory.ai/cli/getting-started/overview"),
        ..ProviderConfig::empty("droid", "Factory Droid", "droid")
    },
    ProviderConfig {
        supports_authenticate: true,
        can_be_disabled: true,
        requires_env_var: Some("MOCK_AGENT_SCRIPT_PATH"),
        ..ProviderConfig::empty("mock", "Mock (E2E)", "node")
    },
];

/// Find a provider by id, or `None` if unknown.
pub fn find_provider(provider_id: &str) -> Option<&'static ProviderConfig> {
    ACP_PROVIDERS.iter().find(|p| p.id == provider_id)
}

/// The default/primary provider (the entry flagged `is_default`, else the
/// first registered provider). Port of `getDefaultProviderConfig`.
pub fn default_provider_config() -> &'static ProviderConfig {
    ACP_PROVIDERS
        .iter()
        .find(|p| p.is_default)
        .or_else(|| ACP_PROVIDERS.first())
        .expect("at least one ACP provider must be configured")
}

/// The default provider id. Port of `getDefaultProviderId`.
pub fn default_provider_id() -> &'static str {
    default_provider_config().id
}

/// Resolve a provider by id, falling back to the default when unknown.
/// Port of `getProviderConfig`.
pub fn provider_config(provider_id: &str) -> &'static ProviderConfig {
    find_provider(provider_id).unwrap_or_else(default_provider_config)
}

/// All registered provider ids, in definition order. Port of `getAllProviderIds`.
pub fn all_provider_ids() -> Vec<&'static str> {
    ACP_PROVIDERS.iter().map(|p| p.id).collect()
}

/// Providers that can be disabled in settings. Port of `getDisableableProviders`.
pub fn disableable_providers() -> Vec<&'static ProviderConfig> {
    ACP_PROVIDERS.iter().filter(|p| p.can_be_disabled).collect()
}

/// Providers that are always enabled. Port of `getAlwaysEnabledProviders`.
pub fn always_enabled_providers() -> Vec<&'static ProviderConfig> {
    ACP_PROVIDERS
        .iter()
        .filter(|p| !p.can_be_disabled)
        .collect()
}

/// Build the user-facing authentication-required message for a provider,
/// including the login command hint (`login_command_hint`, else
/// `{command} login`). Port of `getProviderAuthErrorMessage`.
pub fn auth_error_message(provider_id: &str, is_remote: bool) -> String {
    let config = provider_config(provider_id);
    let login_cmd = config
        .login_command_hint
        .map(|h| h.to_string())
        .unwrap_or_else(|| format!("{} login", config.command));

    if is_remote {
        format!(
            "{} needs to be authenticated on the remote server. Run \"{}\" in a terminal connected to the remote environment.",
            config.display_name, login_cmd
        )
    } else {
        format!(
            "{} needs to be authenticated. Run \"{}\" in a terminal.",
            config.display_name, login_cmd
        )
    }
}

/// Whether an error message indicates the provider needs authentication,
/// using the provider's configured `auth_error_patterns` (case-insensitive).
/// Port of `isProviderAuthenticationError`.
pub fn is_provider_authentication_error(provider_id: &str, error_message: &str) -> bool {
    let config = provider_config(provider_id);
    let Some(patterns) = config.auth_error_patterns else {
        return false;
    };
    let lower = error_message.to_lowercase();
    patterns
        .iter()
        .any(|pattern| lower.contains(&pattern.to_lowercase()))
}
