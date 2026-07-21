//! Typed, strictly-parsed schema for `config.toml` — the daemon's non-secret
//! settings file (`<data_dir>/config.toml`, `INTENTD_CONFIG` override).
//!
//! The struct tree mirrors the dotted `settings.*` wire paths as nested TOML
//! tables with camelCase keys (`server.wsApi.enabled` ↔ `[server.wsApi]
//! enabled = …`). Every table carries `deny_unknown_fields`, so an unknown
//! key, a wrong type, an out-of-range number, or a bad enum value fails the
//! parse with an error naming the offending key precisely. This crate only
//! **returns** those errors — the composition root decides what a startup
//! parse failure means for the daemon.
//!
//! Deliberately excluded from this schema:
//! - **Secrets** (`mcp.servers`, `server.auth.token`, `sourceControl.github.
//!   token`, `linear.token`, `accounts.sentry.token`, `ai.apiToken`) — they
//!   live in `secrets.json` ([`crate::FileSecretStore`]) and must never
//!   appear in `config.toml`.
//! - **Machine-state blobs** (`workspace.changeHistory`,
//!   `workspaceInitializer.state`, `repos.known`, `endUserRules`,
//!   `permissions.rules`, `userRules`, `workspaceRules`) — high-churn state
//!   that stays SQLite-backed.
//!
//! When the file is absent, [`SettingsFile::load_or_init`] writes a
//! fully-commented default file (every key with its default value plus its
//! catalog label/description) and returns [`SettingsFile::default`].

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{DEFAULT_IDLE_REAP_MINUTES, DEFAULT_STREAM_RETENTION_HOURS};
use crate::error::{Error, Result};

/// Root of the `config.toml` schema. One field per top-level TOML table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SettingsFile {
    pub providers: ProvidersSettings,
    pub model: ModelSettings,
    pub background_agents: BackgroundAgentsSettings,
    pub specialists: SpecialistsSettings,
    pub workspace: WorkspaceSettings,
    pub git: GitSettings,
    pub mcp: McpSettings,
    pub notifications: NotificationsSettings,
    pub rtk: RtkSettings,
    pub server: ServerSettings,
    pub source_control: SourceControlSettings,
    pub accounts: AccountsSettings,
    pub ai: AiSettings,
    pub context: ContextSettings,
    pub storage: StorageSettings,
    pub workspaces: WorkspacesSettings,
    pub logging: LoggingSettings,
    pub agents: AgentsSettings,
    pub events: EventsSettings,
}

/// `[providers]` — agent-provider selection (`providers.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ProvidersSettings {
    /// `providers.active` — default agent provider.
    pub active: Option<String>,
    /// `providers.enabled` — providers offered to users (id → enabled).
    pub enabled: Option<BTreeMap<String, bool>>,
    /// `providers.paths` — per-provider CLI path overrides.
    pub paths: BTreeMap<String, String>,
}

/// `[model]` — model defaults (`model.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ModelSettings {
    /// `model.default` — fallback model for new agents.
    pub default: Option<String>,
    /// `model.providerDefaults` — default model per provider.
    pub provider_defaults: BTreeMap<String, String>,
    /// `model.workspaceOverrides` — per-workspace model overrides.
    pub workspace_overrides: BTreeMap<String, String>,
}

/// `[backgroundAgents]` — background-agent model config (`backgroundAgents.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct BackgroundAgentsSettings {
    /// `backgroundAgents.defaultModel` — model for background agents.
    pub default_model: Option<String>,
    /// `backgroundAgents.typeOverrides` — per-agent-type model overrides.
    pub type_overrides: BTreeMap<String, String>,
    /// `backgroundAgents.providerSettings` — per-provider background settings
    /// (opaque FE-owned bags; validated structurally as a table only).
    pub provider_settings: toml::Table,
}

/// `[specialists]` — specialist selection (`specialists.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SpecialistsSettings {
    /// `specialists.default` — specialist applied when none is chosen.
    pub default: Option<String>,
}

/// `[workspace]` — workspace/git-adjacent knobs (`workspace.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceSettings {
    /// `workspace.branchPrefix` — prefix for agent-created branches.
    pub branch_prefix: Option<String>,
    /// `workspace.worktreesLocation` — directory for created worktrees.
    pub worktrees_location: Option<String>,
    /// `workspace.sshKeyPath` — path to the SSH key used for git.
    pub ssh_key_path: Option<String>,
    /// `workspace.defaultShell` — shell used for terminals/scripts.
    pub default_shell: Option<String>,
    /// `workspace.autoFetch` — periodically fetch from the remote.
    pub auto_fetch: bool,
    /// `workspace.cowIsolation` — CoW agent sandboxing for direct-mode
    /// delegations (requires CoW filesystem support).
    pub cow_isolation: bool,
}

/// `[git]` — git behavior (`git.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct GitSettings {
    /// `git.autoCommit` — allow agents to commit without explicit user request.
    pub auto_commit: bool,
}

impl Default for GitSettings {
    fn default() -> Self {
        Self { auto_commit: true }
    }
}

/// `[mcp]` — MCP server lifecycle knobs (`mcp.*`). The server catalog itself
/// (`mcp.servers`) is a secret and lives in `secrets.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct McpSettings {
    /// `mcp.enableUserServers` — start user-scoped MCP servers.
    pub enable_user_servers: bool,
    /// `mcp.disabledServers` — server ids that stay stopped.
    pub disabled_servers: Vec<String>,
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            enable_user_servers: true,
            disabled_servers: Vec::new(),
        }
    }
}

/// `[notifications]` — user notification prefs (`notifications.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct NotificationsSettings {
    /// `notifications.enabled` — whether app notifications are enabled.
    pub enabled: bool,
    /// `notifications.soundEnabled` — whether notification sounds are enabled.
    pub sound_enabled: bool,
    /// `notifications.soundOnlyWhenUnfocused` — only play sounds when the app
    /// is unfocused.
    pub sound_only_when_unfocused: bool,
    /// `notifications.volume` — notification sound volume from 0 to 1.
    #[serde(deserialize_with = "de_lenient_f64")]
    pub volume: f64,
}

impl Default for NotificationsSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            sound_enabled: true,
            sound_only_when_unfocused: true,
            volume: 0.5,
        }
    }
}

/// `[rtk]` — RTK compressed-CLI-output mode (`rtk.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct RtkSettings {
    /// `rtk.enabled` — enable RTK compressed CLI output mode in agent prompts.
    pub enabled: bool,
}

/// `[server]` — transport/listener config (`server.*`). The bearer token
/// (`server.auth.token`) is a secret and lives in `secrets.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ServerSettings {
    /// `server.listenMode` — transport(s) the daemon serves.
    pub listen_mode: ListenMode,
    /// `server.socketPath` — Unix socket path for the UDS listener.
    pub socket_path: Option<String>,
    /// `server.bindAddress` — address the TCP listener binds.
    pub bind_address: String,
    /// `server.port` — TCP port for the WSS listener (1024–65535).
    pub port: u16,
    /// `server.originAllowList` — permitted WS origins.
    pub origin_allow_list: Option<Vec<String>>,
    /// `[server.wsApi]` — WSS API listener runtime toggle.
    pub ws_api: WsApiSettings,
    /// `[server.tls]` — TLS for the TCP listener.
    pub tls: TlsSettings,
    /// `[server.auth]` — bearer-token auth on TCP.
    pub auth: AuthSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            listen_mode: ListenMode::Uds,
            socket_path: None,
            bind_address: "0.0.0.0".to_string(),
            port: 5181,
            origin_allow_list: None,
            ws_api: WsApiSettings::default(),
            tls: TlsSettings::default(),
            auth: AuthSettings::default(),
        }
    }
}

/// `server.listenMode` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenMode {
    #[default]
    Uds,
    Tcp,
    Both,
}

/// `[server.wsApi]` — WSS listener runtime toggle (`server.wsApi.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WsApiSettings {
    /// `server.wsApi.enabled` — enable the TCP/WSS listener at runtime.
    pub enabled: bool,
    /// `server.wsApi.port` — TCP port for the WSS listener (1024–65535).
    pub port: u16,
}

impl Default for WsApiSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 5181,
        }
    }
}

/// `[server.tls]` — TLS toggle (`server.tls.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsSettings {
    /// `server.tls.enabled` — enable TLS for the TCP listener.
    pub enabled: bool,
}

/// `[server.auth]` — bearer auth toggle (`server.auth.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthSettings {
    /// `server.auth.enabled` — require a bearer token on TCP.
    pub enabled: bool,
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// `[sourceControl]` — forge integration (`sourceControl.*`). The GitHub PAT
/// (`sourceControl.github.token`) is a secret and lives in `secrets.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceControlSettings {
    /// `sourceControl.activeProvider` — active forge implementation.
    pub active_provider: SourceControlProvider,
    /// `[sourceControl.github]` — GitHub client config.
    pub github: GithubSettings,
}

/// `sourceControl.activeProvider` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceControlProvider {
    #[default]
    Github,
}

/// Default `sourceControl.github.oauthClientId`: the intent-hq OAuth App
/// registered for the device flow. OAuth device-flow client ids are public by
/// design (no client secret exists or is used), so baking it in is safe.
pub const DEFAULT_GITHUB_OAUTH_CLIENT_ID: &str = "Ov23li8bvmPsd4B4pW38";

/// `[sourceControl.github]` — GitHub client config (`sourceControl.github.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct GithubSettings {
    /// `sourceControl.github.tokenSource` — where the GitHub token comes from.
    pub token_source: GithubTokenSource,
    /// `sourceControl.github.apiBaseUrl` — GitHub (Enterprise) API base.
    pub api_base_url: String,
    /// `sourceControl.github.oauthClientId` — OAuth App client id for the
    /// device flow (public, not a secret).
    pub oauth_client_id: String,
}

impl Default for GithubSettings {
    fn default() -> Self {
        Self {
            token_source: GithubTokenSource::GhCli,
            api_base_url: "https://api.github.com".to_string(),
            oauth_client_id: DEFAULT_GITHUB_OAUTH_CLIENT_ID.to_string(),
        }
    }
}

/// `sourceControl.github.tokenSource` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GithubTokenSource {
    Env,
    #[default]
    GhCli,
    Explicit,
}

/// `[accounts]` — external account config (`accounts.*`). The Sentry API
/// token (`accounts.sentry.token`) is a secret and lives in `secrets.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountsSettings {
    /// `[accounts.sentry]` — Sentry account config.
    pub sentry: SentrySettings,
}

/// `[accounts.sentry]` — Sentry account config (`accounts.sentry.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SentrySettings {
    /// `accounts.sentry.organization` — Sentry organization slug (non-secret
    /// companion of `accounts.sentry.token`).
    pub organization: Option<String>,
}

/// `[ai]` — primary AI provider config (`ai.*`). The bearer token
/// (`ai.apiToken`) is a secret and lives in `secrets.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AiSettings {
    /// `ai.apiUrl` — base URL for the primary AI provider.
    pub api_url: Option<String>,
    /// `ai.model` — default AI model.
    pub model: Option<String>,
    /// `ai.temperature` — sampling temperature (0–2).
    #[serde(deserialize_with = "de_lenient_f64")]
    pub temperature: f64,
    /// `ai.maxTokens` — maximum tokens per completion (>= 1).
    pub max_tokens: u32,
    /// `ai.streamingSpeed` — streaming pacing hint (tokens per second;
    /// 0 = no throttle).
    #[serde(deserialize_with = "de_lenient_f64")]
    pub streaming_speed: f64,
}

impl Default for AiSettings {
    fn default() -> Self {
        Self {
            api_url: None,
            model: None,
            temperature: 0.7,
            max_tokens: 4096,
            streaming_speed: 0.0,
        }
    }
}

/// `[context]` — context engine (`context.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextSettings {
    /// `context.enabled` — enable the auggie context engine.
    pub enabled: bool,
    /// `context.auggiePath` — path to the auggie binary.
    pub auggie_path: Option<String>,
    /// `context.allowIndexing` — permit codebase indexing.
    pub allow_indexing: bool,
}

impl Default for ContextSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            auggie_path: None,
            allow_indexing: true,
        }
    }
}

/// `[storage]` — storage locations (`storage.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct StorageSettings {
    /// `storage.dataDir` — daemon data directory.
    pub data_dir: Option<String>,
}

/// `[workspaces]` — workspace roots (`workspaces.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspacesSettings {
    /// `workspaces.root` — root directory for workspaces.
    pub root: Option<String>,
}

/// `[logging]` — daemon logging (`logging.*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct LoggingSettings {
    /// `logging.level` — daemon log verbosity.
    pub level: LogLevel,
}

/// `logging.level` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

/// `[agents]` — agent lifecycle knobs (`agents.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AgentsSettings {
    /// `agents.maxConcurrent` — concurrent agent session cap (0 = auto based
    /// on system RAM; changes apply on daemon restart; max 200).
    pub max_concurrent: u32,
    /// `agents.idleReapMinutes` — minutes before an idle agent is reaped
    /// (0 disables idle reaping).
    pub idle_reap_minutes: u32,
}

impl Default for AgentsSettings {
    fn default() -> Self {
        Self {
            max_concurrent: 0,
            idle_reap_minutes: DEFAULT_IDLE_REAP_MINUTES,
        }
    }
}

/// `[events]` — event retention (`events.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct EventsSettings {
    /// `events.streamRetentionHours` — hours ephemeral events are retained
    /// before the retention/compaction sweep deletes them (0 disables).
    pub stream_retention_hours: u32,
}

impl Default for EventsSettings {
    fn default() -> Self {
        Self {
            stream_retention_hours: DEFAULT_STREAM_RETENTION_HOURS,
        }
    }
}

/// Accept both TOML integers and floats for `f64` fields, so `volume = 1`
/// parses the same as `volume = 1.0` (users hand-edit this file).
fn de_lenient_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = f64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number")
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<f64, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<f64, E> {
            Ok(v as f64)
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<f64, E> {
            Ok(v as f64)
        }
    }
    deserializer.deserialize_any(V)
}

impl SettingsFile {
    /// Parse `text` as a strict `config.toml`. Unknown keys, wrong types, and
    /// bad enum values are rejected; the error message names the offending key
    /// path (camelCase, dotted) plus the TOML line/column context.
    pub fn parse_str(text: &str) -> Result<Self> {
        let de = toml::de::Deserializer::new(text);
        let file: SettingsFile = serde_path_to_error::deserialize(de).map_err(|e| {
            let key_path = e.path().to_string();
            let detail = e.into_inner().to_string();
            let detail = detail.trim_end();
            if key_path.is_empty() || key_path == "." {
                Error::InvalidInput(format!("invalid config.toml: {detail}"))
            } else {
                Error::InvalidInput(format!("invalid config.toml at `{key_path}`: {detail}"))
            }
        })?;
        file.validate()?;
        Ok(file)
    }

    /// Range/semantic checks the type system cannot express. Errors name the
    /// offending key with its dotted camelCase path.
    pub fn validate(&self) -> Result<()> {
        fn bad(key: &str, msg: String) -> Error {
            Error::InvalidInput(format!("invalid config.toml at `{key}`: {msg}"))
        }
        let v = self.notifications.volume;
        if !(0.0..=1.0).contains(&v) {
            return Err(bad(
                "notifications.volume",
                format!("must be between 0 and 1, got {v}"),
            ));
        }
        if self.server.port < 1024 {
            return Err(bad(
                "server.port",
                format!("must be between 1024 and 65535, got {}", self.server.port),
            ));
        }
        if self.server.ws_api.port < 1024 {
            return Err(bad(
                "server.wsApi.port",
                format!(
                    "must be between 1024 and 65535, got {}",
                    self.server.ws_api.port
                ),
            ));
        }
        let t = self.ai.temperature;
        if !(0.0..=2.0).contains(&t) {
            return Err(bad(
                "ai.temperature",
                format!("must be between 0 and 2, got {t}"),
            ));
        }
        if self.ai.max_tokens < 1 {
            return Err(bad("ai.maxTokens", "must be at least 1".to_string()));
        }
        let s = self.ai.streaming_speed;
        if !s.is_finite() || s < 0.0 {
            return Err(bad(
                "ai.streamingSpeed",
                format!("must be a non-negative number, got {s}"),
            ));
        }
        if self.agents.max_concurrent > 200 {
            return Err(bad(
                "agents.maxConcurrent",
                format!(
                    "must be between 0 and 200, got {}",
                    self.agents.max_concurrent
                ),
            ));
        }
        Ok(())
    }

    /// Load `config.toml` from `path`. When the file does not exist, write
    /// [`DEFAULT_CONFIG_TEMPLATE`] (creating parent directories) and return the
    /// defaults. When it exists, parse it strictly — a malformed file is an
    /// error, never silently ignored.
    pub fn load_or_init(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse_str(&text).map_err(|e| match e {
                Error::InvalidInput(msg) => {
                    Error::InvalidInput(format!("{}: {msg}", path.display()))
                }
                other => other,
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        Error::Internal(format!(
                            "could not create config directory {}: {e}",
                            parent.display()
                        ))
                    })?;
                }
                std::fs::write(path, DEFAULT_CONFIG_TEMPLATE).map_err(|e| {
                    Error::Internal(format!(
                        "could not write default config {}: {e}",
                        path.display()
                    ))
                })?;
                Ok(Self::default())
            }
            Err(err) => Err(Error::Internal(format!(
                "could not read config {}: {err}",
                path.display()
            ))),
        }
    }
}

/// The fully-commented default `config.toml` written by
/// [`SettingsFile::load_or_init`] when no file exists. Every key appears with
/// its default value (or a commented-out example when there is no default),
/// annotated with its catalog label and description. Parsing this template
/// must yield exactly [`SettingsFile::default`] (enforced by a unit test).
pub const DEFAULT_CONFIG_TEMPLATE: &str = r##"# intentd configuration (non-secret settings).
#
# Strictly parsed: unknown keys, wrong types, and out-of-range values are
# startup errors. Secrets (API tokens, MCP server configs) never live here --
# they belong in secrets.json next to this file.

[providers]
# Active provider -- default agent provider.
# active = "claude-code"
# Enabled providers -- providers offered to users (id -> enabled).
# enabled = { claude-code = true }
# Provider paths -- per-provider CLI path overrides.
paths = {}

[model]
# Default model -- fallback model for new agents.
# default = "claude-sonnet-4-5"
# Provider default models -- default model per provider.
providerDefaults = {}
# Workspace model overrides -- per-workspace model overrides.
workspaceOverrides = {}

[backgroundAgents]
# Background default model -- model for background agents.
# defaultModel = "claude-sonnet-4-5"
# Background type overrides -- per-agent-type model overrides.
typeOverrides = {}
# Background provider settings -- per-provider background settings.
providerSettings = {}

[specialists]
# Default specialist -- specialist applied when none is chosen.
# default = "implementor"

[workspace]
# Branch prefix -- prefix for agent-created branches.
# branchPrefix = "agent/"
# Worktrees location -- directory for created worktrees.
# worktreesLocation = "/path/to/worktrees"
# SSH key path -- path to the SSH key used for git.
# sshKeyPath = "~/.ssh/id_ed25519"
# Default shell -- shell used for terminals/scripts.
# defaultShell = "/bin/zsh"
# Auto-fetch -- periodically fetch from the remote.
autoFetch = false
# Copy-on-Write Agent Isolation -- enable CoW agent sandboxing for direct-mode
# delegations (requires CoW filesystem support).
cowIsolation = false

[git]
# Auto-commit -- allow agents to commit without explicit user request.
autoCommit = true

[mcp]
# Enable user MCP servers -- start user-scoped MCP servers.
enableUserServers = true
# Disabled MCP servers -- server ids that stay stopped.
disabledServers = []

[notifications]
# Notifications enabled -- whether app notifications are enabled.
enabled = true
# Notification sounds -- whether notification sounds are enabled.
soundEnabled = true
# Sound only when unfocused -- only play notification sounds when the app is
# unfocused.
soundOnlyWhenUnfocused = true
# Notification volume -- notification sound volume from 0 to 1.
volume = 0.5

[rtk]
# RTK enabled -- enable RTK compressed CLI output mode in agent prompts.
enabled = false

[server]
# Listen mode -- transport(s) the daemon serves: "uds", "tcp", or "both".
listenMode = "uds"
# Socket path -- Unix socket path for the UDS listener.
# socketPath = "/path/to/intentd.sock"
# Bind address -- address the TCP listener binds.
bindAddress = "0.0.0.0"
# WS port -- TCP port for the WSS listener (1024-65535).
port = 5181
# Origin allow-list -- permitted WS origins.
# originAllowList = ["https://example.com"]

[server.wsApi]
# WS API enabled -- enable the TCP/WSS listener at runtime.
enabled = false
# WSS API port -- TCP port for the WSS listener (1024-65535).
port = 5181

[server.tls]
# TLS enabled -- enable TLS for the TCP listener.
enabled = false

[server.auth]
# Auth enabled -- require a bearer token on TCP. The bearer token itself is a
# secret and lives in secrets.json.
enabled = true

[sourceControl]
# Source-control provider -- active forge implementation: "github".
activeProvider = "github"

[sourceControl.github]
# GitHub token source -- where the GitHub token comes from: "env", "gh-cli",
# or "explicit".
tokenSource = "gh-cli"
# GitHub API base URL -- GitHub (Enterprise) API base.
apiBaseUrl = "https://api.github.com"
# GitHub OAuth client ID -- OAuth App client id for the device flow (public,
# not a secret).
oauthClientId = "Ov23li8bvmPsd4B4pW38"

[accounts.sentry]
# Sentry organization -- Sentry organization slug (non-secret companion of the
# accounts.sentry.token secret).
# organization = "my-org"

[ai]
# AI provider API URL -- base URL for the primary AI provider.
# apiUrl = "https://api.example.com"
# AI model -- default AI model.
# model = "claude-sonnet-4-5"
# AI temperature -- sampling temperature for the primary AI provider (0-2).
temperature = 0.7
# AI max tokens -- maximum tokens per completion for the primary AI provider.
maxTokens = 4096
# AI streaming speed -- streaming pacing hint (tokens per second; 0 = no
# throttle).
streamingSpeed = 0.0

[context]
# Context engine -- enable the auggie context engine.
enabled = true
# auggie path -- path to the auggie binary.
# auggiePath = "/usr/local/bin/auggie"
# Allow indexing -- permit codebase indexing.
allowIndexing = true

[storage]
# Data directory -- daemon data directory.
# dataDir = "/path/to/data"

[workspaces]
# Workspaces root -- root directory for workspaces.
# root = "/path/to/workspaces"

[logging]
# Log level -- daemon log verbosity: "error", "warn", "info", "debug", or
# "trace".
level = "info"

[agents]
# Max concurrent agents -- concurrent agent session cap (0 = auto based on
# system RAM; changes apply on daemon restart; max 200).
maxConcurrent = 0
# Idle reap minutes -- minutes before an idle agent is reaped (0 disables idle
# reaping).
idleReapMinutes = 30

[events]
# Stream retention hours -- hours ephemeral events are retained before the
# retention/compaction sweep deletes them (0 disables).
streamRetentionHours = 72
"##;

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("intentd-sf-{}-{}", name, uuid::Uuid::new_v4()))
    }

    #[test]
    fn empty_file_yields_defaults() {
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert_eq!(parsed, SettingsFile::default());
    }

    #[test]
    fn default_template_parses_to_defaults() {
        let parsed = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert_eq!(parsed, SettingsFile::default());
    }

    #[test]
    fn defaults_match_catalog() {
        let d = SettingsFile::default();
        assert_eq!(d.providers.active, None);
        assert_eq!(d.providers.enabled, None);
        assert!(d.providers.paths.is_empty());
        assert_eq!(d.model.default, None);
        assert!(d.background_agents.provider_settings.is_empty());
        assert!(!d.workspace.auto_fetch);
        assert!(!d.workspace.cow_isolation);
        assert!(d.git.auto_commit);
        assert!(d.mcp.enable_user_servers);
        assert!(d.mcp.disabled_servers.is_empty());
        assert!(d.notifications.enabled);
        assert!(d.notifications.sound_enabled);
        assert!(d.notifications.sound_only_when_unfocused);
        assert_eq!(d.notifications.volume, 0.5);
        assert!(!d.rtk.enabled);
        assert_eq!(d.server.listen_mode, ListenMode::Uds);
        assert_eq!(d.server.bind_address, "0.0.0.0");
        assert_eq!(d.server.port, 5181);
        assert_eq!(d.server.origin_allow_list, None);
        assert!(!d.server.ws_api.enabled);
        assert_eq!(d.server.ws_api.port, 5181);
        assert!(!d.server.tls.enabled);
        assert!(d.server.auth.enabled);
        assert_eq!(
            d.source_control.active_provider,
            SourceControlProvider::Github
        );
        assert_eq!(
            d.source_control.github.token_source,
            GithubTokenSource::GhCli
        );
        assert_eq!(
            d.source_control.github.api_base_url,
            "https://api.github.com"
        );
        assert_eq!(
            d.source_control.github.oauth_client_id,
            DEFAULT_GITHUB_OAUTH_CLIENT_ID
        );
        assert_eq!(d.accounts.sentry.organization, None);
        assert_eq!(d.ai.temperature, 0.7);
        assert_eq!(d.ai.max_tokens, 4096);
        assert_eq!(d.ai.streaming_speed, 0.0);
        assert!(d.context.enabled);
        assert!(d.context.allow_indexing);
        assert_eq!(d.logging.level, LogLevel::Info);
        assert_eq!(d.agents.max_concurrent, 0);
        assert_eq!(d.agents.idle_reap_minutes, DEFAULT_IDLE_REAP_MINUTES);
        assert_eq!(
            d.events.stream_retention_hours,
            DEFAULT_STREAM_RETENTION_HOURS
        );
    }

    #[test]
    fn camel_case_keys_parse() {
        let parsed = SettingsFile::parse_str(
            "[agents]\nidleReapMinutes = 5\nmaxConcurrent = 4\n\n[events]\nstreamRetentionHours = 24\n\n[server.wsApi]\nenabled = true\nport = 2000\n",
        )
        .unwrap();
        assert_eq!(parsed.agents.idle_reap_minutes, 5);
        assert_eq!(parsed.agents.max_concurrent, 4);
        assert_eq!(parsed.events.stream_retention_hours, 24);
        assert!(parsed.server.ws_api.enabled);
        assert_eq!(parsed.server.ws_api.port, 2000);
    }

    #[test]
    fn unknown_key_is_rejected_with_path() {
        let err = SettingsFile::parse_str("[agents]\nidleReapMinuets = 5\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("agents"), "names the table: {msg}");
        assert!(msg.contains("idleReapMinuets"), "names the bad key: {msg}");
    }

    #[test]
    fn unknown_top_level_table_is_rejected() {
        let err = SettingsFile::parse_str("[linear]\ntoken = \"secret\"\n").unwrap_err();
        assert!(err.to_string().contains("linear"), "{err}");
    }

    #[test]
    fn snake_case_key_is_rejected() {
        let err = SettingsFile::parse_str("[agents]\nidle_reap_minutes = 5\n").unwrap_err();
        assert!(err.to_string().contains("idle_reap_minutes"), "{err}");
    }

    #[test]
    fn wrong_type_is_rejected_with_path() {
        let err = SettingsFile::parse_str("[agents]\nidleReapMinutes = \"soon\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("agents.idleReapMinutes"),
            "names the key: {msg}"
        );
    }

    #[test]
    fn bad_enum_value_is_rejected() {
        let err = SettingsFile::parse_str("[server]\nlistenMode = \"quic\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("server.listenMode"), "names the key: {msg}");
        assert!(msg.contains("uds"), "lists the variants: {msg}");
    }

    #[test]
    fn negative_integer_for_u32_is_rejected() {
        let err = SettingsFile::parse_str("[agents]\nidleReapMinutes = -1\n").unwrap_err();
        assert!(err.to_string().contains("agents.idleReapMinutes"), "{err}");
    }

    #[test]
    fn out_of_range_values_are_rejected() {
        for (body, key) in [
            ("[notifications]\nvolume = 1.5\n", "notifications.volume"),
            ("[server]\nport = 80\n", "server.port"),
            ("[server.wsApi]\nport = 80\n", "server.wsApi.port"),
            ("[ai]\ntemperature = 3.0\n", "ai.temperature"),
            ("[ai]\nmaxTokens = 0\n", "ai.maxTokens"),
            ("[ai]\nstreamingSpeed = -1.0\n", "ai.streamingSpeed"),
            ("[agents]\nmaxConcurrent = 500\n", "agents.maxConcurrent"),
        ] {
            let err = SettingsFile::parse_str(body).unwrap_err();
            assert!(
                err.to_string().contains(key),
                "{body:?} should fail naming `{key}`: {err}"
            );
        }
    }

    #[test]
    fn floats_accept_integer_literals() {
        let parsed =
            SettingsFile::parse_str("[notifications]\nvolume = 1\n\n[ai]\ntemperature = 2\n")
                .unwrap();
        assert_eq!(parsed.notifications.volume, 1.0);
        assert_eq!(parsed.ai.temperature, 2.0);
    }

    #[test]
    fn provider_maps_and_lists_parse() {
        let parsed = SettingsFile::parse_str(
            "[providers]\nactive = \"claude-code\"\n\n[providers.enabled]\nclaude-code = true\ncodex = false\n\n[providers.paths]\ncodex = \"/usr/local/bin/codex\"\n\n[mcp]\ndisabledServers = [\"linear\"]\n\n[server]\noriginAllowList = [\"https://app.example.com\"]\n\n[backgroundAgents.providerSettings.claude-code]\nmode = \"fast\"\n",
        )
        .unwrap();
        assert_eq!(parsed.providers.active.as_deref(), Some("claude-code"));
        let enabled = parsed.providers.enabled.unwrap();
        assert_eq!(enabled.get("claude-code"), Some(&true));
        assert_eq!(enabled.get("codex"), Some(&false));
        assert_eq!(
            parsed.providers.paths.get("codex").map(String::as_str),
            Some("/usr/local/bin/codex")
        );
        assert_eq!(parsed.mcp.disabled_servers, vec!["linear".to_string()]);
        assert_eq!(
            parsed.server.origin_allow_list,
            Some(vec!["https://app.example.com".to_string()])
        );
        assert!(parsed
            .background_agents
            .provider_settings
            .contains_key("claude-code"));
    }

    #[test]
    fn load_or_init_writes_template_when_missing() {
        let dir = temp_path("init");
        let path = dir.join("config.toml");
        let loaded = SettingsFile::load_or_init(&path).expect("init succeeds");
        assert_eq!(loaded, SettingsFile::default());
        let written = std::fs::read_to_string(&path).expect("file was created");
        assert_eq!(written, DEFAULT_CONFIG_TEMPLATE);
        // Second load reads the file it just wrote.
        let reloaded = SettingsFile::load_or_init(&path).expect("reload succeeds");
        assert_eq!(reloaded, SettingsFile::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_or_init_reads_existing_file() {
        let dir = temp_path("read");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "[agents]\nidleReapMinutes = 7\n").unwrap();
        let loaded = SettingsFile::load_or_init(&path).unwrap();
        assert_eq!(loaded.agents.idle_reap_minutes, 7);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_or_init_fails_on_malformed_file_with_path_context() {
        let dir = temp_path("bad");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "[agents]\nbogusKey = 1\n").unwrap();
        let err = SettingsFile::load_or_init(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("config.toml"), "names the file: {msg}");
        assert!(msg.contains("bogusKey"), "names the key: {msg}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trips_through_toml() {
        let mut file = SettingsFile::default();
        file.providers.active = Some("claude-code".to_string());
        file.server.listen_mode = ListenMode::Both;
        file.agents.idle_reap_minutes = 15;
        let text = toml::to_string(&file).expect("serializes");
        let back = SettingsFile::parse_str(&text).expect("re-parses");
        assert_eq!(back, file);
    }
}
