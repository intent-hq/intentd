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
//!   token`, `linear.token`, `accounts.sentry.token`) — they
//!   live in `secrets.json` ([`crate::FileSecretStore`]) and must never
//!   appear in `config.toml`.
//! - **Machine-state blobs** (`workspace.changeHistory`,
//!   `workspaceInitializer.state`, `hardwareConsole.state`, `repos.known`,
//!   `endUserRules`, `permissions.rules`, `userRules`, `workspaceRules`) —
//!   high-churn state that stays SQLite-backed.
//!
//! Keys that older daemons **used to** persist here but that have since moved
//! back to SQLite or been removed outright are listed in
//! [`LEGACY_SETTINGS_PATHS`]. A file containing one of them still parses (the
//! value is captured for a one-time boot import-or-discard-and-strip by the
//! composition root); any other unknown key remains a hard parse error.
//!
//! When the file is absent, [`SettingsFile::load_or_init`] writes a
//! fully-commented default file (every key with its default value plus its
//! catalog label/description) and returns [`SettingsFile::default`].

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{
    DEFAULT_HOOKS_MAX_PER_AGENT, DEFAULT_IDLE_REAP_MINUTES, DEFAULT_STREAM_RETENTION_HOURS,
    DEFAULT_WORKSPACE_API_MAX_OUTPUT_CHARS, DEFAULT_WORKSPACE_API_TOON_OUTPUT,
};
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
    pub voice: VoiceSettings,
    pub context: ContextSettings,
    pub storage: StorageSettings,
    pub workspaces: WorkspacesSettings,
    pub logging: LoggingSettings,
    pub agents: AgentsSettings,
    pub events: EventsSettings,
    pub workspace_api: WorkspaceApiSettings,
    pub hooks: HooksSettings,
    pub agent_features: AgentFeaturesSettings,
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

/// `[model]` — model defaults (`model.*`). The per-workspace override layer
/// (`model.workspaceOverrides`) is retired (monorepo#1000) and only survives
/// as a tolerated legacy key (see [`LEGACY_SETTINGS_PATHS`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ModelSettings {
    /// `model.default` — fallback model for new agents.
    pub default: Option<String>,
    /// `model.providerDefaults` — default model per provider.
    pub provider_defaults: BTreeMap<String, String>,
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
    /// `workspace.cowIsolation` — CoW workspace provisioning and per-agent
    /// sandboxing (requires CoW filesystem support on the workspaces root;
    /// workspace creation fails when unsupported).
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
    /// `sourceControl.github.exposeGitCredentialToChildren` — inject the
    /// daemon-managed GitHub credential into child process environments as a
    /// github.com-scoped credential helper (never raw
    /// `GITHUB_TOKEN`/`GH_TOKEN`).
    pub expose_git_credential_to_children: bool,
}

impl Default for GithubSettings {
    fn default() -> Self {
        Self {
            token_source: GithubTokenSource::Auto,
            api_base_url: "https://api.github.com".to_string(),
            oauth_client_id: DEFAULT_GITHUB_OAUTH_CLIENT_ID.to_string(),
            expose_git_credential_to_children: true,
        }
    }
}

/// `sourceControl.github.tokenSource` values. `auto` (the default) tries the
/// secrets store, then env, then the `gh` CLI — mirroring
/// `intent_sourcecontrol::TokenSource::Auto`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GithubTokenSource {
    #[default]
    Auto,
    Env,
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

/// `[voice]` — speech-to-text (`voice.*`). The provider API keys
/// (`voice.elevenlabs.apiKey`, `voice.openai.apiKey`) are secrets and live in
/// `secrets.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct VoiceSettings {
    /// `voice.provider` — active speech-to-text provider.
    pub provider: VoiceProvider,
    /// `voice.language` — default transcription language hint (ISO-639-1
    /// code, e.g. `"en"`) applied when a `voice.transcribe` call carries no
    /// per-call `language`. Unset/empty → provider auto-detection.
    pub language: Option<String>,
    /// `[voice.openai]` — OpenAI provider tuning.
    pub openai: VoiceOpenAiSettings,
}

/// `voice.provider` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VoiceProvider {
    #[default]
    Elevenlabs,
    Openai,
}

/// `[voice.openai]` — OpenAI speech-to-text tuning (`voice.openai.*`,
/// non-secret; the API key is a secret in `secrets.json`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct VoiceOpenAiSettings {
    /// `voice.openai.model` — transcription model.
    pub model: VoiceOpenAiModel,
}

/// `voice.openai.model` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoiceOpenAiModel {
    #[default]
    #[serde(rename = "gpt-4o-transcribe")]
    Gpt4oTranscribe,
    #[serde(rename = "gpt-4o-mini-transcribe")]
    Gpt4oMiniTranscribe,
    #[serde(rename = "whisper-1")]
    Whisper1,
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
    /// `agents.flushQueuedMessages` — how the whole queued-message backlog
    /// is delivered when an idle agent drains its queue: `all` batches every
    /// ready entry into one turn, `systemOnly` batches only system-origin
    /// entries (user-origin entries stay FIFO), `off` is one turn per queued
    /// message.
    pub flush_queued_messages: FlushQueuedMessagesMode,
}

impl Default for AgentsSettings {
    fn default() -> Self {
        Self {
            max_concurrent: 0,
            idle_reap_minutes: DEFAULT_IDLE_REAP_MINUTES,
            flush_queued_messages: FlushQueuedMessagesMode::All,
        }
    }
}

/// `agents.flushQueuedMessages` values. Serializes as camelCase strings
/// (`"all"`, `"systemOnly"`, `"off"`); deserialization also accepts the
/// legacy boolean shape (`true` → [`FlushQueuedMessagesMode::All`], `false` →
/// [`FlushQueuedMessagesMode::Off`]) so an existing `config.toml` written by
/// an older daemon still loads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FlushQueuedMessagesMode {
    /// Batch every ready-to-send entry into one combined turn.
    #[default]
    All,
    /// Batch only system-origin ready entries; user-origin entries stay FIFO.
    SystemOnly,
    /// One turn per queued message (legacy `false`).
    Off,
}

impl<'de> Deserialize<'de> for FlushQueuedMessagesMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Bool(bool),
            String(String),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Bool(true) => Ok(FlushQueuedMessagesMode::All),
            Repr::Bool(false) => Ok(FlushQueuedMessagesMode::Off),
            Repr::String(s) => match s.as_str() {
                "all" => Ok(FlushQueuedMessagesMode::All),
                "systemOnly" => Ok(FlushQueuedMessagesMode::SystemOnly),
                "off" => Ok(FlushQueuedMessagesMode::Off),
                other => Err(serde::de::Error::custom(format!(
                    "unknown variant `{other}`, expected one of `all`, `systemOnly`, `off`"
                ))),
            },
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

/// `[workspaceApi]` — `workspace_api` tool output knobs (`workspaceApi.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceApiSettings {
    /// `workspaceApi.maxOutputChars` — max characters of one `workspace_api`
    /// tool result before the output is redirected to a file (0 = unlimited;
    /// min 1000 when non-zero, max 10000000).
    pub max_output_chars: u32,
    /// `workspaceApi.toonOutput` — TOON-encode `workspace_api` tool results
    /// (token-efficient) instead of plain JSON.
    pub toon_output: bool,
}

impl Default for WorkspaceApiSettings {
    fn default() -> Self {
        Self {
            max_output_chars: DEFAULT_WORKSPACE_API_MAX_OUTPUT_CHARS,
            toon_output: DEFAULT_WORKSPACE_API_TOON_OUTPUT,
        }
    }
}

/// `[hooks]` — background-hook scheduler knobs (`hooks.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct HooksSettings {
    /// `hooks.maxPerAgent` — cap on concurrently active (scheduled/running)
    /// hooks per agent.
    pub max_per_agent: u32,
}

impl Default for HooksSettings {
    fn default() -> Self {
        Self {
            max_per_agent: DEFAULT_HOOKS_MAX_PER_AGENT,
        }
    }
}

/// `[agentFeatures]` — per-feature toggles for what agents see and may call
/// (`agentFeatures.*`). All default **on**; changes apply to new agent
/// sessions only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct AgentFeaturesSettings {
    /// `agentFeatures.backgroundHooks` — expose background hooks
    /// (`ws.hook.*`) to agents.
    pub background_hooks: bool,
    /// `agentFeatures.hostExec` — expose one-shot host command execution
    /// (`ws.host.exec`) to agents.
    pub host_exec: bool,
    /// `agentFeatures.scripts` — expose saved scripts (`ws.script.*`) to
    /// agents.
    pub scripts: bool,
    /// `agentFeatures.terminalAccess` — expose terminal read access
    /// (`ws.terminal.*`) to agents.
    pub terminal_access: bool,
    /// `agentFeatures.browserAutomation` — expose browser automation
    /// (`ws.browser.*`) to agents.
    pub browser_automation: bool,
    /// `agentFeatures.richChatBlocks` — include rich chat block guidance
    /// (mermaid, ws-block, nav-link) in agent prompts.
    pub rich_chat_blocks: bool,
    /// `agentFeatures.structuredQuestions` — expose structured questions
    /// (`ws.app.question.ask`) to agents.
    pub structured_questions: bool,
    /// `agentFeatures.attentionRequests` — expose attention requests
    /// (`ws.agent.reportBlocker` / `ws.agent.requestDiscussion`) to agents.
    pub attention_requests: bool,
}

impl Default for AgentFeaturesSettings {
    fn default() -> Self {
        Self {
            background_hooks: true,
            host_exec: true,
            scripts: true,
            terminal_access: true,
            browser_automation: true,
            rich_chat_blocks: true,
            structured_questions: true,
            attention_requests: true,
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

/// Dotted wire paths that older daemons persisted in `config.toml` but that
/// have since moved back to the SQLite `settings` table or been removed from
/// the product entirely. A file containing one of these still parses via
/// [`SettingsFile::parse_str_with_legacy`] — the value is captured so the
/// composition root can run a one-time import-into-SQLite (or discard, for
/// keys with no catalog entry) + strip-from-file at boot. Any other unknown
/// key remains a hard parse error. `ai` covers the whole retired `[ai]`
/// table (the app drives AI via ACP agent providers, not a direct provider).
/// `server.listenMode` is retired outright: the daemon always serves UDS and
/// the TCP/WSS listener is governed by `server.wsApi.enabled` — the value is
/// discarded (no catalog entry remains) and stripped from the file.
pub const LEGACY_SETTINGS_PATHS: &[&str] = &["model.workspaceOverrides", "ai", "server.listenMode"];

/// Legacy values captured during a tolerant parse: dotted wire path → the
/// JSON shape of the TOML value found in the file.
pub type LegacySettings = BTreeMap<String, serde_json::Value>;

impl SettingsFile {
    /// Parse `text` as a strict `config.toml`. Unknown keys (including
    /// [`LEGACY_SETTINGS_PATHS`]), wrong types, and bad enum values are
    /// rejected; the error message names the offending key path (camelCase,
    /// dotted) plus the TOML line/column context.
    pub fn parse_str(text: &str) -> Result<Self> {
        let de = toml::de::Deserializer::parse(text).map_err(|e| {
            let detail = e.to_string();
            Error::InvalidInput(format!("invalid config.toml: {}", detail.trim_end()))
        })?;
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

    /// Parse `text` like [`SettingsFile::parse_str`], but tolerate the known
    /// [`LEGACY_SETTINGS_PATHS`]: their values are removed from the document
    /// before the strict parse and returned in the legacy map (dotted wire
    /// path → JSON value) so the caller can import them into SQLite and strip
    /// the file. Every **other** unknown key is still a hard error.
    pub fn parse_str_with_legacy(text: &str) -> Result<(Self, LegacySettings)> {
        let raw: toml::Table = text.parse().map_err(|e: toml::de::Error| {
            Error::InvalidInput(format!("invalid config.toml: {e}"))
        })?;
        let mut legacy = LegacySettings::new();
        let mut pruned = raw;
        for &path in LEGACY_SETTINGS_PATHS {
            if let Some(value) = toml_table_remove(&mut pruned, path) {
                let json = serde_json::to_value(&value).map_err(|e| {
                    Error::InvalidInput(format!(
                        "invalid config.toml at `{path}`: not representable as JSON: {e}"
                    ))
                })?;
                legacy.insert(path.to_string(), json);
            }
        }
        if legacy.is_empty() {
            // Common case: no legacy keys — the plain strict parse keeps the
            // precise TOML line/column error context.
            return Ok((Self::parse_str(text)?, legacy));
        }
        let file: SettingsFile = serde_path_to_error::deserialize(toml::Value::Table(pruned))
            .map_err(|e| {
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
        Ok((file, legacy))
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
        if self.agents.max_concurrent > 200 {
            return Err(bad(
                "agents.maxConcurrent",
                format!(
                    "must be between 0 and 200, got {}",
                    self.agents.max_concurrent
                ),
            ));
        }
        let chars = self.workspace_api.max_output_chars;
        if chars != 0 && !(1_000..=10_000_000).contains(&chars) {
            return Err(bad(
                "workspaceApi.maxOutputChars",
                format!("must be 0 (unlimited) or between 1000 and 10000000, got {chars}"),
            ));
        }
        Ok(())
    }

    /// Load `config.toml` from `path`. When the file does not exist, write
    /// [`DEFAULT_CONFIG_TEMPLATE`] (creating parent directories) and return the
    /// defaults. When it exists, parse it strictly except for the known
    /// [`LEGACY_SETTINGS_PATHS`] (tolerated so a daemon upgrade can boot and
    /// import them; see [`SettingsFile::load_or_init_with_legacy`]) — any
    /// other malformed content is an error, never silently ignored.
    pub fn load_or_init(path: &Path) -> Result<Self> {
        Self::load_or_init_with_legacy(path).map(|(file, _)| file)
    }

    /// Like [`SettingsFile::load_or_init`], but also return the captured
    /// legacy values (dotted wire path → JSON value; empty when the file has
    /// none) so the composition root can run the one-time import-and-strip.
    pub fn load_or_init_with_legacy(path: &Path) -> Result<(Self, LegacySettings)> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse_str_with_legacy(&text).map_err(|e| match e {
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
                Ok((Self::default(), LegacySettings::new()))
            }
            Err(err) => Err(Error::Internal(format!(
                "could not read config {}: {err}",
                path.display()
            ))),
        }
    }
}

/// Remove a dotted path from a parsed TOML table, returning the value when it
/// was present (no-op `None` otherwise). Empties left behind are kept — the
/// comment-preserving file strip is the registry's concern, not this parse.
fn toml_table_remove(table: &mut toml::Table, path: &str) -> Option<toml::Value> {
    let segs: Vec<&str> = path.split('.').collect();
    let (last, parents) = segs.split_last().expect("dotted path is never empty");
    let mut cur = table;
    for seg in parents {
        cur = cur.get_mut(*seg)?.as_table_mut()?;
    }
    cur.remove(*last)
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
# Copy-on-Write Isolation -- CoW workspaces + per-agent sandboxes (requires
# CoW filesystem support on the workspaces root).
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
# GitHub token source -- where the GitHub token comes from: "auto" (secrets
# store, then env, then gh CLI), "env", "gh-cli", or "explicit".
tokenSource = "auto"
# GitHub API base URL -- GitHub (Enterprise) API base.
apiBaseUrl = "https://api.github.com"
# GitHub OAuth client ID -- OAuth App client id for the device flow (public,
# not a secret).
oauthClientId = "Ov23li8bvmPsd4B4pW38"
# Expose Git credential to terminals and agents -- inject the daemon-managed
# GitHub credential into child process environments as a scoped
# github.com-only credential helper (never raw GITHUB_TOKEN/GH_TOKEN).
exposeGitCredentialToChildren = true

[accounts.sentry]
# Sentry organization -- Sentry organization slug (non-secret companion of the
# accounts.sentry.token secret).
# organization = "my-org"

[voice]
# Voice provider -- active speech-to-text provider: "elevenlabs" or "openai".
# The API keys are secrets and live in secrets.json (voice.elevenlabs.apiKey /
# voice.openai.apiKey).
provider = "elevenlabs"
# Voice language -- default transcription language hint (ISO-639-1 code)
# used when a voice.transcribe call has no per-call language. Unset means
# provider auto-detection.
# language = "en"

[voice.openai]
# OpenAI voice model -- transcription model: "gpt-4o-transcribe",
# "gpt-4o-mini-transcribe", or "whisper-1".
model = "gpt-4o-transcribe"

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
# Flush queued messages -- how the queued-message backlog is delivered when
# an idle agent drains its queue: "all", "systemOnly", or "off".
flushQueuedMessages = "all"

[events]
# Stream retention hours -- hours ephemeral events are retained before the
# retention/compaction sweep deletes them (0 disables).
streamRetentionHours = 72

[workspaceApi]
# Max workspace API output chars -- max characters of one workspace_api tool
# result before the output is redirected to a file (0 = unlimited; min 1000
# when non-zero).
maxOutputChars = 100000
# TOON output -- TOON-encode workspace_api tool results (token-efficient)
# instead of plain JSON.
toonOutput = true

[hooks]
# Max hooks per agent -- cap on concurrently active (scheduled/running)
# background hooks per agent.
maxPerAgent = 5

[agentFeatures]
# All toggles default to on; changes apply to new agent sessions only.
# Background hooks -- expose background hooks (ws.hook.*) to agents.
backgroundHooks = true
# Host exec -- expose one-shot host command execution (ws.host.exec) to
# agents.
hostExec = true
# Saved scripts -- expose saved scripts (ws.script.*) to agents.
scripts = true
# Terminal access -- expose terminal read access (ws.terminal.*) to agents.
terminalAccess = true
# Browser automation -- expose browser automation (ws.browser.*) to agents.
browserAutomation = true
# Rich chat blocks -- include rich chat block guidance (mermaid, ws-block,
# nav-link) in agent prompts.
richChatBlocks = true
# Structured questions -- expose structured questions (ws.app.question.ask)
# to agents.
structuredQuestions = true
# Attention requests -- expose attention requests (ws.agent.reportBlocker /
# ws.agent.requestDiscussion) to agents.
attentionRequests = true
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
            GithubTokenSource::Auto
        );
        assert_eq!(
            d.source_control.github.api_base_url,
            "https://api.github.com"
        );
        assert_eq!(
            d.source_control.github.oauth_client_id,
            DEFAULT_GITHUB_OAUTH_CLIENT_ID
        );
        assert!(d.source_control.github.expose_git_credential_to_children);
        assert_eq!(d.accounts.sentry.organization, None);
        assert_eq!(d.voice.provider, VoiceProvider::Elevenlabs);
        assert_eq!(d.voice.language, None);
        assert_eq!(d.voice.openai.model, VoiceOpenAiModel::Gpt4oTranscribe);
        assert!(d.context.enabled);
        assert!(d.context.allow_indexing);
        assert_eq!(d.logging.level, LogLevel::Info);
        assert_eq!(d.agents.max_concurrent, 0);
        assert_eq!(d.agents.idle_reap_minutes, DEFAULT_IDLE_REAP_MINUTES);
        assert_eq!(d.agents.flush_queued_messages, FlushQueuedMessagesMode::All);
        assert_eq!(
            d.events.stream_retention_hours,
            DEFAULT_STREAM_RETENTION_HOURS
        );
        assert_eq!(
            d.workspace_api.max_output_chars,
            DEFAULT_WORKSPACE_API_MAX_OUTPUT_CHARS
        );
        assert_eq!(
            d.workspace_api.toon_output,
            DEFAULT_WORKSPACE_API_TOON_OUTPUT
        );
        assert_eq!(d.hooks.max_per_agent, DEFAULT_HOOKS_MAX_PER_AGENT);
        assert!(d.agent_features.background_hooks);
        assert!(d.agent_features.host_exec);
        assert!(d.agent_features.scripts);
        assert!(d.agent_features.terminal_access);
        assert!(d.agent_features.browser_automation);
        assert!(d.agent_features.rich_chat_blocks);
        assert!(d.agent_features.structured_questions);
        assert!(d.agent_features.attention_requests);
    }

    #[test]
    fn camel_case_keys_parse() {
        let parsed = SettingsFile::parse_str(
            "[agents]\nidleReapMinutes = 5\nmaxConcurrent = 4\nflushQueuedMessages = false\n\n[events]\nstreamRetentionHours = 24\n\n[workspaceApi]\nmaxOutputChars = 5000\ntoonOutput = false\n\n[server.wsApi]\nenabled = true\nport = 2000\n\n[hooks]\nmaxPerAgent = 9\n\n[agentFeatures]\nbackgroundHooks = false\nhostExec = false\nrichChatBlocks = false\n",
        )
        .unwrap();
        assert_eq!(parsed.agents.idle_reap_minutes, 5);
        assert_eq!(parsed.agents.max_concurrent, 4);
        assert_eq!(
            parsed.agents.flush_queued_messages,
            FlushQueuedMessagesMode::Off
        );
        assert_eq!(parsed.events.stream_retention_hours, 24);
        assert_eq!(parsed.workspace_api.max_output_chars, 5000);
        assert!(!parsed.workspace_api.toon_output);
        assert!(parsed.server.ws_api.enabled);
        assert_eq!(parsed.server.ws_api.port, 2000);
        assert_eq!(parsed.hooks.max_per_agent, 9);
        assert!(!parsed.agent_features.background_hooks);
        assert!(!parsed.agent_features.host_exec);
        assert!(!parsed.agent_features.rich_chat_blocks);
        // Keys absent from a partial [agentFeatures] table keep their default.
        assert!(parsed.agent_features.scripts);
        assert!(parsed.agent_features.terminal_access);
        assert!(parsed.agent_features.browser_automation);
        assert!(parsed.agent_features.structured_questions);
        assert!(parsed.agent_features.attention_requests);
    }

    #[test]
    fn agent_features_unknown_key_is_rejected() {
        let err = SettingsFile::parse_str("[agentFeatures]\nhostExek = false\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("agentFeatures"), "names the table: {msg}");
        assert!(msg.contains("hostExek"), "names the bad key: {msg}");
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
        let err = SettingsFile::parse_str("[logging]\nlevel = \"loud\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("logging.level"), "names the key: {msg}");
        assert!(msg.contains("info"), "lists the variants: {msg}");
    }

    #[test]
    fn flush_queued_messages_accepts_string_variants() {
        for (raw, expected) in [
            ("\"all\"", FlushQueuedMessagesMode::All),
            ("\"systemOnly\"", FlushQueuedMessagesMode::SystemOnly),
            ("\"off\"", FlushQueuedMessagesMode::Off),
        ] {
            let parsed =
                SettingsFile::parse_str(&format!("[agents]\nflushQueuedMessages = {raw}\n"))
                    .expect("parses");
            assert_eq!(parsed.agents.flush_queued_messages, expected, "{raw}");
        }
    }

    #[test]
    fn flush_queued_messages_accepts_legacy_booleans() {
        let parsed = SettingsFile::parse_str("[agents]\nflushQueuedMessages = true\n")
            .expect("legacy true parses");
        assert_eq!(
            parsed.agents.flush_queued_messages,
            FlushQueuedMessagesMode::All
        );
        let parsed = SettingsFile::parse_str("[agents]\nflushQueuedMessages = false\n")
            .expect("legacy false parses");
        assert_eq!(
            parsed.agents.flush_queued_messages,
            FlushQueuedMessagesMode::Off
        );
    }

    #[test]
    fn flush_queued_messages_rejects_unknown_string() {
        let err =
            SettingsFile::parse_str("[agents]\nflushQueuedMessages = \"sometimes\"\n").unwrap_err();
        assert!(err.to_string().contains("flushQueuedMessages"), "{err}");
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
            ("[agents]\nmaxConcurrent = 500\n", "agents.maxConcurrent"),
            (
                "[workspaceApi]\nmaxOutputChars = 500\n",
                "workspaceApi.maxOutputChars",
            ),
            (
                "[workspaceApi]\nmaxOutputChars = 20000000\n",
                "workspaceApi.maxOutputChars",
            ),
        ] {
            let err = SettingsFile::parse_str(body).unwrap_err();
            assert!(
                err.to_string().contains(key),
                "{body:?} should fail naming `{key}`: {err}"
            );
        }
    }

    #[test]
    fn workspace_api_max_output_chars_zero_means_unlimited() {
        let parsed = SettingsFile::parse_str("[workspaceApi]\nmaxOutputChars = 0\n").unwrap();
        assert_eq!(parsed.workspace_api.max_output_chars, 0);
    }

    #[test]
    fn floats_accept_integer_literals() {
        let parsed = SettingsFile::parse_str("[notifications]\nvolume = 1\n").unwrap();
        assert_eq!(parsed.notifications.volume, 1.0);
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
        file.server.ws_api.enabled = true;
        file.agents.idle_reap_minutes = 15;
        let text = toml::to_string(&file).expect("serializes");
        let back = SettingsFile::parse_str(&text).expect("re-parses");
        assert_eq!(back, file);
    }

    #[test]
    fn workspace_overrides_is_no_longer_a_schema_key() {
        // Strict parse rejects the legacy key like any other unknown key.
        let err = SettingsFile::parse_str("[model]\nworkspaceOverrides = { ws1 = \"m1\" }\n")
            .unwrap_err();
        assert!(err.to_string().contains("workspaceOverrides"), "{err}");
        assert!(!DEFAULT_CONFIG_TEMPLATE.contains("workspaceOverrides"));
    }

    #[test]
    fn ai_is_no_longer_a_schema_key() {
        // Strict parse rejects the retired [ai] table like any unknown key.
        let err = SettingsFile::parse_str("[ai]\nmodel = \"m1\"\n").unwrap_err();
        assert!(err.to_string().contains("ai"), "{err}");
        assert!(!DEFAULT_CONFIG_TEMPLATE.contains("[ai]"));
    }

    #[test]
    fn listen_mode_is_no_longer_a_schema_key() {
        // Strict parse rejects the retired key like any other unknown key.
        let err = SettingsFile::parse_str("[server]\nlistenMode = \"uds\"\n").unwrap_err();
        assert!(err.to_string().contains("listenMode"), "{err}");
        assert!(!DEFAULT_CONFIG_TEMPLATE.contains("listenMode"));
    }

    #[test]
    fn legacy_parse_captures_and_tolerates_listen_mode() {
        let text = "[server]\nlistenMode = \"both\"\nport = 5181\n";
        let (file, legacy) = SettingsFile::parse_str_with_legacy(text).expect("tolerant parse");
        assert_eq!(file.server.port, 5181);
        assert_eq!(
            legacy.get("server.listenMode"),
            Some(&serde_json::json!("both"))
        );
        assert_eq!(legacy.len(), 1);
    }

    #[test]
    fn legacy_parse_tolerates_listen_mode_values_the_old_enum_rejected() {
        // Legacy paths are captured BEFORE the strict parse, so even a value
        // the retired ListenMode enum would have rejected (`"quic"` was a
        // hard boot error) now boots — the daemon discards it at import time
        // (no catalog entry) and strips the key from the file.
        let (file, legacy) =
            SettingsFile::parse_str_with_legacy("[server]\nlistenMode = \"quic\"\n")
                .expect("tolerant parse");
        assert_eq!(file.server, ServerSettings::default());
        assert_eq!(
            legacy.get("server.listenMode"),
            Some(&serde_json::json!("quic"))
        );
        assert_eq!(legacy.len(), 1);
    }

    #[test]
    fn legacy_parse_captures_and_tolerates_ai_table() {
        let text = "[ai]\napiUrl = \"https://api.example\"\nmodel = \"m1\"\ntemperature = 0.5\n\n[git]\nautoCommit = false\n";
        let (file, legacy) = SettingsFile::parse_str_with_legacy(text).expect("tolerant parse");
        assert!(!file.git.auto_commit);
        assert_eq!(
            legacy.get("ai"),
            Some(&serde_json::json!({
                "apiUrl": "https://api.example",
                "model": "m1",
                "temperature": 0.5
            }))
        );
        assert_eq!(legacy.len(), 1);
    }

    #[test]
    fn legacy_parse_captures_and_tolerates_workspace_overrides() {
        let text =
            "[model]\ndefault = \"m0\"\nworkspaceOverrides = { ws1 = \"m1\", ws2 = \"m2\" }\n";
        let (file, legacy) = SettingsFile::parse_str_with_legacy(text).expect("tolerant parse");
        assert_eq!(file.model.default.as_deref(), Some("m0"));
        assert_eq!(
            legacy.get("model.workspaceOverrides"),
            Some(&serde_json::json!({ "ws1": "m1", "ws2": "m2" }))
        );
        assert_eq!(legacy.len(), 1);
    }

    #[test]
    fn legacy_parse_returns_empty_map_without_legacy_keys() {
        let (file, legacy) =
            SettingsFile::parse_str_with_legacy("[git]\nautoCommit = false\n").expect("parse");
        assert!(!file.git.auto_commit);
        assert!(legacy.is_empty());
    }

    #[test]
    fn legacy_parse_still_rejects_other_unknown_keys() {
        // An unrelated unknown key fails even when a legacy key is present.
        let err = SettingsFile::parse_str_with_legacy(
            "[model]\nworkspaceOverrides = {}\n\n[agents]\nbogusKey = 1\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogusKey"), "{err}");
        // …and without any legacy key too (delegates to the strict parse).
        let err = SettingsFile::parse_str_with_legacy("[bogus]\nkey = 1\n").unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    #[test]
    fn legacy_parse_still_range_validates() {
        let err = SettingsFile::parse_str_with_legacy(
            "[model]\nworkspaceOverrides = {}\n\n[server]\nport = 80\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("server.port"), "{err}");
    }

    #[test]
    fn load_or_init_with_legacy_reads_existing_file() {
        let dir = temp_path("legacy");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "[model]\nworkspaceOverrides = { ws1 = \"m1\" }\n\n[git]\nautoCommit = false\n",
        )
        .unwrap();
        let (file, legacy) = SettingsFile::load_or_init_with_legacy(&path).expect("load");
        assert!(!file.git.auto_commit);
        assert_eq!(
            legacy.get("model.workspaceOverrides"),
            Some(&serde_json::json!({ "ws1": "m1" }))
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
