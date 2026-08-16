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
    DEFAULT_HOOKS_MAX_PER_AGENT, DEFAULT_IDLE_REAP_MINUTES, DEFAULT_MAX_CONCURRENT_ADAPTERS,
    DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS, DEFAULT_PR_MONITOR_POLL_SECONDS,
    DEFAULT_SERVER_MAX_OUTSTANDING_RPCS, DEFAULT_STREAM_RETENTION_HOURS,
    DEFAULT_WAKE_RESUME_ENABLED, DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS,
    DEFAULT_WORKSPACE_API_MAX_OUTPUT_CHARS, DEFAULT_WORKSPACE_API_TOON_OUTPUT,
    MAX_CONCURRENT_ADAPTERS_LIMIT,
};
use crate::error::{Error, Result};

/// Root of the `config.toml` schema. One field per top-level TOML table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SettingsFile {
    pub providers: ProvidersSettings,
    pub model: ModelSettings,
    pub quick_actions: QuickActionsSettings,
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
    pub wake_resume: WakeResumeSettings,
    pub pr_monitor: PrMonitorSettings,
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
    /// `model.defaultReasoningEffort` — fallback reasoning effort for new
    /// agents. Stored as-is (providers own the vocabulary); a blank value
    /// reads as unset.
    #[serde(deserialize_with = "de_blank_as_none")]
    pub default_reasoning_effort: Option<String>,
}

/// `[quickActions]` — model config for single-shot quick actions
/// (`quickActions.*`): commit messages, PR descriptions, quick tasks. These
/// keys never apply to interactive or delegated agent sessions
/// (monorepo#1729); the group was named `backgroundAgents` before that
/// rename (see [`LEGACY_SETTINGS_PATHS`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct QuickActionsSettings {
    /// `quickActions.defaultModel` — model for quick actions.
    pub default_model: Option<String>,
    /// `quickActions.typeOverrides` — per-quick-action model overrides
    /// (`commit`, `pr`, `review`, `fast`).
    pub type_overrides: BTreeMap<String, String>,
    /// `quickActions.providerSettings` — per-provider quick-action settings
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
    /// `server.maxOutstandingRpcs` — daemon-wide cap on outstanding slow-path
    /// RPCs across every connection; `0` means unlimited.
    pub max_outstanding_rpcs: u32,
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
            max_outstanding_rpcs: DEFAULT_SERVER_MAX_OUTSTANDING_RPCS,
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
    /// `[voice.workspaceVocabulary]` — auto-derived workspace vocabulary.
    pub workspace_vocabulary: VoiceWorkspaceVocabularySettings,
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

/// Default `voice.workspaceVocabulary.maxTerms`.
pub const DEFAULT_VOICE_WORKSPACE_VOCABULARY_MAX_TERMS: u32 = 50;

/// `[voice.workspaceVocabulary]` — auto-derived workspace vocabulary tuning
/// (`voice.workspaceVocabulary.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct VoiceWorkspaceVocabularySettings {
    /// `voice.workspaceVocabulary.maxTerms` — cap on the auto-derived
    /// workspace vocabulary injected into `voice.transcribe` calls carrying a
    /// `workspaceId` and served by `voice.getWorkspaceVocabulary` (0 disables
    /// derivation and injection entirely; max 100).
    pub max_terms: u32,
}

impl Default for VoiceWorkspaceVocabularySettings {
    fn default() -> Self {
        Self {
            max_terms: DEFAULT_VOICE_WORKSPACE_VOCABULARY_MAX_TERMS,
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
    /// `agents.memoryBudgetMb` — aggregate resident-memory budget for the
    /// daemon's whole child-process tree, above which new agent spawns queue
    /// behind idle-process eviction instead of starting immediately and the
    /// periodic reap sweep drains idle agents largest-first without waiting
    /// for a spawn or the idle TTL (monorepo#2063 level 2). Absent
    /// (`None`, the default) = auto (budget derived from system RAM); explicit
    /// `0` = off — preserved because config files written before the auto
    /// default carried a literal `memoryBudgetMb = 0` meaning off, and per the
    /// monorepo#2109 no-migration precedent their behaviour must not change;
    /// positive = budget in MB (changes apply on daemon restart; max
    /// 1,024,000).
    pub memory_budget_mb: Option<u32>,
    /// `agents.maxConcurrentAdapters` — daemon-wide cap on concurrently live
    /// ephemeral ACP adapters (one-shot `agent.completeOnce` completions and
    /// model probes; changes apply on daemon restart; range 1–64). Unlike
    /// `maxConcurrent` this has no "auto" value and no unlimited setting: the
    /// bound exists because each chain costs ~610 MB and one-shots hold no
    /// agent slot, so removing the ceiling is exactly the failure being
    /// fixed (monorepo#2062).
    pub max_concurrent_adapters: u32,
    /// `agents.idleReapMinutes` — minutes before an idle agent is reaped
    /// (0 disables idle reaping).
    pub idle_reap_minutes: u32,
    /// `agents.flushQueuedMessages` — how the whole queued-message backlog
    /// is delivered when an idle agent drains its queue: `all` batches every
    /// ready entry into one turn, `systemOnly` batches only system-origin
    /// entries (user-origin entries stay FIFO), `off` is one turn per queued
    /// message.
    pub flush_queued_messages: FlushQueuedMessagesMode,
    /// `agents.resumeInterruptedOnStart` — whether the daemon resumes
    /// interrupted agents at startup when `--resume-all` is absent: `auto`
    /// resumes only on headless hosts (no display detected), `on` always
    /// resumes, `off` never resumes. The `--resume-all` flag forces the
    /// sweep regardless of this setting.
    pub resume_interrupted_on_start: ResumeInterruptedOnStart,
}

impl Default for AgentsSettings {
    fn default() -> Self {
        Self {
            max_concurrent: 0,
            memory_budget_mb: None,
            max_concurrent_adapters: DEFAULT_MAX_CONCURRENT_ADAPTERS,
            idle_reap_minutes: DEFAULT_IDLE_REAP_MINUTES,
            flush_queued_messages: FlushQueuedMessagesMode::All,
            resume_interrupted_on_start: ResumeInterruptedOnStart::Auto,
        }
    }
}

/// `agents.resumeInterruptedOnStart` values. Serializes as lowercase strings
/// (`"auto"`, `"on"`, `"off"`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResumeInterruptedOnStart {
    /// Resume interrupted agents at startup only on headless hosts.
    #[default]
    Auto,
    /// Always resume interrupted agents at startup.
    On,
    /// Never resume interrupted agents at startup.
    Off,
}

impl ResumeInterruptedOnStart {
    /// The wire/TOML string for this value.
    pub fn as_str(&self) -> &'static str {
        match self {
            ResumeInterruptedOnStart::Auto => "auto",
            ResumeInterruptedOnStart::On => "on",
            ResumeInterruptedOnStart::Off => "off",
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
/// (`agentFeatures.*`). All default **on** except `taskGraph` (opt-in);
/// changes apply to new agent sessions only.
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
    /// `agentFeatures.stateSnapshot` — inject the per-turn agent state
    /// snapshot line (`current ws.agent.snapshot() => {...}`) into outbound
    /// turn prompts. Unlike the other toggles this is read LIVE each turn —
    /// flipping it affects the very next turn of every session, existing
    /// ones included. The `ws.agent.snapshot()` MCP tool itself is never
    /// gated.
    pub state_snapshot: bool,
    /// `agentFeatures.prMonitor` — expose centralized PR monitoring
    /// (`ws.pr.monitor` / `ws.pr.unmonitor`) to agents.
    pub pr_monitor: bool,
    /// `agentFeatures.taskGraph` — teach agents the task-graph workflow:
    /// batch `ws.agent.delegate({ tasks })` guidance, `dependsOn` /
    /// `conflictsWith` relations, inline `@@@task` fence attributes, and the
    /// "Tasks now unblocked…" wake section. Docs/prompt only — the underlying
    /// APIs are never dispatch-denied. Prompt/help gating and unblocked-wake
    /// teaching use the value captured when the parent session is created;
    /// changing the live setting does not affect existing sessions' wakes.
    /// Defaults **off** (opt-in), unlike the other toggles
    /// (intent-hq/monorepo#2445).
    pub task_graph: bool,
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
            state_snapshot: true,
            pr_monitor: true,
            task_graph: false,
        }
    }
}

/// `[wakeResume]` — host sleep/wake detection + resume (`wakeResume.*`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct WakeResumeSettings {
    /// `wakeResume.enabled` — detect host sleep/wake and resume work on wake.
    pub enabled: bool,
    /// `wakeResume.thresholdSeconds` — minimum suspend duration (seconds) that
    /// counts as a sleep; also the resume/enrollment gate.
    pub threshold_seconds: u32,
}

impl Default for WakeResumeSettings {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_WAKE_RESUME_ENABLED,
            threshold_seconds: DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS,
        }
    }
}

/// `[prMonitor]` — centralized PR-monitor loop knobs (`prMonitor.*`). Both
/// values are read live by the monitor loop, so a change applies without a
/// daemon restart; sub-floor values are clamped at read time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct PrMonitorSettings {
    /// `prMonitor.debounceSeconds` — quiet window a changed PR must observe
    /// before its consolidated wake is delivered.
    pub debounce_seconds: u64,
    /// `prMonitor.pollSeconds` — poll cadence for the centralized monitor
    /// loop (config-file key; not exposed in the Settings UI).
    pub poll_seconds: u64,
}

impl Default for PrMonitorSettings {
    fn default() -> Self {
        Self {
            debounce_seconds: DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS,
            poll_seconds: DEFAULT_PR_MONITOR_POLL_SECONDS,
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

/// Normalize a blank optional string to `None`, so a key left as `""` in the
/// file (or cleared with an empty string over the wire) reads as unset rather
/// than as an explicit empty value.
fn de_blank_as_none<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<String> = Option::deserialize(deserializer)?;
    Ok(raw.filter(|v| !v.trim().is_empty()))
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
/// `workspace.autoFetch` is likewise retired outright (the periodic-fetch
/// feature was removed) — discarded and stripped. `backgroundAgents` covers
/// the whole renamed `[backgroundAgents]` table (monorepo#1729): its captured
/// values are migrated into the `quickActions.*` keys at boot and the table is
/// then stripped.
pub const LEGACY_SETTINGS_PATHS: &[&str] = &[
    "model.workspaceOverrides",
    "ai",
    "server.listenMode",
    "workspace.autoFetch",
    "backgroundAgents",
];

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
        // Mirrors the catalog bound so a hand-edited config.toml cannot boot a
        // cap the `settings.update` RPC would have rejected (`0` = unlimited).
        if self.server.max_outstanding_rpcs > 100_000 {
            return Err(bad(
                "server.maxOutstandingRpcs",
                format!(
                    "must be 0 (unlimited) or between 1 and 100000, got {}",
                    self.server.max_outstanding_rpcs
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
        if let Some(mb) = self.agents.memory_budget_mb {
            if mb > 1_024_000 {
                return Err(bad(
                    "agents.memoryBudgetMb",
                    format!("must be absent (auto), 0 (off), or between 1 and 1024000, got {mb}"),
                ));
            }
        }
        // No `0` escape hatch here (unlike maxOutstandingRpcs): an unbounded
        // adapter spawn is the monorepo#2062 failure itself, so a hand-edited
        // config.toml cannot boot without a ceiling.
        let adapters = self.agents.max_concurrent_adapters;
        if !(1..=MAX_CONCURRENT_ADAPTERS_LIMIT).contains(&adapters) {
            return Err(bad(
                "agents.maxConcurrentAdapters",
                format!("must be between 1 and {MAX_CONCURRENT_ADAPTERS_LIMIT}, got {adapters}"),
            ));
        }
        let chars = self.workspace_api.max_output_chars;
        if chars != 0 && !(1_000..=10_000_000).contains(&chars) {
            return Err(bad(
                "workspaceApi.maxOutputChars",
                format!("must be 0 (unlimited) or between 1000 and 10000000, got {chars}"),
            ));
        }
        let terms = self.voice.workspace_vocabulary.max_terms;
        if terms > 100 {
            return Err(bad(
                "voice.workspaceVocabulary.maxTerms",
                format!("must be between 0 and 100, got {terms}"),
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
/// annotated with its catalog label and description — except the opt-in
/// `agentFeatures.taskGraph`, deliberately not seeded so configs without the
/// key pick up a future default flip automatically (intent-hq/monorepo#2643).
/// Parsing this template must yield exactly [`SettingsFile::default`]
/// (enforced by a unit test).
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
# Default reasoning effort -- fallback reasoning effort for new agents; the
# value is provider-defined and stored as-is, and a blank value means unset.
# defaultReasoningEffort = "high"

[quickActions]
# Quick action default model -- model for single-shot quick actions (commit
# messages, PR descriptions, quick tasks); never applied to agent sessions.
# defaultModel = "claude-sonnet-4-5"
# Quick action type overrides -- per-quick-action model overrides.
typeOverrides = {}
# Quick action provider settings -- per-provider quick-action settings.
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
# Max outstanding RPCs -- daemon-wide cap on outstanding slow-path RPCs across
# every connection; over-limit requests are rejected with -32011 "Server
# overloaded" (0 = unlimited; changes apply on daemon restart).
maxOutstandingRpcs = 256

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

[voice.workspaceVocabulary]
# Workspace vocabulary max terms -- cap on the auto-derived workspace
# vocabulary injected into voice.transcribe calls carrying a workspaceId and
# served by voice.getWorkspaceVocabulary (0 disables derivation and injection
# entirely; max 100).
maxTerms = 50

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
# What an agent subtree actually costs, and what each knob below does and does
# not bound, is written up under "Agent process-tree memory" in
# docs/ARCHITECTURE.md of the intent-hq/monorepo repo. Every figure quoted in
# this table is measured (monorepo#2062, #2063, #2109).
# Max concurrent agents -- concurrent agent session cap (0 = auto based on
# system RAM; changes apply on daemon restart; max 200). This is a concurrency
# cap, not a memory cap: a measured agent subtree spans a 22x range (~0.66 GB
# idle, up to 9.6 GB running a test suite), so slot count does not predict
# memory -- idleReapMinutes and memoryBudgetMb are the memory bounds.
maxConcurrent = 0
# Agent memory budget (MB) -- aggregate resident memory the daemon's whole
# child-process tree may use before it reclaims: new agent spawns queue behind
# idle-process eviction, and a background sweep drains idle agents
# largest-first while over budget (nothing running is ever killed; changes
# apply on daemon restart).
# Absent (the default, as in this file) = auto: the daemon picks the budget
# ((RAM - 8 GB) / 2, min 4 GB). Explicit 0 = off, always. Upgrade note:
# config files written before this key defaulted to auto carry a literal
# `memoryBudgetMb = 0`, which stays off -- delete the line to opt into
# auto. A positive value is the budget in MB (max 1024000). A soft
# admission gate rather than a ceiling: measured transient overshoot of
# 65-105% and steady state ~16% over, so budget for roughly 2x the configured
# value as the transient. The overshoot is a fixed offset, not proportional
# to demand -- at 1500 MB a 20-agent burst peaked the same as an 8-agent one
# (3.06 vs 3.09 GB) where the same 20-agent burst unbounded reached 12.37 GB.
# That 2x rule sizes the admission transient for a burst of comparable
# agents; the gate runs at spawn only, so an already-admitted agent whose own
# workload grows (a test suite) is never re-checked and can carry the tree
# past the budget by itself.
# memoryBudgetMb = 8192
# Max concurrent adapters -- daemon-wide cap on concurrently live ephemeral ACP
# adapters (one-shot completions and model probes). Each costs ~610 MB and
# holds no agent slot; over-limit calls queue and fail with "adapter-busy" if
# their own timeout expires first (1-64; changes apply on daemon restart).
# Once a burst exceeds the cap, peak live chains equal the cap exactly and are
# invariant to how much bigger the burst is (a smaller burst is unaffected and
# peaks at its own size). The over-limit caller has spawned nothing, so its
# retry is always safe.
maxConcurrentAdapters = 6
# Idle reap minutes -- minutes before an idle agent is reaped (0 disables idle
# reaping). The main lever on resident memory: every agent touched inside the
# window keeps its whole subtree alive (~0.66 GB each when idle), so a seat
# that touches 20 agents within the window holds all 20 subtrees at once --
# projecting to ~12 GB doing nothing, more if any of them ran a test suite.
# Measured at a 30-minute TTL (the default before monorepo#2109): 40 procs /
# 5.85 GB flat across 10 minutes of full idle, zero exits; the same tree at a
# 2-minute TTL drained to 0 in 122 s. The default is now 10 minutes, so that
# tree starts draining once the window passes instead of holding 5.85 GB for
# another 20. Raise it to keep processes warm for longer and reclaim their
# memory later; lower it for the reverse. Only agents idle past the TTL are
# candidates, and the sweep
# skips any agent reported busy when it checks. An agent is selected within
# the TTL plus one sweep (sweep interval is ttl/4, clamped to 30-300 s); the
# memory comes back as each kill completes, so a large idle set drains over a
# tail rather than all at once.
idleReapMinutes = 10
# Flush queued messages -- how the queued-message backlog is delivered when
# an idle agent drains its queue: "all", "systemOnly", or "off".
flushQueuedMessages = "all"
# Resume interrupted on start -- whether the daemon resumes interrupted
# agents at startup when --resume-all is absent: "auto" resumes only on
# headless hosts (no display detected), "on" always resumes, "off" never
# resumes. --resume-all forces the sweep regardless of this setting.
resumeInterruptedOnStart = "auto"

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
# All toggles listed here default to on; changes apply to new agent
# sessions only.
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
# State snapshot -- inject the per-turn agent state snapshot line into turn
# prompts; unlike the other toggles this applies to the next turn of every
# session (live), existing sessions included.
stateSnapshot = true
# PR monitor -- expose centralized PR monitoring (ws.pr.monitor /
# ws.pr.unmonitor) to agents.
prMonitor = true

[wakeResume]
# Wake resume enabled -- detect host sleep/wake and resume work on wake.
enabled = true
# Wake resume threshold seconds -- minimum suspend duration (in seconds) that
# counts as a sleep; also the resume/enrollment gate.
thresholdSeconds = 10

[prMonitor]
# PR monitor debounce seconds -- quiet window (in seconds) a changed PR must
# observe before its consolidated wake is delivered (minimum 10).
debounceSeconds = 60
# PR monitor poll seconds -- how often (in seconds) the centralized loop polls
# each monitored PR (minimum 10).
pollSeconds = 30
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
        assert_eq!(d.model.default_reasoning_effort, None);
        assert!(d.quick_actions.provider_settings.is_empty());
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
        assert_eq!(
            d.voice.workspace_vocabulary.max_terms,
            DEFAULT_VOICE_WORKSPACE_VOCABULARY_MAX_TERMS
        );
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
        assert!(d.agent_features.state_snapshot);
        assert_eq!(d.wake_resume.enabled, DEFAULT_WAKE_RESUME_ENABLED);
        assert_eq!(
            d.wake_resume.threshold_seconds,
            DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS
        );
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
        assert!(parsed.agent_features.state_snapshot);
        assert!(parsed.agent_features.pr_monitor);
        // `taskGraph` is the one opt-in toggle: absent → off.
        assert!(!parsed.agent_features.task_graph);
    }

    #[test]
    fn agent_features_unknown_key_is_rejected() {
        let err = SettingsFile::parse_str("[agentFeatures]\nhostExek = false\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("agentFeatures"), "names the table: {msg}");
        assert!(msg.contains("hostExek"), "names the bad key: {msg}");
    }

    #[test]
    fn task_graph_defaults_off_and_opts_in() {
        // Opt-in (intent-hq/monorepo#2445): empty file resolves to off, and
        // the shipped template no longer seeds the key (monorepo#2643), so
        // first-boot configs pick up a future default flip automatically; an
        // explicit `taskGraph = true` opts in.
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert!(!parsed.agent_features.task_graph);
        assert!(!DEFAULT_CONFIG_TEMPLATE.contains("taskGraph"));
        let templated = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert!(!templated.agent_features.task_graph);
        let parsed = SettingsFile::parse_str("[agentFeatures]\ntaskGraph = true\n")
            .expect("override parses");
        assert!(parsed.agent_features.task_graph);
        let parsed = SettingsFile::parse_str("[agentFeatures]\ntaskGraph = false\n")
            .expect("override parses");
        assert!(!parsed.agent_features.task_graph);
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
    fn resume_interrupted_on_start_accepts_variants() {
        for (raw, expected) in [
            ("\"auto\"", ResumeInterruptedOnStart::Auto),
            ("\"on\"", ResumeInterruptedOnStart::On),
            ("\"off\"", ResumeInterruptedOnStart::Off),
        ] {
            let parsed =
                SettingsFile::parse_str(&format!("[agents]\nresumeInterruptedOnStart = {raw}\n"))
                    .expect("parses");
            assert_eq!(parsed.agents.resume_interrupted_on_start, expected, "{raw}");
        }
    }

    #[test]
    fn resume_interrupted_on_start_defaults_to_auto() {
        let parsed = SettingsFile::parse_str("").expect("empty parses");
        assert_eq!(
            parsed.agents.resume_interrupted_on_start,
            ResumeInterruptedOnStart::Auto
        );
    }

    #[test]
    fn resume_interrupted_on_start_rejects_unknown_string() {
        let err = SettingsFile::parse_str("[agents]\nresumeInterruptedOnStart = \"maybe\"\n")
            .unwrap_err();
        assert!(
            err.to_string().contains("resumeInterruptedOnStart"),
            "{err}"
        );
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
            "[providers]\nactive = \"claude-code\"\n\n[providers.enabled]\nclaude-code = true\ncodex = false\n\n[providers.paths]\ncodex = \"/usr/local/bin/codex\"\n\n[mcp]\ndisabledServers = [\"linear\"]\n\n[server]\noriginAllowList = [\"https://app.example.com\"]\n\n[quickActions.providerSettings.claude-code]\nmode = \"fast\"\n",
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
            .quick_actions
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
        // An explicit 0 (off) must survive the round trip as `Some(0)`, never
        // collapsing into the absent-key auto default.
        file.agents.memory_budget_mb = Some(0);
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
    fn auto_fetch_is_no_longer_a_schema_key() {
        // Strict parse rejects the retired key like any other unknown key.
        let err = SettingsFile::parse_str("[workspace]\nautoFetch = false\n").unwrap_err();
        assert!(err.to_string().contains("autoFetch"), "{err}");
        assert!(!DEFAULT_CONFIG_TEMPLATE.contains("autoFetch"));
    }

    #[test]
    fn legacy_parse_captures_and_tolerates_auto_fetch() {
        let text = "[workspace]\nautoFetch = false\ncowIsolation = true\n";
        let (file, legacy) = SettingsFile::parse_str_with_legacy(text).expect("tolerant parse");
        assert!(file.workspace.cow_isolation);
        assert_eq!(
            legacy.get("workspace.autoFetch"),
            Some(&serde_json::json!(false))
        );
        assert_eq!(legacy.len(), 1);
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
    fn model_default_reasoning_effort_parses_as_an_optional_string() {
        let parsed = SettingsFile::parse_str(
            "[model]\ndefault = \"m0\"\ndefaultReasoningEffort = \"xhigh\"\n",
        )
        .expect("parse");
        assert_eq!(parsed.model.default.as_deref(), Some("m0"));
        assert_eq!(
            parsed.model.default_reasoning_effort.as_deref(),
            Some("xhigh")
        );

        // Stored as-is: the daemon never normalizes the provider vocabulary.
        let parsed = SettingsFile::parse_str("[model]\ndefaultReasoningEffort = \"Medium\"\n")
            .expect("parse");
        assert_eq!(
            parsed.model.default_reasoning_effort.as_deref(),
            Some("Medium")
        );

        // Absent from `[model]` leaves it unset.
        let parsed = SettingsFile::parse_str("[model]\ndefault = \"m0\"\n").expect("parse");
        assert_eq!(parsed.model.default_reasoning_effort, None);

        // A blank value reads as unset, so no consumer ever observes an
        // explicit empty effort.
        for text in [
            "[model]\ndefaultReasoningEffort = \"\"\n",
            "[model]\ndefaultReasoningEffort = \"   \"\n",
        ] {
            let parsed = SettingsFile::parse_str(text).expect("parse");
            assert_eq!(parsed.model.default_reasoning_effort, None, "{text}");
        }
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
    fn sleep_resume_defaults_enabled_with_threshold_ten() {
        // A file with no [wakeResume] section resolves to the feature-on
        // defaults (enabled = true, thresholdSeconds = 10).
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert!(parsed.wake_resume.enabled);
        assert_eq!(parsed.wake_resume.threshold_seconds, 10);
        // The default template ships the commented [wakeResume] section and
        // parses back to the same defaults.
        assert!(DEFAULT_CONFIG_TEMPLATE.contains("[wakeResume]"));
        let templated = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert!(templated.wake_resume.enabled);
        assert_eq!(templated.wake_resume.threshold_seconds, 10);
    }

    #[test]
    fn sleep_resume_explicit_override_parses() {
        let parsed =
            SettingsFile::parse_str("[wakeResume]\nenabled = false\nthresholdSeconds = 45\n")
                .expect("override parses");
        assert!(!parsed.wake_resume.enabled);
        assert_eq!(parsed.wake_resume.threshold_seconds, 45);
    }

    #[test]
    fn pr_monitor_defaults_and_template_round_trip() {
        // A file with no [prMonitor] section resolves to the shipped defaults,
        // and `agentFeatures.prMonitor` defaults on.
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert!(parsed.agent_features.pr_monitor);
        assert_eq!(
            parsed.pr_monitor.debounce_seconds,
            DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS
        );
        assert_eq!(
            parsed.pr_monitor.poll_seconds,
            DEFAULT_PR_MONITOR_POLL_SECONDS
        );
        assert!(DEFAULT_CONFIG_TEMPLATE.contains("[prMonitor]"));
        let templated = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert_eq!(templated.pr_monitor, parsed.pr_monitor);
        assert!(templated.agent_features.pr_monitor);
    }

    #[test]
    fn max_outstanding_rpcs_defaults_and_template_round_trip() {
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert_eq!(
            parsed.server.max_outstanding_rpcs,
            DEFAULT_SERVER_MAX_OUTSTANDING_RPCS
        );
        assert!(DEFAULT_CONFIG_TEMPLATE.contains("maxOutstandingRpcs"));
        let templated = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert_eq!(
            templated.server.max_outstanding_rpcs,
            DEFAULT_SERVER_MAX_OUTSTANDING_RPCS
        );
    }

    #[test]
    fn max_outstanding_rpcs_explicit_override_parses() {
        let parsed =
            SettingsFile::parse_str("[server]\nmaxOutstandingRpcs = 4\n").expect("override parses");
        assert_eq!(parsed.server.max_outstanding_rpcs, 4);
        // `0` is the documented "unlimited" value, not a range error.
        let unlimited =
            SettingsFile::parse_str("[server]\nmaxOutstandingRpcs = 0\n").expect("zero parses");
        assert_eq!(unlimited.server.max_outstanding_rpcs, 0);
    }

    /// The file path enforces the same `0..=100000` bound the catalog does, so
    /// a hand-edited config.toml cannot boot a cap `settings.update` rejects
    /// (a huge value would also blow past `Semaphore::MAX_PERMITS` on 32-bit).
    #[test]
    fn max_outstanding_rpcs_out_of_range_is_rejected() {
        let err = SettingsFile::parse_str("[server]\nmaxOutstandingRpcs = 500000\n")
            .expect_err("out-of-range value must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("server.maxOutstandingRpcs") && msg.contains("500000"),
            "error names the offending key and value: {msg}"
        );
        assert!(
            SettingsFile::parse_str("[server]\nmaxOutstandingRpcs = 100000\n").is_ok(),
            "the upper bound itself is legal"
        );
    }

    /// The `agents.memoryBudgetMb` parse matrix (monorepo#2063): an absent
    /// key is auto (`None`), an explicit `0` is off (`Some(0)` — every config
    /// file the old template wrote carries that literal, so its behaviour is
    /// preserved), a positive value is an explicit MB budget, and the upper
    /// bound matches what the catalog enforces.
    #[test]
    fn memory_budget_mb_absent_auto_zero_off_positive_explicit() {
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert_eq!(parsed.agents.memory_budget_mb, None, "absent key = auto");
        assert!(
            !DEFAULT_CONFIG_TEMPLATE.contains("\nmemoryBudgetMb ="),
            "the template for new installs must not write the key (auto)"
        );

        let off =
            SettingsFile::parse_str("[agents]\nmemoryBudgetMb = 0\n").expect("explicit 0 parses");
        assert_eq!(
            off.agents.memory_budget_mb,
            Some(0),
            "explicit 0 = off, distinct from the absent-key auto"
        );

        let overridden =
            SettingsFile::parse_str("[agents]\nmemoryBudgetMb = 20480\n").expect("override parses");
        assert_eq!(overridden.agents.memory_budget_mb, Some(20_480));

        let err = SettingsFile::parse_str("[agents]\nmemoryBudgetMb = 2000000\n")
            .expect_err("out-of-range value must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("agents.memoryBudgetMb") && msg.contains("2000000"),
            "error names the offending key and value: {msg}"
        );
        assert!(
            SettingsFile::parse_str("[agents]\nmemoryBudgetMb = 1024000\n").is_ok(),
            "the upper bound itself is legal"
        );
    }

    /// The ephemeral-adapter bound ships enabled: an empty file and the
    /// shipped template both resolve to the same in-range default, so a daemon
    /// that has never been configured still has a ceiling (monorepo#2062).
    #[test]
    fn max_concurrent_adapters_defaults_and_template_round_trip() {
        let parsed = SettingsFile::parse_str("").expect("empty file parses");
        assert_eq!(
            parsed.agents.max_concurrent_adapters,
            DEFAULT_MAX_CONCURRENT_ADAPTERS
        );
        assert!(DEFAULT_CONFIG_TEMPLATE.contains("maxConcurrentAdapters"));
        let templated = SettingsFile::parse_str(DEFAULT_CONFIG_TEMPLATE).expect("template parses");
        assert_eq!(
            templated.agents.max_concurrent_adapters,
            DEFAULT_MAX_CONCURRENT_ADAPTERS
        );
        assert!(
            (4..=8).contains(&DEFAULT_MAX_CONCURRENT_ADAPTERS),
            "the shipped default must stay inside the agreed 4-8 range"
        );
    }

    /// Unlike `maxOutstandingRpcs`, this key has no `0` = unlimited escape
    /// hatch: an unbounded adapter spawn is the failure the bound exists to
    /// prevent, so a hand-edited file cannot reintroduce it.
    #[test]
    fn max_concurrent_adapters_range_is_enforced_with_no_unlimited_value() {
        let parsed = SettingsFile::parse_str("[agents]\nmaxConcurrentAdapters = 4\n")
            .expect("in-range override parses");
        assert_eq!(parsed.agents.max_concurrent_adapters, 4);

        for bad_value in ["0", "65"] {
            let err = SettingsFile::parse_str(&format!(
                "[agents]\nmaxConcurrentAdapters = {bad_value}\n"
            ))
            .expect_err("out-of-range value must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("agents.maxConcurrentAdapters") && msg.contains(bad_value),
                "error names the offending key and value: {msg}"
            );
        }
        assert!(
            SettingsFile::parse_str(&format!(
                "[agents]\nmaxConcurrentAdapters = {MAX_CONCURRENT_ADAPTERS_LIMIT}\n"
            ))
            .is_ok(),
            "the upper bound itself is legal"
        );
    }

    #[test]
    fn pr_monitor_explicit_override_parses() {
        let parsed = SettingsFile::parse_str(
            "[agentFeatures]\nprMonitor = false\n\n[prMonitor]\ndebounceSeconds = 15\npollSeconds = 90\n",
        )
        .expect("override parses");
        assert!(!parsed.agent_features.pr_monitor);
        assert_eq!(parsed.pr_monitor.debounce_seconds, 15);
        assert_eq!(parsed.pr_monitor.poll_seconds, 90);
    }

    #[test]
    fn pr_monitor_unknown_key_is_rejected() {
        let err = SettingsFile::parse_str("[prMonitor]\ndebounceSecs = 30\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("prMonitor"), "names the table: {msg}");
        assert!(msg.contains("debounceSecs"), "names the bad key: {msg}");
    }

    #[test]
    fn sleep_resume_unknown_key_is_rejected() {
        let err = SettingsFile::parse_str("[wakeResume]\nthreshholdSeconds = 20\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("wakeResume"), "names the table: {msg}");
        assert!(
            msg.contains("threshholdSeconds"),
            "names the bad key: {msg}"
        );
    }

    #[test]
    fn sleep_resume_wrong_type_is_rejected() {
        let err =
            SettingsFile::parse_str("[wakeResume]\nthresholdSeconds = \"soon\"\n").unwrap_err();
        assert!(
            err.to_string().contains("wakeResume.thresholdSeconds"),
            "names the key: {err}"
        );
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
