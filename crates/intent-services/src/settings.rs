//! BE-owned settings store + `settings.*` business logic (§9.8, PROTOCOL §5.12).
//!
//! Owns the [`SettingDefinition`] schema (groups A + B of §9.8), type/enum/
//! min/max validation, and the redaction rule for **sensitive** settings.
//! Non-secret values persist in the `settings` table (`intent-store`); sensitive
//! values (`mcp.servers`, `server.auth.token`, `sourceControl.github.token`,
//! `linear.token`, `accounts.sentry.token`, `ai.apiToken`) live in the OS
//! keychain via the [`SecretStore`] seam and are **never** returned in
//! plaintext over the wire — list/get redact them to presence/placeholder
//! only, and `server.auth.token` is read-only. `workspace.sshKeyPath` is a
//! plain non-secret **path** setting (the real secret is the key file on
//! disk); the FE `git`-env consumer must read the value back verbatim.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Error, Result};
use serde_json::{json, Map, Value};
use tokio::task;
use tokio::time::timeout;

use intent_store::Store;

/// Keychain service name used for `intentd` secrets (matches `intent-transport`
/// auth + `intent-sourcecontrol`), so a setting like `server.auth.token` shares
/// the same keychain entry the transport layer reads.
const KEYRING_SERVICE: &str = "intentd";

/// Placeholder returned for a sensitive setting that **has** a stored value, so
/// the wire conveys presence without ever leaking the plaintext (§9.8).
pub(crate) const REDACTED_PLACEHOLDER: &str = "********";

/// Per-secret keychain-call timeout. The [`KeyringSecretStore`] wraps blocking
/// OS-keychain FFI (`Security.framework` / `libsecret` / `wincred`), which can
/// stall indefinitely — e.g. a locked/prompting keychain — and would otherwise
/// hang the transport task servicing `settings.list` / `get` / `update` /
/// `reset`. On timeout the operation is treated as "absent" for reads and
/// surfaces `-32603` for writes so the response is never silently dropped.
const SECRET_OP_TIMEOUT: Duration = Duration::from_secs(3);

/// Abstraction over secret persistence (the sensitive-setting analog of the
/// transport's `TokenStore`). Production uses [`KeyringSecretStore`]; tests
/// inject [`InMemorySecretStore`] so they never touch the real user keychain.
pub trait SecretStore: Send + Sync {
    /// Return the stored secret for `account`, or `None` if unset/unavailable.
    fn load(&self, account: &str) -> Option<String>;
    /// Persist `value` for `account`, replacing any existing secret.
    fn store(&self, account: &str, value: &str) -> Result<()>;
    /// Delete the secret for `account`; absence is an idempotent success.
    fn delete(&self, account: &str) -> Result<()>;
}

/// OS-keychain-backed [`SecretStore`] (the production default).
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyringSecretStore;

impl SecretStore for KeyringSecretStore {
    fn load(&self, account: &str) -> Option<String> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, account).ok()?;
        entry.get_password().ok().filter(|v| !v.is_empty())
    }

    fn store(&self, account: &str, value: &str) -> Result<()> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, account)
            .map_err(|e| Error::Internal(format!("keychain unavailable: {e}")))?;
        entry
            .set_password(value)
            .map_err(|e| Error::Internal(format!("failed to persist secret: {e}")))
    }

    fn delete(&self, account: &str) -> Result<()> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, account)
            .map_err(|e| Error::Internal(format!("keychain unavailable: {e}")))?;
        match entry.delete_credential() {
            Ok(()) => Ok(()),
            // Deleting an absent secret is an idempotent no-op success.
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(Error::Internal(format!("failed to delete secret: {e}"))),
        }
    }
}

/// In-memory [`SecretStore`] for tests — never touches the real keychain.
#[derive(Debug, Default, Clone)]
pub struct InMemorySecretStore {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl SecretStore for InMemorySecretStore {
    fn load(&self, account: &str) -> Option<String> {
        self.map
            .lock()
            .unwrap()
            .get(account)
            .filter(|v| !v.is_empty())
            .cloned()
    }

    fn store(&self, account: &str, value: &str) -> Result<()> {
        self.map
            .lock()
            .unwrap()
            .insert(account.to_string(), value.to_string());
        Ok(())
    }

    fn delete(&self, account: &str) -> Result<()> {
        self.map.lock().unwrap().remove(account);
        Ok(())
    }
}

/// The value-type vocabulary of a [`SettingDefinition`] (PROTOCOL §5.12).
#[derive(Debug, Clone, Copy)]
pub(crate) enum SettingType {
    Boolean,
    Number {
        min: Option<f64>,
        max: Option<f64>,
    },
    String,
    Enum(&'static [&'static str]),
    /// Structured JSON (objects or arrays), e.g. `string[]` / `mcp.servers`.
    Object,
}

impl SettingType {
    fn wire_name(&self) -> &'static str {
        match self {
            SettingType::Boolean => "boolean",
            SettingType::Number { .. } => "number",
            SettingType::String => "string",
            SettingType::Enum(_) => "enum",
            SettingType::Object => "object",
        }
    }
}

/// One BE-owned setting definition (the schema half of §5.12's
/// `SettingDefinitionWithValue`). Ported from `app-settings-schema.ts`
/// `AppSettingDefinition`; group B adds the intentd-only host/daemon settings.
#[derive(Debug, Clone)]
pub(crate) struct SettingDefinition {
    pub path: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub ty: SettingType,
    pub default_value: Option<Value>,
    pub sensitive: bool,
    /// `server.auth.token` is read-only via the API (regenerate, not set).
    pub read_only: bool,
}

impl SettingDefinition {
    /// Serialize the bare `SettingDefinition` (no `value`) per §5.12, including
    /// `enumValues`/`min`/`max`/`defaultValue`/`sensitive` only when present.
    fn definition_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("path".into(), json!(self.path));
        m.insert("label".into(), json!(self.label));
        m.insert("description".into(), json!(self.description));
        m.insert("category".into(), json!(self.category));
        m.insert("type".into(), json!(self.ty.wire_name()));
        match self.ty {
            SettingType::Enum(values) => {
                m.insert("enumValues".into(), json!(values));
            }
            SettingType::Number { min, max } => {
                if let Some(min) = min {
                    m.insert("min".into(), json!(min));
                }
                if let Some(max) = max {
                    m.insert("max".into(), json!(max));
                }
            }
            _ => {}
        }
        if let Some(default) = &self.default_value {
            m.insert("defaultValue".into(), default.clone());
        }
        if self.sensitive {
            m.insert("sensitive".into(), json!(true));
        }
        Value::Object(m)
    }

    /// Validate `value` against this definition (type / enum / min / max).
    /// Failures surface as [`Error::InvalidParams`] → `-32602` (§5.12).
    fn validate(&self, value: &Value) -> Result<()> {
        let invalid = |msg: String| Err(Error::InvalidParams(msg));
        match self.ty {
            SettingType::Boolean => {
                if !value.is_boolean() {
                    return invalid(format!("{}: expected a boolean", self.path));
                }
            }
            SettingType::String => {
                if !value.is_string() {
                    return invalid(format!("{}: expected a string", self.path));
                }
            }
            SettingType::Object => {
                if !(value.is_object() || value.is_array()) {
                    return invalid(format!("{}: expected an object or array", self.path));
                }
            }
            SettingType::Enum(values) => match value.as_str() {
                Some(s) if values.contains(&s) => {}
                _ => {
                    return invalid(format!(
                        "{}: must be one of [{}]",
                        self.path,
                        values.join(", ")
                    ))
                }
            },
            SettingType::Number { min, max } => {
                let n = match value.as_f64() {
                    Some(n) => n,
                    None => return invalid(format!("{}: expected a number", self.path)),
                };
                if let Some(min) = min {
                    if n < min {
                        return invalid(format!("{}: must be >= {min}", self.path));
                    }
                }
                if let Some(max) = max {
                    if n > max {
                        return invalid(format!("{}: must be <= {max}", self.path));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Look up a definition by dotted `path`, or `None` when unknown.
pub(crate) fn find_definition(path: &str) -> Option<SettingDefinition> {
    definitions().into_iter().find(|d| d.path == path)
}

fn boolean(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    default: bool,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::Boolean,
        default_value: Some(json!(default)),
        sensitive: false,
        read_only: false,
    }
}

fn string(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    default: Option<&str>,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::String,
        default_value: default.map(|s| json!(s)),
        sensitive: false,
        read_only: false,
    }
}

fn secret(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::String,
        default_value: None,
        sensitive: true,
        read_only: false,
    }
}

fn number(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    min: Option<f64>,
    max: Option<f64>,
    default: f64,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::Number { min, max },
        default_value: Some(json!(default)),
        sensitive: false,
        read_only: false,
    }
}

fn enumerated(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    values: &'static [&'static str],
    default: &'static str,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::Enum(values),
        default_value: Some(json!(default)),
        sensitive: false,
        read_only: false,
    }
}

fn object(
    path: &'static str,
    label: &'static str,
    description: &'static str,
    category: &'static str,
    default: Option<Value>,
) -> SettingDefinition {
    SettingDefinition {
        path,
        label,
        description,
        category,
        ty: SettingType::Object,
        default_value: default,
        sensitive: false,
        read_only: false,
    }
}

/// The full BE-owned setting schema (§9.8 groups A + B). Group C (FE-only) is
/// deliberately excluded — the daemon neither stores nor exposes it.
pub(crate) fn definitions() -> Vec<SettingDefinition> {
    let mut server_auth_token = secret(
        "server.auth.token",
        "WS auth token",
        "Bearer token for the TCP/WSS listener (regenerate-only)",
        "server",
    );
    server_auth_token.read_only = true;
    vec![
        // --- Group A: providers / agents -----------------------------------
        string(
            "providers.active",
            "Active provider",
            "Default agent provider",
            "providers",
            None,
        ),
        object(
            "providers.enabled",
            "Enabled providers",
            "Providers offered to users",
            "providers",
            None,
        ),
        object(
            "providers.paths",
            "Provider paths",
            "Per-provider CLI path overrides",
            "providers",
            Some(json!({})),
        ),
        string(
            "model.default",
            "Default model",
            "Fallback model for new agents",
            "providers",
            None,
        ),
        object(
            "model.providerDefaults",
            "Provider default models",
            "Default model per provider",
            "providers",
            Some(json!({})),
        ),
        object(
            "model.workspaceOverrides",
            "Workspace model overrides",
            "Per-workspace model overrides",
            "providers",
            Some(json!({})),
        ),
        string(
            "backgroundAgents.defaultModel",
            "Background default model",
            "Model for background agents",
            "providers",
            None,
        ),
        object(
            "backgroundAgents.typeOverrides",
            "Background type overrides",
            "Per-agent-type model overrides",
            "providers",
            Some(json!({})),
        ),
        object(
            "backgroundAgents.providerSettings",
            "Background provider settings",
            "Per-provider background settings",
            "providers",
            Some(json!({})),
        ),
        string(
            "specialists.default",
            "Default specialist",
            "Specialist applied when none is chosen",
            "providers",
            None,
        ),
        // --- Group A: workspace / git ---------------------------------------
        string(
            "workspace.branchPrefix",
            "Branch prefix",
            "Prefix for agent-created branches",
            "workspace",
            None,
        ),
        string(
            "workspace.worktreesLocation",
            "Worktrees location",
            "Directory for created worktrees",
            "workspace",
            None,
        ),
        string(
            "workspace.sshKeyPath",
            "SSH key path",
            "Path to the SSH key used for git",
            "workspace",
            None,
        ),
        string(
            "workspace.defaultShell",
            "Default shell",
            "Shell used for terminals/scripts",
            "workspace",
            None,
        ),
        boolean(
            "workspace.autoFetch",
            "Auto-fetch",
            "Periodically fetch from the remote",
            "workspace",
            false,
        ),
        boolean(
            "git.autoCommit",
            "Auto-commit",
            "Allow agents to commit without explicit user request",
            "git",
            true,
        ),
        // --- Group A: MCP ----------------------------------------------------
        boolean(
            "mcp.enableUserServers",
            "Enable user MCP servers",
            "Start user-scoped MCP servers",
            "mcp",
            true,
        ),
        object(
            "mcp.disabledServers",
            "Disabled MCP servers",
            "Server ids that stay stopped",
            "mcp",
            Some(json!([])),
        ),
        secret(
            "mcp.servers",
            "MCP servers",
            "External MCP server configs (secrets in keychain)",
            "mcp",
        ),
        // --- Group A: user notifications --------------------------------------
        // Ports the FE `notificationSettings` electron-store bag (four fields
        // surfaced individually via the FE app-settings schema `notifications.*`
        // paths) so the daemon owns the persisted notification user prefs and
        // the legacy `settings` electron-store can retire (§9.8 group A).
        boolean(
            "notifications.enabled",
            "Notifications enabled",
            "Whether app notifications are enabled",
            "notifications",
            true,
        ),
        boolean(
            "notifications.soundEnabled",
            "Notification sounds",
            "Whether notification sounds are enabled",
            "notifications",
            true,
        ),
        boolean(
            "notifications.soundOnlyWhenUnfocused",
            "Sound only when unfocused",
            "Only play notification sounds when the app is unfocused",
            "notifications",
            true,
        ),
        number(
            "notifications.volume",
            "Notification volume",
            "Notification sound volume from 0 to 1",
            "notifications",
            Some(0.0),
            Some(1.0),
            0.5,
        ),
        // --- Group B: server / transport ------------------------------------
        enumerated(
            "server.listenMode",
            "Listen mode",
            "Transport(s) the daemon serves",
            "server",
            &["uds", "tcp", "both"],
            "uds",
        ),
        string(
            "server.socketPath",
            "Socket path",
            "Unix socket path for the UDS listener",
            "server",
            None,
        ),
        string(
            "server.bindAddress",
            "Bind address",
            "Address the TCP listener binds",
            "server",
            Some("0.0.0.0"),
        ),
        number(
            "server.port",
            "WS port",
            "TCP port for the WSS listener",
            "server",
            Some(1024.0),
            Some(65535.0),
            5181.0,
        ),
        boolean(
            "server.tls.enabled",
            "TLS enabled",
            "Enable TLS for the TCP listener",
            "server",
            false,
        ),
        boolean(
            "server.auth.enabled",
            "Auth enabled",
            "Require a bearer token on TCP",
            "server",
            true,
        ),
        server_auth_token,
        object(
            "server.originAllowList",
            "Origin allow-list",
            "Permitted WS origins",
            "server",
            None,
        ),
        boolean(
            "server.discovery.enabled",
            "mDNS discovery",
            "Advertise the daemon over mDNS",
            "server",
            false,
        ),
        // --- Group B: source control ----------------------------------------
        enumerated(
            "sourceControl.activeProvider",
            "Source-control provider",
            "Active forge implementation",
            "sourceControl",
            &["github"],
            "github",
        ),
        enumerated(
            "sourceControl.github.tokenSource",
            "GitHub token source",
            "Where the GitHub token comes from",
            "sourceControl",
            &["env", "gh-cli", "explicit"],
            "gh-cli",
        ),
        secret(
            "sourceControl.github.token",
            "GitHub token",
            "PAT used by the GitHub client",
            "sourceControl",
        ),
        string(
            "sourceControl.github.apiBaseUrl",
            "GitHub API base URL",
            "GitHub (Enterprise) API base",
            "sourceControl",
            Some("https://api.github.com"),
        ),
        // --- Group A: Linear integration --------------------------------------
        secret(
            "linear.token",
            "Linear API key",
            "API key used by the Linear integration",
            "linear",
        ),
        // --- Group A: Sentry account -----------------------------------------
        secret(
            "accounts.sentry.token",
            "Sentry API token",
            "API token used by the Sentry integration",
            "accounts",
        ),
        string(
            "accounts.sentry.organization",
            "Sentry organization",
            "Sentry organization slug (non-secret companion of accounts.sentry.token)",
            "accounts",
            None,
        ),
        // --- Group A: primary AI provider config ------------------------------
        // Ports the FE `workspace-config` `config.ai.*` blob so the daemon owns
        // the provider knobs the FE previously stored in electron-store.
        // `ai.apiToken` is a **secret**; the rest are plain settings.
        secret(
            "ai.apiToken",
            "AI provider API token",
            "Bearer token used by the primary AI provider",
            "ai",
        ),
        string(
            "ai.apiUrl",
            "AI provider API URL",
            "Base URL for the primary AI provider",
            "ai",
            None,
        ),
        string("ai.model", "AI model", "Default AI model", "ai", None),
        number(
            "ai.temperature",
            "AI temperature",
            "Sampling temperature for the primary AI provider",
            "ai",
            Some(0.0),
            Some(2.0),
            0.7,
        ),
        number(
            "ai.maxTokens",
            "AI max tokens",
            "Maximum tokens per completion for the primary AI provider",
            "ai",
            Some(1.0),
            None,
            4096.0,
        ),
        number(
            "ai.streamingSpeed",
            "AI streaming speed",
            "Streaming pacing hint (tokens per second; 0 = no throttle)",
            "ai",
            Some(0.0),
            None,
            0.0,
        ),
        // --- Group A: persisted permission rules ------------------------------
        // Port of the FE `ConfigManager` `config.permissions.rules` bag: an array
        // of command allow/deny/ask entries with optional expiries. Structure is
        // opaque here; the runtime enforcement path validates entries.
        object(
            "permissions.rules",
            "Command permission rules",
            "Persisted command allow/deny/ask rules",
            "permissions",
            Some(json!([])),
        ),
        // --- Group A: user + workspace prompt-rules ---------------------------
        // Ports of `ConfigManager` `config.userRules` and `config.workspaceRules`:
        // free-form content injected into agent system prompts. Kept as opaque
        // objects here; the prompt-assembly pipeline validates internal shape.
        object(
            "userRules",
            "User rules",
            "Global user prompt-rule content injected into agent system prompts",
            "rules",
            Some(json!({})),
        ),
        object(
            "workspaceRules",
            "Workspace rules",
            "Workspace-scoped prompt-rule content injected into agent system prompts",
            "rules",
            Some(json!({})),
        ),
        // --- Group A: cross-workspace known repos -----------------------------
        // Port of the FE `repo-registry` electron-store: the ordered list of
        // recently used repositories the FE surfaces in "recent repos" UI.
        object(
            "repos.known",
            "Known repositories",
            "Recently used repositories tracked across workspaces",
            "repos",
            Some(json!([])),
        ),
        // --- Group A: persisted workspace change history ----------------------
        // Port of the FE default `config.json` `changeHistory` bag: per-workspace
        // durable diff summaries the FE renders in the change-history UI.
        object(
            "workspace.changeHistory",
            "Workspace change history",
            "Per-workspace persisted diff summaries",
            "workspace",
            Some(json!({})),
        ),
        // --- Group B: context engine ----------------------------------------
        boolean(
            "context.enabled",
            "Context engine",
            "Enable the auggie context engine",
            "context",
            true,
        ),
        string(
            "context.auggiePath",
            "auggie path",
            "Path to the auggie binary",
            "context",
            None,
        ),
        boolean(
            "context.allowIndexing",
            "Allow indexing",
            "Permit codebase indexing",
            "context",
            true,
        ),
        // --- Group B: storage / runtime -------------------------------------
        string(
            "storage.dataDir",
            "Data directory",
            "Daemon data directory",
            "storage",
            None,
        ),
        string(
            "workspaces.root",
            "Workspaces root",
            "Root directory for workspaces",
            "storage",
            None,
        ),
        enumerated(
            "logging.level",
            "Log level",
            "Daemon log verbosity",
            "logging",
            &["error", "warn", "info", "debug", "trace"],
            "info",
        ),
        number(
            "agents.maxConcurrent",
            "Max concurrent agents",
            "Concurrent agent session cap",
            "agents",
            Some(1.0),
            None,
            8.0,
        ),
        number(
            "agents.idleReapMinutes",
            "Idle reap minutes",
            "Minutes before an idle agent is reaped",
            "agents",
            Some(0.0),
            None,
            30.0,
        ),
    ]
}

/// Read the effective `workspace.branchPrefix` (default empty) — prepended to
/// auto-generated workspace branch names (TS `getBranchPrefix` parity). A
/// missing/garbled row means "no prefix".
pub(crate) async fn branch_prefix(store: &Store) -> String {
    match store.get_setting("workspace.branchPrefix").await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Read the effective `git.autoCommit` flag (default `true`) — the gate behind
/// `assert_agent_commit_allowed` (§9.8 OQ#2). A missing/garbled row defaults to
/// the permissive `true` so the established auto-commit behavior is preserved.
pub(crate) async fn auto_commit_enabled(store: &Store) -> bool {
    match store.get_setting("git.autoCommit").await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        _ => true,
    }
}

/// Stateless executor for the `settings.*` namespace over a [`Store`] +
/// [`SecretStore`]. Construct one per call from the long-lived `Services`.
///
/// The [`SecretStore`] is held as an `Arc` so its blocking keychain FFI calls
/// can be moved into [`tokio::task::spawn_blocking`] with a timeout — without
/// that offload a stalling keychain (locked, prompting, or otherwise) would
/// hang the transport task servicing `settings.*` and the response would never
/// reach the wire.
pub(crate) struct SettingsService<'a> {
    store: &'a Store,
    secrets: Arc<dyn SecretStore>,
}

impl<'a> SettingsService<'a> {
    pub(crate) fn new(store: &'a Store, secrets: Arc<dyn SecretStore>) -> Self {
        Self { store, secrets }
    }

    /// Probe the keychain for `path` without blocking the runtime.
    ///
    /// Runs [`SecretStore::load`] on a blocking thread with a
    /// [`SECRET_OP_TIMEOUT`] budget: on timeout or panic the setting is
    /// treated as absent (returns `false`) so `settings.list` / `get` still
    /// respond promptly — a stalled OS keychain must never silently drop a
    /// well-formed JSON-RPC response.
    async fn secret_present(&self, path: &'static str) -> bool {
        let secrets = Arc::clone(&self.secrets);
        let handle = task::spawn_blocking(move || secrets.load(path).is_some());
        match timeout(SECRET_OP_TIMEOUT, handle).await {
            Ok(Ok(present)) => present,
            Ok(Err(e)) => {
                tracing::warn!(setting = path, error = %e, "secret load panicked");
                false
            }
            Err(_) => {
                tracing::warn!(
                    setting = path,
                    timeout_ms = SECRET_OP_TIMEOUT.as_millis() as u64,
                    "secret load timed out — reporting absent",
                );
                false
            }
        }
    }

    /// Persist `value` for the given sensitive `path` via [`SecretStore::store`]
    /// off the runtime, subject to [`SECRET_OP_TIMEOUT`]. A timeout or panic
    /// surfaces as [`Error::Internal`] → `-32603` so callers always get a
    /// terminal JSON-RPC response instead of silence.
    async fn secret_store(&self, path: &'static str, value: String) -> Result<()> {
        let secrets = Arc::clone(&self.secrets);
        let handle = task::spawn_blocking(move || secrets.store(path, &value));
        match timeout(SECRET_OP_TIMEOUT, handle).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => Err(Error::Internal(format!("secret store panicked: {e}"))),
            Err(_) => Err(Error::Internal(format!(
                "keychain write timed out after {}ms",
                SECRET_OP_TIMEOUT.as_millis()
            ))),
        }
    }

    /// Delete the secret at `path` via [`SecretStore::delete`] off the runtime,
    /// subject to [`SECRET_OP_TIMEOUT`]. Timeout / panic surfaces as
    /// [`Error::Internal`] so `settings.reset` never hangs the transport.
    async fn secret_delete(&self, path: &'static str) -> Result<()> {
        let secrets = Arc::clone(&self.secrets);
        let handle = task::spawn_blocking(move || secrets.delete(path));
        match timeout(SECRET_OP_TIMEOUT, handle).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => Err(Error::Internal(format!("secret delete panicked: {e}"))),
            Err(_) => Err(Error::Internal(format!(
                "keychain delete timed out after {}ms",
                SECRET_OP_TIMEOUT.as_millis()
            ))),
        }
    }

    /// The current value for a definition: sensitive settings are **redacted**
    /// (placeholder when present, `null` when absent — never plaintext);
    /// non-secret settings come from the DB, falling back to the default.
    async fn current_value(&self, def: &SettingDefinition) -> Value {
        if def.sensitive {
            if self.secret_present(def.path).await {
                json!(REDACTED_PLACEHOLDER)
            } else {
                Value::Null
            }
        } else {
            match self.store.get_setting(def.path).await {
                Ok(Some(raw)) => serde_json::from_str(&raw).unwrap_or(Value::Null),
                _ => def.default_value.clone().unwrap_or(Value::Null),
            }
        }
    }

    /// `settings.list` → `{ settings: SettingDefinitionWithValue[] }` (§5.12).
    ///
    /// Sensitive-setting presence probes are launched **concurrently** so the
    /// worst-case latency is one [`SECRET_OP_TIMEOUT`], not `N × timeout`. A
    /// stalled OS keychain therefore bounds the whole call to a single budget
    /// instead of accumulating across every sensitive definition.
    pub(crate) async fn list(&self) -> Result<Value> {
        let defs = definitions();
        // Launch each sensitive probe first so their timeouts run in parallel.
        let mut sensitive: Vec<(&'static str, tokio::task::JoinHandle<bool>)> = Vec::new();
        for def in &defs {
            if def.sensitive {
                let secrets = Arc::clone(&self.secrets);
                let path = def.path;
                let handle = task::spawn(async move {
                    let inner = task::spawn_blocking(move || secrets.load(path).is_some());
                    match timeout(SECRET_OP_TIMEOUT, inner).await {
                        Ok(Ok(present)) => present,
                        Ok(Err(e)) => {
                            tracing::warn!(setting = path, error = %e, "secret load panicked");
                            false
                        }
                        Err(_) => {
                            tracing::warn!(
                                setting = path,
                                timeout_ms = SECRET_OP_TIMEOUT.as_millis() as u64,
                                "secret load timed out — reporting absent",
                            );
                            false
                        }
                    }
                });
                sensitive.push((def.path, handle));
            }
        }
        // Collect all sensitive results — panics fall back to "absent" so the
        // wire response is always well-formed.
        let mut presence: HashMap<&'static str, bool> = HashMap::with_capacity(sensitive.len());
        for (path, handle) in sensitive {
            let present = handle.await.unwrap_or(false);
            presence.insert(path, present);
        }

        let mut out = Vec::with_capacity(defs.len());
        for def in &defs {
            let value = if def.sensitive {
                if presence.get(def.path).copied().unwrap_or(false) {
                    json!(REDACTED_PLACEHOLDER)
                } else {
                    Value::Null
                }
            } else {
                match self.store.get_setting(def.path).await {
                    Ok(Some(raw)) => serde_json::from_str(&raw).unwrap_or(Value::Null),
                    _ => def.default_value.clone().unwrap_or(Value::Null),
                }
            };
            let mut obj = def.definition_json();
            if let Some(map) = obj.as_object_mut() {
                map.insert("value".into(), value);
            }
            out.push(obj);
        }
        Ok(json!({ "settings": out }))
    }

    /// `settings.get` → `{ path, value, definition }`; unknown path → `-32602`.
    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        let def = find_definition(path)
            .ok_or_else(|| Error::InvalidParams(format!("unknown setting: {path}")))?;
        let value = self.current_value(&def).await;
        Ok(json!({
            "path": def.path,
            "value": value,
            "definition": def.definition_json(),
        }))
    }

    /// `settings.update` — validate the whole batch first (unknown path,
    /// read-only path, or type/enum/min/max failure → `-32602`, nothing
    /// applied), then persist. Returns the **redacted** applied `{ path, value }`
    /// pairs for the response + `settings:changed` payload.
    pub(crate) async fn update(&self, changes: &Value) -> Result<Vec<Value>> {
        let entries = changes
            .as_array()
            .ok_or_else(|| Error::InvalidParams("'changes' must be an array".to_string()))?;
        let mut planned: Vec<(SettingDefinition, Value)> = Vec::with_capacity(entries.len());
        for entry in entries {
            let path = entry
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::InvalidParams("each change requires a 'path'".to_string()))?;
            let value = entry.get("value").cloned().ok_or_else(|| {
                Error::InvalidParams(format!("change for {path} requires a 'value'"))
            })?;
            let def = find_definition(path)
                .ok_or_else(|| Error::InvalidParams(format!("unknown setting: {path}")))?;
            if def.read_only {
                return Err(Error::InvalidParams(format!("{path} is read-only")));
            }
            def.validate(&value)?;
            planned.push((def, value));
        }

        let mut applied = Vec::with_capacity(planned.len());
        for (def, value) in planned {
            if def.sensitive {
                let secret_value = match &value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                self.secret_store(def.path, secret_value).await?;
                applied.push(json!({ "path": def.path, "value": REDACTED_PLACEHOLDER }));
            } else {
                let raw = serde_json::to_string(&value)
                    .map_err(|e| Error::Internal(format!("encode setting failed: {e}")))?;
                self.store.set_setting(def.path, &raw).await?;
                applied.push(json!({ "path": def.path, "value": value }));
            }
        }
        Ok(applied)
    }

    /// `settings.reset` → restore the default (delete the persisted/secret value)
    /// and return the **redacted** `{ path, value }`; unknown path → `-32602`.
    pub(crate) async fn reset(&self, path: &str) -> Result<Value> {
        let def = find_definition(path)
            .ok_or_else(|| Error::InvalidParams(format!("unknown setting: {path}")))?;
        if def.sensitive {
            self.secret_delete(def.path).await?;
        } else {
            self.store.delete_setting(def.path).await?;
        }
        let value = self.current_value(&def).await;
        Ok(json!({ "path": def.path, "value": value }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `linear.token` must be a sensitive catalog entry so `settings.update`
    /// persists it to the keychain under service `intentd` / account
    /// `linear.token` (account = setting path) — the exact entry
    /// `intent-linear`'s token resolver reads.
    #[test]
    fn linear_token_is_a_sensitive_catalog_entry() {
        let def = find_definition("linear.token").expect("linear.token missing from catalog");
        assert_eq!(def.path, "linear.token", "keychain account = setting path");
        assert!(
            def.sensitive,
            "must persist to keychain + redact on the wire"
        );
        assert!(!def.read_only);
        assert_eq!(def.category, "linear");
        assert!(matches!(def.ty, SettingType::String));
        assert!(def.default_value.is_none());
    }

    /// `accounts.sentry.token` and `ai.apiToken` — the two secret catalog gaps
    /// closed for R0-4 — must be sensitive so `settings.update` persists them to
    /// the keychain under service `intentd` / account = setting path (never the
    /// DB) and every wire read (`settings.list` / `settings.get`) redacts them
    /// to a placeholder or `null` when unset.
    #[test]
    fn new_secret_catalog_entries_are_sensitive() {
        for path in ["accounts.sentry.token", "ai.apiToken"] {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert_eq!(def.path, path);
            assert!(def.sensitive, "{path} must be a sensitive catalog entry");
            assert!(!def.read_only, "{path} must not be read-only");
            assert!(matches!(def.ty, SettingType::String));
            assert!(def.default_value.is_none(), "{path} default is null");
        }
    }

    /// `workspace.sshKeyPath` is a **plain non-secret** path setting: the value
    /// is a filesystem path pointing at the key, not key material — the real
    /// secret is the key file on disk. Marking the catalog entry as sensitive
    /// makes `settings.get` return the redaction placeholder, which
    /// permanently breaks the FE `git`-env consumer (`app-settings.service.ts`
    /// `getSshKeyPath` must read the real path back to hand it to `git`).
    #[test]
    fn ssh_key_path_is_a_plain_non_secret_string() {
        let def = find_definition("workspace.sshKeyPath").expect("workspace.sshKeyPath missing");
        assert!(
            !def.sensitive,
            "workspace.sshKeyPath is a path setting, not key material"
        );
        assert!(!def.read_only);
        assert_eq!(def.category, "workspace");
        assert!(matches!(def.ty, SettingType::String));
        assert!(def.default_value.is_none());
    }

    /// The non-secret companion of `accounts.sentry.token` lives beside it in
    /// the `accounts` category with no default (the FE opts in per install).
    #[test]
    fn sentry_organization_is_a_plain_string_setting() {
        let def =
            find_definition("accounts.sentry.organization").expect("sentry organization missing");
        assert!(!def.sensitive);
        assert!(matches!(def.ty, SettingType::String));
        assert_eq!(def.category, "accounts");
        assert!(def.default_value.is_none());
    }

    /// The non-secret half of the `ai.*` group (URL / model / temperature /
    /// maxTokens / streamingSpeed) ports the FE `workspace-config` `config.ai.*`
    /// blob one-to-one; `temperature` carries the documented 0..=2 clamp and
    /// `maxTokens` / `streamingSpeed` refuse negative values.
    #[test]
    fn ai_non_secret_group_matches_fe_shape() {
        for (path, default_present) in [
            ("ai.apiUrl", false),
            ("ai.model", false),
            ("ai.temperature", true),
            ("ai.maxTokens", true),
            ("ai.streamingSpeed", true),
        ] {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert!(!def.sensitive, "{path} is non-secret");
            assert_eq!(def.category, "ai");
            assert_eq!(
                def.default_value.is_some(),
                default_present,
                "{path} default presence"
            );
        }
        let temp = find_definition("ai.temperature").unwrap();
        assert!(matches!(
            temp.ty,
            SettingType::Number {
                min: Some(0.0),
                max: Some(2.0),
            }
        ));
        for path in ["ai.maxTokens", "ai.streamingSpeed"] {
            let def = find_definition(path).unwrap();
            let min = match def.ty {
                SettingType::Number { min, .. } => min,
                _ => panic!("{path} must be a Number"),
            };
            assert!(
                min.map(|m| m >= 0.0).unwrap_or(false),
                "{path} must reject negative values"
            );
        }
    }

    /// The five non-secret gap entries live in the catalog as opaque `Object`
    /// settings with a documented default. Each is validated by shape only;
    /// downstream consumers own the internal schema (permission rules, prompt
    /// rules, known repos, change-history bags).
    #[test]
    fn non_secret_object_gap_entries_have_defaults() {
        for path in [
            "permissions.rules",
            "userRules",
            "workspaceRules",
            "repos.known",
            "workspace.changeHistory",
        ] {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert!(!def.sensitive, "{path} must be non-secret");
            assert!(
                matches!(def.ty, SettingType::Object),
                "{path} must be Object"
            );
            assert!(def.default_value.is_some(), "{path} has a default");
        }
    }

    /// Ports the FE `notificationSettings` electron-store bag as four
    /// individually-addressable, non-secret catalog entries under the
    /// `notifications` category. `notifications.volume` carries a documented
    /// `0.0..=1.0` clamp so out-of-range writes surface as `-32602`.
    #[test]
    fn notifications_catalog_entries_match_fe_shape() {
        for (path, expect_bool, default) in [
            ("notifications.enabled", true, json!(true)),
            ("notifications.soundEnabled", true, json!(true)),
            ("notifications.soundOnlyWhenUnfocused", true, json!(true)),
            ("notifications.volume", false, json!(0.5)),
        ] {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert!(!def.sensitive, "{path} must be non-secret");
            assert!(!def.read_only, "{path} must not be read-only");
            assert_eq!(def.category, "notifications");
            assert_eq!(def.default_value.as_ref(), Some(&default));
            if expect_bool {
                assert!(
                    matches!(def.ty, SettingType::Boolean),
                    "{path} must be Boolean"
                );
            } else {
                assert!(
                    matches!(
                        def.ty,
                        SettingType::Number {
                            min: Some(0.0),
                            max: Some(1.0),
                        }
                    ),
                    "{path} must be a Number clamped to 0..=1"
                );
            }
        }
    }

    /// A [`SecretStore`] whose `load` blocks the calling thread for `hang_for`
    /// before returning `None` — models a locked / prompting OS keychain that
    /// used to hang `settings.list` indefinitely.
    #[derive(Debug)]
    struct HangingSecretStore {
        hang_for: Duration,
    }
    impl SecretStore for HangingSecretStore {
        fn load(&self, _account: &str) -> Option<String> {
            std::thread::sleep(self.hang_for);
            None
        }
        fn store(&self, _account: &str, _value: &str) -> Result<()> {
            std::thread::sleep(self.hang_for);
            Ok(())
        }
        fn delete(&self, _account: &str) -> Result<()> {
            std::thread::sleep(self.hang_for);
            Ok(())
        }
    }

    /// Regression: a stalled OS keychain (e.g. locked, prompting) MUST NOT hang
    /// `settings.list`. Each sensitive setting is bounded by
    /// [`SECRET_OP_TIMEOUT`] and, on timeout, reads as `null` (absent) so the
    /// response reaches the wire — never a silent drop over WS/UDS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn settings_list_survives_a_hung_keychain() {
        let tmp =
            std::env::temp_dir().join(format!("intentd-settings-hang-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("open store");
        // Hang for well over the per-op budget so a naive implementation would
        // stall for `N_sensitive * hang_for` seconds and blow past the test's
        // outer timeout.
        let secrets: Arc<dyn SecretStore> = Arc::new(HangingSecretStore {
            hang_for: Duration::from_secs(30),
        });
        let svc = SettingsService::new(&store, secrets);

        let started = std::time::Instant::now();
        // Outer cap: sensitive probes fan out concurrently, so total latency
        // is bounded by ONE `SECRET_OP_TIMEOUT` — well under the 30-second
        // per-call stall the store models. A small slack absorbs task-spawn
        // overhead on cold runners.
        let cap = SECRET_OP_TIMEOUT + Duration::from_secs(2);
        let list = timeout(cap, svc.list())
            .await
            .expect("settings.list must not hang when the keychain stalls")
            .expect("settings.list must not return a domain error");
        let elapsed = started.elapsed();

        // Every sensitive entry MUST redact to `null` (absent) — the timed-out
        // load is treated as "not present" so the response is well-formed.
        let arr = list["settings"].as_array().expect("settings array");
        let mut sensitive_seen = 0;
        for entry in arr {
            if entry["sensitive"] == json!(true) {
                sensitive_seen += 1;
                assert_eq!(
                    entry["value"],
                    Value::Null,
                    "hung keychain must read as absent for {}: {entry}",
                    entry["path"],
                );
            }
        }
        assert!(sensitive_seen > 0, "catalog must contain sensitive entries");
        assert!(
            elapsed < cap,
            "settings.list took {elapsed:?} — timeout cap was {cap:?}",
        );

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// Regression: `settings.update` on a sensitive path with a stalled
    /// keychain MUST surface `Error::Internal` (→ `-32603`) instead of hanging
    /// the transport task — the write path is symmetric with the read path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn settings_update_secret_times_out_on_hung_keychain() {
        let tmp = std::env::temp_dir().join(format!(
            "intentd-settings-hang-update-{}.db",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(&tmp).await.expect("open store");
        let secrets: Arc<dyn SecretStore> = Arc::new(HangingSecretStore {
            hang_for: Duration::from_secs(30),
        });
        let svc = SettingsService::new(&store, secrets);

        let started = std::time::Instant::now();
        let cap = SECRET_OP_TIMEOUT + Duration::from_secs(2);
        let err = timeout(
            cap,
            svc.update(&json!([{ "path": "linear.token", "value": "irrelevant" }])),
        )
        .await
        .expect("settings.update must not hang past the budget")
        .expect_err("hung keychain must surface an error, not success");
        let elapsed = started.elapsed();
        assert!(
            matches!(err, Error::Internal(_)),
            "expected Error::Internal, got {err:?}",
        );
        assert!(
            elapsed < cap,
            "settings.update took {elapsed:?}, cap {cap:?}"
        );

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }
}
