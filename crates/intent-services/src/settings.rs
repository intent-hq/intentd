//! BE-owned settings store + `settings.*` business logic (§9.8, PROTOCOL §5.12).
//!
//! Owns the [`SettingDefinition`] schema (groups A + B of §9.8), type/enum/
//! min/max validation, and the redaction rule for **sensitive** settings.
//! Non-secret values persist in the `settings` table (`intent-store`); sensitive
//! values (`workspace.sshKeyPath`, `mcp.servers`, `server.auth.token`,
//! `sourceControl.github.token`) live in the OS keychain via the [`SecretStore`]
//! seam and are **never** returned in plaintext over the wire — list/get redact
//! them to presence/placeholder only, and `server.auth.token` is read-only.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_core::{Error, Result};
use serde_json::{json, Map, Value};

use intent_store::Store;

/// Keychain service name used for `intentd` secrets (matches `intent-transport`
/// auth + `intent-sourcecontrol`), so a setting like `server.auth.token` shares
/// the same keychain entry the transport layer reads.
const KEYRING_SERVICE: &str = "intentd";

/// Placeholder returned for a sensitive setting that **has** a stored value, so
/// the wire conveys presence without ever leaking the plaintext (§9.8).
pub(crate) const REDACTED_PLACEHOLDER: &str = "********";

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
        secret(
            "workspace.sshKeyPath",
            "SSH key path",
            "Path to the SSH key used for git",
            "workspace",
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
            5180.0,
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
pub(crate) struct SettingsService<'a> {
    store: &'a Store,
    secrets: &'a dyn SecretStore,
}

impl<'a> SettingsService<'a> {
    pub(crate) fn new(store: &'a Store, secrets: &'a dyn SecretStore) -> Self {
        Self { store, secrets }
    }

    /// The current value for a definition: sensitive settings are **redacted**
    /// (placeholder when present, `null` when absent — never plaintext);
    /// non-secret settings come from the DB, falling back to the default.
    async fn current_value(&self, def: &SettingDefinition) -> Value {
        if def.sensitive {
            if self.secrets.load(def.path).is_some() {
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
    pub(crate) async fn list(&self) -> Result<Value> {
        let defs = definitions();
        let mut out = Vec::with_capacity(defs.len());
        for def in &defs {
            let value = self.current_value(def).await;
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
                self.secrets.store(def.path, &secret_value)?;
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
            self.secrets.delete(def.path)?;
        } else {
            self.store.delete_setting(def.path).await?;
        }
        let value = self.current_value(&def).await;
        Ok(json!({ "path": def.path, "value": value }))
    }
}
