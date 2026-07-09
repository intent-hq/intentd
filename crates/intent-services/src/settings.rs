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
use std::time::{Duration, Instant};

use intent_core::{Error, Result};
use serde_json::{json, Map, Value};
use tokio::sync::watch;
use tokio::time::timeout;

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

/// Default bounded wait for a secret **read** before the caller gives up and the
/// setting is reported as unset. A stuck OS keychain (e.g. a pending macOS auth
/// prompt) would otherwise block the caller — and, historically, an entire
/// tokio worker — indefinitely.
const DEFAULT_LOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Default bounded wait for a secret **write** (`store` / `delete`). Longer than
/// the read budget because a first-time write can prompt the user for keychain
/// approval; still must not block the runtime forever.
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a load result (present or absent) is served from the in-process
/// cache before the next call re-consults the backing keychain. Keeps
/// `settings.list` cheap on the FE mount path without turning secret changes
/// into a long propagation window.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);
/// Rate-limit window per account for the `keychain load timed out` warning so
/// a wedged Keychain doesn't drown the daemon log.
const DEFAULT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Async, single-flight, TTL-cached wrapper around a synchronous [`SecretStore`]
/// so blocking OS keychain calls (macOS Security framework, etc.) never wedge
/// the tokio runtime. Every backing call runs on the blocking pool via
/// [`tokio::task::spawn_blocking`]; reads are bounded by a short timeout and
/// coalesced per account via single-flight so a hung keychain occupies at most
/// one blocking-pool thread total (not one per request). Cache entries are
/// invalidated on successful writes and expire on TTL.
pub(crate) struct AsyncSecretStore {
    inner: Arc<dyn SecretStore>,
    state: Arc<Mutex<AsyncState>>,
    load_timeout: Duration,
    write_timeout: Duration,
    cache_ttl: Duration,
    warn_interval: Duration,
}

/// Per-account async state: cached values (with expiry) and in-flight load
/// registrations, plus rate-limit bookkeeping for the timeout warning and a
/// monotonic counter used by the generation guard in [`AsyncSecretStore::spawn_load`].
struct AsyncState {
    entries: HashMap<String, Entry>,
    last_warn: HashMap<String, Instant>,
    /// Monotonic counter dispensing a unique `load_id` per in-flight load, so a
    /// delayed spawn_blocking result can tell whether it still owns the slot.
    next_load_id: u64,
}

/// One per-account cache slot: either an in-flight load that later resolvers
/// can wait on, or a resolved value valid until `expires_at`.
enum Entry {
    /// A blocking load is in progress. `rx` receives `Some(value)` when the
    /// spawn_blocking task finishes; `started_at` lets late waiters shrink their
    /// remaining budget so the effective wait per caller stays bounded.
    /// `load_id` uniquely tags this in-flight load so a delayed completion can
    /// detect an intervening store/delete/newer load and refuse to clobber the
    /// fresher slot.
    InFlight {
        rx: watch::Receiver<Option<Option<String>>>,
        started_at: Instant,
        load_id: u64,
    },
    /// A resolved value cached in-process; served without touching the keychain
    /// until `expires_at`.
    Cached {
        value: Option<String>,
        expires_at: Instant,
    },
}

impl AsyncSecretStore {
    /// Wrap `inner` with the production timeout/TTL defaults.
    pub(crate) fn new(inner: Arc<dyn SecretStore>) -> Self {
        Self::with_timings(
            inner,
            DEFAULT_LOAD_TIMEOUT,
            DEFAULT_WRITE_TIMEOUT,
            DEFAULT_CACHE_TTL,
            DEFAULT_WARN_INTERVAL,
        )
    }

    /// Wrap `inner` with explicit timings — used by tests to compress the
    /// timeout / TTL windows so a full round-trip fits in a test budget.
    pub(crate) fn with_timings(
        inner: Arc<dyn SecretStore>,
        load_timeout: Duration,
        write_timeout: Duration,
        cache_ttl: Duration,
        warn_interval: Duration,
    ) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(AsyncState {
                entries: HashMap::new(),
                last_warn: HashMap::new(),
                next_load_id: 0,
            })),
            load_timeout,
            write_timeout,
            cache_ttl,
            warn_interval,
        }
    }

    /// Read the secret for `account`, returning `None` on absent / timeout /
    /// backing-error. Concurrent callers for the same `account` are coalesced
    /// into a single spawn_blocking; a cached result is served without touching
    /// the keychain until it expires.
    pub(crate) async fn load(&self, account: &str) -> Option<String> {
        let action = {
            let mut state = self.state.lock().unwrap();
            match state.entries.get(account) {
                Some(Entry::Cached { value, expires_at }) if *expires_at > Instant::now() => {
                    return value.clone();
                }
                Some(Entry::InFlight { rx, started_at, .. }) => LoadAction::Wait {
                    rx: rx.clone(),
                    started_at: *started_at,
                },
                _ => {
                    let (tx, rx) = watch::channel::<Option<Option<String>>>(None);
                    let started_at = Instant::now();
                    let load_id = state.next_load_id;
                    state.next_load_id = state.next_load_id.wrapping_add(1);
                    state.entries.insert(
                        account.to_string(),
                        Entry::InFlight {
                            rx: rx.clone(),
                            started_at,
                            load_id,
                        },
                    );
                    LoadAction::Start { tx, rx, load_id }
                }
            }
        };
        match action {
            LoadAction::Wait { mut rx, started_at } => {
                let remaining = self.load_timeout.saturating_sub(started_at.elapsed());
                self.await_load(account, &mut rx, remaining).await
            }
            LoadAction::Start {
                tx,
                mut rx,
                load_id,
            } => {
                self.spawn_load(account.to_string(), tx, load_id);
                self.await_load(account, &mut rx, self.load_timeout).await
            }
        }
    }

    /// Persist `value` for `account`. Runs the blocking write off the async
    /// runtime with a bounded timeout, then refreshes the cache slot so
    /// subsequent `load` calls observe the new value without re-hitting the
    /// keychain. Timeouts / backing errors surface as [`Error::Internal`].
    pub(crate) async fn store(&self, account: &str, value: &str) -> Result<()> {
        let inner = self.inner.clone();
        let account_owned = account.to_string();
        let value_owned = value.to_string();
        let handle = tokio::task::spawn_blocking(move || inner.store(&account_owned, &value_owned));
        match timeout(self.write_timeout, handle).await {
            Ok(Ok(Ok(()))) => {
                self.set_cached(account, Some(value.to_string()));
                Ok(())
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join_err)) => Err(Error::Internal(format!(
                "keychain write task panicked: {join_err}"
            ))),
            Err(_) => {
                self.warn_timeout(account, "keychain write timed out");
                Err(Error::Internal(format!(
                    "keychain write timed out for {account}"
                )))
            }
        }
    }

    /// Delete the stored secret for `account`. Runs the blocking delete off the
    /// async runtime with a bounded timeout, then updates the cache to reflect
    /// absence. Absent secrets are idempotent successes per [`SecretStore`].
    pub(crate) async fn delete(&self, account: &str) -> Result<()> {
        let inner = self.inner.clone();
        let account_owned = account.to_string();
        let handle = tokio::task::spawn_blocking(move || inner.delete(&account_owned));
        match timeout(self.write_timeout, handle).await {
            Ok(Ok(Ok(()))) => {
                self.set_cached(account, None);
                Ok(())
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join_err)) => Err(Error::Internal(format!(
                "keychain delete task panicked: {join_err}"
            ))),
            Err(_) => {
                self.warn_timeout(account, "keychain delete timed out");
                Err(Error::Internal(format!(
                    "keychain delete timed out for {account}"
                )))
            }
        }
    }

    /// Kick off the blocking load for `account`, publishing the result via `tx`
    /// and swapping the InFlight slot for a Cached one so subsequent callers
    /// short-circuit. Runs to completion even after every awaiting caller has
    /// timed out — that's the point: only ONE blocking-pool thread per account.
    /// The `load_id` generation guard ensures a delayed completion does NOT
    /// overwrite a slot that an intervening `store` / `delete` / newer load
    /// already refreshed: the write only happens if the slot is still the
    /// InFlight tagged with `load_id`.
    fn spawn_load(&self, account: String, tx: watch::Sender<Option<Option<String>>>, load_id: u64) {
        let inner = self.inner.clone();
        let state = self.state.clone();
        let ttl = self.cache_ttl;
        tokio::spawn(async move {
            let load_account = account.clone();
            let result: Option<String> =
                tokio::task::spawn_blocking(move || inner.load(&load_account))
                    .await
                    .unwrap_or_default();
            {
                let mut guard = state.lock().unwrap();
                let still_ours = matches!(
                    guard.entries.get(&account),
                    Some(Entry::InFlight { load_id: id, .. }) if *id == load_id,
                );
                if still_ours {
                    guard.entries.insert(
                        account.clone(),
                        Entry::Cached {
                            value: result.clone(),
                            expires_at: Instant::now() + ttl,
                        },
                    );
                }
            }
            let _ = tx.send(Some(result));
        });
    }

    /// Wait up to `remaining` for the in-flight load to publish a value; on
    /// timeout return `None` (the current caller gives up but the underlying
    /// blocking task keeps running and will populate the cache when it
    /// eventually completes).
    async fn await_load(
        &self,
        account: &str,
        rx: &mut watch::Receiver<Option<Option<String>>>,
        remaining: Duration,
    ) -> Option<String> {
        if let Some(v) = rx.borrow().clone() {
            return v;
        }
        if remaining.is_zero() {
            self.warn_timeout(account, "keychain load timed out");
            return None;
        }
        let start = Instant::now();
        loop {
            let left = remaining.saturating_sub(start.elapsed());
            if left.is_zero() {
                self.warn_timeout(account, "keychain load timed out");
                return None;
            }
            match timeout(left, rx.changed()).await {
                Ok(Ok(())) => {
                    if let Some(v) = rx.borrow().clone() {
                        return v;
                    }
                }
                Ok(Err(_)) => return None,
                Err(_) => {
                    self.warn_timeout(account, "keychain load timed out");
                    return None;
                }
            }
        }
    }

    /// Replace the cache slot for `account` with a fresh Cached entry (used by
    /// writes to reflect the just-persisted state, so a follow-up load doesn't
    /// have to hit the keychain again).
    fn set_cached(&self, account: &str, value: Option<String>) {
        let mut guard = self.state.lock().unwrap();
        guard.entries.insert(
            account.to_string(),
            Entry::Cached {
                value,
                expires_at: Instant::now() + self.cache_ttl,
            },
        );
    }

    /// Emit a rate-limited WARN naming `account` when a keychain call times
    /// out, so a wedged Keychain surfaces in the daemon log without spamming.
    fn warn_timeout(&self, account: &str, msg: &str) {
        let should = {
            let mut guard = self.state.lock().unwrap();
            let now = Instant::now();
            match guard.last_warn.get(account) {
                Some(prev) if now.duration_since(*prev) < self.warn_interval => false,
                _ => {
                    guard.last_warn.insert(account.to_string(), now);
                    true
                }
            }
        };
        if should {
            tracing::warn!(account = %account, "{msg}");
        }
    }
}

/// Internal choice returned by the entry-map probe in [`AsyncSecretStore::load`].
enum LoadAction {
    /// A load is already in flight; wait on the existing receiver.
    Wait {
        rx: watch::Receiver<Option<Option<String>>>,
        started_at: Instant,
    },
    /// No load in flight; the current caller registered a new InFlight slot
    /// (tagged with `load_id`) and now owns the spawn_blocking / notify
    /// responsibility.
    Start {
        tx: watch::Sender<Option<Option<String>>>,
        rx: watch::Receiver<Option<Option<String>>>,
        load_id: u64,
    },
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

/// Stateless executor for the `settings.*` namespace over a [`Store`] +
/// [`AsyncSecretStore`]. Construct one per call from the long-lived `Services`.
pub(crate) struct SettingsService<'a> {
    store: &'a Store,
    secrets: &'a AsyncSecretStore,
}

impl<'a> SettingsService<'a> {
    pub(crate) fn new(store: &'a Store, secrets: &'a AsyncSecretStore) -> Self {
        Self { store, secrets }
    }

    /// The current value for a definition: sensitive settings are **redacted**
    /// (placeholder when present, `null` when absent — never plaintext);
    /// non-secret settings come from the DB, falling back to the default.
    async fn current_value(&self, def: &SettingDefinition) -> Value {
        if def.sensitive {
            if self.secrets.load(def.path).await.is_some() {
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
                self.secrets.store(def.path, &secret_value).await?;
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
            self.secrets.delete(def.path).await?;
        } else {
            self.store.delete_setting(def.path).await?;
        }
        let value = self.current_value(&def).await;
        Ok(json!({ "path": def.path, "value": value }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// A `SecretStore` whose `load` blocks forever (well past any test budget).
    /// Used to prove that the async wrapper's timeout + single-flight actually
    /// keep the async runtime free when the keychain wedges.
    #[derive(Default)]
    struct BlockingSecretStore {
        load_calls: AtomicUsize,
    }

    impl SecretStore for BlockingSecretStore {
        fn load(&self, _account: &str) -> Option<String> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            // Long enough to outlive the wrapper's compressed test timeout so
            // callers observe the timeout branch, but short enough that the
            // tokio runtime's blocking-pool shutdown at end-of-test doesn't
            // hold up the whole test binary.
            thread::sleep(Duration::from_millis(500));
            None
        }
        fn store(&self, _account: &str, _value: &str) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _account: &str) -> Result<()> {
            Ok(())
        }
    }

    /// A `SecretStore` that counts `load` calls so tests can verify cache hits.
    #[derive(Default)]
    struct CountingSecretStore {
        load_calls: AtomicUsize,
        value: Mutex<Option<String>>,
    }

    impl SecretStore for CountingSecretStore {
        fn load(&self, _account: &str) -> Option<String> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            self.value.lock().unwrap().clone()
        }
        fn store(&self, _account: &str, value: &str) -> Result<()> {
            *self.value.lock().unwrap() = Some(value.to_string());
            Ok(())
        }
        fn delete(&self, _account: &str) -> Result<()> {
            *self.value.lock().unwrap() = None;
            Ok(())
        }
    }

    /// A `SecretStore` whose `load` waits on a barrier so tests can hold ONE
    /// call in flight while probing single-flight semantics.
    struct BarrierSecretStore {
        load_calls: AtomicUsize,
        barrier: Arc<Barrier>,
    }

    impl SecretStore for BarrierSecretStore {
        fn load(&self, _account: &str) -> Option<String> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait();
            Some("released".to_string())
        }
        fn store(&self, _account: &str, _value: &str) -> Result<()> {
            Ok(())
        }
        fn delete(&self, _account: &str) -> Result<()> {
            Ok(())
        }
    }

    /// A wedged keychain: `load` returns `None` within the timeout and the
    /// caller sees an unset setting instead of hanging forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_returns_none_on_timeout() {
        let inner: Arc<dyn SecretStore> = Arc::new(BlockingSecretStore::default());
        let store = AsyncSecretStore::with_timings(
            inner,
            Duration::from_millis(50),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let start = Instant::now();
        let v = store.load("acct").await;
        let elapsed = start.elapsed();
        assert!(v.is_none(), "wedged keychain must resolve to None");
        assert!(
            elapsed < Duration::from_millis(500),
            "load must return within its deadline, took {elapsed:?}"
        );
    }

    /// Concurrent callers for the same account share the single in-flight
    /// keychain call — a wedged keychain occupies one blocking-pool thread
    /// total, not one per caller.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_loads_are_single_flight() {
        let sync = Arc::new(BlockingSecretStore::default());
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = Arc::new(AsyncSecretStore::with_timings(
            inner,
            Duration::from_millis(50),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            handles.push(tokio::spawn(async move { s.load("acct").await }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_none());
        }
        assert_eq!(
            sync.load_calls.load(Ordering::SeqCst),
            1,
            "single-flight must coalesce concurrent callers into ONE keychain load"
        );
    }

    /// A late arrival for an in-flight load waits on the existing keychain
    /// call rather than spawning a second one, even after the initial waiters
    /// have already timed out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn late_caller_reuses_in_flight_load() {
        let barrier = Arc::new(Barrier::new(2));
        let sync = Arc::new(BarrierSecretStore {
            load_calls: AtomicUsize::new(0),
            barrier: barrier.clone(),
        });
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = Arc::new(AsyncSecretStore::with_timings(
            inner,
            Duration::from_millis(50),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        // First caller: starts the in-flight load, then times out (barrier
        // is still holding the sync `load` inside the blocking pool).
        assert!(store.load("acct").await.is_none());
        // Second caller arrives while the first blocking call is still parked
        // on the barrier — it must NOT spawn a fresh load.
        assert!(store.load("acct").await.is_none());
        // Release the sync `load`; the completion task caches "released".
        barrier.wait();
        for _ in 0..50 {
            if store.load("acct").await == Some("released".to_string()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            sync.load_calls.load(Ordering::SeqCst),
            1,
            "in-flight load must be reused by later arrivals; got {} calls",
            sync.load_calls.load(Ordering::SeqCst)
        );
    }

    /// Once a load completes, follow-up calls within the TTL are served from
    /// the in-process cache and the backing keychain is not touched again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_hit_skips_keychain_within_ttl() {
        let sync = Arc::new(CountingSecretStore::default());
        *sync.value.lock().unwrap() = Some("tok".to_string());
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = AsyncSecretStore::with_timings(
            inner,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(store.load("acct").await, Some("tok".to_string()));
        assert_eq!(store.load("acct").await, Some("tok".to_string()));
        assert_eq!(store.load("acct").await, Some("tok".to_string()));
        assert_eq!(
            sync.load_calls.load(Ordering::SeqCst),
            1,
            "cache must serve repeat reads without re-hitting the keychain"
        );
    }

    /// A successful write updates the cache so a follow-up read returns the
    /// just-persisted value without another keychain load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_updates_cache() {
        let sync = Arc::new(CountingSecretStore::default());
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = AsyncSecretStore::new(inner);
        store.store("acct", "new-value").await.unwrap();
        assert_eq!(store.load("acct").await, Some("new-value".to_string()));
        assert_eq!(
            sync.load_calls.load(Ordering::SeqCst),
            0,
            "post-write cache should serve the read without hitting the keychain"
        );
    }

    /// A successful `delete` refreshes the cache so a follow-up read reports
    /// the secret as absent without another keychain load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_updates_cache() {
        let sync = Arc::new(CountingSecretStore::default());
        *sync.value.lock().unwrap() = Some("tok".to_string());
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = AsyncSecretStore::new(inner);
        store.delete("acct").await.unwrap();
        assert_eq!(store.load("acct").await, None);
        assert_eq!(
            sync.load_calls.load(Ordering::SeqCst),
            0,
            "post-delete cache should serve the read without hitting the keychain"
        );
    }

    /// A `store` that lands while a slow load is still parked in the blocking
    /// pool must win: the delayed load result must not clobber the fresher
    /// cache entry. Guards against the pre-generation-counter race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn intervening_store_wins_over_slow_load() {
        let barrier = Arc::new(Barrier::new(2));
        let sync = Arc::new(BarrierSecretStore {
            load_calls: AtomicUsize::new(0),
            barrier: barrier.clone(),
        });
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = Arc::new(AsyncSecretStore::with_timings(
            inner,
            Duration::from_millis(50),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        // Start a load; the caller times out but the sync `load` is still
        // parked on the barrier inside the blocking pool.
        assert!(store.load("acct").await.is_none());
        // Fresh write lands while the slow load is still pending.
        store.store("acct", "new-value").await.unwrap();
        // Release the sync `load`; its completion task must refuse to clobber
        // the fresher Cached slot (the load_id no longer matches).
        barrier.wait();
        // Give the completion task time to run and observe the mismatch.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            store.load("acct").await,
            Some("new-value".to_string()),
            "intervening store must win against a delayed load result"
        );
    }

    /// A `delete` that lands while a slow load is still parked must win: the
    /// delayed load result must not resurrect the (now-absent) value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn intervening_delete_wins_over_slow_load() {
        let barrier = Arc::new(Barrier::new(2));
        let sync = Arc::new(BarrierSecretStore {
            load_calls: AtomicUsize::new(0),
            barrier: barrier.clone(),
        });
        let inner: Arc<dyn SecretStore> = sync.clone();
        let store = Arc::new(AsyncSecretStore::with_timings(
            inner,
            Duration::from_millis(50),
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        assert!(store.load("acct").await.is_none());
        store.delete("acct").await.unwrap();
        barrier.wait();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            store.load("acct").await,
            None,
            "intervening delete must win against a delayed load result"
        );
    }
}
