//! BE-owned settings store + `settings.*` business logic (§9.8, PROTOCOL §5.12).
//!
//! Owns the [`SettingDefinition`] schema (groups A + B of §9.8), type/enum/
//! min/max validation, and the redaction rule for **sensitive** settings.
//! Non-secret **TOML-backed** values (the [`KNOWN_PATHS`] catalog subset) are
//! read from and written to the layered [`SettingsRegistry`] (`config.toml`);
//! the remaining non-secret keys (SQLite-backed machine-state blobs such as
//! `workspace.changeHistory` / `permissions.rules`) persist in the `settings`
//! table (`intent-store`). Sensitive values (`mcp.servers`,
//! `server.auth.token`, `sourceControl.github.token`, `linear.token`,
//! `accounts.sentry.token`, `ai.apiToken`) live in the file-backed secrets
//! store (`~/intent/secrets.json`, via [`intent_core::FileSecretStore`])
//! behind the [`SecretStore`] seam and are **never** returned in plaintext
//! over the wire — list/get redact them to presence/placeholder only, and
//! `server.auth.token` is read-only. `workspace.sshKeyPath` is a plain
//! non-secret **path** setting (the real secret is the key file on disk); the
//! FE `git`-env consumer must read the value back verbatim.
//!
//! Internal readers of TOML-backed keys (e.g. [`branch_prefix`],
//! [`max_concurrent_agents`]) consume the effective typed [`SettingsFile`]
//! from the registry snapshot (`Services::effective_settings`); the SQLite
//! `settings` table only persists the machine-state blobs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::settings_file::SettingsFile;
use intent_core::{Error, Result};
use serde_json::{json, Map, Value};
use tokio::sync::watch;
use tokio::time::timeout;

use intent_store::Store;

use crate::settings_registry::{SettingOrigin, SettingsRegistry, KNOWN_PATHS};

/// Placeholder returned for a sensitive setting that **has** a stored value, so
/// the wire conveys presence without ever leaking the plaintext (§9.8).
pub(crate) const REDACTED_PLACEHOLDER: &str = "********";

/// Abstraction over secret persistence (the sensitive-setting analog of the
/// transport's `TokenStore`). Production uses the file-backed
/// [`intent_core::FileSecretStore`]; tests inject [`InMemorySecretStore`] so
/// they never touch the real secrets file.
pub trait SecretStore: Send + Sync {
    /// Return the stored secret for `account`. `Ok(None)` when confirmed absent;
    /// `Err` on timeout / backing-store failure so snapshot capture can fail closed.
    fn load(&self, account: &str) -> Result<Option<String>>;
    /// Persist `value` for `account`, replacing any existing secret.
    fn store(&self, account: &str, value: &str) -> Result<()>;
    /// Delete the secret for `account`; absence is an idempotent success.
    fn delete(&self, account: &str) -> Result<()>;
}

/// File-backed production default: delegate to the shared
/// [`intent_core::FileSecretStore`] (`~/intent/secrets.json`), whose accounts
/// are the sensitive setting paths (account = setting path).
impl SecretStore for intent_core::FileSecretStore {
    fn load(&self, account: &str) -> Result<Option<String>> {
        intent_core::FileSecretStore::load(self, account)
    }

    fn store(&self, account: &str, value: &str) -> Result<()> {
        intent_core::FileSecretStore::store(self, account, value)
    }

    fn delete(&self, account: &str) -> Result<()> {
        intent_core::FileSecretStore::delete(self, account)
    }
}

/// In-memory [`SecretStore`] for tests — never touches the real keychain.
#[derive(Debug, Default, Clone)]
pub struct InMemorySecretStore {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl SecretStore for InMemorySecretStore {
    fn load(&self, account: &str) -> Result<Option<String>> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .get(account)
            .filter(|v| !v.is_empty())
            .cloned())
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
/// setting is reported as unset. A stuck backing store (historically, a
/// pending macOS keychain auth prompt) would otherwise block the caller — and
/// an entire tokio worker — indefinitely.
const DEFAULT_LOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Default bounded wait for a secret **write** (`store` / `delete`). Longer
/// than the read budget so a slow disk never spuriously fails a persist; still
/// must not block the runtime forever.
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a load result (present or absent) is served from the in-process
/// cache before the next call re-consults the backing store. Keeps
/// `settings.list` cheap on the FE mount path without turning secret changes
/// into a long propagation window.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);
/// Rate-limit window per account for the `secret-store load timed out` warning
/// so a wedged backing store doesn't drown the daemon log.
const DEFAULT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Async, single-flight, TTL-cached wrapper around a synchronous [`SecretStore`]
/// so blocking secret-store calls (file I/O) never wedge
/// the tokio runtime. Every backing call runs on the blocking pool via
/// [`tokio::task::spawn_blocking`]; reads are bounded by a short timeout and
/// coalesced per account via single-flight so a hung backing store occupies at
/// most one blocking-pool thread total (not one per request). Cache entries are
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
    /// A blocking load is in progress. `rx` receives `Some(result)` when the
    /// spawn_blocking task finishes; `started_at` lets late waiters shrink their
    /// remaining budget so the effective wait per caller stays bounded.
    /// `load_id` uniquely tags this in-flight load so a delayed completion can
    /// detect an intervening store/delete/newer load and refuse to clobber the
    /// fresher slot.
    InFlight {
        rx: watch::Receiver<Option<Result<Option<String>>>>,
        started_at: Instant,
        load_id: u64,
    },
    /// A resolved value cached in-process; served without touching the keychain
    /// until `expires_at`. Only successful loads (`Ok`) are cached; errors never
    /// cache so the next call re-attempts the backing store.
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

    /// Read the secret for `account`. `Ok(None)` when confirmed absent;
    /// `Err` on timeout / backing-error. Concurrent callers for the same `account`
    /// are coalesced into a single spawn_blocking; a cached result is served
    /// without touching the backing store until it expires.
    pub(crate) async fn load(&self, account: &str) -> Result<Option<String>> {
        let action = {
            let mut state = self.state.lock().unwrap();
            match state.entries.get(account) {
                Some(Entry::Cached { value, expires_at }) if *expires_at > Instant::now() => {
                    return Ok(value.clone());
                }
                Some(Entry::InFlight { rx, started_at, .. }) => LoadAction::Wait {
                    rx: rx.clone(),
                    started_at: *started_at,
                },
                _ => {
                    let (tx, rx) = watch::channel::<Option<Result<Option<String>>>>(None);
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
    /// backing store. Timeouts / backing errors surface as [`Error::Internal`].
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
                "secret-store write task panicked: {join_err}"
            ))),
            Err(_) => {
                self.warn_timeout(account, "secret-store write timed out");
                Err(Error::Internal(format!(
                    "secret-store write timed out for {account}"
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
                "secret-store delete task panicked: {join_err}"
            ))),
            Err(_) => {
                self.warn_timeout(account, "secret-store delete timed out");
                Err(Error::Internal(format!(
                    "secret-store delete timed out for {account}"
                )))
            }
        }
    }

    /// Kick off the blocking load for `account`, publishing the result via `tx`
    /// and swapping the InFlight slot for a Cached one if successful (errors never
    /// cache, so the next call re-attempts). Runs to completion even after every
    /// awaiting caller has timed out — that's the point: only ONE blocking-pool
    /// thread per account. The `load_id` generation guard ensures a delayed
    /// completion does NOT overwrite a slot that an intervening `store` / `delete` /
    /// newer load already refreshed: the write only happens if the slot is still
    /// the InFlight tagged with `load_id`.
    fn spawn_load(
        &self,
        account: String,
        tx: watch::Sender<Option<Result<Option<String>>>>,
        load_id: u64,
    ) {
        let inner = self.inner.clone();
        let state = self.state.clone();
        let ttl = self.cache_ttl;
        tokio::spawn(async move {
            let load_account = account.clone();
            let result: Result<Option<String>> =
                match tokio::task::spawn_blocking(move || inner.load(&load_account)).await {
                    Ok(r) => r,
                    Err(join_err) => Err(Error::Internal(format!(
                        "secret-store load task panicked: {join_err}"
                    ))),
                };
            {
                let mut guard = state.lock().unwrap();
                let still_ours = matches!(
                    guard.entries.get(&account),
                    Some(Entry::InFlight { load_id: id, .. }) if *id == load_id,
                );
                if still_ours {
                    // Only cache successful results; errors force retry next call.
                    if let Ok(value) = &result {
                        guard.entries.insert(
                            account.clone(),
                            Entry::Cached {
                                value: value.clone(),
                                expires_at: Instant::now() + ttl,
                            },
                        );
                    } else {
                        // Remove the InFlight entry so the next call creates a fresh one.
                        guard.entries.remove(&account);
                    }
                }
            }
            let _ = tx.send(Some(result));
        });
    }

    /// Wait up to `remaining` for the in-flight load to publish a result; on
    /// timeout return `Err` (the current caller gives up but the underlying
    /// blocking task keeps running and will populate the cache when it
    /// eventually completes).
    async fn await_load(
        &self,
        account: &str,
        rx: &mut watch::Receiver<Option<Result<Option<String>>>>,
        remaining: Duration,
    ) -> Result<Option<String>> {
        if let Some(v) = rx.borrow().as_ref() {
            return match v {
                Ok(opt) => Ok(opt.clone()),
                Err(e) => Err(Error::Internal(e.to_string())),
            };
        }
        if remaining.is_zero() {
            self.warn_timeout(account, "secret-store load timed out");
            return Err(Error::Internal(format!(
                "secret-store load timed out for {account}"
            )));
        }
        let start = Instant::now();
        loop {
            let left = remaining.saturating_sub(start.elapsed());
            if left.is_zero() {
                self.warn_timeout(account, "secret-store load timed out");
                return Err(Error::Internal(format!(
                    "secret-store load timed out for {account}"
                )));
            }
            match timeout(left, rx.changed()).await {
                Ok(Ok(())) => {
                    if let Some(v) = rx.borrow().as_ref() {
                        return match v {
                            Ok(opt) => Ok(opt.clone()),
                            Err(e) => Err(Error::Internal(e.to_string())),
                        };
                    }
                }
                Ok(Err(_)) => {
                    return Err(Error::Internal(
                        "secret-store load watch channel closed".to_string(),
                    ))
                }
                Err(_) => {
                    self.warn_timeout(account, "secret-store load timed out");
                    return Err(Error::Internal(format!(
                        "secret-store load timed out for {account}"
                    )));
                }
            }
        }
    }

    /// Replace the cache slot for `account` with a fresh Cached entry (used by
    /// writes to reflect the just-persisted state, so a follow-up load doesn't
    /// have to hit the backing store again).
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

    /// Emit a rate-limited WARN naming `account` when a secret-store call times
    /// out, so a wedged backing store surfaces in the daemon log without spamming.
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
        rx: watch::Receiver<Option<Result<Option<String>>>>,
        started_at: Instant,
    },
    /// No load in flight; the current caller registered a new InFlight slot
    /// (tagged with `load_id`) and now owns the spawn_blocking / notify
    /// responsibility.
    Start {
        tx: watch::Sender<Option<Result<Option<String>>>>,
        rx: watch::Receiver<Option<Result<Option<String>>>>,
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
        boolean(
            "workspace.cowIsolation",
            "Copy-on-Write Agent Isolation",
            "Enable CoW agent sandboxing for direct-mode delegations (requires CoW filesystem support)",
            "workspace",
            false,
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
            "External MCP server configs (secrets in the secrets file)",
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
        boolean(
            "rtk.enabled",
            "RTK enabled",
            "Enable RTK compressed CLI output mode in agent prompts",
            "tools",
            false,
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
        number(
            "server.wsApi.port",
            "WSS API port",
            "TCP port for the WSS listener",
            "server",
            Some(1024.0),
            Some(65535.0),
            5181.0,
        ),
        boolean(
            "server.wsApi.enabled",
            "WS API enabled",
            "Enable the TCP/WSS listener at runtime",
            "server",
            false,
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
            &["auto", "env", "gh-cli", "explicit"],
            "auto",
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
        string(
            "sourceControl.github.oauthClientId",
            "GitHub OAuth client ID",
            "OAuth App client id for the device flow (public, not a secret)",
            "sourceControl",
            Some(intent_core::settings_file::DEFAULT_GITHUB_OAUTH_CLIENT_ID),
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
        // --- Group A: workspace initializer form state ------------------------
        // Persisted home-screen workspace-initializer form state: last selected
        // repository, recent repositories, branch-by-repo, form drafts.
        object(
            "workspaceInitializer.state",
            "Workspace initializer state",
            "Persisted home-screen workspace-initializer form state (last selected repo, recent repos, branch-by-repo, form drafts)",
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
            "Concurrent agent session cap (0 = auto based on system RAM; changes apply on daemon restart)",
            "agents",
            Some(0.0),
            Some(200.0), // Upper bound to prevent resource exhaustion
            0.0,
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
        number(
            "events.streamRetentionHours",
            "Event stream retention hours",
            "Hours ephemeral events are retained before the retention sweep (0 disables)",
            "events",
            Some(0.0),
            None,
            72.0,
        ),
    ]
}

/// The effective `workspace.branchPrefix` (default empty) — prepended to
/// auto-generated workspace branch names (TS `getBranchPrefix` parity).
pub(crate) fn branch_prefix(settings: &SettingsFile) -> String {
    settings.workspace.branch_prefix.clone().unwrap_or_default()
}

/// The effective `agents.maxConcurrent` setting: a positive integer sets an
/// explicit cap; 0 (the default) means "auto" (RAM-based cap via
/// `default_process_cap`). The typed schema already rejects negative /
/// out-of-range / garbled values.
pub fn max_concurrent_agents(settings: &SettingsFile) -> Option<usize> {
    let n = settings.agents.max_concurrent;
    (n > 0).then_some(n as usize)
}

/// Normalize a registry-read value for the wire: `Number`-typed settings are
/// reported as floats (`5181.0`), matching the numeric shape the catalog
/// defaults always had, so clients see one shape regardless of origin.
pub(crate) fn wire_value(def: &SettingDefinition, value: Value) -> Value {
    match def.ty {
        SettingType::Number { .. } => match value.as_f64() {
            Some(n) => json!(n),
            None => value,
        },
        _ => value,
    }
}

/// Coerce a validated wire value for the typed registry schema: whole-valued
/// floats become integers so `u16`/`u32` fields (e.g. `server.port`) accept
/// the `5182.0` shape JSON clients commonly send. Float fields re-accept
/// integers via the schema's lenient deserializer.
fn registry_value(def: &SettingDefinition, value: &Value) -> Value {
    if let SettingType::Number { .. } = def.ty {
        if let Some(n) = value.as_f64() {
            if n.is_finite() && n.fract() == 0.0 && n.abs() <= i64::MAX as f64 {
                return json!(n as i64);
            }
        }
    }
    value.clone()
}

/// Stateless executor for the `settings.*` namespace over a [`Store`] +
/// [`AsyncSecretStore`] + optional [`SettingsRegistry`]. Construct one per
/// call from the long-lived `Services`. When the registry is wired (the
/// production composition root always wires it), the TOML-backed
/// [`KNOWN_PATHS`] keys read from and write through the registry
/// (`config.toml`); without it, every non-secret key keeps the legacy
/// SQLite path (read-only test wiring).
pub(crate) struct SettingsService<'a> {
    store: &'a Store,
    secrets: &'a AsyncSecretStore,
    registry: Option<&'a SettingsRegistry>,
}

impl<'a> SettingsService<'a> {
    pub(crate) fn new(
        store: &'a Store,
        secrets: &'a AsyncSecretStore,
        registry: Option<&'a SettingsRegistry>,
    ) -> Self {
        Self {
            store,
            secrets,
            registry,
        }
    }

    /// The registry serving `path`, when it is a TOML-backed key and the
    /// registry is wired. `None` falls back to the legacy SQLite path.
    fn registry_for(&self, path: &str) -> Option<&'a SettingsRegistry> {
        self.registry.filter(|_| KNOWN_PATHS.contains(&path))
    }

    /// The wire `origin` for a TOML-backed key (`default` | `file` | `flag`),
    /// or `None` for secrets / SQLite-backed keys (no origin on the wire).
    fn origin_for(&self, path: &str) -> Option<&'static str> {
        self.registry_for(path)
            .and_then(|reg| reg.origin(path))
            .map(|o| o.as_str())
    }

    /// The current value for a **non-secret** definition: TOML-backed keys
    /// read the registry's effective snapshot (defaults ⊕ file ⊕ pins);
    /// SQLite-backed keys read the DB, falling back to the default.
    async fn non_secret_value(&self, def: &SettingDefinition) -> Value {
        if let Some(reg) = self.registry_for(def.path) {
            return wire_value(def, reg.get(def.path).unwrap_or(Value::Null));
        }
        match self.store.get_setting(def.path).await {
            Ok(Some(raw)) => serde_json::from_str(&raw).unwrap_or(Value::Null),
            _ => def.default_value.clone().unwrap_or(Value::Null),
        }
    }

    /// The current value for a definition: sensitive settings are **redacted**
    /// (placeholder when present, `null` when absent — never plaintext);
    /// non-secret settings come from the registry/DB, falling back to the
    /// default. Best-effort: load errors (timeout/backing failure) treated as
    /// absent for display.
    async fn current_value(&self, def: &SettingDefinition) -> Value {
        if def.sensitive {
            match self.secrets.load(def.path).await {
                Ok(Some(_)) => json!(REDACTED_PLACEHOLDER),
                Ok(None) | Err(_) => Value::Null,
            }
        } else {
            self.non_secret_value(def).await
        }
    }

    /// `settings.list` → `{ settings: SettingDefinitionWithValue[] }` (§5.12).
    ///
    /// Sensitive-setting presence probes go through [`AsyncSecretStore`], whose
    /// per-account bounded timeout, single-flight, and TTL cache mean each
    /// probe returns within one secret-store budget. The probes are polled via
    /// `tokio::select!` on all `load` futures concurrently through a `join!`
    /// analog so a stalled account never blocks the others.
    pub(crate) async fn list(&self) -> Result<Value> {
        let defs = definitions();
        let sensitive: Vec<&'static str> = defs
            .iter()
            .filter(|d| d.sensitive)
            .map(|d| d.path)
            .collect();
        // Drive every load future concurrently on the current task: a single
        // stalled account never blocks the others because `join_all_pinned`
        // polls every future on each wake-up. Best-effort: errors treated as absent.
        type LoadFuture<'a> = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<String>>> + Send + 'a>,
        >;
        let futs: Vec<LoadFuture<'_>> = sensitive
            .iter()
            .map(|path| {
                let fut = self.secrets.load(path);
                Box::pin(fut) as LoadFuture<'_>
            })
            .collect();
        let results = join_all_pinned(futs).await;
        let mut presence: HashMap<&'static str, bool> = HashMap::with_capacity(sensitive.len());
        for (path, result) in sensitive.into_iter().zip(results) {
            presence.insert(path, result.ok().flatten().is_some());
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
                self.non_secret_value(def).await
            };
            let mut obj = def.definition_json();
            if let Some(map) = obj.as_object_mut() {
                map.insert("value".into(), value);
                if let Some(origin) = self.origin_for(def.path) {
                    map.insert("origin".into(), json!(origin));
                }
            }
            out.push(obj);
        }
        Ok(json!({ "settings": out }))
    }

    /// `settings.get` → `{ path, value, definition }` (+ `origin` for
    /// TOML-backed keys: `default` | `file` | `flag`); unknown path → `-32602`.
    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        let def = find_definition(path)
            .ok_or_else(|| Error::InvalidParams(format!("unknown setting: {path}")))?;
        let value = self.current_value(&def).await;
        let mut out = json!({
            "path": def.path,
            "value": value,
            "definition": def.definition_json(),
        });
        if let Some(origin) = self.origin_for(def.path) {
            out["origin"] = json!(origin);
        }
        Ok(out)
    }

    /// `settings.update` — validate the whole batch first (unknown path,
    /// read-only path, or type/enum/min/max failure → `-32602`, nothing
    /// applied), then persist. TOML-backed keys go through the registry as
    /// **one atomic apply** (typed-schema + pin validation, single
    /// comment-preserving `config.toml` rewrite; a flag-pinned key rejects the
    /// whole batch with `-32602` before anything mutates) and never touch the
    /// SQLite `settings` table. Secrets and state-blob keys keep their
    /// existing stores. If a later secret/DB write in a mixed batch fails,
    /// the already-applied registry batch is compensated (prior values
    /// restored, config.toml rewritten back) so an error return never leaves
    /// a durable file change without a `settings:changed` event. Returns the
    /// **redacted** applied `{ path, value }` pairs for the response +
    /// `settings:changed` payload.
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

        // Apply the TOML-backed subset first, as one atomic registry batch:
        // unknown/pinned keys and typed-schema violations reject here with
        // `-32602` before ANY store (secret, DB, file) has been touched.
        // Capture the prior effective values so a later secret/DB failure in
        // a mixed batch can compensate (restore + rewrite config.toml back).
        let mut registry_rollback: Vec<(String, Value)> = Vec::new();
        if let Some(reg) = self.registry {
            let registry_changes: Vec<(String, Value)> = planned
                .iter()
                .filter(|(def, _)| KNOWN_PATHS.contains(&def.path))
                .map(|(def, value)| (def.path.to_string(), registry_value(def, value)))
                .collect();
            if !registry_changes.is_empty() {
                registry_rollback = registry_changes
                    .iter()
                    .map(|(path, _)| {
                        // `origin == File` means the key was explicitly in the
                        // file; otherwise roll back by removing it (Null).
                        let prior = match reg.origin(path) {
                            Some(SettingOrigin::File) => reg.get(path).unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        (path.clone(), prior)
                    })
                    .collect();
                reg.apply(&registry_changes)?;
            }
        }

        let mut applied = Vec::with_capacity(planned.len());
        for (def, value) in planned {
            let persisted = if def.sensitive {
                let secret_value = match &value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                self.secrets
                    .store(def.path, &secret_value)
                    .await
                    .map(|()| json!({ "path": def.path, "value": REDACTED_PLACEHOLDER }))
            } else if self.registry_for(def.path).is_some() {
                // Already applied via the registry batch above. Normalize the
                // echoed value so number-typed settings keep the float wire
                // shape (`5181.0`) that `settings.get`/`settings.list` report.
                Ok(json!({ "path": def.path, "value": wire_value(&def, value) }))
            } else {
                match serde_json::to_string(&value) {
                    Ok(raw) => self
                        .store
                        .set_setting(def.path, &raw)
                        .await
                        .map(|()| json!({ "path": def.path, "value": value })),
                    Err(e) => Err(Error::Internal(format!("encode setting failed: {e}"))),
                }
            };
            match persisted {
                Ok(entry) => applied.push(entry),
                Err(e) => {
                    // Compensate the registry batch: without this, the TOML
                    // subset would stay applied on disk while the caller sees
                    // an error and never emits `settings:changed`.
                    if !registry_rollback.is_empty() {
                        if let Some(reg) = self.registry {
                            if let Err(rollback_err) = reg.apply(&registry_rollback) {
                                tracing::error!(
                                    error = %rollback_err,
                                    "settings.update registry rollback failed after \
                                     secret/DB persistence error"
                                );
                            }
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(applied)
    }

    /// `settings.reset` → restore the default (remove the key from
    /// `config.toml` for TOML-backed keys / delete the persisted or secret
    /// value otherwise) and return the **redacted** `{ path, value }`;
    /// unknown path → `-32602`, flag-pinned key → `-32602`.
    pub(crate) async fn reset(&self, path: &str) -> Result<Value> {
        let def = find_definition(path)
            .ok_or_else(|| Error::InvalidParams(format!("unknown setting: {path}")))?;
        if def.sensitive {
            self.secrets.delete(def.path).await?;
        } else if let Some(reg) = self.registry_for(def.path) {
            reg.apply(&[(def.path.to_string(), Value::Null)])?;
        } else {
            self.store.delete_setting(def.path).await?;
        }
        let value = self.current_value(&def).await;
        Ok(json!({ "path": def.path, "value": value }))
    }
}

/// Poll every future concurrently on the current task and collect their
/// results in input order. Small hand-rolled combinator that avoids pulling in
/// the `futures` crate solely for `join_all` on a bounded, short-lived vector.
async fn join_all_pinned<T>(
    mut futs: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + '_>>>,
) -> Vec<T> {
    use std::task::Poll;
    let mut out: Vec<Option<T>> = (0..futs.len()).map(|_| None).collect();
    let mut remaining = futs.len();
    std::future::poll_fn(|cx| {
        for (i, slot) in out.iter_mut().enumerate() {
            if slot.is_none() {
                if let Poll::Ready(v) = futs[i].as_mut().poll(cx) {
                    *slot = Some(v);
                    remaining -= 1;
                }
            }
        }
        if remaining == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    out.into_iter().map(|s| s.unwrap()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `linear.token` must be a sensitive catalog entry so `settings.update`
    /// persists it to the shared secrets store under account `linear.token`
    /// (account = setting path) — the exact entry `intent-linear`'s token
    /// resolver reads.
    #[test]
    fn linear_token_is_a_sensitive_catalog_entry() {
        let def = find_definition("linear.token").expect("linear.token missing from catalog");
        assert_eq!(
            def.path, "linear.token",
            "secrets-store account = setting path"
        );
        assert!(
            def.sensitive,
            "must persist to the secret store + redact on the wire"
        );
        assert!(!def.read_only);
        assert_eq!(def.category, "linear");
        assert!(matches!(def.ty, SettingType::String));
        assert!(def.default_value.is_none());
    }

    /// `accounts.sentry.token` and `ai.apiToken` — the two secret catalog gaps
    /// closed for R0-4 — must be sensitive so `settings.update` persists them to
    /// the shared secrets store under account = setting path (never the DB) and
    /// every wire read (`settings.list` / `settings.get`) redacts them to a
    /// placeholder or `null` when unset.
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

    /// The six non-secret gap entries live in the catalog as opaque `Object`
    /// settings with a documented default. Each is validated by shape only;
    /// downstream consumers own the internal schema (permission rules, prompt
    /// rules, known repos, change-history bags, workspace-initializer state).
    #[test]
    fn non_secret_object_gap_entries_have_defaults() {
        for path in [
            "permissions.rules",
            "userRules",
            "workspaceRules",
            "repos.known",
            "workspace.changeHistory",
            "workspaceInitializer.state",
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
        fn load(&self, _account: &str) -> Result<Option<String>> {
            std::thread::sleep(self.hang_for);
            Ok(None)
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
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, None);

        let started = std::time::Instant::now();
        // Outer cap: single-flight + TTL cache in AsyncSecretStore bounds the
        // total latency to ONE keychain budget — well under the 30-second
        // per-call stall the store models. A small slack absorbs task-spawn
        // overhead on cold runners.
        let cap = DEFAULT_LOAD_TIMEOUT + Duration::from_secs(2);
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
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, None);

        let started = std::time::Instant::now();
        // AsyncSecretStore's write budget is DEFAULT_WRITE_TIMEOUT; use it plus
        // a small slack so the assertion measures the bounded write path.
        let cap = DEFAULT_WRITE_TIMEOUT + Duration::from_secs(2);
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

    /// `max_concurrent_agents` reads the effective `agents.maxConcurrent`:
    /// positive value → explicit override; 0 (the schema default) → `None`
    /// (fallback to `default_process_cap()`). Negative / garbled values are
    /// rejected by the registry's typed validation before ever reaching here.
    #[test]
    fn max_concurrent_agents_resolves_override_or_auto() {
        // Default (0) → None (auto).
        let mut settings = SettingsFile::default();
        assert_eq!(max_concurrent_agents(&settings), None);

        // Positive integer → Some(cap).
        settings.agents.max_concurrent = 12;
        assert_eq!(max_concurrent_agents(&settings), Some(12));
    }

    /// Q1 regression: with the registry wired (production composition), a
    /// `settings.update` of a TOML-backed key persists to `config.toml` only —
    /// it must NOT write a row to the SQLite `settings` table, which now holds
    /// machine-state blobs + dynamic non-TOML keys exclusively.
    #[tokio::test]
    async fn update_of_toml_backed_key_does_not_write_sqlite() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-nodb-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-nodb-{tag}.toml"));
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        svc.update(&json!([{ "path": "git.autoCommit", "value": false }]))
            .await
            .expect("update TOML-backed key");

        // Effective value comes from the registry (config.toml)…
        assert_eq!(registry.get("git.autoCommit"), Some(json!(false)));
        // …and the SQLite settings table stays untouched.
        assert_eq!(
            store
                .get_setting("git.autoCommit")
                .await
                .expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }
}
