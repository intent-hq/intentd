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
//! `accounts.sentry.token`) live in the file-backed secrets
//! store (`~/intent/secrets.json`, via [`intent_core::FileSecretStore`])
//! behind the [`SecretStore`] seam and are **never** returned in plaintext
//! over the wire — list/get redact them to presence/placeholder only, and
//! `server.auth.token` is read-only. `workspace.sshKeyPath` is a plain
//! non-secret **path** setting (the real secret is the key file on disk); the
//! FE `git`-env consumer must read the value back verbatim.
//!
//! Internal readers of TOML-backed keys (e.g. [`branch_prefix`],
//! [`max_concurrent_agents`]) consume the effective typed [`SettingsFile`]
//! from the registry snapshot (`Services::effective_settings`); the `SQLite`
//! `settings` table only persists the machine-state blobs. Retired keys
//! (`model.workspaceOverrides`, monorepo#1000) have no catalog entry:
//! `settings.update` tolerates-and-ignores them and
//! [`cleanup_retired_settings`] deletes their stale rows on boot.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
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

/// The retired per-workspace model override path (monorepo#1000). No catalog
/// entry remains: `settings.get`/`settings.reset` reject it as unknown, but
/// old clients still writing it via `settings.update` are tolerated-and-
/// ignored, and [`cleanup_retired_settings`] deletes the stale `SQLite` row on
/// boot.
pub(crate) const RETIRED_WORKSPACE_OVERRIDES_PATH: &str = "model.workspaceOverrides";

/// The retired `backgroundAgents.*` paths, renamed to `quickActions.*`
/// (monorepo#1729). No catalog entry remains: `settings.get`/`settings.reset`
/// reject them as unknown, but old clients still writing them via
/// `settings.update` are tolerated-and-ignored rather than failing the whole
/// batch. A stored `config.toml` `[backgroundAgents]` table is carried over to
/// the new keys once by [`migrate_quick_action_settings`].
pub(crate) const RETIRED_BACKGROUND_AGENT_PATHS: &[&str] = &[
    "backgroundAgents.defaultModel",
    "backgroundAgents.typeOverrides",
    "backgroundAgents.providerSettings",
];

/// Settings path of the user-editable transcription vocabulary (§5.12).
pub(crate) const VOICE_VOCABULARY_PATH: &str = "voice.vocabulary";

/// Abstraction over secret persistence (the sensitive-setting analog of the
/// transport's `TokenStore`). Production uses the file-backed
/// [`intent_core::FileSecretStore`]; tests inject [`InMemorySecretStore`] so
/// they never touch the real secrets file.
pub trait SecretStore: Send + Sync {
    /// Return the stored secret for `account`. `Ok(None)` when confirmed absent;
    /// `Err` on timeout / backing-store failure so snapshot capture can fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error on timeout or backing-store failure, so snapshot capture can fail closed.
    fn load(&self, account: &str) -> Result<Option<String>>;
    /// Persist `value` for `account`, replacing any existing secret.
    ///
    /// # Errors
    ///
    /// Returns an error if the secret cannot be persisted.
    fn store(&self, account: &str, value: &str) -> Result<()>;
    /// Delete the secret for `account`; absence is an idempotent success.
    ///
    /// # Errors
    ///
    /// Returns an error if the backing store fails; absence is not an error.
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
    /// delayed `spawn_blocking` result can tell whether it still owns the slot.
    next_load_id: u64,
}

/// One per-account cache slot: either an in-flight load that later resolvers
/// can wait on, or a resolved value valid until `expires_at`.
enum Entry {
    /// A blocking load is in progress. `rx` receives `Some(result)` when the
    /// `spawn_blocking` task finishes; `started_at` lets late waiters shrink their
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
    /// are coalesced into a single `spawn_blocking`; a cached result is served
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
    /// and swapping the `InFlight` slot for a Cached one if successful (errors never
    /// cache, so the next call re-attempts). Runs to completion even after every
    /// awaiting caller has timed out — that's the point: only ONE blocking-pool
    /// thread per account. The `load_id` generation guard ensures a delayed
    /// completion does NOT overwrite a slot that an intervening `store` / `delete` /
    /// newer load already refreshed: the write only happens if the slot is still
    /// the `InFlight` tagged with `load_id`.
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
    /// No load in flight; the current caller registered a new `InFlight` slot
    /// (tagged with `load_id`) and now owns the `spawn_blocking` / notify
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
    /// A single string or an array of strings (`server.bindAddress`,
    /// monorepo#3314). Advertised as `string` on the wire so the FE keeps
    /// rendering the common single-value form; the array shape is accepted
    /// on write and echoed as-is on read.
    StringOrStringArray,
    Enum(&'static [&'static str]),
    /// Structured JSON (objects or arrays), e.g. `string[]` / `mcp.servers`.
    Object,
}

impl SettingType {
    fn wire_name(&self) -> &'static str {
        match self {
            SettingType::Boolean => "boolean",
            SettingType::Number { .. } => "number",
            SettingType::String | SettingType::StringOrStringArray => "string",
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
            SettingType::StringOrStringArray => {
                let array_of_strings = value
                    .as_array()
                    .is_some_and(|items| items.iter().all(Value::is_string));
                if !value.is_string() && !array_of_strings {
                    return invalid(format!(
                        "{}: expected a string or an array of strings",
                        self.path
                    ));
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
                let Some(n) = value.as_f64() else {
                    return invalid(format!("{}: expected a number", self.path));
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

/// `server.bindAddress` (monorepo#3314): a single IP string (back-compat) or
/// an array of IP strings — one listener per address, same port. Semantic
/// validation (IP syntax, duplicates, unspecified-only-alone) happens in the
/// typed registry schema ([`intent_core::settings_file::BindAddress`]).
fn bind_address_definition() -> SettingDefinition {
    SettingDefinition {
        path: "server.bindAddress",
        label: "Bind address",
        description: "Address(es) the TCP listener binds: a single IP or a list of IPs \
                      (one listener per address, same port); 0.0.0.0 exposes it on every \
                      interface, including untrusted networks",
        category: "server",
        ty: SettingType::StringOrStringArray,
        default_value: Some(json!("127.0.0.1")),
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

/// Catalog max for `agents.memoryBudgetMb` when total RAM cannot be detected
/// (RAM detection supports Linux and macOS only), and the ceiling on the
/// detected value. Matches the static bound
/// [`intent_core::settings_file::SettingsFile`] enforces when parsing
/// `config.toml`, which stays machine-independent on purpose: a config file
/// written on one seat must not fail to parse on another.
const MEMORY_BUDGET_MAX_MB_FALLBACK: f64 = 1_024_000.0;

/// Catalog max for `agents.memoryBudgetMb`: total physical RAM in MB, so the
/// FE renders a slider over the range the setting can meaningfully take,
/// falling back to [`MEMORY_BUDGET_MAX_MB_FALLBACK`] where detection is
/// unavailable.
///
/// Budgeting more than the machine has is not a configuration this daemon can
/// honour — the admission gate would simply never fire — so the bound also
/// makes `settings.update` reject such a value (`-32602`) rather than storing
/// a knob that silently does nothing.
///
/// This is the one catalog bound that deliberately does **not** match the
/// `config.toml` parse bound, so a `config.toml` carrying a budget above this
/// machine's RAM (hand-edited, or copied from a larger seat) still loads and
/// is still reported by `settings.get` / `settings.list` — with a `value`
/// above the advertised `max`. The alternative, tightening the parse bound to
/// match, is worse: it would make a config file machine-dependent and fail
/// `Config::resolve` — i.e. refuse to boot — on the smaller seat. The
/// inconsistency is confined to configs the API itself will no longer create,
/// and any write through `settings.update` brings the value back into range;
/// a client should clamp its slider rather than assume `value <= max`.
///
/// The divergence is **one-directional**: this bound is never looser than the
/// parse bound. See [`memory_budget_max_mb_for`] for why that matters.
///
/// Detected **once** and cached: `definitions()` is rebuilt by every
/// `settings.list` / `settings.get`, and the Linux detection reads
/// `/proc/meminfo`, so computing it inline would put a synchronous file read on
/// a client-facing read path. Installed physical RAM cannot change under a live
/// process, so this is the degenerate case of the derived-field ladder — the
/// value is invalidated by nothing, and one read off the first call is enough.
fn memory_budget_max_mb() -> f64 {
    static DETECTED: OnceLock<f64> = OnceLock::new();
    *DETECTED.get_or_init(|| memory_budget_max_mb_for(crate::agent_manager::total_memory_bytes()))
}

/// [`memory_budget_max_mb`] against an explicit detection result, so every
/// branch is testable on a host where detection itself always succeeds and
/// reports one particular size.
///
/// A zero reading counts as "undetected": a max of 0 would collapse the slider
/// onto its own minimum and lock the setting off.
///
/// Detected RAM is **clamped** to [`MEMORY_BUDGET_MAX_MB_FALLBACK`], which is
/// also the `config.toml` parse bound. Without the clamp, a host with more
/// than 1,024,000 MB of RAM (a 1 TiB seat reports 1,048,576) would advertise a
/// max that `settings.update` accepts here and `SettingsFile::validate` then
/// rejects inside `SettingsRegistry::apply` — the catalog would be telling
/// clients a value is settable that the write path refuses. Clamping keeps the
/// asymmetry strictly one-directional (catalog bound ≤ parse bound), which is
/// the invariant every claim in these doc comments depends on.
// MiB counts above 2^53 do not occur; loss-free in f64.
#[allow(clippy::cast_precision_loss)]
fn memory_budget_max_mb_for(total_memory_bytes: Option<u64>) -> f64 {
    match total_memory_bytes.filter(|&bytes| bytes > 0) {
        // INVARIANT: this bound may be tighter than the `config.toml` parse
        // bound, never looser. Dropping the `.min` advertises a max the write
        // path rejects; see the doc comment above before changing it.
        Some(bytes) => ((bytes / (1024 * 1024)) as f64).min(MEMORY_BUDGET_MAX_MB_FALLBACK),
        None => MEMORY_BUDGET_MAX_MB_FALLBACK,
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
    let mut specialists_dir = string(
        "specialists.dir",
        "Specialists directory",
        "Base specialist directory replacing the built-in set (INTENTD_SPECIALISTS_DIR startup pin or config.toml; read-only on the wire)",
        "providers",
        None,
    );
    specialists_dir.read_only = true;
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
        string(
            "model.defaultReasoningEffort",
            "Default reasoning effort",
            "Fallback reasoning effort for new agents (provider-defined value, stored as-is; blank means unset)",
            "providers",
            None,
        ),
        string(
            "quickActions.defaultModel",
            "Default quick action model",
            "Model for single-shot quick actions (commit messages, PR descriptions, quick tasks); never applied to agent sessions",
            "providers",
            None,
        ),
        object(
            "quickActions.typeOverrides",
            "Quick action model overrides",
            "Per-quick-action model overrides (commit, pr, review, fast)",
            "providers",
            Some(json!({})),
        ),
        object(
            "quickActions.providerSettings",
            "Per-provider quick action settings",
            "Per-provider quick-action settings",
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
        specialists_dir,
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
            "git.autoCommit",
            "Auto-commit",
            "Allow agents to commit without explicit user request",
            "git",
            true,
        ),
        boolean(
            "workspace.cowIsolation",
            "Copy-on-Write Isolation",
            "CoW workspaces + per-agent sandboxes (requires CoW filesystem support on the workspaces root)",
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
        string(
            "server.socketPath",
            "Socket path",
            "Unix socket path for the UDS listener",
            "server",
            None,
        ),
        bind_address_definition(),
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
        number(
            "server.maxOutstandingRpcs",
            "Max outstanding RPCs",
            "Daemon-wide cap on outstanding slow-path RPCs across every connection; over-limit requests are rejected with -32011 \"Server overloaded\" (0 = unlimited; changes apply on daemon restart)",
            "server",
            Some(0.0),
            Some(100_000.0),
            256.0,
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
            "Where the GitHub token comes from: auto tries the secrets store, then \
             environment variables, then the gh CLI",
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
        boolean(
            "sourceControl.github.exposeGitCredentialToChildren",
            "Expose Git credential to terminals and agents",
            "Inject the daemon-managed GitHub credential into child process \
             environments as a scoped github.com-only credential helper — \
             never as a raw GITHUB_TOKEN/GH_TOKEN",
            "sourceControl",
            true,
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
        // --- Group A: voice (speech-to-text) -----------------------------------
        enumerated(
            "voice.provider",
            "Voice provider",
            "Active speech-to-text provider for voice.transcribe",
            "voice",
            &["elevenlabs", "openai"],
            "elevenlabs",
        ),
        string(
            "voice.language",
            "Voice language",
            "Default transcription language hint (ISO-639-1 code) when a voice.transcribe call has no per-call language; unset means auto-detect",
            "voice",
            None,
        ),
        secret(
            "voice.elevenlabs.apiKey",
            "ElevenLabs API key",
            "API key used by the ElevenLabs Scribe transcription provider",
            "voice",
        ),
        secret(
            "voice.openai.apiKey",
            "OpenAI API key",
            "API key used by the OpenAI transcription provider",
            "voice",
        ),
        enumerated(
            "voice.openai.model",
            "OpenAI voice model",
            "Transcription model used by the OpenAI provider",
            "voice",
            &["gpt-4o-transcribe", "gpt-4o-mini-transcribe", "whisper-1"],
            "gpt-4o-transcribe",
        ),
        number(
            "voice.workspaceVocabulary.maxTerms",
            "Workspace vocabulary max terms",
            "Cap on the auto-derived workspace vocabulary injected into voice.transcribe calls with a workspaceId and served by voice.getWorkspaceVocabulary (0 disables)",
            "voice",
            Some(0.0),
            Some(100.0),
            50.0,
        ),
        object(
            "voice.vocabulary",
            "Voice vocabulary",
            "Vocabulary terms (string array) biased into every voice.transcribe call",
            "voice",
            Some(json!(crate::voice_ops::DEFAULT_VOCABULARY)),
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
        // --- Group A: hardware console state ----------------------------------
        // Persisted FE-owned hardware-console (control surface) configuration:
        // key assignments, action mappings, prompt-picker limit.
        object(
            "hardwareConsole.state",
            "Hardware console state",
            "Persisted hardware-console configuration (key assignments, action mappings, prompt-picker limit)",
            "hardware",
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
        // No `default_value`: the default is the *absent* key (auto, derived
        // from system RAM), which `number()` cannot express (monorepo#2063).
        SettingDefinition {
            path: "agents.memoryBudgetMb",
            label: "Agent memory budget (MB)",
            description: "Aggregate resident memory the daemon's whole child-process tree may use before it reclaims: new agent spawns queue behind idle-process eviction, and a background sweep drains idle agents largest-first while over budget (absent = auto, derived from system RAM; 0 = off; nothing running is ever killed; changes apply on daemon restart)",
            category: "agents",
            ty: SettingType::Number {
                min: Some(0.0),
                max: Some(memory_budget_max_mb()),
            },
            default_value: None,
            sensitive: false,
            read_only: false,
        },
        number(
            "agents.maxConcurrentAdapters",
            "Max concurrent one-shot adapters",
            "Daemon-wide cap on concurrently live ephemeral ACP adapters (one-shot completions and model probes). Each costs ~610 MB and holds no agent slot; over-limit calls queue and fail with error.data.code \"adapter-busy\" if their own timeout expires first (changes apply on daemon restart)",
            "agents",
            Some(1.0),
            Some(f64::from(intent_core::config::MAX_CONCURRENT_ADAPTERS_LIMIT)),
            f64::from(intent_core::config::DEFAULT_MAX_CONCURRENT_ADAPTERS),
        ),
        number(
            "agents.idleReapMinutes",
            "Idle reap minutes",
            "Minutes before an idle agent is reaped (0 disables idle reaping)",
            "agents",
            Some(0.0),
            None,
            f64::from(intent_core::config::DEFAULT_IDLE_REAP_MINUTES),
        ),
        enumerated(
            "agents.flushQueuedMessages",
            "Flush queued messages",
            "Controls how messages waiting in the queue are delivered to the agent when a turn ends: \
             all batches every ready entry into one turn, systemOnly batches only system-origin \
             entries (user-origin entries stay FIFO), off delivers one turn per queued message",
            "agents",
            &["all", "systemOnly", "off"],
            "all",
        ),
        enumerated(
            "agents.resumeInterruptedOnStart",
            "Resume interrupted agents on start",
            "Whether the daemon resumes interrupted agents at startup when --resume-all is absent: \
             auto resumes only on headless hosts (no display detected), on always resumes, \
             off never resumes; update-triggered restarts always resume regardless of this \
             setting (changes apply on daemon restart)",
            "agents",
            &["auto", "on", "off"],
            "auto",
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
        number(
            "workspaceApi.maxOutputChars",
            "Max workspace API output chars",
            "Max characters of one workspace_api tool result before the output is redirected to a file (0 = unlimited; min 1000 when non-zero)",
            "workspaceApi",
            Some(0.0),
            Some(10_000_000.0),
            100_000.0,
        ),
        boolean(
            "workspaceApi.toonOutput",
            "TOON output",
            "TOON-encode workspace_api tool results (token-efficient) instead of plain JSON",
            "workspaceApi",
            true,
        ),
        boolean(
            "agentFeatures.backgroundHooks",
            "Background hooks",
            "Expose background hooks (ws.hook.*) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.hostExec",
            "Host exec",
            "Expose one-shot host command execution (ws.host.exec) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.scripts",
            "Saved scripts",
            "Expose saved scripts (ws.script.*) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.terminalAccess",
            "Terminal access",
            "Expose terminal read access (ws.terminal.*) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.browserAutomation",
            "Browser automation",
            "Expose browser automation (ws.browser.*) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.richChatBlocks",
            "Rich chat blocks",
            "Include rich chat block guidance (mermaid, ws-block, nav-link) in agent prompts; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.structuredQuestions",
            "Structured questions",
            "Expose structured questions (ws.app.question.ask) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.attentionRequests",
            "Attention requests",
            "Expose attention requests (ws.agent.reportBlocker / ws.agent.requestDiscussion) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.stateSnapshot",
            "State snapshot",
            "Inject the per-turn agent state snapshot line into turn prompts; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.prMonitor",
            "PR monitor",
            "Expose centralized PR monitoring (ws.pr.monitor / ws.pr.unmonitor) to agents; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.taskGraph",
            "Task graph teaching",
            "Teach agents the task-graph workflow (batch delegate, dependsOn/conflictsWith, @@@task fence attributes, unblocked-wake hints); docs/prompt only, APIs always work; applies to new sessions only",
            "agentFeatures",
            true,
        ),
        boolean(
            "agentFeatures.unreadSummaries",
            "Unread summaries",
            "Expose the unread-digest surface (ws.chat.unread) to top-level agents; applies to new sessions only",
            "agentFeatures",
            false,
        ),
        number(
            "agentFeatures.unreadSummarizeThreshold",
            "Unread summarize threshold",
            "Unread-message count at which ws.chat.unread guidance suggests summarizing instead of reading in full; applies to new sessions only",
            "agentFeatures",
            Some(1.0),
            Some(1_000.0),
            4.0,
        ),
        number(
            "prMonitor.debounceSeconds",
            "PR monitor debounce seconds",
            "Quiet window (in seconds) a changed PR must observe before its consolidated wake is delivered (minimum 10)",
            "prMonitor",
            Some(10.0),
            Some(86_400.0),
            60.0,
        ),
        number(
            "prMonitor.pollSeconds",
            "PR monitor poll seconds",
            "How often (in seconds) the centralized loop polls each monitored PR (minimum 10)",
            "prMonitor",
            Some(10.0),
            Some(3_600.0),
            30.0,
        ),
    ]
}

/// The effective `workspace.branchPrefix` (default empty) — prepended to
/// auto-generated workspace branch names (TS `getBranchPrefix` parity).
pub(crate) fn branch_prefix(settings: &SettingsFile) -> String {
    settings.workspace.branch_prefix.clone().unwrap_or_default()
}

/// The effective `workspace.worktreesLocation` (default empty = unset) — the
/// parent directory `workspace.create` provisions new checkouts under.
pub(crate) fn worktrees_location(settings: &SettingsFile) -> String {
    settings
        .workspace
        .worktrees_location
        .clone()
        .unwrap_or_default()
}

/// The effective `agents.maxConcurrent` setting: a positive integer sets an
/// explicit cap; 0 (the default) means "auto" (RAM-based cap via
/// `default_process_cap`). The typed schema already rejects negative /
/// out-of-range / garbled values.
#[must_use]
pub fn max_concurrent_agents(settings: &SettingsFile) -> Option<usize> {
    let n = settings.agents.max_concurrent;
    (n > 0).then_some(n as usize)
}

/// The effective `agents.memoryBudgetMb` setting in bytes: a positive value
/// installs the aggregate child-tree memory budget (monorepo#2063); an
/// explicit `0` is off; an absent key (`None`, the default) resolves to the
/// recommended budget derived from `total_memory_bytes`
/// ([`intent_services::recommended_memory_budget_bytes`]).
#[must_use]
pub fn agent_memory_budget_bytes(settings: &SettingsFile, total_memory_bytes: u64) -> Option<u64> {
    match settings.agents.memory_budget_mb {
        None => Some(crate::agent_manager::recommended_memory_budget_bytes(
            total_memory_bytes,
        )),
        Some(0) => None,
        Some(mb) => Some(u64::from(mb) * 1024 * 1024),
    }
}

/// The effective `agents.maxConcurrentAdapters` setting: the daemon-wide cap
/// on concurrently live ephemeral ACP adapters (monorepo#2062). Unlike
/// `maxConcurrent` there is no "auto" and no unlimited value, so this always
/// yields a usable bound — a `0` that predates the schema bound (or survived a
/// hand-edited file) falls back to the default rather than admitting an
/// unbounded spawn.
#[must_use]
pub fn max_concurrent_adapters(settings: &SettingsFile) -> u32 {
    let n = settings.agents.max_concurrent_adapters;
    if n == 0 {
        intent_core::config::DEFAULT_MAX_CONCURRENT_ADAPTERS
    } else {
        n
    }
}

/// One-time boot import of legacy `config.toml` keys back into the `SQLite`
/// `settings` table (import-or-discard-and-strip). The registry's load
/// tolerated the [`intent_core::settings_file::LEGACY_SETTINGS_PATHS`] keys
/// and captured their values; here each captured value is persisted to `SQLite`
/// when it matches its catalog definition (overwriting any existing row —
/// the file value is the user's most recent intent) or discarded with a
/// warning when it does not (all current legacy keys — `[ai]`,
/// `server.listenMode`, `model.workspaceOverrides`, `workspace.autoFetch`,
/// `[backgroundAgents]` — are retired without a catalog entry, so they are
/// discarded; the `[backgroundAgents]` values are carried over into
/// `quickActions.*` beforehand by [`migrate_quick_action_settings`]), and the
/// keys are then stripped from the file with a comment-preserving rewrite.
/// Nothing is stripped when a
/// `SQLite` write fails, so the next boot retries the import. The strip itself
/// is best-effort: once the values are safely in `SQLite`, a failed file
/// rewrite (read-only file, perms, full disk) is logged and startup continues
/// — the next boot re-runs the import, which idempotently overwrites the same
/// rows and retries the strip. Returns the stripped paths (empty when the
/// file had no legacy keys or the rewrite failed).
///
/// # Errors
///
/// Returns `Error::Internal` if encoding a legacy value or persisting it to the store fails.
pub async fn import_legacy_settings(
    registry: &SettingsRegistry,
    store: &Store,
) -> Result<Vec<String>> {
    let legacy = registry.legacy_values();
    if legacy.is_empty() {
        return Ok(Vec::new());
    }
    for (path, value) in &legacy {
        let Some(def) = find_definition(path) else {
            tracing::warn!(
                path,
                "legacy config.toml key has no catalog entry; discarding"
            );
            continue;
        };
        if let Err(e) = def.validate(value) {
            tracing::warn!(path, error = %e, "legacy config.toml value is invalid; discarding");
            continue;
        }
        let raw = serde_json::to_string(value)
            .map_err(|e| Error::Internal(format!("encode legacy setting {path} failed: {e}")))?;
        store.set_setting(path, &raw).await?;
        tracing::info!(path, "imported legacy config.toml key into SQLite");
    }
    let stripped = match registry.strip_legacy() {
        Ok(stripped) => stripped,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "failed to strip legacy keys from config.toml; continuing with imported values (next boot retries)"
            );
            Vec::new()
        }
    };
    if !stripped.is_empty() {
        tracing::info!(?stripped, "stripped legacy keys from config.toml");
    }
    Ok(stripped)
}

/// One-time boot carry-over of the renamed `[backgroundAgents]` table into
/// `quickActions.*` (monorepo#1729). The registry's load captured the whole
/// legacy table; each of its `defaultModel` / `typeOverrides` /
/// `providerSettings` members is written to the matching `quickActions.*` key
/// **only** when that key is still at its schema default, so an already-
/// migrated (or deliberately re-picked) value is never clobbered. Runs before
/// [`import_legacy_settings`], which then discards and strips the legacy
/// table. A file with no `[backgroundAgents]` table is a no-op.
///
/// Each member is applied on its own: `apply` validates a batch atomically, so
/// batching them would let one malformed legacy value (say `defaultModel = 1`)
/// discard the valid siblings the same boot that strips the legacy table. A
/// member that fails the typed schema is skipped with a warning and the rest
/// still carry over.
///
/// # Errors
///
/// Never errors today: migration failures are logged and skipped. The `Result` keeps parity with the other startup migrations.
pub fn migrate_quick_action_settings(registry: &SettingsRegistry) -> Result<()> {
    const MEMBERS: [&str; 3] = ["defaultModel", "typeOverrides", "providerSettings"];
    let Some(legacy) = registry.legacy_values().get("backgroundAgents").cloned() else {
        return Ok(());
    };
    let Some(table) = legacy.as_object() else {
        tracing::warn!("legacy [backgroundAgents] is not a table; discarding");
        return Ok(());
    };
    let mut migrated: Vec<String> = Vec::new();
    let unknown: Vec<&str> = table
        .keys()
        .map(String::as_str)
        .filter(|k| !MEMBERS.contains(k))
        .collect();
    if !unknown.is_empty() {
        tracing::warn!(
            members = ?unknown,
            "legacy [backgroundAgents] members have no quickActions.* counterpart; dropping"
        );
    }
    for member in MEMBERS {
        let Some(value) = table.get(member) else {
            continue;
        };
        let path = format!("quickActions.{member}");
        if registry.origin(&path) != Some(SettingOrigin::Default) {
            tracing::debug!(path, "quick-action key already set; keeping current value");
            continue;
        }
        if let Err(e) = registry.apply(&[(path.clone(), value.clone())]) {
            tracing::warn!(path, error = %e, "failed to migrate legacy [backgroundAgents] member; discarding");
            continue;
        }
        migrated.push(path);
    }
    if !migrated.is_empty() {
        tracing::info!(
            paths = ?migrated,
            "migrated legacy [backgroundAgents] into quickActions.*"
        );
    }
    Ok(())
}

/// One-time boot cleanup of stale `SQLite` rows for retired settings. The
/// per-workspace override blob (`model.workspaceOverrides`, monorepo#1000)
/// no longer has a catalog entry or any reader; delete its row so stale
/// state cannot resurface if the key ever returns. Idempotent — deleting an
/// absent row is a no-op.
///
/// # Errors
///
/// Returns `Error::Internal` if deleting the settings row fails.
pub async fn cleanup_retired_settings(store: &Store) -> Result<()> {
    if store
        .delete_setting(RETIRED_WORKSPACE_OVERRIDES_PATH)
        .await?
    {
        tracing::info!(
            path = RETIRED_WORKSPACE_OVERRIDES_PATH,
            "deleted stale settings row for retired setting"
        );
    }
    Ok(())
}

/// One-time boot migration for the `voice.vocabulary` default trim: a stored
/// row that exactly matches the retired 17-term seed default (order-sensitive)
/// only ever persisted the old default, so it is deleted and the catalog
/// default (`["Intent"]`) applies again. Any other stored value — a
/// user-modified list, or even a malformed blob — is never touched.
/// Idempotent — an absent or non-matching row is a no-op.
///
/// # Errors
///
/// Returns `Error::Internal` if reading or deleting the stored setting fails.
pub async fn migrate_default_vocabulary(store: &Store) -> Result<()> {
    let Some(raw) = store.get_setting(VOICE_VOCABULARY_PATH).await? else {
        return Ok(());
    };
    let legacy = json!(crate::voice_ops::LEGACY_DEFAULT_VOCABULARY);
    if matches!(serde_json::from_str::<Value>(&raw), Ok(value) if value == legacy) {
        store.delete_setting(VOICE_VOCABULARY_PATH).await?;
        tracing::info!(
            path = VOICE_VOCABULARY_PATH,
            "deleted stored voice.vocabulary matching the retired 17-term default; the new default applies"
        );
    }
    Ok(())
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
// The `n.abs() <= i64::MAX as f64` guard bounds the float→int cast; the
// i64::MAX→f64 comparison constant rounding up by one ULP is harmless here.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
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
/// `SQLite` path (read-only test wiring).
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
    /// registry is wired. `None` falls back to the legacy `SQLite` path.
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
        // Drive every load future concurrently on the current task: a single
        // stalled account never blocks the others because `join_all_pinned`
        // polls every future on each wake-up. Best-effort: errors treated as absent.
        type LoadFuture<'a> = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<String>>> + Send + 'a>,
        >;
        let defs = definitions();
        let sensitive: Vec<&'static str> = defs
            .iter()
            .filter(|d| d.sensitive)
            .map(|d| d.path)
            .collect();
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
    /// `SQLite` `settings` table. Secrets and state-blob keys keep their
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
            // monorepo#1000 compatibility: old clients still write the retired
            // per-workspace override path on every workspace-scoped model
            // pick. Tolerate-and-ignore the entry (nothing validated,
            // persisted, echoed, or published) instead of rejecting the whole
            // batch as an unknown path.
            // monorepo#1729 compatibility: pre-rename clients still write the
            // `backgroundAgents.*` paths. Same tolerate-and-ignore treatment —
            // the renamed `quickActions.*` keys are the only writable surface.
            if path == RETIRED_WORKSPACE_OVERRIDES_PATH
                || RETIRED_BACKGROUND_AGENT_PATHS.contains(&path)
            {
                tracing::debug!(path, "ignoring settings.update for retired setting");
                continue;
            }
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

    /// `accounts.sentry.token` must be sensitive so `settings.update` persists
    /// it to the shared secrets store under account = setting path (never the
    /// DB) and every wire read (`settings.list` / `settings.get`) redacts it to
    /// a placeholder or `null` when unset.
    #[test]
    fn new_secret_catalog_entries_are_sensitive() {
        let path = "accounts.sentry.token";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert_eq!(def.path, path);
        assert!(def.sensitive, "{path} must be a sensitive catalog entry");
        assert!(!def.read_only, "{path} must not be read-only");
        assert!(matches!(def.ty, SettingType::String));
        assert!(def.default_value.is_none(), "{path} default is null");
    }

    /// `voice.vocabulary` is a non-secret string-array (Object) entry whose
    /// default is the minimal `["Intent"]` seed — users add their own terms.
    #[test]
    fn voice_vocabulary_is_a_string_array_with_the_default_terms() {
        let def = find_definition("voice.vocabulary").expect("voice.vocabulary missing");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "voice");
        assert!(matches!(def.ty, SettingType::Object));
        let default = def.default_value.expect("voice.vocabulary default");
        assert_eq!(default, json!(crate::voice_ops::DEFAULT_VOCABULARY));
        assert_eq!(default, json!(["Intent"]));
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

    /// The `ai.*` group is retired: no catalog entry may remain for any of its
    /// keys (the app drives AI via ACP agent providers, not a direct provider).
    #[test]
    fn ai_group_is_gone_from_the_catalog() {
        for path in [
            "ai.apiToken",
            "ai.apiUrl",
            "ai.model",
            "ai.temperature",
            "ai.maxTokens",
            "ai.streamingSpeed",
        ] {
            assert!(
                find_definition(path).is_none(),
                "{path} must not be in the catalog"
            );
        }
        assert!(definitions().iter().all(|d| d.category != "ai"));
    }

    /// `server.listenMode` is retired: the daemon always serves UDS and the
    /// TCP/WSS listener is governed by `server.wsApi.enabled`, so no catalog
    /// entry may remain for the old key.
    #[test]
    fn listen_mode_is_gone_from_the_catalog() {
        assert!(
            find_definition("server.listenMode").is_none(),
            "server.listenMode must not be in the catalog"
        );
    }

    /// `model.workspaceOverrides` is retired (monorepo#1000): the per-workspace
    /// model override layer is gone, so no catalog entry may remain and
    /// `settings.list` never advertises the path.
    #[test]
    fn workspace_overrides_is_gone_from_the_catalog() {
        assert!(
            find_definition(RETIRED_WORKSPACE_OVERRIDES_PATH).is_none(),
            "model.workspaceOverrides must not be in the catalog"
        );
    }

    /// The non-secret gap entries live in the catalog as opaque `Object`
    /// settings with a documented default. Each is validated by shape only;
    /// downstream consumers own the internal schema (permission rules, prompt
    /// rules, known repos, change-history bags, workspace-initializer state,
    /// hardware-console state).
    #[test]
    fn non_secret_object_gap_entries_have_defaults() {
        for path in [
            "permissions.rules",
            "userRules",
            "workspaceRules",
            "repos.known",
            "workspace.changeHistory",
            "workspaceInitializer.state",
            "hardwareConsole.state",
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
        // Compressed load budget (`with_timings`) so the timeout path runs in
        // milliseconds instead of the production 3s default.
        let load_timeout = Duration::from_millis(100);
        // Hang for well over the per-op budget so a naive implementation would
        // stall for `N_sensitive * hang_for` and blow past the test's outer
        // timeout — but short enough that the blocking-pool shutdown at
        // end-of-test doesn't hold up the whole test binary.
        let secrets: Arc<dyn SecretStore> = Arc::new(HangingSecretStore {
            hang_for: Duration::from_millis(750),
        });
        let secrets = AsyncSecretStore::with_timings(
            secrets,
            load_timeout,
            DEFAULT_WRITE_TIMEOUT,
            DEFAULT_CACHE_TTL,
            DEFAULT_WARN_INTERVAL,
        );
        let svc = SettingsService::new(&store, &secrets, None);

        let started = std::time::Instant::now();
        // Outer cap: single-flight + TTL cache in AsyncSecretStore bounds the
        // total latency to ONE keychain budget — well under the per-call
        // stall the store models. A generous slack absorbs task-spawn
        // overhead on cold runners.
        let cap = load_timeout + Duration::from_secs(2);
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
        // Compressed write budget (`with_timings`) so the timeout path runs
        // in milliseconds instead of the production 10s default; the stall
        // stays well over the budget but short enough that the blocking-pool
        // shutdown at end-of-test doesn't hold up the whole test binary.
        let write_timeout = Duration::from_millis(150);
        let secrets: Arc<dyn SecretStore> = Arc::new(HangingSecretStore {
            hang_for: Duration::from_millis(750),
        });
        let secrets = AsyncSecretStore::with_timings(
            secrets,
            DEFAULT_LOAD_TIMEOUT,
            write_timeout,
            DEFAULT_CACHE_TTL,
            DEFAULT_WARN_INTERVAL,
        );
        let svc = SettingsService::new(&store, &secrets, None);

        let started = std::time::Instant::now();
        // AsyncSecretStore's write budget is `write_timeout`; use it plus a
        // small slack so the assertion measures the bounded write path.
        let cap = write_timeout + Duration::from_secs(2);
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

    /// `agents.memoryBudgetMb` is a TOML-backed bounded number, and
    /// `agent_memory_budget_bytes` resolves the absent/0/positive
    /// matrix (monorepo#2063): absent = auto (resolves to the recommended budget
    /// derived from system RAM), explicit 0 = off (a 0 must stay `None` rather
    /// than becoming a 0-byte budget that would refuse every spawn), positive =
    /// MB converted to bytes. The catalog carries no `default_value` because the
    /// default is the absent key.
    #[test]
    fn agent_memory_budget_matrix_absent_auto_zero_off_positive_bytes() {
        use crate::agent_manager::recommended_memory_budget_bytes;
        let def = find_definition("agents.memoryBudgetMb")
            .expect("agents.memoryBudgetMb missing from catalog");
        assert!(!def.sensitive);
        assert_eq!(def.category, "agents");
        let SettingType::Number { min, max } = def.ty else {
            panic!("agents.memoryBudgetMb is not a number");
        };
        assert_eq!(min, Some(0.0));
        // The max is the detected RAM bound, not a static figure — assert the
        // catalog carries whatever this host resolves to, and that it is a
        // usable range rather than a degenerate 0..0.
        assert_eq!(max, Some(memory_budget_max_mb()));
        assert!(max.expect("bounded") > 0.0);
        assert_eq!(def.default_value, None, "the default is the absent key");
        assert!(KNOWN_PATHS.contains(&"agents.memoryBudgetMb"));

        let mut settings = SettingsFile::default();
        assert_eq!(settings.agents.memory_budget_mb, None);
        // Absent key resolves to recommended budget. Test with 48 GB RAM.
        let total_ram = 48 * 1024 * 1024 * 1024;
        assert_eq!(
            agent_memory_budget_bytes(&settings, total_ram),
            Some(recommended_memory_budget_bytes(total_ram)),
        );

        settings.agents.memory_budget_mb = Some(0);
        assert_eq!(agent_memory_budget_bytes(&settings, total_ram), None);

        settings.agents.memory_budget_mb = Some(20_480);
        assert_eq!(
            agent_memory_budget_bytes(&settings, total_ram),
            Some(20 * 1024 * 1024 * 1024),
        );
    }

    /// The `agents.memoryBudgetMb` catalog max is the machine's own RAM in MB
    /// where detection works, and the static fallback where it does not —
    /// a slider running to 1 TB on a 48 GB seat is unusable. Both branches are
    /// exercised through the injectable form, since detection on this host
    /// always succeeds; a zero reading is treated as undetected so the max
    /// never collapses onto the minimum.
    #[test]
    #[allow(clippy::float_cmp)] // asserting exact literals round-tripped through config parsing
    fn memory_budget_max_tracks_detected_ram_with_static_fallback() {
        assert_eq!(
            memory_budget_max_mb_for(Some(48 * 1024 * 1024 * 1024)),
            49_152.0
        );
        assert_eq!(
            memory_budget_max_mb_for(Some(16 * 1024 * 1024 * 1024)),
            16_384.0
        );
        assert_eq!(
            memory_budget_max_mb_for(None),
            MEMORY_BUDGET_MAX_MB_FALLBACK
        );
        assert_eq!(
            memory_budget_max_mb_for(Some(0)),
            MEMORY_BUDGET_MAX_MB_FALLBACK
        );
        // A seat larger than the parse bound is clamped to it: advertising
        // 1,048,576 on a 1 TiB host would be a max the write path rejects.
        assert_eq!(
            memory_budget_max_mb_for(Some(1024 * 1024 * 1024 * 1024)),
            MEMORY_BUDGET_MAX_MB_FALLBACK
        );

        // The wired-up form agrees with the injectable one on this host.
        assert_eq!(
            memory_budget_max_mb(),
            memory_budget_max_mb_for(crate::agent_manager::total_memory_bytes()),
        );
    }

    /// The catalog bound may be tighter than the `config.toml` parse bound but
    /// must never be looser, and the direction is load-bearing in both senses:
    ///
    /// - **Never looser** — everything the catalog advertises as settable must
    ///   survive the write path. `settings.update` re-validates through
    ///   `SettingsFile::validate` inside `SettingsRegistry::apply`, so a max
    ///   above the parse bound would advertise values that path rejects.
    /// - **May be tighter** — a config copied from a larger seat must still
    ///   parse here (tightening the parse bound would fail `Config::resolve`
    ///   and refuse to boot), while `settings.update` declines to newly create
    ///   a budget the admission gate could never fire on. The cost a client
    ///   tolerates is a reported `value` above the advertised `max`.
    ///
    /// The host-independent half is asserted unconditionally; the window that
    /// demonstrates the tighter-than case only exists on a seat smaller than
    /// the parse bound, which is every real one but need not be assumed.
    #[test]
    // The advertised max is a small whole-valued float: casts are exact.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn memory_budget_catalog_bound_is_never_looser_than_the_parse_bound() {
        let def = find_definition("agents.memoryBudgetMb").expect("in catalog");
        let max = memory_budget_max_mb();

        assert!(
            max <= MEMORY_BUDGET_MAX_MB_FALLBACK,
            "catalog advertised {max} above the parse bound — settings.update would accept a \
             value SettingsFile::validate then rejects in SettingsRegistry::apply",
        );

        // Whatever the catalog advertises as its ceiling must be settable
        // through both gates the write path runs.
        def.validate(&json!(max))
            .expect("the advertised max must pass catalog validation");
        SettingsFile::parse_str(&format!("[agents]\nmemoryBudgetMb = {}\n", max as u64))
            .expect("the advertised max must pass the config.toml parse bound");

        // Tighter-than case: a value in the gap parses but is refused by the API.
        if max < MEMORY_BUDGET_MAX_MB_FALLBACK {
            let over_ram = max + 1.0;
            def.validate(&json!(over_ram))
                .expect_err("settings.update must reject a budget above this machine's RAM");
            let parsed = SettingsFile::parse_str(&format!(
                "[agents]\nmemoryBudgetMb = {}\n",
                over_ram as u64
            ))
            .expect("a config.toml from a larger seat must still parse, not refuse to boot");
            assert_eq!(parsed.agents.memory_budget_mb, Some(over_ram as u32));
        }
    }

    /// The shipped idle-reap default is 10 minutes (lowered from 30,
    /// monorepo#2109) and the catalog advertises the same constant the
    /// config-file layer defaults to — the two drifting apart is exactly how
    /// the FE ends up showing a default the daemon does not use. `0` stays
    /// valid as the disable value.
    #[test]
    fn idle_reap_catalog_default_matches_the_shipped_constant() {
        let def = find_definition("agents.idleReapMinutes")
            .expect("agents.idleReapMinutes missing from catalog");
        assert_eq!(intent_core::config::DEFAULT_IDLE_REAP_MINUTES, 10);
        assert_eq!(def.default_value, Some(json!(10.0)));
        assert_eq!(
            SettingsFile::default().agents.idle_reap_minutes,
            intent_core::config::DEFAULT_IDLE_REAP_MINUTES,
        );
        assert!(matches!(def.ty, SettingType::Number { min: Some(0.0), .. }));
        def.validate(&json!(0)).expect("0 disables idle reaping");
    }

    /// `agents.maxConcurrentAdapters` is a bounded catalog entry with no
    /// "auto" and no unlimited value, and the resolver never yields `0` —
    /// a zero that slipped past the schema would deadlock every adapter run,
    /// so it falls back to the shipped default instead (monorepo#2062).
    #[test]
    fn max_concurrent_adapters_catalog_entry_and_resolver() {
        let def = find_definition("agents.maxConcurrentAdapters")
            .expect("agents.maxConcurrentAdapters missing from catalog");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "agents");
        assert!(matches!(
            def.ty,
            SettingType::Number {
                min: Some(1.0),
                max: Some(64.0)
            }
        ));
        assert_eq!(def.default_value, Some(json!(6.0)));
        assert!(
            def.description.contains("daemon restart"),
            "description must state the restart requirement: {}",
            def.description
        );
        assert!(KNOWN_PATHS.contains(&"agents.maxConcurrentAdapters"));

        let mut settings = SettingsFile::default();
        assert_eq!(
            max_concurrent_adapters(&settings),
            intent_core::config::DEFAULT_MAX_CONCURRENT_ADAPTERS
        );
        settings.agents.max_concurrent_adapters = 4;
        assert_eq!(max_concurrent_adapters(&settings), 4);
        settings.agents.max_concurrent_adapters = 0;
        assert_eq!(
            max_concurrent_adapters(&settings),
            intent_core::config::DEFAULT_MAX_CONCURRENT_ADAPTERS,
            "a 0 must never reach the semaphore as an unbounded cap"
        );
    }

    /// `server.maxOutstandingRpcs` is a non-secret TOML-backed bounded number
    /// (0 = unlimited, max 100k, default 256) registered in `KNOWN_PATHS`, and
    /// its description states the restart requirement (the limiter is built
    /// once in the composition root).
    #[test]
    fn max_outstanding_rpcs_catalog_entry_is_toml_backed() {
        let def = find_definition("server.maxOutstandingRpcs")
            .expect("server.maxOutstandingRpcs missing from catalog");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "server");
        assert!(matches!(
            def.ty,
            SettingType::Number {
                min: Some(0.0),
                max: Some(100_000.0)
            }
        ));
        assert_eq!(def.default_value, Some(json!(256.0)));
        assert!(
            def.description.contains("daemon restart"),
            "description must state the restart requirement: {}",
            def.description
        );
        assert!(KNOWN_PATHS.contains(&"server.maxOutstandingRpcs"));
    }

    /// `workspaceApi.*` are non-secret TOML-backed catalog entries:
    /// `maxOutputChars` is a bounded number (0 = unlimited, max 10M, default
    /// 100k) and `toonOutput` a boolean defaulting to `true`.
    #[test]
    fn workspace_api_catalog_entries_are_toml_backed() {
        let def = find_definition("workspaceApi.maxOutputChars")
            .expect("workspaceApi.maxOutputChars missing");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "workspaceApi");
        assert!(matches!(
            def.ty,
            SettingType::Number {
                min: Some(0.0),
                max: Some(10_000_000.0)
            }
        ));
        assert_eq!(def.default_value, Some(json!(100_000.0)));
        assert!(KNOWN_PATHS.contains(&"workspaceApi.maxOutputChars"));

        let def =
            find_definition("workspaceApi.toonOutput").expect("workspaceApi.toonOutput missing");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "workspaceApi");
        assert!(matches!(def.ty, SettingType::Boolean));
        assert_eq!(def.default_value, Some(json!(true)));
        assert!(KNOWN_PATHS.contains(&"workspaceApi.toonOutput"));
    }

    /// `workspaceApi.*` round-trip through the registry-wired service:
    /// defaults read with `default` origin, updates persist to config.toml
    /// (`file` origin, never `SQLite`), the sub-1000 non-zero value for
    /// `maxOutputChars` rejects with `-32602` via the typed schema, `0`
    /// (unlimited) is accepted, and reset restores the defaults.
    #[tokio::test]
    async fn workspace_api_settings_round_trip_via_registry() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-wsapi-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-wsapi-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Defaults with `default` origin.
        let got = svc.get("workspaceApi.maxOutputChars").await.expect("get");
        assert_eq!(got["value"], json!(100_000.0));
        assert_eq!(got["origin"], json!("default"));
        let got = svc.get("workspaceApi.toonOutput").await.expect("get");
        assert_eq!(got["value"], json!(true));
        assert_eq!(got["origin"], json!("default"));

        // Updates persist to config.toml with `file` origin, never SQLite.
        svc.update(&json!([
            { "path": "workspaceApi.maxOutputChars", "value": 250_000 },
            { "path": "workspaceApi.toonOutput", "value": false },
        ]))
        .await
        .expect("update");
        let got = svc.get("workspaceApi.maxOutputChars").await.expect("get");
        assert_eq!(got["value"], json!(250_000.0));
        assert_eq!(got["origin"], json!("file"));
        let got = svc.get("workspaceApi.toonOutput").await.expect("get");
        assert_eq!(got["value"], json!(false));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("maxOutputChars"), "{text}");
        assert!(text.contains("toonOutput"), "{text}");
        for path in ["workspaceApi.maxOutputChars", "workspaceApi.toonOutput"] {
            assert_eq!(
                store.get_setting(path).await.expect("read settings table"),
                None,
                "TOML-backed keys must never write a SQLite settings row"
            );
        }

        // Non-zero values below 1000 reject via the typed schema (-32602).
        let err = svc
            .update(&json!([{ "path": "workspaceApi.maxOutputChars", "value": 500 }]))
            .await
            .expect_err("sub-1000 non-zero value must reject");
        assert!(
            matches!(err, Error::InvalidParams(ref msg) if msg.contains("workspaceApi.maxOutputChars")),
            "expected InvalidParams naming the key, got {err:?}"
        );
        // …and the prior value is untouched.
        let got = svc.get("workspaceApi.maxOutputChars").await.expect("get");
        assert_eq!(got["value"], json!(250_000.0));

        // 0 (unlimited) is accepted.
        svc.update(&json!([{ "path": "workspaceApi.maxOutputChars", "value": 0 }]))
            .await
            .expect("0 = unlimited must be accepted");
        let got = svc.get("workspaceApi.maxOutputChars").await.expect("get");
        assert_eq!(got["value"], json!(0.0));

        // Reset restores the defaults and strips the keys from the file.
        let reset = svc
            .reset("workspaceApi.maxOutputChars")
            .await
            .expect("reset");
        assert_eq!(reset["value"], json!(100_000.0));
        let reset = svc.reset("workspaceApi.toonOutput").await.expect("reset");
        assert_eq!(reset["value"], json!(true));
        let got = svc.get("workspaceApi.maxOutputChars").await.expect("get");
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `agents.flushQueuedMessages` is a TOML-backed enum (`all` / `systemOnly`
    /// / `off`) defaulting to `all`: the catalog entry and wire round-trip
    /// through the registry-wired service (default origin → file override →
    /// reset). Also covers a legacy boolean already on disk loading as the
    /// wire-reported string.
    #[tokio::test]
    async fn agents_flush_queued_messages_round_trip_via_registry() {
        let def = find_definition("agents.flushQueuedMessages")
            .expect("agents.flushQueuedMessages missing");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "agents");
        assert!(
            matches!(def.ty, SettingType::Enum(values) if values == ["all", "systemOnly", "off"])
        );
        assert_eq!(def.default_value, Some(json!("all")));
        assert!(KNOWN_PATHS.contains(&"agents.flushQueuedMessages"));

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-flushq-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-flushq-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Default with `default` origin.
        let got = svc.get("agents.flushQueuedMessages").await.expect("get");
        assert_eq!(got["value"], json!("all"));
        assert_eq!(got["origin"], json!("default"));

        // Update persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([
            { "path": "agents.flushQueuedMessages", "value": "systemOnly" },
        ]))
        .await
        .expect("update");
        let got = svc.get("agents.flushQueuedMessages").await.expect("get");
        assert_eq!(got["value"], json!("systemOnly"));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("flushQueuedMessages"), "{text}");
        assert_eq!(
            store
                .get_setting("agents.flushQueuedMessages")
                .await
                .expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Rejects an unknown enum value.
        let err = svc
            .update(&json!([
                { "path": "agents.flushQueuedMessages", "value": "sometimes" },
            ]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("flushQueuedMessages"), "{err}");

        // Reset restores the default.
        let reset = svc
            .reset("agents.flushQueuedMessages")
            .await
            .expect("reset");
        assert_eq!(reset["value"], json!("all"));
        let got = svc.get("agents.flushQueuedMessages").await.expect("get");
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `agents.resumeInterruptedOnStart` is a TOML-backed enum (`auto` / `on`
    /// / `off`) defaulting to `auto`: the catalog entry and wire round-trip
    /// through the registry-wired service (default origin → file override →
    /// reset).
    #[tokio::test]
    async fn agents_resume_interrupted_on_start_round_trip_via_registry() {
        let def = find_definition("agents.resumeInterruptedOnStart")
            .expect("agents.resumeInterruptedOnStart missing");
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "agents");
        assert!(matches!(def.ty, SettingType::Enum(values) if values == ["auto", "on", "off"]));
        assert_eq!(def.default_value, Some(json!("auto")));
        assert!(KNOWN_PATHS.contains(&"agents.resumeInterruptedOnStart"));

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-resume-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-resume-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Default with `default` origin.
        let got = svc
            .get("agents.resumeInterruptedOnStart")
            .await
            .expect("get");
        assert_eq!(got["value"], json!("auto"));
        assert_eq!(got["origin"], json!("default"));

        // Update persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([
            { "path": "agents.resumeInterruptedOnStart", "value": "on" },
        ]))
        .await
        .expect("update");
        let got = svc
            .get("agents.resumeInterruptedOnStart")
            .await
            .expect("get");
        assert_eq!(got["value"], json!("on"));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("resumeInterruptedOnStart"), "{text}");
        assert_eq!(
            store
                .get_setting("agents.resumeInterruptedOnStart")
                .await
                .expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Rejects an unknown enum value.
        let err = svc
            .update(&json!([
                { "path": "agents.resumeInterruptedOnStart", "value": "maybe" },
            ]))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("resumeInterruptedOnStart"),
            "{err}"
        );

        // Reset restores the default.
        let reset = svc
            .reset("agents.resumeInterruptedOnStart")
            .await
            .expect("reset");
        assert_eq!(reset["value"], json!("auto"));
        let got = svc
            .get("agents.resumeInterruptedOnStart")
            .await
            .expect("get");
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// A `config.toml` written by an older daemon (`flushQueuedMessages =
    /// true/false`) still loads through the registry, wire-reporting the
    /// equivalent string value.
    #[tokio::test]
    async fn agents_flush_queued_messages_legacy_boolean_loads_via_registry() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-flushq-legacy-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-flushq-legacy-{tag}.toml"));
        std::fs::write(&config_path, "[agents]\nflushQueuedMessages = false\n")
            .expect("write legacy config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        let got = svc.get("agents.flushQueuedMessages").await.expect("get");
        assert_eq!(got["value"], json!("off"));
        assert_eq!(got["origin"], json!("file"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// The `agentFeatures.*` toggles are TOML-backed booleans — all default
    /// `true`: each has a catalog entry in the `agentFeatures` category and a
    /// `KNOWN_PATHS` entry, and each round-trips through the registry-wired
    /// service (default origin → file override → reset).
    #[tokio::test]
    async fn agent_features_toggles_round_trip_via_registry() {
        let paths = [
            ("agentFeatures.backgroundHooks", true),
            ("agentFeatures.hostExec", true),
            ("agentFeatures.scripts", true),
            ("agentFeatures.terminalAccess", true),
            ("agentFeatures.browserAutomation", true),
            ("agentFeatures.richChatBlocks", true),
            ("agentFeatures.structuredQuestions", true),
            ("agentFeatures.attentionRequests", true),
            ("agentFeatures.stateSnapshot", true),
            ("agentFeatures.prMonitor", true),
            ("agentFeatures.taskGraph", true),
            ("agentFeatures.unreadSummaries", false),
        ];
        for (path, default) in paths {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert!(!def.sensitive, "{path} must be non-secret");
            assert!(!def.read_only, "{path} must not be read-only");
            assert_eq!(def.category, "agentFeatures");
            assert!(matches!(def.ty, SettingType::Boolean), "{path} boolean");
            assert_eq!(
                def.default_value,
                Some(json!(default)),
                "{path} default mismatch"
            );
            assert!(KNOWN_PATHS.contains(&path), "{path} must be TOML-backed");
        }

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-agentfeat-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-agentfeat-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        for (path, default) in paths {
            // Default with `default` origin.
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["value"], json!(default), "{path} default");
            assert_eq!(got["origin"], json!("default"), "{path} origin");

            // Update persists to config.toml with `file` origin, never SQLite.
            svc.update(&json!([{ "path": path, "value": !default }]))
                .await
                .expect("update");
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["value"], json!(!default), "{path} updated");
            assert_eq!(got["origin"], json!("file"), "{path} origin");
            assert_eq!(
                store.get_setting(path).await.expect("read settings table"),
                None,
                "TOML-backed {path} must never write a SQLite settings row"
            );

            // Reset restores the default.
            let reset = svc.reset(path).await.expect("reset");
            assert_eq!(reset["value"], json!(default), "{path} reset");
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["origin"], json!("default"), "{path} origin after reset");
        }

        // Mistyped values reject with -32602 semantics (InvalidParams).
        let err = svc
            .update(&json!([{ "path": "agentFeatures.hostExec", "value": "yes" }]))
            .await
            .expect_err("string value must reject");
        assert!(matches!(err, Error::InvalidParams(_)), "{err}");

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `[prMonitor]` exposes two TOML-backed numbers with a floor of 10:
    /// `debounceSeconds` (default 60) and `pollSeconds` (default 30, a
    /// config-file key the Settings UI does not surface). Both round-trip
    /// through the registry-wired service and reject sub-floor values.
    #[tokio::test]
    async fn pr_monitor_intervals_round_trip_via_registry() {
        for (path, default) in [
            ("prMonitor.debounceSeconds", 60.0),
            ("prMonitor.pollSeconds", 30.0),
        ] {
            let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
            assert!(!def.sensitive, "{path} must be non-secret");
            assert!(!def.read_only, "{path} must not be read-only");
            assert_eq!(def.category, "prMonitor");
            assert!(
                matches!(
                    def.ty,
                    SettingType::Number {
                        min: Some(10.0),
                        ..
                    }
                ),
                "{path} number with a floor of 10"
            );
            assert_eq!(def.default_value, Some(json!(default)), "{path} default");
            assert!(KNOWN_PATHS.contains(&path), "{path} must be TOML-backed");
        }

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-prmon-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-prmon-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        for (path, default) in [
            ("prMonitor.debounceSeconds", 60.0),
            ("prMonitor.pollSeconds", 30.0),
        ] {
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["value"], json!(default), "{path} default");
            assert_eq!(got["origin"], json!("default"), "{path} origin");

            svc.update(&json!([{ "path": path, "value": 120 }]))
                .await
                .expect("update");
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["value"], json!(120.0), "{path} updated");
            assert_eq!(got["origin"], json!("file"), "{path} origin");
            assert_eq!(
                store.get_setting(path).await.expect("read settings table"),
                None,
                "TOML-backed {path} must never write a SQLite settings row"
            );

            // Sub-floor values reject before anything is written.
            let err = svc
                .update(&json!([{ "path": path, "value": 5 }]))
                .await
                .expect_err("sub-floor value must reject");
            assert!(matches!(err, Error::InvalidParams(_)), "{err}");

            let reset = svc.reset(path).await.expect("reset");
            assert_eq!(reset["value"], json!(default), "{path} reset");
            let got = svc.get(path).await.expect("get");
            assert_eq!(got["origin"], json!("default"), "{path} origin after reset");
        }

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// Q1 regression: with the registry wired (production composition), a
    /// `settings.update` of a TOML-backed key persists to `config.toml` only —
    /// it must NOT write a row to the `SQLite` `settings` table, which now holds
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

    /// monorepo#884: `sourceControl.github.exposeGitCredentialToChildren`
    /// gates whether daemon-managed GitHub credentials are injected into
    /// child process environments — a non-sensitive TOML-backed boolean,
    /// default `true` (opt-out), that round-trips through `settings.update`
    /// / `settings.reset` with config.toml persistence and `origin`.
    #[tokio::test]
    async fn expose_git_credential_to_children_is_a_toml_backed_boolean() {
        let path = "sourceControl.github.exposeGitCredentialToChildren";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert!(matches!(def.ty, SettingType::Boolean));
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "sourceControl");
        assert_eq!(def.default_value, Some(json!(true)));

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-gitcred-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-gitcred-{tag}.toml"));
        // Start from an empty file (not the commented default template) so
        // origins read `default` until the key is explicitly written.
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Default-on with `default` origin.
        let got = svc.get(path).await.expect("get default");
        assert_eq!(got["value"], json!(true));
        assert_eq!(got["origin"], json!("default"));

        // Opt-out persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([{ "path": path, "value": false }]))
            .await
            .expect("update");
        let got = svc.get(path).await.expect("get updated");
        assert_eq!(got["value"], json!(false));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("exposeGitCredentialToChildren"), "{text}");
        assert_eq!(
            store.get_setting(path).await.expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Reset restores the default and strips the key from the file.
        let reset = svc.reset(path).await.expect("reset");
        assert_eq!(reset["value"], json!(true));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(
            !text.contains("exposeGitCredentialToChildren = false"),
            "{text}"
        );
        let got = svc.get(path).await.expect("get after reset");
        assert_eq!(got["value"], json!(true));
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `voice.openai.model` is a TOML-backed enum with the three supported
    /// `OpenAI` transcription models and the gpt-4o-transcribe default; it
    /// persists through `settings.update` to config.toml (never `SQLite`).
    #[tokio::test]
    async fn voice_openai_model_is_a_toml_backed_enum() {
        let path = "voice.openai.model";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert!(matches!(
            def.ty,
            SettingType::Enum(&["gpt-4o-transcribe", "gpt-4o-mini-transcribe", "whisper-1"])
        ));
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "voice");
        assert_eq!(def.default_value, Some(json!("gpt-4o-transcribe")));

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-voicemodel-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-voicemodel-{tag}.toml"));
        // Start from an empty file (not the commented default template) so
        // origins read `default` until the key is explicitly written.
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Default with `default` origin.
        let got = svc.get(path).await.expect("get default");
        assert_eq!(got["value"], json!("gpt-4o-transcribe"));
        assert_eq!(got["origin"], json!("default"));

        // A picked model persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([{ "path": path, "value": "whisper-1" }]))
            .await
            .expect("update");
        let got = svc.get(path).await.expect("get updated");
        assert_eq!(got["value"], json!("whisper-1"));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("whisper-1"), "{text}");
        assert_eq!(
            store.get_setting(path).await.expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Values outside the enum are rejected.
        svc.update(&json!([{ "path": path, "value": "gpt-5-transcribe" }]))
            .await
            .expect_err("out-of-enum value must be rejected");

        // Reset restores the default.
        let reset = svc.reset(path).await.expect("reset");
        assert_eq!(reset["value"], json!("gpt-4o-transcribe"));
        let got = svc.get(path).await.expect("get after reset");
        assert_eq!(got["value"], json!("gpt-4o-transcribe"));
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `voice.language` is a TOML-backed optional string (no default —
    /// unset means provider auto-detection); it persists through
    /// `settings.update` to config.toml (never `SQLite`).
    #[tokio::test]
    async fn voice_language_is_a_toml_backed_optional_string() {
        let path = "voice.language";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert!(matches!(def.ty, SettingType::String));
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "voice");
        assert_eq!(def.default_value, None);

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-voicelang-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-voicelang-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Unset by default with `default` origin.
        let got = svc.get(path).await.expect("get default");
        assert_eq!(got["value"], serde_json::Value::Null);
        assert_eq!(got["origin"], json!("default"));

        // A stored language persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([{ "path": path, "value": "de" }]))
            .await
            .expect("update");
        let got = svc.get(path).await.expect("get updated");
        assert_eq!(got["value"], json!("de"));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("de"), "{text}");
        assert_eq!(
            store.get_setting(path).await.expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Reset restores the unset default.
        let reset = svc.reset(path).await.expect("reset");
        assert_eq!(reset["value"], serde_json::Value::Null);
        let got = svc.get(path).await.expect("get after reset");
        assert_eq!(got["value"], serde_json::Value::Null);
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `model.defaultReasoningEffort` is a TOML-backed optional string (no
    /// default — unset means no global effort preference), stored as-is under
    /// `[model]`; it round-trips through `settings.update` / `settings.reset`
    /// to config.toml (never `SQLite`), and a blank string clears it to unset.
    #[tokio::test]
    async fn model_default_reasoning_effort_is_a_toml_backed_optional_string() {
        let path = "model.defaultReasoningEffort";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert!(matches!(def.ty, SettingType::String));
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "providers");
        assert_eq!(def.default_value, None);

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-effort-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-effort-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Unset by default with `default` origin.
        let got = svc.get(path).await.expect("get default");
        assert_eq!(got["value"], serde_json::Value::Null);
        assert_eq!(got["origin"], json!("default"));

        // A stored level persists verbatim to config.toml, never SQLite.
        svc.update(&json!([{ "path": path, "value": "xhigh" }]))
            .await
            .expect("update");
        let got = svc.get(path).await.expect("get updated");
        assert_eq!(got["value"], json!("xhigh"));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("defaultReasoningEffort"), "{text}");
        assert!(text.contains("xhigh"), "{text}");
        assert_eq!(
            store.get_setting(path).await.expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // A fresh registry over the same file reads the value back (round-trip).
        let reloaded = SettingsRegistry::load(&config_path).expect("reload registry");
        assert_eq!(reloaded.get(path), Some(json!("xhigh")));
        assert_eq!(
            reloaded
                .snapshot()
                .effective
                .model
                .default_reasoning_effort
                .as_deref(),
            Some("xhigh")
        );

        // An empty string clears it back to unset: the effective value is
        // `None` and the wire value reads `null`, so no client (and no future
        // resolution step) ever observes an explicit empty effort.
        svc.update(&json!([{ "path": path, "value": "" }]))
            .await
            .expect("clear with empty string");
        let got = svc.get(path).await.expect("get after clear");
        assert_eq!(
            got["value"],
            serde_json::Value::Null,
            "an empty string must read as unset"
        );
        assert_eq!(
            registry
                .snapshot()
                .effective
                .model
                .default_reasoning_effort
                .as_deref(),
            None,
            "an empty string must read as unset"
        );
        let reloaded = SettingsRegistry::load(&config_path).expect("reload registry");
        assert_eq!(
            reloaded.get(path),
            Some(serde_json::Value::Null),
            "a blank value in the file must read as unset"
        );

        // Reset restores the unset default.
        let reset = svc.reset(path).await.expect("reset");
        assert_eq!(reset["value"], serde_json::Value::Null);
        let got = svc.get(path).await.expect("get after reset");
        assert_eq!(got["value"], serde_json::Value::Null);
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `voice.workspaceVocabulary.maxTerms` is a TOML-backed bounded number
    /// (default 50, min 0, max 100 — 0 disables derivation and injection;
    /// PROTOCOL §5.12, v4.6); it persists through `settings.update` to
    /// config.toml (never `SQLite`) and rejects out-of-range values.
    #[tokio::test]
    #[allow(clippy::float_cmp)] // asserting exact literal bounds from the setting definition
    async fn voice_workspace_vocabulary_max_terms_is_a_bounded_toml_number() {
        let path = "voice.workspaceVocabulary.maxTerms";
        let def = find_definition(path).unwrap_or_else(|| panic!("{path} missing"));
        assert!(matches!(
            def.ty,
            SettingType::Number {
                min: Some(min),
                max: Some(max)
            } if min == 0.0 && max == 100.0
        ));
        assert!(!def.sensitive);
        assert!(!def.read_only);
        assert_eq!(def.category, "voice");
        assert_eq!(def.default_value, Some(json!(50.0)));

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-voicewsvocab-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-voicewsvocab-{tag}.toml"));
        std::fs::write(&config_path, "").expect("write empty config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // Default with `default` origin.
        let got = svc.get(path).await.expect("get default");
        assert_eq!(got["value"], json!(50.0));
        assert_eq!(got["origin"], json!("default"));

        // An updated cap persists to config.toml with `file` origin, never SQLite.
        svc.update(&json!([{ "path": path, "value": 0 }]))
            .await
            .expect("update to 0 (disable)");
        let got = svc.get(path).await.expect("get updated");
        assert_eq!(got["value"], json!(0.0));
        assert_eq!(got["origin"], json!("file"));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(text.contains("maxTerms = 0"), "{text}");
        assert_eq!(
            store.get_setting(path).await.expect("read settings table"),
            None,
            "TOML-backed keys must never write a SQLite settings row"
        );

        // Out-of-range and wrong-type values are rejected.
        svc.update(&json!([{ "path": path, "value": 101 }]))
            .await
            .expect_err("over max must be rejected");
        svc.update(&json!([{ "path": path, "value": -1 }]))
            .await
            .expect_err("under min must be rejected");
        svc.update(&json!([{ "path": path, "value": "fifty" }]))
            .await
            .expect_err("non-number must be rejected");

        // Reset restores the default.
        let reset = svc.reset(path).await.expect("reset");
        assert_eq!(reset["value"], json!(50.0));
        let got = svc.get(path).await.expect("get after reset");
        assert_eq!(got["value"], json!(50.0));
        assert_eq!(got["origin"], json!("default"));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// `model.workspaceOverrides` is retired (monorepo#1000) but old clients
    /// still write it on every workspace-scoped model pick: `settings.update`
    /// tolerates-and-ignores the entry (nothing persisted, nothing echoed in
    /// `applied`) instead of rejecting the batch, while the rest of a mixed
    /// batch still applies. `settings.get`/`settings.reset` reject the path
    /// as unknown like any other uncataloged key.
    #[tokio::test]
    async fn workspace_overrides_update_is_tolerated_and_ignored() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-wsov-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-wsov-{tag}.toml"));
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        // A retired-path-only batch succeeds with nothing applied.
        let applied = svc
            .update(&json!([{
                "path": "model.workspaceOverrides",
                "value": { "ws-1": "auggie:opus" }
            }]))
            .await
            .expect("retired path must be tolerated");
        assert_eq!(applied, Vec::<Value>::new());
        // Even a malformed entry (no 'value') is ignored, not validated.
        let applied = svc
            .update(&json!([{ "path": "model.workspaceOverrides" }]))
            .await
            .expect("retired path must be tolerated without a value");
        assert_eq!(applied, Vec::<Value>::new());
        // Nothing was persisted to SQLite or config.toml.
        assert_eq!(
            store
                .get_setting("model.workspaceOverrides")
                .await
                .expect("read settings table"),
            None
        );
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("workspaceOverrides"), "{text}");

        // A mixed batch still applies the live entries.
        let applied = svc
            .update(&json!([
                { "path": "model.workspaceOverrides", "value": { "ws-1": "m1" } },
                { "path": "workspace.branchPrefix", "value": "feat/" },
            ]))
            .await
            .expect("mixed batch must apply the live entry");
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0]["path"], "workspace.branchPrefix");

        // get/reset reject the retired path as unknown.
        assert!(matches!(
            svc.get("model.workspaceOverrides").await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            svc.reset("model.workspaceOverrides").await,
            Err(Error::InvalidParams(_))
        ));

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// monorepo#1729: the renamed `backgroundAgents.*` paths are tolerated-
    /// and-ignored by `settings.update` (pre-rename clients keep writing them)
    /// and rejected as unknown by `settings.get`/`settings.reset`.
    #[tokio::test]
    async fn background_agent_paths_update_is_tolerated_and_ignored() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-bgagents-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-bgagents-{tag}.toml"));
        let registry = SettingsRegistry::load(&config_path).expect("load registry");
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let secrets = AsyncSecretStore::new(secrets);
        let svc = SettingsService::new(&store, &secrets, Some(&registry));

        let applied = svc
            .update(&json!([
                { "path": "backgroundAgents.defaultModel", "value": "auggie:haiku" },
                { "path": "backgroundAgents.typeOverrides", "value": { "commit": "m" } },
                { "path": "backgroundAgents.providerSettings", "value": {} },
            ]))
            .await
            .expect("renamed paths must be tolerated");
        assert_eq!(applied, Vec::<Value>::new());
        assert_eq!(registry.get("quickActions.defaultModel"), Some(Value::Null));
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("backgroundAgents"), "{text}");

        for path in RETIRED_BACKGROUND_AGENT_PATHS {
            assert!(matches!(svc.get(path).await, Err(Error::InvalidParams(_))));
            assert!(matches!(
                svc.reset(path).await,
                Err(Error::InvalidParams(_))
            ));
        }

        // A batch mixing a retired path with a live one still applies the live
        // entry instead of failing wholesale.
        let applied = svc
            .update(&json!([
                { "path": "backgroundAgents.defaultModel", "value": "auggie:haiku" },
                { "path": "quickActions.defaultModel", "value": "auggie:opus" },
            ]))
            .await
            .expect("mixed batch must apply its live entry");
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0]["path"], "quickActions.defaultModel");
        assert_eq!(
            registry.get("quickActions.defaultModel"),
            Some(json!("auggie:opus"))
        );

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// monorepo#1729: a `config.toml` still carrying `[backgroundAgents]` has
    /// its values carried over into the unset `quickActions.*` keys, and the
    /// legacy table is then stripped by [`import_legacy_settings`].
    #[tokio::test]
    async fn quick_action_migration_carries_over_legacy_table() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-qamig-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-qamig-{tag}.toml"));
        std::fs::write(
            &config_path,
            "[backgroundAgents]\ndefaultModel = \"auggie:haiku\"\ntypeOverrides = { commit = \"auggie:fast\" }\nproviderSettings = { claude-code = { mode = \"fast\" } }\n",
        )
        .expect("seed legacy config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");

        migrate_quick_action_settings(&registry).expect("migrate");
        assert_eq!(
            registry.get("quickActions.defaultModel"),
            Some(json!("auggie:haiku"))
        );
        assert_eq!(
            registry.get("quickActions.typeOverrides"),
            Some(json!({ "commit": "auggie:fast" }))
        );
        assert_eq!(
            registry.get("quickActions.providerSettings"),
            Some(json!({ "claude-code": { "mode": "fast" } })),
            "the structurally-validated member carries over too"
        );

        import_legacy_settings(&registry, &store)
            .await
            .expect("import");
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("backgroundAgents"), "{text}");
        assert!(text.contains("quickActions"), "{text}");

        // Second boot: the stripped file reloads, the migration is a no-op and
        // the carried-over values stay put.
        let registry2 = SettingsRegistry::load(&config_path).expect("reload registry");
        migrate_quick_action_settings(&registry2).expect("migrate again");
        assert_eq!(
            registry2.get("quickActions.defaultModel"),
            Some(json!("auggie:haiku"))
        );
        assert_eq!(
            registry2.get("quickActions.providerSettings"),
            Some(json!({ "claude-code": { "mode": "fast" } }))
        );

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// PR #1010 review: one malformed legacy member must not take its valid
    /// siblings down with it — members are applied individually, so the bad
    /// one is skipped and the rest still carry over before the legacy table
    /// is stripped. A member with no `quickActions.*` counterpart is dropped
    /// (warned about) rather than failing the carry-over.
    #[tokio::test]
    async fn quick_action_migration_skips_only_the_malformed_member() {
        let tag = uuid::Uuid::new_v4();
        let config_path = std::env::temp_dir().join(format!("intentd-settings-qabad-{tag}.toml"));
        std::fs::write(
            &config_path,
            "[backgroundAgents]\ndefaultModel = 1\ntypeOverrides = { commit = \"auggie:fast\" }\nsomethingRetired = true\n",
        )
        .expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");

        migrate_quick_action_settings(&registry).expect("migrate");
        assert_eq!(
            registry.get("quickActions.typeOverrides"),
            Some(json!({ "commit": "auggie:fast" })),
            "a valid sibling must survive a malformed member"
        );
        assert_eq!(
            registry.origin("quickActions.defaultModel"),
            Some(SettingOrigin::Default),
            "the malformed member must be skipped, not persisted"
        );

        let _ = std::fs::remove_file(&config_path);
    }

    /// The carry-over never clobbers a `quickActions.*` key the user already
    /// set (a re-run after the first migration, or a deliberate re-pick).
    #[tokio::test]
    async fn quick_action_migration_keeps_existing_value() {
        let tag = uuid::Uuid::new_v4();
        let config_path = std::env::temp_dir().join(format!("intentd-settings-qakeep-{tag}.toml"));
        std::fs::write(
            &config_path,
            "[backgroundAgents]\ndefaultModel = \"auggie:haiku\"\n\n[quickActions]\ndefaultModel = \"auggie:opus\"\n",
        )
        .expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("load registry");

        migrate_quick_action_settings(&registry).expect("migrate");
        assert_eq!(
            registry.get("quickActions.defaultModel"),
            Some(json!("auggie:opus")),
            "an already-set quick-action key must win over the legacy value"
        );

        let _ = std::fs::remove_file(&config_path);
    }

    /// [`cleanup_retired_settings`] deletes the stale `SQLite` row left behind
    /// by the retired per-workspace override layer, and is an idempotent
    /// no-op when the row is absent.
    #[tokio::test]
    async fn cleanup_retired_settings_deletes_stale_row() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-retired-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");

        store
            .set_setting("model.workspaceOverrides", r#"{"ws1":"m1"}"#)
            .await
            .expect("seed stale row");
        cleanup_retired_settings(&store).await.expect("cleanup");
        assert_eq!(
            store
                .get_setting("model.workspaceOverrides")
                .await
                .expect("read settings table"),
            None,
            "stale row must be deleted"
        );
        // Second run: nothing to delete, still Ok.
        cleanup_retired_settings(&store).await.expect("idempotent");

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// [`migrate_default_vocabulary`] deletes a stored `voice.vocabulary`
    /// row that exactly matches the retired 17-term default (so the new
    /// `["Intent"]` default applies), and is an idempotent no-op afterwards.
    #[tokio::test]
    async fn migrate_default_vocabulary_deletes_untouched_old_default() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-vocab-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");

        let legacy = serde_json::to_string(&json!(crate::voice_ops::LEGACY_DEFAULT_VOCABULARY))
            .expect("encode legacy default");
        store
            .set_setting(VOICE_VOCABULARY_PATH, &legacy)
            .await
            .expect("seed old default row");
        migrate_default_vocabulary(&store).await.expect("migrate");
        assert_eq!(
            store
                .get_setting(VOICE_VOCABULARY_PATH)
                .await
                .expect("read settings table"),
            None,
            "untouched old default must be deleted"
        );
        // Second run: no row, still Ok.
        migrate_default_vocabulary(&store)
            .await
            .expect("idempotent");

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// [`migrate_default_vocabulary`] never touches a user-modified list —
    /// including reorderings, subsets, and supersets of the old default —
    /// or a malformed stored blob.
    #[tokio::test]
    async fn migrate_default_vocabulary_preserves_user_modified_lists() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-vocab-user-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");

        let mut reordered: Vec<&str> = crate::voice_ops::LEGACY_DEFAULT_VOCABULARY.to_vec();
        reordered.reverse();
        let mut extended: Vec<&str> = crate::voice_ops::LEGACY_DEFAULT_VOCABULARY.to_vec();
        extended.push("Endara");
        let cases = [
            serde_json::to_string(&json!(["Endara", "TOON"])).unwrap(),
            serde_json::to_string(&json!(reordered)).unwrap(),
            serde_json::to_string(&json!(extended)).unwrap(),
            serde_json::to_string(&json!(crate::voice_ops::LEGACY_DEFAULT_VOCABULARY[..16]))
                .unwrap(),
            "not-json".to_string(),
        ];
        for stored in cases {
            store
                .set_setting(VOICE_VOCABULARY_PATH, &stored)
                .await
                .expect("seed row");
            migrate_default_vocabulary(&store).await.expect("migrate");
            assert_eq!(
                store
                    .get_setting(VOICE_VOCABULARY_PATH)
                    .await
                    .expect("read settings table"),
                Some(stored.clone()),
                "user-modified value must be preserved"
            );
        }

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// Boot-time legacy handling: a `config.toml` carrying the retired
    /// `model.workspaceOverrides` key loads (tolerated), the value is
    /// DISCARDED (the key has no catalog entry since monorepo#1000, so
    /// nothing lands in `SQLite`), the key is stripped from the file with
    /// comments preserved, and a second import is a no-op.
    #[tokio::test]
    async fn import_legacy_settings_discards_and_strips() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-legacy-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path = std::env::temp_dir().join(format!("intentd-settings-legacy-{tag}.toml"));
        std::fs::write(
            &config_path,
            "# my config\n[model]\ndefault = \"m0\"\nworkspaceOverrides = { ws1 = \"m1\" }\n",
        )
        .expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("legacy key must load");

        let stripped = import_legacy_settings(&registry, &store)
            .await
            .expect("import");
        assert_eq!(stripped, vec!["model.workspaceOverrides".to_string()]);

        // Value was discarded, never imported into SQLite.
        assert_eq!(
            store
                .get_setting("model.workspaceOverrides")
                .await
                .expect("read settings table"),
            None
        );
        // File stripped, comment + sibling key preserved.
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("workspaceOverrides"), "{text}");
        assert!(text.contains("# my config"), "{text}");
        assert!(text.contains("default = \"m0\""), "{text}");

        // Second boot: clean load, import is a no-op.
        let registry2 = SettingsRegistry::load(&config_path).expect("clean reload");
        assert!(registry2.legacy_values().is_empty());
        assert_eq!(
            import_legacy_settings(&registry2, &store)
                .await
                .expect("no-op import"),
            Vec::<String>::new()
        );

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// A hand-edited file can carry the legacy key as a TOML table header
    /// (`[model.workspaceOverrides]`) instead of an inline table. The capture
    /// and strip paths must handle that form too — the one-shot migration
    /// would otherwise leave the retired key in the file forever.
    #[tokio::test]
    async fn import_legacy_settings_handles_table_header_form() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-legacyhdr-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-legacyhdr-{tag}.toml"));
        std::fs::write(
            &config_path,
            "# my config\n[git]\nautoCommit = false\n\n[model.workspaceOverrides]\nws1 = \"m1\"\nws2 = \"m2\"\n",
        )
        .expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("legacy header form must load");

        let stripped = import_legacy_settings(&registry, &store)
            .await
            .expect("import");
        assert_eq!(stripped, vec!["model.workspaceOverrides".to_string()]);

        // Discarded, not imported (no catalog entry since monorepo#1000).
        assert_eq!(
            store
                .get_setting("model.workspaceOverrides")
                .await
                .expect("read settings table"),
            None
        );
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("workspaceOverrides"), "{text}");
        assert!(text.contains("# my config"), "{text}");
        assert!(text.contains("autoCommit = false"), "{text}");

        let registry2 = SettingsRegistry::load(&config_path).expect("clean reload");
        assert!(registry2.legacy_values().is_empty());

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// The strip step is best-effort: a failed file rewrite (e.g. unwritable
    /// config directory) must not fail the import — the daemon continues and
    /// the next boot retries the strip. The rewrite is an atomic temp-file +
    /// rename in the config's directory, so a read-only directory makes it
    /// fail.
    #[cfg(unix)]
    #[tokio::test]
    async fn import_legacy_settings_tolerates_strip_failure() {
        use std::os::unix::fs::PermissionsExt;

        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-legacyro-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_dir = std::env::temp_dir().join(format!("intentd-settings-legacyro-{tag}"));
        std::fs::create_dir(&config_dir).expect("mkdir");
        let config_path = config_dir.join("config.toml");
        let body = "[model]\nworkspaceOverrides = { ws1 = \"m1\" }\n";
        std::fs::write(&config_path, body).expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("legacy key must load");

        // Make the directory read-only so the temp-file rewrite fails.
        std::fs::set_permissions(&config_dir, std::fs::Permissions::from_mode(0o555))
            .expect("chmod dir read-only");

        let stripped = import_legacy_settings(&registry, &store)
            .await
            .expect("import must succeed despite strip failure");
        assert_eq!(stripped, Vec::<String>::new(), "nothing was stripped");

        // The retired value was discarded (never imported)…
        assert_eq!(
            store
                .get_setting("model.workspaceOverrides")
                .await
                .expect("read settings table"),
            None
        );
        // …and the file is untouched for the next-boot retry.
        assert_eq!(
            std::fs::read_to_string(&config_path).expect("read config"),
            body
        );

        std::fs::set_permissions(&config_dir, std::fs::Permissions::from_mode(0o755))
            .expect("chmod dir back");
        let _ = std::fs::remove_dir_all(&config_dir);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }

    /// Retired-key values are discarded regardless of shape (no catalog
    /// entry to validate against) but still stripped so the daemon does not
    /// re-warn forever.
    #[tokio::test]
    async fn import_legacy_settings_discards_invalid_values() {
        for body in [
            "[model]\nworkspaceOverrides = \"not-an-object\"\n",
            "[model]\nworkspaceOverrides = [\"m1\"]\n",
            "[model]\nworkspaceOverrides = { ws1 = 42 }\n",
        ] {
            let tag = uuid::Uuid::new_v4();
            let tmp = std::env::temp_dir().join(format!("intentd-settings-legacybad-{tag}.db"));
            let store = Store::open(&tmp).await.expect("open store");
            let config_path =
                std::env::temp_dir().join(format!("intentd-settings-legacybad-{tag}.toml"));
            std::fs::write(&config_path, body).expect("seed config");
            let registry = SettingsRegistry::load(&config_path).expect("legacy key must load");

            let stripped = import_legacy_settings(&registry, &store)
                .await
                .expect("import");
            assert_eq!(stripped, vec!["model.workspaceOverrides".to_string()]);
            assert_eq!(
                store
                    .get_setting("model.workspaceOverrides")
                    .await
                    .expect("read settings table"),
                None,
                "invalid legacy value must be discarded, not imported: {body}"
            );
            let text = std::fs::read_to_string(&config_path).expect("read config");
            assert!(!text.contains("workspaceOverrides"), "{text}");

            let _ = std::fs::remove_file(&config_path);
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                    "{}{suffix}",
                    tmp.display()
                )));
            }
        }
    }

    /// The retired `[ai]` table has no catalog entry at all: a config.toml
    /// still carrying it boots (tolerated), the values are DISCARDED (never
    /// imported into `SQLite`), and the whole table is stripped from the file
    /// with comments and sibling keys preserved.
    #[tokio::test]
    async fn import_legacy_settings_discards_and_strips_ai_table() {
        let tag = uuid::Uuid::new_v4();
        let tmp = std::env::temp_dir().join(format!("intentd-settings-legacyai-{tag}.db"));
        let store = Store::open(&tmp).await.expect("open store");
        let config_path =
            std::env::temp_dir().join(format!("intentd-settings-legacyai-{tag}.toml"));
        std::fs::write(
            &config_path,
            "# my config\n[git]\nautoCommit = false\n\n[ai]\napiUrl = \"https://api.example\"\nmodel = \"m1\"\ntemperature = 0.5\nmaxTokens = 2048\nstreamingSpeed = 10.0\n",
        )
        .expect("seed config");
        let registry = SettingsRegistry::load(&config_path).expect("legacy [ai] must load");

        let stripped = import_legacy_settings(&registry, &store)
            .await
            .expect("import");
        assert_eq!(stripped, vec!["ai".to_string()]);

        // Nothing landed in SQLite — the whole group is discarded.
        for path in [
            "ai",
            "ai.apiUrl",
            "ai.model",
            "ai.temperature",
            "ai.maxTokens",
            "ai.streamingSpeed",
        ] {
            assert_eq!(
                store.get_setting(path).await.expect("read settings table"),
                None,
                "{path} must not be imported"
            );
        }
        // File stripped; comment + sibling table preserved.
        let text = std::fs::read_to_string(&config_path).expect("read config");
        assert!(!text.contains("[ai]"), "{text}");
        assert!(!text.contains("apiUrl"), "{text}");
        assert!(text.contains("# my config"), "{text}");
        assert!(text.contains("autoCommit = false"), "{text}");

        // Second boot: clean load, import is a no-op.
        let registry2 = SettingsRegistry::load(&config_path).expect("clean reload");
        assert!(registry2.legacy_values().is_empty());

        let _ = std::fs::remove_file(&config_path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(std::path::PathBuf::from(format!(
                "{}{suffix}",
                tmp.display()
            )));
        }
    }
}
