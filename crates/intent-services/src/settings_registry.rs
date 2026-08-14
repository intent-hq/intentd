//! Layered runtime settings registry backing the `config.toml` migration.
//!
//! Effective values are layered as **schema defaults < config.toml <
//! startup pins (env/CLI flags)**. The registry owns:
//!
//! - cheap concurrent reads via an `Arc` snapshot (typed [`SettingsFile`]
//!   access plus JSON-value access keyed by dotted wire path),
//! - per-key origin tracking (`default` | `file` | `flag`),
//! - a pin API for the composition root — pinned keys reject wire mutation
//!   ([`Error::InvalidParams`], "overridden by startup flag") and their file
//!   value is ignored,
//! - [`SettingsRegistry::apply`]: validate against the typed schema, mutate
//!   in-memory values, and atomically rewrite `config.toml` (temp file +
//!   rename) via `toml_edit`, preserving user comments/layout — only touched
//!   keys change in the document,
//! - [`SettingsRegistry::reload`]: strict re-parse of externally edited file
//!   text (the live-reload watcher feeds this),
//! - a `tokio::sync::watch` channel notifying subscribers of changed key
//!   sets, and
//! - a self-write guard (generation counter + a time-bounded history of
//!   content hashes of recently self-written files) so the watcher can
//!   distinguish self-writes from external edits — keeping a *history*
//!   (not just the last write) means a stale or coalesced watcher read that
//!   observes an earlier self-write is still recognized and skipped.
//!
//! Keys are addressed by their dotted **wire path** (e.g.
//! `server.wsApi.enabled`), which maps one-to-one onto the camelCase nested
//! TOML schema. Secrets and the SQLite-backed machine-state blobs are not the
//! registry's concern: they are absent from [`KNOWN_PATHS`] and read as
//! `None`/unknown.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use intent_core::settings_file::{LegacySettings, SettingsFile};
use intent_core::{Error, Result};
use serde_json::Value;
use tokio::sync::watch;
use toml_edit::DocumentMut;

/// Every TOML-backed setting, addressed by its dotted wire path. Mirrors the
/// [`SettingsFile`] schema leaf-for-leaf (maps such as `providers.paths` are
/// single leaves — their dynamic children are not individually addressable).
pub const KNOWN_PATHS: &[&str] = &[
    "providers.active",
    "providers.enabled",
    "providers.paths",
    "model.default",
    "model.providerDefaults",
    "model.defaultReasoningEffort",
    "quickActions.defaultModel",
    "quickActions.typeOverrides",
    "quickActions.providerSettings",
    "specialists.default",
    "workspace.branchPrefix",
    "workspace.worktreesLocation",
    "workspace.sshKeyPath",
    "workspace.defaultShell",
    "workspace.cowIsolation",
    "git.autoCommit",
    "mcp.enableUserServers",
    "mcp.disabledServers",
    "notifications.enabled",
    "notifications.soundEnabled",
    "notifications.soundOnlyWhenUnfocused",
    "notifications.volume",
    "rtk.enabled",
    "server.socketPath",
    "server.bindAddress",
    "server.port",
    "server.originAllowList",
    "server.maxOutstandingRpcs",
    "server.wsApi.enabled",
    "server.wsApi.port",
    "server.tls.enabled",
    "server.auth.enabled",
    "sourceControl.activeProvider",
    "sourceControl.github.tokenSource",
    "sourceControl.github.apiBaseUrl",
    "sourceControl.github.oauthClientId",
    "sourceControl.github.exposeGitCredentialToChildren",
    "accounts.sentry.organization",
    "voice.provider",
    "voice.language",
    "voice.openai.model",
    "voice.workspaceVocabulary.maxTerms",
    "context.enabled",
    "context.auggiePath",
    "context.allowIndexing",
    "storage.dataDir",
    "workspaces.root",
    "logging.level",
    "agents.maxConcurrent",
    "agents.maxConcurrentAdapters",
    "agents.memoryBudgetMb",
    "agents.idleReapMinutes",
    "agents.flushQueuedMessages",
    "events.streamRetentionHours",
    "workspaceApi.maxOutputChars",
    "workspaceApi.toonOutput",
    "agentFeatures.backgroundHooks",
    "agentFeatures.hostExec",
    "agentFeatures.scripts",
    "agentFeatures.terminalAccess",
    "agentFeatures.browserAutomation",
    "agentFeatures.richChatBlocks",
    "agentFeatures.structuredQuestions",
    "agentFeatures.attentionRequests",
    "agentFeatures.stateSnapshot",
    "agentFeatures.prMonitor",
    "agentFeatures.taskGraph",
    "prMonitor.debounceSeconds",
    "prMonitor.pollSeconds",
];

/// Where a key's effective value comes from (lowest to highest precedence).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SettingOrigin {
    /// Schema default — the key is absent from the file and not pinned.
    Default,
    /// Explicitly present in `config.toml`.
    File,
    /// Pinned at boot by a startup flag / env var; file value ignored.
    Flag,
}

impl SettingOrigin {
    /// Wire spelling of the origin (`default` | `file` | `flag`).
    pub fn as_str(&self) -> &'static str {
        match self {
            SettingOrigin::Default => "default",
            SettingOrigin::File => "file",
            SettingOrigin::Flag => "flag",
        }
    }
}

/// Payload broadcast on the watch channel after `apply`/`reload` changes
/// effective values. `generation` is the self-write generation at send time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SettingsChanged {
    /// Self-write generation when the change was published.
    pub generation: u64,
    /// Dotted wire paths whose **effective** value changed.
    pub changed: BTreeSet<String>,
}

/// How many recent self-write stamps the guard keeps. Under rapid successive
/// writes a debounced watcher read can observe the file at any of the last
/// few self-writes (stale read across the atomic rename, or a coalesced
/// event burst processed after later writes), so one stamp is not enough.
const SELF_WRITE_HISTORY: usize = 8;

/// How long a self-write stamp counts as "recent" for the guard. Watcher
/// reads land within the debounce window (milliseconds); anything older is
/// a human-timescale edit that should reload normally even if it happens to
/// byte-match an old self-write.
const SELF_WRITE_WINDOW: Duration = Duration::from_secs(10);

/// Identity of one file content the registry itself wrote — the file watcher
/// compares against the recent history of these to suppress self-write
/// events (see [`SettingsRegistry::is_self_write`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteStamp {
    /// Monotonic counter, incremented on every successful self-write.
    pub generation: u64,
    /// Hash of the exact file content written (see
    /// [`SettingsRegistry::is_self_write`]).
    pub content_hash: u64,
}

/// A boot-time pin: the value that overrides the file, plus the flag/env
/// label used in rejection messages (e.g. `--insecure`, `INTENTD_TCP_PORT`).
#[derive(Debug, Clone)]
struct Pin {
    value: Value,
    flag: String,
}

/// Mutation-side state, guarded by a `Mutex`. Readers never touch this — they
/// clone the `Arc<SettingsSnapshot>` instead.
struct Inner {
    /// Typed parse of the file (schema defaults merged with file values).
    file: SettingsFile,
    /// Raw document for comment-preserving write-back.
    doc: DocumentMut,
    /// Legacy values captured at load
    /// ([`intent_core::settings_file::LEGACY_SETTINGS_PATHS`] keys found in
    /// the file), pending the one-time boot import-and-strip.
    legacy: LegacySettings,
    /// Boot-time pins keyed by dotted wire path.
    pins: BTreeMap<String, Pin>,
    /// Self-write generation counter.
    generation: u64,
    /// Recent self-written file contents (oldest first, newest last), capped
    /// at [`SELF_WRITE_HISTORY`] entries; each stamp carries the write time
    /// so only writes within [`SELF_WRITE_WINDOW`] count as self-writes.
    recent_writes: VecDeque<(WriteStamp, Instant)>,
}

impl Inner {
    /// Record a successful self-write of `text`: bump the generation and
    /// push its stamp onto the bounded history.
    fn record_write(&mut self, text: &str) {
        self.generation += 1;
        if self.recent_writes.len() == SELF_WRITE_HISTORY {
            self.recent_writes.pop_front();
        }
        self.recent_writes.push_back((
            WriteStamp {
                generation: self.generation,
                content_hash: content_hash(text),
            },
            Instant::now(),
        ));
    }
}

/// Immutable view of the effective settings, cheap to clone via `Arc`.
#[derive(Debug, Clone)]
pub struct SettingsSnapshot {
    /// Typed effective values (defaults ⊕ file ⊕ pins) for direct access,
    /// e.g. `snapshot.effective.server.ws_api.enabled`.
    pub effective: SettingsFile,
    /// The same values as a nested JSON tree, for path-keyed access.
    effective_json: Value,
    /// Origin per known dotted wire path.
    origins: BTreeMap<String, SettingOrigin>,
}

impl SettingsSnapshot {
    /// Effective JSON value for a dotted wire path. `None` for unknown paths
    /// (including secrets and SQLite-backed state blobs, which are not this
    /// registry's concern). Known-but-unset optional keys read `Some(Null)`.
    pub fn get(&self, path: &str) -> Option<Value> {
        if !KNOWN_PATHS.contains(&path) {
            return None;
        }
        json_get(&self.effective_json, path).cloned()
    }

    /// Origin of a dotted wire path, or `None` when unknown.
    pub fn origin(&self, path: &str) -> Option<SettingOrigin> {
        self.origins.get(path).copied()
    }

    /// All known paths with their origins (for `settings.list`-style views).
    pub fn origins(&self) -> &BTreeMap<String, SettingOrigin> {
        &self.origins
    }
}

/// Layered runtime settings store. See the module docs for the full contract.
pub struct SettingsRegistry {
    path: PathBuf,
    inner: Mutex<Inner>,
    snapshot: RwLock<Arc<SettingsSnapshot>>,
    tx: watch::Sender<SettingsChanged>,
}

impl SettingsRegistry {
    /// Load (or initialize) `config.toml` at `path` and build the registry.
    /// Missing file ⇒ the fully-commented default template is written first
    /// (via [`SettingsFile::load_or_init_with_legacy`]); malformed file ⇒
    /// error. Known [`intent_core::settings_file::LEGACY_SETTINGS_PATHS`]
    /// keys are tolerated — their values are captured for the boot-time
    /// import-and-strip
    /// ([`SettingsRegistry::legacy_values`] / [`SettingsRegistry::strip_legacy`]).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let (file, legacy) = SettingsFile::load_or_init_with_legacy(&path)?;
        let text = std::fs::read_to_string(&path).map_err(|e| {
            Error::Internal(format!("could not read config {}: {e}", path.display()))
        })?;
        let doc: DocumentMut = text.parse().map_err(|e| {
            Error::Internal(format!("could not parse config {}: {e}", path.display()))
        })?;
        let inner = Inner {
            file,
            doc,
            legacy,
            pins: BTreeMap::new(),
            generation: 0,
            recent_writes: VecDeque::new(),
        };
        let snapshot = Arc::new(build_snapshot(&inner)?);
        let (tx, _rx) = watch::channel(SettingsChanged::default());
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            snapshot: RwLock::new(snapshot),
            tx,
        })
    }

    /// Legacy values captured at load (dotted wire path → JSON value; empty
    /// when the file had none). Non-empty until [`SettingsRegistry::strip_legacy`]
    /// runs.
    pub fn legacy_values(&self) -> LegacySettings {
        self.inner
            .lock()
            .expect("settings registry lock poisoned")
            .legacy
            .clone()
    }

    /// One-time boot cleanup: remove every captured
    /// [`intent_core::settings_file::LEGACY_SETTINGS_PATHS`] key from
    /// `config.toml` with a comment-preserving rewrite (temp file +
    /// rename; only the legacy keys change in the document) and clear the
    /// captured map. No-op when nothing was captured. Returns the stripped
    /// paths. Callers run this only **after** the captured values are safely
    /// persisted elsewhere, so a failed import keeps the file intact for the
    /// next boot to retry.
    pub fn strip_legacy(&self) -> Result<Vec<String>> {
        let mut inner = self.inner.lock().expect("settings registry lock poisoned");
        if inner.legacy.is_empty() {
            return Ok(Vec::new());
        }
        let stripped: Vec<String> = inner.legacy.keys().cloned().collect();
        // Strip on a clone and only swap it in after the write succeeds, so a
        // failed rewrite leaves the in-memory document in sync with the file.
        let mut doc = inner.doc.clone();
        for path in &stripped {
            doc_remove(&mut doc, path);
        }
        let text = doc.to_string();
        atomic_write(&self.path, &text)?;
        inner.doc = doc;
        inner.record_write(&text);
        inner.legacy.clear();
        Ok(stripped)
    }

    /// The `config.toml` path this registry reads and writes.
    pub fn config_path(&self) -> &Path {
        &self.path
    }

    /// All dotted wire paths the registry manages.
    pub fn known_paths() -> &'static [&'static str] {
        KNOWN_PATHS
    }

    /// Current effective snapshot (cheap `Arc` clone; never blocks writers
    /// for longer than the pointer swap).
    pub fn snapshot(&self) -> Arc<SettingsSnapshot> {
        self.snapshot
            .read()
            .expect("settings snapshot lock poisoned")
            .clone()
    }

    /// Effective JSON value for a dotted wire path (see
    /// [`SettingsSnapshot::get`]).
    pub fn get(&self, path: &str) -> Option<Value> {
        self.snapshot().get(path)
    }

    /// Origin of a dotted wire path, or `None` when unknown.
    pub fn origin(&self, path: &str) -> Option<SettingOrigin> {
        self.snapshot().origin(path)
    }

    /// Subscribe to changed-key notifications from `apply`/`reload`.
    pub fn subscribe(&self) -> watch::Receiver<SettingsChanged> {
        self.tx.subscribe()
    }

    /// Identity of the most recent self-written file content, if any.
    pub fn write_stamp(&self) -> Option<WriteStamp> {
        self.inner
            .lock()
            .expect("settings registry lock poisoned")
            .recent_writes
            .back()
            .map(|(stamp, _)| *stamp)
    }

    /// Current self-write generation (0 until the first `apply` write).
    pub fn generation(&self) -> u64 {
        self.inner
            .lock()
            .expect("settings registry lock poisoned")
            .generation
    }

    /// Whether `text` is byte-identical to any *recent* self-write (within
    /// [`SELF_WRITE_WINDOW`], across the last [`SELF_WRITE_HISTORY`] writes)
    /// — the file watcher uses this to suppress events for the daemon's own
    /// writes, including stale/coalesced reads of an earlier write-back.
    pub fn is_self_write(&self, text: &str) -> bool {
        let hash = content_hash(text);
        self.inner
            .lock()
            .expect("settings registry lock poisoned")
            .recent_writes
            .iter()
            .any(|(stamp, at)| stamp.content_hash == hash && at.elapsed() <= SELF_WRITE_WINDOW)
    }

    /// Pin a key to `value` at boot (env/CLI precedence layer). `flag` names
    /// the startup flag/env var for rejection messages (e.g. `--insecure`,
    /// `INTENTD_TCP_PORT`). The pin overrides the file value; while pinned,
    /// the key rejects [`SettingsRegistry::apply`]. The pin value is
    /// validated against the typed schema. Does not notify subscribers —
    /// pinning happens at boot, before anyone subscribes.
    pub fn pin(&self, path: &str, value: Value, flag: &str) -> Result<()> {
        if !KNOWN_PATHS.contains(&path) {
            return Err(Error::InvalidParams(format!("unknown setting: {path}")));
        }
        let mut inner = self.inner.lock().expect("settings registry lock poisoned");
        let mut json = file_json(&inner.file)?;
        for (pin_path, pin) in &inner.pins {
            json_set(&mut json, pin_path, pin.value.clone());
        }
        json_set(&mut json, path, value.clone());
        typed_from_json(json, path)?;
        inner.pins.insert(
            path.to_string(),
            Pin {
                value,
                flag: flag.to_string(),
            },
        );
        self.swap_snapshot(&inner)?;
        Ok(())
    }

    /// Validate `changes` (dotted wire path → JSON value) against the typed
    /// schema, mutate the in-memory values, and atomically rewrite
    /// `config.toml` (temp file + rename), preserving comments/layout — only
    /// touched keys change in the document. `Null` clears a key back to its
    /// schema default (the key is removed from the file). Unknown paths and
    /// pinned keys are rejected with [`Error::InvalidParams`] before anything
    /// mutates. Returns the changed-key notice (also broadcast to
    /// subscribers when non-empty).
    pub fn apply(&self, changes: &[(String, Value)]) -> Result<SettingsChanged> {
        let mut inner = self.inner.lock().expect("settings registry lock poisoned");
        for (path, _) in changes {
            if !KNOWN_PATHS.contains(&path.as_str()) {
                return Err(Error::InvalidParams(format!("unknown setting: {path}")));
            }
            if let Some(pin) = inner.pins.get(path) {
                return Err(Error::InvalidParams(format!(
                    "{path} is overridden by startup flag {} and cannot be changed \
                     while the daemon is running",
                    pin.flag
                )));
            }
        }

        // Validate the full batch against the typed schema before mutating
        // anything (per-change deserialization attributes errors to the key).
        // `Null` removes the key from the candidate tree (rather than setting
        // an explicit null) so `#[serde(default)]` restores the schema default
        // for optional AND non-optional keys alike.
        let mut json = file_json(&inner.file)?;
        let mut candidate = inner.file.clone();
        for (path, value) in changes {
            if value.is_null() {
                json_remove(&mut json, path);
            } else {
                json_set(&mut json, path, value.clone());
            }
            candidate = typed_from_json(json.clone(), path)?;
        }

        for (path, value) in changes {
            doc_set(&mut inner.doc, path, value)?;
        }
        inner.file = candidate;

        let text = inner.doc.to_string();
        atomic_write(&self.path, &text)?;
        inner.record_write(&text);

        self.publish(&inner)
    }

    /// Re-parse externally edited file `text` (strict schema) and adopt it as
    /// the new file layer. Pins keep overriding, so external edits to pinned
    /// keys never change effective values. On parse/validation errors the
    /// registry state is untouched — the caller (live-reload watcher) keeps
    /// last-good. Returns the changed-key notice (also broadcast to
    /// subscribers when non-empty).
    pub fn reload(&self, text: &str) -> Result<SettingsChanged> {
        let file = SettingsFile::parse_str(text)?;
        let doc: DocumentMut = text
            .parse()
            .map_err(|e| Error::InvalidInput(format!("invalid config.toml: {e}")))?;
        let mut inner = self.inner.lock().expect("settings registry lock poisoned");
        inner.file = file;
        inner.doc = doc;
        // The accepted external edit supersedes every earlier self-write:
        // clear the history so a later external edit that happens to match
        // earlier self-written bytes (e.g. a manual revert) is not
        // misclassified as a self-write and skipped by the watcher.
        inner.recent_writes.clear();
        self.publish(&inner)
    }

    /// Rebuild the snapshot from `inner`, diff effective values against the
    /// previous snapshot, swap, and broadcast when anything changed.
    fn publish(&self, inner: &Inner) -> Result<SettingsChanged> {
        let old = self.snapshot();
        let new = self.swap_snapshot(inner)?;
        let changed: BTreeSet<String> = KNOWN_PATHS
            .iter()
            .filter(|p| json_get(&old.effective_json, p) != json_get(&new.effective_json, p))
            .map(|p| p.to_string())
            .collect();
        let notice = SettingsChanged {
            generation: inner.generation,
            changed,
        };
        if !notice.changed.is_empty() {
            self.tx.send_replace(notice.clone());
        }
        Ok(notice)
    }

    /// Rebuild + install the read snapshot; returns the new snapshot.
    fn swap_snapshot(&self, inner: &Inner) -> Result<Arc<SettingsSnapshot>> {
        let snapshot = Arc::new(build_snapshot(inner)?);
        *self
            .snapshot
            .write()
            .expect("settings snapshot lock poisoned") = snapshot.clone();
        Ok(snapshot)
    }
}

/// Build the read snapshot: file JSON ⊕ pins, typed re-parse, and per-key
/// origins (`flag` if pinned, `file` if present in the document, else
/// `default`).
fn build_snapshot(inner: &Inner) -> Result<SettingsSnapshot> {
    let mut json = file_json(&inner.file)?;
    for (path, pin) in &inner.pins {
        json_set(&mut json, path, pin.value.clone());
    }
    let effective: SettingsFile = serde_json::from_value(json.clone())
        .map_err(|e| Error::Internal(format!("settings snapshot deserialize: {e}")))?;
    let mut origins = BTreeMap::new();
    for &path in KNOWN_PATHS {
        let origin = if inner.pins.contains_key(path) {
            SettingOrigin::Flag
        } else if doc_has_path(&inner.doc, path) {
            SettingOrigin::File
        } else {
            SettingOrigin::Default
        };
        origins.insert(path.to_string(), origin);
    }
    Ok(SettingsSnapshot {
        effective,
        effective_json: json,
        origins,
    })
}

/// Serialize the typed file to its nested camelCase JSON tree.
fn file_json(file: &SettingsFile) -> Result<Value> {
    serde_json::to_value(file).map_err(|e| Error::Internal(format!("settings serialize: {e}")))
}

/// Deserialize + range-validate a candidate JSON tree back into the typed
/// schema, attributing failures to the wire `path` being changed.
fn typed_from_json(json: Value, path: &str) -> Result<SettingsFile> {
    let typed: SettingsFile =
        serde_json::from_value(json).map_err(|e| Error::InvalidParams(format!("{path}: {e}")))?;
    typed.validate().map_err(|e| match e {
        Error::InvalidInput(msg) => Error::InvalidParams(msg),
        other => other,
    })?;
    Ok(typed)
}

/// Navigate a nested JSON tree by dotted path.
fn json_get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Set `value` at a dotted path in a nested JSON tree, creating intermediate
/// objects as needed.
fn json_set(root: &mut Value, path: &str, value: Value) {
    let segs: Vec<&str> = path.split('.').collect();
    let (last, parents) = segs.split_last().expect("dotted path is never empty");
    let mut cur = root;
    for seg in parents {
        if !cur.is_object() {
            *cur = Value::Object(serde_json::Map::new());
        }
        cur = cur
            .as_object_mut()
            .expect("just ensured object")
            .entry(seg.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if !cur.is_object() {
        *cur = Value::Object(serde_json::Map::new());
    }
    cur.as_object_mut()
        .expect("just ensured object")
        .insert(last.to_string(), value);
}

/// Remove a dotted path from a nested JSON tree (no-op when absent). The
/// typed re-parse then restores the schema default via `#[serde(default)]`.
fn json_remove(root: &mut Value, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    let (last, parents) = segs.split_last().expect("dotted path is never empty");
    let mut cur = root;
    for seg in parents {
        match cur.as_object_mut().and_then(|o| o.get_mut(*seg)) {
            Some(next) => cur = next,
            None => return,
        }
    }
    if let Some(obj) = cur.as_object_mut() {
        obj.remove(*last);
    }
}

/// Whether the raw TOML document explicitly contains a dotted path.
fn doc_has_path(doc: &DocumentMut, path: &str) -> bool {
    let mut item = doc.as_item();
    for seg in path.split('.') {
        match item.as_table_like().and_then(|t| t.get(seg)) {
            Some(next) => item = next,
            None => return false,
        }
    }
    true
}

/// Set a dotted path in the raw document to a JSON value, preserving all
/// other content (comments, layout, key order). `Null` removes the key.
/// Existing values are replaced in place so comments above the key survive;
/// missing intermediate tables are created implicitly (rendered as `[a.b]`
/// headers only when they hold values).
fn doc_set(doc: &mut DocumentMut, path: &str, value: &Value) -> Result<()> {
    if value.is_null() {
        doc_remove(doc, path);
        return Ok(());
    }
    let segs: Vec<&str> = path.split('.').collect();
    let (last, parents) = segs.split_last().expect("dotted path is never empty");
    let mut item = doc.as_item_mut();
    for seg in parents {
        let parent_is_inline = item.is_inline_table();
        let table = item.as_table_like_mut().ok_or_else(|| {
            Error::Internal(format!("config.toml: parent of `{path}` is not a table"))
        })?;
        if table.get(seg).is_none() {
            let child = if parent_is_inline {
                toml_edit::Item::Value(toml_edit::InlineTable::new().into())
            } else {
                let mut t = toml_edit::Table::new();
                t.set_implicit(true);
                toml_edit::Item::Table(t)
            };
            table.insert(seg, child);
        }
        item = table.get_mut(seg).expect("present or just inserted");
    }
    let table = item.as_table_like_mut().ok_or_else(|| {
        Error::Internal(format!("config.toml: parent of `{path}` is not a table"))
    })?;
    let new_item = toml_edit::Item::Value(json_to_toml_value(value)?);
    match table.get_mut(last) {
        // Replace the value in place: the existing key (and any comment
        // attached above it) is untouched.
        Some(existing) => *existing = new_item,
        None => {
            table.insert(last, new_item);
        }
    }
    Ok(())
}

/// Remove a dotted path from the raw document (no-op when absent).
fn doc_remove(doc: &mut DocumentMut, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    let (last, parents) = segs.split_last().expect("dotted path is never empty");
    let mut item = doc.as_item_mut();
    for seg in parents {
        match item.as_table_like_mut().and_then(|t| t.get_mut(seg)) {
            Some(next) => item = next,
            None => return,
        }
    }
    if let Some(table) = item.as_table_like_mut() {
        table.remove(last);
    }
}

/// Convert a (non-null) JSON value to a TOML value for the document.
fn json_to_toml_value(value: &Value) -> Result<toml_edit::Value> {
    Ok(match value {
        Value::Null => {
            return Err(Error::Internal(
                "null cannot be encoded in TOML".to_string(),
            ))
        }
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into()
            } else if let Some(f) = n.as_f64() {
                f.into()
            } else {
                return Err(Error::InvalidParams(format!(
                    "number out of TOML range: {n}"
                )));
            }
        }
        Value::String(s) => s.as_str().into(),
        Value::Array(items) => {
            let mut arr = toml_edit::Array::new();
            for item in items {
                arr.push(json_to_toml_value(item)?);
            }
            arr.into()
        }
        Value::Object(map) => {
            let mut table = toml_edit::InlineTable::new();
            for (key, val) in map {
                table.insert(key.as_str(), json_to_toml_value(val)?);
            }
            table.into()
        }
    })
}

/// Crash-safe file replacement: write to a unique temp file in the same
/// directory, fsync, then rename over the target. Readers only ever observe
/// the old or the new complete content.
fn atomic_write(path: &Path, text: &str) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| Error::Internal(format!("config path has no parent: {}", path.display())))?;
    std::fs::create_dir_all(dir).map_err(|e| {
        Error::Internal(format!(
            "could not create config directory {}: {e}",
            dir.display()
        ))
    })?;
    let tmp = dir.join(format!(".config.toml.tmp.{}", uuid::Uuid::new_v4()));
    let write = || -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        // Best-effort directory fsync so the rename itself is durable across
        // a crash/power loss (without it some filesystems may surface the old
        // file). Failures are ignored: not all platforms support fsync on a
        // directory handle, and the data file itself is already synced.
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    };
    write().map_err(|e| {
        std::fs::remove_file(&tmp).ok();
        Error::Internal(format!("could not write config {}: {e}", path.display()))
    })
}

/// Content hash used by the self-write guard (identity within one process).
fn content_hash(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::DEFAULT_CONFIG_TEMPLATE;
    use serde_json::json;

    fn temp_config(contents: Option<&str>) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        if let Some(text) = contents {
            std::fs::write(&path, text).expect("seed config");
        }
        (dir, path)
    }

    fn set(path: &str, value: Value) -> Vec<(String, Value)> {
        vec![(path.to_string(), value)]
    }

    #[test]
    fn missing_file_inits_template_with_default_effective_values() {
        let (_dir, path) = temp_config(None);
        let reg = SettingsRegistry::load(&path).expect("load inits");
        assert_eq!(
            std::fs::read_to_string(&path).expect("file written"),
            DEFAULT_CONFIG_TEMPLATE
        );
        assert_eq!(reg.get("git.autoCommit"), Some(json!(true)));
        assert_eq!(reg.get("server.wsApi.port"), Some(json!(5181)));
        assert!(!reg.snapshot().effective.server.ws_api.enabled);
    }

    #[test]
    fn every_known_path_resolves_in_the_effective_tree() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        for p in SettingsRegistry::known_paths() {
            assert!(reg.get(p).is_some(), "known path `{p}` must resolve");
            assert!(reg.origin(p).is_some(), "known path `{p}` must have origin");
        }
    }

    #[test]
    fn excluded_keys_are_unknown_to_the_registry() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        for p in [
            "linear.token",
            "server.auth.token",
            "mcp.servers",
            "workspace.changeHistory",
            "workspaceInitializer.state",
            "hardwareConsole.state",
            "repos.known",
            "endUserRules",
            "permissions.rules",
            "userRules",
            "workspaceRules",
            "model.workspaceOverrides",
        ] {
            assert_eq!(reg.get(p), None, "`{p}` must be unknown");
            assert_eq!(reg.origin(p), None, "`{p}` must have no origin");
            let err = reg.apply(&set(p, json!(true))).unwrap_err();
            assert!(matches!(err, Error::InvalidParams(_)), "{p}: {err}");
        }
    }

    #[test]
    fn precedence_is_default_then_file_then_flag() {
        let (_dir, path) = temp_config(Some("[server.wsApi]\nenabled = true\nport = 6000\n"));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.pin("server.wsApi.port", json!(7000), "INTENTD_TCP_PORT")
            .expect("pin");

        // default layer: key absent from file and unpinned
        assert_eq!(reg.get("notifications.volume"), Some(json!(0.5)));
        assert_eq!(
            reg.origin("notifications.volume"),
            Some(SettingOrigin::Default)
        );
        // file layer
        assert_eq!(reg.get("server.wsApi.enabled"), Some(json!(true)));
        assert_eq!(
            reg.origin("server.wsApi.enabled"),
            Some(SettingOrigin::File)
        );
        // flag layer beats the file value 6000
        assert_eq!(reg.get("server.wsApi.port"), Some(json!(7000)));
        assert_eq!(reg.origin("server.wsApi.port"), Some(SettingOrigin::Flag));

        // typed accessors see the same layering
        let snap = reg.snapshot();
        assert!(snap.effective.server.ws_api.enabled);
        assert_eq!(snap.effective.server.ws_api.port, 7000);
        assert_eq!(snap.effective.notifications.volume, 0.5);
    }

    #[test]
    fn pinned_key_rejects_apply_with_flag_message() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.pin("server.wsApi.enabled", json!(true), "--insecure")
            .expect("pin");
        let err = reg
            .apply(&set("server.wsApi.enabled", json!(false)))
            .unwrap_err();
        match err {
            Error::InvalidParams(msg) => {
                assert!(msg.contains("overridden by startup flag"), "{msg}");
                assert!(msg.contains("--insecure"), "{msg}");
            }
            other => panic!("expected InvalidParams, got {other}"),
        }
        // nothing changed, and the pin still wins
        assert_eq!(reg.get("server.wsApi.enabled"), Some(json!(true)));
    }

    #[test]
    fn pin_validates_against_the_schema() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        assert!(reg.pin("server.wsApi.port", json!(80), "--port").is_err());
        assert!(reg.pin("bogus.key", json!(1), "--bogus").is_err());
        assert_eq!(reg.get("server.wsApi.port"), Some(json!(5181)));
    }

    #[test]
    fn apply_validates_types_ranges_and_unknown_keys() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        for (p, v) in [
            ("git.autoCommit", json!("yes")),
            ("server.wsApi.port", json!(80)),
            ("notifications.volume", json!(2.0)),
            ("logging.level", json!("verbose")),
            ("agents.maxConcurrent", json!(500)),
        ] {
            let err = reg.apply(&set(p, v)).unwrap_err();
            assert!(matches!(err, Error::InvalidParams(_)), "{p}: {err}");
        }
        let err = reg.apply(&set("no.such.key", json!(1))).unwrap_err();
        assert!(err.to_string().contains("unknown setting"), "{err}");
        // failed applies never touched the file
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "");
    }

    #[test]
    fn apply_round_trips_through_a_fresh_registry() {
        let (_dir, path) = temp_config(None);
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&[
            ("git.autoCommit".to_string(), json!(false)),
            ("providers.active".to_string(), json!("claude-code")),
            (
                "model.providerDefaults".to_string(),
                json!({"claude-code": "claude-sonnet-4-5"}),
            ),
            ("mcp.disabledServers".to_string(), json!(["linear"])),
            ("notifications.volume".to_string(), json!(0.25)),
        ])
        .expect("apply");

        let reloaded = SettingsRegistry::load(&path).expect("reload from disk");
        assert_eq!(reloaded.get("git.autoCommit"), Some(json!(false)));
        assert_eq!(reloaded.get("providers.active"), Some(json!("claude-code")));
        assert_eq!(
            reloaded.get("model.providerDefaults"),
            Some(json!({"claude-code": "claude-sonnet-4-5"}))
        );
        assert_eq!(reloaded.get("mcp.disabledServers"), Some(json!(["linear"])));
        assert_eq!(reloaded.get("notifications.volume"), Some(json!(0.25)));
        assert_eq!(reloaded.origin("git.autoCommit"), Some(SettingOrigin::File));
    }

    #[test]
    fn apply_preserves_comments_and_untouched_keys() {
        let seed = "# top comment\n\n[git]\n# keep me\nautoCommit = true\n\n[rtk]\n# rtk stays off\nenabled = false\n";
        let (_dir, path) = temp_config(Some(seed));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("git.autoCommit", json!(false)))
            .expect("apply");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# top comment"), "{text}");
        assert!(text.contains("# keep me"), "{text}");
        assert!(text.contains("autoCommit = false"), "{text}");
        // untouched section is byte-identical
        assert!(
            text.contains("[rtk]\n# rtk stays off\nenabled = false"),
            "{text}"
        );
    }

    #[test]
    fn apply_creates_missing_tables_without_disturbing_the_rest() {
        let seed = "# header\n[git]\nautoCommit = true\n";
        let (_dir, path) = temp_config(Some(seed));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("server.wsApi.enabled", json!(true)))
            .expect("apply");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# header"), "{text}");
        assert!(text.contains("autoCommit = true"), "{text}");
        assert!(text.contains("enabled = true"), "{text}");
        // the new value re-parses strictly and is effective
        let reloaded = SettingsRegistry::load(&path).expect("reload");
        assert_eq!(reloaded.get("server.wsApi.enabled"), Some(json!(true)));
    }

    #[test]
    fn apply_null_clears_an_optional_key_back_to_default() {
        let (_dir, path) = temp_config(Some("[providers]\nactive = \"codex\"\n"));
        let reg = SettingsRegistry::load(&path).expect("load");
        assert_eq!(reg.origin("providers.active"), Some(SettingOrigin::File));
        reg.apply(&set("providers.active", Value::Null))
            .expect("apply null");
        assert_eq!(reg.get("providers.active"), Some(Value::Null));
        assert_eq!(reg.origin("providers.active"), Some(SettingOrigin::Default));
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(!text.contains("active"), "{text}");
    }

    #[test]
    fn apply_notifies_subscribers_with_the_changed_key_set() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        let mut rx = reg.subscribe();
        let notice = reg
            .apply(&[
                ("git.autoCommit".to_string(), json!(false)),
                ("rtk.enabled".to_string(), json!(true)),
            ])
            .expect("apply");
        assert_eq!(
            notice.changed,
            BTreeSet::from(["git.autoCommit".to_string(), "rtk.enabled".to_string()])
        );
        assert!(rx.has_changed().expect("sender alive"));
        assert_eq!(*rx.borrow_and_update(), notice);
    }

    #[test]
    fn reload_applies_external_changes_and_ignores_pinned_keys() {
        let (_dir, path) = temp_config(Some("[server.wsApi]\nport = 6000\n"));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.pin("server.wsApi.port", json!(7000), "INTENTD_TCP_PORT")
            .expect("pin");
        let rx = reg.subscribe();

        let notice = reg
            .reload("[server.wsApi]\nport = 9000\nenabled = true\n")
            .expect("reload");
        // the pinned port did not change effectively; enabled did
        assert_eq!(
            notice.changed,
            BTreeSet::from(["server.wsApi.enabled".to_string()])
        );
        assert_eq!(reg.get("server.wsApi.port"), Some(json!(7000)));
        assert_eq!(reg.get("server.wsApi.enabled"), Some(json!(true)));
        assert!(rx.has_changed().expect("sender alive"));
    }

    #[test]
    fn flush_queued_messages_defaults_on_overrides_and_reloads() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        // Schema default: "all".
        assert_eq!(reg.get("agents.flushQueuedMessages"), Some(json!("all")));
        assert_eq!(
            reg.origin("agents.flushQueuedMessages"),
            Some(SettingOrigin::Default)
        );

        // File override via apply, surviving a fresh load from disk.
        reg.apply(&set("agents.flushQueuedMessages", json!("systemOnly")))
            .expect("apply");
        assert_eq!(
            reg.get("agents.flushQueuedMessages"),
            Some(json!("systemOnly"))
        );
        assert_eq!(
            reg.origin("agents.flushQueuedMessages"),
            Some(SettingOrigin::File)
        );
        let reloaded = SettingsRegistry::load(&path).expect("reload from disk");
        assert_eq!(
            reloaded.get("agents.flushQueuedMessages"),
            Some(json!("systemOnly"))
        );

        // External reload without the key restores the schema default.
        let notice = reg.reload("").expect("reload");
        assert!(notice.changed.contains("agents.flushQueuedMessages"));
        assert_eq!(reg.get("agents.flushQueuedMessages"), Some(json!("all")));
    }

    #[test]
    fn flush_queued_messages_legacy_boolean_file_loads_and_reapplies() {
        // A `config.toml` written by an older daemon still loads, reporting
        // the equivalent string value with `File` origin.
        let (_dir, path) = temp_config(Some("[agents]\nflushQueuedMessages = true\n"));
        let reg = SettingsRegistry::load(&path).expect("load legacy true");
        assert_eq!(reg.get("agents.flushQueuedMessages"), Some(json!("all")));
        assert_eq!(
            reg.origin("agents.flushQueuedMessages"),
            Some(SettingOrigin::File)
        );

        let (_dir2, path2) = temp_config(Some("[agents]\nflushQueuedMessages = false\n"));
        let reg2 = SettingsRegistry::load(&path2).expect("load legacy false");
        assert_eq!(reg2.get("agents.flushQueuedMessages"), Some(json!("off")));
        assert_eq!(
            reg2.origin("agents.flushQueuedMessages"),
            Some(SettingOrigin::File)
        );
    }

    #[test]
    fn reload_rejects_invalid_text_and_keeps_last_good_state() {
        let (_dir, path) = temp_config(Some("[git]\nautoCommit = false\n"));
        let reg = SettingsRegistry::load(&path).expect("load");
        let err = reg.reload("[git]\nautoCommit = \"nope\"\n").unwrap_err();
        assert!(err.to_string().contains("git.autoCommit"), "{err}");
        let err = reg.reload("[bogus]\nkey = 1\n").unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
    }

    #[test]
    fn self_write_guard_tracks_generation_and_content_hash() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        assert_eq!(reg.generation(), 0);
        assert_eq!(reg.write_stamp(), None);
        assert!(!reg.is_self_write(""));

        reg.apply(&set("rtk.enabled", json!(true))).expect("apply");
        assert_eq!(reg.generation(), 1);
        let stamp = reg.write_stamp().expect("stamp after write");
        assert_eq!(stamp.generation, 1);
        let on_disk = std::fs::read_to_string(&path).expect("read");
        assert!(reg.is_self_write(&on_disk));
        assert!(!reg.is_self_write(&format!("{on_disk}\n# external edit\n")));

        reg.apply(&set("rtk.enabled", json!(false)))
            .expect("apply again");
        assert_eq!(reg.generation(), 2);
        assert_ne!(reg.write_stamp(), Some(stamp));
    }

    #[test]
    fn recent_self_writes_all_match_the_guard() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("rtk.enabled", json!(true)))
            .expect("apply A");
        let write_a = std::fs::read_to_string(&path).expect("read A");
        reg.apply(&set("rtk.enabled", json!(false)))
            .expect("apply B");
        let write_b = std::fs::read_to_string(&path).expect("read B");
        // A stale/coalesced watcher read can observe the file at any *recent*
        // self-write, not just the last one — both must be classified as
        // self-writes so the watcher never adopts them as external edits.
        assert!(reg.is_self_write(&write_b), "latest write must match");
        assert!(
            reg.is_self_write(&write_a),
            "earlier recent write must match"
        );
    }

    #[test]
    fn self_write_history_is_bounded() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("agents.maxConcurrent", json!(1)))
            .expect("apply first");
        let first = std::fs::read_to_string(&path).expect("read first");
        for i in 2..=(SELF_WRITE_HISTORY as i64 + 1) {
            reg.apply(&set("agents.maxConcurrent", json!(i)))
                .expect("apply");
        }
        // The first write has been evicted from the bounded history; the
        // most recent writes still match.
        assert!(!reg.is_self_write(&first));
        let latest = std::fs::read_to_string(&path).expect("read latest");
        assert!(reg.is_self_write(&latest));
    }

    #[test]
    fn reload_clears_the_self_write_history() {
        let (_dir, path) = temp_config(Some(""));
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("rtk.enabled", json!(true))).expect("apply");
        let self_written = std::fs::read_to_string(&path).expect("read");
        assert!(reg.is_self_write(&self_written));
        // Adopting an external edit supersedes every earlier self-write: a
        // later manual revert to those bytes must be treated as external.
        reg.reload("[git]\nautoCommit = false\n").expect("reload");
        assert!(!reg.is_self_write(&self_written));
        assert_eq!(reg.write_stamp(), None);
    }

    #[test]
    fn atomic_write_leaves_no_temp_files_and_valid_content() {
        let (dir, path) = temp_config(None);
        let reg = SettingsRegistry::load(&path).expect("load");
        reg.apply(&set("git.autoCommit", json!(false)))
            .expect("apply");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name != "config.toml")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        // the on-disk file is complete and strictly parseable
        let text = std::fs::read_to_string(&path).expect("read");
        SettingsFile::parse_str(&text).expect("valid config on disk");
    }

    #[test]
    fn concurrent_reads_see_consistent_snapshots() {
        let (_dir, path) = temp_config(Some(""));
        let reg = Arc::new(SettingsRegistry::load(&path).expect("load"));
        let snap_before = reg.snapshot();
        reg.apply(&set("rtk.enabled", json!(true))).expect("apply");
        // the old snapshot is immutable; new reads see the new value
        assert!(!snap_before.effective.rtk.enabled);
        assert!(reg.snapshot().effective.rtk.enabled);
    }

    #[test]
    fn load_tolerates_legacy_workspace_overrides_and_captures_them() {
        let seed = "[model]\ndefault = \"m0\"\nworkspaceOverrides = { ws1 = \"m1\" }\n";
        let (_dir, path) = temp_config(Some(seed));
        let reg = SettingsRegistry::load(&path).expect("legacy key must not refuse load");
        // The legacy key is not a registry path: unknown on the wire surface.
        assert_eq!(reg.get("model.workspaceOverrides"), None);
        assert_eq!(reg.origin("model.workspaceOverrides"), None);
        // The rest of the file is effective as usual.
        assert_eq!(reg.get("model.default"), Some(json!("m0")));
        // The captured value is available for the boot import.
        assert_eq!(
            reg.legacy_values().get("model.workspaceOverrides"),
            Some(&json!({ "ws1": "m1" }))
        );
    }

    #[test]
    fn strip_legacy_rewrites_file_preserving_comments() {
        let seed = "# top comment\n\n[model]\n# my default\ndefault = \"m0\"\nworkspaceOverrides = { ws1 = \"m1\" }\n\n[git]\n# keep me\nautoCommit = false\n";
        let (_dir, path) = temp_config(Some(seed));
        let reg = SettingsRegistry::load(&path).expect("load");
        let stripped = reg.strip_legacy().expect("strip");
        assert_eq!(stripped, vec!["model.workspaceOverrides".to_string()]);

        let text = std::fs::read_to_string(&path).expect("read");
        assert!(!text.contains("workspaceOverrides"), "{text}");
        assert!(text.contains("# top comment"), "{text}");
        assert!(text.contains("# my default"), "{text}");
        assert!(text.contains("default = \"m0\""), "{text}");
        assert!(text.contains("# keep me"), "{text}");
        assert!(text.contains("autoCommit = false"), "{text}");

        // The strip counts as a self-write for the live-reload watcher, and
        // the captured map is cleared (second strip is a no-op).
        assert!(reg.is_self_write(&text));
        assert!(reg.legacy_values().is_empty());
        assert_eq!(
            reg.strip_legacy().expect("no-op strip"),
            Vec::<String>::new()
        );

        // A fresh registry loads the stripped file cleanly with no legacy.
        let reloaded = SettingsRegistry::load(&path).expect("clean reload");
        assert!(reloaded.legacy_values().is_empty());
        assert_eq!(reloaded.get("model.default"), Some(json!("m0")));
    }

    #[test]
    fn strip_legacy_is_a_no_op_without_legacy_keys() {
        let seed = "[git]\nautoCommit = false\n";
        let (_dir, path) = temp_config(Some(seed));
        let reg = SettingsRegistry::load(&path).expect("load");
        assert_eq!(reg.strip_legacy().expect("no-op"), Vec::<String>::new());
        // The file was not rewritten at all.
        assert_eq!(std::fs::read_to_string(&path).expect("read"), seed);
        assert_eq!(reg.generation(), 0);
    }

    #[test]
    fn load_still_rejects_other_unknown_keys() {
        let seed = "[model]\nworkspaceOverrides = {}\n\n[agents]\nbogusKey = 1\n";
        let (_dir, path) = temp_config(Some(seed));
        let err = match SettingsRegistry::load(&path) {
            Ok(_) => panic!("unknown key must refuse load"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("bogusKey"), "{err}");
    }
}
