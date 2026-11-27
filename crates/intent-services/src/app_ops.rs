//! Intent apps (PROTOCOL §5.60, Labs): the app library, approval per bundle
//! hash, app sessions and the app-principal methods.
//!
//! Inert unless used: nothing here runs until a client calls an `app.*`
//! method. The transport is the first line (the level allowlists at the
//! inbound chokepoint); every method here checks its caller again:
//!
//! - Library methods need the daemon administrator (owner wire caller,
//!   agent or daemon). `app.approve` and `app.session.open` are user-only:
//!   an agent gets `forbidden` / `user-only`.
//! - App-principal methods need [`Caller::App`] and a live session; every
//!   other caller gets `forbidden` / `app principal required`.
//!
//! Approval is bound to the bundle hash. A replace install with a new hash
//! resets approval to `pending` and ends the app's session; a session opens
//! only for an app approved for its installed hash.

pub(crate) mod bundle;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use intent_core::apps::{
    app_invalid_params, app_refusal, forbidden, AppAttachment, AppError, AppPrincipal,
    AppSessionEnd, APP_FEATURE_MODELS, APP_FEATURE_NOTIFICATIONS,
};
use intent_core::{current_caller, now_iso, Caller, Error, Result, WorkspaceId};
use intent_store::{AppStorageWrite, NewEvent};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use self::bundle::{Manifest, Publisher, Signature, WindowSpec};
use crate::Services;

/// Per-app storage quota: key bytes plus serialized value bytes.
pub(crate) const STORAGE_QUOTA_BYTES: u64 = 10 * 1024 * 1024;
const STORAGE_VALUE_MAX_BYTES: usize = 256 * 1024;
const STORAGE_KEY_MAX_BYTES: usize = 256;
const NOTIFY_LIMIT: usize = 10;
const NOTIFY_WINDOW: Duration = Duration::from_secs(60);
const COMPLETE_MAX_IN_FLIGHT: usize = 2;
const COMPLETE_DEFAULT_TIMEOUT_MS: u64 = 120_000;
/// Providers `app.complete` can use: the one-shot-capable providers of
/// `agent.completeOnce` (§5.32).
const ONE_SHOT_PROVIDERS: &[&str] = &["auggie", "claude-code", "codex", "pi"];

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Permissions {
    pub(crate) level: u8,
    pub(crate) features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Approval {
    pub(crate) status: String,
    pub(crate) level: Option<u8>,
    pub(crate) features: Vec<String>,
    pub(crate) bundle_hash: Option<String>,
    pub(crate) approved_at: Option<String>,
}

impl Approval {
    fn pending() -> Self {
        Self {
            status: "pending".into(),
            level: None,
            features: Vec::new(),
            bundle_hash: None,
            approved_at: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Source {
    pub(crate) workspace_id: String,
    pub(crate) path: String,
}

/// The stored record; [`AppRecord::to_wire`] adds `open` and resolves a
/// vanished source to `null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AppRecord {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: Option<String>,
    pub(crate) icon: Option<String>,
    pub(crate) entry: String,
    pub(crate) bundle_hash: String,
    pub(crate) file_count: u64,
    pub(crate) size_bytes: u64,
    pub(crate) permissions: Permissions,
    pub(crate) window: WindowSpec,
    pub(crate) approval: Approval,
    pub(crate) source: Option<Source>,
    pub(crate) installed_at: String,
    pub(crate) updated_at: String,
    /// Reserved (§5.60 "Publisher and signature"): stored, never verified.
    #[serde(default)]
    pub(crate) publisher: Option<Publisher>,
    #[serde(default)]
    pub(crate) update_url: Option<String>,
    #[serde(default)]
    pub(crate) signature: Option<Signature>,
}

impl AppRecord {
    fn to_wire(&self, open: bool, source_present: bool) -> Value {
        let mut v = serde_json::to_value(self).unwrap_or(Value::Null);
        if let Some(obj) = v.as_object_mut() {
            if !source_present {
                obj.insert("source".into(), Value::Null);
            }
            obj.insert("open".into(), json!(open));
            // M1 never verifies: no client may show either as trust.
            if let Some(Value::Object(p)) = obj.get_mut("publisher") {
                p.insert("verified".into(), json!(false));
            }
            if let Some(Value::Object(sig)) = obj.get_mut("signature") {
                sig.insert("status".into(), json!("unverified"));
                sig.insert("verified".into(), json!(false));
            }
        }
        v
    }

    fn approved_for_installed_hash(&self) -> bool {
        self.approval.status == "approved"
            && self.approval.bundle_hash.as_deref() == Some(self.bundle_hash.as_str())
    }
}

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

struct SessionEntry {
    principal: Arc<AppPrincipal>,
    token_hash: [u8; 32],
    /// The `AppSession` result without `token`.
    result: Value,
    end: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct Sessions {
    ids: HashMap<String, SessionEntry>,
    tokens: HashMap<[u8; 32], String>,
    apps_open: HashMap<String, String>,
}

impl Sessions {
    fn end(&mut self, session_id: &str, reason: AppSessionEnd) -> bool {
        let Some(entry) = self.ids.remove(session_id) else {
            return false;
        };
        self.tokens.remove(&entry.token_hash);
        if self
            .apps_open
            .get(&entry.principal.app_id)
            .map(String::as_str)
            == Some(session_id)
        {
            self.apps_open.remove(&entry.principal.app_id);
        }
        tracing::info!(
            app_id = %entry.principal.app_id,
            session_id,
            reason = ?reason,
            "app session ended"
        );
        let _ = entry.end.send(true);
        true
    }

    fn end_app(&mut self, app_id: &str, reason: AppSessionEnd) {
        if let Some(id) = self.apps_open.get(app_id).cloned() {
            self.end(&id, reason);
        }
    }
}

/// The daemon's Intent apps state. One per daemon; cloned `Services`
/// handles share it.
pub(crate) struct AppsState {
    /// `<data dir>/apps`.
    root: PathBuf,
    /// Serializes every library mutation (install, uninstall, approve,
    /// revoke) so a check and its write never interleave with another.
    library: tokio::sync::Mutex<()>,
    /// Storage writes hold it shared and recheck their session under it;
    /// uninstall holds it exclusively while it ends the session and deletes
    /// the storage, so a write admitted before an uninstall can never land
    /// after it.
    storage_gate: tokio::sync::RwLock<()>,
    /// Test seam: parks a storage write after admission, before the gate.
    #[cfg(test)]
    storage_write_pause: Mutex<Option<StorageWritePause>>,
    sessions: Mutex<Sessions>,
    notify: Mutex<HashMap<String, VecDeque<Instant>>>,
    in_flight: Mutex<HashMap<String, usize>>,
}

impl AppsState {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            library: tokio::sync::Mutex::new(()),
            storage_gate: tokio::sync::RwLock::new(()),
            #[cfg(test)]
            storage_write_pause: Mutex::new(None),
            sessions: Mutex::new(Sessions::default()),
            notify: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, Sessions> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn app_dir(&self, app_id: &str) -> PathBuf {
        self.root.join(app_id)
    }

    fn bundle_dir(&self, app_id: &str, hash: &str) -> PathBuf {
        self.app_dir(app_id).join(bundle::hash_hex(hash))
    }

    fn is_open(&self, app_id: &str) -> bool {
        self.sessions().apps_open.contains_key(app_id)
    }
}

/// Test seam for [`AppsState::storage_write_pause`]: the write signals
/// `entered`, then waits for `release`.
#[cfg(test)]
pub(crate) struct StorageWritePause {
    pub(crate) entered: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
}

/// Releases one `app.complete` in-flight slot on drop.
struct InFlight<'a> {
    state: &'a AppsState,
    app_id: String,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut map = self
            .state
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = map.get_mut(&self.app_id) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                map.remove(&self.app_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Caller checks and params
// ---------------------------------------------------------------------------

fn require_library_caller(method: &str) -> Result<()> {
    Services::require_administrator(method)
}

/// User-only (§5.60 "Roles"): the administrator over the wire, never an
/// agent (MCP or `ws.rpc`). Checked before the administrator gate so an
/// agent always learns the documented reason.
fn require_user(method: &str) -> Result<()> {
    if matches!(current_caller(), Some(Caller::Agent { .. })) {
        return Err(Error::Forbidden(forbidden::USER_ONLY.into()));
    }
    require_library_caller(method)
}

fn require_app_caller() -> Result<Arc<AppPrincipal>> {
    match current_caller() {
        Some(Caller::App(app)) => Ok(app),
        _ => Err(Error::Forbidden(forbidden::APP_PRINCIPAL_REQUIRED.into())),
    }
}

fn require_feature(app: &AppPrincipal, feature: &str) -> Result<()> {
    if app.has_feature(feature) {
        Ok(())
    } else {
        Err(Error::Forbidden(forbidden::feature_not_approved(feature)))
    }
}

fn req_str<'a>(params: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    match params.get(key) {
        Some(Value::String(s)) => Ok(s),
        None | Some(Value::Null) => Err(app_invalid_params(format!("{key} is required"))),
        Some(_) => Err(app_invalid_params(format!("{key} must be a string"))),
    }
}

fn opt_str<'a>(params: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(app_invalid_params(format!("{key} must be a string"))),
    }
}

fn opt_bool(params: &Map<String, Value>, key: &str) -> Result<bool> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(app_invalid_params(format!("{key} must be a boolean"))),
    }
}

fn opt_u64(params: &Map<String, Value>, key: &str) -> Result<Option<u64>> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .filter(|_| v.is_u64())
            .map(Some)
            .ok_or_else(|| app_invalid_params(format!("{key} must be a non-negative integer"))),
    }
}

fn app_not_found() -> Error {
    app_refusal("app-not-found", "app not found")
}

fn hash_mismatch(installed: &str) -> Error {
    AppError::params(
        "hash-mismatch",
        "the bundle hash does not match the installed bundle",
        json!({ "bundleHash": installed }),
    )
    .into_error()
}

fn level_unavailable() -> Error {
    app_refusal(
        "level-unavailable",
        "full access (level 3) is not available yet",
    )
}

fn path_not_found(detail: impl Into<String>) -> Error {
    let detail = detail.into();
    AppError::params(
        "path-not-found",
        format!("path not found: {detail}"),
        json!({ "detail": detail }),
    )
    .into_error()
}

fn rate_limited(retry_after_ms: u64) -> Error {
    AppError::service(
        "rate-limited",
        "rate limited",
        json!({ "retryAfterMs": retry_after_ms }),
    )
    .into_error()
}

fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// `principal.me` on an app connection (§5.60 "The app principal").
pub(crate) fn app_principal_me(app: &AppPrincipal) -> Value {
    json!({
        "kind": "app",
        "id": format!("app:{}", app.app_id),
        "appId": app.app_id,
        "bundleHash": app.bundle_hash,
        "level": app.level,
        "features": app.features,
        "sessionId": app.session_id,
        "isAdministrator": false,
        "login": Value::Null,
        "displayName": app.name,
        "avatarUrl": Value::Null,
    })
}

fn storage_key(params: &Map<String, Value>) -> Result<String> {
    let Some(Value::String(key)) = params.get("key") else {
        return Err(app_refusal("invalid-key", "key must be a string"));
    };
    if key.is_empty()
        || key.len() > STORAGE_KEY_MAX_BYTES
        || key.chars().any(bundle::is_control_char)
    {
        return Err(app_refusal(
            "invalid-key",
            "key must be 1–256 UTF-8 bytes without control characters",
        ));
    }
    Ok(key.clone())
}

fn decode_cursor(cursor: &str) -> Result<String> {
    let bytes = (0..cursor.len())
        .step_by(2)
        .map(|i| {
            cursor
                .get(i..i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        })
        .collect::<Option<Vec<u8>>>()
        .filter(|b| !b.is_empty() && cursor.len().is_multiple_of(2))
        .ok_or_else(|| app_invalid_params("cursor is invalid"))?;
    String::from_utf8(bytes).map_err(|_| app_invalid_params("cursor is invalid"))
}

// ---------------------------------------------------------------------------
// Service methods
// ---------------------------------------------------------------------------

impl Services {
    /// `WorkspaceApi::app_call`: dispatch one router `app.*` method.
    pub(crate) async fn app_call_op(
        &self,
        method: &str,
        params: &Map<String, Value>,
    ) -> Result<Value> {
        match method {
            "app.install" => self.app_install(params).await,
            "app.uninstall" => self.app_uninstall(params).await,
            "app.list" => self.app_list().await,
            "app.get" => self.app_get(params).await,
            "app.approve" => self.app_approve(params).await,
            "app.revoke" => self.app_revoke(params).await,
            "app.readBundle" => self.app_read_bundle(params).await,
            "app.session.open" => self.app_session_open(params).await,
            "app.session.close" => self.app_session_close(params),
            "app.storage.get" => self.app_storage_get(params).await,
            "app.storage.set" => self.app_storage_set(params).await,
            "app.storage.delete" => self.app_storage_delete(params).await,
            "app.storage.list" => self.app_storage_list(params).await,
            "app.notify" => self.app_notify(params),
            "app.models.list" => self.app_models_list(),
            "app.complete" => self.app_complete(params).await,
            _ => Err(Error::NotFound(format!("method {method}"))),
        }
    }

    async fn load_app(&self, app_id: &str) -> Result<Option<AppRecord>> {
        match self.store.get_app_record(app_id).await? {
            None => Ok(None),
            Some(json) => serde_json::from_str(&json)
                .map(Some)
                .map_err(|e| Error::Internal(format!("app record {app_id} is unreadable: {e}"))),
        }
    }

    async fn save_app(&self, record: &AppRecord) -> Result<()> {
        let json = serde_json::to_string(record)
            .map_err(|e| Error::Internal(format!("app record encode failed: {e}")))?;
        self.store
            .put_app_record(&record.id, &json, &record.updated_at)
            .await
    }

    async fn source_present(&self, record: &AppRecord) -> bool {
        match &record.source {
            None => false,
            Some(s) => self
                .store
                .get_workspace(&WorkspaceId::from_string(s.workspace_id.clone()))
                .await
                .is_ok(),
        }
    }

    async fn app_wire(&self, record: &AppRecord) -> Value {
        let present = self.source_present(record).await;
        record.to_wire(self.apps.is_open(&record.id), present)
    }

    async fn publish_app_event(&self, event_type: &str, data: Value) {
        let event = NewEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            timestamp: now_iso(),
            event_type: event_type.to_owned(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    async fn publish_approval_changed(&self, record: &AppRecord) {
        self.publish_app_event(
            intent_core::events::APP_APPROVAL_CHANGED,
            json!({
                "appId": record.id,
                "bundleHash": record.bundle_hash,
                "status": record.approval.status,
                "level": record.approval.level,
                "features": record.approval.features,
            }),
        )
        .await;
    }

    /// Resolve the install source directory inside the workspace root.
    async fn install_source(&self, workspace_id: &str, path: &str) -> Result<PathBuf> {
        let ws = self
            .store
            .get_workspace(&WorkspaceId::from_string(workspace_id.to_owned()))
            .await?;
        let root = crate::file_ops::workspace_root(&ws);
        if root.is_empty() {
            return Err(path_not_found("the workspace has no folder"));
        }
        let rel = path.trim().to_owned();
        if rel.starts_with('/') || rel.contains('\\') || rel.contains('\0') {
            return Err(path_not_found(
                "path must be relative to the workspace root",
            ));
        }
        let root = PathBuf::from(root);
        tokio::task::spawn_blocking(move || -> Result<PathBuf> {
            let canon_root = std::fs::canonicalize(&root)
                .map_err(|_| path_not_found("the workspace folder is missing"))?;
            let joined = if rel.is_empty() || rel == "." {
                canon_root.clone()
            } else {
                canon_root.join(&rel)
            };
            let canon = std::fs::canonicalize(&joined)
                .map_err(|_| path_not_found("the bundle directory does not exist"))?;
            if !canon.starts_with(&canon_root) {
                return Err(path_not_found("the path is outside the workspace root"));
            }
            if !canon.is_dir() {
                return Err(path_not_found("the path is not a directory"));
            }
            let manifest = std::fs::symlink_metadata(canon.join(bundle::MANIFEST_FILE))
                .map_err(|_| path_not_found("intent-app.json is missing"))?;
            if manifest.file_type().is_symlink() {
                return Err(AppError::params(
                    "invalid-bundle",
                    "invalid bundle: symlinks are not allowed",
                    json!({ "detail": "symlinks are not allowed", "path": bundle::MANIFEST_FILE }),
                )
                .into_error());
            }
            if !manifest.is_file() {
                return Err(path_not_found("intent-app.json is missing"));
            }
            Ok(canon)
        })
        .await
        .map_err(|e| Error::Internal(format!("install source task failed: {e}")))?
    }

    async fn app_install(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.install")?;
        let workspace_id = req_str(params, "workspaceId")?.to_owned();
        let path = req_str(params, "path")?.to_owned();
        let replace = opt_bool(params, "replace")?;
        let dry_run = opt_bool(params, "dryRun")?;

        let dir = self.install_source(&workspace_id, &path).await?;
        let read = tokio::task::spawn_blocking(
            move || -> Result<(bundle::Bundle, Manifest, Option<Signature>)> {
                let bundle = bundle::read_bundle_dir(&dir)?;
                let manifest = bundle::parse_manifest(&bundle)?;
                let signature = bundle
                    .signature
                    .as_deref()
                    .map(bundle::parse_signature)
                    .transpose()?;
                Ok((bundle, manifest, signature))
            },
        )
        .await
        .map_err(|e| Error::Internal(format!("bundle read task failed: {e}")))??;
        let (bundle, manifest, signature) = read;
        if manifest.level == 3 {
            return Err(level_unavailable());
        }

        let _library = self.apps.library.lock().await;
        let existing = self.load_app(&manifest.id).await?;
        if existing.is_some() && !replace {
            return Err(app_refusal(
                "app-exists",
                "an app with this id is installed",
            ));
        }
        let now = now_iso();
        let approval_reset = existing
            .as_ref()
            .is_some_and(|old| old.bundle_hash != bundle.hash);
        let approval = match &existing {
            Some(old) if !approval_reset && !dry_run => old.approval.clone(),
            _ => Approval::pending(),
        };
        let record = AppRecord {
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            icon: manifest.icon.clone(),
            entry: manifest.entry.clone(),
            bundle_hash: bundle.hash.clone(),
            file_count: bundle.files.len() as u64,
            size_bytes: bundle.size_bytes,
            permissions: Permissions {
                level: manifest.level,
                features: manifest.features.clone(),
            },
            window: manifest.window.clone(),
            approval,
            source: Some(Source {
                workspace_id: workspace_id.clone(),
                path: path.clone(),
            }),
            installed_at: existing
                .as_ref()
                .map_or_else(|| now.clone(), |old| old.installed_at.clone()),
            updated_at: now,
            publisher: manifest.publisher.clone(),
            update_url: manifest.update_url.clone(),
            signature,
        };
        let replaced = existing.is_some();
        if dry_run {
            // Validates and answers; stores, ends and publishes nothing.
            return Ok(json!({
                "app": record.to_wire(false, true),
                "replaced": replaced,
                "approvalReset": approval_reset,
                "dryRun": true,
            }));
        }

        // Write the files (from the bytes that were hashed) before the
        // record, so a record never names a missing bundle directory.
        let target = self.apps.bundle_dir(&record.id, &record.bundle_hash);
        let old_dir = existing
            .as_ref()
            .filter(|old| old.bundle_hash != record.bundle_hash)
            .map(|old| self.apps.bundle_dir(&old.id, &old.bundle_hash));
        tokio::task::spawn_blocking(move || -> Result<()> {
            // The directory is named by the hash of exactly these bytes, so
            // an existing one (a same-hash replace) already holds them.
            if target.is_dir() {
                return Ok(());
            }
            bundle::write_bundle(&bundle, &target)
                .map_err(|e| Error::Internal(format!("app bundle write failed: {e}")))
        })
        .await
        .map_err(|e| Error::Internal(format!("bundle write task failed: {e}")))??;
        self.save_app(&record).await?;
        if replaced {
            self.apps
                .sessions()
                .end_app(&record.id, AppSessionEnd::Updated);
        }
        if let Some(old_dir) = old_dir {
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(old_dir)).await;
        }
        tracing::info!(app_id = %record.id, bundle_hash = %record.bundle_hash, replaced, "app installed");
        self.publish_app_event(
            intent_core::events::APP_INSTALLED,
            json!({
                "appId": record.id,
                "version": record.version,
                "bundleHash": record.bundle_hash,
                "replaced": replaced,
            }),
        )
        .await;
        if approval_reset {
            self.publish_approval_changed(&record).await;
        }
        Ok(json!({
            "app": self.app_wire(&record).await,
            "replaced": replaced,
            "approvalReset": approval_reset,
            "dryRun": false,
        }))
    }

    async fn app_uninstall(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.uninstall")?;
        let app_id = req_str(params, "appId")?.to_owned();
        let _library = self.apps.library.lock().await;
        let _storage = self.apps.storage_gate.write().await;
        self.apps
            .sessions()
            .end_app(&app_id, AppSessionEnd::Uninstalled);
        let removed = self.store.delete_app(&app_id).await?;
        if !removed {
            return Ok(json!({ "removed": false }));
        }
        if bundle::is_app_id(&app_id) {
            let dir = self.apps.app_dir(&app_id);
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dir)).await;
        }
        tracing::info!(app_id = %app_id, "app uninstalled");
        self.publish_app_event(intent_core::events::APP_REMOVED, json!({ "appId": app_id }))
            .await;
        Ok(json!({ "removed": true }))
    }

    async fn app_list(&self) -> Result<Value> {
        require_library_caller("app.list")?;
        let mut records = Vec::new();
        for json in self.store.list_app_records().await? {
            let record: AppRecord = serde_json::from_str(&json)
                .map_err(|e| Error::Internal(format!("app record is unreadable: {e}")))?;
            records.push(record);
        }
        records.sort_by(|a, b| {
            a.name
                .to_lowercase()
                .cmp(&b.name.to_lowercase())
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut apps = Vec::with_capacity(records.len());
        for record in &records {
            apps.push(self.app_wire(record).await);
        }
        Ok(json!({ "apps": apps }))
    }

    async fn app_get(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.get")?;
        let app_id = req_str(params, "appId")?;
        let record = self.load_app(app_id).await?.ok_or_else(app_not_found)?;
        Ok(json!({ "app": self.app_wire(&record).await }))
    }

    async fn app_approve(&self, params: &Map<String, Value>) -> Result<Value> {
        require_user("app.approve")?;
        let app_id = req_str(params, "appId")?.to_owned();
        let bundle_hash = req_str(params, "bundleHash")?.to_owned();
        let level = match params.get("level") {
            Some(v) if v.is_u64() => v.as_u64().unwrap_or(0),
            None | Some(Value::Null) => return Err(app_invalid_params("level is required")),
            Some(_) => return Err(app_invalid_params("level must be an integer")),
        };
        let mut features: Vec<String> = match params.get("features") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| app_invalid_params("features must be strings"))
                })
                .collect::<Result<_>>()?,
            None | Some(Value::Null) => return Err(app_invalid_params("features is required")),
            Some(_) => return Err(app_invalid_params("features must be an array")),
        };
        features.sort();
        let before = features.len();
        features.dedup();
        if features.len() != before {
            return Err(app_invalid_params("features must be unique"));
        }

        let _library = self.apps.library.lock().await;
        let mut record = self.load_app(&app_id).await?.ok_or_else(app_not_found)?;
        if bundle_hash != record.bundle_hash {
            return Err(hash_mismatch(&record.bundle_hash));
        }
        if level == 3 {
            return Err(level_unavailable());
        }
        if level != u64::from(record.permissions.level) {
            return Err(app_invalid_params("level must equal permissions.level"));
        }
        if !features
            .iter()
            .all(|f| record.permissions.features.contains(f))
        {
            return Err(app_invalid_params(
                "features must be a subset of permissions.features",
            ));
        }
        let level = record.permissions.level;
        if record.approved_for_installed_hash()
            && record.approval.level == Some(level)
            && record.approval.features == features
        {
            return Ok(json!({ "app": self.app_wire(&record).await }));
        }
        let features_changed =
            record.approval.status == "approved" && record.approval.features != features;
        let now = now_iso();
        record.approval = Approval {
            status: "approved".into(),
            level: Some(level),
            features,
            bundle_hash: Some(record.bundle_hash.clone()),
            approved_at: Some(now.clone()),
        };
        record.updated_at = now;
        self.save_app(&record).await?;
        if features_changed {
            self.apps
                .sessions()
                .end_app(&record.id, AppSessionEnd::Updated);
        }
        tracing::info!(app_id = %record.id, bundle_hash = %record.bundle_hash, "app approved");
        self.publish_approval_changed(&record).await;
        Ok(json!({ "app": self.app_wire(&record).await }))
    }

    async fn app_revoke(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.revoke")?;
        let app_id = req_str(params, "appId")?.to_owned();
        let _library = self.apps.library.lock().await;
        let mut record = self.load_app(&app_id).await?.ok_or_else(app_not_found)?;
        self.apps
            .sessions()
            .end_app(&record.id, AppSessionEnd::Revoked);
        if record.approval.status != "approved" {
            return Ok(json!({ "app": self.app_wire(&record).await, "revoked": false }));
        }
        record.approval.status = "revoked".into();
        record.updated_at = now_iso();
        self.save_app(&record).await?;
        tracing::info!(app_id = %record.id, "app approval revoked");
        self.publish_approval_changed(&record).await;
        Ok(json!({ "app": self.app_wire(&record).await, "revoked": true }))
    }

    async fn app_read_bundle(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.readBundle")?;
        let app_id = req_str(params, "appId")?;
        let path = req_str(params, "path")?.to_owned();
        let expected = opt_str(params, "bundleHash")?;
        let record = self.load_app(app_id).await?.ok_or_else(app_not_found)?;
        if expected.is_some_and(|h| h != record.bundle_hash) {
            return Err(hash_mismatch(&record.bundle_hash));
        }
        let dir = self.apps.bundle_dir(&record.id, &record.bundle_hash);
        let read_path = path.clone();
        let bytes = tokio::task::spawn_blocking(move || bundle::read_installed(&dir, &read_path))
            .await
            .map_err(|e| Error::Internal(format!("bundle read task failed: {e}")))?
            .ok_or_else(|| app_refusal("file-not-found", "no such file in the bundle"))?;
        Ok(json!({
            "path": path,
            "mimeType": bundle::mime_type(&path),
            "sizeBytes": bytes.len(),
            "contentBase64": base64::engine::general_purpose::STANDARD.encode(&bytes),
        }))
    }

    async fn app_session_open(&self, params: &Map<String, Value>) -> Result<Value> {
        require_user("app.session.open")?;
        let app_id = req_str(params, "appId")?.to_owned();
        let _library = self.apps.library.lock().await;
        let record = self.load_app(&app_id).await?.ok_or_else(app_not_found)?;
        if !record.approved_for_installed_hash() {
            return Err(app_refusal(
                "not-approved",
                "the app is not approved for its installed bundle",
            ));
        }
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw)
            .map_err(|e| Error::Internal(format!("session token generation failed: {e}")))?;
        let token = bundle::hex(&raw);
        let session_id = format!("as_{}", uuid::Uuid::new_v4().simple());
        let level = record.approval.level.unwrap_or(record.permissions.level);
        let principal = Arc::new(AppPrincipal {
            app_id: record.id.clone(),
            name: record.name.clone(),
            bundle_hash: record.bundle_hash.clone(),
            level,
            features: record.approval.features.clone(),
            session_id: session_id.clone(),
        });
        let result = json!({
            "sessionId": session_id,
            "appId": record.id,
            "bundleHash": record.bundle_hash,
            "level": level,
            "features": record.approval.features,
            "entry": record.entry,
            "window": record.window,
        });
        let (end, _) = tokio::sync::watch::channel(false);
        {
            let mut sessions = self.apps.sessions();
            // At most one session per app: a new open ends the old one.
            sessions.end_app(&record.id, AppSessionEnd::Closed);
            let hash = token_hash(&token);
            sessions.tokens.insert(hash, session_id.clone());
            sessions
                .apps_open
                .insert(record.id.clone(), session_id.clone());
            sessions.ids.insert(
                session_id.clone(),
                SessionEntry {
                    principal,
                    token_hash: hash,
                    result: result.clone(),
                    end,
                },
            );
        }
        tracing::info!(app_id = %record.id, session_id = %session_id, "app session opened");
        let mut out = result;
        if let Some(obj) = out.as_object_mut() {
            obj.insert("token".into(), json!(token));
        }
        Ok(out)
    }

    fn app_session_close(&self, params: &Map<String, Value>) -> Result<Value> {
        require_library_caller("app.session.close")?;
        let session_id = req_str(params, "sessionId")?;
        let closed = self.apps.sessions().end(session_id, AppSessionEnd::Closed);
        Ok(json!({ "closed": closed }))
    }

    /// `app.session.attach`: the token's live session, with an end signal
    /// subscribed under the same lock that proves it is live, so an end
    /// after this returns is always observed.
    pub(crate) fn app_session_attach_op(&self, token: &str) -> Result<AppAttachment> {
        require_library_caller("app.session.attach")?;
        let invalid = || app_refusal("session-invalid", "the app session is unknown or ended");
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let sessions = self.apps.sessions();
        let id = sessions
            .tokens
            .get(&token_hash(token))
            .ok_or_else(invalid)?;
        let entry = sessions.ids.get(id).ok_or_else(invalid)?;
        Ok(AppAttachment {
            principal: entry.principal.clone(),
            result: entry.result.clone(),
            ended: entry.end.subscribe(),
        })
    }

    /// End the sessions an administrator connection opened (it closed).
    pub(crate) fn app_sessions_end_op(&self, session_ids: &[String]) {
        let mut sessions = self.apps.sessions();
        for id in session_ids {
            sessions.end(id, AppSessionEnd::Closed);
        }
    }

    /// The bound app, re-checked against the live session table: a call that
    /// raced its session's end is refused even before the transport closes
    /// the connection.
    fn live_app(&self) -> Result<Arc<AppPrincipal>> {
        let app = require_app_caller()?;
        if self.apps.sessions().ids.contains_key(&app.session_id) {
            Ok(app)
        } else {
            Err(Error::Forbidden("app session ended".into()))
        }
    }

    /// Enter the storage gate for a write and recheck, under it, that the
    /// session is still live (an uninstall in between ended it).
    async fn storage_write_guard(
        &self,
        app: &AppPrincipal,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        #[cfg(test)]
        {
            let pause = self
                .apps
                .storage_write_pause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.release.notified().await;
            }
        }
        let guard = self.apps.storage_gate.read().await;
        if self.apps.sessions().ids.contains_key(&app.session_id) {
            Ok(guard)
        } else {
            Err(Error::Forbidden("app session ended".into()))
        }
    }

    async fn app_storage_get(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        let key = storage_key(params)?;
        match self.store.app_storage_get(&app.app_id, &key).await? {
            None => Ok(json!({ "found": false, "value": Value::Null })),
            Some(json) => {
                let value: Value = serde_json::from_str(&json)
                    .map_err(|e| Error::Internal(format!("app storage value unreadable: {e}")))?;
                Ok(json!({ "found": true, "value": value }))
            }
        }
    }

    async fn app_storage_set(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        let key = storage_key(params)?;
        let Some(value) = params.get("value") else {
            return Err(app_invalid_params("value is required"));
        };
        let encoded = serde_json::to_string(value)
            .map_err(|e| Error::Internal(format!("app storage encode failed: {e}")))?;
        if encoded.len() > STORAGE_VALUE_MAX_BYTES {
            return Err(app_refusal(
                "value-too-large",
                "the serialized value is larger than 256 KiB",
            ));
        }
        let size = (key.len() + encoded.len()) as u64;
        let _storage = self.storage_write_guard(&app).await?;
        match self
            .store
            .app_storage_set(&app.app_id, &key, &encoded, size, STORAGE_QUOTA_BYTES)
            .await?
        {
            AppStorageWrite::Written { used_bytes } => Ok(json!({ "usedBytes": used_bytes })),
            AppStorageWrite::OverQuota { used_bytes } => Err(AppError::params(
                "quota-exceeded",
                "the app storage quota is exceeded",
                json!({ "limitBytes": STORAGE_QUOTA_BYTES, "usedBytes": used_bytes }),
            )
            .into_error()),
        }
    }

    async fn app_storage_delete(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        let key = storage_key(params)?;
        let _storage = self.storage_write_guard(&app).await?;
        let deleted = self.store.app_storage_delete(&app.app_id, &key).await?;
        Ok(json!({ "deleted": deleted }))
    }

    async fn app_storage_list(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        let prefix = opt_str(params, "prefix")?.unwrap_or("");
        let after = opt_str(params, "cursor")?.map(decode_cursor).transpose()?;
        let limit = opt_u64(params, "limit")?.unwrap_or(100);
        if !(1..=1000).contains(&limit) {
            return Err(app_invalid_params("limit must be 1–1000"));
        }
        let limit = u32::try_from(limit).unwrap_or(1000);
        let mut keys = self
            .store
            .app_storage_list(&app.app_id, prefix, after.as_deref(), limit + 1)
            .await?;
        let next_cursor = if keys.len() > limit as usize {
            keys.truncate(limit as usize);
            keys.last().map(|k| bundle::hex(k.as_bytes()))
        } else {
            None
        };
        Ok(json!({ "keys": keys, "nextCursor": next_cursor }))
    }

    fn app_notify(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        require_feature(&app, APP_FEATURE_NOTIFICATIONS)?;
        let title = req_str(params, "title")?.trim();
        let title_len = title.chars().count();
        if title_len == 0 || title_len > 128 {
            return Err(app_invalid_params("title must be 1–128 chars"));
        }
        if opt_str(params, "body")?.is_some_and(|b| b.chars().count() > 1024) {
            return Err(app_invalid_params("body must be at most 1024 chars"));
        }
        if opt_str(params, "tag")?.is_some_and(|t| !(1..=64).contains(&t.chars().count())) {
            return Err(app_invalid_params("tag must be 1–64 chars"));
        }
        {
            let mut map = self
                .apps
                .notify
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            let window = map.entry(app.app_id.clone()).or_default();
            while window
                .front()
                .is_some_and(|t| now.duration_since(*t) >= NOTIFY_WINDOW)
            {
                window.pop_front();
            }
            if window.len() >= NOTIFY_LIMIT {
                let oldest = window.front().copied().unwrap_or(now);
                let wait = NOTIFY_WINDOW.saturating_sub(now.duration_since(oldest));
                return Err(rate_limited(
                    u64::try_from(wait.as_millis()).unwrap_or(u64::MAX).max(1),
                ));
            }
            window.push_back(now);
        }
        Ok(json!({ "notificationId": format!("an_{}", uuid::Uuid::new_v4().simple()) }))
    }

    /// The `AppModelProvider` rows and the default provider (§5.60).
    /// The `AppModelProvider` rows and the default provider (§5.60): every
    /// registered one-shot provider and every named profile of one, as the
    /// exact reference the shared one-shot route runs (a profile keeps its
    /// own account). `available` is the same readiness decision the route
    /// depends on: enabled, not refused by a cached sign-in verdict, and
    /// installed (adapter and CLI) — or, on an orchestrator, enabled only,
    /// since the call runs on a machine.
    fn app_model_providers(&self) -> (Option<String>, Vec<Value>) {
        let settings = self.effective_settings();
        let catalog = self.cached_models();
        let mut refs: Vec<(String, String)> = Vec::new();
        for id in ONE_SHOT_PROVIDERS {
            if intent_providers::find_provider(id).is_some() {
                refs.push((
                    (*id).to_owned(),
                    intent_providers::provider_config(id)
                        .display_name
                        .to_owned(),
                ));
            }
        }
        for meta in self.provider_profiles.profiles().unwrap_or_default().iter() {
            let base = meta.base_provider.as_str();
            if ONE_SHOT_PROVIDERS.contains(&base) && intent_providers::find_provider(base).is_some()
            {
                refs.push((
                    format!("{base}@{}", meta.id),
                    format!(
                        "{} ({})",
                        intent_providers::provider_config(base).display_name,
                        meta.name
                    ),
                ));
            }
        }
        let local = self.may_spawn_provider_locally();
        let mut providers = Vec::new();
        for (provider_ref, display) in &refs {
            let is_profile = intent_providers::split_profile_ref(provider_ref).is_some();
            // A profile, and a Default with saved overrides, runs with its
            // own overlay: the bare installation's sign-in verdict does not
            // apply to it (as for agent creation).
            let check_bare_verdict = !is_profile && !self.default_has_overrides(provider_ref);
            let is_auggie = intent_providers::base_provider_id(provider_ref) == "auggie";
            let method = "app.complete";
            let ready = if !local {
                crate::agent_ops::ensure_provider_enabled(
                    method,
                    provider_ref,
                    settings.providers.enabled.as_ref(),
                )
            } else if is_auggie {
                // The auggie print route resolves its CLI itself
                // (`context.auggiePath` exclusively when set, else
                // discovery), so its readiness is that resolution.
                crate::agent_ops::ensure_provider_enabled(
                    method,
                    provider_ref,
                    settings.providers.enabled.as_ref(),
                )
                .and_then(|()| {
                    if check_bare_verdict {
                        crate::agent_ops::ensure_provider_authenticated(
                            method,
                            provider_ref,
                            crate::provider_auth::cached_auth_verdict(provider_ref),
                        )
                    } else {
                        Ok(())
                    }
                })
                .and_then(|()| self.one_shot_auggie_bin().map(|_| ()))
            } else {
                crate::agent_ops::ensure_provider_available_checked(
                    method,
                    provider_ref,
                    &settings.providers,
                    check_bare_verdict,
                )
            }
            .and_then(|()| {
                self.ensure_provider_ref_selectable_in(provider_ref, &settings.providers)
            });
            let models: Vec<Value> = catalog
                .cached_catalog_rows(provider_ref)
                .unwrap_or_default()
                .into_iter()
                .map(|row| json!({ "id": row.id, "displayName": row.name }))
                .collect();
            providers.push(json!({
                "providerId": provider_ref,
                "displayName": display,
                "available": ready.is_ok(),
                "reason": ready.err().map(|e| e.to_string()),
                "defaultModel": catalog.cached_default_model(provider_ref),
                "models": models,
            }));
        }
        // The effective default as configured (a profile reference stays a
        // profile reference), when it is one of the rows.
        let default = crate::agent_session::derived_default_provider(&settings)
            .filter(|p| refs.iter().any(|(r, _)| r == p));
        (default, providers)
    }

    fn app_models_list(&self) -> Result<Value> {
        let app = self.live_app()?;
        require_feature(&app, APP_FEATURE_MODELS)?;
        let (default, providers) = self.app_model_providers();
        Ok(json!({ "defaultProvider": default, "providers": providers }))
    }

    async fn app_complete(&self, params: &Map<String, Value>) -> Result<Value> {
        let app = self.live_app()?;
        require_feature(&app, APP_FEATURE_MODELS)?;
        let prompt = req_str(params, "prompt")?;
        let prompt_len = prompt.chars().count();
        if prompt_len == 0 || prompt_len > 200_000 {
            return Err(app_invalid_params("prompt must be 1–200000 chars"));
        }
        let system = opt_str(params, "system")?;
        if system.is_some_and(|s| s.chars().count() > 50_000) {
            return Err(app_invalid_params("system must be at most 50000 chars"));
        }
        let reasoning_effort = opt_str(params, "reasoningEffort")?;
        let timeout_ms = opt_u64(params, "timeoutMs")?.unwrap_or(COMPLETE_DEFAULT_TIMEOUT_MS);
        if !(1000..=300_000).contains(&timeout_ms) {
            return Err(app_invalid_params("timeoutMs must be 1000–300000"));
        }
        let (default, providers) = self.app_model_providers();
        let (provider_id, model) = match resolve_app_route(
            opt_str(params, "provider")?,
            opt_str(params, "model")?,
            default.as_deref(),
            &providers,
        )? {
            AppRoute::Run { provider, model } => (provider, model),
            AppRoute::Unavailable(reason) => {
                return Ok(json!({ "available": false, "reason": reason }))
            }
        };

        let _slot = {
            let mut map = self
                .apps
                .in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let n = map.entry(app.app_id.clone()).or_insert(0);
            if *n >= COMPLETE_MAX_IN_FLIGHT {
                return Err(rate_limited(1000));
            }
            *n += 1;
            InFlight {
                state: &self.apps,
                app_id: app.app_id.clone(),
            }
        };
        // No workspace: a private temporary directory, removed after.
        let cwd = tempfile::Builder::new()
            .prefix("intent-app-")
            .tempdir()
            .map_err(|e| Error::Internal(format!("app.complete temp dir failed: {e}")))?;
        tracing::info!(
            app_id = %app.app_id,
            provider = %provider_id,
            model = model.as_deref().unwrap_or(""),
            "app.complete"
        );
        // The call runs in its own task and the deadline is applied from
        // here: a provider step that blocks its thread (CLI discovery, a
        // stuck spawn) cannot hold the timeout back. On expiry the task is
        // aborted, which drops the provider child (`kill_on_drop`) and the
        // private cwd. The inner route keeps a longer deadline of its own.
        let inner_timeout = timeout_ms + 5_000;
        let svc = self.clone();
        let (run_provider, run_model) = (provider_id.clone(), model.clone());
        let (prompt, system, effort) = (
            prompt.to_owned(),
            system.map(str::to_owned),
            reasoning_effort.map(str::to_owned),
        );
        let task = intent_core::caller::spawn_with_current_caller(async move {
            let settings = svc.effective_settings();
            let effective = crate::agent_session::derived_default_provider(&settings);
            let result = Box::pin(svc.complete_once_on_route(
                &settings,
                effective.as_deref(),
                run_provider,
                run_model,
                None,
                effort.as_deref(),
                prompt,
                system,
                crate::complete_ops::CompleteOnceCwd::Private(cwd.path().to_path_buf()),
                Some(inner_timeout),
                inner_timeout,
            ))
            .await;
            drop(cwd);
            result
        });
        let abort = task.abort_handle();
        let outcome = match tokio::time::timeout(Duration::from_millis(timeout_ms), task).await {
            Err(_) => {
                abort.abort();
                Err(())
            }
            Ok(Ok(result)) => Ok(result),
            Ok(Err(join)) => {
                return Err(Error::Internal(format!("app.complete task failed: {join}")))
            }
        };
        let timeout =
            || AppError::service("timeout", "the model call timed out", json!({})).into_error();
        let result = match outcome {
            Err(()) => return Err(timeout()),
            Ok(Err(e)) if e.to_string().contains("timed out") => return Err(timeout()),
            Ok(other) => other?,
        };
        if result.get("available") == Some(&json!(false)) {
            return Ok(json!({
                "available": false,
                "reason": result.get("reason").cloned().unwrap_or(Value::Null),
            }));
        }
        let text = result
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let model_used = result
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(model)
            .unwrap_or_default();
        Ok(json!({
            "available": true,
            "text": text,
            "provider": provider_id,
            "model": model_used,
        }))
    }
}

/// The route `app.complete` runs on (§5.60 "explicit provider routing").
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AppRoute {
    Run {
        provider: String,
        model: Option<String>,
    },
    Unavailable(String),
}

/// Choose the provider and model for `app.complete`: an explicit `provider`
/// is used as given (the default never overrides it) and must be listed;
/// `model` resolves against that provider only, else its `defaultModel`.
/// No provider at all, or an unavailable one, is the typed unavailable
/// result.
pub(crate) fn resolve_app_route(
    provider: Option<&str>,
    model: Option<&str>,
    default: Option<&str>,
    providers: &[Value],
) -> Result<AppRoute> {
    let Some(provider_id) = provider.or(default) else {
        return Ok(AppRoute::Unavailable(
            "No default model provider is set.".into(),
        ));
    };
    let Some(row) = providers
        .iter()
        .find(|p| p["providerId"].as_str() == Some(provider_id))
    else {
        return Err(app_refusal("provider-unknown", "unknown provider"));
    };
    let model = match model {
        Some(m) => {
            let known = row["models"]
                .as_array()
                .is_some_and(|ms| ms.iter().any(|x| x["id"].as_str() == Some(m)));
            if !known {
                return Err(app_refusal("model-unknown", "unknown model"));
            }
            Some(m.to_owned())
        }
        None => row["defaultModel"].as_str().map(str::to_owned),
    };
    if row["available"] == json!(false) {
        return Ok(AppRoute::Unavailable(
            row["reason"]
                .as_str()
                .unwrap_or("The provider is not available.")
                .to_owned(),
        ));
    }
    Ok(AppRoute::Run {
        provider: provider_id.to_owned(),
        model,
    })
}

/// The default library root for a `Services` without a configured data dir
/// (tests): a per-instance temporary directory, never shared.
pub(crate) fn default_apps_root() -> PathBuf {
    std::env::temp_dir().join(format!("intentd-apps-{}", uuid::Uuid::new_v4().simple()))
}

/// `<data dir>/apps`.
pub(crate) fn apps_root(data_dir: &Path) -> PathBuf {
    data_dir.join("apps")
}
