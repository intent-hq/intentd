//! Staged, chunked attachment upload (`file.attachmentUpload.*`, PROTOCOL
//! §5.9): the counterpart of the single-shot `file.placeAttachment` for
//! payloads larger than one RPC frame, pattern-matched on the staged
//! workspace import (`transfer_import.rs`). `begin` validates the header
//! (workspace, name, declared size ≤ the 1 GiB cap, sha) and opens a staging
//! session; `chunk` stages seq-numbered base64 slices (idempotent per seq —
//! a retry overwrites the same chunk file; any-order arrival); `commit`
//! reassembles the payload, verifies its SHA-256, and delegates to the same
//! placement + attachment-registry path `file.placeAttachment` uses, so the
//! result is byte-shape-identical to a successful `placeAttachment`; `abort`
//! deletes the staging state. Sessions are in-memory only — a daemon restart
//! drops them and orphaned staging dirs are swept lazily by the next
//! `begin` — and nothing is visible (no file, no registry row) until
//! `commit` succeeds. Sessions are bounded (monorepo#2275): each workspace
//! may hold at most [`ATTACHMENT_UPLOAD_MAX_SESSIONS_PER_WORKSPACE`] live
//! sessions, and a session idle past [`ATTACHMENT_UPLOAD_IDLE_TTL`] (no
//! begin/chunk/commit activity) is expired lazily — reclaimed by the next
//! `begin` and reported as a clear caller error to its own late calls.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine as _;
use intent_core::{Error, Result, WorkspaceId};
use sha2::Digest as _;

use crate::Services;

/// Maximum DECODED bytes per `file.attachmentUpload.chunk` call. Base64
/// inflates this by 4/3 on the wire (~21.4 MiB), keeping the full JSON-RPC
/// frame comfortably under the 40 MiB inbound cap (PROTOCOL §1.3). Same
/// value as the import surface's `IMPORT_MAX_CHUNK_BYTES`.
pub(crate) const ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Maximum declared attachment size accepted by `begin` (decoded bytes).
pub(crate) const ATTACHMENT_UPLOAD_MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

/// Maximum live staged-upload sessions per workspace (monorepo#2275): a
/// `begin` beyond the cap is rejected until one settles (commit/abort) or
/// expires. Bounds the staging disk a single workspace can pin.
pub(crate) const ATTACHMENT_UPLOAD_MAX_SESSIONS_PER_WORKSPACE: usize = 4;

/// How long a session may sit with no begin/chunk/commit activity before it
/// is expired and its staging reclaimed (monorepo#2275). Generous next to
/// the per-chunk cadence of a live upload — even a slow link lands a 16 MiB
/// chunk well inside 15 minutes — while bounding how long an abandoned
/// session (client crash, dropped connection) pins up to 1 GiB of staging.
const ATTACHMENT_UPLOAD_IDLE_TTL: Duration = Duration::from_secs(15 * 60);

/// Resolve the idle TTL, honoring the `INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS`
/// test seam (milliseconds) so regression coverage need not wait 15 minutes.
fn attachment_upload_idle_ttl() -> Duration {
    std::env::var("INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(ATTACHMENT_UPLOAD_IDLE_TTL, Duration::from_millis)
}

/// One in-flight staged attachment upload: everything `chunk`/`commit`/
/// `abort` need between calls. Lives in [`Services::attachment_uploads`];
/// in-memory only.
pub(crate) struct AttachmentUploadSession {
    pub workspace_id: WorkspaceId,
    pub file_name: String,
    pub mime_type: Option<String>,
    /// `<workspaces_root>/.attachment-upload-staging/<uploadId>/`.
    pub staging_dir: PathBuf,
    /// Declared final payload size — chunks may not exceed it in sum.
    pub declared_size: u64,
    /// Declared lowercase-hex SHA-256 of the complete payload.
    pub declared_sha256: String,
    /// Bytes staged so far, keyed by chunk seq (a retried seq replaces its
    /// entry, so the sum never double-counts).
    pub chunk_sizes: HashMap<u64, u64>,
    /// Set while `commit` is verifying/placing: chunks and aborts are
    /// rejected so concurrent calls cannot mutate the files being hashed or
    /// race the commit's cleanup. Cleared on a failed commit (the session
    /// survives for retry or abort).
    pub committing: bool,
    /// Last begin/chunk/commit activity — the idle-TTL clock. A committing
    /// session never expires (the flag guards it), and a failed commit
    /// refreshes the clock so the retry window restarts.
    pub last_activity: Instant,
    /// Client-minted idempotency key from `begin` (intent-hq/intent#4691),
    /// bound at commit like a keyed `file.placeAttachment`.
    pub idempotency_key: Option<String>,
}

impl AttachmentUploadSession {
    /// Payload identity the session's key binds to (the chunked arm's
    /// fingerprint includes the declared SHA-256).
    fn fingerprint(&self) -> String {
        attachment_fingerprint(
            &self.file_name,
            self.declared_size,
            Some(&self.declared_sha256),
        )
    }

    /// Idle past the TTL and safe to reclaim. Never true while a commit is
    /// in flight — expiring mid-commit would race the files being hashed.
    fn expired(&self, ttl: Duration) -> bool {
        !self.committing && self.last_activity.elapsed() >= ttl
    }

    fn received_bytes(&self) -> u64 {
        self.chunk_sizes.values().sum()
    }
}

/// Chunk file name for `seq` inside the staging dir (zero-padded so a
/// directory listing sorts in seq order for humans; commit reads by index).
fn chunk_file_name(seq: u64) -> String {
    format!("chunk-{seq:08}")
}

/// Remove every idle-expired session from the registry, returning their
/// staging dirs for the caller to delete OUTSIDE the lock (filesystem I/O
/// never happens under the registry mutex). Committing sessions are never
/// drained.
fn drain_expired_sessions(uploads: &mut HashMap<String, AttachmentUploadSession>) -> Vec<PathBuf> {
    let ttl = attachment_upload_idle_ttl();
    let expired: Vec<String> = uploads
        .iter()
        .filter(|(_, s)| s.expired(ttl))
        .map(|(id, _)| id.clone())
        .collect();
    expired
        .iter()
        .filter_map(|id| {
            tracing::info!(upload = %id, "expiring idle attachment upload session");
            uploads.remove(id).map(|s| s.staging_dir)
        })
        .collect()
}

/// Retention window for idempotency-key bindings (intent-hq/intent#4691):
/// after this the key reads as unknown and a retry places a fresh copy —
/// the recovery window a client needs is minutes, not days.
const ATTACHMENT_IDEMPOTENCY_RETENTION_MINUTES: i64 = 7 * 24 * 60;

/// Maximum length of a client-supplied `idempotencyKey`.
const ATTACHMENT_IDEMPOTENCY_KEY_MAX_LEN: usize = 128;

/// Validate the optional `idempotencyKey` param: absent stays absent; a
/// present value must be 1–128 characters with no surrounding whitespace
/// trimmed away (the key is an opaque client-minted token — a UUID is the
/// recommended shape).
pub(crate) fn validate_idempotency_key(key: Option<String>) -> Result<Option<String>> {
    let Some(key) = key else {
        return Ok(None);
    };
    if key.is_empty() || key.trim().len() != key.len() {
        return Err(Error::InvalidParams(
            "idempotencyKey must be a non-empty string without surrounding whitespace".to_string(),
        ));
    }
    if key.chars().count() > ATTACHMENT_IDEMPOTENCY_KEY_MAX_LEN {
        return Err(Error::InvalidParams(format!(
            "idempotencyKey exceeds {ATTACHMENT_IDEMPOTENCY_KEY_MAX_LEN} characters"
        )));
    }
    Ok(Some(key))
}

/// The ISO-8601 UTC instant bindings created at/before which are expired.
pub(crate) fn idempotency_retention_cutoff() -> String {
    intent_core::iso_minutes_ago(ATTACHMENT_IDEMPOTENCY_RETENTION_MINUTES)
}

/// Payload identity an idempotency key is bound to: the requested
/// `fileName` (pre-sanitization, pre-suffix — what the client resends), the
/// byte size, and — for the base64 and chunked arms — the lowercase-hex
/// SHA-256. Encoded as a JSON array so a `fileName` can carry any character.
pub(crate) fn attachment_fingerprint(file_name: &str, size: u64, sha256: Option<&str>) -> String {
    serde_json::json!([file_name, size, sha256.map(str::to_ascii_lowercase)]).to_string()
}

/// Lowercase-hex SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for b in digest {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

const CONFLICTING_PAYLOAD: &str = "idempotencyKey already used with a different payload";

/// A keyed placement request (intent-hq/intent#4691).
pub(crate) struct KeyedPlacement {
    pub(crate) key: String,
    /// Payload identity for the pre-placement replay / conflict lookup.
    /// `None` (an unreadable `sourcePath`) skips the lookup so placement
    /// classifies the source error.
    pub(crate) lookup_fingerprint: Option<String>,
    /// SHA-256 part of the bound fingerprint (base64 + chunked arms). The
    /// size part is always the PLACED byte length, never a pre-copy stat —
    /// a `sourcePath` file resized between stat and copy would otherwise
    /// bind a size the attachment does not have.
    pub(crate) sha256: Option<String>,
}

/// Per-`(workspace, idempotencyKey)` in-flight locks for keyed placements
/// (see `Services::attachment_idempotency_inflight`).
pub(crate) type IdempotencyInflight =
    HashMap<(WorkspaceId, String), std::sync::Arc<tokio::sync::Mutex<()>>>;

/// What a `commit` takes off its session under the registry lock before the
/// fallible reassembly + placement runs: `(staging_dir, declared_size,
/// declared_sha256, workspace_id, file_name, mime_type, seqs,
/// idempotency_key)`.
type CommitClaim = (
    PathBuf,
    u64,
    String,
    WorkspaceId,
    String,
    Option<String>,
    Vec<u64>,
    Option<String>,
);

/// The original placement result rebuilt from its registry row, plus the
/// additive `replayed: true` marker. The disk is deliberately not
/// re-checked — `file.getAttachmentInfo.exists` is the disk signal.
fn replayed_placement_result(record: &intent_store::AttachmentRecord) -> serde_json::Value {
    let mut result = serde_json::json!({
        "ok": true,
        "path": record.stored_path,
        "fileName": record.file_name,
        "size": record.size,
        "attachmentId": record.id,
        "uploadedAt": record.uploaded_at,
        "replayed": true,
    });
    if let Some(mime) = &record.mime_type {
        result["mimeType"] = serde_json::json!(mime);
    }
    result
}

impl Services {
    /// Best-effort retention sweep of expired idempotency-key bindings
    /// (intent-hq/intent#4691): runs at boot and lazily on every keyed
    /// placement / `begin`. A failed sweep never fails the caller.
    pub async fn sweep_expired_attachment_idempotency_keys(&self) {
        self.sweep_expired_attachment_idempotency_keys_before(&idempotency_retention_cutoff())
            .await;
    }

    /// [`Self::sweep_expired_attachment_idempotency_keys`] at an explicit
    /// cutoff, so a keyed operation's sweep, lookup, and bind all judge
    /// expiry at the same instant.
    async fn sweep_expired_attachment_idempotency_keys_before(&self, cutoff: &str) {
        match self
            .store
            .sweep_expired_attachment_idempotency_keys(cutoff)
            .await
        {
            Ok(0) => {}
            Ok(removed) => {
                tracing::info!(removed, "swept expired attachment idempotency keys");
            }
            Err(e) => {
                tracing::warn!(error = %e, "attachment idempotency key sweep failed");
            }
        }
    }

    /// Take the per-`(workspace, key)` in-flight lock for a keyed placement.
    fn idempotency_inflight_lock(
        &self,
        workspace_id: &WorkspaceId,
        key: &str,
    ) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        self.attachment_idempotency_inflight
            .lock()
            .expect("attachment idempotency registry poisoned")
            .entry((workspace_id.clone(), key.to_string()))
            .or_default()
            .clone()
    }

    /// Drop the in-flight entry once no other caller holds it.
    fn release_idempotency_inflight_lock(
        &self,
        workspace_id: &WorkspaceId,
        key: &str,
        lock: &std::sync::Arc<tokio::sync::Mutex<()>>,
    ) {
        let mut inflight = self
            .attachment_idempotency_inflight
            .lock()
            .expect("attachment idempotency registry poisoned");
        // Two strong refs = the map's and ours; anyone else waiting on the
        // key holds a third.
        if std::sync::Arc::strong_count(lock) <= 2 {
            inflight.remove(&(workspace_id.clone(), key.to_string()));
        }
    }

    /// Shared placement + registry path behind `file.placeAttachment` and
    /// `file.attachmentUpload.commit` (PROTOCOL §5.9): resolves the
    /// workspace root, ensures the `.intent/` exclusion, places the payload
    /// collision-safely, and registers it under a daemon-minted UUID. With
    /// a [`KeyedPlacement`] (intent-hq/intent#4691) the whole sequence runs
    /// under the key's in-flight guard: a live binding with the same
    /// fingerprint short-circuits to the ORIGINAL result plus
    /// `replayed: true` (nothing placed), a live binding with a different
    /// fingerprint is `InvalidParams`, and a fresh key is bound in the same
    /// store transaction as the registry row — with the fingerprint's size
    /// taken from the placed bytes. An unset lookup fingerprint (an
    /// unreadable `sourcePath`) skips the lookup so placement classifies the
    /// source error; the primary key still rejects a double bind.
    pub(crate) async fn place_attachment_registered(
        &self,
        workspace_id: WorkspaceId,
        file_name: &str,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<String>,
        idempotency: Option<KeyedPlacement>,
    ) -> Result<serde_json::Value> {
        let Some(keyed) = idempotency else {
            return self
                .place_attachment_registered_inner(workspace_id, file_name, source, mime_type, None)
                .await;
        };
        // One cutoff for the sweep, the lookup, and the bind: a binding
        // that crosses the retention boundary between them is judged
        // expired consistently (the keyed insert replaces it in-transaction
        // rather than tripping the primary key).
        let cutoff = idempotency_retention_cutoff();
        self.sweep_expired_attachment_idempotency_keys_before(&cutoff)
            .await;
        let lock = self.idempotency_inflight_lock(&workspace_id, &keyed.key);
        let result = {
            let _guard = lock.lock().await;
            let bound = match &keyed.lookup_fingerprint {
                Some(_) => {
                    self.store
                        .get_attachment_by_idempotency_key(&workspace_id, &keyed.key, &cutoff)
                        .await
                }
                None => Ok(None),
            };
            match bound {
                Err(e) => Err(e),
                Ok(Some((binding, record))) => {
                    if Some(&binding.fingerprint) == keyed.lookup_fingerprint.as_ref() {
                        Ok(replayed_placement_result(&record))
                    } else {
                        Err(Error::InvalidParams(CONFLICTING_PAYLOAD.to_string()))
                    }
                }
                Ok(None) => {
                    self.place_attachment_registered_inner(
                        workspace_id.clone(),
                        file_name,
                        source,
                        mime_type,
                        Some((keyed.key.as_str(), keyed.sha256.as_deref(), cutoff.as_str())),
                    )
                    .await
                }
            }
        };
        self.release_idempotency_inflight_lock(&workspace_id, &keyed.key, &lock);
        result
    }

    /// `idempotency` = `(key, sha256, expired_before)`: the key is bound to
    /// `(fileName, placed size, sha256)`, replacing a binding of the same
    /// key created at/before `expired_before`.
    async fn place_attachment_registered_inner(
        &self,
        workspace_id: WorkspaceId,
        file_name: &str,
        source: &crate::file_ops::AttachmentSource<'_>,
        mime_type: Option<String>,
        idempotency: Option<(&str, Option<&str>, &str)>,
    ) -> Result<serde_json::Value> {
        let store = &self.store;
        let root = crate::file_ops::resolve_root(store, &workspace_id, None).await;
        if root.is_empty() {
            return Err(Error::Internal(
                "workspace has no resolved filesystem root".to_string(),
            ));
        }
        // The exclusion contract (monorepo#1948) rides on the default
        // `.intent/.gitignore` (ignore everything except config.json), so
        // make sure the directory + gitignore exist before placing.
        // `place_attachment` additionally drops an ignore-all `.gitignore`
        // inside `attachments/` to cover repos with a customized
        // `.intent/.gitignore`.
        crate::repo_config::ensure_intent_dir(std::path::Path::new(&root)).await?;
        let mut result =
            crate::file_ops::place_attachment(&root, file_name, source).map_err(|e| {
                // Surface placement failures in the daemon log so field
                // reports are diagnosable without a client-side trace
                // (monorepo#2144).
                tracing::warn!(
                    workspace = %workspace_id.as_str(),
                    file_name = %file_name,
                    error = %e,
                    "file.placeAttachment failed"
                );
                e
            })?;
        // Attachment registry (PROTOCOL §5.9): record the placed file
        // under a daemon-minted UUID so agents can retrieve it later via
        // `ws.file.getAttachment`, and return the registry fields
        // additively (presence-detected; old clients unaffected).
        let record = intent_store::AttachmentRecord {
            id: crate::new_uuid(),
            workspace_id,
            file_name: result["fileName"].as_str().unwrap_or(file_name).to_string(),
            mime_type: mime_type.filter(|m| !m.trim().is_empty()),
            size: result["size"].as_i64().unwrap_or_default(),
            uploaded_at: intent_core::now_iso(),
            stored_path: result["path"].as_str().unwrap_or_default().to_string(),
        };
        let inserted = match idempotency {
            Some((key, sha256, expired_before)) => {
                let placed_size = result["size"].as_u64().unwrap_or_default();
                let fingerprint = attachment_fingerprint(file_name, placed_size, sha256);
                store
                    .insert_attachment_with_idempotency_key(
                        &record,
                        key,
                        &fingerprint,
                        expired_before,
                    )
                    .await
            }
            None => store.insert_attachment(&record).await,
        };
        if let Err(e) = inserted {
            // Don't leave a durable-but-unregistered file behind: a
            // retry would place a collision-suffixed second copy that
            // no attachmentId can ever retrieve.
            let _ = std::fs::remove_file(std::path::Path::new(&root).join(&record.stored_path));
            return Err(e);
        }
        result["attachmentId"] = serde_json::json!(record.id);
        result["uploadedAt"] = serde_json::json!(record.uploaded_at);
        if let Some(mime) = &record.mime_type {
            result["mimeType"] = serde_json::json!(mime);
        }
        Ok(result)
    }

    /// Root directory staged attachment uploads live under. Sibling of the
    /// workspace checkouts, mirroring the import staging root.
    fn attachment_upload_staging_root(&self) -> PathBuf {
        self.workspaces_root
            .clone()
            .unwrap_or_else(crate::default_workspaces_root)
            .join(".attachment-upload-staging")
    }

    /// `file.attachmentUpload.begin`: validate the header and open a staging
    /// session. Rejects (all `InvalidParams` / `NotFound`, each naming the
    /// specifics, per monorepo#2144): an unknown workspace, a file name that
    /// placement would reject (empty, or a basename reducing to `.`/`..`/
    /// nothing — the same sanitization commit applies, so a doomed name
    /// fails here instead of after staging up to a gigabyte), a zero or
    /// over-cap declared size, a malformed sha, and — after idle-expired
    /// sessions are reclaimed — a workspace already holding
    /// [`ATTACHMENT_UPLOAD_MAX_SESSIONS_PER_WORKSPACE`] live sessions
    /// (monorepo#2275). Returns `{ uploadId, maxChunkBytes }`. With an
    /// `idempotency_key` (intent-hq/intent#4691): a key already bound to a
    /// committed attachment with the same `(fileName, sizeBytes, sha256)`
    /// is `InvalidParams` ("already committed; look it up") — begin stays
    /// shape-stable and the client recovers via `file.getAttachmentInfo`;
    /// a bound key with a different payload identity is `InvalidParams`
    /// (conflicting payload); a key held by a LIVE session of this
    /// workspace with the same identity replays that session's
    /// `{ uploadId, maxChunkBytes, replayed: true }` (a lost begin reply),
    /// while a different identity is the same conflict error.
    pub(crate) async fn file_attachment_upload_begin_op(
        &self,
        workspace_id: WorkspaceId,
        file_name: String,
        size_bytes: u64,
        sha256: String,
        mime_type: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value> {
        let idempotency_key = validate_idempotency_key(idempotency_key)?;
        if file_name.trim().is_empty() {
            return Err(Error::InvalidParams(
                "fileName must not be empty".to_string(),
            ));
        }
        if crate::file_ops::sanitize_attachment_name(&file_name).is_none() {
            return Err(Error::InvalidParams(format!(
                "invalid attachment fileName: {file_name:?}"
            )));
        }
        if size_bytes == 0 {
            return Err(Error::InvalidParams(
                "sizeBytes must be positive".to_string(),
            ));
        }
        if size_bytes > ATTACHMENT_UPLOAD_MAX_TOTAL_BYTES {
            return Err(Error::InvalidParams(format!(
                "sizeBytes {size_bytes} exceeds the {ATTACHMENT_UPLOAD_MAX_TOTAL_BYTES} byte attachment cap"
            )));
        }
        let sha = sha256.trim().to_ascii_lowercase();
        if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::InvalidParams(
                "sha256 must be 64 hex characters".to_string(),
            ));
        }
        // The workspace must exist NOW — failing at begin beats staging a
        // gigabyte and failing at commit.
        self.store
            .get_workspace(&workspace_id)
            .await
            .map_err(|e| match e {
                Error::NotFound(_) => {
                    Error::NotFound(format!("unknown workspace: {}", workspace_id.0))
                }
                other => other,
            })?;

        let fingerprint = attachment_fingerprint(&file_name, size_bytes, Some(&sha));
        if let Some(key) = &idempotency_key {
            let cutoff = idempotency_retention_cutoff();
            self.sweep_expired_attachment_idempotency_keys_before(&cutoff)
                .await;
            if let Some((binding, _)) = self
                .store
                .get_attachment_by_idempotency_key(&workspace_id, key, &cutoff)
                .await?
            {
                if binding.fingerprint != fingerprint {
                    return Err(Error::InvalidParams(CONFLICTING_PAYLOAD.to_string()));
                }
                return Err(Error::InvalidParams(format!(
                    "idempotencyKey {key:?} already committed; look it up via \
                     file.getAttachmentInfo {{ workspaceId, idempotencyKey }}"
                )));
            }
        }

        let upload_id = format!("upload-{}", uuid::Uuid::new_v4());
        let staging_dir = self.attachment_upload_staging_root().join(&upload_id);

        // Register the session BEFORE any directory exists, so every early
        // return leaves nothing on disk — and so the orphan sweep (which
        // checks the registry at removal time) can never classify this
        // upload's directory as an orphan. Expiry, the cap check, and the
        // insert happen under ONE lock hold, so concurrent begins cannot
        // both pass the cap (monorepo#2275); expired sessions are drained
        // first so they never hold cap slots.
        let expired_dirs;
        let admitted = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            expired_dirs = drain_expired_sessions(&mut uploads);
            // A live session already holding this key in this workspace:
            // same identity ⇒ hand back its uploadId (the begin reply was
            // lost) and treat the replay as activity, so a retry just under
            // the idle TTL is not expired by its own next chunk; different
            // identity ⇒ conflict. Checked under the same lock hold as the
            // cap so two keyed begins cannot both open.
            let same_key = idempotency_key.as_ref().and_then(|key| {
                uploads
                    .iter_mut()
                    .find(|(_, s)| {
                        s.workspace_id == workspace_id && s.idempotency_key.as_ref() == Some(key)
                    })
                    .map(|(id, s)| {
                        let same_payload = s.fingerprint() == fingerprint;
                        if same_payload {
                            s.last_activity = Instant::now();
                        }
                        (id.clone(), s.staging_dir.clone(), same_payload)
                    })
            });
            let live = uploads
                .values()
                .filter(|s| s.workspace_id == workspace_id)
                .count();
            if let Some((existing_id, existing_dir, same_payload)) = same_key {
                if same_payload {
                    Ok(Some((existing_id, existing_dir)))
                } else {
                    Err(Error::InvalidParams(CONFLICTING_PAYLOAD.to_string()))
                }
            } else if live >= ATTACHMENT_UPLOAD_MAX_SESSIONS_PER_WORKSPACE {
                Err(Error::InvalidParams(format!(
                    "workspace {} already has {live} attachment uploads in progress \
                     (max {ATTACHMENT_UPLOAD_MAX_SESSIONS_PER_WORKSPACE}) — commit or \
                     abort one before beginning another",
                    workspace_id.0
                )))
            } else {
                let session = AttachmentUploadSession {
                    workspace_id,
                    file_name,
                    mime_type,
                    staging_dir: staging_dir.clone(),
                    declared_size: size_bytes,
                    declared_sha256: sha,
                    chunk_sizes: HashMap::new(),
                    committing: false,
                    last_activity: Instant::now(),
                    idempotency_key,
                };
                uploads.insert(upload_id.clone(), session);
                Ok(None)
            }
        };
        // Expired staging is reclaimed even when this begin was rejected at
        // the cap — the drain already dropped the sessions. Best-effort.
        for dir in expired_dirs {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }
        if let Some((existing_id, existing_dir)) = admitted? {
            // The original begin registers its session before creating the
            // staging dir, so a replay racing it could hand out an uploadId
            // whose directory does not exist yet and fail the caller's first
            // chunk. `create_dir_all` is idempotent and the session is live
            // (the orphan sweep skips it), so creating it here is safe.
            if let Err(e) = tokio::fs::create_dir_all(&existing_dir).await {
                return Err(Error::Internal(format!(
                    "create attachment upload staging dir failed: {e}"
                )));
            }
            return Ok(serde_json::json!({
                "uploadId": existing_id,
                "maxChunkBytes": ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES,
                "replayed": true,
            }));
        }

        // Lazy sweep: staging dirs with no live session are orphans (a
        // daemon restart drops the in-memory registry). Best-effort — a
        // failed sweep never fails begin.
        self.sweep_orphaned_upload_staging_dirs().await;

        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            self.attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned")
                .remove(&upload_id);
            return Err(Error::Internal(format!(
                "create attachment upload staging dir failed: {e}"
            )));
        }

        Ok(serde_json::json!({
            "uploadId": upload_id,
            "maxChunkBytes": ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES,
        }))
    }

    /// Delete `.attachment-upload-staging/<id>` directories whose id has no
    /// live session (orphans from a daemon restart mid-upload). Best-effort.
    /// Liveness is checked against the registry immediately before each
    /// removal — not against a snapshot taken before the directory listing —
    /// so a `begin` that registers concurrently with a sweep in flight can
    /// never have its staging directory removed.
    async fn sweep_orphaned_upload_staging_dirs(&self) {
        let root = self.attachment_upload_staging_root();
        let Ok(mut entries) = tokio::fs::read_dir(&root).await else {
            // no staging root yet — nothing to sweep
            return;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let live = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned")
                .contains_key(&name);
            if live {
                continue;
            }
            tracing::info!(staging = %name, "sweeping orphaned attachment upload staging dir");
            if let Err(e) = tokio::fs::remove_dir_all(entry.path()).await {
                tracing::warn!(staging = %name, error = %e, "orphan upload staging sweep failed");
            }
        }
    }

    /// If `upload_id` names an idle-expired session, drop it, delete its
    /// staging dir, and return the clear "expired" caller error the late
    /// call surfaces (monorepo#2275). A live, unknown, or committing
    /// session returns `Ok(())` — the caller's own lookup handles those.
    async fn reclaim_upload_if_expired(&self, upload_id: &str) -> Result<()> {
        let staging_dir = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            let ttl = attachment_upload_idle_ttl();
            match uploads.get(upload_id) {
                Some(session) if session.expired(ttl) => {
                    tracing::info!(upload = %upload_id, "expiring idle attachment upload session");
                    uploads.remove(upload_id).map(|s| s.staging_dir)
                }
                _ => return Ok(()),
            }
        };
        if let Some(dir) = staging_dir {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }
        Err(Error::InvalidParams(format!(
            "attachment upload {upload_id} expired after {}s of inactivity — begin a new upload",
            attachment_upload_idle_ttl().as_secs()
        )))
    }

    /// `file.attachmentUpload.chunk`: stage one seq-numbered slice of the
    /// payload. `data` is base64; the decoded slice is written to its own
    /// `chunk-<seq>` file, so retrying a seq is idempotent (same bytes land
    /// in the same file) and chunks may arrive in any order. Rejects decoded
    /// slices over [`ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES`], totals beyond
    /// the declared size, and idle-expired sessions (the expired session is
    /// reclaimed on the spot — monorepo#2275). Returns
    /// `{ uploadId, seq, receivedBytes }`.
    pub(crate) async fn file_attachment_upload_chunk_op(
        &self,
        upload_id: String,
        seq: u64,
        data: String,
    ) -> Result<serde_json::Value> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data.trim())
            .map_err(|e| Error::InvalidParams(format!("chunk data is not valid base64: {e}")))?;
        if bytes.is_empty() {
            return Err(Error::InvalidParams("chunk data is empty".to_string()));
        }
        if bytes.len() > ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES {
            return Err(Error::InvalidParams(format!(
                "chunk of {} bytes exceeds the {} byte cap",
                bytes.len(),
                ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES
            )));
        }
        // An idle-expired session is reclaimed on the spot and this call
        // gets the clear caller error (monorepo#2275).
        self.reclaim_upload_if_expired(&upload_id).await?;
        // Reserve this seq's bytes under ONE lock hold: the size check and
        // the `chunk_sizes` update are atomic, so concurrent chunks cannot
        // both pass the check and push the total past the declared size.
        let (staging_dir, prior_this_seq, received) = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            let session = uploads.get_mut(&upload_id).ok_or_else(|| {
                Error::NotFound(format!("no attachment upload in progress: {upload_id}"))
            })?;
            if session.committing {
                return Err(Error::InvalidParams(format!(
                    "upload {upload_id} is committing — chunks are no longer accepted"
                )));
            }
            session.last_activity = Instant::now();
            // A retried seq replaces its previous bytes; only NEW bytes
            // count against the declared total.
            let prior_this_seq = session.chunk_sizes.get(&seq).copied();
            let new_total =
                session.received_bytes() - prior_this_seq.unwrap_or(0) + bytes.len() as u64;
            if new_total > session.declared_size {
                return Err(Error::InvalidParams(format!(
                    "received {new_total} bytes exceed the declared attachment size {}",
                    session.declared_size
                )));
            }
            session.chunk_sizes.insert(seq, bytes.len() as u64);
            (session.staging_dir.clone(), prior_this_seq, new_total)
        };
        let path = staging_dir.join(chunk_file_name(seq));
        if let Err(e) = tokio::fs::write(&path, &bytes).await {
            // Roll the reservation back so a retry accounts correctly.
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            if let Some(session) = uploads.get_mut(&upload_id) {
                match prior_this_seq {
                    Some(prior) => session.chunk_sizes.insert(seq, prior),
                    None => session.chunk_sizes.remove(&seq),
                };
            }
            return Err(Error::Internal(format!(
                "write attachment upload chunk failed: {e}"
            )));
        }
        Ok(serde_json::json!({
            "uploadId": upload_id,
            "seq": seq,
            "receivedBytes": received,
        }))
    }

    /// `file.attachmentUpload.abort`: drop the staging session and delete
    /// its directory. Idempotent — aborting an unknown id succeeds quietly
    /// (the client may retry an abort after a timeout). Rejected while a
    /// commit of the same upload is in flight (abort would race the
    /// commit's cleanup).
    pub(crate) async fn file_attachment_upload_abort_op(
        &self,
        upload_id: String,
    ) -> Result<serde_json::Value> {
        let session = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            if uploads.get(&upload_id).is_some_and(|s| s.committing) {
                return Err(Error::InvalidParams(format!(
                    "upload {upload_id} is committing — wait for the commit to settle"
                )));
            }
            uploads.remove(&upload_id)
        };
        let aborted = session.is_some();
        if let Some(session) = session {
            let _ = tokio::fs::remove_dir_all(&session.staging_dir).await;
        }
        Ok(serde_json::json!({ "uploadId": upload_id, "aborted": aborted }))
    }

    /// `file.attachmentUpload.commit`: reassemble the staged chunks, verify
    /// the payload SHA-256 against the declared checksum, and place the file
    /// through the same path `file.placeAttachment` uses (collision-safe
    /// naming + attachment-registry row), so the result is
    /// byte-shape-identical to a successful `placeAttachment`. The staging
    /// session survives a failed commit (the client can retry or abort); it
    /// is removed only after the placement succeeds. While a commit runs,
    /// its session is flagged `committing`, so a concurrent
    /// commit/chunk/abort of the same upload is rejected instead of
    /// mutating the files being hashed.
    pub(crate) async fn file_attachment_upload_commit_op(
        &self,
        upload_id: String,
    ) -> Result<serde_json::Value> {
        // An idle-expired session is reclaimed on the spot and this call
        // gets the clear caller error (monorepo#2275).
        self.reclaim_upload_if_expired(&upload_id).await?;
        // Phase 1 — validate and CLAIM the `committing` flag under one lock
        // hold. Errors here (unknown id, already committing, incomplete,
        // gaps) never touch a flag another commit owns.
        let claim = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            let session = uploads.get_mut(&upload_id).ok_or_else(|| {
                Error::NotFound(format!("no attachment upload in progress: {upload_id}"))
            })?;
            if session.committing {
                return Err(Error::InvalidParams(format!(
                    "upload {upload_id} is already committing"
                )));
            }
            session.last_activity = Instant::now();
            let received = session.received_bytes();
            if received != session.declared_size {
                return Err(Error::InvalidParams(format!(
                    "attachment incomplete: received {received} of {} declared bytes",
                    session.declared_size
                )));
            }
            let mut seqs: Vec<u64> = session.chunk_sizes.keys().copied().collect();
            seqs.sort_unstable();
            if seqs.first() != Some(&0) || seqs.last() != Some(&(seqs.len() as u64 - 1)) {
                return Err(Error::InvalidParams(format!(
                    "chunk sequence has gaps: got seqs {seqs:?}, expected contiguous from 0"
                )));
            }
            session.committing = true;
            (
                session.staging_dir.clone(),
                session.declared_size,
                session.declared_sha256.clone(),
                session.workspace_id.clone(),
                session.file_name.clone(),
                session.mime_type.clone(),
                seqs,
                session.idempotency_key.clone(),
            )
        };

        // Phase 2 — the fallible work. On failure, clear the flag THIS call
        // set (phase 1 claimed it exclusively), so the session survives for
        // retry or abort without racing a concurrent commit's flag.
        let result = self
            .file_attachment_upload_commit_body(&upload_id, claim)
            .await;
        if result.is_err() {
            self.release_failed_commit_claim(&upload_id);
        }
        result
    }

    /// Release a failed commit's `committing` claim so the session survives
    /// for retry or abort. Also refreshes the idle clock: the commit itself
    /// may have outlived the TTL (claim time is the last refresh before the
    /// body runs), and without this the next call — or a `begin` sweep —
    /// would expire the session instantly instead of granting the
    /// documented fresh retry window.
    fn release_failed_commit_claim(&self, upload_id: &str) {
        if let Some(session) = self
            .attachment_uploads
            .lock()
            .expect("attachment upload registry poisoned")
            .get_mut(upload_id)
        {
            session.committing = false;
            session.last_activity = Instant::now();
        }
    }

    async fn file_attachment_upload_commit_body(
        &self,
        upload_id: &str,
        claim: CommitClaim,
    ) -> Result<serde_json::Value> {
        let (
            staging_dir,
            declared_size,
            declared_sha,
            workspace_id,
            file_name,
            mime_type,
            seqs,
            idempotency_key,
        ) = claim;

        // Reassemble + hash on the blocking pool (sync I/O), landing the
        // assembled payload next to the chunks so the final placement copies
        // from a file instead of buffering ~1 GiB in memory.
        let assembled = staging_dir.join("assembled");
        {
            let staging_dir = staging_dir.clone();
            let assembled = assembled.clone();
            let declared_sha = declared_sha.clone();
            tokio::task::spawn_blocking(move || {
                assemble_and_verify(
                    &staging_dir,
                    &seqs,
                    declared_size,
                    &declared_sha,
                    &assembled,
                )
            })
            .await
            .map_err(|e| Error::Internal(format!("attachment assembly task failed: {e}")))??;
        }

        // Delegate to the placeAttachment path (same-host copy arm): the
        // collision-safe placement, registry insert, and result shape are
        // shared, so the commit result is byte-shape-identical to a
        // successful `file.placeAttachment` (PROTOCOL §5.9). A failure here
        // leaves the session alive for retry or abort. The session's
        // idempotency key (if any) binds here with the chunked-arm
        // fingerprint — the verified declared size + SHA-256.
        let idempotency = idempotency_key.map(|key| KeyedPlacement {
            key,
            lookup_fingerprint: Some(attachment_fingerprint(
                &file_name,
                declared_size,
                Some(&declared_sha),
            )),
            sha256: Some(declared_sha.clone()),
        });
        let result = self
            .place_attachment_registered(
                workspace_id.clone(),
                &file_name,
                &crate::file_ops::AttachmentSource::CopyFrom(&assembled),
                mime_type,
                idempotency,
            )
            .await
            .map_err(|e| {
                tracing::warn!(
                    workspace = %workspace_id.as_str(),
                    file_name = %file_name,
                    upload = %upload_id,
                    error = %e,
                    "file.attachmentUpload.commit placement failed"
                );
                e
            })?;

        // Session retired; staging deleted.
        self.attachment_uploads
            .lock()
            .expect("attachment upload registry poisoned")
            .remove(upload_id);
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;

        Ok(result)
    }
}

/// Concatenate the staged chunks into `assembled`, verifying the total size
/// and SHA-256 along the way. Runs on the blocking pool.
fn assemble_and_verify(
    staging_dir: &std::path::Path,
    chunk_seqs: &[u64],
    declared_size: u64,
    declared_sha256: &str,
    assembled: &std::path::Path,
) -> Result<()> {
    use std::io::Write as _;

    let mut hasher = sha2::Sha256::new();
    let mut out = std::fs::File::create(assembled)
        .map_err(|e| Error::Internal(format!("create assembled attachment failed: {e}")))?;
    let mut total = 0u64;
    for seq in chunk_seqs {
        let bytes = std::fs::read(staging_dir.join(chunk_file_name(*seq))).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                // A reserved-but-unwritten chunk: the commit was pipelined
                // behind a chunk call whose reservation landed but whose
                // disk write had not yet — a caller-side race, not a daemon
                // fault (monorepo#2275). The session survives; retry once
                // the chunk call has returned.
                Error::InvalidParams(format!(
                    "chunk {seq} is still being written — wait for the chunk call to return, \
                     then retry the commit"
                ))
            } else {
                Error::Internal(format!("read staged chunk {seq} failed: {e}"))
            }
        })?;
        hasher.update(&bytes);
        total += bytes.len() as u64;
        out.write_all(&bytes)
            .map_err(|e| Error::Internal(format!("assemble attachment failed: {e}")))?;
    }
    out.flush()
        .map_err(|e| Error::Internal(format!("assemble attachment flush failed: {e}")))?;
    drop(out);
    if total != declared_size {
        // Phase 1 already proved the RESERVATIONS sum to declared_size, so a
        // short assembly means a chunk file was read mid-write — the
        // partial-write guise of the same pipelined race as the NotFound arm
        // above (monorepo#2275). Same remedy: the session survives; retry
        // once the chunk call has returned.
        return Err(Error::InvalidParams(format!(
            "assembled attachment is {total} bytes, expected {declared_size} — a chunk may \
             still be being written; wait for the chunk call to return, then retry the commit"
        )));
    }
    let actual: String = hasher.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    });
    if actual != declared_sha256 {
        return Err(Error::InvalidParams(format!(
            "attachment checksum mismatch: expected sha256 {declared_sha256}, got {actual}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};

    use base64::Engine as _;
    use intent_core::{Error, WorkspaceApi as _, WorkspaceId};
    use intent_store::Store;
    use sha2::Digest as _;

    use crate::Services;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(prefix: &str) -> Self {
            let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).expect("mkdir");
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        sha2::Sha256::digest(bytes)
            .iter()
            .fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            })
    }

    /// One in-process service stack with a seeded workspace whose checkout
    /// root is a real temp dir (so `commit`'s placement path resolves).
    async fn seeded_services(ws: &WorkspaceId, ws_root: &Path, checkout: &Path) -> Services {
        let db = std::env::temp_dir().join(format!("attach-up-test-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&db).await.expect("open store");
        let mut row = crate::tests::workspace(ws);
        row.worktree_path = Some(checkout.to_string_lossy().into_owned());
        store.insert_workspace(&row).await.expect("seed workspace");
        Services::new(store).with_workspaces_root(ws_root.to_path_buf())
    }

    async fn begin(svc: &Services, ws: &WorkspaceId, payload: &[u8]) -> String {
        let r = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "report.bin".to_string(),
                payload.len() as u64,
                sha256_hex(payload),
                None,
                None,
            )
            .await
            .expect("begin");
        assert_eq!(
            r["maxChunkBytes"].as_u64(),
            Some(super::ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES as u64)
        );
        r["uploadId"].as_str().expect("uploadId").to_string()
    }

    /// Happy path: multi-chunk out-of-order staging with a seq retry, then
    /// a commit whose result is byte-shape-identical to placeAttachment
    /// (registry fields included) and whose staging state is cleaned up.
    #[tokio::test]
    async fn upload_multi_chunk_out_of_order_with_retry_commits() {
        let ws = WorkspaceId("ws-up-happy".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload: Vec<u8> = (0u32..200_000).flat_map(u32::to_le_bytes).collect();
        let mid = payload.len() / 2;
        let upload_id = begin(&svc, &ws, &payload).await;

        // Chunk 1 first (out of order), then a garbage chunk 0, then the
        // idempotent retry of seq 0 with the real bytes.
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(&payload[mid..]))
            .await
            .expect("chunk 1");
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&vec![0u8; mid]))
            .await
            .expect("chunk 0 (garbage)");
        let r = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload[..mid]))
            .await
            .expect("chunk 0 retry");
        // The retry replaced seq 0 — the running total never double-counts.
        assert_eq!(r["receivedBytes"].as_u64(), Some(payload.len() as u64));

        let result = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect("commit");
        assert_eq!(result["ok"], serde_json::json!(true));
        assert_eq!(result["fileName"], serde_json::json!("report.bin"));
        assert_eq!(
            result["path"],
            serde_json::json!(".intent/attachments/report.bin")
        );
        assert_eq!(result["size"].as_u64(), Some(payload.len() as u64));
        assert!(result["attachmentId"].is_string(), "registry id: {result}");
        assert!(result["uploadedAt"].is_string(), "uploadedAt: {result}");

        let on_disk = std::fs::read(checkout.0.join(".intent/attachments/report.bin")).unwrap();
        assert_eq!(on_disk, payload);
        // Session retired + staging removed; a second commit is unknown-id.
        assert!(!ws_root
            .0
            .join(".attachment-upload-staging")
            .join(&upload_id)
            .exists());
        let err = svc
            .file_attachment_upload_commit_op(upload_id)
            .await
            .expect_err("second commit");
        assert!(matches!(err, Error::NotFound(_)), "got {err}");
    }

    /// Keyed chunked uploads (intent-hq/intent#4691): a same-key `begin`
    /// with the same `(fileName, sizeBytes, sha256)` while the session is
    /// live replays its `uploadId` (+ `replayed: true`) instead of opening a
    /// second session; a different identity under the key is the conflict
    /// error; after `commit` the key is bound to the attachment, so a
    /// re-`begin` is rejected shape-stably ("already committed; look it
    /// up"), `getAttachmentInfo` resolves the key, and a keyed
    /// `file.placeAttachment` of the same bytes replays the committed
    /// result (the base64 and chunked arms share one fingerprint).
    #[tokio::test]
    async fn keyed_begin_replays_live_session_and_commit_binds_key() {
        let ws = WorkspaceId("ws-up-keyed".to_string());
        let other_ws = WorkspaceId("ws-up-keyed-other".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let other_checkout = TempDir::new("attach-up-co-other");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;
        let mut other_row = crate::tests::workspace(&other_ws);
        other_row.worktree_path = Some(other_checkout.0.to_string_lossy().into_owned());
        svc.store()
            .insert_workspace(&other_row)
            .await
            .expect("seed other ws");

        let payload = b"keyed chunked payload".to_vec();
        let sha = sha256_hex(&payload);
        let key = Some("key-chunked".to_string());
        let first = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "keyed.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                Some("application/octet-stream".to_string()),
                key.clone(),
            )
            .await
            .expect("keyed begin");
        assert!(first.get("replayed").is_none(), "{first}");
        let upload_id = first["uploadId"].as_str().unwrap().to_string();

        // Lost begin reply: same identity → the same session, marked replayed.
        let again = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "keyed.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect("keyed begin replay");
        assert_eq!(again["uploadId"], serde_json::json!(upload_id));
        assert_eq!(again["maxChunkBytes"], first["maxChunkBytes"]);
        assert_eq!(again["replayed"], serde_json::json!(true));
        assert_eq!(
            svc.attachment_uploads.lock().unwrap().len(),
            1,
            "no second session"
        );

        // Same key, different identity while live → conflict.
        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "keyed.bin".to_string(),
                payload.len() as u64 + 1,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect_err("live conflict");
        assert!(err.to_string().contains("different payload"), "got {err}");

        // Another workspace may use the same key freely.
        let other = svc
            .file_attachment_upload_begin_op(
                other_ws.clone(),
                "keyed.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect("other ws keyed begin");
        assert!(other.get("replayed").is_none(), "{other}");
        assert_ne!(other["uploadId"], first["uploadId"]);

        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");
        let committed = svc
            .file_attachment_upload_commit_op(upload_id)
            .await
            .expect("commit");
        assert!(committed.get("replayed").is_none(), "{committed}");
        assert_eq!(committed["fileName"], serde_json::json!("keyed.bin"));

        // Committed key: begin is rejected shape-stably and points at lookup.
        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "keyed.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect_err("begin after commit");
        assert!(matches!(err, Error::InvalidParams(_)), "got {err}");
        assert!(err.to_string().contains("already committed"), "got {err}");
        // A different identity under the committed key is the conflict.
        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "other-name.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect_err("begin after commit, other identity");
        assert!(err.to_string().contains("different payload"), "got {err}");

        // Lookup by key resolves the committed attachment.
        let info = svc
            .file_get_attachment_info_by_key(ws.clone(), "key-chunked".to_string())
            .await
            .expect("lookup by key");
        assert_eq!(info["attachmentId"], committed["attachmentId"]);
        assert_eq!(
            info["mimeType"],
            serde_json::json!("application/octet-stream")
        );
        assert_eq!(info["exists"], serde_json::json!(true));

        // The base64 arm shares the fingerprint: same bytes under the same
        // key replay the committed placement instead of placing a copy.
        let replayed = svc
            .file_place_attachment(
                ws.clone(),
                "keyed.bin".to_string(),
                Some(b64(&payload)),
                None,
                None,
                key.clone(),
            )
            .await
            .expect("keyed place replays commit");
        let mut expected = committed.clone();
        expected["replayed"] = serde_json::json!(true);
        assert_eq!(replayed, expected);
        assert!(!checkout.0.join(".intent/attachments/keyed-2.bin").exists());

        // The other workspace's live session is untouched by the commit.
        assert_eq!(svc.attachment_uploads.lock().unwrap().len(), 1);
    }

    /// Cross-surface replay on `commit` (PROTOCOL §5.9, #1841 review): a
    /// keyed session whose key a same-key single-shot placement bound first
    /// (matching fingerprint) commits as a replay of that placement —
    /// nothing placed, the session retired, `replayed: true` on the result.
    /// A session whose key the single-shot bound with a DIFFERENT payload
    /// commits into the conflict error and stays alive for abort.
    #[tokio::test]
    async fn keyed_commit_replays_single_shot_bound_first_and_conflicts_otherwise() {
        let ws = WorkspaceId("ws-up-keyed-xsurface".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"cross-surface".to_vec();
        let sha = sha256_hex(&payload);
        let key = Some("key-x".to_string());
        let begun = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "x.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect("keyed begin");
        let upload_id = begun["uploadId"].as_str().unwrap().to_string();
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");

        // The single-shot arm binds the key first with the same identity.
        let placed = svc
            .file_place_attachment(
                ws.clone(),
                "x.bin".to_string(),
                Some(b64(&payload)),
                None,
                None,
                key.clone(),
            )
            .await
            .expect("single-shot place");
        assert!(placed.get("replayed").is_none(), "{placed}");

        let committed = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect("commit replays");
        let mut expected = placed.clone();
        expected["replayed"] = serde_json::json!(true);
        assert_eq!(committed, expected);
        assert!(!checkout.0.join(".intent/attachments/x-2.bin").exists());
        assert!(svc.attachment_uploads.lock().unwrap().is_empty());
        let err = svc
            .file_attachment_upload_commit_op(upload_id)
            .await
            .expect_err("session retired");
        assert!(
            err.to_string().contains("no attachment upload"),
            "got {err}"
        );

        // Conflict guise: the session's identity differs from the binding.
        let other = b"cross-surface!".to_vec();
        let other_sha = sha256_hex(&other);
        let begun = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "y.bin".to_string(),
                other.len() as u64,
                other_sha.clone(),
                None,
                Some("key-y".to_string()),
            )
            .await
            .expect("keyed begin y");
        let upload_id = begun["uploadId"].as_str().unwrap().to_string();
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&other))
            .await
            .expect("chunk y");
        svc.file_place_attachment(
            ws.clone(),
            "y.bin".to_string(),
            Some(b64(&payload)),
            None,
            None,
            Some("key-y".to_string()),
        )
        .await
        .expect("single-shot place y with other bytes");
        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("conflict");
        assert!(matches!(err, Error::InvalidParams(_)), "got {err}");
        assert!(err.to_string().contains("different payload"), "got {err}");
        assert_eq!(svc.attachment_uploads.lock().unwrap().len(), 1);
        let aborted = svc
            .file_attachment_upload_abort_op(upload_id)
            .await
            .expect("abort");
        assert_eq!(aborted["aborted"], serde_json::json!(true));
    }

    /// Regression (#1841 review): a keyed `sourcePath` placement binds the
    /// key to the PLACED size, not the pre-copy stat. `/proc` files stat as
    /// 0 bytes yet copy non-empty, so the two deterministically disagree;
    /// the bound fingerprint must carry the copied length and a re-place of
    /// the same (now stable) file must match it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn keyed_source_path_binds_placed_size_not_stat_size() {
        let ws = WorkspaceId("ws-up-keyed-drift".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let src = "/proc/self/status";
        assert_eq!(std::fs::metadata(src).unwrap().len(), 0, "precondition");
        let placed = svc
            .file_place_attachment(
                ws.clone(),
                "status.txt".to_string(),
                None,
                Some(src.to_string()),
                None,
                Some("key-drift".to_string()),
            )
            .await
            .expect("keyed sourcePath place");
        let placed_size = placed["size"].as_u64().unwrap();
        assert!(placed_size > 0, "{placed}");

        let (binding, _) = svc
            .store()
            .get_attachment_by_idempotency_key(
                &ws,
                "key-drift",
                &super::idempotency_retention_cutoff(),
            )
            .await
            .expect("lookup")
            .expect("bound");
        assert_eq!(
            binding.fingerprint,
            super::attachment_fingerprint("status.txt", placed_size, None),
            "bound fingerprint must use the placed size"
        );
        assert_ne!(
            binding.fingerprint,
            super::attachment_fingerprint("status.txt", 0, None)
        );
    }

    /// Regression (#1841 review): an expired binding still present at bind
    /// time — the sweep missed it (failed, or the binding crossed the
    /// retention boundary between the sweep and the lookup) — must not turn
    /// the fresh placement into a spurious "already bound". This drives the
    /// post-lookup path directly with the binding back-dated past the
    /// cutoff and no sweep: the file is placed, the key is rebound to the
    /// new row, and the original row and file are untouched.
    #[tokio::test]
    async fn keyed_placement_replaces_expired_binding_the_sweep_missed() {
        let ws = WorkspaceId("ws-up-keyed-expired".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"boundary".to_vec();
        let sha = sha256_hex(&payload);
        let first = svc
            .file_place_attachment(
                ws.clone(),
                "b.bin".to_string(),
                Some(b64(&payload)),
                None,
                None,
                Some("key-boundary".to_string()),
            )
            .await
            .expect("first keyed place");
        let first_id = first["attachmentId"].as_str().unwrap().to_string();
        // Back-date the binding past retention; do NOT sweep.
        sqlx::query("UPDATE attachment_idempotency_keys SET created_at = ? WHERE key = ?")
            .bind(intent_core::iso_minutes_ago(8 * 24 * 60))
            .bind("key-boundary")
            .execute(svc.store().write_pool())
            .await
            .expect("back-date binding");
        let cutoff = super::idempotency_retention_cutoff();
        assert!(svc
            .store()
            .get_attachment_by_idempotency_key(&ws, "key-boundary", &cutoff)
            .await
            .expect("lookup")
            .is_none());

        let fresh = svc
            .place_attachment_registered_inner(
                ws.clone(),
                "b.bin",
                &crate::file_ops::AttachmentSource::Bytes(&payload),
                None,
                Some(("key-boundary", Some(sha.as_str()), cutoff.as_str())),
            )
            .await
            .expect("fresh placement rebinding the expired key");
        assert!(fresh.get("replayed").is_none(), "{fresh}");
        assert_ne!(fresh["attachmentId"], serde_json::json!(first_id));
        assert_eq!(fresh["fileName"], serde_json::json!("b-2.bin"));
        assert!(checkout.0.join(".intent/attachments/b.bin").is_file());
        assert!(checkout.0.join(".intent/attachments/b-2.bin").is_file());

        let (binding, row) = svc
            .store()
            .get_attachment_by_idempotency_key(&ws, "key-boundary", &cutoff)
            .await
            .expect("lookup rebound")
            .expect("rebound");
        assert_eq!(serde_json::json!(row.id), fresh["attachmentId"]);
        assert_eq!(
            binding.fingerprint,
            super::attachment_fingerprint("b.bin", payload.len() as u64, Some(&sha))
        );
        let original = svc
            .file_get_attachment_info(first_id)
            .await
            .expect("original row survives");
        assert_eq!(original["fileName"], serde_json::json!("b.bin"));
    }

    /// Regression (#1841 review): a keyed `begin` replay is activity. A
    /// session idle just under the TTL that is replayed must not be swept
    /// by the caller's very next op — the replay refreshes `last_activity`.
    #[tokio::test]
    async fn keyed_begin_replay_refreshes_last_activity() {
        let _env = crate::agent_manager::tests::EnvGuard::set_all(&[(
            "INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS",
            "100",
        )]);
        let ws = WorkspaceId("ws-up-keyed-refresh".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"refresh-me".to_vec();
        let sha = sha256_hex(&payload);
        let key = Some("key-refresh".to_string());
        let first = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "r.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect("keyed begin");
        let upload_id = first["uploadId"].as_str().unwrap().to_string();
        // Backdate to just inside the TTL: still live for the replay's
        // sweep, but expired by the next op unless the replay refreshes.
        {
            let mut uploads = svc.attachment_uploads.lock().unwrap();
            let session = uploads.get_mut(&upload_id).unwrap();
            session.last_activity = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_millis(80))
                .expect("backdate");
        }
        let again = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "r.bin".to_string(),
                payload.len() as u64,
                sha,
                None,
                key,
            )
            .await
            .expect("keyed begin replay");
        assert_eq!(again["uploadId"], serde_json::json!(upload_id));
        assert_eq!(again["replayed"], serde_json::json!(true));
        let idle = svc
            .attachment_uploads
            .lock()
            .unwrap()
            .get(&upload_id)
            .expect("session still live")
            .last_activity
            .elapsed();
        assert!(
            idle < std::time::Duration::from_millis(50),
            "not refreshed: {idle:?}"
        );

        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk after replay");
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("commit after replay");
    }

    /// Regression (#1841 review): a keyed `begin` replay racing the original
    /// begin (session registered, staging dir not yet created) must hand
    /// back an uploadId whose staging dir exists, so the first chunk lands.
    #[tokio::test]
    async fn keyed_begin_replay_creates_missing_staging_dir() {
        let ws = WorkspaceId("ws-up-keyed-dir".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"dir-race".to_vec();
        let sha = sha256_hex(&payload);
        let key = Some("key-dir".to_string());
        let first = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "d.bin".to_string(),
                payload.len() as u64,
                sha.clone(),
                None,
                key.clone(),
            )
            .await
            .expect("keyed begin");
        let upload_id = first["uploadId"].as_str().unwrap().to_string();
        let staging = svc
            .attachment_uploads
            .lock()
            .unwrap()
            .get(&upload_id)
            .unwrap()
            .staging_dir
            .clone();
        // Simulate the window before the original begin created its dir.
        std::fs::remove_dir_all(&staging).expect("remove staging dir");
        assert!(!staging.exists());

        let again = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "d.bin".to_string(),
                payload.len() as u64,
                sha,
                None,
                key,
            )
            .await
            .expect("keyed begin replay");
        assert_eq!(again["uploadId"], serde_json::json!(upload_id));
        assert!(staging.is_dir(), "replay must recreate the staging dir");
        svc.file_attachment_upload_chunk_op(upload_id, 0, b64(&payload))
            .await
            .expect("chunk lands after replay");
    }

    /// `begin` rejections: unknown workspace, empty name, zero size,
    /// over-cap size, malformed sha — each naming the specifics.
    #[tokio::test]
    async fn begin_rejections() {
        let ws = WorkspaceId("ws-up-reject".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;
        let sha = "a".repeat(64);

        let err = svc
            .file_attachment_upload_begin_op(
                WorkspaceId("ws-nope".to_string()),
                "f.bin".to_string(),
                10,
                sha.clone(),
                None,
                None,
            )
            .await
            .expect_err("unknown ws");
        assert!(err.to_string().contains("ws-nope"), "got {err}");

        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "  ".to_string(),
                10,
                sha.clone(),
                None,
                None,
            )
            .await
            .expect_err("empty name");
        assert!(err.to_string().contains("fileName"), "got {err}");

        // Names that placement's sanitization would reject fail at begin —
        // before any bytes are staged — not at commit.
        for doomed in ["/", "..", "dir/", "a/.."] {
            let err = svc
                .file_attachment_upload_begin_op(
                    ws.clone(),
                    doomed.to_string(),
                    10,
                    sha.clone(),
                    None,
                    None,
                )
                .await
                .expect_err("doomed name");
            assert!(
                err.to_string().contains("invalid attachment fileName"),
                "{doomed:?} got {err}"
            );
        }
        // A path-bearing name whose basename is usable still passes begin
        // (placement keeps only the basename). Abort it so the trailing
        // no-staging-left-behind assertion stays meaningful.
        let r = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "dir/nested.bin".to_string(),
                10,
                sha.clone(),
                None,
                None,
            )
            .await
            .expect("basename-usable name");
        svc.file_attachment_upload_abort_op(r["uploadId"].as_str().unwrap().to_string())
            .await
            .expect("abort");

        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "f.bin".to_string(),
                0,
                sha.clone(),
                None,
                None,
            )
            .await
            .expect_err("zero size");
        assert!(err.to_string().contains("positive"), "got {err}");

        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "f.bin".to_string(),
                super::ATTACHMENT_UPLOAD_MAX_TOTAL_BYTES + 1,
                sha,
                None,
                None,
            )
            .await
            .expect_err("oversize");
        assert!(err.to_string().contains("cap"), "got {err}");

        let err = svc
            .file_attachment_upload_begin_op(
                ws,
                "f.bin".to_string(),
                10,
                "nothex".to_string(),
                None,
                None,
            )
            .await
            .expect_err("sha");
        assert!(err.to_string().contains("64 hex"), "got {err}");
        // No early-return left a staging dir behind (the root itself may
        // exist from the successful basename-usable begin above).
        let leftovers: Vec<_> = std::fs::read_dir(ws_root.0.join(".attachment-upload-staging"))
            .map(|d| d.map(|e| e.unwrap().path()).collect())
            .unwrap_or_default();
        assert!(leftovers.is_empty(), "got {leftovers:?}");
    }

    /// `chunk` rejections: unknown uploadId, bad base64, empty data, and
    /// staging more bytes than declared (both single-chunk and cumulative).
    #[tokio::test]
    async fn chunk_rejections_and_over_staging() {
        let ws = WorkspaceId("ws-up-chunk".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let err = svc
            .file_attachment_upload_chunk_op("upload-nope".to_string(), 0, b64(b"x"))
            .await
            .expect_err("unknown id");
        assert!(matches!(err, Error::NotFound(_)), "got {err}");
        assert!(err.to_string().contains("upload-nope"), "got {err}");

        let payload = b"0123456789".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;

        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 0, "!!!not-base64".to_string())
            .await
            .expect_err("bad base64");
        assert!(err.to_string().contains("base64"), "got {err}");

        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 0, String::new())
            .await
            .expect_err("empty");
        assert!(err.to_string().contains("empty"), "got {err}");

        // Cumulative over-staging: 6 + 6 > 10 declared bytes.
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload[..6]))
            .await
            .expect("chunk 0");
        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(b"abcdef"))
            .await
            .expect_err("over-staging");
        assert!(err.to_string().contains("declared"), "got {err}");
        // The rejected chunk reserved nothing: finishing correctly works.
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(&payload[6..]))
            .await
            .expect("chunk 1");
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("commit");
    }

    /// A decoded chunk over the 16 MiB per-chunk cap is rejected before any
    /// reservation or write, and the session survives for correctly sized
    /// chunks.
    #[tokio::test]
    async fn chunk_over_per_chunk_cap_rejected() {
        let ws = WorkspaceId("ws-up-cap".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let oversized = vec![0u8; super::ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES + 1];
        let r = svc
            .file_attachment_upload_begin_op(
                ws,
                "big.bin".to_string(),
                oversized.len() as u64,
                sha256_hex(&oversized),
                None,
                None,
            )
            .await
            .expect("begin");
        let upload_id = r["uploadId"].as_str().unwrap().to_string();

        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&oversized))
            .await
            .expect_err("over cap");
        assert!(err.to_string().contains("byte cap"), "got {err}");
        // Nothing was reserved and no chunk file landed.
        {
            let uploads = svc.attachment_uploads.lock().unwrap();
            assert_eq!(uploads.get(&upload_id).unwrap().received_bytes(), 0);
        }
        // The session still accepts correctly sized chunks.
        svc.file_attachment_upload_chunk_op(
            upload_id,
            0,
            b64(&oversized[..super::ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES]),
        )
        .await
        .expect("in-cap chunk");
    }

    /// `commit` rejects incomplete staging, seq gaps, and a checksum
    /// mismatch — and the session survives each failed commit (retry after
    /// the fix succeeds; abort works too).
    #[tokio::test]
    async fn commit_incomplete_gap_and_sha_mismatch() {
        let ws = WorkspaceId("ws-up-commit".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        // Incomplete: only half the declared bytes staged.
        let payload = b"half-and-half".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload[..6]))
            .await
            .expect("chunk");
        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("incomplete");
        assert!(err.to_string().contains("incomplete"), "got {err}");
        // Session survives — finish staging, commit succeeds.
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(&payload[6..]))
            .await
            .expect("finish");
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("retry commit");

        // Seq gap: bytes complete but staged at seqs {0, 2}.
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload[..6]))
            .await
            .expect("chunk 0");
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 2, b64(&payload[6..]))
            .await
            .expect("chunk 2");
        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("gap");
        assert!(err.to_string().contains("gaps"), "got {err}");
        svc.file_attachment_upload_abort_op(upload_id)
            .await
            .expect("abort gapped");

        // Checksum mismatch: right size, wrong bytes.
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&vec![7u8; payload.len()]))
            .await
            .expect("wrong bytes");
        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("sha mismatch");
        assert!(err.to_string().contains("checksum mismatch"), "got {err}");
        // Nothing new landed (only the earlier successful commit's file plus
        // the ignore-all marker) and the session survives for abort.
        let placed: Vec<String> = std::fs::read_dir(checkout.0.join(".intent/attachments"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n != ".gitignore")
            .collect();
        assert_eq!(placed, vec!["report.bin".to_string()], "got {placed:?}");
        let r = svc
            .file_attachment_upload_abort_op(upload_id.clone())
            .await
            .expect("abort");
        assert_eq!(r["aborted"], serde_json::json!(true));
        // Abort is idempotent: the second call succeeds with aborted: false.
        let r = svc
            .file_attachment_upload_abort_op(upload_id.clone())
            .await
            .expect("abort again");
        assert_eq!(r["aborted"], serde_json::json!(false));
        assert!(!ws_root
            .0
            .join(".attachment-upload-staging")
            .join(&upload_id)
            .exists());
    }

    /// A restart drops sessions (fresh `Services` knows no uploadId) and
    /// the next `begin` sweeps the orphaned staging dir.
    #[tokio::test]
    async fn restart_drops_session_and_begin_sweeps_orphans() {
        let ws = WorkspaceId("ws-up-restart".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"restart-me".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");
        let orphan_dir = ws_root
            .0
            .join(".attachment-upload-staging")
            .join(&upload_id);
        assert!(orphan_dir.exists());

        // "Restart": a fresh Services stack over the same roots.
        let svc2 = seeded_services(
            &WorkspaceId("ws-up-restart-2".to_string()),
            &ws_root.0,
            &checkout.0,
        )
        .await;
        let err = svc2
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("dropped session");
        assert!(matches!(err, Error::NotFound(_)), "got {err}");

        // The next begin sweeps the orphan.
        let _new_id = begin(&svc2, &WorkspaceId("ws-up-restart-2".to_string()), &payload).await;
        assert!(!orphan_dir.exists(), "orphan staging dir swept");
    }

    /// Regression: the sweep checks session liveness against the registry at
    /// removal time, so a session registered while a sweep is in flight (its
    /// id absent from any pre-listing snapshot) keeps its staging dir; only
    /// truly session-less dirs are removed.
    #[tokio::test]
    async fn sweep_spares_live_sessions_registered_after_listing() {
        let ws = WorkspaceId("ws-up-sweep".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let staging_root = ws_root.0.join(".attachment-upload-staging");
        let live_dir = staging_root.join("upload-live");
        let orphan_dir = staging_root.join("upload-orphan");
        std::fs::create_dir_all(&live_dir).unwrap();
        std::fs::create_dir_all(&orphan_dir).unwrap();
        // Register `upload-live` directly (as a begin racing the sweep
        // would), so it is live in the registry but absent from any snapshot
        // taken before its directory existed.
        svc.attachment_uploads.lock().unwrap().insert(
            "upload-live".to_string(),
            super::AttachmentUploadSession {
                workspace_id: ws.clone(),
                file_name: "live.bin".to_string(),
                mime_type: None,
                staging_dir: live_dir.clone(),
                declared_size: 1,
                declared_sha256: "a".repeat(64),
                chunk_sizes: std::collections::HashMap::new(),
                committing: false,
                last_activity: std::time::Instant::now(),
                idempotency_key: None,
            },
        );

        svc.sweep_orphaned_upload_staging_dirs().await;
        assert!(live_dir.exists(), "live session dir must survive the sweep");
        assert!(!orphan_dir.exists(), "orphan dir must be swept");
    }

    /// Regression (monorepo#2275): the 5th concurrent `begin` for one
    /// workspace is rejected with a caller error naming the cap, sessions in
    /// OTHER workspaces don't count against it, and settling a session
    /// (abort) frees the slot.
    #[tokio::test]
    async fn begin_rejects_fifth_concurrent_session_per_workspace() {
        // The TTL env seam is read process-wide, so pin a huge TTL here:
        // this test's four deliberately idle sessions must never be drained
        // by a tiny TTL leaking from a concurrently running expiry test —
        // holding the guard also serializes us against them via ENV_LOCK.
        let _env = crate::agent_manager::tests::EnvGuard::set_all(&[(
            "INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS",
            "3600000",
        )]);
        let ws = WorkspaceId("ws-up-cap4".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"cap-me".to_vec();
        let mut ids = Vec::new();
        for _ in 0..4 {
            ids.push(begin(&svc, &ws, &payload).await);
        }
        let err = svc
            .file_attachment_upload_begin_op(
                ws.clone(),
                "fifth.bin".to_string(),
                payload.len() as u64,
                sha256_hex(&payload),
                None,
                None,
            )
            .await
            .expect_err("fifth begin must hit the cap");
        assert!(matches!(err, Error::InvalidParams(_)), "got {err:?}");
        assert!(err.to_string().contains('4'), "cap named: {err}");

        // Another workspace is unaffected by this one's full slots.
        let ws2 = WorkspaceId("ws-up-cap4-other".to_string());
        let mut row2 = crate::tests::workspace(&ws2);
        row2.worktree_path = Some(checkout.0.to_string_lossy().into_owned());
        svc.store
            .insert_workspace(&row2)
            .await
            .expect("seed second workspace");
        begin(&svc, &ws2, &payload).await;

        // Settling one session frees the slot.
        svc.file_attachment_upload_abort_op(ids.pop().unwrap())
            .await
            .expect("abort");
        begin(&svc, &ws, &payload).await;
    }

    /// Regression (monorepo#2275): a session idle past the TTL is dropped —
    /// its staging dir swept — and subsequent chunk/commit calls get a clear
    /// caller error instead of operating on reclaimed state. The TTL is
    /// pinned tiny via the `INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS` seam.
    #[tokio::test]
    async fn idle_session_expires_and_subsequent_ops_fail_cleanly() {
        let _env = crate::agent_manager::tests::EnvGuard::set_all(&[(
            "INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS",
            "50",
        )]);
        let ws = WorkspaceId("ws-up-ttl".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"expire-me".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");
        let staging = ws_root
            .0
            .join(".attachment-upload-staging")
            .join(&upload_id);
        assert!(staging.exists());

        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        // The expired session fails cleanly and its staging dir is swept.
        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(b"x"))
            .await
            .expect_err("chunk after expiry");
        assert!(
            matches!(err, Error::InvalidParams(_)) && err.to_string().contains("expired"),
            "got {err:?}"
        );
        assert!(!staging.exists(), "expired staging dir must be swept");
        // The session is gone now — later calls get the unknown-id caller
        // error (the expiry already reclaimed it).
        let err = svc
            .file_attachment_upload_commit_op(upload_id)
            .await
            .expect_err("commit after expiry");
        assert!(
            matches!(err, Error::NotFound(_)) || err.to_string().contains("expired"),
            "got {err:?}"
        );
    }

    /// Regression (monorepo#2275): expired sessions don't hold cap slots or
    /// staging dirs — the next `begin` reclaims them — while a session kept
    /// live by ongoing chunk activity is never reclaimed.
    #[tokio::test]
    async fn begin_reclaims_expired_sessions_and_spares_active_ones() {
        let _env = crate::agent_manager::tests::EnvGuard::set_all(&[(
            "INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS",
            "1000",
        )]);
        let ws = WorkspaceId("ws-up-reclaim".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        // Fill all 4 slots, then let them all go idle past the TTL.
        let payload = b"reclaim-me".to_vec();
        let mut stale_ids = Vec::new();
        for _ in 0..4 {
            stale_ids.push(begin(&svc, &ws, &payload).await);
        }
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // A 5th begin succeeds — the expired sessions no longer count — and
        // their staging dirs are swept.
        let live_id = begin(&svc, &ws, &payload).await;
        let staging_root = ws_root.0.join(".attachment-upload-staging");
        for stale in &stale_ids {
            assert!(
                !staging_root.join(stale).exists(),
                "expired staging dir must be swept: {stale}"
            );
        }

        // Ongoing chunk activity keeps a session alive well past one TTL of
        // wall time (5 x 250ms > 1000ms), with a 4x cadence-to-TTL margin so
        // a stalled CI scheduler doesn't expire the "live" session.
        let mid = payload.len() / 2;
        for _ in 0..5 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            svc.file_attachment_upload_chunk_op(live_id.clone(), 0, b64(&payload[..mid]))
                .await
                .expect("keep-alive chunk");
        }
        svc.file_attachment_upload_chunk_op(live_id.clone(), 1, b64(&payload[mid..]))
            .await
            .expect("final chunk");
        svc.file_attachment_upload_commit_op(live_id)
            .await
            .expect("active session commits after > TTL wall time");
    }

    /// Regression (monorepo#2275): a pipelined commit racing an in-flight
    /// chunk write (seq reserved in `chunk_sizes`, file not yet on disk) is
    /// an `InvalidParams` caller error advising a retry — not `Internal` —
    /// and the retried commit succeeds once the chunk write lands.
    #[tokio::test]
    async fn commit_racing_unwritten_chunk_is_caller_error_and_retryable() {
        let ws = WorkspaceId("ws-up-race".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"pipelined-final-chunk!".to_vec();
        let mid = payload.len() / 2;
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload[..mid]))
            .await
            .expect("chunk 0");
        // Simulate the final chunk mid-write: its bytes are reserved under
        // the registry lock (as `chunk` does before its disk write) but the
        // chunk file has not landed yet.
        svc.attachment_uploads
            .lock()
            .unwrap()
            .get_mut(&upload_id)
            .unwrap()
            .chunk_sizes
            .insert(1, (payload.len() - mid) as u64);

        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("commit racing the unwritten chunk");
        assert!(
            matches!(err, Error::InvalidParams(_)),
            "must be a caller error, got {err:?}"
        );
        assert!(err.to_string().contains("retry"), "advises retry: {err}");

        // Same race, second guise: the chunk file exists but was read
        // mid-write (tokio::fs::write is not atomic), so assembly comes up
        // short. Also a caller error advising a retry — not a message that
        // pushes the client to abort a recoverable upload.
        let staging = ws_root
            .0
            .join(".attachment-upload-staging")
            .join(&upload_id);
        std::fs::write(staging.join(super::chunk_file_name(1)), &payload[mid..=mid]).unwrap();
        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("commit racing the partially written chunk");
        assert!(
            matches!(err, Error::InvalidParams(_)),
            "must be a caller error, got {err:?}"
        );
        assert!(err.to_string().contains("retry"), "advises retry: {err}");

        // The in-flight write lands; the retried commit succeeds.
        std::fs::write(staging.join(super::chunk_file_name(1)), &payload[mid..]).unwrap();
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("retried commit");
    }

    /// Regression (monorepo#2275 review): a commit that fails after running
    /// LONGER than the idle TTL must still leave the session a fresh retry
    /// window. The failure path refreshes `last_activity` when it releases
    /// the `committing` claim — without that, the claim-time timestamp is
    /// already past the TTL and the next op (or a `begin` sweep) expires
    /// the session instead of allowing the documented retry.
    #[tokio::test]
    async fn failed_commit_outliving_ttl_still_gets_fresh_retry_window() {
        let _env = crate::agent_manager::tests::EnvGuard::set_all(&[(
            "INTENTD_ATTACHMENT_UPLOAD_IDLE_TTL_MS",
            "100",
        )]);
        let ws = WorkspaceId("ws-up-slow-commit".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"slow-commit".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");
        // Simulate a commit claimed well over one TTL ago and still in
        // flight: `committing` held, claim-time `last_activity` long stale.
        {
            let mut uploads = svc.attachment_uploads.lock().unwrap();
            let session = uploads.get_mut(&upload_id).unwrap();
            session.committing = true;
            session.last_activity = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(2))
                .expect("backdate claim time");
        }
        // The slow commit fails; its failure path releases the claim.
        svc.release_failed_commit_claim(&upload_id);
        // The session must NOT be instantly expired: the retry window
        // restarts at the failure, so an immediate retry succeeds.
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("retry inside the refreshed window");
    }

    /// Regression: a commit rejected with "already committing" must NOT
    /// clear the `committing` flag owned by the in-flight commit — chunks
    /// and aborts stay rejected while the first commit runs.
    #[tokio::test]
    async fn rejected_concurrent_commit_leaves_committing_flag_intact() {
        let ws = WorkspaceId("ws-up-flag".to_string());
        let ws_root = TempDir::new("attach-up-root");
        let checkout = TempDir::new("attach-up-co");
        let svc = seeded_services(&ws, &ws_root.0, &checkout.0).await;

        let payload = b"flag-owner".to_vec();
        let upload_id = begin(&svc, &ws, &payload).await;
        svc.file_attachment_upload_chunk_op(upload_id.clone(), 0, b64(&payload))
            .await
            .expect("chunk");
        // Simulate an in-flight first commit holding the claim.
        svc.attachment_uploads
            .lock()
            .unwrap()
            .get_mut(&upload_id)
            .unwrap()
            .committing = true;

        let err = svc
            .file_attachment_upload_commit_op(upload_id.clone())
            .await
            .expect_err("second commit rejected");
        assert!(err.to_string().contains("already committing"), "got {err}");
        // The rejection did not release the first commit's claim: the flag
        // is still set, and chunk/abort remain rejected.
        assert!(
            svc.attachment_uploads
                .lock()
                .unwrap()
                .get(&upload_id)
                .unwrap()
                .committing,
            "committing flag must still be held"
        );
        let err = svc
            .file_attachment_upload_chunk_op(upload_id.clone(), 1, b64(b"x"))
            .await
            .expect_err("chunk during commit");
        assert!(err.to_string().contains("committing"), "got {err}");
        let err = svc
            .file_attachment_upload_abort_op(upload_id.clone())
            .await
            .expect_err("abort during commit");
        assert!(err.to_string().contains("committing"), "got {err}");

        // Release the claim (as the first commit's failure path would) and
        // the retry path works end to end.
        svc.attachment_uploads
            .lock()
            .unwrap()
            .get_mut(&upload_id)
            .unwrap()
            .committing = false;
        svc.file_attachment_upload_commit_op(upload_id)
            .await
            .expect("commit after release");
    }
}
