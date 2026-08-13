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
//! `commit` succeeds.

use std::collections::HashMap;
use std::path::PathBuf;

use base64::Engine as _;
use intent_core::{Error, Result, WorkspaceApi as _, WorkspaceId};
use sha2::Digest as _;

use crate::Services;

/// Maximum DECODED bytes per `file.attachmentUpload.chunk` call. Base64
/// inflates this by 4/3 on the wire (~21.4 MiB), keeping the full JSON-RPC
/// frame comfortably under the 40 MiB inbound cap (PROTOCOL §1.3). Same
/// value as the import surface's `IMPORT_MAX_CHUNK_BYTES`.
pub(crate) const ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Maximum declared attachment size accepted by `begin` (decoded bytes).
pub(crate) const ATTACHMENT_UPLOAD_MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

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
}

impl AttachmentUploadSession {
    fn received_bytes(&self) -> u64 {
        self.chunk_sizes.values().sum()
    }
}

/// Chunk file name for `seq` inside the staging dir (zero-padded so a
/// directory listing sorts in seq order for humans; commit reads by index).
fn chunk_file_name(seq: u64) -> String {
    format!("chunk-{seq:08}")
}

impl Services {
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
    /// specifics, per monorepo#2144): an unknown workspace, an empty file
    /// name, a zero or over-cap declared size, and a malformed sha. Returns
    /// `{ uploadId, maxChunkBytes }`.
    pub(crate) async fn file_attachment_upload_begin_op(
        &self,
        workspace_id: WorkspaceId,
        file_name: String,
        size_bytes: u64,
        sha256: String,
        mime_type: Option<String>,
    ) -> Result<serde_json::Value> {
        if file_name.trim().is_empty() {
            return Err(Error::InvalidParams(
                "fileName must not be empty".to_string(),
            ));
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

        let upload_id = format!("upload-{}", uuid::Uuid::new_v4());
        let staging_dir = self.attachment_upload_staging_root().join(&upload_id);

        // Register the session BEFORE any directory exists, so every early
        // return leaves nothing on disk. `live_ids` snapshots the registry
        // for the orphan sweep below.
        let live_ids: Vec<String> = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            let session = AttachmentUploadSession {
                workspace_id,
                file_name,
                mime_type,
                staging_dir: staging_dir.clone(),
                declared_size: size_bytes,
                declared_sha256: sha,
                chunk_sizes: HashMap::new(),
                committing: false,
            };
            uploads.insert(upload_id.clone(), session);
            uploads.keys().cloned().collect()
        };

        // Lazy sweep: staging dirs with no live session are orphans (a
        // daemon restart drops the in-memory registry). Best-effort — a
        // failed sweep never fails begin.
        self.sweep_orphaned_upload_staging_dirs(&live_ids).await;

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
    async fn sweep_orphaned_upload_staging_dirs(&self, live_ids: &[String]) {
        let root = self.attachment_upload_staging_root();
        let mut entries = match tokio::fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(_) => return, // no staging root yet — nothing to sweep
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if live_ids.contains(&name) {
                continue;
            }
            tracing::info!(staging = %name, "sweeping orphaned attachment upload staging dir");
            if let Err(e) = tokio::fs::remove_dir_all(entry.path()).await {
                tracing::warn!(staging = %name, error = %e, "orphan upload staging sweep failed");
            }
        }
    }

    /// `file.attachmentUpload.chunk`: stage one seq-numbered slice of the
    /// payload. `data` is base64; the decoded slice is written to its own
    /// `chunk-<seq>` file, so retrying a seq is idempotent (same bytes land
    /// in the same file) and chunks may arrive in any order. Rejects decoded
    /// slices over [`ATTACHMENT_UPLOAD_MAX_CHUNK_BYTES`] and totals beyond
    /// the declared size. Returns `{ uploadId, seq, receivedBytes }`.
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
        let result = self.file_attachment_upload_commit_inner(&upload_id).await;
        if result.is_err() {
            // The session survives a failed commit — clear the in-flight
            // flag so a retry (or abort) is accepted.
            if let Some(session) = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned")
                .get_mut(&upload_id)
            {
                session.committing = false;
            }
        }
        result
    }

    async fn file_attachment_upload_commit_inner(
        &self,
        upload_id: &str,
    ) -> Result<serde_json::Value> {
        let (staging_dir, declared_size, declared_sha, workspace_id, file_name, mime_type, seqs) = {
            let mut uploads = self
                .attachment_uploads
                .lock()
                .expect("attachment upload registry poisoned");
            let session = uploads.get_mut(upload_id).ok_or_else(|| {
                Error::NotFound(format!("no attachment upload in progress: {upload_id}"))
            })?;
            if session.committing {
                return Err(Error::InvalidParams(format!(
                    "upload {upload_id} is already committing"
                )));
            }
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
            )
        };

        // Reassemble + hash on the blocking pool (sync I/O), landing the
        // assembled payload next to the chunks so the final placement copies
        // from a file instead of buffering ~1 GiB in memory.
        let assembled = staging_dir.join("assembled");
        {
            let staging_dir = staging_dir.clone();
            let assembled = assembled.clone();
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
        // leaves the session alive for retry or abort.
        let result = self
            .file_place_attachment(
                workspace_id.clone(),
                file_name.clone(),
                None,
                Some(assembled.to_string_lossy().into_owned()),
                mime_type,
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
        let bytes = std::fs::read(staging_dir.join(chunk_file_name(*seq)))
            .map_err(|e| Error::Internal(format!("read staged chunk {seq} failed: {e}")))?;
        hasher.update(&bytes);
        total += bytes.len() as u64;
        out.write_all(&bytes)
            .map_err(|e| Error::Internal(format!("assemble attachment failed: {e}")))?;
    }
    out.flush()
        .map_err(|e| Error::Internal(format!("assemble attachment flush failed: {e}")))?;
    drop(out);
    if total != declared_size {
        return Err(Error::InvalidParams(format!(
            "assembled attachment is {total} bytes, expected {declared_size}"
        )));
    }
    let actual = format!("{:x}", hasher.finalize());
    if actual != declared_sha256 {
        return Err(Error::InvalidParams(format!(
            "attachment checksum mismatch: expected sha256 {declared_sha256}, got {actual}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use base64::Engine as _;
    use intent_core::{Error, WorkspaceId};
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
        format!("{:x}", sha2::Sha256::digest(bytes))
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

        let payload: Vec<u8> = (0u32..200_000).flat_map(|i| i.to_le_bytes()).collect();
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
            )
            .await
            .expect_err("unknown ws");
        assert!(err.to_string().contains("ws-nope"), "got {err}");

        let err = svc
            .file_attachment_upload_begin_op(ws.clone(), "  ".to_string(), 10, sha.clone(), None)
            .await
            .expect_err("empty name");
        assert!(err.to_string().contains("fileName"), "got {err}");

        let err = svc
            .file_attachment_upload_begin_op(ws.clone(), "f.bin".to_string(), 0, sha.clone(), None)
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
            )
            .await
            .expect_err("sha");
        assert!(err.to_string().contains("64 hex"), "got {err}");
        // No early-return left a staging dir behind.
        assert!(!ws_root.0.join(".attachment-upload-staging").exists());
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
}
