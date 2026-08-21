//! Source-side export pipeline (`workspace.export.*`, PROTOCOL §5.1): the
//! source half of the FE-mediated transfer relay. `start` stops the
//! workspace's agents (durable queues preserved; in-flight agents captured as
//! pending `interrupted_agent` rows, which ride the archive so the target —
//! and the source, if the export is aborted — can offer resumption), then
//! builds the transfer zip archive in a staging dir on a background task,
//! emitting `workspace:transfer:progress` per stage and `:ready` (with the
//! manifest + checksum + chunk count the FE hands to `workspace.import.begin`)
//! or `:failed` (staging cleaned, WIP snapshots unwound, workspace intact).
//! `read` serves the sealed archive in seq-numbered chunks, idempotently.
//! `finalize` settles the source after a successful relay: optional final
//! status message + archive applied, then WIP snapshots unwound and staging
//! deleted. `abort` cleans up and leaves the workspace usable (agents stay
//! stopped).
//!
//! The workspace is expected to stay quiet between `start` and
//! `finalize`/`abort`: nothing enforces it, and an agent respawned after
//! `:ready` works on top of the WIP snapshot sentinel commit — the settle
//! unwind (correctly) refuses to pop a sentinel that is no longer HEAD, so
//! any such commits leave the sentinel permanently in branch history.
//!
//! Archive layout (agreed with [`crate::transfer_import`]): `manifest.json`
//! (the [`TransferManifest`]), `rows/<table>.jsonl`
//! ([`intent_store::TRANSFER_TABLES`] vocabulary), `assets/<assetId>`,
//! `attachments/<attachmentId>` (the registered attachment files that still
//! exist in the canonical `.intent/attachments/` store — a registry row whose
//! file was deleted rides the rows payload with NO file entry), and — when
//! the workspace has a repository — `git/repo.bundle` + `git/refs.json` (the
//! [`TransferRefsManifest`]).

use std::fmt::Write as _;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use intent_core::events::{
    WORKSPACE_TRANSFER_FAILED, WORKSPACE_TRANSFER_PROGRESS, WORKSPACE_TRANSFER_READY,
};
use intent_core::transfer::TransferManifest;
use intent_core::{
    clock::now_iso, AgentId, AgentStatus, Error, Result, Workspace, WorkspaceApi, WorkspaceId,
    WorkspaceUpdate,
};
use intent_store::NewEvent;
use sha2::Digest as _;

use crate::transfer_git::{create_transfer_bundle, unwind_wip};
use crate::{git_ops, publish_event, system_actor, Services};

/// Maximum bytes per `workspace.export.read` chunk BEFORE base64 encoding.
/// Matches [`crate::transfer_import::IMPORT_MAX_CHUNK_BYTES`] so the FE can
/// pipe downloaded chunks straight into `workspace.import.chunk`; the ~21.4
/// MiB encoded frame stays comfortably under the 40 MiB cap (PROTOCOL §1.3).
pub(crate) const EXPORT_MAX_CHUNK_BYTES: usize = crate::transfer_import::IMPORT_MAX_CHUNK_BYTES;

/// The sealed archive of a [`ExportState::Ready`] session.
pub(crate) struct ReadyArchive {
    pub archive_path: PathBuf,
    pub size_bytes: u64,
    pub sha256: String,
    pub manifest: TransferManifest,
}

/// Lifecycle of one export session. There is no `Failed` state: a failed
/// build cleans up and removes the session (the outcome travels on the
/// `workspace:transfer:failed` event), so a retry is a fresh `start`.
pub(crate) enum ExportState {
    /// The background build task is running. `aborted` is set by
    /// `workspace.export.abort`; the build observes it between stages and
    /// cleans up instead of sealing the archive.
    Building {
        aborted: bool,
    },
    Ready(Box<ReadyArchive>),
}

/// One in-flight export: everything `read`/`finalize`/`abort` need between
/// calls. Lives in `Services::transfer_exports`; in-memory only — a daemon
/// restart drops sessions and the boot sweep clears their staging dirs.
pub(crate) struct ExportSession {
    pub workspace_id: WorkspaceId,
    /// `<workspaces_root>/.export-staging/<exportId>/`.
    pub staging_dir: PathBuf,
    pub state: ExportState,
    /// Repos left holding a transfer WIP snapshot commit by the bundle
    /// builder (worktree and/or sandboxes); unwound when the export settles
    /// (finalize, abort, or a failed build after bundling succeeded).
    pub wip_paths: Vec<PathBuf>,
    /// Per-chunk byte budget served by `read`. Always
    /// [`EXPORT_MAX_CHUNK_BYTES`] in production; tests shrink it to exercise
    /// multi-chunk reads with tiny archives.
    pub max_chunk_bytes: usize,
}

impl ExportSession {
    fn total_chunks(&self, size_bytes: u64) -> u64 {
        size_bytes.div_ceil(self.max_chunk_bytes as u64).max(1)
    }
}

impl Services {
    /// Root directory staged exports live under (sibling of the import
    /// staging root, same filesystem reasoning).
    fn export_staging_root(&self) -> PathBuf {
        self.workspaces_root
            .clone()
            .unwrap_or_else(crate::default_workspaces_root)
            .join(".export-staging")
    }

    /// `workspace.export.start`: validate, register the session, and kick
    /// off the background archive build. Returns `{ exportId, maxChunkBytes }`
    /// immediately; progress/outcome travel on `workspace:transfer:*` events.
    /// Rejects the chief workspace, unknown ids, and a second concurrent
    /// export of the same workspace.
    pub(crate) async fn workspace_export_start_op(
        &self,
        id: WorkspaceId,
    ) -> Result<serde_json::Value> {
        if id.is_chief() {
            return Err(Error::InvalidParams(
                "The chief workspace cannot be exported".to_string(),
            ));
        }
        let ws = self.store.get_workspace(&id).await?;
        let export_id = format!("export-{}", uuid::Uuid::new_v4());
        let staging_dir = self.export_staging_root().join(&export_id);
        {
            let mut exports = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned");
            if let Some((other, _)) = exports.iter().find(|(_, s)| s.workspace_id == id) {
                return Err(Error::InvalidParams(format!(
                    "an export of workspace {} is already in progress ({other})",
                    id.0
                )));
            }
            exports.insert(
                export_id.clone(),
                ExportSession {
                    workspace_id: id.clone(),
                    staging_dir: staging_dir.clone(),
                    state: ExportState::Building { aborted: false },
                    wip_paths: Vec::new(),
                    max_chunk_bytes: EXPORT_MAX_CHUNK_BYTES,
                },
            );
        }
        // Lazy sweep: export staging dirs with no live session are orphans
        // from a prior daemon run. Best-effort; never fails start.
        self.sweep_stale_export_staging_dirs().await;
        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            self.transfer_exports
                .lock()
                .expect("transfer export registry poisoned")
                .remove(&export_id);
            return Err(Error::Internal(format!(
                "create export staging dir failed: {e}"
            )));
        }
        let svc = self.clone();
        let export_id_for_task = export_id.clone();
        tokio::spawn(async move {
            svc.run_export_build(export_id_for_task, ws).await;
        });
        Ok(serde_json::json!({
            "exportId": export_id,
            "maxChunkBytes": EXPORT_MAX_CHUNK_BYTES,
        }))
    }

    /// Background build wrapper: runs the staged build, then settles the
    /// session — seal + `workspace:transfer:ready` on success, cleanup +
    /// `workspace:transfer:failed` on error, quiet cleanup when the build
    /// observed an abort (`Ok(None)`).
    async fn run_export_build(&self, export_id: String, ws: Workspace) {
        let workspace_id = ws.id.clone();
        match self.build_export_archive(&export_id, ws).await {
            Ok(Some(ready)) => {
                // Seal under the lock; the session may have been aborted
                // while the last stage ran, in which case clean up instead.
                // The event payload reads the session's chunk values so
                // `:ready` and `read` share one source of truth.
                let event_data = {
                    let mut exports = self
                        .transfer_exports
                        .lock()
                        .expect("transfer export registry poisoned");
                    match exports.get_mut(&export_id) {
                        Some(session)
                            if !matches!(
                                session.state,
                                ExportState::Building { aborted: true }
                            ) =>
                        {
                            let data = serde_json::json!({
                                "workspaceId": workspace_id.as_str(),
                                "exportId": export_id,
                                "manifest": ready.manifest,
                                "archiveSizeBytes": ready.size_bytes,
                                "archiveSha256": ready.sha256,
                                "maxChunkBytes": session.max_chunk_bytes,
                                "totalChunks": session.total_chunks(ready.size_bytes),
                            });
                            session.state = ExportState::Ready(Box::new(ready));
                            Some(data)
                        }
                        _ => None,
                    }
                };
                let Some(event_data) = event_data else {
                    self.cleanup_export(&export_id).await;
                    return;
                };
                publish_event(
                    &self.event_bus,
                    transfer_event(&workspace_id, WORKSPACE_TRANSFER_READY, event_data),
                )
                .await;
            }
            Ok(None) => {
                // Abort observed between stages — cleanup, no failed event.
                self.cleanup_export(&export_id).await;
            }
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.as_str(),
                    export = %export_id,
                    error = %e,
                    "workspace export failed"
                );
                self.cleanup_export(&export_id).await;
                publish_event(
                    &self.event_bus,
                    transfer_event(
                        &workspace_id,
                        WORKSPACE_TRANSFER_FAILED,
                        serde_json::json!({
                            "workspaceId": workspace_id.as_str(),
                            "exportId": export_id,
                            "reason": e.to_string(),
                        }),
                    ),
                )
                .await;
            }
        }
    }

    /// The staged archive build: stop agents → manifest + rows → git bundle
    /// → zip. Returns `Ok(None)` when an abort was observed between stages
    /// (the caller cleans up quietly). Every stage emits a
    /// `workspace:transfer:progress` event before it runs.
    async fn build_export_archive(
        &self,
        export_id: &str,
        ws: Workspace,
    ) -> Result<Option<ReadyArchive>> {
        let id = ws.id.clone();
        let staging_dir = {
            let exports = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned");
            match exports.get(export_id) {
                Some(s) => s.staging_dir.clone(),
                None => return Ok(None),
            }
        };

        // Stage 1: capture in-flight agents as pending interrupted rows,
        // then stop every workspace agent. The interrupted rows are inserted
        // BEFORE the stop (which settles persisted statuses to idle) so they
        // ride the archive — the import side's exported-pending-rows-win
        // merge makes them the target's resumption offers, and they equally
        // cover the source if the export is aborted. Durable queues are
        // untouched (stop_many never clears `agent_queue`).
        self.emit_export_progress(&id, export_id, "stopping-agents", None)
            .await;
        let sessions = self.store.list_agent_session_summaries(&id).await?;
        let manager = self.agent_manager();
        let busy: std::collections::HashSet<AgentId> = manager
            .as_ref()
            .map(|m| {
                m.list_busy()
                    .into_iter()
                    .filter(|(_, ws_id)| *ws_id == id)
                    .map(|(agent_id, _)| agent_id)
                    .collect()
            })
            .unwrap_or_default();
        let now = now_iso();
        for session in &sessions {
            let status_in_flight = matches!(
                session.status,
                AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
            );
            if !status_in_flight && !busy.contains(&session.id) {
                continue;
            }
            // Busy membership is authoritative for the race where the
            // persisted status hasn't caught up yet (same fallback as the
            // graceful-shutdown capture).
            let prev = match serde_json::to_value(session.status) {
                Ok(serde_json::Value::String(s)) if status_in_flight => s,
                _ => "active".to_string(),
            };
            if let Err(e) = self
                .store
                .insert_interrupted_agent(&session.id, &id, &prev, &now)
                .await
            {
                tracing::warn!(agent = %session.id.0, error = %e, "export: interrupted-agent capture failed");
            }
        }
        // Hold the teardown fence across the row export below: it blocks the
        // lazy-spawn paths for the swept agents, so a queued message cannot
        // respawn one mid-export and mutate rows or the worktree/sandboxes
        // while they are being captured. Held through the git bundle (stage
        // 3) so the bundle is built against the same quiesced state as the
        // rows.
        let _teardown_fence = match manager.as_ref() {
            Some(manager) => {
                let ids: Vec<AgentId> = sessions.iter().map(|s| s.id.clone()).collect();
                Some(manager.stop_many(&ids).await)
            }
            None => None,
        };
        if self.export_aborted(export_id) {
            return Ok(None);
        }

        // Stage 2: manifest + rows. The manifest is built by the plan op
        // (same shape the FE previewed); the embedded copy must byte-match
        // the one the `:ready` event carries — both use this value.
        self.emit_export_progress(&id, export_id, "exporting-rows", None)
            .await;
        let manifest = self.workspace_transfer_plan_op(id.clone()).await?.manifest;
        let rows = self.store.transfer_export_rows(&id).await?;
        // Resolve the attachment files the manifest promises (`exists: true`)
        // to their canonical on-disk paths for the archive writer. Rows whose
        // file is already gone carry no entry — deleted-is-deleted transfers
        // as a row without a file and must never fail the export.
        let attachment_sources: Vec<(String, PathBuf)> = {
            let root = crate::file_ops::workspace_root(&ws);
            if root.is_empty() || manifest.attachments.iter().all(|a| !a.exists) {
                Vec::new()
            } else {
                let records = self.store.list_attachments(&id).await?;
                manifest
                    .attachments
                    .iter()
                    .filter(|a| a.exists)
                    .filter_map(|a| {
                        let record = records.iter().find(|r| r.id == a.id)?;
                        let path =
                            crate::file_ops::resolve_attachment_source(&root, &record.stored_path)
                                .ok()?;
                        Some((a.id.clone(), path))
                    })
                    .collect()
            }
        };
        if self.export_aborted(export_id) {
            return Ok(None);
        }
        self.check_export_failpoint("bundling-git")?;

        // Stage 3: git bundle (skipped when the workspace has no repository).
        // On success the WIP snapshot commits stay in place — they are what
        // the bundle refs point at — and their repo paths are recorded on
        // the session so finalize/abort/failure unwinds them.
        let git_payload = if manifest.git.has_repository {
            self.emit_export_progress(&id, export_id, "bundling-git", None)
                .await;
            let sandboxes = self.store.list_sandboxes(&id).await?;
            let live: Vec<intent_store::Sandbox> = sandboxes
                .into_iter()
                .filter(|s| {
                    matches!(
                        s.status,
                        intent_store::SandboxStatus::Created
                            | intent_store::SandboxStatus::Merging
                            | intent_store::SandboxStatus::MergePending
                            | intent_store::SandboxStatus::ConflictBounced
                    )
                })
                .collect();
            let ws_for_bundle = ws.clone();
            let bundle_staging = staging_dir.clone();
            let (bundle_path, refs) = tokio::task::spawn_blocking(move || {
                create_transfer_bundle(&ws_for_bundle, &live, &bundle_staging)
            })
            .await
            .map_err(|e| Error::Internal(format!("export bundle task failed: {e}")))??;
            // Record the repos holding WIP snapshots for the settle unwind.
            let mut wip_paths = Vec::new();
            if refs.workspace_wip_commit_sha.is_some() {
                if let Some(worktree) = git_ops::worktree_path(&ws) {
                    wip_paths.push(worktree);
                }
            }
            for sb in &refs.sandboxes {
                if sb.wip_commit_sha.is_some() {
                    // Sandbox paths were validated to exist by the bundler.
                    if let Some(path) = sandbox_path_for(&self.store, &id, &sb.agent_id).await {
                        wip_paths.push(path);
                    }
                }
            }
            {
                let mut exports = self
                    .transfer_exports
                    .lock()
                    .expect("transfer export registry poisoned");
                if let Some(session) = exports.get_mut(export_id) {
                    session.wip_paths = wip_paths;
                }
            }
            Some((bundle_path, refs))
        } else {
            None
        };
        drop(_teardown_fence);
        if self.export_aborted(export_id) {
            return Ok(None);
        }
        self.check_export_failpoint("writing-archive")?;

        // Stage 4: write + seal the zip archive, hashing as it goes.
        self.emit_export_progress(&id, export_id, "writing-archive", None)
            .await;
        let assets_dir = self.assets_root.clone().map(|root| root.join(&id.0));
        let manifest_for_zip = manifest.clone();
        let archive_staging = staging_dir.clone();
        let (archive_path, size_bytes, sha256) = tokio::task::spawn_blocking(move || {
            write_archive(
                &archive_staging,
                &manifest_for_zip,
                &rows,
                assets_dir.as_deref(),
                &attachment_sources,
                git_payload.as_ref().map(|(p, r)| (p.as_path(), r)),
            )
        })
        .await
        .map_err(|e| Error::Internal(format!("export archive task failed: {e}")))??;
        self.emit_export_progress(&id, export_id, "writing-archive", Some(size_bytes))
            .await;

        Ok(Some(ReadyArchive {
            archive_path,
            size_bytes,
            sha256,
            manifest,
        }))
    }

    /// `workspace.export.read`: serve one seq-numbered chunk of a sealed
    /// archive as base64. Idempotent — any seq may be re-requested in any
    /// order. Rejects unknown ids, still-building sessions, and out-of-range
    /// seqs. Returns `{ exportId, seq, totalChunks, data }`.
    pub(crate) async fn workspace_export_read_op(
        &self,
        export_id: String,
        seq: u64,
    ) -> Result<serde_json::Value> {
        let (archive_path, size_bytes, max_chunk, total_chunks) = {
            let exports = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned");
            let session = exports
                .get(&export_id)
                .ok_or_else(|| Error::NotFound(format!("no export in progress: {export_id}")))?;
            match &session.state {
                ExportState::Building { .. } => {
                    return Err(Error::InvalidParams(format!(
                        "export {export_id} is still building — wait for workspace:transfer:ready"
                    )));
                }
                ExportState::Ready(ready) => (
                    ready.archive_path.clone(),
                    ready.size_bytes,
                    session.max_chunk_bytes,
                    session.total_chunks(ready.size_bytes),
                ),
            }
        };
        if seq >= total_chunks {
            return Err(Error::InvalidParams(format!(
                "chunk seq {seq} out of range (archive has {total_chunks} chunks)"
            )));
        }
        let offset = seq * max_chunk as u64;
        let len = usize::try_from((size_bytes - offset).min(max_chunk as u64))
            .expect("value fits in usize");
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut file = std::fs::File::open(&archive_path)
                .map_err(|e| Error::Internal(format!("open export archive failed: {e}")))?;
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(|e| Error::Internal(format!("seek export archive failed: {e}")))?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)
                .map_err(|e| Error::Internal(format!("read export archive failed: {e}")))?;
            Ok(buf)
        })
        .await
        .map_err(|e| Error::Internal(format!("export read task failed: {e}")))??;
        Ok(serde_json::json!({
            "exportId": export_id,
            "seq": seq,
            "totalChunks": total_chunks,
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
        }))
    }

    /// `workspace.export.finalize`: settle the source after a successful
    /// relay — apply the optional final status message and archive the
    /// workspace when requested, then unwind the WIP snapshots and delete
    /// staging. The workspace mutations run before the session is retired so
    /// a failed mutation leaves the export intact and finalize can be
    /// retried. Only valid on a Ready session. Returns
    /// `{ exportId, finalized: true, workspace }`.
    pub(crate) async fn workspace_export_finalize_op(
        &self,
        export_id: String,
        archive_source: bool,
        final_status_message: Option<String>,
    ) -> Result<serde_json::Value> {
        let workspace_id = {
            let exports = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned");
            let session = exports
                .get(&export_id)
                .ok_or_else(|| Error::NotFound(format!("no export in progress: {export_id}")))?;
            if matches!(session.state, ExportState::Building { .. }) {
                return Err(Error::InvalidParams(format!(
                    "export {export_id} is still building — finalize applies to a completed export"
                )));
            }
            session.workspace_id.clone()
        };
        if let Some(message) = final_status_message {
            self.update_workspace(
                workspace_id.clone(),
                WorkspaceUpdate {
                    status_message: Some(message),
                    ..Default::default()
                },
            )
            .await?;
        }
        let ws = if archive_source {
            self.archive_workspace(workspace_id.clone(), None).await?
        } else {
            self.store.get_workspace(&workspace_id).await?
        };
        self.cleanup_export(&export_id).await;
        Ok(serde_json::json!({
            "exportId": export_id,
            "finalized": true,
            "workspace": ws,
        }))
    }

    /// `workspace.export.abort`: cancel an export. A Building session is
    /// flagged and the build task cleans up when it next checks; a Ready
    /// session is cleaned up inline (WIP snapshots unwound, staging
    /// deleted). Idempotent — unknown ids return `{ aborted: false }`. The
    /// workspace stays usable; agents stay stopped (the user restarts them).
    pub(crate) async fn workspace_export_abort_op(
        &self,
        export_id: String,
    ) -> Result<serde_json::Value> {
        let building = {
            let mut exports = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned");
            match exports.get_mut(&export_id) {
                None => {
                    return Ok(serde_json::json!({
                        "exportId": export_id,
                        "aborted": false,
                    }))
                }
                Some(session) => match &mut session.state {
                    ExportState::Building { aborted } => {
                        *aborted = true;
                        true
                    }
                    ExportState::Ready(_) => false,
                },
            }
        };
        if !building {
            self.cleanup_export(&export_id).await;
        }
        Ok(serde_json::json!({
            "exportId": export_id,
            "aborted": true,
        }))
    }

    /// Consult the test-only failpoint before the named build stage; a
    /// returned error fails the build through the normal
    /// `workspace:transfer:failed` path. Always `Ok(())` in production
    /// wiring (the seam is `None`).
    fn check_export_failpoint(&self, stage: &str) -> Result<()> {
        match self.export_build_failpoint.as_ref().and_then(|f| f(stage)) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// True when the session is gone or flagged aborted (build-task stage
    /// gate).
    fn export_aborted(&self, export_id: &str) -> bool {
        let exports = self
            .transfer_exports
            .lock()
            .expect("transfer export registry poisoned");
        match exports.get(export_id) {
            Some(session) => matches!(session.state, ExportState::Building { aborted: true }),
            None => true,
        }
    }

    /// Remove the session, unwind any recorded WIP snapshot commits, and
    /// delete the staging dir. Idempotent and best-effort throughout.
    async fn cleanup_export(&self, export_id: &str) {
        let session = self
            .transfer_exports
            .lock()
            .expect("transfer export registry poisoned")
            .remove(export_id);
        let Some(session) = session else { return };
        for path in &session.wip_paths {
            let repo_path = path.clone();
            let unwound = tokio::task::spawn_blocking(move || unwind_wip(&repo_path)).await;
            match unwound {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(path = %path.display(), error = %e, "export cleanup: WIP unwind failed");
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "export cleanup: WIP unwind task failed");
                }
            }
        }
        let _ = tokio::fs::remove_dir_all(&session.staging_dir).await;
    }

    /// Delete `.export-staging/<id>` dirs whose id has no live session.
    /// Called lazily from `start` and from the boot sweep (where the
    /// registry is empty, so every leftover dir is an orphan — sessions are
    /// in-memory and cannot survive a restart). The registry is re-checked
    /// per directory entry at deletion time, so a session registered while
    /// the sweep walks the root (a concurrent `start`) keeps its staging
    /// dir. Best-effort.
    pub(crate) async fn sweep_stale_export_staging_dirs(&self) {
        let root = self.export_staging_root();
        let mut entries = match tokio::fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(_) => return, // no staging root yet — nothing to sweep
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let live = self
                .transfer_exports
                .lock()
                .expect("transfer export registry poisoned")
                .contains_key(&name);
            if live {
                continue;
            }
            tracing::info!(staging = %name, "sweeping orphaned export staging dir");
            if let Err(e) = tokio::fs::remove_dir_all(entry.path()).await {
                tracing::warn!(staging = %name, error = %e, "orphan export staging sweep failed");
            }
        }
    }

    /// Boot entry point for the orphan sweep: no sessions are live after a
    /// restart, so every leftover export staging dir is removed.
    pub async fn sweep_stale_export_staging(&self) {
        self.sweep_stale_export_staging_dirs().await;
    }

    async fn emit_export_progress(
        &self,
        workspace_id: &WorkspaceId,
        export_id: &str,
        stage: &str,
        bytes_written: Option<u64>,
    ) {
        let mut data = serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "exportId": export_id,
            "stage": stage,
        });
        if let Some(bytes) = bytes_written {
            data.as_object_mut()
                .expect("progress data is an object")
                .insert("bytesWritten".to_string(), serde_json::json!(bytes));
        }
        publish_event(
            &self.event_bus,
            transfer_event(workspace_id, WORKSPACE_TRANSFER_PROGRESS, data),
        )
        .await;
    }
}

/// Resolve a sandbox's on-disk path from its row (WIP unwind bookkeeping).
async fn sandbox_path_for(
    store: &intent_store::Store,
    workspace_id: &WorkspaceId,
    agent_id: &str,
) -> Option<PathBuf> {
    store
        .list_sandboxes(workspace_id)
        .await
        .ok()?
        .into_iter()
        .find(|s| s.agent_id.0 == agent_id)
        .map(|s| PathBuf::from(s.path))
}

/// Build a `workspace:transfer:*` event (PROTOCOL §6.5): self-sufficient
/// payloads so the FE wizard renders progress with no follow-up read.
fn transfer_event(
    workspace_id: &WorkspaceId,
    event_type: &str,
    data: serde_json::Value,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// Write the transfer zip (`archive.zip` in the staging dir): manifest,
/// `rows/<table>.jsonl`, `assets/<assetId>`, `attachments/<attachmentId>`,
/// and the optional `git/repo.bundle` + `git/refs.json`. Returns the path,
/// size, and SHA-256 (hashed from the sealed file, the exact bytes `read`
/// serves). Blocking (sync file I/O + zip deflation) — callers run it via
/// `spawn_blocking`.
fn write_archive(
    staging_dir: &Path,
    manifest: &TransferManifest,
    rows: &[(String, Vec<serde_json::Value>)],
    assets_dir: Option<&Path>,
    attachments: &[(String, PathBuf)],
    git: Option<(&Path, &crate::transfer_git::TransferRefsManifest)>,
) -> Result<(PathBuf, u64, String)> {
    let archive_path = staging_dir.join("archive.zip");
    let file = std::fs::File::create(&archive_path)
        .map_err(|e| Error::Internal(format!("create export archive failed: {e}")))?;
    let mut zip = zip::ZipWriter::new(file);
    let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
    let zerr = |what: &str, e: zip::result::ZipError| {
        Error::Internal(format!("export archive: {what} failed: {e}"))
    };
    let werr = |what: &str, e: std::io::Error| {
        Error::Internal(format!("export archive: {what} failed: {e}"))
    };

    zip.start_file("manifest.json", options)
        .map_err(|e| zerr("manifest entry", e))?;
    let manifest_bytes = serde_json::to_vec(manifest)
        .map_err(|e| Error::Internal(format!("serialize manifest failed: {e}")))?;
    zip.write_all(&manifest_bytes)
        .map_err(|e| werr("manifest write", e))?;

    for (table, objects) in rows {
        if objects.is_empty() {
            continue; // stable layout: only tables with rows get a file
        }
        zip.start_file(format!("rows/{table}.jsonl"), options)
            .map_err(|e| zerr("row entry", e))?;
        for object in objects {
            let line = serde_json::to_string(object)
                .map_err(|e| Error::Internal(format!("serialize {table} row failed: {e}")))?;
            zip.write_all(line.as_bytes())
                .and_then(|()| zip.write_all(b"\n"))
                .map_err(|e| werr("row write", e))?;
        }
    }

    // Assets: the manifest names what a transfer copies; write exactly those
    // (a file deleted since the manifest was built fails the export rather
    // than silently shipping an archive that undercuts its manifest).
    if let Some(dir) = assets_dir {
        for asset in &manifest.assets {
            let bytes = std::fs::read(dir.join(&asset.id))
                .map_err(|e| Error::Internal(format!("read asset {} failed: {e}", asset.id)))?;
            zip.start_file(format!("assets/{}", asset.id), options)
                .map_err(|e| zerr("asset entry", e))?;
            zip.write_all(&bytes).map_err(|e| werr("asset write", e))?;
        }
    }

    // Attachments: unlike assets, a file that vanished between the manifest
    // build and this write is SKIPPED, not an error — deletion is a
    // first-class attachment state (the registry row still rides the rows
    // payload, and the target reports `exists: false`).
    for (attachment_id, source) in attachments {
        let mut file = match std::fs::File::open(source) {
            Ok(file) => file,
            // Only a genuinely-gone file is the deleted state; any other
            // open failure (permissions, I/O) must fail the export rather
            // than silently shipping a live registry row without its file.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    attachment = %attachment_id,
                    "export: attachment file vanished since plan — exporting row without file"
                );
                continue;
            }
            Err(e) => {
                return Err(Error::Internal(format!(
                    "read attachment {attachment_id} failed: {e}"
                )))
            }
        };
        zip.start_file(format!("attachments/{attachment_id}"), options)
            .map_err(|e| zerr("attachment entry", e))?;
        std::io::copy(&mut file, &mut zip).map_err(|e| werr("attachment write", e))?;
    }

    if let Some((bundle_path, refs)) = git {
        zip.start_file("git/repo.bundle", options)
            .map_err(|e| zerr("bundle entry", e))?;
        let mut bundle = std::fs::File::open(bundle_path)
            .map_err(|e| Error::Internal(format!("open git bundle failed: {e}")))?;
        std::io::copy(&mut bundle, &mut zip).map_err(|e| werr("bundle write", e))?;
        zip.start_file("git/refs.json", options)
            .map_err(|e| zerr("refs entry", e))?;
        let refs_bytes = serde_json::to_vec(refs)
            .map_err(|e| Error::Internal(format!("serialize refs manifest failed: {e}")))?;
        zip.write_all(&refs_bytes)
            .map_err(|e| werr("refs write", e))?;
    }

    let file = zip.finish().map_err(|e| zerr("finish", e))?;
    file.sync_all().map_err(|e| werr("sync", e))?;
    drop(file);
    // The bundle's bytes now live inside the zip; the loose copy is dead
    // weight in staging.
    if let Some((bundle_path, _)) = git {
        let _ = std::fs::remove_file(bundle_path);
    }

    // Hash the sealed file — the exact bytes `read` serves.
    let mut file = std::fs::File::open(&archive_path)
        .map_err(|e| Error::Internal(format!("reopen export archive failed: {e}")))?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut size_bytes = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| Error::Internal(format!("hash export archive failed: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size_bytes += n as u64;
    }
    let sha256: String = hasher.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    });
    Ok((archive_path, size_bytes, sha256))
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{AgentSession, WorkspaceStatus};
    use intent_store::Store;

    fn session(agent_id: &AgentId, ws: &WorkspaceId, status: AgentStatus) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "a".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status,
            is_active: matches!(status, AgentStatus::Active),
            system_prompt: None,
            messages: vec![],
            created_at: now_iso(),
            updated_at: now_iso(),
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

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

    async fn fresh_services(workspaces_root: &Path, assets_root: &Path) -> Services {
        let db = std::env::temp_dir().join(format!("export-test-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&db).await.expect("open store");
        Services::new(store)
            .with_workspaces_root(workspaces_root.to_path_buf())
            .with_assets_root(assets_root.to_path_buf())
    }

    /// Seed a repo-less workspace with a note, an in-flight agent session,
    /// a queued message, one asset file, and two attachment-registry rows —
    /// one whose stored file exists under the workspace's (non-git)
    /// worktree dir, one whose file was deleted out-of-band.
    async fn seed_workspace(svc: &Services, assets_root: &Path, ws_dir: &Path, id: &WorkspaceId) {
        let mut ws = crate::tests::workspace(id);
        std::fs::create_dir_all(ws_dir).expect("ws dir");
        ws.worktree_path = Some(ws_dir.to_string_lossy().to_string());
        svc.store.insert_workspace(&ws).await.expect("workspace");
        let att_dir = ws_dir.join(".intent/attachments");
        std::fs::create_dir_all(&att_dir).expect("attachments dir");
        std::fs::write(att_dir.join("doc.pdf"), b"attachment-bytes").expect("attachment");
        for (att_id, name) in [("att-live", "doc.pdf"), ("att-gone", "gone.txt")] {
            svc.store
                .insert_attachment(&intent_store::AttachmentRecord {
                    id: att_id.to_string(),
                    workspace_id: id.clone(),
                    file_name: name.to_string(),
                    mime_type: None,
                    size: 16,
                    uploaded_at: now_iso(),
                    stored_path: format!(".intent/attachments/{name}"),
                })
                .await
                .expect("attachment row");
        }
        let agent = AgentId("agent-exp".to_string());
        svc.store
            .insert_agent_session(&session(&agent, id, AgentStatus::Active))
            .await
            .expect("session");
        let note = intent_core::Note {
            id: intent_core::NoteId::from("n1"),
            workspace_id: id.clone(),
            title: "N".to_string(),
            content: "body".to_string(),
            content_type: intent_core::ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: intent_core::NoteVisibility::Workspace,
            metadata: intent_core::NoteMetadata::default(),
            created_at: now_iso(),
            rev: 0,
            updated_at: now_iso(),
        };
        svc.store.insert_note(&note).await.expect("note");
        let dir = assets_root.join(&id.0);
        std::fs::create_dir_all(&dir).expect("assets dir");
        std::fs::write(dir.join("img.png"), b"asset-bytes").expect("asset");
    }

    /// Wait until the background build settles the session (Ready) or the
    /// session disappears (failed build). Returns true when Ready.
    async fn wait_ready(svc: &Services, export_id: &str) -> bool {
        for _ in 0..200 {
            {
                let exports = svc.transfer_exports.lock().unwrap();
                match exports.get(export_id) {
                    Some(s) if matches!(s.state, ExportState::Ready(_)) => return true,
                    Some(_) => {}
                    None => return false,
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        false
    }

    fn ready_meta(svc: &Services, export_id: &str) -> (u64, String) {
        let exports = svc.transfer_exports.lock().unwrap();
        match &exports.get(export_id).expect("session").state {
            ExportState::Ready(r) => (r.size_bytes, r.sha256.clone()),
            _ => panic!("session not ready"),
        }
    }

    /// Happy path (repo-less workspace): start → ready; chunked reads
    /// reassemble to the exact archive (idempotent re-reads included); the
    /// archive round-trips through the import extractor's own layout
    /// expectations; the in-flight agent gained a pending interrupted row.
    #[tokio::test]
    async fn export_builds_readable_archive() {
        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let id = WorkspaceId("ws-export".to_string());
        seed_workspace(&svc, &assets_root.0, &ws_root.0.join("checkout"), &id).await;

        let started = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("start");
        let export_id = started["exportId"].as_str().expect("exportId").to_string();
        assert_eq!(
            usize::try_from(started["maxChunkBytes"].as_u64().unwrap())
                .expect("value fits in usize"),
            EXPORT_MAX_CHUNK_BYTES
        );
        assert!(wait_ready(&svc, &export_id).await, "build must succeed");

        // Reading while shrinking the chunk budget exercises multi-chunk.
        {
            let mut exports = svc.transfer_exports.lock().unwrap();
            exports.get_mut(&export_id).unwrap().max_chunk_bytes = 256;
        }
        let (size, sha) = ready_meta(&svc, &export_id);
        let first = svc
            .workspace_export_read_op(export_id.clone(), 0)
            .await
            .expect("read 0");
        let total_chunks = first["totalChunks"].as_u64().expect("totalChunks");
        assert_eq!(total_chunks, size.div_ceil(256).max(1));
        let mut archive = Vec::new();
        for seq in 0..total_chunks {
            let chunk = svc
                .workspace_export_read_op(export_id.clone(), seq)
                .await
                .expect("read");
            archive.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(chunk["data"].as_str().unwrap())
                    .expect("base64"),
            );
        }
        assert_eq!(archive.len() as u64, size);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&archive);
        let actual: String = hasher.finalize().iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        });
        assert_eq!(actual, sha);

        // Idempotent re-read: same seq, same bytes.
        let again = svc
            .workspace_export_read_op(export_id.clone(), 0)
            .await
            .expect("re-read");
        assert_eq!(again["data"], first["data"]);
        // Out-of-range seq rejected.
        assert!(svc
            .workspace_export_read_op(export_id.clone(), total_chunks)
            .await
            .is_err());

        // The archive is a valid zip with the agreed layout.
        let reader = std::io::Cursor::new(archive);
        let mut zip = zip::ZipArchive::new(reader).expect("valid zip");
        let names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        assert!(names.contains(&"manifest.json".to_string()));
        assert!(names.contains(&"rows/workspace.jsonl".to_string()));
        assert!(names.contains(&"rows/note.jsonl".to_string()));
        assert!(names.contains(&"rows/agent_session.jsonl".to_string()));
        assert!(names.contains(&"rows/interrupted_agent.jsonl".to_string()));
        assert!(names.contains(&"rows/attachments.jsonl".to_string()));
        assert!(names.contains(&"assets/img.png".to_string()));
        // The existing attachment file rides as attachments/<id>; the
        // deleted one transfers as a row only (deleted-is-deleted never
        // fails the export).
        assert!(names.contains(&"attachments/att-live".to_string()));
        assert!(!names.contains(&"attachments/att-gone".to_string()));
        assert!(!names.iter().any(|n| n.starts_with("git/")));
        let mut attachment_bytes = Vec::new();
        zip.by_name("attachments/att-live")
            .unwrap()
            .read_to_end(&mut attachment_bytes)
            .unwrap();
        assert_eq!(attachment_bytes, b"attachment-bytes");
        let mut manifest_bytes = Vec::new();
        zip.by_name("manifest.json")
            .unwrap()
            .read_to_end(&mut manifest_bytes)
            .unwrap();
        let embedded: TransferManifest =
            serde_json::from_slice(&manifest_bytes).expect("manifest parses");
        assert_eq!(embedded.workspace_id, id);
        assert!(!embedded.git.has_repository);
        assert_eq!(embedded.attachments.len(), 2);
        let att = |aid: &str| {
            embedded
                .attachments
                .iter()
                .find(|a| a.id == aid)
                .expect("attachment entry")
        };
        assert!(att("att-live").exists);
        assert_eq!(att("att-live").size_bytes, 16);
        assert!(!att("att-gone").exists);

        // The in-flight agent was captured as a pending interrupted row —
        // it rides the archive and covers the source on abort.
        let interrupted = svc
            .store
            .list_interrupted_agents()
            .await
            .expect("interrupted");
        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].agent_id.0, "agent-exp");
        assert_eq!(interrupted[0].prev_status, "active");

        // Finalize with a status message + archive: source settles, session
        // retired, staging gone.
        let staging = svc.export_staging_root().join(&export_id);
        assert!(staging.exists());
        let finalized = svc
            .workspace_export_finalize_op(
                export_id.clone(),
                true,
                Some("Transferred elsewhere".to_string()),
            )
            .await
            .expect("finalize");
        assert_eq!(finalized["finalized"], true);
        let ws = svc.store.get_workspace(&id).await.expect("workspace");
        assert_eq!(ws.status, WorkspaceStatus::Archived);
        assert_eq!(ws.status_message.as_deref(), Some("Transferred elsewhere"));
        assert!(!staging.exists());
        assert!(svc.transfer_exports.lock().unwrap().is_empty());
        // Read after finalize: session gone.
        assert!(matches!(
            svc.workspace_export_read_op(export_id, 0).await,
            Err(Error::NotFound(_))
        ));
    }

    /// Guards: chief and unknown workspaces are rejected; a second start
    /// while one export is live is rejected; reading a still-building
    /// session is rejected; abort is idempotent and cleans staging.
    #[tokio::test]
    async fn export_guards_and_abort() {
        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let id = WorkspaceId("ws-guards".to_string());
        seed_workspace(&svc, &assets_root.0, &ws_root.0.join("checkout"), &id).await;

        assert!(svc
            .workspace_export_start_op(WorkspaceId::chief())
            .await
            .is_err());
        assert!(matches!(
            svc.workspace_export_start_op(WorkspaceId("ws-nope".to_string()))
                .await,
            Err(Error::NotFound(_))
        ));

        let started = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("start");
        let export_id = started["exportId"].as_str().unwrap().to_string();
        // Second concurrent export of the same workspace is rejected
        // (regardless of build state).
        let err = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect_err("dup");
        assert!(err.to_string().contains("already in progress"));

        assert!(wait_ready(&svc, &export_id).await);
        let staging = svc.export_staging_root().join(&export_id);
        assert!(staging.exists());
        let aborted = svc
            .workspace_export_abort_op(export_id.clone())
            .await
            .expect("abort");
        assert_eq!(aborted["aborted"], true);
        assert!(!staging.exists());
        // Idempotent: a second abort reports nothing to do.
        let again = svc
            .workspace_export_abort_op(export_id.clone())
            .await
            .expect("abort again");
        assert_eq!(again["aborted"], false);
        // The workspace is intact and a fresh export can start.
        assert!(svc.store.get_workspace(&id).await.is_ok());
        svc.workspace_export_start_op(id).await.expect("restart");
    }

    /// Finalizing a still-building session is rejected; finalize without
    /// archiveSource leaves the workspace active.
    #[tokio::test]
    async fn export_finalize_without_archive() {
        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let id = WorkspaceId("ws-fin".to_string());
        seed_workspace(&svc, &assets_root.0, &ws_root.0.join("checkout"), &id).await;

        let started = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("start");
        let export_id = started["exportId"].as_str().unwrap().to_string();
        // Finalizing a Building session is rejected: flip the settled
        // session back to Building deterministically, assert, restore.
        assert!(wait_ready(&svc, &export_id).await);
        let prior = {
            let mut exports = svc.transfer_exports.lock().unwrap();
            let session = exports.get_mut(&export_id).unwrap();
            std::mem::replace(&mut session.state, ExportState::Building { aborted: false })
        };
        let err = svc
            .workspace_export_finalize_op(export_id.clone(), false, None)
            .await
            .expect_err("finalize while building");
        assert!(err.to_string().contains("still building"));
        {
            let mut exports = svc.transfer_exports.lock().unwrap();
            exports.get_mut(&export_id).unwrap().state = prior;
        }
        let finalized = svc
            .workspace_export_finalize_op(export_id, false, None)
            .await
            .expect("finalize");
        assert_eq!(finalized["finalized"], true);
        let ws = svc.store.get_workspace(&id).await.expect("workspace");
        assert_eq!(ws.status, WorkspaceStatus::Active);
        assert!(ws.status_message.is_none());

        // Unknown export id.
        assert!(matches!(
            svc.workspace_export_finalize_op("export-nope".to_string(), false, None)
                .await,
            Err(Error::NotFound(_))
        ));
    }

    /// Git path: a dirty worktree is snapshotted as a WIP commit that rides
    /// the bundle (`git/repo.bundle` + `git/refs.json` in the archive) and
    /// stays in place while the archive is served; abort unwinds it,
    /// restoring the dirty state exactly.
    #[tokio::test]
    async fn export_bundles_git_and_abort_unwinds_wip() {
        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let id = WorkspaceId("ws-git".to_string());

        // A real repo with one commit and a dirty file.
        let repo_path = ws_root.0.join(&id.0).join("repo");
        std::fs::create_dir_all(&repo_path).expect("repo dir");
        {
            let repo = git2::Repository::init_opts(
                &repo_path,
                git2::RepositoryInitOptions::new().initial_head("main"),
            )
            .expect("init");
            std::fs::write(repo_path.join("README.md"), "hello\n").expect("file");
            let sig = git2::Signature::now("Test", "test@example.com").expect("sig");
            let tree_id = {
                let mut index = repo.index().expect("index");
                index.add_path(Path::new("README.md")).expect("add");
                index.write().expect("write");
                index.write_tree().expect("tree")
            };
            let tree = repo.find_tree(tree_id).expect("tree");
            repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
                .expect("commit");
        }
        std::fs::write(repo_path.join("dirty.txt"), "uncommitted\n").expect("dirty");

        let mut ws = crate::tests::workspace(&id);
        ws.repository_path = Some(repo_path.to_string_lossy().to_string());
        svc.store.insert_workspace(&ws).await.expect("workspace");

        let started = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("start");
        let export_id = started["exportId"].as_str().unwrap().to_string();
        assert!(wait_ready(&svc, &export_id).await, "build must succeed");

        // While the archive is served the WIP snapshot is in place: the
        // worktree is clean and HEAD carries the sentinel commit.
        let repo = git2::Repository::open(&repo_path).expect("open");
        let head_msg = repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .message()
            .unwrap_or("")
            .to_string();
        assert!(
            head_msg.starts_with("intent-transfer: WIP snapshot"),
            "HEAD is the WIP snapshot: {head_msg}"
        );
        drop(repo);

        // The archive carries the git payload.
        {
            let mut exports = svc.transfer_exports.lock().unwrap();
            let session = exports.get_mut(&export_id).unwrap();
            assert_eq!(session.wip_paths, vec![repo_path.clone()]);
        }
        let (size, _) = ready_meta(&svc, &export_id);
        let max_chunk = EXPORT_MAX_CHUNK_BYTES as u64;
        let total_chunks = size.div_ceil(max_chunk).max(1);
        let mut archive = Vec::new();
        for seq in 0..total_chunks {
            let chunk = svc
                .workspace_export_read_op(export_id.clone(), seq)
                .await
                .expect("read");
            archive.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(chunk["data"].as_str().unwrap())
                    .expect("base64"),
            );
        }
        let reader = std::io::Cursor::new(archive);
        let mut zip = zip::ZipArchive::new(reader).expect("valid zip");
        let names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        assert!(names.contains(&"git/repo.bundle".to_string()));
        assert!(names.contains(&"git/refs.json".to_string()));
        let mut refs_bytes = Vec::new();
        zip.by_name("git/refs.json")
            .unwrap()
            .read_to_end(&mut refs_bytes)
            .unwrap();
        let refs: crate::transfer_git::TransferRefsManifest =
            serde_json::from_slice(&refs_bytes).expect("refs parse");
        assert_eq!(refs.workspace_branch, "main");
        assert!(refs.workspace_wip_commit_sha.is_some());

        // Abort unwinds the WIP snapshot: dirty state restored.
        svc.workspace_export_abort_op(export_id)
            .await
            .expect("abort");
        let repo = git2::Repository::open(&repo_path).expect("reopen");
        let head_msg = repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .message()
            .unwrap_or("")
            .to_string();
        assert_eq!(head_msg.trim(), "Initial commit");
        assert!(repo_path.join("dirty.txt").exists(), "dirty file restored");
        let statuses = repo
            .statuses(Some(git2::StatusOptions::new().include_untracked(true)))
            .expect("statuses");
        assert!(
            statuses
                .iter()
                .any(|s| s.path().unwrap_or_default() == "dirty.txt"
                    && s.status() == git2::Status::WT_NEW),
            "dirty.txt is untracked again"
        );
    }

    /// Sorted (path, status) pairs — the exact staged/unstaged/untracked
    /// split — for asserting a repo is restored bit-for-bit after an unwind.
    fn status_fingerprint(repo_path: &Path) -> Vec<(String, git2::Status)> {
        let repo = git2::Repository::open(repo_path).expect("open repo");
        let statuses = repo
            .statuses(Some(git2::StatusOptions::new().include_untracked(true)))
            .expect("statuses");
        let mut fingerprint: Vec<(String, git2::Status)> = statuses
            .iter()
            .map(|s| (s.path().unwrap_or_default().to_string(), s.status()))
            .collect();
        fingerprint.sort_by(|a, b| a.0.cmp(&b.0));
        fingerprint
    }

    /// Failure contract (the branch intentd#1118 flagged as untested): the
    /// build fails mid-flight AFTER bundling produced staging artifacts and
    /// a WIP snapshot. Asserts staging is cleaned, `workspace:transfer:failed`
    /// carries the right payload, the WIP snapshot is unwound (status
    /// fingerprint identical, staged/unstaged split preserved), no session
    /// stays registered, and a retry on the same workspace succeeds.
    #[tokio::test]
    async fn export_build_failure_cleans_up_and_allows_retry() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let db = std::env::temp_dir().join(format!("export-test-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&db).await.expect("open store");
        let bus = crate::EventBus::new(store.clone());
        let id = WorkspaceId("ws-fail".to_string());
        let repo_path = ws_root.0.join(&id.0).join("repo");
        let armed = Arc::new(AtomicBool::new(true));
        let failpoint_armed = armed.clone();
        // Captured by the failpoint: HEAD's commit message at the moment of
        // the injected failure. Proves the failure really lands AFTER
        // bundling (HEAD must be the WIP sentinel commit), so the unwind
        // assertions below exercise a snapshot that actually exists.
        let head_at_failure = Arc::new(std::sync::Mutex::new(None::<String>));
        let head_probe = head_at_failure.clone();
        let probe_repo = repo_path.clone();
        let svc = Services::new(store)
            .with_workspaces_root(ws_root.0.clone())
            .with_assets_root(assets_root.0.clone())
            .with_event_bus(bus.clone())
            .with_export_build_failpoint(Arc::new(move |stage: &str| {
                if stage != "writing-archive" || !failpoint_armed.load(Ordering::SeqCst) {
                    return None;
                }
                *head_probe.lock().unwrap() =
                    git2::Repository::open(&probe_repo).ok().and_then(|repo| {
                        let commit = repo.head().ok()?.peel_to_commit().ok()?;
                        Some(commit.message().unwrap_or("").to_string())
                    });
                Some(Error::Internal(
                    "injected archive-write failure".to_string(),
                ))
            }));

        // A repo with every dirty flavor: a staged new file, an unstaged
        // modification, and an untracked file.
        std::fs::create_dir_all(&repo_path).expect("repo dir");
        {
            let repo = git2::Repository::init_opts(
                &repo_path,
                git2::RepositoryInitOptions::new().initial_head("main"),
            )
            .expect("init");
            std::fs::write(repo_path.join("README.md"), "hello\n").expect("file");
            let sig = git2::Signature::now("Test", "test@example.com").expect("sig");
            let tree_id = {
                let mut index = repo.index().expect("index");
                index.add_path(Path::new("README.md")).expect("add");
                index.write().expect("write");
                index.write_tree().expect("tree")
            };
            let tree = repo.find_tree(tree_id).expect("tree");
            repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
                .expect("commit");
            std::fs::write(repo_path.join("staged.txt"), "staged\n").expect("staged");
            let mut index = repo.index().expect("index");
            index.add_path(Path::new("staged.txt")).expect("add staged");
            index.write().expect("write index");
        }
        std::fs::write(repo_path.join("README.md"), "hello\nmodified\n").expect("modify");
        std::fs::write(repo_path.join("untracked.txt"), "untracked\n").expect("untracked");
        let before = status_fingerprint(&repo_path);
        assert_eq!(before.len(), 3, "three dirty entries seeded: {before:?}");

        let mut ws = crate::tests::workspace(&id);
        ws.repository_path = Some(repo_path.to_string_lossy().to_string());
        svc.store.insert_workspace(&ws).await.expect("workspace");

        let mut sub = bus.subscribe(crate::SubscriptionFilter {
            event_types: vec![WORKSPACE_TRANSFER_FAILED.to_string()],
            workspace_id: Some(id.0.clone()),
            ..Default::default()
        });

        let started = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("start");
        let export_id = started["exportId"].as_str().unwrap().to_string();

        // The failed event is published AFTER cleanup, so receiving it means
        // the failure path has fully settled.
        let batch = tokio::time::timeout(std::time::Duration::from_secs(10), sub.recv())
            .await
            .expect("failed event delivered")
            .expect("subscription open");
        let failed = batch
            .iter()
            .find(|e| e.event_type == WORKSPACE_TRANSFER_FAILED)
            .expect("workspace:transfer:failed event");
        assert_eq!(failed.data["workspaceId"], serde_json::json!(id.as_str()));
        assert_eq!(failed.data["exportId"], serde_json::json!(export_id));
        let reason = failed.data["reason"].as_str().expect("reason");
        assert!(
            reason.contains("injected archive-write failure"),
            "reason carries the build error: {reason}"
        );

        // The failure landed post-bundle: at the failpoint HEAD was the WIP
        // sentinel commit, i.e. bundling had snapshotted the dirty worktree.
        let head_msg_at_failure = head_at_failure
            .lock()
            .unwrap()
            .clone()
            .expect("failpoint observed HEAD");
        assert!(
            head_msg_at_failure.starts_with(crate::transfer_git::TRANSFER_WIP_SENTINEL),
            "HEAD at failure must be the WIP snapshot: {head_msg_at_failure}"
        );

        // No session left registered; staging is gone.
        assert!(svc.transfer_exports.lock().unwrap().is_empty());
        assert!(!svc.export_staging_root().join(&export_id).exists());

        // The WIP snapshot (bundling had already run when the build failed)
        // was unwound: HEAD is the original commit and the exact
        // staged/unstaged/untracked split is restored.
        {
            let repo = git2::Repository::open(&repo_path).expect("reopen");
            let head_msg = repo
                .head()
                .unwrap()
                .peel_to_commit()
                .unwrap()
                .message()
                .unwrap_or("")
                .to_string();
            assert_eq!(head_msg.trim(), "Initial commit");
        }
        assert_eq!(status_fingerprint(&repo_path), before);

        // The workspace stays usable: with the failpoint disarmed the same
        // workspace exports successfully.
        armed.store(false, Ordering::SeqCst);
        let retried = svc
            .workspace_export_start_op(id.clone())
            .await
            .expect("retry start");
        let retry_id = retried["exportId"].as_str().unwrap().to_string();
        assert!(wait_ready(&svc, &retry_id).await, "retry must succeed");
        svc.workspace_export_abort_op(retry_id)
            .await
            .expect("abort");
        assert_eq!(status_fingerprint(&repo_path), before);
    }

    /// The orphan sweep clears stale staging dirs but leaves dirs whose id
    /// has a live session in the registry.
    #[tokio::test]
    async fn export_staging_sweep() {
        let ws_root = TempDir::new("export-ws-root");
        let assets_root = TempDir::new("export-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let root = svc.export_staging_root();
        std::fs::create_dir_all(root.join("export-stale")).expect("stale dir");
        std::fs::create_dir_all(root.join("export-live")).expect("live dir");
        svc.transfer_exports
            .lock()
            .expect("transfer export registry poisoned")
            .insert(
                "export-live".to_string(),
                ExportSession {
                    workspace_id: WorkspaceId("ws-live".to_string()),
                    staging_dir: root.join("export-live"),
                    state: ExportState::Building { aborted: false },
                    wip_paths: Vec::new(),
                    max_chunk_bytes: EXPORT_MAX_CHUNK_BYTES,
                },
            );
        svc.sweep_stale_export_staging_dirs().await;
        assert!(!root.join("export-stale").exists());
        assert!(root.join("export-live").exists());
        // Boot sweep (registry empty again, as after a restart) removes
        // everything.
        svc.transfer_exports
            .lock()
            .expect("transfer export registry poisoned")
            .clear();
        svc.sweep_stale_export_staging().await;
        assert!(!root.join("export-live").exists());
    }
}
