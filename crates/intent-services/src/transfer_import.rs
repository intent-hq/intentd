//! Staged, atomic workspace import (`workspace.import.*`, PROTOCOL §5.1):
//! the target side of the FE-mediated transfer relay. `begin` validates the
//! manifest header (format version, compatible intentd version, id collision) and
//! opens a staging session; `chunk` stages seq-numbered archive bytes
//! (idempotent per seq — a retry overwrites the same chunk file); `commit`
//! reassembles the archive, verifies its SHA-256, unpacks it, applies the row
//! transforms (path rewrites, session-id clearing, in-flight → interrupted),
//! materializes the git payload when the archive carries one
//! ([`crate::transfer_materialize`]: checkout + sandboxes recreated from the
//! bundle, rows rewritten to the target paths), inserts every row in ONE
//! store transaction, places assets, and rehydrates agent queues /
//! delegation groups / completion watches / event subscriptions / hooks /
//! PR monitors without a daemon restart; `abort` deletes the staging state.
//! Nothing is visible in `workspace.list` until `commit` succeeds.
//!
//! Archive layout (agreed with the export orchestrator):
//! `manifest.json` (the [`TransferManifest`]), `rows/<table>.jsonl` (one JSON
//! object per row, [`intent_store::TRANSFER_TABLES`] vocabulary),
//! `assets/<assetId>`, `attachments/<attachmentId>` (registered attachment
//! files; a registry row whose stored file was deleted rides the rows payload
//! with no file entry), and optionally `git/repo.bundle` + `git/refs.json`
//! (the [`crate::transfer_git::TransferRefsManifest`]).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use intent_core::transfer::{
    transfer_versions_compatible, TransferManifest, TRANSFER_FORMAT_VERSION,
};
use intent_core::{clock::now_iso, AgentId, Error, Result, Workspace, WorkspaceId};
use intent_store::{Sandbox, SandboxStatus};
use sha2::Digest as _;

use crate::transfer_git::TransferRefsManifest;
use crate::{publish_event, workspace_created_event, workspace_setup_completed_event, Services};

/// Maximum DECODED bytes per `workspace.import.chunk` call. Base64 inflates
/// this by 4/3 on the wire (~21.4 MiB), keeping the full JSON-RPC frame
/// comfortably under the 40 MiB inbound cap (PROTOCOL §1.3).
pub(crate) const IMPORT_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// One in-flight staged import: everything `chunk`/`commit`/`abort` need
/// between calls. Lives in [`Services::transfer_imports`]; in-memory only.
pub(crate) struct ImportSession {
    pub manifest: TransferManifest,
    pub workspace_id: WorkspaceId,
    /// `<workspaces_root>/.import-staging/<importId>/`.
    pub staging_dir: PathBuf,
    /// Declared final archive size — chunks may not exceed it in sum.
    pub declared_size: u64,
    /// Declared lowercase-hex SHA-256 of the complete archive.
    pub declared_sha256: String,
    /// Bytes staged so far, keyed by chunk seq (a retried seq replaces its
    /// entry, so the sum never double-counts).
    pub chunk_sizes: HashMap<u64, u64>,
    /// Set while `commit` is verifying/unpacking/inserting: chunks and
    /// aborts are rejected so concurrent calls cannot mutate the files
    /// being hashed or race the commit's cleanup. Cleared on a failed
    /// commit (the session survives for retry or abort).
    pub committing: bool,
}

impl ImportSession {
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
    /// Root directory staged imports live under. Sibling of the workspace
    /// checkouts so the final asset/worktree moves stay on one filesystem.
    fn import_staging_root(&self) -> PathBuf {
        self.workspaces_root
            .clone()
            .unwrap_or_else(crate::default_workspaces_root)
            .join(".import-staging")
    }

    /// `workspace.import.begin`: validate the manifest header and open a
    /// staging session. Rejects (all `InvalidParams`, each naming the
    /// specifics): unknown archive format versions, archives whose creating
    /// intentd version is not patch-compatible with this daemon (prereleases
    /// require an exact match), malformed versions, the chief workspace,
    /// and workspace-id collisions (an existing row OR another pending
    /// import). Returns `{ importId, maxChunkBytes }`.
    pub(crate) async fn workspace_import_begin_op(
        &self,
        manifest: serde_json::Value,
        archive_size_bytes: u64,
        archive_sha256: String,
    ) -> Result<serde_json::Value> {
        let manifest: TransferManifest = serde_json::from_value(manifest)
            .map_err(|e| Error::InvalidParams(format!("invalid transfer manifest: {e}")))?;
        if manifest.format_version != TRANSFER_FORMAT_VERSION {
            return Err(Error::InvalidParams(format!(
                "unsupported transfer format version {} (this daemon supports {})",
                manifest.format_version, TRANSFER_FORMAT_VERSION
            )));
        }
        let own_version = env!("CARGO_PKG_VERSION");
        if !transfer_versions_compatible(&manifest.creating_intentd_version, own_version) {
            return Err(Error::InvalidParams(format!(
                "archive was created by intentd {} but this daemon is {} — valid released versions must share major/minor; prereleases must match exactly",
                manifest.creating_intentd_version, own_version
            )));
        }
        for table in &manifest.tables {
            if !is_transfer_table(&table.name) {
                return Err(Error::InvalidParams(format!(
                    "unsupported transfer table {} in manifest",
                    table.name
                )));
            }
        }
        if manifest.workspace_id.is_chief() {
            return Err(Error::InvalidParams(
                "The chief workspace cannot be imported".to_string(),
            ));
        }
        if archive_size_bytes == 0 {
            return Err(Error::InvalidParams(
                "archiveSizeBytes must be positive".to_string(),
            ));
        }
        let sha = archive_sha256.trim().to_ascii_lowercase();
        if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::InvalidParams(
                "archiveSha256 must be 64 hex characters".to_string(),
            ));
        }
        self.reject_workspace_collision(&manifest.workspace_id)
            .await?;

        let import_id = format!("import-{}", uuid::Uuid::new_v4());
        let staging_dir = self.import_staging_root().join(&import_id);

        // Register the session BEFORE any directory exists, so every early
        // return leaves nothing on disk — and so the orphan sweep (which
        // checks the registry at removal time) can never classify this
        // import's directory as an orphan.
        {
            let mut imports = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned");
            if imports
                .values()
                .any(|s| s.workspace_id == manifest.workspace_id)
            {
                return Err(Error::InvalidParams(format!(
                    "another import of workspace {} is already in progress",
                    manifest.workspace_id.0
                )));
            }
            let session = ImportSession {
                workspace_id: manifest.workspace_id.clone(),
                manifest,
                staging_dir: staging_dir.clone(),
                declared_size: archive_size_bytes,
                declared_sha256: sha,
                chunk_sizes: HashMap::new(),
                committing: false,
            };
            imports.insert(import_id.clone(), session);
        }

        // Lazy sweep: staging dirs with no live session are orphans (a
        // daemon restart drops the in-memory registry, and pre-fix daemons
        // could leak dirs). Best-effort — a failed sweep never fails begin.
        self.sweep_orphaned_staging_dirs().await;

        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            self.transfer_imports
                .lock()
                .expect("transfer import registry poisoned")
                .remove(&import_id);
            return Err(Error::Internal(format!(
                "create import staging dir failed: {e}"
            )));
        }

        Ok(serde_json::json!({
            "importId": import_id,
            "maxChunkBytes": IMPORT_MAX_CHUNK_BYTES,
        }))
    }

    /// Delete `.import-staging/<id>` directories whose id has no live
    /// session (orphans from a daemon restart mid-upload). Best-effort.
    /// Liveness is checked against the registry immediately before each
    /// removal — not against a snapshot taken before the directory listing —
    /// so a `begin` that registers concurrently with a sweep in flight can
    /// never have its staging directory removed.
    async fn sweep_orphaned_staging_dirs(&self) {
        let root = self.import_staging_root();
        let Ok(mut entries) = tokio::fs::read_dir(&root).await else {
            // no staging root yet — nothing to sweep
            return;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let live = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned")
                .contains_key(&name);
            if live {
                continue;
            }
            tracing::info!(staging = %name, "sweeping orphaned import staging dir");
            if let Err(e) = tokio::fs::remove_dir_all(entry.path()).await {
                tracing::warn!(staging = %name, error = %e, "orphan staging sweep failed");
            }
        }
    }

    /// `InvalidParams` naming the conflict when `id` already exists on this
    /// daemon (import never overwrites; the user renames/deletes first).
    async fn reject_workspace_collision(&self, id: &WorkspaceId) -> Result<()> {
        match self.store.get_workspace(id).await {
            Ok(existing) => Err(Error::InvalidParams(format!(
                "workspace id {} already exists on this daemon (title: {:?}) — delete it or rename before importing",
                id.0, existing.title
            ))),
            Err(Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `workspace.import.chunk`: stage one seq-numbered slice of the archive.
    /// `data` is base64; the decoded slice is written to its own
    /// `chunk-<seq>` file, so retrying a seq is idempotent (same bytes land
    /// in the same file) and chunks may arrive in any order. Rejects decoded
    /// slices over [`IMPORT_MAX_CHUNK_BYTES`] and totals beyond the declared
    /// archive size. Returns `{ importId, seq, receivedBytes }`.
    pub(crate) async fn workspace_import_chunk_op(
        &self,
        import_id: String,
        seq: u64,
        data: String,
    ) -> Result<serde_json::Value> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data.trim())
            .map_err(|e| Error::InvalidParams(format!("chunk data is not valid base64: {e}")))?;
        if bytes.is_empty() {
            return Err(Error::InvalidParams("chunk data is empty".to_string()));
        }
        if bytes.len() > IMPORT_MAX_CHUNK_BYTES {
            return Err(Error::InvalidParams(format!(
                "chunk of {} bytes exceeds the {} byte cap",
                bytes.len(),
                IMPORT_MAX_CHUNK_BYTES
            )));
        }
        // Reserve this seq's bytes under ONE lock hold: the size check and
        // the `chunk_sizes` update are atomic, so concurrent chunks cannot
        // both pass the check and push the total past the declared size.
        let (staging_dir, prior_this_seq, received) = {
            let mut imports = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned");
            let session = imports
                .get_mut(&import_id)
                .ok_or_else(|| Error::NotFound(format!("no import in progress: {import_id}")))?;
            if session.committing {
                return Err(Error::InvalidParams(format!(
                    "import {import_id} is committing — chunks are no longer accepted"
                )));
            }
            // A retried seq replaces its previous bytes; only NEW bytes
            // count against the declared total.
            let prior_this_seq = session.chunk_sizes.get(&seq).copied();
            let new_total =
                session.received_bytes() - prior_this_seq.unwrap_or(0) + bytes.len() as u64;
            if new_total > session.declared_size {
                return Err(Error::InvalidParams(format!(
                    "received {new_total} bytes exceed the declared archive size {}",
                    session.declared_size
                )));
            }
            session.chunk_sizes.insert(seq, bytes.len() as u64);
            (session.staging_dir.clone(), prior_this_seq, new_total)
        };
        let path = staging_dir.join(chunk_file_name(seq));
        if let Err(e) = tokio::fs::write(&path, &bytes).await {
            // Roll the reservation back so a retry accounts correctly.
            let mut imports = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned");
            if let Some(session) = imports.get_mut(&import_id) {
                match prior_this_seq {
                    Some(prior) => session.chunk_sizes.insert(seq, prior),
                    None => session.chunk_sizes.remove(&seq),
                };
            }
            return Err(Error::Internal(format!("write import chunk failed: {e}")));
        }
        Ok(serde_json::json!({
            "importId": import_id,
            "seq": seq,
            "receivedBytes": received,
        }))
    }

    /// `workspace.import.abort`: drop the staging session and delete its
    /// directory. Idempotent — aborting an unknown id succeeds quietly (the
    /// FE may retry an abort after a timeout). Rejected while a commit of
    /// the same import is in flight (abort would race the commit's cleanup).
    pub(crate) async fn workspace_import_abort_op(
        &self,
        import_id: String,
    ) -> Result<serde_json::Value> {
        let session = {
            let mut imports = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned");
            if imports.get(&import_id).is_some_and(|s| s.committing) {
                return Err(Error::InvalidParams(format!(
                    "import {import_id} is committing — wait for the commit to settle"
                )));
            }
            imports.remove(&import_id)
        };
        let aborted = session.is_some();
        if let Some(session) = session {
            let _ = tokio::fs::remove_dir_all(&session.staging_dir).await;
        }
        Ok(serde_json::json!({ "importId": import_id, "aborted": aborted }))
    }

    /// `workspace.import.commit`: reassemble the staged chunks, verify the
    /// archive SHA-256 against the declared checksum, unpack, validate the
    /// embedded manifest matches `begin`'s, transform the rows
    /// ([`transform_rows`]), insert them in ONE store transaction, place
    /// assets, hand the extracted git payload to the materialization seam,
    /// and rehydrate hooks / event subscriptions / PR monitors / agent
    /// queues. The staging session survives a failed commit (the FE can
    /// retry or abort); it is removed only after the rows are committed.
    /// While a commit runs, its session is flagged `committing`, so a
    /// concurrent commit/chunk/abort of the same import is rejected instead
    /// of mutating the files being hashed.
    pub(crate) async fn workspace_import_commit_op(
        &self,
        import_id: String,
    ) -> Result<serde_json::Value> {
        // Phase 1 — validate and CLAIM the `committing` flag under one lock
        // hold. Errors here (unknown id, already committing, incomplete,
        // gaps) never touch a flag another commit owns.
        let claim = {
            let mut imports = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned");
            let session = imports
                .get_mut(&import_id)
                .ok_or_else(|| Error::NotFound(format!("no import in progress: {import_id}")))?;
            if session.committing {
                return Err(Error::InvalidParams(format!(
                    "import {import_id} is already committing"
                )));
            }
            let received = session.received_bytes();
            if received != session.declared_size {
                return Err(Error::InvalidParams(format!(
                    "archive incomplete: received {received} of {} declared bytes",
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
                session.manifest.clone(),
                session.workspace_id.clone(),
                seqs,
            )
        };

        // Phase 2 — the fallible work. On failure, clear the flag THIS call
        // set (phase 1 claimed it exclusively), so the session survives for
        // retry or abort without racing a concurrent commit's flag.
        let result = self.workspace_import_commit_body(&import_id, claim).await;
        if result.is_err() {
            if let Some(session) = self
                .transfer_imports
                .lock()
                .expect("transfer import registry poisoned")
                .get_mut(&import_id)
            {
                session.committing = false;
            }
        }
        result
    }

    async fn workspace_import_commit_body(
        &self,
        import_id: &str,
        claim: (
            PathBuf,
            u64,
            String,
            TransferManifest,
            WorkspaceId,
            Vec<u64>,
        ),
    ) -> Result<serde_json::Value> {
        let (staging_dir, declared_size, declared_sha, manifest, workspace_id, chunk_seqs) = claim;

        // Reassemble + hash + unpack on the blocking pool (sync I/O + zip).
        let extracted_dir = staging_dir.join("extracted");
        {
            let staging_dir = staging_dir.clone();
            let extracted_dir = extracted_dir.clone();
            tokio::task::spawn_blocking(move || {
                assemble_and_extract(
                    &staging_dir,
                    &chunk_seqs,
                    declared_size,
                    &declared_sha,
                    &extracted_dir,
                )
            })
            .await
            .map_err(|e| Error::Internal(format!("import unpack task failed: {e}")))??;
        }

        // The embedded manifest must be the one `begin` validated.
        let embedded = tokio::fs::read(extracted_dir.join("manifest.json"))
            .await
            .map_err(|e| Error::InvalidParams(format!("archive has no manifest.json: {e}")))?;
        let embedded: TransferManifest = serde_json::from_slice(&embedded)
            .map_err(|e| Error::InvalidParams(format!("archive manifest.json is invalid: {e}")))?;
        if embedded != manifest {
            return Err(Error::InvalidParams(
                "archive manifest.json does not match the manifest supplied to workspace.import.begin"
                    .to_string(),
            ));
        }
        // Re-check the collision window between begin and commit.
        self.reject_workspace_collision(&workspace_id).await?;

        // Unknown payloads must fail, not silently disappear when a newer
        // patch introduces a table this receiver does not understand.
        let rows = load_row_files(&extracted_dir.join("rows")).await?;
        // Every row must be scoped to the manifest's workspace — collision
        // validation only ran for that id, so smuggled rows for other
        // workspaces (or agents outside the archive) fail the commit.
        validate_row_scope(&rows, &workspace_id)?;

        // Transform: path rewrites against OUR workspaces root, session-id
        // clearing, in-flight → interrupted, drafts dropped.
        let target_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(crate::default_workspaces_root);
        let mut outcome = transform_rows(rows, &workspace_id, &target_root, &now_iso())?;

        // Materialize the git payload BEFORE the rows land, so the seam can
        // rewrite the transformed workspace/sandbox rows to the target
        // checkout (repository_path, checkout_mode, branch, sandbox paths,
        // dropped sandboxes) and the apply()'d versions are what the store
        // commits. An error fails the commit: disk materialization is
        // all-or-nothing, no rows have landed, and the staging session
        // survives for retry or abort.
        let materialized = self
            .materialize_imported_git(&workspace_id, &extracted_dir, &mut outcome.rows)
            .await?;

        // Materialize attachment files AFTER git (the destination root is
        // the rewritten checkout) and BEFORE the rows land — same
        // all-or-nothing contract. A failure unwinds the git checkout and
        // fails the commit with nothing inserted.
        let attachment_paths = match materialize_imported_attachments(
            &extracted_dir.join("attachments"),
            &outcome.rows,
        )
        .await
        {
            Ok(paths) => paths,
            Err(e) => {
                if let Some(out) = &materialized {
                    crate::transfer_materialize::rollback_materialized(
                        out,
                        &target_root.join(&workspace_id.0),
                    );
                }
                return Err(e);
            }
        };

        // Assets must also be placed before committing rows. A patch archive
        // must not succeed while silently losing files on this receiver.
        let asset_dir = match self
            .place_imported_assets(&workspace_id, &extracted_dir.join("assets"))
            .await
        {
            Ok(dir) => dir,
            Err(e) => {
                rollback_materialized_attachments(&attachment_paths).await;
                if let Some(out) = &materialized {
                    crate::transfer_materialize::rollback_materialized(
                        out,
                        &target_root.join(&workspace_id.0),
                    );
                }
                return Err(e);
            }
        };

        // If the row insert fails AFTER materialization succeeded, unwind
        // the checkout/sandboxes so a retried commit does not hit
        // "materialize target already exists".
        let imported_rows = match self.store.transfer_import_rows(&outcome.rows).await {
            Ok(n) => n,
            Err(e) => {
                if let Some(dir) = asset_dir {
                    let _ = tokio::fs::remove_dir_all(dir).await;
                }
                rollback_materialized_attachments(&attachment_paths).await;
                if let Some(out) = &materialized {
                    crate::transfer_materialize::rollback_materialized(
                        out,
                        &target_root.join(&workspace_id.0),
                    );
                }
                return Err(e);
            }
        };

        // Rows are committed — the import can no longer be rolled back.
        // Everything below is best-effort enrichment of the now-live
        // workspace; failures are logged, never surfaced as a failed import.
        let rehydrated = self.rehydrate_after_import().await;

        // Session retired; staging deleted.
        self.transfer_imports
            .lock()
            .expect("transfer import registry poisoned")
            .remove(import_id);
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;

        let ws = self.store.get_workspace(&workspace_id).await?;
        publish_event(self.event_bus.as_ref(), workspace_created_event(&ws)).await;
        // Imports run no setup stage: publish the completion immediately so
        // the watcher registry starts this workspace's watchers instead of
        // holding the deferred start until the setup backstop expires.
        publish_event(
            self.event_bus.as_ref(),
            workspace_setup_completed_event(&workspace_id, false, None),
        )
        .await;

        Ok(serde_json::json!({
            "workspace": ws,
            "importedRows": imported_rows,
            "interruptedAgents": outcome.interrupted_agent_ids,
            "rehydrated": rehydrated,
        }))
    }

    /// Copy `assets/<assetId>` files into `<assets_root>/<workspaceId>/`.
    /// Return the newly owned directory so failed row inserts can unwind it.
    async fn place_imported_assets(
        &self,
        workspace_id: &WorkspaceId,
        assets_dir: &Path,
    ) -> Result<Option<PathBuf>> {
        let mut entries = match tokio::fs::read_dir(assets_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Internal(format!("read imported assets failed: {e}"))),
        };
        let Some(first) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Internal(format!("read imported assets failed: {e}")))?
        else {
            return Ok(None);
        };
        let root = self.assets_root.as_ref().ok_or_else(|| {
            Error::InvalidParams(
                "import archive carries assets but no assets root is configured".into(),
            )
        })?;
        tokio::fs::create_dir_all(root)
            .await
            .map_err(|e| Error::Internal(format!("create assets root failed: {e}")))?;
        let dest_dir = root.join(&workspace_id.0);
        // Never overwrite pre-existing files; rollback owns only this dir.
        tokio::fs::create_dir(&dest_dir)
            .await
            .map_err(|e| Error::Internal(format!("create imported assets dir failed: {e}")))?;
        let result: Result<()> = async {
            let mut entry = Some(first);
            while let Some(file) = entry {
                tokio::fs::copy(file.path(), dest_dir.join(file.file_name()))
                    .await
                    .map_err(|e| Error::Internal(format!("copy imported asset failed: {e}")))?;
                entry = entries
                    .next_entry()
                    .await
                    .map_err(|e| Error::Internal(format!("read imported assets failed: {e}")))?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            let _ = tokio::fs::remove_dir_all(&dest_dir).await;
            return Err(e);
        }
        Ok(Some(dest_dir))
    }

    /// Materialize the imported git payload (`git/repo.bundle` +
    /// `git/refs.json` under `extracted_dir`) via
    /// [`crate::transfer_materialize::materialize_workspace_git`]: clone the
    /// bundle into `<workspaces_root>/<wsId>/<repo-slug>`, fetch the base
    /// ref, re-provision sandboxes, and unwind WIP snapshots. The checkout
    /// is workspace-owned storage and is NOT registered in `known_repo`
    /// (intent-hq/monorepo#2227). Runs BEFORE the store insert with the
    /// transformed rows passed mutably: the [`MaterializedGit::apply`] row
    /// rewrites (workspace `repository_path` → the new checkout,
    /// `worktree_path` cleared, `checkout_mode` → direct, `branch` → the
    /// bundled branch, `base_commit_sha` backfill, sandbox path rewrites,
    /// dropping sandbox rows whose branch is absent from the bundle) are
    /// written back onto the JSON rows, so the `apply()`'d versions are what
    /// land in the store. An error fails the commit: materialization is
    /// all-or-nothing on disk, no rows have been inserted, and the staging
    /// session survives for retry or abort. Returns the materialization
    /// result (`None` when the archive carries no repository) so the caller
    /// can unwind it via [`rollback_materialized`] if a LATER commit step
    /// fails.
    ///
    /// [`MaterializedGit::apply`]: crate::transfer_materialize::MaterializedGit::apply
    /// [`rollback_materialized`]: crate::transfer_materialize::rollback_materialized
    async fn materialize_imported_git(
        &self,
        workspace_id: &WorkspaceId,
        extracted_dir: &Path,
        rows: &mut [(String, Vec<serde_json::Value>)],
    ) -> Result<Option<crate::transfer_materialize::MaterializedGit>> {
        let git_dir = extracted_dir.join("git");
        let bundle = git_dir.join("repo.bundle");
        if !bundle.exists() {
            return Ok(None); // no repository in the archive
        }
        let refs = tokio::fs::read(git_dir.join("refs.json"))
            .await
            .map_err(|e| {
                Error::InvalidParams(format!(
                    "archive carries git/repo.bundle but git/refs.json is unreadable: {e}"
                ))
            })?;
        let refs: TransferRefsManifest = serde_json::from_slice(&refs)
            .map_err(|e| Error::InvalidParams(format!("archive git/refs.json is invalid: {e}")))?;

        // Rebuild just enough of the workspace/sandbox models from the
        // transformed DB-shaped rows for the materializer (repo naming
        // inputs and the `apply()` targets); everything else takes inert
        // defaults and is never written back.
        let ws_row = rows
            .iter()
            .find(|(t, _)| t == "workspace")
            .and_then(|(_, objects)| objects.first())
            .ok_or_else(|| {
                Error::InvalidParams(
                    "archive carries a git bundle but no workspace row".to_string(),
                )
            })?;
        let mut ws = workspace_for_materialize(workspace_id, ws_row);
        let mut sandboxes: Vec<Sandbox> = rows
            .iter()
            .find(|(t, _)| t == "sandbox")
            .map(|(_, objects)| objects.iter().map(sandbox_for_materialize).collect())
            .unwrap_or_default();

        let target_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(crate::default_workspaces_root);
        let out = crate::transfer_materialize::materialize_workspace_git(
            bundle,
            refs,
            ws.clone(),
            sandboxes.clone(),
            target_root,
        )
        .await?;
        out.apply(&mut ws, &mut sandboxes);

        // Write the applied values back onto the JSON rows.
        for (table, objects) in rows.iter_mut() {
            match table.as_str() {
                "workspace" => {
                    for row in objects.iter_mut() {
                        let Some(map) = row.as_object_mut() else {
                            continue;
                        };
                        map.insert(
                            "repository_path".to_string(),
                            serde_json::json!(ws.repository_path),
                        );
                        map.insert("worktree_path".to_string(), serde_json::Value::Null);
                        map.insert(
                            "checkout_mode".to_string(),
                            serde_json::json!(ws.checkout_mode),
                        );
                        map.insert("branch".to_string(), serde_json::json!(ws.branch));
                        map.insert(
                            "base_commit_sha".to_string(),
                            serde_json::json!(ws.base_commit_sha),
                        );
                    }
                }
                "sandbox" => {
                    objects.retain_mut(|row| {
                        let agent = row
                            .get("agent_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        match sandboxes.iter().find(|sb| sb.agent_id.0 == agent) {
                            Some(sb) => {
                                if let Some(map) = row.as_object_mut() {
                                    map.insert("path".to_string(), serde_json::json!(sb.path));
                                }
                                true
                            }
                            None => false,
                        }
                    });
                }
                _ => {}
            }
        }
        Ok(Some(out))
    }

    /// Boot-style rehydration after a committed import so the workspace's
    /// agent queues, delegation groups, completion watches, event
    /// subscriptions, hooks, and PR monitors go live without a daemon
    /// restart. Every loader is idempotent (each skips entries already live
    /// in memory), so re-running them over the whole store is safe.
    /// Best-effort: failures are logged per loader.
    async fn rehydrate_after_import(&self) -> serde_json::Value {
        // Boot order (main.rs): queues → delegation groups → completion
        // watches (grouped watches need their live groups) → event
        // subscriptions → hooks → PR monitors.
        let agent_queues = self.rehydrate_agent_queues().await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "post-import agent queue rehydration failed");
            0
        });
        let delegation_groups = self
            .heal_delegation_groups_on_startup()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "post-import delegation group rehydration failed");
                0
            });
        let completion_watches = self
            .heal_completion_watches_on_startup()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "post-import completion watch rehydration failed");
                0
            });
        let event_subscriptions = self
            .heal_event_subscriptions_on_startup()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "post-import event subscription rehydration failed");
                0
            });
        let hooks = self.rehydrate_hooks().await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "post-import hook rehydration failed");
            0
        });
        let pr_monitors = self.rehydrate_pr_monitors().await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "post-import PR monitor rehydration failed");
            0
        });
        serde_json::json!({
            "hooks": hooks,
            "eventSubscriptions": event_subscriptions,
            "prMonitors": pr_monitors,
            "agentQueues": agent_queues,
            "delegationGroups": delegation_groups,
            "completionWatches": completion_watches,
        })
    }
}

/// Concatenate the staged chunk files, verify the SHA-256 and size against
/// the declared values, and extract the resulting zip into `extracted_dir`.
/// Runs on the blocking pool (sync file I/O + zip inflation).
fn assemble_and_extract(
    staging_dir: &Path,
    chunk_seqs: &[u64],
    declared_size: u64,
    declared_sha256: &str,
    extracted_dir: &Path,
) -> Result<()> {
    use std::io::Write as _;

    let archive_path = staging_dir.join("archive.zip");
    let mut hasher = sha2::Sha256::new();
    let mut out = std::fs::File::create(&archive_path)
        .map_err(|e| Error::Internal(format!("create assembled archive failed: {e}")))?;
    let mut total = 0u64;
    for seq in chunk_seqs {
        let bytes = std::fs::read(staging_dir.join(chunk_file_name(*seq)))
            .map_err(|e| Error::Internal(format!("read staged chunk {seq} failed: {e}")))?;
        hasher.update(&bytes);
        total += bytes.len() as u64;
        out.write_all(&bytes)
            .map_err(|e| Error::Internal(format!("assemble archive failed: {e}")))?;
    }
    out.flush()
        .map_err(|e| Error::Internal(format!("assemble archive flush failed: {e}")))?;
    drop(out);
    if total != declared_size {
        return Err(Error::InvalidParams(format!(
            "assembled archive is {total} bytes, expected {declared_size}"
        )));
    }
    let actual: String = hasher.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    });
    if actual != declared_sha256 {
        return Err(Error::InvalidParams(format!(
            "archive checksum mismatch: expected sha256 {declared_sha256}, got {actual}"
        )));
    }

    let file = std::fs::File::open(&archive_path)
        .map_err(|e| Error::Internal(format!("open assembled archive failed: {e}")))?;
    let mut zip = zip::ZipArchive::new(file)
        .map_err(|e| Error::InvalidParams(format!("archive is not a valid zip: {e}")))?;
    let mut names = std::collections::HashSet::new();
    for index in 0..zip.len() {
        let entry = zip
            .by_index(index)
            .map_err(|e| Error::InvalidParams(format!("invalid archive entry: {e}")))?;
        let name = entry.name();
        let supported = if entry.is_dir() {
            matches!(name, "rows/" | "assets/" | "attachments/" | "git/")
        } else {
            match name {
                "manifest.json" | "git/repo.bundle" | "git/refs.json" => true,
                _ => name.split_once('/').is_some_and(|(dir, file)| {
                    !file.is_empty()
                        && file != "."
                        && file != ".."
                        && !file.contains(['/', '\\'])
                        && match dir {
                            "rows" => file.strip_suffix(".jsonl").is_some_and(is_transfer_table),
                            "assets" | "attachments" => true,
                            _ => false,
                        }
                }),
            }
        };
        if !supported || entry.is_symlink() || !names.insert(name.to_string()) {
            return Err(Error::InvalidParams(format!(
                "unsupported or duplicate transfer archive entry {name:?}"
            )));
        }
    }
    if names.contains("git/repo.bundle") != names.contains("git/refs.json") {
        return Err(Error::InvalidParams(
            "archive must carry git/repo.bundle and git/refs.json together".into(),
        ));
    }
    std::fs::create_dir_all(extracted_dir)
        .map_err(|e| Error::Internal(format!("create extraction dir failed: {e}")))?;
    // `ZipArchive::extract` sanitizes entry names via `enclosed_name` and
    // refuses symlinks whose target escapes the destination (zip-slip safe;
    // pinned by the `extract_rejects_hostile_entries` regression test).
    zip.extract(extracted_dir)
        .map_err(|e| Error::InvalidParams(format!("archive extraction failed: {e}")))?;
    Ok(())
}

fn is_transfer_table(table: &str) -> bool {
    intent_store::TRANSFER_TABLES
        .iter()
        .any(|(name, _)| *name == table)
}

/// Read every `rows/<table>.jsonl` file into `(table, rows)` pairs. Unknown
/// tables/files (including excluded event history) fail closed.
async fn load_row_files(rows_dir: &Path) -> Result<Vec<(String, Vec<serde_json::Value>)>> {
    let mut out = Vec::new();
    let mut entries = match tokio::fs::read_dir(rows_dir).await {
        Ok(entries) => entries,
        Err(e) => {
            return Err(Error::InvalidParams(format!(
                "archive has no rows/ directory: {e}"
            )))
        }
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| Error::Internal(format!("read archive rows directory failed: {e}")))?
    {
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(table) = name.strip_suffix(".jsonl") else {
            return Err(Error::InvalidParams(format!(
                "unsupported archive row file {name}"
            )));
        };
        if !is_transfer_table(table) {
            return Err(Error::InvalidParams(format!(
                "unsupported transfer table {table}"
            )));
        }
        let content = tokio::fs::read_to_string(entry.path())
            .await
            .map_err(|e| Error::Internal(format!("read rows/{name} failed: {e}")))?;
        let mut rows = Vec::new();
        for (i, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            rows.push(serde_json::from_str(line).map_err(|e| {
                Error::InvalidParams(format!("rows/{name} line {} is invalid JSON: {e}", i + 1))
            })?);
        }
        out.push((table.to_string(), rows));
    }
    Ok(out)
}

/// Materialize `attachments/<attachmentId>` archive entries into the target
/// workspace's canonical `.intent/attachments/` store, at the `stored_path`
/// each (transformed) registry row records. Runs AFTER git materialization
/// (the workspace row already points at the target checkout) and BEFORE the
/// row insert, preserving the commit's all-or-nothing contract: any failure
/// returns `Err` and the caller unwinds. Returns every path created so
/// [`rollback_materialized_attachments`] can delete them if a LATER commit
/// step fails.
///
/// Registry rows with no matching file entry are the exported
/// deleted-is-deleted state — nothing to place, `getAttachment` on the
/// target reports the deleted-from-disk error. File entries with no
/// matching registry row fail the commit (never silently drop files from a
/// newer patch archive). A `stored_path` that escapes the workspace root fails
/// the commit — a tampered row must never write outside the workspace.
async fn materialize_imported_attachments(
    attachments_dir: &Path,
    rows: &[(String, Vec<serde_json::Value>)],
) -> Result<Vec<PathBuf>> {
    let mut entries = match tokio::fs::read_dir(attachments_dir).await {
        Ok(entries) => entries,
        // Absent dir = no attachments/ in the archive; any OTHER read_dir
        // failure must fail the commit — silently committing registry rows
        // without their promised files would break the all-or-nothing
        // contract.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(Error::Internal(format!(
                "read staged attachments dir failed: {e}"
            )))
        }
    };

    // id → stored_path from the transformed registry rows.
    let stored_paths: HashMap<&str, &str> = rows
        .iter()
        .filter(|(t, _)| t == "attachments")
        .flat_map(|(_, objects)| objects)
        .filter_map(|r| {
            Some((
                r.get("id").and_then(|v| v.as_str())?,
                r.get("stored_path").and_then(|v| v.as_str())?,
            ))
        })
        .collect();

    // The canonical workspace root the runtime resolves attachments against
    // (`file_ops::workspace_root`: worktree || repository), from the
    // transformed (and git-materialized) workspace row; the workspace `path`
    // column is the repo-less fallback.
    let ws_row = rows
        .iter()
        .find(|(t, _)| t == "workspace")
        .and_then(|(_, objects)| objects.first());
    let root = ws_row
        .and_then(|r| {
            ["worktree_path", "repository_path", "path"]
                .iter()
                .find_map(|k| r.get(*k).and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
        })
        .map(str::to_string);

    let mut created: Vec<PathBuf> = Vec::new();
    let result: Result<()> = async {
        loop {
            // An iteration error is a real I/O failure mid-materialization —
            // propagate it (and unwind) rather than silently stopping short.
            let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| Error::Internal(format!("read staged attachments dir failed: {e}")))?
            else {
                break;
            };
            let id = entry.file_name().to_string_lossy().to_string();
            let Some(stored_path) = stored_paths.get(id.as_str()) else {
                return Err(Error::InvalidParams(format!(
                    "archive attachment {id} has no registry row with a stored_path"
                )));
            };
            let Some(root) = root.as_deref() else {
                return Err(Error::InvalidParams(format!(
                    "archive carries attachment {id} but the workspace row resolves no \
                     filesystem root to place it under"
                )));
            };
            let escape_err = || {
                Error::InvalidParams(format!(
                    "attachment {id} stored_path {stored_path:?} escapes the workspace \
                     root — import rejected"
                ))
            };
            // The root may not exist yet on repo-less imports; the import
            // owns creating it, and `resolve_attachment_source`'s
            // symlink-aware gate needs it on disk to canonicalize — ensure
            // it before resolving.
            tokio::fs::create_dir_all(root)
                .await
                .map_err(|e| Error::Internal(format!("create workspace root failed: {e}")))?;
            let dest = crate::file_ops::resolve_attachment_source(root, stored_path)
                .map_err(|_| escape_err())?;
            let file_name = dest.file_name().map(PathBuf::from).ok_or_else(escape_err)?;
            let parent = dest.parent().ok_or_else(escape_err)?;
            // `resolve_attachment_source` already re-checks containment
            // through symlinks, but the checks below stay: the git bundle
            // just materialized tracked content, which can include a
            // symlinked ancestor (e.g. a tracked `.intent` symlink) pointing
            // outside the checkout, and both `create_dir_all` and `copy`
            // would follow it. Canonicalize the deepest EXISTING ancestor
            // and require it inside the canonical root BEFORE creating
            // anything — a symlinked escape fails the commit — and re-verify
            // the created parent afterwards (TOCTOU belt-and-braces).
            let canon_root = tokio::fs::canonicalize(root)
                .await
                .map_err(|e| Error::Internal(format!("canonicalize workspace root failed: {e}")))?;
            let mut probe = parent.to_path_buf();
            let existing = loop {
                match tokio::fs::canonicalize(&probe).await {
                    Ok(canon) => break canon,
                    Err(_) => match probe.parent() {
                        Some(p) => probe = p.to_path_buf(),
                        None => return Err(escape_err()),
                    },
                }
            };
            if !existing.starts_with(&canon_root) {
                return Err(escape_err());
            }
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::Internal(format!("create attachments dir failed: {e}")))?;
            let parent = tokio::fs::canonicalize(parent).await.map_err(|e| {
                Error::Internal(format!("canonicalize attachments dir failed: {e}"))
            })?;
            if !parent.starts_with(&canon_root) {
                return Err(escape_err());
            }
            let dest = parent.join(file_name);
            // Nor may the destination itself be a pre-existing symlink —
            // `copy` would follow it and write through to its target.
            if let Ok(meta) = tokio::fs::symlink_metadata(&dest).await {
                if meta.file_type().is_symlink() {
                    return Err(escape_err());
                }
            }
            // Same ignore-all marker `place_attachment` drops, so imported
            // files stay out of git tracking on the target.
            let marker = parent.join(".gitignore");
            if !marker.exists() {
                tokio::fs::write(&marker, "*\n").await.map_err(|e| {
                    Error::Internal(format!("write attachments marker failed: {e}"))
                })?;
                created.push(marker);
            }
            // Record BEFORE copying: a copy that fails partway can leave a
            // partial dest, and rollback must remove it (it tolerates a
            // missing file).
            created.push(dest.clone());
            tokio::fs::copy(entry.path(), &dest)
                .await
                .map_err(|e| Error::Internal(format!("materialize attachment {id} failed: {e}")))?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = result {
        // All-or-nothing on disk: a partial materialization never leaks.
        rollback_materialized_attachments(&created).await;
        return Err(e);
    }
    Ok(created)
}

/// Best-effort unwind of [`materialize_imported_attachments`]: delete every
/// created file, then prune the (now possibly empty) parent directories.
/// A missing file is fine — destinations are recorded before the copy, so
/// the last entry may never have been written.
async fn rollback_materialized_attachments(created: &[PathBuf]) {
    for path in created {
        if let Err(e) = tokio::fs::remove_file(path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "attachment rollback failed");
            }
        }
    }
    for path in created {
        if let Some(parent) = path.parent() {
            // Fails (and is ignored) unless the directory is empty.
            let _ = tokio::fs::remove_dir(parent).await;
        }
    }
}

/// Rebuild the [`Workspace`] fields the materializer consumes from the
/// transformed DB-shaped workspace row: `repository_name`/`repository_path`
/// name the checkout slug, `base_commit_sha` gates the backfill, and
/// `branch` labels errors. Everything else takes inert defaults — the model
/// is only an input to `materialize_workspace_git` and its `apply()`; the
/// JSON row (not this struct) is what lands in the store.
fn workspace_for_materialize(workspace_id: &WorkspaceId, row: &serde_json::Value) -> Workspace {
    let s = |key: &str| -> Option<String> {
        row.get(key)
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
    };
    let now = now_iso();
    Workspace {
        id: workspace_id.clone(),
        title: s("title").unwrap_or_default(),
        branch: s("branch").unwrap_or_default(),
        base_ref: s("base_ref"),
        base_commit_sha: s("base_commit_sha"),
        status: intent_core::WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: intent_core::WorkspaceActivity::Idle,
        attention: intent_core::WorkspaceAttention::None,
        created_at: now.clone(),
        updated_at: now,
        last_activity: None,
        tags: vec![],
        path: s("path"),
        repository_path: s("repository_path"),
        repository_owner: s("repository_owner"),
        repository_name: s("repository_name"),
        worktree_path: s("worktree_path"),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        pending_delete_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
    }
}

/// Rebuild the [`Sandbox`] fields the materializer consumes from a
/// transformed DB-shaped sandbox row (`agent_id` matches bundle entries and
/// names the target directory; `branch` labels warnings). Same contract as
/// [`workspace_for_materialize`]: input-only, never stored.
fn sandbox_for_materialize(row: &serde_json::Value) -> Sandbox {
    let s = |key: &str| -> String {
        row.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    Sandbox {
        id: s("id"),
        workspace_id: WorkspaceId(s("workspace_id")),
        agent_id: AgentId(s("agent_id")),
        path: s("path"),
        branch: s("branch"),
        base_commit_sha: s("base_commit_sha"),
        snapshot_commit_sha: None,
        status: SandboxStatus::Created,
        retry_count: 0,
        created_at: s("created_at"),
        updated_at: s("updated_at"),
    }
}

/// Reject archives whose rows reach beyond the manifest's workspace:
/// `workspace` rows must carry exactly the imported id, every table with a
/// `workspace_id` column must match it, `completion_watch` must touch it
/// from at least one end, and `agent_message`/`agent_queue` rows (scoped
/// through their owning session) must name an `agent_session` in the
/// archive. `agent_message_payload` rows additionally must name an
/// `agent_message` in the archive — the row attaches (and hydrates) by
/// `message_id`, so an in-scope `agent_id` alone would let a hostile
/// archive splice payload bytes into an existing message elsewhere.
/// Collision validation ran only for the manifest's id, so anything else
/// would land unvalidated.
fn validate_row_scope(
    rows: &[(String, Vec<serde_json::Value>)],
    workspace_id: &WorkspaceId,
) -> Result<()> {
    let session_ids: std::collections::HashSet<&str> = rows
        .iter()
        .filter(|(t, _)| t == "agent_session")
        .flat_map(|(_, objects)| objects)
        .filter_map(|r| r.get("id").and_then(|v| v.as_str()))
        .collect();
    let message_ids: std::collections::HashSet<&str> = rows
        .iter()
        .filter(|(t, _)| t == "agent_message")
        .flat_map(|(_, objects)| objects)
        .filter_map(|r| r.get("id").and_then(|v| v.as_str()))
        .collect();
    let field = |row: &serde_json::Value, key: &str| -> String {
        row.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    for (table, objects) in rows {
        for row in objects {
            let ok = match table.as_str() {
                "workspace" => field(row, "id") == workspace_id.0,
                "agent_message" | "agent_queue" => {
                    session_ids.contains(field(row, "agent_id").as_str())
                }
                "agent_message_payload" => {
                    session_ids.contains(field(row, "agent_id").as_str())
                        && message_ids.contains(field(row, "message_id").as_str())
                }
                "completion_watch" => {
                    field(row, "parent_workspace_id") == workspace_id.0
                        || field(row, "child_workspace_id") == workspace_id.0
                }
                _ => field(row, "workspace_id") == workspace_id.0,
            };
            if !ok {
                return Err(Error::InvalidParams(format!(
                    "archive rows/{table}.jsonl contains a row outside workspace {} — import rejected",
                    workspace_id.0
                )));
            }
        }
    }
    Ok(())
}

/// What [`transform_rows`] produced: the insert-ready batch plus the agents
/// that were in-flight at export time (now carrying `interrupted_agent`
/// rows with `resolution='pending'`).
pub(crate) struct TransformOutcome {
    pub rows: Vec<(String, Vec<serde_json::Value>)>,
    pub interrupted_agent_ids: Vec<String>,
}

/// Agent statuses that count as in-flight at export time — the same set
/// [`crate::is_stale_in_flight_status`] heals after a daemon restart, in
/// their persisted DB spellings (serde wire forms of `Active` / `Processing`
/// / `Waiting`).
const IN_FLIGHT_STATUSES: &[&str] = &["active", "Processing", "Waiting"];

/// Apply the import-side row transforms (spec §3/§4):
///
/// - **workspace**: `worktree_path`, `repository_path`, and `path` are
///   rewritten under `<target_root>/<workspaceId>/`; PR linkage columns are
///   kept (monitors re-poll).
/// - **`agent_session`**: `acp_session_id` / `backend_session_id` nulled (no
///   stale resume, ACP sessions are process-local), `is_active` forced 0;
///   in-flight statuses (`active`/`Processing`/`Waiting`) become `idle` with
///   a stop reason, and each such agent gains an `interrupted_agent` row
///   (`resolution='pending'`) so `agent.listInterrupted` offers resumption.
///   The 0103 stats counters (`message_count` / `assistant_message_count` /
///   `conversation_bytes`) are zeroed: the target re-inserts the transferred
///   `agent_message` rows through the counter triggers, which rebuild them
///   from zero — importing the exported values would double-count (same
///   rebuild-on-target approach as the FTS index).
/// - **sandbox**: `path` rewritten under the target root.
/// - **script**: absolute `cwd` rewritten under the target root.
/// - **draft**: dropped — drafts FK onto `client`, which never transfers.
/// - **`interrupted_agent`**: exported pending rows are kept; synthesized rows
///   for newly-interrupted agents are added unless the agent already has one.
fn transform_rows(
    rows: Vec<(String, Vec<serde_json::Value>)>,
    workspace_id: &WorkspaceId,
    target_root: &Path,
    now: &str,
) -> Result<TransformOutcome> {
    let ws_dir = target_root.join(&workspace_id.0);
    let mut interrupted: Vec<(String, String)> = Vec::new(); // (agent_id, prev_status)
    let mut out: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    let mut existing_interrupted: Vec<serde_json::Value> = Vec::new();

    for (table, mut objects) in rows {
        match table.as_str() {
            "draft" => continue,
            "interrupted_agent" => {
                existing_interrupted = objects;
                continue;
            }
            "workspace" => {
                for object in &mut objects {
                    let map = expect_object(&table, object)?;
                    for key in ["worktree_path", "repository_path"] {
                        rewrite_path(map, key, &ws_dir);
                    }
                    // `path` is the workspace dir itself — re-root it whole.
                    if let Some(serde_json::Value::String(value)) = map.get_mut("path") {
                        if Path::new(value.as_str()).is_absolute() {
                            *value = ws_dir.to_string_lossy().to_string();
                        }
                    }
                }
            }
            "agent_session" => {
                for object in &mut objects {
                    let map = expect_object(&table, object)?;
                    // Old patch archives bypass the receiver's one-time
                    // migration 0113. Preserve its identity split on import.
                    normalize_imported_model(map, "model", "provider");
                    normalize_imported_model(map, "last_turn_model", "last_turn_provider");
                    map.insert("acp_session_id".into(), serde_json::Value::Null);
                    map.insert("backend_session_id".into(), serde_json::Value::Null);
                    map.insert("is_active".into(), serde_json::json!(0));
                    for counter in [
                        "message_count",
                        "assistant_message_count",
                        "conversation_bytes",
                    ] {
                        map.insert(counter.into(), serde_json::json!(0));
                    }
                    let status = map
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if IN_FLIGHT_STATUSES.contains(&status.as_str()) {
                        let agent_id = map
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        interrupted.push((agent_id, status.clone()));
                        map.insert("status".into(), serde_json::json!("idle"));
                        map.insert(
                            "stop_reason".into(),
                            serde_json::json!(format!(
                                "workspace was transferred while the agent was responding (previous status: {status})"
                            )),
                        );
                        map.insert("stop_reason_timestamp".into(), serde_json::json!(now));
                    }
                }
            }
            "sandbox" => {
                for object in &mut objects {
                    let map = expect_object(&table, object)?;
                    let agent = map
                        .get("agent_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if let Some(serde_json::Value::String(path)) = map.get_mut("path") {
                        // <ws-dir>/sandboxes/<agentId>/<last component>
                        let slug = Path::new(path.as_str()).file_name().map_or_else(
                            || "repo".to_string(),
                            |s| s.to_string_lossy().to_string(),
                        );
                        *path = ws_dir
                            .join("sandboxes")
                            .join(&agent)
                            .join(slug)
                            .to_string_lossy()
                            .to_string();
                    }
                }
            }
            "script" => {
                for object in &mut objects {
                    let map = expect_object(&table, object)?;
                    rewrite_path(map, "cwd", &ws_dir);
                }
            }
            _ => {}
        }
        out.push((table, objects));
    }

    // Merge synthesized interrupted rows with exported ones. Exported
    // PENDING rows win (they carry the original interruption context), but
    // resolved rows are audit history and must not suppress a fresh pending
    // row for an agent in-flight at export time — `agent_id` is the table's
    // PRIMARY KEY, so such a row is rewritten back to pending in place.
    let mut pending: std::collections::HashSet<String> = existing_interrupted
        .iter()
        .filter(|r| r.get("resolution").and_then(|v| v.as_str()) == Some("pending"))
        .filter_map(|r| r.get("agent_id").and_then(|v| v.as_str()))
        .map(str::to_string)
        .collect();
    let mut interrupted_ids: Vec<String> = pending.iter().cloned().collect();
    for (agent_id, prev_status) in interrupted {
        if agent_id.is_empty() || pending.contains(&agent_id) {
            continue;
        }
        pending.insert(agent_id.clone());
        let row = serde_json::json!({
            "agent_id": agent_id,
            "workspace_id": workspace_id.0,
            "prev_status": prev_status,
            "interrupted_at": now,
            "resolution": "pending",
            "resolved_at": serde_json::Value::Null,
        });
        match existing_interrupted
            .iter_mut()
            .find(|r| r.get("agent_id").and_then(|v| v.as_str()) == Some(agent_id.as_str()))
        {
            Some(resolved_audit_row) => *resolved_audit_row = row,
            None => existing_interrupted.push(row),
        }
        interrupted_ids.push(agent_id);
    }
    interrupted_ids.sort();
    out.push(("interrupted_agent".to_string(), existing_interrupted));

    Ok(TransformOutcome {
        rows: out,
        interrupted_agent_ids: interrupted_ids,
    })
}

/// Replace `map[key]`'s LAST path component context: the stored absolute
/// source path keeps only its final component, re-rooted under `ws_dir`.
/// Non-string / relative / empty values are left untouched.
/// Match migration 0113, including leading-colon and empty-model handling.
/// No-op for bare ids; effort and resolved display identities stay untouched.
fn normalize_imported_model(
    map: &mut serde_json::Map<String, serde_json::Value>,
    model_key: &str,
    provider_key: &str,
) {
    let Some(model) = map.get(model_key).and_then(|value| value.as_str()) else {
        return;
    };
    if !model.contains(':') {
        return;
    }
    let model = model.trim_start_matches(':');
    let (provider, model) = match model.split_once(':') {
        Some((provider, model)) => (Some(provider.to_string()), model.to_string()),
        None => (None, model.to_string()),
    };
    if let Some(provider) = provider {
        map.insert(provider_key.into(), serde_json::json!(provider));
    }
    map.insert(
        model_key.into(),
        if model.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::json!(model)
        },
    );
}

fn rewrite_path(map: &mut serde_json::Map<String, serde_json::Value>, key: &str, ws_dir: &Path) {
    let Some(serde_json::Value::String(value)) = map.get_mut(key) else {
        return;
    };
    let path = Path::new(value.as_str());
    if !path.is_absolute() {
        return;
    }
    let Some(name) = path.file_name() else {
        return;
    };
    *value = ws_dir.join(name).to_string_lossy().to_string();
}

fn expect_object<'v>(
    table: &str,
    value: &'v mut serde_json::Value,
) -> Result<&'v mut serde_json::Map<String, serde_json::Value>> {
    value
        .as_object_mut()
        .ok_or_else(|| Error::InvalidParams(format!("{table} row is not a JSON object")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> WorkspaceId {
        WorkspaceId("ws-import".to_string())
    }

    fn root() -> PathBuf {
        PathBuf::from("/target/workspaces")
    }

    fn transform_one(table: &str, row: serde_json::Value) -> TransformOutcome {
        transform_rows(
            vec![(table.to_string(), vec![row])],
            &ws(),
            &root(),
            "2026-08-11T00:00:00Z",
        )
        .expect("transform")
    }

    fn find<'o>(outcome: &'o TransformOutcome, table: &str) -> &'o Vec<serde_json::Value> {
        &outcome
            .rows
            .iter()
            .find(|(t, _)| t == table)
            .unwrap_or_else(|| panic!("table {table} missing from outcome"))
            .1
    }

    /// Workspace paths are re-rooted under `<target_root>/<workspaceId>/`,
    /// keeping only the source path's final component.
    #[test]
    fn transform_rewrites_workspace_paths() {
        let outcome = transform_one(
            "workspace",
            serde_json::json!({
                "id": "ws-import",
                "worktree_path": "/home/src/intent/workspaces/ws-import/repo",
                "repository_path": "/home/src/code/repo",
                "path": "/home/src/intent/workspaces/ws-import",
            }),
        );
        let row = &find(&outcome, "workspace")[0];
        assert_eq!(row["worktree_path"], "/target/workspaces/ws-import/repo");
        assert_eq!(row["repository_path"], "/target/workspaces/ws-import/repo");
        assert_eq!(row["path"], "/target/workspaces/ws-import");
    }

    /// Relative / null path values are left untouched by the rewrite.
    #[test]
    fn transform_leaves_relative_and_null_paths() {
        let outcome = transform_one(
            "workspace",
            serde_json::json!({
                "id": "ws-import",
                "worktree_path": null,
                "repository_path": "relative/path",
            }),
        );
        let row = &find(&outcome, "workspace")[0];
        assert_eq!(row["worktree_path"], serde_json::Value::Null);
        assert_eq!(row["repository_path"], "relative/path");
    }

    /// In-flight agent sessions (all three spellings) are forced idle with
    /// nulled session ids and gain a pending `interrupted_agent` row; settled
    /// sessions only get the session-id/is_active scrub.
    #[test]
    fn transform_interrupts_in_flight_agents() {
        let rows = vec![(
            "agent_session".to_string(),
            vec![
                serde_json::json!({
                    "id": "agent-a", "status": "active",
                    "acp_session_id": "acp-1", "backend_session_id": "b-1",
                    "is_active": 1
                }),
                serde_json::json!({ "id": "agent-b", "status": "Processing" }),
                serde_json::json!({ "id": "agent-c", "status": "Waiting" }),
                serde_json::json!({
                    "id": "agent-d", "status": "idle", "acp_session_id": "acp-4"
                }),
            ],
        )];
        let outcome =
            transform_rows(rows, &ws(), &root(), "2026-08-11T00:00:00Z").expect("transform");
        let sessions = find(&outcome, "agent_session");
        for s in sessions {
            assert_eq!(s["acp_session_id"], serde_json::Value::Null);
            assert_eq!(s["backend_session_id"], serde_json::Value::Null);
            assert_eq!(s["is_active"], 0);
            assert_eq!(s["message_count"], 0, "0103 counters zeroed for rebuild");
            assert_eq!(s["assistant_message_count"], 0);
            assert_eq!(s["conversation_bytes"], 0);
        }
        assert!(sessions[..3].iter().all(|s| s["status"] == "idle"));
        assert!(sessions[..3]
            .iter()
            .all(|s| s["stop_reason"].as_str().unwrap().contains("transferred")));
        assert_eq!(sessions[3]["status"], "idle");
        assert!(sessions[3].get("stop_reason").is_none());

        assert_eq!(
            outcome.interrupted_agent_ids,
            vec!["agent-a", "agent-b", "agent-c"]
        );
        let interrupted = find(&outcome, "interrupted_agent");
        assert_eq!(interrupted.len(), 3);
        let a = interrupted
            .iter()
            .find(|r| r["agent_id"] == "agent-a")
            .expect("agent-a row");
        assert_eq!(a["prev_status"], "active");
        assert_eq!(a["resolution"], "pending");
        assert_eq!(a["workspace_id"], "ws-import");
    }

    /// An exported pending `interrupted_agent` row wins over a synthesized
    /// one for the same agent (original interruption context preserved).
    #[test]
    fn transform_keeps_exported_interrupted_rows() {
        let rows = vec![
            (
                "agent_session".to_string(),
                vec![serde_json::json!({ "id": "agent-a", "status": "active" })],
            ),
            (
                "interrupted_agent".to_string(),
                vec![serde_json::json!({
                    "agent_id": "agent-a", "workspace_id": "ws-import",
                    "prev_status": "Waiting", "interrupted_at": "2026-01-01T00:00:00Z",
                    "resolution": "pending"
                })],
            ),
        ];
        let outcome =
            transform_rows(rows, &ws(), &root(), "2026-08-11T00:00:00Z").expect("transform");
        let interrupted = find(&outcome, "interrupted_agent");
        assert_eq!(interrupted.len(), 1, "no duplicate for agent-a");
        assert_eq!(interrupted[0]["prev_status"], "Waiting");
        assert_eq!(interrupted[0]["interrupted_at"], "2026-01-01T00:00:00Z");
    }

    /// A RESOLVED audit row (the agent was interrupted before, then resumed)
    /// must NOT suppress the fresh pending row for an agent in-flight at
    /// export time: `agent_id` is the PK, so the audit row is rewritten back
    /// to pending in place.
    #[test]
    fn transform_resolved_audit_row_does_not_suppress_new_interruption() {
        let rows = vec![
            (
                "agent_session".to_string(),
                vec![serde_json::json!({ "id": "agent-a", "status": "active" })],
            ),
            (
                "interrupted_agent".to_string(),
                vec![serde_json::json!({
                    "agent_id": "agent-a", "workspace_id": "ws-import",
                    "prev_status": "Waiting", "interrupted_at": "2026-01-01T00:00:00Z",
                    "resolution": "resumed", "resolved_at": "2026-01-02T00:00:00Z"
                })],
            ),
        ];
        let outcome =
            transform_rows(rows, &ws(), &root(), "2026-08-11T00:00:00Z").expect("transform");
        let interrupted = find(&outcome, "interrupted_agent");
        assert_eq!(interrupted.len(), 1, "one row per agent (PK)");
        assert_eq!(interrupted[0]["resolution"], "pending");
        assert_eq!(interrupted[0]["prev_status"], "active");
        assert_eq!(interrupted[0]["interrupted_at"], "2026-08-11T00:00:00Z");
        assert_eq!(interrupted[0]["resolved_at"], serde_json::Value::Null);
        assert_eq!(outcome.interrupted_agent_ids, vec!["agent-a"]);
    }

    /// Rows outside the manifest's workspace are rejected: smuggled second
    /// `workspace` rows, child rows scoped to another workspace, and
    /// `agent_message` rows whose owning session is not in the archive.
    #[test]
    fn validate_row_scope_rejects_out_of_scope_rows() {
        let ws = ws();
        let scoped = |rows: Vec<(&str, Vec<serde_json::Value>)>| {
            validate_row_scope(
                &rows
                    .into_iter()
                    .map(|(t, r)| (t.to_string(), r))
                    .collect::<Vec<_>>(),
                &ws,
            )
        };

        assert!(scoped(vec![(
            "workspace",
            vec![
                serde_json::json!({ "id": "ws-import" }),
                serde_json::json!({ "id": "ws-smuggled" }),
            ],
        )])
        .is_err());
        assert!(scoped(vec![(
            "note",
            vec![serde_json::json!({ "id": "n1", "workspace_id": "ws-other" })],
        )])
        .is_err());
        assert!(scoped(vec![(
            "agent_message",
            vec![serde_json::json!({ "id": 1, "agent_id": "agent-elsewhere" })],
        )])
        .is_err());
        // Payload row with an in-scope agent_id but a message_id pointing
        // outside the archive: rejected — it would splice payload bytes
        // into an existing message of another workspace.
        assert!(scoped(vec![
            ("workspace", vec![serde_json::json!({ "id": "ws-import" })],),
            (
                "agent_session",
                vec![serde_json::json!({ "id": "agent-a", "workspace_id": "ws-import" })],
            ),
            (
                "agent_message",
                vec![serde_json::json!({ "id": "msg-a", "agent_id": "agent-a" })],
            ),
            (
                "agent_message_payload",
                vec![serde_json::json!({
                    "message_id": "msg-foreign", "agent_id": "agent-a",
                    "block_ordinal": 0, "kind": "tool_result_output",
                    "encoding": "none", "body": { "$base64": "e30=" }
                })],
            ),
        ])
        .is_err());
        assert!(scoped(vec![(
            "completion_watch",
            vec![serde_json::json!({
                "id": "w1", "parent_workspace_id": "ws-a", "child_workspace_id": "ws-b"
            })],
        )])
        .is_err());

        assert!(scoped(vec![
            ("workspace", vec![serde_json::json!({ "id": "ws-import" })],),
            (
                "agent_session",
                vec![serde_json::json!({ "id": "agent-a", "workspace_id": "ws-import" })],
            ),
            (
                "agent_message",
                vec![serde_json::json!({ "id": "msg-a", "agent_id": "agent-a" })],
            ),
            (
                "agent_message_payload",
                vec![serde_json::json!({
                    "message_id": "msg-a", "agent_id": "agent-a",
                    "block_ordinal": 0, "kind": "tool_result_output",
                    "encoding": "none", "body": { "$base64": "e30=" }
                })],
            ),
            (
                "completion_watch",
                vec![serde_json::json!({
                    "id": "w1", "parent_workspace_id": "ws-import",
                    "child_workspace_id": "ws-b"
                })],
            ),
        ])
        .is_ok());
    }

    /// Drafts are dropped entirely (their owning `client` never transfers).
    #[test]
    fn transform_drops_drafts() {
        let outcome = transform_one(
            "draft",
            serde_json::json!({ "workspace_id": "ws-import", "text": "d" }),
        );
        assert!(!outcome.rows.iter().any(|(t, _)| t == "draft"));
    }

    #[test]
    fn transform_normalizes_legacy_model_identities() {
        for (model, provider, expected_model, expected_provider) in [
            ("codex:gpt-test", "old", Some("gpt-test"), "codex"),
            ("::codex:gpt-test", "old", Some("gpt-test"), "codex"),
            (":bare", "old", Some("bare"), "old"),
            ("codex:", "old", None, "codex"),
            ("::", "old", None, "old"),
            ("bare", "old", Some("bare"), "old"),
        ] {
            let out = transform_one(
                "agent_session",
                serde_json::json!({
                    "id": "a", "model": model, "provider": provider,
                    "last_turn_model": model, "last_turn_provider": provider,
                    "reasoning_effort": "high", "resolved_model": "display",
                }),
            );
            let row = &find(&out, "agent_session")[0];
            assert_eq!(row["model"], serde_json::json!(expected_model));
            assert_eq!(row["provider"], expected_provider);
            assert_eq!(row["last_turn_model"], serde_json::json!(expected_model));
            assert_eq!(row["last_turn_provider"], expected_provider);
            assert_eq!(row["reasoning_effort"], "high");
            assert_eq!(row["resolved_model"], "display");
        }
    }

    /// Sandbox paths are re-provisioned under
    /// `<ws-dir>/sandboxes/<agentId>/<repo-slug>`; script cwds re-root like
    /// workspace paths.
    #[test]
    fn transform_rewrites_sandbox_and_script_paths() {
        let outcome = transform_one(
            "sandbox",
            serde_json::json!({
                "id": "sb-1", "agent_id": "agent-a",
                "path": "/src/ws/old/sandboxes/agent-a/repo-slug"
            }),
        );
        assert_eq!(
            find(&outcome, "sandbox")[0]["path"],
            "/target/workspaces/ws-import/sandboxes/agent-a/repo-slug"
        );

        let outcome = transform_one(
            "script",
            serde_json::json!({ "id": "s-1", "cwd": "/src/ws/old/repo" }),
        );
        assert_eq!(
            find(&outcome, "script")[0]["cwd"],
            "/target/workspaces/ws-import/repo"
        );
    }

    /// Untouched tables (notes, hooks, monitors…) pass through verbatim.
    #[test]
    fn transform_passes_other_tables_through() {
        let row = serde_json::json!({
            "hook_id": "h-1", "workspace_id": "ws-import",
            "code": "return { dispatch: false }", "state": "scheduled"
        });
        let outcome = transform_one("hook", row.clone());
        assert_eq!(find(&outcome, "hook")[0], row);
    }

    // ---- functional lifecycle tests -------------------------------------

    use intent_store::Store;

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

    /// Pin the zip-slip safety `assemble_and_extract` relies on: hostile
    /// entry names (`../evil`, absolute paths) and symlinks targeting
    /// outside the destination must not write outside the extraction dir,
    /// whatever the `zip` crate's policy (sanitize or error) is.
    #[test]
    fn extract_rejects_hostile_entries() {
        use std::io::Write as _;

        type Writer<'a> = zip::ZipWriter<&'a mut std::io::Cursor<Vec<u8>>>;
        let run = |build: &dyn for<'a> Fn(&mut Writer<'a>)| {
            let staging = TempDir::new("import-zip-slip");
            let mut buf = std::io::Cursor::new(Vec::new());
            let mut writer = zip::ZipWriter::new(&mut buf);
            build(&mut writer);
            writer.finish().expect("zip");
            let archive = buf.into_inner();
            std::fs::write(staging.0.join(chunk_file_name(0)), &archive).expect("chunk");
            let extracted = staging.0.join("extracted");
            let result = assemble_and_extract(
                &staging.0,
                &[0],
                archive.len() as u64,
                &sha256_hex(&archive),
                &extracted,
            );
            (staging, result)
        };
        let options = || zip::write::FileOptions::<'_, ()>::default();

        // Path traversal in an entry name.
        let (staging, result) = run(&|writer| {
            writer.start_file("../evil.txt", options()).expect("entry");
            writer.write_all(b"escaped").expect("bytes");
        });
        assert!(
            !staging.0.parent().unwrap().join("evil.txt").exists(),
            "traversal entry must not escape the extraction dir (result: {result:?})"
        );

        // Absolute entry name.
        let escape_target = TempDir::new("import-zip-slip-target");
        let abs = escape_target.0.join("evil-abs.txt");
        let abs_name = abs.to_string_lossy().to_string();
        let (_staging, result) = run(&|writer| {
            writer.start_file(&*abs_name, options()).expect("entry");
            writer.write_all(b"escaped").expect("bytes");
        });
        assert!(
            !abs.exists(),
            "absolute entry must not escape the extraction dir (result: {result:?})"
        );

        // Symlink whose target points outside the destination, plus a file
        // extracted THROUGH it. The extraction must FAIL (failing the whole
        // commit — extraction is not atomic, so an intermediate entry may
        // linger inside the staging dir, which a failed commit discards) and
        // nothing may land outside the extraction dir.
        let (_staging, result) = run(&|writer| {
            writer
                .add_symlink("link", "/", options())
                .expect("symlink entry");
            writer
                .start_file("link/evil-via-link.txt", options())
                .expect("entry");
            writer.write_all(b"escaped").expect("bytes");
        });
        assert!(
            result.is_err(),
            "escaping symlink must fail the extraction (result: {result:?})"
        );
        assert!(
            !Path::new("/evil-via-link.txt").exists(),
            "symlink-relayed entry must not escape (result: {result:?})"
        );
    }

    async fn fresh_services(workspaces_root: &Path, assets_root: &Path) -> Services {
        let db = std::env::temp_dir().join(format!("import-test-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&db).await.expect("open store");
        Services::new(store)
            .with_workspaces_root(workspaces_root.to_path_buf())
            .with_assets_root(assets_root.to_path_buf())
    }

    fn manifest(ws: &WorkspaceId) -> TransferManifest {
        TransferManifest {
            format_version: TRANSFER_FORMAT_VERSION,
            creating_intentd_version: env!("CARGO_PKG_VERSION").to_string(),
            workspace_id: ws.clone(),
            created_at: "2026-08-11T00:00:00Z".to_string(),
            tables: vec![],
            assets: vec![],
            attachments: vec![],
            git: intent_core::transfer::TransferGitSummary {
                has_repository: false,
                branch: None,
                dirty_files: vec![],
                sandbox_branches: vec![],
            },
        }
    }

    /// Build a fixture archive: manifest.json + rows/*.jsonl (+ one asset).
    fn build_archive(m: &TransferManifest, rows: &[(&str, Vec<serde_json::Value>)]) -> Vec<u8> {
        build_archive_full(m, rows, None, &[])
    }

    /// [`build_archive`] plus an optional `git/repo.bundle` + `git/refs.json`
    /// payload, matching the export orchestrator's archive layout.
    fn build_archive_with_git(
        m: &TransferManifest,
        rows: &[(&str, Vec<serde_json::Value>)],
        git: Option<(&Path, &TransferRefsManifest)>,
    ) -> Vec<u8> {
        build_archive_full(m, rows, git, &[])
    }

    /// [`build_archive`] plus optional git payload and
    /// `attachments/<attachmentId>` file entries.
    fn build_archive_full(
        m: &TransferManifest,
        rows: &[(&str, Vec<serde_json::Value>)],
        git: Option<(&Path, &TransferRefsManifest)>,
        attachments: &[(&str, &[u8])],
    ) -> Vec<u8> {
        use std::io::Write as _;
        let mut buf = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(&mut buf);
        let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
        zip.start_file("manifest.json", options).expect("manifest");
        zip.write_all(serde_json::to_string(m).expect("json").as_bytes())
            .expect("manifest bytes");
        for (table, objects) in rows {
            zip.start_file(format!("rows/{table}.jsonl"), options)
                .expect("row file");
            for o in objects {
                let line = format!("{}\n", serde_json::to_string(o).expect("row json"));
                zip.write_all(line.as_bytes()).expect("row bytes");
            }
        }
        zip.start_file("assets/img.png", options).expect("asset");
        zip.write_all(b"asset-bytes").expect("asset bytes");
        for (id, bytes) in attachments {
            zip.start_file(format!("attachments/{id}"), options)
                .expect("attachment");
            zip.write_all(bytes).expect("attachment bytes");
        }
        if let Some((bundle_path, refs)) = git {
            zip.start_file("git/repo.bundle", options).expect("bundle");
            zip.write_all(&std::fs::read(bundle_path).expect("bundle bytes"))
                .expect("bundle write");
            zip.start_file("git/refs.json", options).expect("refs");
            zip.write_all(serde_json::to_string(refs).expect("refs json").as_bytes())
                .expect("refs write");
        }
        zip.finish().expect("finish zip");
        buf.into_inner()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(bytes);
        hasher.finalize().iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn fixture_rows(ws: &WorkspaceId) -> Vec<(&'static str, Vec<serde_json::Value>)> {
        let t = "2026-08-11T00:00:00Z";
        vec![
            (
                "workspace",
                vec![serde_json::json!({
                    "id": ws.0, "title": "Imported", "branch": "main",
                    "status": "Active", "worktree_path": "/src/ws/repo",
                    "created_at": t, "updated_at": t
                })],
            ),
            (
                "note",
                vec![serde_json::json!({
                    "id": "n1", "workspace_id": ws.0, "title": "N", "content": "body",
                    "created_at": t, "updated_at": t
                })],
            ),
            (
                "agent_session",
                // Exported 0103 counter values are deliberately wrong (999):
                // the import transform zeroes them and the agent_message
                // re-inserts below rebuild them through the triggers.
                vec![serde_json::json!({
                    "id": "agent-live", "workspace_id": ws.0, "name": "A",
                    "status": "active", "is_active": 1,
                    "acp_session_id": "acp-1", "backend_session_id": "b-1",
                    "model": "codex:gpt-test", "provider": "old-provider",
                    "last_turn_model": "codex:gpt-test", "last_turn_provider": "old-provider",
                    "message_count": 999, "assistant_message_count": 999,
                    "conversation_bytes": 999,
                    "created_at": t, "updated_at": t
                })],
            ),
            (
                "agent_message",
                vec![
                    serde_json::json!({
                        "id": "m-1", "agent_id": "agent-live", "seq": 1,
                        "role": "user", "content": "[\"hi\"]", "created_at": t
                    }),
                    serde_json::json!({
                        "id": "m-2", "agent_id": "agent-live", "seq": 2,
                        "role": "assistant", "content": "[\"hello\"]", "created_at": t
                    }),
                ],
            ),
            (
                "hook",
                vec![serde_json::json!({
                    "hook_id": "h-1", "workspace_id": ws.0, "agent_id": "agent-live",
                    "name": "H", "code": "return { dispatch: false }",
                    "delay_ms": 10000, "state": "scheduled", "created_at": t,
                    "next_run_at": "2027-01-01T00:00:00Z",
                    "expires_at": "2027-01-01T00:30:00Z"
                })],
            ),
            (
                "draft",
                vec![serde_json::json!({
                    "workspace_id": ws.0, "agent_id": "agent-live",
                    "client_id": "client-x", "text": "d", "updated_at": t
                })],
            ),
            (
                "attachments",
                vec![
                    serde_json::json!({
                        "id": "att-live", "workspace_id": ws.0,
                        "file_name": "doc.pdf", "mime_type": "application/pdf",
                        "size": 16, "uploaded_at": t,
                        "stored_path": ".intent/attachments/doc.pdf"
                    }),
                    // Deleted-is-deleted: a registry row with no matching
                    // archive file entry imports as a row without a file.
                    serde_json::json!({
                        "id": "att-gone", "workspace_id": ws.0,
                        "file_name": "gone.txt", "mime_type": null,
                        "size": 3, "uploaded_at": t,
                        "stored_path": ".intent/attachments/gone.txt"
                    }),
                ],
            ),
        ]
    }

    /// Full lifecycle: begin → two chunks (out of order, one retried) →
    /// commit. The workspace is invisible before commit and live after, with
    /// transforms applied (paths re-rooted, session ids nulled, in-flight
    /// agent interrupted, draft dropped), the asset placed, and the hook
    /// rehydrated without a restart.
    #[tokio::test]
    async fn import_lifecycle_begin_chunk_commit() {
        let ws = WorkspaceId("ws-imported".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let m = manifest(&ws);
        let archive = build_archive_full(
            &m,
            &fixture_rows(&ws),
            None,
            &[("att-live", b"attachment-bytes")],
        );
        let sha = sha256_hex(&archive);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        assert!(begin["maxChunkBytes"].as_u64().unwrap() > 0);

        // Nothing visible before commit.
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));

        // Chunks out of order; seq 0 retried (idempotent overwrite).
        let mid = archive.len() / 2;
        svc.workspace_import_chunk_op(import_id.clone(), 1, b64(&archive[mid..]))
            .await
            .expect("chunk 1");
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive[..mid]))
            .await
            .expect("chunk 0");
        let retried = svc
            .workspace_import_chunk_op(import_id.clone(), 0, b64(&archive[..mid]))
            .await
            .expect("chunk 0 retry");
        assert_eq!(
            retried["receivedBytes"].as_u64().unwrap(),
            archive.len() as u64,
            "retried chunk must not double-count"
        );

        let committed = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect("commit");

        let imported = svc.store.get_workspace(&ws).await.expect("workspace live");
        assert_eq!(imported.title, "Imported");
        let expected_wt = ws_root.0.join(&ws.0).join("repo");
        assert_eq!(
            imported.worktree_path.as_deref(),
            Some(expected_wt.to_str().unwrap()),
            "worktree path re-rooted under the target workspaces root"
        );

        let session = svc
            .store
            .get_agent_session(&intent_core::AgentId("agent-live".to_string()))
            .await
            .expect("session");
        assert_eq!(session.status, intent_core::AgentStatus::RuntimeIdle);
        assert!(session.acp_session_id.is_none());
        assert_eq!(session.model.as_deref(), Some("gpt-test"));
        assert_eq!(session.provider.as_deref(), Some("codex"));

        // 0103 stats counters rebuilt from the re-inserted messages by the
        // triggers — not the exported 999s, and not doubled.
        let stats = svc
            .store
            .get_agent_session_message_stats(&ws)
            .await
            .expect("message stats");
        assert_eq!(
            stats.get("agent-live"),
            Some(&(2, true, ("[\"hi\"]".len() + "[\"hello\"]".len()) as u64)),
            "counters must equal a live recompute of the imported messages"
        );
        assert_eq!(
            committed["interruptedAgents"],
            serde_json::json!(["agent-live"])
        );
        assert!(committed["rehydrated"]["hooks"].as_u64().unwrap() >= 1);
        assert!(committed["importedRows"].as_u64().unwrap() >= 4);

        // Draft dropped; asset placed.
        let stats = svc.store.transfer_table_stats(&ws).await.expect("stats");
        let row_count = |n: &str| {
            stats
                .iter()
                .find(|s| s.name == n)
                .map_or(-1, |s| s.row_count)
        };
        assert_eq!(row_count("draft"), 0);
        assert_eq!(row_count("note"), 1);
        assert_eq!(row_count("interrupted_agent"), 1);
        assert_eq!(
            std::fs::read(assets_root.0.join(&ws.0).join("img.png")).expect("asset"),
            b"asset-bytes"
        );

        // Attachment registry rows landed; the existing file materialized at
        // its stored path (under the re-rooted worktree) with the ignore-all
        // marker, while the deleted one imported as a row without a file.
        let atts = svc.store.list_attachments(&ws).await.expect("attachments");
        assert_eq!(atts.len(), 2);
        let att_dir = expected_wt.join(".intent/attachments");
        assert_eq!(
            std::fs::read(att_dir.join("doc.pdf")).expect("attachment file"),
            b"attachment-bytes"
        );
        assert_eq!(
            std::fs::read_to_string(att_dir.join(".gitignore")).expect("marker"),
            "*\n"
        );
        assert!(!att_dir.join("gone.txt").exists(), "deleted-is-deleted");

        // Session retired: a second commit is NotFound.
        assert!(matches!(
            svc.workspace_import_commit_op(import_id).await,
            Err(Error::NotFound(_))
        ));
    }

    // ---- git materialization e2e ----------------------------------------

    fn init_repo(repo_path: &Path) {
        std::fs::create_dir_all(repo_path).unwrap();
        let repo = git2::Repository::init_opts(
            repo_path,
            git2::RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        std::fs::write(repo_path.join("README.md"), "hello\n").unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("README.md")).unwrap();
            index.write().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn commit_file(repo_path: &Path, file: &str, content: &str, message: &str) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        std::fs::write(repo_path.join(file), content).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(file)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[&parent])
            .unwrap()
            .to_string()
    }

    fn repo_head(repo_path: &Path) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        let sha = repo.head().unwrap().target().unwrap().to_string();
        sha
    }

    /// Full `begin → chunk → commit` over an archive that carries a real git
    /// payload: a dirty workspace repo (staged + untracked) and one sandbox,
    /// plus a sandbox row whose branch never made it into the bundle. The
    /// workspace ROW records a stale branch name (`feature-stale`) that
    /// diverges from the branch HEAD actually pointed at when the bundle was
    /// built (`feature`) — the bundle wins. Commit must materialize the
    /// checkout and sandbox on disk, rewrite the stored workspace row
    /// (`repository_path` → checkout, `worktree_path` cleared, `checkout_mode`
    /// direct, branch → the bundled branch, `base_commit_sha` backfilled),
    /// rewrite the stored sandbox path, and drop the bundle-less sandbox
    /// row — WITHOUT registering the workspace-owned checkout in `known_repo`
    /// (intent-hq/monorepo#2227).
    #[tokio::test]
    async fn import_commit_materializes_git() {
        let ws = WorkspaceId("ws-git-imported".to_string());
        let ws_root = TempDir::new("import-git-ws-root");
        let assets_root = TempDir::new("import-git-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        // Source workspace repo on branch `feature` with committed work and
        // dirty state (staged + untracked).
        let src = TempDir::new("import-git-src");
        let repo = src.0.join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);
        {
            let r = git2::Repository::open(&repo).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            r.branch("feature", &head, false).unwrap();
            r.set_head("refs/heads/feature").unwrap();
        }
        let feature_tip = commit_file(&repo, "feature.txt", "feature\n", "feat: branch work");
        std::fs::write(repo.join("staged.txt"), "staged\n").unwrap();
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.add_path(Path::new("staged.txt")).unwrap();
            index.write().unwrap();
        }
        std::fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();

        // Source sandbox: clone on `sb/<agent>` with one commit.
        let agent = "agent-sb";
        let sb_branch = format!("sb/{agent}");
        let sb_src = src.0.join("sandbox");
        let out = std::process::Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg(&repo)
            .arg(&sb_src)
            .output()
            .unwrap();
        assert!(out.status.success());
        {
            let r = git2::Repository::open(&sb_src).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            r.branch(&sb_branch, &head, false).unwrap();
            r.set_head(&format!("refs/heads/{sb_branch}")).unwrap();
        }
        commit_file(&sb_src, "sb.txt", "sandbox work\n", "feat: sandbox commit");

        // Source-side rows (DB-shaped, source-absolute paths). The row's
        // `branch` is stale — HEAD is on `feature`, so the bundle records
        // `feature` and the import must rewrite the row to match.
        let t = "2026-08-11T00:00:00Z";
        let ws_row = serde_json::json!({
            "id": ws.0, "title": "Git WS", "branch": "feature-stale",
            "base_ref": "main", "status": "Active",
            "repository_path": repo.to_string_lossy(),
            "repository_name": "test-repo",
            "created_at": t, "updated_at": t
        });
        let sb_row = serde_json::json!({
            "id": "sb-1", "workspace_id": ws.0, "agent_id": agent,
            "path": sb_src.to_string_lossy(), "branch": sb_branch,
            "base_commit_sha": base, "status": "created",
            "created_at": t, "updated_at": t
        });
        // A second sandbox row with no branch in the bundle — dropped.
        let ghost_row = serde_json::json!({
            "id": "sb-ghost", "workspace_id": ws.0, "agent_id": "agent-ghost",
            "path": "/gone/sandbox", "branch": "sb/agent-ghost",
            "base_commit_sha": base, "status": "created",
            "created_at": t, "updated_at": t
        });
        let sessions = serde_json::json!([
            { "id": agent, "workspace_id": ws.0, "name": "SB",
              "status": "idle", "created_at": t, "updated_at": t },
            { "id": "agent-ghost", "workspace_id": ws.0, "name": "G",
              "status": "idle", "created_at": t, "updated_at": t }
        ]);

        // Bundle the source exactly as the export orchestrator would — the
        // models are built as independent literals (not via the
        // workspace_for_materialize / sandbox_for_materialize helpers under
        // test) so a row-key mapping mistake in a helper cannot cancel out
        // across the export/import round-trip.
        let src_ws = {
            let mut w = crate::tests::workspace(&ws);
            w.branch = "feature".to_string();
            w.base_ref = Some("main".to_string());
            w.repository_path = Some(repo.to_string_lossy().into_owned());
            w.repository_name = Some("test-repo".to_string());
            w
        };
        let src_sb = Sandbox {
            id: "sb-1".to_string(),
            workspace_id: ws.clone(),
            agent_id: AgentId(agent.to_string()),
            path: sb_src.to_string_lossy().into_owned(),
            branch: sb_branch.clone(),
            base_commit_sha: base.clone(),
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: t.to_string(),
            updated_at: t.to_string(),
        };
        let staging = src.0.join("staging");
        let (bundle_path, refs) = crate::transfer_git::create_transfer_bundle(
            &src_ws,
            std::slice::from_ref(&src_sb),
            &staging,
        )
        .expect("bundle");
        assert!(refs.workspace_wip_commit_sha.is_some(), "source was dirty");

        let m = manifest(&ws);
        let rows: Vec<(&str, Vec<serde_json::Value>)> = vec![
            ("workspace", vec![ws_row]),
            ("agent_session", sessions.as_array().unwrap().clone()),
            ("sandbox", vec![sb_row, ghost_row]),
        ];
        let archive = build_archive_with_git(&m, &rows, Some((&bundle_path, &refs)));
        let sha = sha256_hex(&archive);

        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");
        svc.workspace_import_commit_op(import_id)
            .await
            .expect("commit");

        // Stored workspace row: rewritten to the materialized checkout, and
        // the stale row branch replaced by the branch the bundle carried.
        let imported = svc.store.get_workspace(&ws).await.expect("workspace live");
        let checkout = ws_root.0.join(&ws.0).join("test-repo");
        assert_eq!(
            imported.repository_path.as_deref(),
            Some(checkout.to_str().unwrap())
        );
        assert_eq!(imported.worktree_path, None);
        assert_eq!(
            imported.checkout_mode,
            Some(intent_core::CheckoutMode::Direct)
        );
        assert_eq!(imported.branch, "feature", "row branch follows the bundle");
        assert_eq!(imported.base_commit_sha.as_deref(), Some(base.as_str()));

        // Checkout on disk: right branch/tip with the WIP snapshot unwound
        // (dirty files restored, not committed).
        assert_eq!(repo_head(&checkout), feature_tip);
        assert_eq!(
            std::fs::read_to_string(checkout.join("untracked.txt")).unwrap(),
            "untracked\n"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("staged.txt")).unwrap(),
            "staged\n"
        );

        // Sandbox rows: the bundled one rewritten on disk + in the store,
        // the ghost dropped.
        let sandboxes = svc.store.list_sandboxes(&ws).await.expect("sandboxes");
        assert_eq!(sandboxes.len(), 1);
        let expected_sb = ws_root
            .0
            .join(&ws.0)
            .join("sandboxes")
            .join(agent)
            .join("test-repo");
        assert_eq!(sandboxes[0].path, expected_sb.to_string_lossy());
        assert_eq!(
            std::fs::read_to_string(expected_sb.join("sb.txt")).unwrap(),
            "sandbox work\n"
        );

        // The workspace-owned checkout stays out of known_repo.
        assert_eq!(
            svc.store.list_known_repos().await.expect("known repos"),
            vec![],
            "materialized checkout is not registered in known_repo"
        );
    }

    /// A git-carrying archive whose `git/refs.json` is missing or invalid
    /// fails the commit, and the staging session survives for retry/abort.
    #[tokio::test]
    async fn import_commit_rejects_bundle_without_refs() {
        let ws = WorkspaceId("ws-git-norefs".to_string());
        let ws_root = TempDir::new("import-git-ws-root");
        let assets_root = TempDir::new("import-git-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        // Archive with a git/repo.bundle but no git/refs.json.
        let m = manifest(&ws);
        let t = "2026-08-11T00:00:00Z";
        let rows: Vec<(&str, Vec<serde_json::Value>)> = vec![(
            "workspace",
            vec![serde_json::json!({
                "id": ws.0, "title": "W", "branch": "main", "status": "Active",
                "created_at": t, "updated_at": t
            })],
        )];
        let archive = {
            use std::io::Write as _;
            let mut buf = std::io::Cursor::new(build_archive(&m, &rows));
            buf.set_position(buf.get_ref().len() as u64);
            let mut zip = zip::ZipWriter::new_append(buf).expect("append zip");
            let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
            zip.start_file("git/repo.bundle", options).expect("bundle");
            zip.write_all(b"not a real bundle").expect("bundle bytes");
            zip.finish().expect("finish").into_inner()
        };
        let sha = sha256_hex(&archive);

        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");

        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("commit must fail");
        assert!(
            err.to_string().contains("refs.json"),
            "error names refs.json: {err}"
        );
        // Nothing landed; the session survives for abort.
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));
        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort after failed commit");
    }

    /// If the row insert fails AFTER git materialization succeeded, the
    /// commit unwinds the materialization: the checkout directory is
    /// removed and a retried commit re-materializes cleanly instead of
    /// failing on "materialize target already exists".
    #[tokio::test]
    async fn import_commit_rolls_back_materialization_on_row_failure() {
        let ws = WorkspaceId("ws-git-rollback".to_string());
        let ws_root = TempDir::new("import-git-rb-ws-root");
        let assets_root = TempDir::new("import-git-rb-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let src = TempDir::new("import-git-rb-src");
        let repo = src.0.join("source-repo");
        init_repo(&repo);

        let t = "2026-08-11T00:00:00Z";
        let ws_row = serde_json::json!({
            "id": ws.0, "title": "RB WS", "branch": "main", "status": "Active",
            "repository_path": repo.to_string_lossy(),
            "repository_name": "test-repo",
            "created_at": t, "updated_at": t
        });
        // A note row with a column the schema does not have — the row
        // insert fails after materialization has already succeeded.
        let bad_note = serde_json::json!({
            "id": "note-1", "workspace_id": ws.0, "title": "N",
            "no_such_column": "boom",
            "created_at": t, "updated_at": t
        });

        let src_ws = {
            let mut w = crate::tests::workspace(&ws);
            w.repository_path = Some(repo.to_string_lossy().into_owned());
            w.repository_name = Some("test-repo".to_string());
            w
        };
        let staging = src.0.join("staging");
        let (bundle_path, refs) =
            crate::transfer_git::create_transfer_bundle(&src_ws, &[], &staging).expect("bundle");

        let m = manifest(&ws);
        let rows: Vec<(&str, Vec<serde_json::Value>)> =
            vec![("workspace", vec![ws_row]), ("note", vec![bad_note])];
        let archive = build_archive_with_git(&m, &rows, Some((&bundle_path, &refs)));
        let sha = sha256_hex(&archive);

        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");

        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("commit must fail on the bad row");
        assert!(
            err.to_string().contains("no column"),
            "row insert failure surfaced: {err}"
        );

        // Materialization unwound: no checkout on disk, no known_repo
        // registration, no rows landed.
        let checkout = ws_root.0.join(&ws.0).join("test-repo");
        assert!(!checkout.exists(), "checkout removed by rollback");
        assert!(!ws_root.0.join(&ws.0).exists(), "workspace dir removed");
        assert!(
            svc.store
                .list_known_repos()
                .await
                .expect("known repos")
                .is_empty(),
            "known_repo registration rolled back"
        );
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));

        // The session survived; a retried commit re-materializes cleanly
        // and fails on the same row error — NOT on "materialize target
        // already exists".
        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("retry fails on the same rows");
        assert!(
            err.to_string().contains("no column"),
            "retry re-materialized cleanly then hit the row error: {err}"
        );
        assert!(!checkout.exists(), "retry rollback also unwound");

        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort after failed commits");
    }

    /// If the row insert fails AFTER attachment materialization succeeded,
    /// the commit unwinds the attachment files (and prunes the created
    /// dirs), keeping the all-or-nothing contract; a tampered registry row
    /// whose stored path escapes the workspace root fails the commit before
    /// anything lands.
    #[tokio::test]
    async fn import_commit_rolls_back_attachments_on_row_failure() {
        let ws = WorkspaceId("ws-att-rollback".to_string());
        let ws_root = TempDir::new("import-att-rb-ws-root");
        let assets_root = TempDir::new("import-att-rb-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let t = "2026-08-11T00:00:00Z";
        let ws_row = serde_json::json!({
            "id": ws.0, "title": "RB WS", "branch": "main", "status": "Active",
            "worktree_path": "/src/ws/repo",
            "created_at": t, "updated_at": t
        });
        let att_row = serde_json::json!({
            "id": "att-live", "workspace_id": ws.0,
            "file_name": "doc.pdf", "mime_type": null, "size": 16,
            "uploaded_at": t, "stored_path": ".intent/attachments/doc.pdf"
        });
        // A note row with a column the schema does not have — the row
        // insert fails after the attachment file has landed.
        let bad_note = serde_json::json!({
            "id": "note-1", "workspace_id": ws.0, "title": "N",
            "no_such_column": "boom",
            "created_at": t, "updated_at": t
        });

        let m = manifest(&ws);
        let rows: Vec<(&str, Vec<serde_json::Value>)> = vec![
            ("workspace", vec![ws_row.clone()]),
            ("attachments", vec![att_row.clone()]),
            ("note", vec![bad_note]),
        ];
        let archive = build_archive_full(&m, &rows, None, &[("att-live", b"attachment-bytes")]);
        let sha = sha256_hex(&archive);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");

        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("commit must fail on the bad row");
        assert!(
            err.to_string().contains("no column"),
            "row insert failure surfaced: {err}"
        );

        // The materialized attachment (and its marker) was unwound with the
        // re-rooted worktree dir pruned; no rows landed.
        let att_dir = ws_root.0.join(&ws.0).join("repo/.intent/attachments");
        assert!(!att_dir.join("doc.pdf").exists(), "attachment rolled back");
        assert!(!att_dir.exists(), "empty attachments dir pruned");
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));

        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort after failed commit");

        // Containment: a stored_path escaping the workspace root fails the
        // commit before any row lands. A good attachment rides the same
        // archive so — depending on read_dir order — the internal
        // partial-materialization rollback is exercised: whichever entry is
        // processed first, NOTHING may survive the failed commit.
        let good_att = serde_json::json!({
            "id": "att-good", "workspace_id": ws.0,
            "file_name": "good.txt", "mime_type": null, "size": 4,
            "uploaded_at": t, "stored_path": ".intent/attachments/good.txt"
        });
        let hostile_att = serde_json::json!({
            "id": "att-live", "workspace_id": ws.0,
            "file_name": "evil", "mime_type": null, "size": 4,
            "uploaded_at": t, "stored_path": "../../evil"
        });
        let rows: Vec<(&str, Vec<serde_json::Value>)> = vec![
            ("workspace", vec![ws_row]),
            ("attachments", vec![good_att, hostile_att]),
        ];
        let archive = build_archive_full(
            &m,
            &rows,
            None,
            &[("att-good", b"good"), ("att-live", b"evil")],
        );
        let sha = sha256_hex(&archive);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin hostile");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk hostile");
        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("escaping stored_path rejected");
        assert!(
            err.to_string().contains("escapes the workspace root"),
            "containment error surfaced: {err}"
        );
        // Whichever entry read_dir served first, the good file (if placed)
        // was unwound and the escape target never materialized.
        assert!(
            !att_dir.join("good.txt").exists(),
            "partially materialized attachment rolled back"
        );
        assert!(
            !ws_root.0.join("evil").exists() && !ws_root.0.join(&ws.0).join("evil").exists(),
            "escaping stored_path never landed"
        );
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));
        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort hostile");
    }

    /// A symlinked ancestor inside the materialized checkout (e.g. a tracked
    /// `.intent` symlink riding the git bundle) must not let an attachment
    /// escape the workspace root: the canonical-ancestor re-check fails the
    /// commit and nothing lands outside.
    #[tokio::test]
    async fn import_commit_rejects_symlinked_attachment_ancestor() {
        let ws = WorkspaceId("ws-att-symlink".to_string());
        let ws_root = TempDir::new("import-att-sym-ws-root");
        let assets_root = TempDir::new("import-att-sym-assets-root");
        let outside = TempDir::new("import-att-sym-outside");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        // Simulate what a hostile bundle materializes: the checkout exists
        // and `.intent` is a symlink pointing outside the workspace.
        let checkout = ws_root.0.join(&ws.0).join("repo");
        std::fs::create_dir_all(&checkout).expect("checkout dir");
        std::os::unix::fs::symlink(&outside.0, checkout.join(".intent")).expect("evil symlink");

        let t = "2026-08-11T00:00:00Z";
        let ws_row = serde_json::json!({
            "id": ws.0, "title": "Sym WS", "branch": "main", "status": "Active",
            "worktree_path": "/src/ws/repo",
            "created_at": t, "updated_at": t
        });
        let att_row = serde_json::json!({
            "id": "att-live", "workspace_id": ws.0,
            "file_name": "doc.pdf", "mime_type": null, "size": 5,
            "uploaded_at": t, "stored_path": ".intent/attachments/doc.pdf"
        });
        let m = manifest(&ws);
        let rows: Vec<(&str, Vec<serde_json::Value>)> =
            vec![("workspace", vec![ws_row]), ("attachments", vec![att_row])];
        let archive = build_archive_full(&m, &rows, None, &[("att-live", b"bytes")]);
        let sha = sha256_hex(&archive);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");
        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("symlinked ancestor rejected");
        assert!(
            err.to_string().contains("escapes the workspace root"),
            "containment error surfaced: {err}"
        );
        // Nothing escaped through the symlink; no rows landed.
        assert!(
            std::fs::read_dir(&outside.0)
                .expect("outside dir")
                .next()
                .is_none(),
            "nothing written outside the workspace"
        );
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));
        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort symlink");
    }

    /// `begin` rejects: bad format version, version mismatch (naming both
    /// versions), chief workspace, malformed sha, and id collisions.
    #[tokio::test]
    async fn import_begin_rejections() {
        let ws = WorkspaceId("ws-reject".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let sha = "a".repeat(64);

        let mut bad_format = manifest(&ws);
        bad_format.format_version = 999;
        let err = svc
            .workspace_import_begin_op(serde_json::to_value(&bad_format).unwrap(), 10, sha.clone())
            .await
            .expect_err("format");
        assert!(err.to_string().contains("999"));

        let mut bad_version = manifest(&ws);
        bad_version.creating_intentd_version = "0.0.1-other".to_string();
        let err = svc
            .workspace_import_begin_op(serde_json::to_value(&bad_version).unwrap(), 10, sha.clone())
            .await
            .expect_err("version");
        let msg = err.to_string();
        assert!(msg.contains("0.0.1-other") && msg.contains(env!("CARGO_PKG_VERSION")));

        for version in [
            "99.0.0",
            "0.99.0",
            "0.9.6-rc.1",
            "not-semver",
            "0.9",
            "v0.9.6",
        ] {
            if version == env!("CARGO_PKG_VERSION") {
                continue;
            }
            bad_version.creating_intentd_version = version.to_string();
            let err = svc
                .workspace_import_begin_op(
                    serde_json::to_value(&bad_version).unwrap(),
                    10,
                    sha.clone(),
                )
                .await
                .expect_err("incompatible version");
            assert!(err.to_string().contains(version));
        }
        // Harmless additive metadata remains forwards-compatible; required
        // new semantics must use a format version this receiver rejects.
        let mut extended = serde_json::to_value(manifest(&ws)).unwrap();
        extended["exporterLabel"] = serde_json::json!("optional description");
        extended["git"]["displayLabel"] = serde_json::json!("optional git metadata");
        let accepted = svc
            .workspace_import_begin_op(extended, 10, sha.clone())
            .await
            .expect("additive metadata");
        svc.workspace_import_abort_op(accepted["importId"].as_str().unwrap().to_string())
            .await
            .unwrap();

        let chief = manifest(&WorkspaceId::chief());
        assert!(svc
            .workspace_import_begin_op(serde_json::to_value(&chief).unwrap(), 10, sha.clone())
            .await
            .is_err());

        let err = svc
            .workspace_import_begin_op(
                serde_json::to_value(manifest(&ws)).unwrap(),
                10,
                "nothex".to_string(),
            )
            .await
            .expect_err("sha");
        assert!(err.to_string().contains("64 hex"));

        // Collision with an existing workspace row.
        let existing = crate::tests::workspace(&ws);
        svc.store.insert_workspace(&existing).await.expect("seed");
        let err = svc
            .workspace_import_begin_op(serde_json::to_value(manifest(&ws)).unwrap(), 10, sha)
            .await
            .expect_err("collision");
        assert!(err.to_string().contains(ws.0.as_str()));
    }

    /// Version compatibility never authorizes dropping unknown data, ignoring
    /// scope, or bypassing the target schema. Every rejection leaves no rows.
    #[tokio::test]
    async fn import_patch_compatible_archive_rejects_incompatible_data() {
        let ws = WorkspaceId("ws-patch-invalid".into());
        let ws_root = TempDir::new("import-patch-ws");
        let assets_root = TempDir::new("import-patch-assets");
        let mut svc = fresh_services(&ws_root.0, &assets_root.0).await;
        let mut m = manifest(&ws);
        let mut version = semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
        if version.pre.is_empty() {
            version.patch += 1;
        }
        m.creating_intentd_version = version.to_string();
        for (case, expected) in [
            ("table", "future_state"),
            ("column", "future_column"),
            ("scope", "outside workspace"),
            ("constraint", "NOT NULL"),
            ("orphan_attachment", "no registry row"),
            ("assets_unconfigured", "no assets root"),
        ] {
            let mut rows = fixture_rows(&ws);
            let mut attachments: Vec<(&str, &[u8])> = Vec::new();
            match case {
                "table" => rows.push((
                    "future_state",
                    vec![serde_json::json!({"workspace_id": ws.0})],
                )),
                "column" => rows[1].1[0]["future_column"] = serde_json::json!("must not disappear"),
                "scope" => rows[1].1[0]["workspace_id"] = serde_json::json!("other-workspace"),
                "constraint" => {
                    rows[1].1[0]["title"] = serde_json::Value::Null;
                }
                "orphan_attachment" => attachments.push(("unregistered", b"must not disappear")),
                "assets_unconfigured" => svc.assets_root = None,
                _ => unreachable!(),
            }
            let archive = build_archive_full(&m, &rows, None, &attachments);
            let begun = svc
                .workspace_import_begin_op(
                    serde_json::to_value(&m).unwrap(),
                    archive.len() as u64,
                    sha256_hex(&archive),
                )
                .await
                .expect("compatible version");
            let id = begun["importId"].as_str().unwrap().to_string();
            svc.workspace_import_chunk_op(id.clone(), 0, b64(&archive))
                .await
                .unwrap();
            let err = svc
                .workspace_import_commit_op(id.clone())
                .await
                .expect_err(case);
            assert!(err.to_string().contains(expected), "{case}: {err}");
            assert!(matches!(
                svc.store.get_workspace(&ws).await,
                Err(Error::NotFound(_))
            ));
            assert!(!assets_root.0.join(&ws.0).exists());
            svc.workspace_import_abort_op(id).await.unwrap();
        }
    }

    #[tokio::test]
    async fn import_asset_copy_failure_rolls_back_without_overwriting_existing_paths() {
        let ws = WorkspaceId("ws-assets-failure".into());
        let ws_root = TempDir::new("import-assets-ws");
        let assets_root = TempDir::new("import-assets-dest");
        let staged = TempDir::new("import-assets-staged");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;
        std::fs::write(staged.0.join("good.png"), b"real asset bytes").unwrap();
        // An actual copy I/O error, regardless of the order read_dir yields.
        std::fs::create_dir(staged.0.join("uncopyable.png")).unwrap();
        let dest = assets_root.0.join(&ws.0);
        let err = svc
            .place_imported_assets(&ws, &staged.0)
            .await
            .expect_err("copy fails");
        assert!(err.to_string().contains("copy imported asset"), "{err}");
        assert!(
            !dest.exists(),
            "owned directory and any earlier copies rolled back"
        );

        std::fs::create_dir(&dest).unwrap();
        std::fs::write(dest.join("good.png"), b"pre-existing bytes").unwrap();
        let err = svc
            .place_imported_assets(&ws, &staged.0)
            .await
            .expect_err("do not overwrite");
        assert!(
            err.to_string().contains("create imported assets dir"),
            "{err}"
        );
        assert_eq!(
            std::fs::read(dest.join("good.png")).unwrap(),
            b"pre-existing bytes"
        );
        assert!(!dest.join("uncopyable.png").exists());
    }

    /// `commit` rejects an incomplete upload and a checksum mismatch — and
    /// the session survives the failed commit for a retry or abort; `abort`
    /// removes the staging dir and is idempotent.
    #[tokio::test]
    async fn import_commit_verification_and_abort() {
        let ws = WorkspaceId("ws-verify".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let m = manifest(&ws);
        let archive = build_archive(&m, &fixture_rows(&ws));
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                "b".repeat(64), // deliberately wrong checksum
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().unwrap().to_string();

        // Incomplete: only half the bytes staged.
        let mid = archive.len() / 2;
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive[..mid]))
            .await
            .expect("chunk 0");
        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("incomplete");
        assert!(err.to_string().contains("incomplete"));

        // Complete but checksum mismatch.
        svc.workspace_import_chunk_op(import_id.clone(), 1, b64(&archive[mid..]))
            .await
            .expect("chunk 1");
        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("checksum");
        assert!(err.to_string().contains("checksum mismatch"));
        assert!(matches!(
            svc.store.get_workspace(&ws).await,
            Err(Error::NotFound(_))
        ));

        // Session survived both failures; abort cleans it up.
        let staging = svc.import_staging_root().join(&import_id);
        assert!(staging.exists());
        let aborted = svc
            .workspace_import_abort_op(import_id.clone())
            .await
            .expect("abort");
        assert_eq!(aborted["aborted"], true);
        assert!(!staging.exists());
        let again = svc
            .workspace_import_abort_op(import_id)
            .await
            .expect("abort again");
        assert_eq!(again["aborted"], false);
    }

    /// Chunk staging enforces the per-chunk cap, the declared total, and
    /// rejects unknown import ids; a second `begin` for the same workspace
    /// while one import is pending is rejected.
    #[tokio::test]
    async fn import_chunk_guards() {
        let ws = WorkspaceId("ws-chunk".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let m = manifest(&ws);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                8,
                "c".repeat(64),
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().unwrap().to_string();

        // Duplicate begin for the same workspace id.
        let err = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                8,
                "c".repeat(64),
            )
            .await
            .expect_err("dup begin");
        assert!(err.to_string().contains("already in progress"));

        // Over the declared total.
        let err = svc
            .workspace_import_chunk_op(import_id.clone(), 0, b64(b"123456789"))
            .await
            .expect_err("over total");
        assert!(err.to_string().contains("declared archive size"));

        // Unknown import id.
        assert!(matches!(
            svc.workspace_import_chunk_op("import-nope".to_string(), 0, b64(b"x"))
                .await,
            Err(Error::NotFound(_))
        ));

        svc.workspace_import_abort_op(import_id)
            .await
            .expect("abort");
    }

    /// Regression (monorepo#2274): a commit rejected with "already
    /// committing" must NOT clear the `committing` flag owned by the
    /// in-flight commit — chunks and aborts stay rejected while the first
    /// commit runs.
    #[tokio::test]
    async fn rejected_concurrent_commit_leaves_committing_flag_intact() {
        let ws = WorkspaceId("ws-commit-flag".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let m = manifest(&ws);
        let archive = build_archive(&m, &fixture_rows(&ws));
        let sha = sha256_hex(&archive);
        let begin = svc
            .workspace_import_begin_op(
                serde_json::to_value(&m).expect("manifest json"),
                archive.len() as u64,
                sha,
            )
            .await
            .expect("begin");
        let import_id = begin["importId"].as_str().expect("importId").to_string();
        svc.workspace_import_chunk_op(import_id.clone(), 0, b64(&archive))
            .await
            .expect("chunk");
        // Simulate an in-flight first commit holding the claim.
        svc.transfer_imports
            .lock()
            .unwrap()
            .get_mut(&import_id)
            .unwrap()
            .committing = true;

        let err = svc
            .workspace_import_commit_op(import_id.clone())
            .await
            .expect_err("second commit rejected");
        assert!(err.to_string().contains("already committing"), "got {err}");
        // The rejection did not release the first commit's claim: the flag
        // is still set, and chunk/abort remain rejected.
        assert!(
            svc.transfer_imports
                .lock()
                .unwrap()
                .get(&import_id)
                .unwrap()
                .committing,
            "committing flag must still be held"
        );
        let err = svc
            .workspace_import_chunk_op(import_id.clone(), 1, b64(b"x"))
            .await
            .expect_err("chunk during commit");
        assert!(err.to_string().contains("committing"), "got {err}");
        let err = svc
            .workspace_import_abort_op(import_id.clone())
            .await
            .expect_err("abort during commit");
        assert!(err.to_string().contains("committing"), "got {err}");

        // Release the claim (as the first commit's failure path would) and
        // the retry path works end to end.
        svc.transfer_imports
            .lock()
            .unwrap()
            .get_mut(&import_id)
            .unwrap()
            .committing = false;
        svc.workspace_import_commit_op(import_id)
            .await
            .expect("commit after release");
    }

    /// Regression (monorepo#2274): the sweep checks session liveness against
    /// the registry at removal time, so a session registered while a sweep
    /// is in flight (its id absent from any pre-listing snapshot) keeps its
    /// staging dir; only truly session-less dirs are removed.
    #[tokio::test]
    async fn sweep_spares_live_sessions_registered_after_listing() {
        let ws = WorkspaceId("ws-sweep-race".to_string());
        let ws_root = TempDir::new("import-ws-root");
        let assets_root = TempDir::new("import-assets-root");
        let svc = fresh_services(&ws_root.0, &assets_root.0).await;

        let staging_root = svc.import_staging_root();
        let live_dir = staging_root.join("import-live");
        let orphan_dir = staging_root.join("import-orphan");
        std::fs::create_dir_all(&live_dir).unwrap();
        std::fs::create_dir_all(&orphan_dir).unwrap();
        // Register `import-live` directly (as a begin racing the sweep
        // would), so it is live in the registry but absent from any snapshot
        // taken before its directory existed.
        svc.transfer_imports.lock().unwrap().insert(
            "import-live".to_string(),
            super::ImportSession {
                manifest: manifest(&ws),
                workspace_id: ws.clone(),
                staging_dir: live_dir.clone(),
                declared_size: 1,
                declared_sha256: "a".repeat(64),
                chunk_sizes: std::collections::HashMap::new(),
                committing: false,
            },
        );

        svc.sweep_orphaned_staging_dirs().await;
        assert!(live_dir.exists(), "live session dir must survive the sweep");
        assert!(!orphan_dir.exists(), "orphan dir must be swept");
    }
}
