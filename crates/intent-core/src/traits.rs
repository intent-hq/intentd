//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::{AgentId, ClientId, HookId, NoteId, PrMonitorId, WorkspaceGitRootId, WorkspaceId};
use crate::model::{
    AgentDelegateInput, AgentLite, AgentSession, CommentAddResult, CommentDeleteResult,
    CommentGetThreadResult, CommentListResult, CommentResolveThreadResult, CommentRespondResult,
    ContextItem, Draft, EventQueryParams, EventSubscribeResult, EventUnsubscribeResult,
    GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult, GitMergeConflicts,
    GitPullResult, GitStatus, LineAttributionComputeResult, LineAttributionData, MessageOrigin,
    Note, NoteAddInput, NoteAddResult, NoteCreate, NoteCreateResult, NoteDeleteResult,
    NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteRestoreVersionResult, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVersion, NoteVersionSummary, ProjectType, ReadAssetResult,
    RepoConfig, SaveAssetResult, ScriptCreateParams, SetupScript, TaskAgentLink,
    TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskListResult, TaskMarkAsTaskResult, TaskRemoveAgentFromAllTasksResult,
    TaskSetRelationsResult, TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult,
    TokenUsage, Workspace, WorkspaceCreate, WorkspaceCreateResult, WorkspaceEventSummary,
    WorkspaceTask, WorkspaceUpdate,
};

/// Boxed, `Send` future — keeps [`WorkspaceApi`] object-safe so it can be held
/// as `Arc<dyn WorkspaceApi>` (the agent→BE callback handle, §6.8).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The single shared service surface (§6.8, "one impl, two front doors"): the
/// transport JSON-RPC router and the agent→BE MCP callback both dispatch to the
/// same `WorkspaceApi`, so an agent calling `note.*`/`task.*`/`comment.*`/
/// `agent.delegate`/event queries reuses the FE's service logic without a
/// dependency cycle. Defined here in the leaf crate so `intent-acp` can hold an
/// `Arc<dyn WorkspaceApi>`; the real, store-backed implementation lives in
/// `intent-services` (§3.2 rule 3). The default bodies return an internal error
/// so downstream stubs compile until they override these methods.
pub trait WorkspaceApi: Send + Sync {
    /// List workspaces, optionally including archived ones (PROTOCOL §5.1).
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let _ = include_archived;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_workspaces not implemented".to_string(),
            ))
        })
    }

    /// Store-backed workspace rows plus live `activity` and the cheap status
    /// aggregates (`taskStats` via a counting query, derived `displayStatus`,
    /// lifetime-cached `cowSupported`) — no notes/sessions enrichment. Used by
    /// the workspace subscription snapshot so seq-0 cannot emit multi-MB
    /// enriched payloads that HOL the connection writer (observed ~4.5 MiB /
    /// 80 workspaces) while staying self-sufficient for client status
    /// rendering. The heavy card aggregates (`agentSummary`/`diffSummary`) are
    /// omitted; clients treat missing fields as "derive locally / wait for
    /// deltas" (same as a notes-read failure on list). Default falls back to
    /// full `list_workspaces`.
    fn list_workspaces_lite(
        &self,
        include_archived: bool,
    ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        self.list_workspaces(include_archived)
    }

    /// Fetch one workspace by id; `NotFound` if absent (PROTOCOL §5.1).
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_workspace not implemented".to_string(),
            ))
        })
    }

    /// `workspace.diskUsage`: on-demand cached physical footprint of the
    /// workspace's daemon-managed directory (PROTOCOL §5.1) —
    /// `{ diskUsage?, refreshing }`. A fresh cache entry returns the usage
    /// with `refreshing: false`; a stale or absent entry arms a background
    /// walk and returns `refreshing: true` (the stale value is served when
    /// available). Non-qualifying workspaces (remote, skip-isolation, chief,
    /// never-provisioned directory) return `refreshing: false` with the
    /// field omitted; `NotFound` if the workspace is absent.
    fn workspace_disk_usage(&self, id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_disk_usage not implemented".to_string(),
            ))
        })
    }

    /// `workspace.transfer.plan`: read-only preview of a workspace transfer
    /// (PROTOCOL §5.1) — the versioned [`crate::transfer::TransferManifest`]
    /// plus a size estimate broken down as DB row bytes + asset bytes +
    /// estimated git bundle bytes, and non-blocking pre-flight warnings.
    /// No side effects; `NotFound` if the workspace is absent.
    fn workspace_transfer_plan(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, Result<crate::transfer::TransferPlan>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_transfer_plan not implemented".to_string(),
            ))
        })
    }

    /// `workspace.import.begin`: validate a transfer archive's manifest
    /// header (format version, exact creating-intentd-version match, id
    /// collision) and open a staged import session (PROTOCOL §5.1). Returns
    /// `{ importId, maxChunkBytes }`.
    fn workspace_import_begin(
        &self,
        manifest: serde_json::Value,
        archive_size_bytes: u64,
        archive_sha256: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (manifest, archive_size_bytes, archive_sha256);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_import_begin not implemented".to_string(),
            ))
        })
    }

    /// `workspace.import.chunk`: stage one seq-numbered base64 slice of the
    /// archive; retrying a seq is idempotent (PROTOCOL §5.1). Returns
    /// `{ importId, seq, receivedBytes }`.
    fn workspace_import_chunk(
        &self,
        import_id: String,
        seq: u64,
        data: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (import_id, seq, data);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_import_chunk not implemented".to_string(),
            ))
        })
    }

    /// `workspace.import.commit`: verify the assembled archive's checksum,
    /// unpack it, and atomically import the workspace — rows in one
    /// transaction, then assets, git materialization, and boot-style
    /// rehydration (PROTOCOL §5.1). Returns `{ workspace, importedRows,
    /// interruptedAgents, rehydrated }`.
    fn workspace_import_commit(
        &self,
        import_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = import_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_import_commit not implemented".to_string(),
            ))
        })
    }

    /// `workspace.import.abort`: delete a staged import's session and
    /// staging directory; idempotent (PROTOCOL §5.1). Returns
    /// `{ importId, aborted }`.
    fn workspace_import_abort(
        &self,
        import_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = import_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_import_abort not implemented".to_string(),
            ))
        })
    }

    /// `workspace.export.start`: stop the workspace's agents and build the
    /// transfer zip archive on a background task (PROTOCOL §5.1); progress
    /// and outcome travel on `workspace:transfer:progress` / `:ready` /
    /// `:failed` events. Returns `{ exportId, maxChunkBytes }` immediately.
    fn workspace_export_start(&self, id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_export_start not implemented".to_string(),
            ))
        })
    }

    /// `workspace.export.read`: serve one seq-numbered base64 chunk of a
    /// ready export archive; idempotent per seq (PROTOCOL §5.1). Returns
    /// `{ exportId, seq, totalChunks, data }`.
    fn workspace_export_read(
        &self,
        export_id: String,
        seq: u64,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (export_id, seq);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_export_read not implemented".to_string(),
            ))
        })
    }

    /// `workspace.export.finalize`: settle the source after a successful
    /// relay — unwind WIP snapshots, delete staging, apply the optional
    /// final status message, and archive the workspace when requested
    /// (PROTOCOL §5.1). Returns `{ exportId, finalized, workspace }`.
    fn workspace_export_finalize(
        &self,
        export_id: String,
        archive_source: bool,
        final_status_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (export_id, archive_source, final_status_message);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_export_finalize not implemented".to_string(),
            ))
        })
    }

    /// `workspace.export.abort`: cancel an export — staging deleted, WIP
    /// snapshots unwound, workspace left usable (agents stay stopped);
    /// idempotent (PROTOCOL §5.1). Returns `{ exportId, aborted }`.
    fn workspace_export_abort(
        &self,
        export_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = export_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::workspace_export_abort not implemented".to_string(),
            ))
        })
    }

    /// Create a workspace from wire input, filling ids/defaults, and
    /// orchestrate the optional initial agent — created and its prompt
    /// delivered inside the same idempotency scope, so `initialAgent` is
    /// present in the result iff an agent was created (PROTOCOL §5.1).
    ///
    /// `idempotency_key` is the optional `params.idempotencyKey` (design note TB-0
    /// §5): when present and previously recorded, the original result is returned
    /// without re-executing; soft-launch when absent (warn + execute).
    fn create_workspace(
        &self,
        input: WorkspaceCreate,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<WorkspaceCreateResult>> {
        let _ = (input, idempotency_key);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_workspace not implemented".to_string(),
            ))
        })
    }

    /// Apply a partial update to a workspace (PROTOCOL §5.1).
    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let _ = (id, update);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_workspace not implemented".to_string(),
            ))
        })
    }

    /// Delete a workspace by id; `NotFound` if absent (PROTOCOL §5.1).
    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::delete_workspace not implemented".to_string(),
            ))
        })
    }

    /// Schedule a workspace deletion after a grace window (PROTOCOL §5.1):
    /// registers an in-memory pending deletion with deadline
    /// `now + undo_delay_ms` (clamped to the 60s cap) and returns the ISO
    /// `deleteAt` deadline. Re-scheduling while pending is idempotent (returns
    /// the existing deadline). On expiry the daemon runs the immediate-delete
    /// cascade ([`WorkspaceApi::delete_workspace`]). Never persisted — a
    /// daemon restart drops the pending deletion.
    fn schedule_workspace_delete(
        &self,
        id: WorkspaceId,
        undo_delay_ms: u64,
    ) -> BoxFuture<'_, Result<String>> {
        let _ = (id, undo_delay_ms);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::schedule_workspace_delete not implemented".to_string(),
            ))
        })
    }

    /// Cancel a pending workspace deletion (PROTOCOL §5.1). Returns `true`
    /// when a pending deletion was cancelled, `false` when nothing was
    /// pending (already committed, or never scheduled) — a non-error,
    /// race-safe outcome.
    fn cancel_workspace_delete(&self, id: WorkspaceId) -> BoxFuture<'_, Result<bool>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cancel_workspace_delete not implemented".to_string(),
            ))
        })
    }

    /// Archive a workspace (status→archived) (PROTOCOL §5.1).
    ///
    /// `caller_agent_id` names the agent that initiated the archive (the
    /// agent-facing `ws.workspace.archive` host: the MCP front door and the
    /// background-hook runtime); it is excluded from the graceful interrupt
    /// sweep so an agent archiving its own workspace is not interrupted
    /// mid-tool-call. The RPC front door (FE/iOS) passes `None`, which
    /// interrupts every in-flight turn in the workspace.
    fn archive_workspace(
        &self,
        id: WorkspaceId,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let _ = (id, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::archive_workspace not implemented".to_string(),
            ))
        })
    }

    /// Unarchive a workspace (status→active) (PROTOCOL §5.1).
    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::unarchive_workspace not implemented".to_string(),
            ))
        })
    }

    /// Clear the dismissible `attention` flag to `none` (PROTOCOL §5.1).
    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::dismiss_attention not implemented".to_string(),
            ))
        })
    }

    /// Mark the workspace seen, clearing an `unread` `attention` flag (§5.1).
    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mark_seen not implemented".to_string(),
            ))
        })
    }

    /// Publish an event onto the event bus (§10). The bindings layer can call
    /// this to emit app:*/workspace:*/note:* events that live subscribers will
    /// receive. When no bus is wired (test/minimal configs), this is a no-op.
    /// The default impl returns `Ok(())` so bindings can call it unconditionally.
    fn publish_event(&self, event: PublishEvent) -> BoxFuture<'_, Result<()>> {
        let _ = event;
        Box::pin(async { Ok(()) })
    }

    /// Read the durable token/credit usage snapshot for a workspace (§5.23). The
    /// scan job itself is daemon-internal (no RPC); this is the wire **read** and
    /// returns a default (empty, `lastScanAt: null`) snapshot before the first
    /// scan. `NotFound` if the workspace is absent (router maps it to `-32602`).
    fn get_token_usage(&self, id: WorkspaceId) -> BoxFuture<'_, Result<TokenUsage>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_token_usage not implemented".to_string(),
            ))
        })
    }

    /// Read the effective per-workspace auto-commit state (§5.1): the
    /// persisted workspace override when set (mirrored from the global
    /// `git.autoCommit` at create time), else the current global setting
    /// (pre-migration rows have no override). Returns
    /// `{ enabled, source: "workspace" | "global" }`. `NotFound` if the
    /// workspace is absent (router maps it to `-32602`).
    fn get_workspace_auto_commit(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_workspace_auto_commit not implemented".to_string(),
            ))
        })
    }

    /// Persist the per-workspace auto-commit override (§5.1) and emit a
    /// `workspace:updated` event carrying `{ autoCommitEnabled }` so live
    /// clients mirror the toggle. `NotFound` if the workspace is absent.
    fn set_workspace_auto_commit(
        &self,
        id: WorkspaceId,
        enabled: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, enabled);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::set_workspace_auto_commit not implemented".to_string(),
            ))
        })
    }

    /// Read the durable worktree setup script for a workspace (§5.25). Returns a
    /// default (empty `script`, `updatedAt: 0`) record before the first save.
    /// `NotFound` if the workspace is absent (router maps it to `-32602`).
    fn get_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_setup_script not implemented".to_string(),
            ))
        })
    }

    /// Persist a hand-written setup-script body and return the stored record with
    /// `generatedBy: "user"` and a fresh `updatedAt` (§5.25). `NotFound` if the
    /// workspace is absent.
    fn save_setup_script(
        &self,
        id: WorkspaceId,
        script: String,
    ) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = (id, script);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::save_setup_script not implemented".to_string(),
            ))
        })
    }

    /// Classify the workspace's project from manifest files (§5.25); `None` when
    /// no known manifest is found. `NotFound` if the workspace is absent.
    fn detect_project_type(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Option<ProjectType>>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::detect_project_type not implemented".to_string(),
            ))
        })
    }

    /// Produce an AI-assisted draft setup script (§5.25), returned (not persisted)
    /// with `generatedBy: "agent"`. `NotFound` if the workspace is absent.
    fn generate_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::generate_setup_script not implemented".to_string(),
            ))
        })
    }

    /// Read the repo config from `.intent/config.json` keyed on `workspaceId`.
    /// The server resolves the repository path from the workspace's worktree/repository
    /// fields (same resolution as `detectProjectType`). Returns an empty config when
    /// the file doesn't exist or is invalid (tolerant, never errors). `NotFound` if
    /// the workspace is absent (router maps to `-32602`).
    fn get_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<RepoConfig>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_repo_config not implemented".to_string(),
            ))
        })
    }

    /// Merge a partial config patch into `.intent/config.json` keyed on
    /// `workspaceId` (JSON-level field merge): keys **present** in `config`
    /// overwrite the on-disk values, keys **absent** are preserved, and an
    /// explicit `null` clears a field. Unknown keys still round-trip.
    /// Creates the `.intent/` directory and `.gitignore` if they don't exist.
    /// Never overwrites an existing `.gitignore`. Returns the merged config as
    /// written. `NotFound` if the workspace is absent.
    fn save_repo_config(
        &self,
        id: WorkspaceId,
        config: serde_json::Map<String, serde_json::Value>,
    ) -> BoxFuture<'_, Result<RepoConfig>> {
        let _ = (id, config);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::save_repo_config not implemented".to_string(),
            ))
        })
    }

    /// Check if a workspace has an `.intent/config.json` file.
    /// `NotFound` if the workspace is absent (router maps to `-32602`).
    fn has_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<bool>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::has_repo_config not implemented".to_string(),
            ))
        })
    }

    /// Ensure the `.intent/` directory exists with a proper `.gitignore`.
    /// Call this when initializing a workspace from a repo that doesn't have one yet.
    /// `NotFound` if the workspace is absent (router maps to `-32602`).
    fn ensure_repo_intent_dir(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::ensure_repo_intent_dir not implemented".to_string(),
            ))
        })
    }

    /// Read the workspace's chat-context attachment list (PROTOCOL §5.1). Returns
    /// an empty vec when nothing has been stored yet; `NotFound` if the workspace
    /// itself is absent (router maps to `-32602`).
    fn get_workspace_context(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Vec<ContextItem>>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_workspace_context not implemented".to_string(),
            ))
        })
    }

    /// Replace the workspace's chat-context attachment list atomically
    /// (PROTOCOL §5.1). Item order is preserved. Emits
    /// `workspace:context-changed` with the persisted list. `NotFound` if
    /// the workspace is absent.
    fn update_workspace_context(
        &self,
        id: WorkspaceId,
        items: Vec<ContextItem>,
    ) -> BoxFuture<'_, Result<Vec<ContextItem>>> {
        let _ = (id, items);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_workspace_context not implemented".to_string(),
            ))
        })
    }

    /// Read the workspace's UI context blob (PROTOCOL §5.1). Returns `None`
    /// when nothing has been stored yet (pre-first-save default); `NotFound`
    /// if the workspace itself is absent (router maps to `-32602`).
    fn get_workspace_ui_context(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Option<serde_json::Value>>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_workspace_ui_context not implemented".to_string(),
            ))
        })
    }

    /// Update the workspace's UI context blob (PROTOCOL §5.1). The daemon
    /// treats the payload as an opaque JSON blob authored by the FE; no
    /// interpretation, no shape coercion — byte-for-byte round-trip
    /// preservation is the correctness requirement. Returns the persisted blob
    /// read back from the store. `NotFound` if the workspace is absent.
    fn update_workspace_ui_context(
        &self,
        id: WorkspaceId,
        ui_context: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, ui_context);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_workspace_ui_context not implemented".to_string(),
            ))
        })
    }

    /// Upsert a task↔agent link (PROTOCOL §5.4). The caller-supplied
    /// `taskKey` mirrors the FE derivation (`association.taskKey ??
    /// association.taskText`); `createdAt` is set to the current epoch-ms.
    /// Existing rows at the same key are overwritten (FE parity with
    /// `addTaskAgentAssociation`). Emits `task:agent-linked` with the
    /// persisted row.
    fn link_task_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_key: String,
        task_text: String,
        agent_id: String,
    ) -> BoxFuture<'_, Result<TaskAgentLink>> {
        let _ = (workspace_id, note_id, task_key, task_text, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::link_task_agent not implemented".to_string(),
            ))
        })
    }

    /// Remove a task↔agent link (PROTOCOL §5.4). Returns whether the row
    /// was actually removed; deleting an unknown key is not an error.
    /// Emits `task:agent-unlinked` only when a row was actually removed.
    fn unlink_task_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_key: String,
    ) -> BoxFuture<'_, Result<bool>> {
        let _ = (workspace_id, note_id, task_key);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::unlink_task_agent not implemented".to_string(),
            ))
        })
    }

    /// List every task↔agent link for a workspace, oldest first (PROTOCOL
    /// §5.4). Hydration read for `task.listAgentLinks`; the router groups
    /// the flat vec into the FE-parity `byNoteId → byTaskKey` shape.
    fn list_task_agent_links(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Vec<TaskAgentLink>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_task_agent_links not implemented".to_string(),
            ))
        })
    }

    /// Duplicate a workspace (PROTOCOL §5.1): clone the persisted metadata into
    /// a freshly minted id, seed the well-known `spec` note, and copy over any
    /// non-`spec` notes from the source. Runtime-only fields (`activity`, card
    /// aggregates, timeline, changesets) are not carried over; the new
    /// workspace starts on a fresh branch derived from its id (uniquified with
    /// a `-N` suffix against the source repo's local/remote-tracking refs on
    /// collision, same as `workspace.create`). When the source carries a
    /// local `repositoryPath` and is not `skipWorktree`/`isRemote`, the daemon
    /// provisions a linked worktree at `<root>/<newId>/<repo-slug>` on that
    /// branch (mirroring the `workspace.create` flow); provisioning failures
    /// are logged and the duplicate still returns without a `worktreePath`
    /// (FE parity: "user can create it manually"). `newTitle` overrides the
    /// auto-suffixed `"<source> (Copy)"` title. `NotFound` if the source
    /// workspace is absent.
    fn duplicate_workspace(
        &self,
        id: WorkspaceId,
        new_title: Option<String>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let _ = (id, new_title);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::duplicate_workspace not implemented".to_string(),
            ))
        })
    }

    /// Restore an archived workspace to `active` (PROTOCOL §5.1). Alias of
    /// [`Self::unarchive_workspace`] with the same event emission; provided so
    /// clients can express intent (archive → restore) rather than the raw state
    /// transition. `NotFound` if the workspace is absent.
    fn restore_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        self.unarchive_workspace(id)
    }

    /// Best-effort per-workspace cleanup (PROTOCOL §5.1): reclaim the workspace
    /// cache directory and, when a local worktree exists, run `git gc` on it.
    /// Cache reclamation runs the recursive-delete under the daemon-owned
    /// `<workspaces_root>/<id>/cache/` path (never a caller-supplied path);
    /// both cache-removal and `git gc` failures are logged and swallowed so
    /// the workspace stays healthy. `NotFound` if the workspace is absent;
    /// otherwise this RPC always resolves `Ok(())`.
    fn cleanup_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cleanup_workspace not implemented".to_string(),
            ))
        })
    }

    /// Scan a directory for git repositories (PROTOCOL §5.1). Returns absolute
    /// paths (as strings) of every directory that contains a `.git` folder,
    /// walking a bounded depth to keep the scan cheap. Non-repo directories are
    /// recursed into; a git repo is emitted and its subtree is skipped.
    fn find_repositories(&self, directory: String) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = directory;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::find_repositories not implemented".to_string(),
            ))
        })
    }

    /// Initialize a new git repository at `path` (PROTOCOL §5.1): create the
    /// directory when missing, `git init -b main`, seed a `.gitignore` and
    /// `README.md`, and land an initial commit. When the target is already a
    /// git repository with at least one commit the call is a quiet no-op.
    /// Failures propagate as `Internal` errors.
    fn initialize_repository(&self, path: String) -> BoxFuture<'_, Result<()>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::initialize_repository not implemented".to_string(),
            ))
        })
    }

    /// List notes in a workspace (PROTOCOL §5.2).
    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_notes not implemented".to_string(),
            ))
        })
    }

    /// Fetch one note by id, scoped to the workspace (PROTOCOL §5.2).
    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_note not implemented".to_string(),
            ))
        })
    }

    /// Create a note from wire input (PROTOCOL §5.2).
    ///
    /// `idempotency_key` is the optional `params.idempotencyKey` (design note TB-0
    /// §5): when present and previously recorded, the original result is returned
    /// without re-executing; soft-launch when absent (warn + execute).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored (FE/RPC
    /// path). Mirrors the LC-1 `task.updateNoteStatus` provenance threading.
    ///
    /// Returns the created note (refetched after `@@@task` auto-conversion)
    /// plus the conversion outcome (`convertedCount`, `createdTaskNoteIds`,
    /// `createdTasks`, `warnings`), matching the four content-write results.
    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
        idempotency_key: Option<String>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteCreateResult>> {
        let _ = (workspace_id, input, idempotency_key, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_note not implemented".to_string(),
            ))
        })
    }

    /// CRUD `note.update`: raw content set, or title/tags metadata (PROTOCOL §5.2).
    fn update_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteUpdateInput,
    ) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, note_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_note not implemented".to_string(),
            ))
        })
    }

    /// `note.add`: append/prepend/insert content (PROTOCOL §5.2).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored.
    fn add_to_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let _ = (workspace_id, note_id, input, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::add_to_note not implemented".to_string(),
            ))
        })
    }

    /// `note.edit`: first exact-match replacement (PROTOCOL §5.2).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored.
    fn edit_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let _ = (workspace_id, note_id, input, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::edit_note not implemented".to_string(),
            ))
        })
    }

    /// `note.editLines`: 1-based inclusive line replace/delete/insert (PROTOCOL §5.2).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored.
    fn edit_note_lines(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let _ = (workspace_id, note_id, input, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::edit_note_lines not implemented".to_string(),
            ))
        })
    }

    /// `note.setContent`: full replace with the reduction guard (PROTOCOL §5.2).
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored.
    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
        expected_version: Option<i64>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let _ = (
            workspace_id,
            note_id,
            content,
            confirm_replacement,
            expected_version,
            caller_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::set_note_content not implemented".to_string(),
            ))
        })
    }

    /// `note.updateMetadata`: title/tags (spec title is skipped) (PROTOCOL §5.2).
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    ///
    /// `caller_agent_id` is accepted for uniformity with the other note-mutation
    /// methods; metadata-only writes do not push a version snapshot, so the
    /// hint is currently unused (parity with `notes.service.ts`).
    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
        expected_version: Option<i64>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        let _ = (
            workspace_id,
            note_id,
            title,
            tags,
            expected_version,
            caller_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_note_metadata not implemented".to_string(),
            ))
        })
    }

    /// `note.delete`: remove a note (PROTOCOL §5.2). `expected_version` gates the
    /// delete on the current `rev` when `Some` (§5.6); on a stale value the
    /// conflict carries the current entity snapshot prior to deletion.
    fn delete_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let _ = (workspace_id, note_id, expected_version);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::delete_note not implemented".to_string(),
            ))
        })
    }

    /// `note.listTasks`: parse checkbox rows from a note's content (PROTOCOL §5.2).
    fn list_note_tasks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_note_tasks not implemented".to_string(),
            ))
        })
    }

    /// `note.readAsset`: read an asset (id or `workspace-asset://` URL) (PROTOCOL §5.2).
    fn read_asset(
        &self,
        workspace_id: WorkspaceId,
        asset: String,
    ) -> BoxFuture<'_, Result<ReadAssetResult>> {
        let _ = (workspace_id, asset);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::read_asset not implemented".to_string(),
            ))
        })
    }

    /// `note.saveAsset`: write an image asset (base64 `data`, an optional
    /// `data:` URL prefix is stripped) under the workspace assets root and
    /// return `{ assetId, path, url }` (PROTOCOL §5.2 — additive asset write
    /// behind note image paste/upload).
    fn save_asset(
        &self,
        workspace_id: WorkspaceId,
        data: String,
        mime_type: String,
        original_name: Option<String>,
    ) -> BoxFuture<'_, Result<SaveAssetResult>> {
        let _ = (workspace_id, data, mime_type, original_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::save_asset not implemented".to_string(),
            ))
        })
    }

    /// `note.listVersions`: stored versions ascending by `v`, without content
    /// blobs (PROTOCOL §5.2 version-history extensions).
    fn list_note_versions(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteVersionSummary>>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_note_versions not implemented".to_string(),
            ))
        })
    }

    /// `note.getVersion`: one stored version with content (PROTOCOL §5.2
    /// version-history extensions).
    fn get_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
    ) -> BoxFuture<'_, Result<NoteVersion>> {
        let _ = (workspace_id, note_id, v);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_note_version not implemented".to_string(),
            ))
        })
    }

    /// `note.restoreVersion`: reset content to version `v` and append a new
    /// version capturing the restored state (PROTOCOL §5.2 version-history
    /// extensions).
    ///
    /// `caller_agent_id` attributes the captured note version to the invoking
    /// agent (the MCP front door passes it); `None` → user-authored.
    fn restore_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteRestoreVersionResult>> {
        let _ = (workspace_id, note_id, v, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::restore_note_version not implemented".to_string(),
            ))
        })
    }

    /// `note.lineAttribution.load`: return the most recently persisted
    /// per-line attribution snapshot for `note_id`, or `None` when the
    /// daemon has not yet computed one (PROTOCOL §5.2.1). Payload shape is
    /// FE-parity with what the `line-attribution:load` IPC handler served.
    fn line_attribution_load(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Option<LineAttributionData>>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::line_attribution_load not implemented".to_string(),
            ))
        })
    }

    /// `note.lineAttribution.computeNow`: force an immediate recompute of
    /// `note_id`’s attributions (PROTOCOL §5.2.1). Persists the fresh
    /// snapshot and emits `line-attribution:updated` before returning.
    fn line_attribution_compute_now(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<LineAttributionComputeResult>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::line_attribution_compute_now not implemented".to_string(),
            ))
        })
    }

    /// `task.updateStatus`: flip a checkbox by exact task text (PROTOCOL §5.4).
    fn task_update_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_text: String,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
        let _ = (workspace_id, note_id, task_text, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update_status not implemented".to_string(),
            ))
        })
    }

    /// `task.updateNoteStatus`: set task-note metadata status (PROTOCOL §5.4).
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    /// `caller_agent_id` attributes the change to the invoking agent (the MCP
    /// front door passes it): the emitted `task:status-changed` then carries an
    /// agent actor and an `agentId` payload field, mirroring the TS provenance
    /// (`notes.service.ts` `agentId: currentActor?.type === 'agent' ? … : undefined`).
    fn task_update_note_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        expected_version: Option<i64>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let _ = (
            workspace_id,
            note_id,
            status,
            expected_version,
            caller_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update_note_status not implemented".to_string(),
            ))
        })
    }

    /// `task.update`: atomic single-line edit with `expected` conflict check (§5.4).
    fn task_update(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        line: i64,
        text: Option<String>,
        status: Option<String>,
        expected: Option<String>,
    ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
        let _ = (workspace_id, note_id, line, text, status, expected);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update not implemented".to_string(),
            ))
        })
    }

    /// `task.getMyTask`: read a task note with subtasks + assignees (PROTOCOL §5.4).
    fn get_my_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
        let _ = (workspace_id, task_note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_my_task not implemented".to_string(),
            ))
        })
    }

    /// `task.list`: project ALL of a workspace's task notes (every note with
    /// task metadata except the spec itself — direct spec children, subtasks,
    /// and unlinked tasks alike, each flagged with `specLinked`) into the
    /// canonical `WorkspaceTask` list **plus** the workspace-wide `taskStats`
    /// aggregate (PROTOCOL §5.4). `status` optionally filters the task list to
    /// a single status; `stats` stays the unfiltered spec-linked direct-child
    /// rollup so the FE can render the progress rollup verbatim (mirrors the
    /// canonical FE `computeTaskStats` in `task-stats.ts`).
    fn task_list(
        &self,
        workspace_id: WorkspaceId,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskListResult>> {
        let _ = (workspace_id, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_list not implemented".to_string(),
            ))
        })
    }

    /// `task.get`: project a single task note into a `WorkspaceTask` (PROTOCOL
    /// §5.4). Errors with `NotFound` when the note is absent/cross-workspace and
    /// `Internal` when the note carries no task metadata.
    fn task_get(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<WorkspaceTask>> {
        let _ = (workspace_id, task_note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_get not implemented".to_string(),
            ))
        })
    }

    /// `task.markAsTask`: attach/replace task metadata on a note (PROTOCOL §5.4).
    ///
    /// `depends_on` / `conflicts_with` optionally seed the task's relations
    /// (validated + cycle/tree-checked like `task.setRelations`); `None`
    /// leaves any existing relations untouched.
    ///
    /// `caller_agent_id` attributes the resulting `task:created` /
    /// `task:status-changed` event to the invoking agent (the MCP front door
    /// passes it); `None` → system-attributed.
    #[allow(clippy::too_many_arguments)]
    fn mark_as_task(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        acceptance_criteria: Vec<String>,
        effort: Option<String>,
        depends_on: Option<Vec<NoteId>>,
        conflicts_with: Option<Vec<NoteId>>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskMarkAsTaskResult>> {
        let _ = (
            workspace_id,
            note_id,
            status,
            acceptance_criteria,
            effort,
            depends_on,
            conflicts_with,
            caller_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mark_as_task not implemented".to_string(),
            ))
        })
    }

    /// `task.setRelations`: replace a task's `dependsOn` / `conflictsWith`
    /// relation lists (PROTOCOL §5.4). `None` keeps the existing list;
    /// `Some(vec![])` clears it. Ids are validated (must be task notes in the
    /// same workspace, no self-edges) and `depends_on` writes that would close
    /// a dependency cycle — or name a tree ancestor/descendant of the task
    /// (monorepo#1982) — are rejected with the offending path/relationship
    /// named in the error.
    fn task_set_relations(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        depends_on: Option<Vec<NoteId>>,
        conflicts_with: Option<Vec<NoteId>>,
    ) -> BoxFuture<'_, Result<TaskSetRelationsResult>> {
        let _ = (workspace_id, note_id, depends_on, conflicts_with);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_set_relations not implemented".to_string(),
            ))
        })
    }

    /// `task.convertBlocks`: `@@@task` blocks → linked child task notes (§5.4).
    ///
    /// `caller_agent_id` attributes the resulting "Converted task blocks"
    /// version snapshot to the invoking agent (the MCP front door passes it);
    /// `None` → user-authored.
    fn convert_task_blocks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
        let _ = (workspace_id, note_id, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::convert_task_blocks not implemented".to_string(),
            ))
        })
    }

    /// `task.createPrerequisite`: create a child task note (PROTOCOL §5.4).
    ///
    /// `caller_agent_id` attributes the child's `task:created` event to the
    /// invoking agent (the MCP front door passes it); `None` → system.
    fn create_prerequisite(
        &self,
        workspace_id: WorkspaceId,
        dependent_note_id: NoteId,
        title: String,
        content: Option<String>,
        status: Option<String>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskCreatePrerequisiteResult>> {
        let _ = (
            workspace_id,
            dependent_note_id,
            title,
            content,
            status,
            caller_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_prerequisite not implemented".to_string(),
            ))
        })
    }

    /// `task.assignAgent`: append an agent to a task's assignee list (§5.4).
    /// Assigning a NEW agent to a task that already has a live assigned agent
    /// is rejected unless `force` is `Some(true)`; re-assigning an
    /// already-assigned id stays idempotent-ok.
    fn assign_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
        force: Option<bool>,
    ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
        let _ = (workspace_id, note_id, agent_id, force);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::assign_agent not implemented".to_string(),
            ))
        })
    }

    /// `task.removeAgentFromAllTasks` (§5.4 extension): strip `agent_id` from
    /// every task-note's `assignedAgentIds` in the workspace. Called from the
    /// agent teardown path (delete-agent, wake-or-create stale-assignment
    /// cleanup) so those callers do not need to enumerate tasks and issue
    /// per-note updates.
    fn remove_agent_from_all_tasks(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<TaskRemoveAgentFromAllTasksResult>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::remove_agent_from_all_tasks not implemented".to_string(),
            ))
        })
    }

    /// `agent.delegate`: delegate a task to a new agent (PROTOCOL §5.5). Part of
    /// the shared MCP surface agents call back into; the runtime wiring
    /// (spawn/ACP) lands in a later milestone, so the default returns an
    /// internal error and the result is the opaque service value.
    ///
    /// `parent_agent_id` is the caller/spawning agent: the MCP front door passes
    /// `Some(caller)` to stamp the child's `parentAgentId`; the FE/RPC front door
    /// passes `None` (top-level creates stay parentless).
    fn agent_delegate(
        &self,
        workspace_id: WorkspaceId,
        input: AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, input, parent_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_delegate not implemented".to_string(),
            ))
        })
    }

    /// `agent.list`: workspace agents as the stripped [`AgentLite`] projection
    /// (PROTOCOL §5.5). Excludes soft-retired sessions (`retiredAt` set) —
    /// the wire `includeRetired: true` variant is
    /// [`WorkspaceApi::agent_list_including_retired`].
    fn agent_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list not implemented".to_string(),
            ))
        })
    }

    /// `agent.list` with `includeRetired: true` (PROTOCOL §5.5): every
    /// session including soft-retired ones, whose rows carry `retiredAt`.
    fn agent_list_including_retired(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list_including_retired not implemented".to_string(),
            ))
        })
    }

    /// `agent.list` with `retiredOnly: true` (PROTOCOL §5.5): ONLY
    /// soft-retired sessions, whose rows carry `retiredAt`.
    fn agent_list_retired_only(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list_retired_only not implemented".to_string(),
            ))
        })
    }

    /// Number of soft-retired sessions in a workspace — the `retiredCount`
    /// field attached to every `agent.list` response variant (PROTOCOL §5.5).
    fn agent_retired_count(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<u64>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_retired_count not implemented".to_string(),
            ))
        })
    }

    /// `agent.listActive`: daemon-global mid-turn agent streams (PROTOCOL
    /// §5.5). No workspace id is required because the result spans workspaces.
    fn agent_list_active(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list_active not implemented".to_string(),
            ))
        })
    }

    /// `agent.get`: one agent as [`AgentLite`]; `NotFound` falls back to disk
    /// then surfaces `-32602` (PROTOCOL §5.5).
    fn agent_get(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentLite>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get not implemented".to_string(),
            ))
        })
    }

    /// `agent.getConversation`: `{ agentId, messages, truncated, totalMessages }`
    /// capped to the most-recent `limit` (PROTOCOL §5.5). The optional
    /// `around_message_id` seeks to the page containing that message instead
    /// of the newest window; `around_index` seeks to the page containing that
    /// 0-based ordinal (from the oldest message), clamped into
    /// `[0, totalMessages - 1]`. The two seek params are mutually exclusive
    /// (enforced at the transport boundary). The optional `projection`
    /// requests bounded tool/image block bodies
    /// ([`crate::ConversationProjection`]); absent all optional params,
    /// behavior is byte-identical to before.
    #[allow(clippy::too_many_arguments)]
    fn agent_get_conversation(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        workspace_id: Option<WorkspaceId>,
        page_token: Option<String>,
        around_message_id: Option<String>,
        around_index: Option<i64>,
        projection: Option<crate::ConversationProjection>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            agent_id,
            limit,
            workspace_id,
            page_token,
            around_message_id,
            around_index,
            projection,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_conversation not implemented".to_string(),
            ))
        })
    }

    /// `agent.getMessageBlock`: one full content block of one persisted
    /// message, by block id — `{ block }` (PROTOCOL §5.5). The on-demand
    /// counterpart of the `projection: "slim"` conversation read: a client
    /// holding a truncated block fetches the full body here. Block ids are
    /// the served identity — persisted assistant ids and the serve-time
    /// synthetic `{messageId}:{index}` ids both resolve. Bounded cost: one
    /// primary-key message row read, never a transcript hydration.
    /// `NotFound` on an unknown agent or a workspace mismatch;
    /// `InvalidParams` on an unknown message or block id.
    fn agent_get_message_block(
        &self,
        agent_id: AgentId,
        message_id: String,
        block_id: String,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id, block_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_message_block not implemented".to_string(),
            ))
        })
    }

    /// `agent.listUserMessages`: all user-role messages of an agent as
    /// lightweight index items, oldest→newest —
    /// `{ agentId, items: [{ id, preview, createdAt, metadata? }], total }`
    /// (PROTOCOL §5.5). `preview` is the extracted plain text bounded to
    /// `preview_chars` characters (`None` → server default, server-clamped);
    /// `metadata` is the persisted row metadata verbatim when present.
    /// Non-user rows are never included. Bounded cost: one role-filtered
    /// SQL read whose previews are extracted and truncated inside SQL —
    /// full content blobs never leave the database and the transcript is
    /// never hydrated. `NotFound` on an unknown agent or a workspace
    /// mismatch.
    fn agent_list_user_messages(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        preview_chars: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id, preview_chars);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list_user_messages not implemented".to_string(),
            ))
        })
    }

    /// `agent.getSession`: full [`AgentSession`] projection including
    /// `systemPrompt`, `specialist`, and the persisted metadata block —
    /// the superset that `agent.get`/`AgentLite` strips (PROTOCOL §5.5).
    /// `messages` is loaded from the append-only log (chronological order).
    /// Used by the FE-side agent-backend-handler retirement to rehydrate the
    /// full `AgentSession` shape from the daemon (C1d/C1e). `NotFound` when
    /// the session is unknown.
    fn agent_get_session(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentSession>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_session not implemented".to_string(),
            ))
        })
    }

    /// `agent.update`: partial update of the persisted [`AgentSession`] from
    /// a `changes` object (PROTOCOL §5.5). Only the listed fields are touched;
    /// omitted fields are preserved. Write-once (`acpSessionId`) and immutable
    /// (`provider`) invariants are enforced by the store. Emits `agent:updated`
    /// (or `agent:renamed` when `name` is the only mutated field). Returns
    /// `{ success: true, agent: AgentLite }` on success. `NotFound` when the
    /// session is unknown; `InvalidParams` when `changes` carries an unknown
    /// field or a malformed value.
    fn agent_update(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        changes: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id, changes);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_update not implemented".to_string(),
            ))
        })
    }

    /// `agent.appendMessage`: append a raw [`AgentMessage`] to the transcript
    /// (PROTOCOL §5.5). Used by the FE for wake-message insert and the
    /// `saveMessage` renderer path — mutations that carry the persisted role
    /// and pre-composed `contentBlocks` verbatim. Rejected with
    /// `InvalidParams` when the agent is mid-turn (message-log mutation must
    /// not race the daemon's streaming writer). Returns `{ success: true,
    /// message: AgentMessage }` on success. Emits `agent:message`.
    fn agent_append_message(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        role: String,
        content: serde_json::Value,
        metadata: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id, role, content, metadata);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_append_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.replaceMessages`: atomically swap the entire transcript with
    /// `messages` (PROTOCOL §5.5). Used by the FE's edit-truncate path so a
    /// re-generated turn does not leave orphaned rows. Rejected with
    /// `InvalidParams` when the agent is mid-turn (same rationale as
    /// [`WorkspaceApi::agent_append_message`]). Returns `{ success: true,
    /// messages: AgentMessage[] }` with the freshly-minted ids/`seq`s.
    fn agent_replace_messages(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        messages: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id, messages);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_replace_messages not implemented".to_string(),
            ))
        })
    }

    /// The agent's in-flight ("live") turn, if a `session/prompt` is currently
    /// streaming (CS-0 D5): the partial assistant message as `{ messageId,
    /// contentBlocks }` so `chat.subscribe` can merge it into the seq-0 snapshot
    /// and a client arriving mid-turn sees a coherent in-flight message. A
    /// synchronous in-memory read (no I/O). The default returns `None` (no live
    /// turn surfaced) so non-agent `WorkspaceApi` impls need not implement it.
    fn agent_live_turn(&self, agent_id: AgentId) -> Option<serde_json::Value> {
        let _ = agent_id;
        None
    }

    /// Whether a turn loop is currently in flight for `agent_id` — the
    /// authoritative "active worker" signal backing the chat snapshot's live
    /// merge gate. `chat.subscribe` consults this before merging the in-memory
    /// `agent_live_turn` so a lingering live-turn slot with no real worker (or
    /// a session that never finalized across a crash) does not surface a
    /// phantom streaming message. Synchronous (no I/O); default `false` so
    /// non-agent `WorkspaceApi` impls need not implement it.
    fn agent_is_busy(&self, agent_id: AgentId) -> bool {
        let _ = agent_id;
        false
    }

    /// The daemon-owned runtime activity flags for `agent_id` as the object
    /// `{ isResponding, isWaitingOnTool, isWaitingForOtherAgents, waitingForAgentIds }`
    /// (PROTOCOL §5.5/§7.1): the BE-authoritative port of the FE agent-state
    /// selectors so `chat.subscribe`'s seq-0 snapshot carries the same liveness
    /// signal as the `AgentLite` projection. `isResponding` is the in-flight
    /// "active worker" signal ([`agent_is_busy`](WorkspaceApi::agent_is_busy));
    /// `isWaitingOnTool` is true when that turn has an unresolved `tool_use`;
    /// `isWaitingForOtherAgents` is true when the agent parents one or more
    /// pending completion watches; `waitingForAgentIds` is the distinct child
    /// agent-ids of those watches (non-empty iff `isWaitingForOtherAgents`,
    /// always emitted as `[]` otherwise — never null/omitted). Default returns
    /// all-`false`/`[]` so non-agent `WorkspaceApi` impls need not implement it.
    fn agent_activity_flags(&self, agent_id: AgentId) -> BoxFuture<'_, serde_json::Value> {
        let _ = agent_id;
        Box::pin(async {
            serde_json::json!({
                "isResponding": false,
                "isWaitingOnTool": false,
                "isWaitingForOtherAgents": false,
                "waitingForAgentIds": [],
            })
        })
    }

    /// `agent.create`: persist a new agent session; returns
    /// `{ agent: <AgentLite> }` (the full projection — a superset of the earlier
    /// `{ id, name }` shape, so existing readers stay green) (PROTOCOL §5.5).
    ///
    /// `parent_agent_id` is the caller/spawning agent: the MCP front door passes
    /// `Some(caller)` to stamp the child's `parentAgentId`; the FE/RPC front door
    /// passes `None` (top-level creates stay parentless).
    ///
    /// Agent ids are server-assigned: the daemon always mints a fresh
    /// `agent-{uuid}` id and returns it in the `AgentLite` projection.
    /// Requests still carrying a client-supplied `agentId` are rejected at
    /// the transport boundary as `-32602`.
    ///
    /// `extra` carries the widened FE-facing spawn hints
    /// (`provider`/`agentType`/`metadata`/`workspacePath`/`workspaceContext`).
    /// Only `provider` currently lands on the persisted session; the other
    /// fields are accepted so the FE seam can bind to the wire shape ahead of
    /// full persistence.
    #[allow(clippy::too_many_arguments)]
    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist_id: Option<String>,
        parent_agent_id: Option<AgentId>,
        idempotency_key: Option<String>,
        extra: crate::model::AgentCreateExtra,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            name,
            model,
            specialist_id,
            parent_agent_id,
            idempotency_key,
            extra,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_create not implemented".to_string(),
            ))
        })
    }

    /// `agent.sendToTask`: follow up with the agent assigned to a task note
    /// (PROTOCOL §5.5). `message_metadata` is the same opaque per-message
    /// payload as `agent.sendMessage` (persisted on the user row; e.g. the
    /// `agent_message` sender-attribution block for agent-to-agent sends).
    fn agent_send_to_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        priority: Option<String>,
        message_metadata: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            task_note_id,
            message,
            priority,
            message_metadata,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_send_to_task not implemented".to_string(),
            ))
        })
    }

    /// `agent.sendMessage`: deliver a user message, auto-queuing when the agent
    /// is mid-stream; `{ success, queued, messageId? }` (PROTOCOL §5.5).
    /// `priority: "interrupt"` preempts an in-flight turn instead of queueing:
    /// the current turn is cancelled keep-alive (the agent process is never
    /// killed) and the message is delivered immediately as a fresh turn.
    ///
    /// `image_blocks` / `file_blocks` are FE-supplied attachment arrays that
    /// reach the agent as ACP content blocks appended after the text prompt
    /// (reference-parity `acp-provider.ts`); a queued message preserves them
    /// so the drained turn carries the same blocks.
    ///
    /// `note_ids` / `stdin_context` / `context_references` are the FE-side
    /// per-turn prompt-assembly hints (PROTOCOL §5.5). `stdin_context` is
    /// prepended verbatim to the outbound prompt as a `Context:` block
    /// (reference-parity `acp-provider.ts`); the other two are threaded
    /// through to the prompt builder for downstream note-image /
    /// context-reference resolution.
    ///
    /// `origin` marks who originated the delivery (question hold, PROTOCOL
    /// §5.5): the FE RPC front door passes [`MessageOrigin::User`] (never
    /// held); the MCP front door and every internal wake/continuation path
    /// pass [`MessageOrigin::Automatic`], which enqueues instead of starting
    /// a turn while the target agent's question hold is active.
    #[allow(clippy::too_many_arguments)]
    fn agent_send_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        content: String,
        message_id: Option<String>,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
        priority: Option<String>,
        note_ids: Option<serde_json::Value>,
        stdin_context: Option<String>,
        context_references: Option<serde_json::Value>,
        message_metadata: Option<serde_json::Value>,
        origin: MessageOrigin,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            agent_id,
            content,
            message_id,
            image_blocks,
            file_blocks,
            priority,
            note_ids,
            stdin_context,
            context_references,
            message_metadata,
            origin,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_send_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.sendQueuedMessageNow`: atomically dequeue the queued entry
    /// named by `message_id` and deliver it immediately with interrupt
    /// priority, preserving the rest of the queue (PROTOCOL §5.5). An absent
    /// entry is `-32602` ("queued message not found") with NO side effects —
    /// deliberately NOT idempotent (unlike `agent.removeQueuedMessage`), so
    /// the client knows the atomic send did not happen.
    fn agent_send_queued_message_now(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, message_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_send_queued_message_now not implemented".to_string(),
            ))
        })
    }

    /// `agent.dismissQuestions`: persist the question-dismissal marker
    /// (`message_id` — the assistant message whose trailing question resource
    /// blocks the user dismissed) on the agent session, emit `agent:updated`,
    /// and kick the queue drain so messages held by the question hold resume
    /// (PROTOCOL §5.5). Idempotent: re-dismissing the same message succeeds.
    fn agent_dismiss_questions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, message_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_dismiss_questions not implemented".to_string(),
            ))
        })
    }

    /// `agent.markSeen`: persist the per-conversation seen marker
    /// (`message_id` — the newest transcript message the user has seen) on
    /// the agent session and emit `agent:updated` (PROTOCOL §5.5). Monotonic:
    /// naming a message OLDER than the current marker is a no-op returning
    /// the current marker. Idempotent: re-marking the same message succeeds.
    fn agent_mark_seen(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, message_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_mark_seen not implemented".to_string(),
            ))
        })
    }

    /// `agent.editAndRegenerate`: edit a past user message and regenerate from
    /// that point (PROTOCOL §5.5). Stops any in-flight turn, truncates the
    /// transcript to just before `message_id` (which must reference an existing
    /// **user** message — otherwise `-32602`), forces a fresh ACP session on
    /// the next prompt (the truncated history replays as `<supervisor>` XML so
    /// the provider does not retain the truncated turns), then sends `content`
    /// as a fresh user message with the same per-turn semantics as
    /// `agent.sendMessage`.
    #[allow(clippy::too_many_arguments)]
    fn agent_edit_and_regenerate(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
        content: String,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
        model: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            agent_id,
            message_id,
            content,
            image_blocks,
            file_blocks,
            model,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_edit_and_regenerate not implemented".to_string(),
            ))
        })
    }

    /// `agent.queueMessage`: explicitly enqueue a message; `{ success,
    /// queuedMessage }` where `queuedMessage` is `{ id, content, queuedAt,
    /// position, imageBlocks?, fileBlocks? }` (PROTOCOL §5.5). Attachment
    /// arrays are preserved on the queued entry so the drained turn carries
    /// the same blocks.
    fn agent_queue_message(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, content, image_blocks, file_blocks);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_queue_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.editQueuedMessage`: edit a queued message's content (PROTOCOL §5.5).
    /// `editing` (optional) toggles the entry's under-edit state — when `Some(true)`
    /// the entry is excluded from the ready-to-send queue (drain skips it); when
    /// `Some(false)` it is re-included and self-drains; when `None` the editing
    /// flag is left unchanged (backwards-compatible with the original wire shape).
    fn agent_edit_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
        editing: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id, content, editing);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_edit_queued_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.removeQueuedMessage`: remove a queued message (PROTOCOL §5.5).
    fn agent_remove_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_remove_queued_message not implemented".to_string(),
            ))
        })
    }

    /// Ownership-checked removal for the MCP `ws.agent.removeQueuedMessage`
    /// binding (PROTOCOL §6.8): removes the entry ONLY when its
    /// `messageMetadata.fromAgentId` equals `caller_agent_id` — an agent may
    /// retract its own pending sends but never another sender's (or the
    /// user's). Unlike the idempotent FE `agent.removeQueuedMessage`, an
    /// unknown message id is an error. Removal republishes
    /// `agent:queue:updated` and persists (same path as the FE RPC).
    fn agent_remove_queued_message_owned(
        &self,
        agent_id: AgentId,
        message_id: String,
        caller_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_remove_queued_message_owned not implemented".to_string(),
            ))
        })
    }

    /// `agent.getQueue`: the agent's pending message queue; `{ success, queue:
    /// [{ id, content, queuedAt, position, imageBlocks?, fileBlocks? }] }` (PROTOCOL §5.5).
    /// When `workspace_id` is supplied the callee verifies the session belongs
    /// to that workspace (defense-in-depth against a bare `agentId` probe).
    fn agent_get_queue(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_queue not implemented".to_string(),
            ))
        })
    }

    /// `agent.stop`: cancel an in-flight stream; `{ success: true }` (PROTOCOL §5.5).
    fn agent_stop(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_stop not implemented".to_string(),
            ))
        })
    }

    /// `agent.retry`: redrive a failed agent spawn — only valid when the agent
    /// status is `error`; returns `{ ok: false }` otherwise. Clears the error
    /// status back to pending, tears down any stale child, and attempts to
    /// redrive the front-of-queue message (requeued at exhaustion) plus any
    /// subsequent messages. Reuses the spawn-retry/backoff machinery, so a
    /// retry that fails again lands back in the `error` state with events
    /// (new in intentd — not part of the ported 104).
    fn agent_retry(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_retry not implemented".to_string(),
            ))
        })
    }

    /// `agent.setModel`: change an agent's model (PROTOCOL §5.5).
    /// `provider_id` optionally names the intended provider explicitly
    /// (additive param): absent keeps the historical behavior; present it
    /// must be a registered provider, must agree with a compound
    /// `model_id`'s prefix, and owns the validation of a bare `model_id`
    /// (session.provider is reconciled to it on success).
    fn agent_set_model(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        model_id: String,
        provider_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, model_id, provider_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_set_model not implemented".to_string(),
            ))
        })
    }

    /// `agent.getModels`: `{ models: [{ id, name, provider, description? }] }`
    /// from the auggie CLI, degrading to an empty list when the CLI is
    /// unavailable; no `workspaceId` (PROTOCOL §5.5).
    fn agent_get_models(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_models not implemented".to_string(),
            ))
        })
    }

    /// `models.list`: the rich model catalog for FE model pickers; no
    /// `workspaceId` (PROTOCOL §5.30). Without `provider_id` this is the
    /// backward-compatible auggie path —
    /// `{ models: [ModelInfo…], source: "auggie" | "static" }` from
    /// `auggie model list --json` (plain-text fallback), degrading to an
    /// empty `source: "static"` list when the CLI is unavailable — except
    /// that `force_refresh` with
    /// a failed probe may serve the last-good cached list with `stale`/
    /// `warning` fields added. With a `provider_id` the catalog comes from
    /// that provider's registered source through the generic per-provider
    /// cache (version-keyed, served indefinitely; a probe runs only on a
    /// miss or forced read), returning
    /// `{ providerId, models, source, stale?, warning? }`. `force_refresh`
    /// skips the cache read and awaits a fresh probe.
    fn models_list(
        &self,
        provider_id: Option<String>,
        force_refresh: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (provider_id, force_refresh);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::models_list not implemented".to_string(),
            ))
        })
    }

    /// `stats.getUsage`: the global usage-stats read behind the agentic
    /// usage-stats cards, aggregated over the `usage_stats_hourly` store; no
    /// `workspaceId`. `period` is `"24h"` / `"month"` / `"year"` (`key` —
    /// `"YYYY-MM"` / `"YYYY"` — is required for month/year and ignored for
    /// 24h); `tz_offset_minutes` is the client's offset east of UTC, applied
    /// to the UTC hour buckets before period filtering and hour-of-day /
    /// month grouping so results reflect the client's local time.
    fn stats_get_usage(
        &self,
        period: String,
        key: Option<String>,
        tz_offset_minutes: i64,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (period, key, tz_offset_minutes);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::stats_get_usage not implemented".to_string(),
            ))
        })
    }

    /// `stats.getRateHistory`: the global per-minute token-rate history
    /// behind the HUD TOK/MIN chart, read from the capped
    /// `usage_rate_minutely` store; no `workspaceId`. Returns the trailing
    /// `limit` minute samples (default 60, max 1440) ending at the current
    /// UTC minute, zero-filled and in chronological order.
    fn stats_get_rate_history(
        &self,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::stats_get_rate_history not implemented".to_string(),
            ))
        })
    }

    /// `agent.enhancePrompt`: one-shot prompt-enhance / AI-layout generation via
    /// the auggie CLI — `{ enhanced, original, mode }`; `mode` is `"enhance"` or
    /// `"layout"`, `workspaceId` optionally pins the CLI's cwd (PROTOCOL §5.31).
    fn agent_enhance_prompt(
        &self,
        prompt: String,
        mode: String,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (prompt, mode, model, workspace_id, timeout_ms);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_enhance_prompt not implemented".to_string(),
            ))
        })
    }

    /// `agent.completeOnce`: stateless one-shot prompt→completion via the
    /// auggie CLI — `{ text }`. Optional `system_prompt` is composed with
    /// `prompt` before dispatch; `workspace_id` optionally pins the CLI's cwd
    /// (PROTOCOL §5.32). Daemon owns the full lifecycle including reap on
    /// timeout/failure; no session or agent state is created.
    ///
    /// `quick_action_type` is the optional quick-action `type` hint keying
    /// `quickActions.typeOverrides`; with no explicit `model` the daemon
    /// resolves that override then `quickActions.defaultModel` before the
    /// provider CLI default (monorepo#1734).
    fn agent_complete_once(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        quick_action_type: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            prompt,
            system_prompt,
            model,
            quick_action_type,
            workspace_id,
            timeout_ms,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_complete_once not implemented".to_string(),
            ))
        })
    }

    /// `agent.respondPermission`: resolve an outstanding interactive permission
    /// prompt by `requestId`, unblocking the agent with the §8 `outcome`
    /// (`{ outcome: "selected", optionId }` / `{ outcome: "cancelled" }`).
    /// `{ resolved: bool }` (`false` when no such pending prompt) (PROTOCOL §8).
    fn agent_respond_permission(
        &self,
        request_id: String,
        outcome: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (request_id, outcome);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_respond_permission not implemented".to_string(),
            ))
        })
    }

    /// `agent.pendingPermissions`: the outstanding interactive permission prompts
    /// as `{ requests: [PermissionRequestData…] }`, optionally filtered to one
    /// agent by `agentId` (= `sessionId`) (PROTOCOL §8).
    fn agent_pending_permissions(
        &self,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_pending_permissions not implemented".to_string(),
            ))
        })
    }

    /// `agent.rename`: rename an agent; `{ success: true, name }` (PROTOCOL §5.5).
    /// With `skip_if_explicitly_set = true`, an already-explicitly-named session
    /// is left untouched and the result carries `skipped: true` (P3-1.2b).
    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, name, skip_if_explicitly_set);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_rename not implemented".to_string(),
            ))
        })
    }

    /// `agent.delete`: delete an agent session; `{ success: true }` (PROTOCOL §5.5).
    fn agent_delete(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_delete not implemented".to_string(),
            ))
        })
    }

    /// Whether `agent_id`'s session is soft-retired (`retiredAt` set) — the
    /// cheap point read backing the MCP dispatch guard that keeps a caller
    /// inert for the remainder of the turn that retired it (retirement lands
    /// mid-turn; the ACP stream only stops at the turn boundary). Missing
    /// sessions and read errors report `false`: absence is handled by the
    /// per-method `require_*` guards, and a transient store error must not
    /// blanket-deny every `workspace_api` call. Default `false` so non-agent
    /// `WorkspaceApi` impls need not implement it.
    fn agent_is_retired(&self, agent_id: AgentId) -> BoxFuture<'_, bool> {
        let _ = agent_id;
        Box::pin(async { false })
    }

    /// Soft retire (`ws.agent.retire`): set `retiredAt` on the session,
    /// keeping the row and its full conversation intact. The retired session
    /// is inert until `agent.restore` clears the mark. Emits `agent:retired`;
    /// idempotent on an already-retired session.
    fn agent_retire(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        reason: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id, reason);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_retire not implemented".to_string(),
            ))
        })
    }

    /// `agent.restore` (PROTOCOL §5.5): clear a session's `retiredAt`,
    /// returning it to normal service. Restoring a non-retired session is a
    /// no-op (`{ success: true, restored: false }`); a real restore emits
    /// `agent:restored`.
    fn agent_restore(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_restore not implemented".to_string(),
            ))
        })
    }

    /// Schedule an agent-session deletion after a grace window (PROTOCOL
    /// §5.5): registers an in-memory pending deletion with deadline
    /// `now + undo_delay_ms` (clamped to the 60s cap) and returns the ISO
    /// `deleteAt` deadline. Scheduling does NOT stop the agent — the commit
    /// performs the ordinary [`WorkspaceApi::agent_delete`] (which does).
    /// Re-scheduling while pending is idempotent (returns the existing
    /// deadline). Never persisted — a daemon restart drops the pending
    /// deletion.
    fn agent_schedule_delete(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        undo_delay_ms: u64,
    ) -> BoxFuture<'_, Result<String>> {
        let _ = (agent_id, workspace_id, undo_delay_ms);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_schedule_delete not implemented".to_string(),
            ))
        })
    }

    /// `agent.cancelDelete`: cancel a pending agent-session deletion
    /// (PROTOCOL §5.5). Returns `true` when a pending deletion was cancelled,
    /// `false` when nothing was pending (already committed, or never
    /// scheduled) — a non-error, race-safe outcome.
    fn agent_cancel_delete(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<bool>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_cancel_delete not implemented".to_string(),
            ))
        })
    }

    /// `agent.wakeOrCreate` (PROTOCOL §5.5, widened by C1d-10a): resume the
    /// newest live/resumable agent assigned to the task, or create + assign a
    /// new one — inheriting specialist/model from the most-recent previous
    /// session and honoring the FE `WakeOrCreateTaskAgentTool` create payload
    /// (name/contextReferences/metadata/skipAutoCommit) — then deliver the
    /// context message (optionally tagged with `input.messageMetadata`).
    /// `input.callerAgentId`/`input.delegationDepth` gate the
    /// delegation-depth guard. All fields on [`crate::AgentWakeOrCreateInput`]
    /// are optional so the pre-widening 3-required-params callers stay green.
    fn agent_wake_or_create(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        input: crate::model::AgentWakeOrCreateInput,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, task_note_id, context_message, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_wake_or_create not implemented".to_string(),
            ))
        })
    }

    /// `agent.summary`: a quick summary of what the agent did (PROTOCOL §5.5).
    fn agent_summary(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_summary not implemented".to_string(),
            ))
        })
    }

    /// `agent.getSessionStats`: the per-session credit/message/tool rollup as
    /// `{ stats: SessionStats }` (PROTOCOL §5.24). `sessionId` is required; an
    /// unknown session surfaces `NotFound` which the router maps to `-32602`.
    /// When `workspace_id` is supplied the callee verifies the session belongs
    /// to that workspace (defense-in-depth against a bare `sessionId` probe).
    fn agent_get_session_stats(
        &self,
        session_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (session_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_session_stats not implemented".to_string(),
            ))
        })
    }

    /// `agent.diagnostics`: a sanitized snapshot of agent statuses,
    /// subscriptions, delegation groups, and stuck-risk signals as
    /// `{ ok, diagnostics, text }` (PROTOCOL §5.5). Optional `agent_id` /
    /// `task_note_id` focus the snapshot; `stale_responding_after_ms` tunes the
    /// stale-responding threshold (default 600000).
    fn agent_diagnostics(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        stale_responding_after_ms: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            agent_id,
            task_note_id,
            stale_responding_after_ms,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_diagnostics not implemented".to_string(),
            ))
        })
    }

    /// `ws.agent.snapshot` (MCP-only, PROTOCOL §7.1): the calling agent's own
    /// compact state digest — active hooks, completion watches, queued
    /// messages, event subscriptions, actively running (in-flight) children,
    /// pending structured questions, pending attention request, current UTC
    /// time. Zero-count and
    /// null fields are omitted from the returned object; `time` is always
    /// present. Never gated by `agentFeatures.stateSnapshot` (the toggle
    /// governs only the per-turn prompt injection). A workspace mismatch
    /// surfaces `NotFound` (defense-in-depth against bare-id probes).
    fn agent_snapshot(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_snapshot not implemented".to_string(),
            ))
        })
    }

    /// `agent.listInterrupted`: list pending interrupted agents (INT-41,
    /// agent-resumption phase 1). Returns joined data: agent ID, workspace info,
    /// agent name, prev status, interrupted timestamp. Sessions deleted since
    /// interruption are excluded. (PROTOCOL §5.5).
    fn agent_list_interrupted(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list_interrupted not implemented".to_string(),
            ))
        })
    }

    /// `agent.resolveInterrupted`: resume or abandon interrupted agents (INT-41,
    /// agent-resumption phase 2). Resume: mark row `resumed`, re-register parent
    /// completion watch if delegated, deliver continuation message. Abandon: mark
    /// row `abandoned`, append system interruption message. Returns
    /// `{ resumed: string[], abandoned: string[], failed: [{ agentId, error }] }`.
    /// Ids must be pending `interrupted_agent` rows; unknown/already-resolved ids
    /// land in `failed`. An id in both lists is `-32602`. (PROTOCOL §5.5).
    fn agent_resolve_interrupted(
        &self,
        resume: Option<Vec<String>>,
        abandon: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            let _ = (resume, abandon);
            Err(Error::Internal(
                "WorkspaceApi::agent_resolve_interrupted not implemented".to_string(),
            ))
        })
    }

    /// `agent.reportToParent`: child→parent report; `-32603` when the caller is
    /// not a delegated agent (PROTOCOL §5.5).
    ///
    /// `caller_agent_id` is the agent invoking the tool: the MCP front door
    /// passes `Some(caller)` so the impl can resolve the caller's
    /// `parentAgentId`; the FE/RPC front door passes `None`, which always
    /// surfaces `-32603` (there is no caller context to be a delegated agent).
    fn agent_report_to_parent(
        &self,
        workspace_id: WorkspaceId,
        report: serde_json::Value,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, report, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_report_to_parent not implemented".to_string(),
            ))
        })
    }

    /// `agent.requestAttention`: an agent explicitly raises attention before
    /// ending its turn — `kind` is `"discussion"` (`ws.agent.requestDiscussion`)
    /// or `"blocker"` (`ws.agent.reportBlocker`), `reason` is required.
    /// Persists the pending request on the session, appends a system-role
    /// transcript notice, emits `agent:attention-requested`, transitions the
    /// linked task (`discussion_needed` / `blocked`), and wakes a delegated
    /// caller's parent. Available to all agents (delegated or not, with or
    /// without a linked task).
    ///
    /// `caller_agent_id` is the agent invoking the tool: the MCP front door
    /// passes `Some(caller)`; the FE/RPC front door passes `None`, which
    /// surfaces `-32603` (there is no caller session to attach the request to).
    fn agent_request_attention(
        &self,
        workspace_id: WorkspaceId,
        kind: String,
        reason: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, kind, reason, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_request_attention not implemented".to_string(),
            ))
        })
    }

    /// `agent.getSubscriptions`: `{ subscriptions, delegationGroups, agentStatuses }`
    /// (PROTOCOL §5.5).
    fn agent_get_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_subscriptions not implemented".to_string(),
            ))
        })
    }

    /// Auto-subscribe a parent agent to a child's completion: register a
    /// parent→child completion watch (the TS
    /// `subscribeCallerToAgentCompletion`). Called by the MCP `create_agent`
    /// front door after the child session exists and before its first turn
    /// starts. Returns `{ ok, subscriptionId }`; `ok: false` (no watch) when
    /// the parent session is deleted.
    fn agent_watch_completion(
        &self,
        workspace_id: WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, parent_agent_id, child_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_watch_completion not implemented".to_string(),
            ))
        })
    }

    /// Conditionally auto-subscribe a coordination-message SENDER to the
    /// target agent's completion (SUB-1, the TS
    /// `maybeSubscribeCallerToAgentCompletionForCoordinationMessage`). Called
    /// by the MCP `send_message_to_agent` / `send_message_to_task_agent`
    /// front doors after delivery. Foreground/coordinator senders get a
    /// completion watch; delegated background task senders are skipped (their
    /// sibling coordination messages would otherwise create noisy wakeups).
    /// Returns `{ ok, subscriptionId }`; `ok: false` (null id) when skipped.
    fn agent_watch_completion_for_sender(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, caller_agent_id, target_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_watch_completion_for_sender not implemented".to_string(),
            ))
        })
    }

    /// `agent.watch` (the `ws.agent.watch(agentId)` MCP binding,
    /// monorepo#1229): explicit caller→target subscription to the
    /// target's harness-curated completion set — idle/completed, failed,
    /// deleted, blocker raised, discussion requested. Unlike the
    /// auto-registered watches this watch also wakes on the target's
    /// attention requests (attention wakes do not consume it). Like every
    /// ungrouped watch it is deliver-once: the first delivered completion
    /// retires it, so a caller that wants further completions re-arms with
    /// another `agent.watch`. Returns `{ ok, subscriptionId, agentId }`.
    fn agent_watch(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, caller_agent_id, target_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_watch not implemented".to_string(),
            ))
        })
    }

    /// `agent.unwatch` (the `ws.agent.unwatch` MCP binding, monorepo#1229):
    /// remove one of the caller's own completion watches, addressed by
    /// `subscription_id` or by the watched `target_agent_id`. Returns
    /// `{ ok, removed }`.
    fn agent_unwatch(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        subscription_id: Option<String>,
        target_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            caller_agent_id,
            subscription_id,
            target_agent_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_unwatch not implemented".to_string(),
            ))
        })
    }

    /// `app.agents.waitFor` (the `ws.app.agents.waitFor` MCP binding):
    /// register completion watches for `caller_agent_id` on a set of existing
    /// target agents — the subscription side of `agent.delegate` without
    /// creating children. Semantics are identical to workspace agent
    /// subscriptions: `wait_mode` `"immediate"` (default) registers a
    /// watch per target; `"after_all"` enrolls every target in the caller's
    /// open delegation group (one aggregated wake once the caller idles and
    /// all targets settle). Targets in other workspaces are permitted only
    /// for chief-workspace callers (the shared registration scope gate).
    /// Returns `{ ok, waitMode, results }` where each result is
    /// `{ agentId, agentName, workspaceId, subscriptionId, groupId }`.
    fn app_agents_wait(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        agent_ids: Vec<String>,
        wait_mode: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, caller_agent_id, agent_ids, wait_mode);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::app_agents_wait not implemented".to_string(),
            ))
        })
    }

    /// `agent.cancelSubscriptions`: cancel an agent's subscriptions —
    /// everything when unscoped, or just the named completion watch
    /// (`subscription_id`) / delegation group (`group_id`) when scoped;
    /// `{ success: true }` (PROTOCOL §5.5).
    fn agent_cancel_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        subscription_id: Option<String>,
        group_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, subscription_id, group_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_cancel_subscriptions not implemented".to_string(),
            ))
        })
    }

    /// `agent.subscribe` (deprecated alias): service-style subscription result;
    /// not the WS streaming surface (use `events.subscribe`) (PROTOCOL §5.5/§6).
    /// `subscriber_agent_id` is the agent that receives batched wake messages
    /// when matching events fire; `None` (front-door caller with no agent
    /// identity) registers a match-only subscription with no wake target.
    fn agent_subscribe(
        &self,
        workspace_id: WorkspaceId,
        subscriber_agent_id: Option<AgentId>,
        event_types: Vec<String>,
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            subscriber_agent_id,
            event_types,
            exclude_self,
            batch_window,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_subscribe not implemented".to_string(),
            ))
        })
    }

    /// `agent.unsubscribe` (deprecated alias): service-style result (PROTOCOL §5.5/§6).
    fn agent_unsubscribe(
        &self,
        workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, subscription_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_unsubscribe not implemented".to_string(),
            ))
        })
    }

    /// `sandbox.cow.merge`: manually trigger merge-back for a sandboxed agent (PROTOCOL §5.34).
    /// Returns `{ ok, status, ... }` with merge outcome (merged | conflict | blocked | `merge_pending`).
    fn sandbox_merge(
        &self,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, sandbox_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sandbox_merge not implemented".to_string(),
            ))
        })
    }

    /// `sandbox.cow.discard`: manually discard a sandbox (PROTOCOL §5.34).
    /// Returns `{ ok }`.
    fn sandbox_discard(
        &self,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, sandbox_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sandbox_discard not implemented".to_string(),
            ))
        })
    }

    /// `comment.add`: text-anchored comment via searchContext + commentTarget (§5.3).
    ///
    /// `author_type` is the optional wire `authorType` (`"user"` | `"agent"`);
    /// it defaults to `agent` for backward compatibility with agent/MCP callers.
    ///
    /// `idempotency_key` is the optional `params.idempotencyKey` (design note TB-0
    /// §5): when present and previously recorded, the original result is returned
    /// without re-executing; soft-launch when absent (warn + execute).
    ///
    /// `comment_id` is the optional wire `commentId`: a client-supplied UUID used
    /// for the comment row, thread id, anchor ids, and the embedded
    /// `<!--anchor:{id}:start/end-->` markers, so a client that inserted
    /// optimistic anchors under that id converges with the daemon's rewrite.
    /// Absent → the daemon mints a fresh UUID (backward compatible).
    #[allow(clippy::too_many_arguments)]
    fn comment_add(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        search_context: String,
        comment_target: String,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
        author_type: Option<String>,
        idempotency_key: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        let _ = (
            workspace_id,
            note_id,
            search_context,
            comment_target,
            comment,
            kind,
            author,
            author_type,
            idempotency_key,
            comment_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_add not implemented".to_string(),
            ))
        })
    }

    /// `comment.list`: thread summaries with optional filters (PROTOCOL §5.3).
    fn comment_list(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        since: Option<String>,
        author_type: Option<String>,
        status: Option<String>,
        include_comments: bool,
    ) -> BoxFuture<'_, Result<CommentListResult>> {
        let _ = (
            workspace_id,
            note_id,
            since,
            author_type,
            status,
            include_comments,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_list not implemented".to_string(),
            ))
        })
    }

    /// `comment.getThread`: one thread by `threadId` or `commentId` (§5.3).
    fn comment_get_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentGetThreadResult>> {
        let _ = (workspace_id, note_id, thread_id, comment_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_get_thread not implemented".to_string(),
            ))
        })
    }

    /// `comment.respond`: add a reply to a thread (PROTOCOL §5.3).
    ///
    /// `author_type` is the optional wire `authorType` (`"user"` | `"agent"`);
    /// it defaults to `agent` for backward compatibility with agent/MCP callers.
    #[allow(clippy::too_many_arguments)]
    fn comment_respond(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
        author_type: Option<String>,
        suggestion_original: Option<String>,
        suggestion_proposed: Option<String>,
    ) -> BoxFuture<'_, Result<CommentRespondResult>> {
        let _ = (
            workspace_id,
            note_id,
            thread_id,
            comment_id,
            comment,
            kind,
            author,
            author_type,
            suggestion_original,
            suggestion_proposed,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_respond not implemented".to_string(),
            ))
        })
    }

    /// `comment.delete`: remove a comment by id (PROTOCOL §5.3).
    fn comment_delete(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        comment_id: String,
    ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
        let _ = (workspace_id, note_id, comment_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_delete not implemented".to_string(),
            ))
        })
    }

    /// `comment.resolveThread`: mark every comment in a thread resolved (or
    /// `resolved = false` to reopen), identified by `threadId` or `commentId`.
    fn comment_resolve_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        resolved: bool,
    ) -> BoxFuture<'_, Result<CommentResolveThreadResult>> {
        let _ = (workspace_id, note_id, thread_id, comment_id, resolved);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_resolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `event.agentActivity`: per-agent files (with `agentId`) or aggregated
    /// agent activity (without). The union result is returned as raw JSON
    /// (`FileActivity[]` or `AgentActivity[]`) (PROTOCOL §5.10).
    fn event_agent_activity(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<String>,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, minutes_ago);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_agent_activity not implemented".to_string(),
            ))
        })
    }

    /// `event.workspaceSummary`: aggregated activity summary (PROTOCOL §5.10).
    fn event_workspace_summary(
        &self,
        workspace_id: WorkspaceId,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<WorkspaceEventSummary>> {
        let _ = (workspace_id, minutes_ago);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_workspace_summary not implemented".to_string(),
            ))
        })
    }

    /// `event.query`: filtered event query over the append-only log (§5.10).
    /// Returns the legacy bare array by default; when `params.paginate` (or a
    /// `params.page_token`) is set it returns the `{ items, nextToken }`
    /// pagination envelope (TA-2 / §5.5), newest→oldest with the limit clamped.
    fn event_query(
        &self,
        workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_query not implemented".to_string(),
            ))
        })
    }

    /// `event.subscribe` (deprecated alias): service-style subscription result;
    /// does NOT wire WS streaming (use `events.subscribe`) (PROTOCOL §5.10/§6).
    /// `exclude_self`/`batch_window` mirror the TS shim's forwarded options.
    /// `subscriber_agent_id` is the agent that receives batched wake messages
    /// when matching events fire; `None` registers a match-only subscription
    /// with no wake target.
    fn event_subscribe(
        &self,
        workspace_id: WorkspaceId,
        subscriber_agent_id: Option<AgentId>,
        event_types: Vec<String>,
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
        let _ = (
            workspace_id,
            subscriber_agent_id,
            event_types,
            exclude_self,
            batch_window,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_subscribe not implemented".to_string(),
            ))
        })
    }

    /// `event.unsubscribe` (deprecated alias): service-style result (§5.10/§6).
    fn event_unsubscribe(
        &self,
        workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<EventUnsubscribeResult>> {
        let _ = (workspace_id, subscription_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_unsubscribe not implemented".to_string(),
            ))
        })
    }

    /// `git.status`: working-tree status for a workspace. Remote workspaces and
    /// non-repositories return the empty status (PROTOCOL §5.6). When
    /// `git_root_id` is set, the scan runs against that registered git root's
    /// path instead of the workspace worktree; an unknown id or one belonging
    /// to another workspace is `InvalidParams` (`-32602`).
    fn git_status(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<GitStatus>> {
        let _ = (workspace_id, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_status not implemented".to_string(),
            ))
        })
    }

    /// `git.status` with request options. `force_refresh` bypasses cached and
    /// pre-existing in-flight status results. The default preserves the
    /// existing behavior for implementations that only provide [`Self::git_status`].
    fn git_status_with_options(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: Option<WorkspaceGitRootId>,
        force_refresh: bool,
    ) -> BoxFuture<'_, Result<GitStatus>> {
        let _ = force_refresh;
        self.git_status(workspace_id, git_root_id)
    }

    /// `gitRoot.list`: every registered git root for a workspace (agent-
    /// registered and auto-detected), as the wire envelope
    /// `{ gitRoots: [...] }` with each row carrying the persisted
    /// `WorkspaceGitRoot` fields plus a live-read `branch`. A missing
    /// workspace is `NotFound` (`-32602`); a workspace with no registered
    /// roots returns an empty list (PROTOCOL §5.6 extensions,
    /// intent-hq/monorepo#2053).
    fn git_root_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_root_list not implemented".to_string(),
            ))
        })
    }

    /// Resolve a registered git root to its filesystem path after validating
    /// it belongs to `workspace_id`. An unknown id — or an id registered to a
    /// different workspace — is `InvalidParams` (`-32602`). Used by the
    /// transport for path-based git reads (`git.branchStatus`) that accept a
    /// `gitRootId` in place of a raw `repoPath`.
    fn git_root_path(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: WorkspaceGitRootId,
    ) -> BoxFuture<'_, Result<String>> {
        let _ = (workspace_id, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_root_path not implemented".to_string(),
            ))
        })
    }

    /// `ws.git.registerRoot` (MCP-only): register an existing local git
    /// repository root for the workspace's multi git root tracking
    /// (monorepo#2053). `path` must exist and contain a `.git` entry; it is
    /// canonicalized and may live anywhere on the host (no worktree
    /// containment). The workspace's own primary root is rejected (it is
    /// tracked implicitly). Registration is idempotent by canonical path —
    /// re-registration appends `agent_id` to the row's attribution, and an
    /// auto-detected row is upgraded to `source: "agent"` in place. Returns
    /// the stored row in the `gitRoot.list` row shape (persisted fields plus
    /// a live-read `branch`). Invalid paths are `InvalidParams` (`-32602`); a
    /// missing workspace is `NotFound`.
    fn git_root_register(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_root_register not implemented".to_string(),
            ))
        })
    }

    /// `ws.git.unregisterRoot` (MCP-only): remove the git root registered for
    /// `path` (canonicalized when the directory still exists, so the same
    /// spelling that registered it resolves; the raw path is matched
    /// otherwise). Returns `{ ok, gitRootId, path }`. A path with no
    /// registered root is `NotFound` (monorepo#2053).
    fn git_root_unregister(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_root_unregister not implemented".to_string(),
            ))
        })
    }

    /// `git.getConfig`: read the raw `.git/config` file content from the
    /// workspace's repository (STAB-10 — retire FE filesystem reads). Returns
    /// `{ config: String }` where `config` is the entire file content.
    /// For linked worktrees, resolves the `gitdir:` pointer and `commondir` to
    /// read the main repo's config. If the worktree is not a repo, walks parent
    /// directories to find a containing repo's config (nested-repo parity with
    /// the FE). Remote workspaces and non-repositories return `{ config: "" }`.
    /// A missing workspace is `-32602`.
    fn git_get_config(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<String>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_get_config not implemented".to_string(),
            ))
        })
    }

    /// `git.stage`: stage `paths` (CSV string or array). `.`/`*`/`--all` are
    /// rejected (`-32603`); returns the validated path list (PROTOCOL §5.6).
    fn git_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_stage not implemented".to_string(),
            ))
        })
    }

    /// `git.unstage`: unstage `paths` (CSV string or array), the inverse of
    /// `git.stage`. `.`/`*`/`--all` are rejected (`-32603`); idempotent on
    /// already-unstaged paths. Returns the validated path list (PROTOCOL §5.6).
    fn git_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_unstage not implemented".to_string(),
            ))
        })
    }

    /// `git.discard`: discard working-tree changes to `paths` (CSV string or
    /// array), mirroring `gitService.discardChanges`: tracked paths are
    /// restored from the index (`git checkout -- <paths>`); untracked paths
    /// are deleted from disk. Staged changes are untouched. `.`/`*`/`--all`
    /// are rejected (`-32603`); idempotent on already-clean paths. Returns the
    /// validated path list (PROTOCOL §5.6).
    fn git_discard(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_discard not implemented".to_string(),
            ))
        })
    }

    /// `git.getBranches`: branches for a known `repo_path`; an unknown repo path
    /// is `-32602` (PROTOCOL §5.6).
    fn git_get_branches(
        &self,
        repo_path: String,
        include_remote: bool,
    ) -> BoxFuture<'_, Result<GitBranches>> {
        let _ = (repo_path, include_remote);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_get_branches not implemented".to_string(),
            ))
        })
    }

    /// `git.branchStatus`: ahead/behind vs `origin/<branch_name>` + working-tree
    /// uncommitted-changes flag for a known `repo_path`. Same known-repo gate as
    /// `git_get_branches`; an unknown repo path is `-32602` (PROTOCOL §5.6).
    fn git_branch_status(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<GitBranchStatus>> {
        let _ = (repo_path, branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_branch_status not implemented".to_string(),
            ))
        })
    }

    /// `git.pull`: fetch + rebase-pull `branch_name` from `origin` for the repo
    /// at `repo_path` (path-based like `git_get_branches`; an invalid repo path
    /// is `-32602`). Ordinary pull failures are a structured `{ ok: false,
    /// error }` result, not an `Err` (PROTOCOL §5.6).
    fn git_pull(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<GitPullResult>> {
        let _ = (repo_path, branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_pull not implemented".to_string(),
            ))
        })
    }

    /// `git.commit` (deprecated; prefer `git_agent_commit`): commit the already
    /// staged changes with `message`. Failures (incl. nothing to commit) are
    /// `-32603` (PROTOCOL §5.6).
    fn git_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<GitCommitResult>> {
        let _ = (workspace_id, message, idempotency_key);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commit not implemented".to_string(),
            ))
        })
    }

    /// `git.agentCommit`: stage the agent's changes (or `files` when given) and
    /// commit them; `user_requested` bypasses the auto-commit gate (PROTOCOL
    /// §5.6). When `agent_id` (and optionally `linked_note_id`) are present, the
    /// commit body carries `Agent-Id:` / `Linked-Note-Id:` attribution trailers;
    /// the FE/transport path passes `None` for both (no agent context).
    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        agent_id: Option<AgentId>,
        linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        let _ = (
            workspace_id,
            message,
            agent_id,
            linked_note_id,
            files,
            user_requested,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_agent_commit not implemented".to_string(),
            ))
        })
    }

    /// `git.checkMergeConflicts`: whether merging the current branch into
    /// `target_branch` (or the detected default) would conflict (PROTOCOL §5.6).
    fn git_check_merge_conflicts(
        &self,
        workspace_id: WorkspaceId,
        target_branch: Option<String>,
    ) -> BoxFuture<'_, Result<GitMergeConflicts>> {
        let _ = (workspace_id, target_branch);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_check_merge_conflicts not implemented".to_string(),
            ))
        })
    }

    /// `git.changes`: the working-tree file list (`FileStatus[]`) for a
    /// workspace — the same `files` array as `git.status`. Remote workspaces and
    /// non-repositories return an empty array (wire §7.7). When `git_root_id`
    /// is set, the scan targets that registered root's path; an unknown or
    /// foreign id is `InvalidParams` (`-32602`).
    fn git_changes(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_changes not implemented".to_string(),
            ))
        })
    }

    /// `git.diffs`: per-file diff hunks. When `commit_hash` is set, returns
    /// the per-file hunks for `<commit_hash>^..<commit_hash>` (the commit's own
    /// changes vs its first parent; a root commit yields all-additions) and the
    /// `staged` flag is ignored. Otherwise `staged` selects the HEAD→index diff
    /// (`true`) or the index→workdir diff (`false`, default). `paths` restricts
    /// the result to exactly those workspace-relative files (literal matching;
    /// the wire `path` param arrives folded into this set); `None` or an empty
    /// vec means the full tree. Returns `[{ path, hunks }]`; remote/non-repo
    /// workspaces and an unresolvable `commit_hash` return an empty array
    /// (wire §7.7). When `git_root_id` is set, the walk targets that
    /// registered root's path; an unknown or foreign id is `InvalidParams`
    /// (`-32602`).
    fn git_diffs(
        &self,
        workspace_id: WorkspaceId,
        paths: Option<Vec<String>>,
        staged: bool,
        commit_hash: Option<String>,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, paths, staged, commit_hash, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_diffs not implemented".to_string(),
            ))
        })
    }

    /// `git.commitDetails`: metadata + per-file `(additions, deletions)` for a
    /// single commit, addressed by `commit_hash` (full SHA or short ref).
    /// Returns the wire shape `{ commitHash, author, authorEmail, date, message,
    /// files: string[], fileDetails: [{ path, additions, deletions }] }`.
    /// Remote/non-repo workspaces and an unresolvable hash return an empty
    /// envelope (`{ commitHash, fileDetails: [], files: [] }`) so the FE renders
    /// a friendly empty state instead of crashing (wire §7.7). When
    /// `git_root_id` is set, the read runs against that registered git root's
    /// path instead of the workspace worktree; an unknown id or one belonging
    /// to another workspace is `InvalidParams` (`-32602`).
    fn git_commit_details(
        &self,
        workspace_id: WorkspaceId,
        commit_hash: String,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, commit_hash, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commit_details not implemented".to_string(),
            ))
        })
    }

    /// `git.commits`: paginated reverse-chronological commit history as the
    /// canonical §5.5 page envelope `{ items: CommitInfo[], nextToken }`. Remote
    /// workspaces and non-repositories return an empty page (wire §7.7). When
    /// `git_root_id` is set, the walk targets that registered root's path; an
    /// unknown or foreign id is `InvalidParams` (`-32602`).
    fn git_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
        page_token: Option<String>,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, limit, page_token, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commits not implemented".to_string(),
            ))
        })
    }

    /// `git.showFile`: raw file content at a revision (`git show <ref>:<path>`
    /// semantics) as `{ content }`. `ref` accepts anything revparse-able plus
    /// the index ref `":0"`; a path missing at the ref and remote/non-repo
    /// workspaces return `{ content: "" }` (wire §7.7), while an unresolvable
    /// `ref` is `-32603` (PROTOCOL §5.6 extensions). When `git_root_id` is
    /// set, the read targets that registered root's path; an unknown or
    /// foreign id is `InvalidParams` (`-32602`).
    fn git_show_file(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        git_ref: String,
        git_root_id: Option<WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, file_path, git_ref, git_root_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_show_file not implemented".to_string(),
            ))
        })
    }

    /// `git.numstat`: per-file additions/deletions for a workspace's tracked
    /// changes (mirrors `git diff --numstat`). When `base_ref`/`base_sha` are
    /// set, resolves the branch boundary (merge-base of `target_ref` and
    /// `base_ref`, else `base_sha` when it is an ancestor of `target_ref`) and
    /// returns `<boundary>..<target_ref>`; else `staged=true` selects HEAD→index
    /// (`--cached`), `staged=false` selects index→workdir tracked-only, and the
    /// unset/`None` default is HEAD→workdir tracked-only. `target_ref` defaults
    /// to `HEAD`. `paths` filters to the given repo-relative paths. Result is
    /// `[{ filePath, additions, deletions }]`; remote/non-repo workspaces and
    /// an unresolved boundary return an empty array (wire §7.7).
    fn git_numstat(
        &self,
        workspace_id: WorkspaceId,
        staged: Option<bool>,
        base_ref: Option<String>,
        base_sha: Option<String>,
        target_ref: Option<String>,
        paths: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, staged, base_ref, base_sha, target_ref, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_numstat not implemented".to_string(),
            ))
        })
    }

    /// `git.branchDiff`: committed diff of `target_ref` vs the branch boundary
    /// (merge-base of `target_ref` and `base_ref`, else `base_sha` when it is
    /// an ancestor of `target_ref`). Returns
    /// `[{ file, chunks: [], oldContent, newContent }]` — each entry carries the
    /// full file contents at the boundary and target so the FE branch-base
    /// viewer can render the diff without a follow-up read. `target_ref`
    /// defaults to `HEAD`. `paths` narrows the result. Remote/non-repo
    /// workspaces and an unresolved boundary return an empty array (wire §7.7);
    /// omitting both `base_ref` and `base_sha` is `-32602`.
    fn git_branch_diff(
        &self,
        workspace_id: WorkspaceId,
        base_ref: Option<String>,
        base_sha: Option<String>,
        target_ref: Option<String>,
        paths: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, base_ref, base_sha, target_ref, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_branch_diff not implemented".to_string(),
            ))
        })
    }

    /// `git.getRemoteUrl`: the configured URL of the named remote for the git
    /// repository at `repo_path` (default `remote_name` = `"origin"`). Result
    /// is `{ url: string | null }` — a missing remote folds to `null` rather
    /// than an error (FE `git-tracking:get-remote-url` parity). Path-based
    /// like the branch reads (§5.6): a nonexistent or non-git `repo_path` is
    /// `-32602`.
    fn git_get_remote_url(
        &self,
        repo_path: String,
        remote_name: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (repo_path, remote_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_get_remote_url not implemented".to_string(),
            ))
        })
    }

    /// `git.stageHunk`: apply `hunk_patch` (a unified-diff patch for one or
    /// more hunks) to the index only for `file_path`, mirroring
    /// `gitService.stageHunk` (`git apply --cached [--3way]`). Failures surface
    /// as `-32603` (PROTOCOL §5.6).
    fn git_stage_hunk(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        hunk_patch: String,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (workspace_id, file_path, hunk_patch);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_stage_hunk not implemented".to_string(),
            ))
        })
    }

    /// `git.unstageHunk`: reverse-apply `hunk_patch` to the index only for
    /// `file_path`, mirroring `gitService.unstageHunk`
    /// (`git apply --cached --reverse [--3way]`). Failures surface as `-32603`
    /// (PROTOCOL §5.6).
    fn git_unstage_hunk(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        hunk_patch: String,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (workspace_id, file_path, hunk_patch);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_unstage_hunk not implemented".to_string(),
            ))
        })
    }

    /// `git.push`: push the workspace's current branch to `origin` (with
    /// `+refs/heads/<branch>` when `force` is set), mirroring
    /// `gitService.push`. Returns `{ branch, pushedSha }` (camelCase on the
    /// wire — see PROTOCOL §5.6). Failures surface as `-32603`.
    fn git_push(
        &self,
        workspace_id: WorkspaceId,
        force: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, force);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_push not implemented".to_string(),
            ))
        })
    }

    /// `git.fetch`: fetch the workspace's current branch from `origin`,
    /// mirroring `gitService.fetch`. Updates the local remote-tracking ref.
    /// Failures surface as `-32603` (PROTOCOL §5.6).
    fn git_fetch(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_fetch not implemented".to_string(),
            ))
        })
    }

    /// `git.createBranch`: create `branch_name` from `HEAD` in the workspace's
    /// worktree and optionally check it out (`gitService.createBranch`).
    /// Failures surface as `-32603` (PROTOCOL §5.6).
    fn git_create_branch(
        &self,
        workspace_id: WorkspaceId,
        branch_name: String,
        checkout: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, branch_name, checkout);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_create_branch not implemented".to_string(),
            ))
        })
    }

    /// `git.checkoutBranch`: check out an existing `branch_name` in the
    /// workspace's worktree (`gitService.checkoutBranch`). Failures surface as
    /// `-32603` (PROTOCOL §5.6).
    fn git_checkout_branch(
        &self,
        workspace_id: WorkspaceId,
        branch_name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_checkout_branch not implemented".to_string(),
            ))
        })
    }

    /// `git.renameBranch`: rename `old_branch_name` → `new_branch_name` in the
    /// workspace's repository (`gitService.renameBranch`). Refuses when the old
    /// branch is missing, the new name already exists, or the new name is
    /// checked out in another worktree. A same-as-old new name (after trim) is
    /// a no-op. Failures surface as `-32603`; validation failures (empty
    /// `old_branch_name` or empty `new_branch_name`) as `-32602`
    /// (PROTOCOL §5.6).
    fn git_rename_branch(
        &self,
        workspace_id: WorkspaceId,
        old_branch_name: String,
        new_branch_name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, old_branch_name, new_branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_rename_branch not implemented".to_string(),
            ))
        })
    }

    /// `git.removeLockFile`: delete `index.lock` in the workspace worktree's
    /// git dir (`gitService.removeLockFile`). Returns `{ removed: bool }`;
    /// a missing lock file is not an error. Remote workspaces short-circuit
    /// `{ removed: false }`. Failures surface as `-32603` (PROTOCOL §5.6).
    fn git_remove_lock_file(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_remove_lock_file not implemented".to_string(),
            ))
        })
    }

    /// `git.clone`: streaming clone of `url` into `<parent_dir>/<target_name>`
    /// (or the URL-derived basename when `target_name` is `None`). Returns
    /// `{ requestId, targetPath }` promptly and pushes `git:clone:progress`
    /// frames followed by a terminal `git:clone:done` on the event bus,
    /// correlated by `requestId` (PROTOCOL §5.6 / §6.5). Payloads never carry
    /// the source URL or credentials. `-32602` on missing/invalid params;
    /// `-32603` when the daemon cannot spawn `git`.
    fn git_clone(
        &self,
        url: String,
        parent_dir: String,
        target_name: Option<String>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (url, parent_dir, target_name, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_clone not implemented".to_string(),
            ))
        })
    }

    /// `repo.list`: the persistent known-repository registry, most-recently-used
    /// first, as `{ repos: KnownRepo[] }` (PROTOCOL §5.6). Populates the iOS
    /// Create-Workspace picker; the first invocation also lazily syncs repos from
    /// existing workspaces (never blocking/failing the response).
    fn repo_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::repo_list not implemented".to_string(),
            ))
        })
    }

    /// `repo.remove`: delete one entry from the persistent known-repository
    /// registry by `path`, as `{ removed: bool }` (PROTOCOL §5.11). Removing a
    /// path that is not registered is not an error — `removed` is `false`.
    fn repo_remove(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::repo_remove not implemented".to_string(),
            ))
        })
    }

    /// `repo.warmCache`: kick off an opportunistic background refresh of the
    /// daemon-managed repo cache for one GitHub repo, as
    /// `{ started: true, owner, repo }` — the RPC returns immediately while
    /// the fetch runs detached. At most one warm runs daemon-wide; a call
    /// while one is in flight is rejected with [`Error::WarmInFlight`]
    /// (PROTOCOL §5.6). A `githubUrl` with no owner/repo pair is `-32602`.
    fn repo_warm_cache(&self, github_url: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = github_url;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::repo_warm_cache not implemented".to_string(),
            ))
        })
    }

    /// `pr.status`: the active PR's state, mergeability, and summary. Requires an
    /// active PR; otherwise `-32603` (PROTOCOL §5.7).
    fn pr_status(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_status not implemented".to_string(),
            ))
        })
    }

    /// `pr.refresh`: force the same PR discovery/refresh the background sweep
    /// runs for one workspace, returning the post-refresh linkage state as
    /// `{ outcome, prNumber?, prUrl?, prStatus?, pullRequests }`. Ineligible
    /// workspaces (remote/archived/no repo) report `outcome: "skipped"` rather
    /// than erroring (PROTOCOL §5.7 extension).
    fn pr_refresh(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_refresh not implemented".to_string(),
            ))
        })
    }

    /// `ws.pr.snapshot` engine (MCP-only surface, not in the FE router
    /// catalog): a compact, diff-friendly snapshot of PR `pr_number` — state,
    /// mergeability + blocked reason, check-run tally, review decision, and
    /// comment counts — for hook-based PR monitoring. Scoped to the
    /// workspace's repo unless `repo` (an `"owner/name"` slug) overrides it;
    /// the result always echoes the resolved repo as `repo` so a wrong-repo
    /// read is detectable. `pr_number` is REQUIRED; there is no active-PR
    /// fallback.
    fn pr_state(
        &self,
        workspace_id: WorkspaceId,
        pr_number: u64,
        repo: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, pr_number, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_state not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // `github.*` explicit-addressing surface (PROTOCOL §5.27). Unlike `pr.*`
    // (workspace/active-PR scoped), every data method takes `(owner, repo[,
    // number])` directly. Backed by the same `intent-sourcecontrol` engine.
    // ------------------------------------------------------------------------

    /// `github.pulls.create`: open a PR with `head` sent **verbatim** (no
    /// `owner:branch` login prefix) — `{ pull }` (PROTOCOL §5.27).
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_create(
        &self,
        owner: String,
        repo: String,
        title: String,
        body: String,
        head: String,
        base: String,
        draft: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, title, body, head, base, draft);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_create not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.get`: `GET /repos/{owner}/{repo}/pulls/{number}` → `{ pull }`.
    fn github_pulls_get(
        &self,
        owner: String,
        repo: String,
        number: u64,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_get not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.list`: `GET /repos/{owner}/{repo}/pulls` → `{ pulls, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        head: Option<String>,
        base: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, state, head, base, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_list not implemented".to_string(),
            ))
        })
    }

    // ========================================================================
    // github.* browse / auth / identity (PROTOCOL §5.27)
    //
    // Repo-addressed GitHub operations backed by the `SourceControl` engine,
    // distinct from the workspace/active-PR `pr.*` surface. The PAT comes from
    // the environment and is NEVER logged, echoed, or returned over the wire —
    // only derived identity / connection state crosses it.
    // ========================================================================

    /// `github.repos.list`: the authenticated user's repositories
    /// (`GET /user/repos`) → `{ repos: GithubRepo[], nextToken }`.
    fn github_repos_list(
        &self,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_list not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.search`: `GET /search/issues` (`is:pr` + `@me`
    /// involvement + free-text `query`) → `{ pulls, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        query: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, filter, state, query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_search not implemented".to_string(),
            ))
        })
    }

    /// `github.repos.search`: search repositories (`GET /search/repositories`)
    /// → `{ repos: GithubRepo[], nextToken }`.
    fn github_repos_search(
        &self,
        query: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_search not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.merge`: `PUT /repos/{owner}/{repo}/pulls/{number}/merge`
    /// → `{ merged, message, sha? }`.
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_merge(
        &self,
        owner: String,
        repo: String,
        number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            owner,
            repo,
            number,
            merge_method,
            commit_title,
            commit_message,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_merge not implemented".to_string(),
            ))
        })
    }

    /// `github.repos.get`: a single repository's metadata
    /// (`GET /repos/{owner}/{repo}`) → `{ repo: GithubRepo | null }`.
    fn github_repos_get(
        &self,
        owner: String,
        repo: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_get not implemented".to_string(),
            ))
        })
    }

    /// `github.repoConfig.get`: a remote repository's `.intent/config.json`
    /// fetched via the contents API (no clone) → `{ config, exists }`. A
    /// missing file yields `{ config: null, exists: false }`; a present but
    /// invalid file folds tolerantly to `{ config: {}, exists: true }`
    /// (mirrors the local `repoConfig.get` parse semantics).
    fn github_repo_config_get(
        &self,
        owner: String,
        repo: String,
        git_ref: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, git_ref);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repo_config_get not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.updateBranch`:
    /// `PUT /repos/{owner}/{repo}/pulls/{number}/update-branch` → `{ message, url? }`.
    fn github_pulls_update_branch(
        &self,
        owner: String,
        repo: String,
        number: u64,
        expected_head_sha: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, expected_head_sha);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_update_branch not implemented".to_string(),
            ))
        })
    }

    /// `github.issues.list`: `GET /repos/{owner}/{repo}/issues` (PRs filtered
    /// out) → `{ issues, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_issues_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        labels: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, state, labels, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_issues_list not implemented".to_string(),
            ))
        })
    }

    /// `github.branches.list`: a repository's remote branch names
    /// (`GET /repos/{owner}/{repo}/branches`) → `{ branches: string[], nextToken }`.
    /// A non-empty `prefix` narrows the listing server-side to branch names
    /// starting with it (`GET /git/matching-refs/heads/{prefix}`); absent or
    /// blank keeps the unfiltered listing.
    fn github_branches_list(
        &self,
        owner: String,
        repo: String,
        prefix: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, prefix, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_branches_list not implemented".to_string(),
            ))
        })
    }

    /// `github.branches.listCached`: branch names from the daemon's local
    /// repo cache — no network I/O — →
    /// `{ cached: boolean, branches: string[], defaultBranch?: string }`
    /// (`cached: false` ⇒ empty branches, no defaultBranch).
    fn github_branches_list_cached(
        &self,
        owner: String,
        repo: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_branches_list_cached not implemented".to_string(),
            ))
        })
    }

    /// `github.issues.search`: `GET /search/issues` (`is:issue` + free-text
    /// `query`) → `{ issues, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_issues_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        query: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, filter, state, query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_issues_search not implemented".to_string(),
            ))
        })
    }

    /// `github.authStatus`: validate the resolved token via `GET /user` and
    /// report connection state, plus the in-flight device-flow state
    /// (`deviceFlow: null | { status, userCode, verificationUri, expiresIn,
    /// interval }`) when a `github.connect` flow is pending or terminal.
    /// Never returns the token.
    fn github_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `github.listReviewComments`:
    /// `GET /repos/{owner}/{repo}/pulls/{number}/comments` → `{ comments, nextToken }`.
    fn github_list_review_comments(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_list_review_comments not implemented".to_string(),
            ))
        })
    }

    /// `github.connect`: start (or return the still-pending) GitHub OAuth
    /// device flow → `{ ok, userCode, verificationUri, expiresIn, interval }`.
    /// The daemon polls GitHub in the background and emits
    /// `github:auth-changed` on terminal transitions; the token is persisted
    /// server-side and never crosses the wire.
    fn github_connect(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_connect not implemented".to_string(),
            ))
        })
    }

    /// `github.cancelAuth`: abort the in-flight device flow, if any →
    /// `{ ok, cancelled }` (`cancelled: false` when nothing was pending).
    fn github_cancel_auth(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_cancel_auth not implemented".to_string(),
            ))
        })
    }

    /// `github.replyReviewComment`: reply to a review comment
    /// (`inReplyToId = commentId`) → `{ comment }`.
    fn github_reply_review_comment(
        &self,
        owner: String,
        repo: String,
        number: u64,
        comment_id: u64,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, comment_id, body);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_reply_review_comment not implemented".to_string(),
            ))
        })
    }

    /// `github.revoke`: delete the stored `sourceControl.github.token` secret
    /// and abort any in-flight device flow → `{ ok }`. Env / `gh` CLI
    /// fallback resolution is untouched.
    fn github_revoke(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_revoke not implemented".to_string(),
            ))
        })
    }

    /// `github.getReviewThreads`: GraphQL `pullRequest.reviewThreads` →
    /// `{ threads, nextToken }`.
    fn github_get_review_threads(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_get_review_threads not implemented".to_string(),
            ))
        })
    }

    /// `github.resolveThread`: GraphQL `resolveReviewThread` → `{ isResolved: true }`.
    fn github_resolve_thread(&self, thread_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = thread_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_resolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `github.unresolveThread`: GraphQL `unresolveReviewThread` →
    /// `{ isResolved: false }`.
    fn github_unresolve_thread(
        &self,
        thread_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = thread_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_unresolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `github.getUser`: GitHub-derived identity (`GET /user`) →
    /// `{ user: GithubUser | null }`. Never includes the PAT.
    fn github_get_user(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_get_user not implemented".to_string(),
            ))
        })
    }

    /// `linear.authStatus`: validate the resolved Linear API key via the GraphQL
    /// `viewer` probe and report `{ authenticated, login?, scopes }`. The key is
    /// never returned. A missing/invalid key surfaces as `Internal` (PROTOCOL
    /// §5.28).
    fn linear_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `linear.listIssues`: the viewer's issues for the typed `filter`
    /// (`assigned`|`created`|`subscribed`|`team`|`all`, default `assigned`),
    /// returned as `{ issues: LinearIssueResult[], nextToken }`. `next_token`
    /// is the opaque base64 token from a previous page (§5.5; malformed →
    /// first page); the result's `nextToken` is an opaque base64 string when
    /// another page exists, else `null` (PROTOCOL §5.28).
    fn linear_list_issues(
        &self,
        filter: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (filter, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_issues not implemented".to_string(),
            ))
        })
    }

    /// `linear.searchIssues`: full-text issue search by `query`, returned as
    /// `{ issues: LinearIssueResult[], nextToken }` with the same cursor
    /// semantics as `linear.listIssues` (PROTOCOL §5.28).
    fn linear_search_issues(
        &self,
        query: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_search_issues not implemented".to_string(),
            ))
        })
    }

    /// `linear.getIssue`: a single flattened `LinearIssueResult` looked up by
    /// UUID `id` or `ENG-123` `identifier` (PROTOCOL §5.28).
    fn linear_get_issue(
        &self,
        id_or_identifier: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id_or_identifier;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_get_issue not implemented".to_string(),
            ))
        })
    }

    /// `linear.viewer`: the authenticated user as a bare `LinearUser`
    /// (PROTOCOL §5.28).
    fn linear_viewer(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_viewer not implemented".to_string(),
            ))
        })
    }

    /// `linear.listTeams`: teams as a bare `LinearTeam[]` (PROTOCOL §5.28).
    fn linear_list_teams(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_teams not implemented".to_string(),
            ))
        })
    }

    /// `linear.listWorkflowStates`: workflow states as a bare
    /// `LinearWorkflowState[]` (PROTOCOL §5.28).
    fn linear_list_workflow_states(
        &self,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_workflow_states not implemented".to_string(),
            ))
        })
    }

    /// `linear.listProjects`: projects as a bare `LinearProject[]`
    /// (PROTOCOL §5.28).
    fn linear_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_projects not implemented".to_string(),
            ))
        })
    }

    /// `linear.listLabels`: issue labels as a bare `LinearLabel[]`
    /// (PROTOCOL §5.28).
    fn linear_list_labels(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_labels not implemented".to_string(),
            ))
        })
    }

    /// `linear.createIssue`: create a Linear issue (`issueCreate` mutation).
    /// `request` is the bare `CreateIssueRequest` JSON: `title` and `teamId`
    /// are required; everything else is optional. Returns the flattened
    /// issue (`LinearIssueResult`); the API key never crosses the wire
    /// (PROTOCOL §5.28).
    fn linear_create_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_create_issue not implemented".to_string(),
            ))
        })
    }

    /// `linear.updateIssue`: update a Linear issue (`issueUpdate` mutation).
    /// `request` is the bare `UpdateIssueRequest` JSON: `issueId` is required;
    /// only the fields present are sent through `IssueUpdateInput`. Returns
    /// the flattened issue (`LinearIssueResult`); the API key never crosses
    /// the wire (PROTOCOL §5.28).
    fn linear_update_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_update_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.authStatus`: probe the resolved Sentry credentials via
    /// `GET /organizations/{org}/` and report `{ authenticated, organization?,
    /// error? }`. The token is never returned. A missing pair surfaces as
    /// `Internal` (PROTOCOL §5.29).
    fn sentry_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `sentry.listIssues`: issues matching the typed `status` filter
    /// (`unresolved`|`resolved`|`ignored`|`all`, default `unresolved`),
    /// optional `project` slug, optional free-text `query`, returned as
    /// `{ issues: SentryIssueResult[], nextToken }`. `next_token` is the
    /// opaque base64 token from a previous page (§5.5; malformed → first
    /// page); the result's `nextToken` is an opaque base64 string when
    /// another page exists, else `null` (PROTOCOL §5.29).
    fn sentry_list_issues(
        &self,
        project: Option<String>,
        status: Option<String>,
        query: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (project, status, query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_list_issues not implemented".to_string(),
            ))
        })
    }

    /// `sentry.searchIssues`: full-text issue search by `query`, optional
    /// `project` slug, returned as `{ issues: SentryIssueResult[], nextToken }`
    /// with the same cursor semantics as `sentry.listIssues` (PROTOCOL §5.29).
    fn sentry_search_issues(
        &self,
        query: String,
        project: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, project, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_search_issues not implemented".to_string(),
            ))
        })
    }

    /// `sentry.listProjects`: projects for the configured organization as a
    /// bare `SentryProject[]` (PROTOCOL §5.29).
    fn sentry_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_list_projects not implemented".to_string(),
            ))
        })
    }

    /// `sentry.getIssue`: a single flattened `SentryIssueResult` looked up by
    /// numeric/UUID id or shortId (e.g. `WEB-1`). The router enforces that at
    /// least one of `id`/`shortId` is supplied (PROTOCOL §5.29).
    fn sentry_get_issue(&self, id_or_short_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id_or_short_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_get_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.resolveIssue`: mutate the issue's status to `resolved` and
    /// return the updated flattened issue (PROTOCOL §5.29).
    fn sentry_resolve_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_resolve_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.ignoreIssue`: mutate the issue's status to `ignored` and return
    /// the updated flattened issue (PROTOCOL §5.29).
    fn sentry_ignore_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_ignore_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.assignIssue`: assign the issue to `assignedTo` (an explicit
    /// `null`/absent unassigns) and return the updated flattened issue
    /// (PROTOCOL §5.29).
    fn sentry_assign_issue(
        &self,
        id: String,
        assigned_to: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, assigned_to);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_assign_issue not implemented".to_string(),
            ))
        })
    }

    /// `voice.transcribe`: speech-to-text over a pluggable provider
    /// (`ElevenLabs` Scribe | `OpenAI`). `params` carries `audio` (required,
    /// base64), optional `mimeType`, `language`, `provider` override, and
    /// `context { prompt?, keyterms? }`; returns `{ text, provider,
    /// durationMs? }`. Missing/oversized/invalid audio → `InvalidParams`
    /// (-32602); a missing API key or provider failure → `Internal` (-32603).
    /// The provider API keys never cross the wire.
    fn voice_transcribe(
        &self,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = params;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::voice_transcribe not implemented".to_string(),
            ))
        })
    }

    /// `voice.getWorkspaceVocabulary`: the auto-derived workspace vocabulary
    /// — derived terms only, the user's `voice.vocabulary` is not merged in —
    /// as `{ terms: string[] }` (PROTOCOL §5.41, v4.6). Served from the same
    /// content-hash cache the `voice.transcribe` injection uses, capped by
    /// `voice.workspaceVocabulary.maxTerms` (`{ terms: [] }` when the setting
    /// is `0` or nothing derives). An unknown `workspaceId` → `NotFound`
    /// (-32602 with `error.data.code: "not-found"` on the wire).
    fn voice_get_workspace_vocabulary(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::voice_get_workspace_vocabulary not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.getChanges`: the filtered tracked-change list
    /// (`{ changes, truncated, totalCount }`) (PROTOCOL §5.19).
    fn file_tracking_get_changes(
        &self,
        workspace_id: WorkspaceId,
        filter: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, filter);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_get_changes not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.loadCommits`: commit history with attribution
    /// (`{ commits: CommitWithAttribution[], boundarySha, nextToken }`).
    /// Wire shape details pending docs/protocol/ update (see monorepo Task 3).
    fn file_tracking_load_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
        page_token: Option<String>,
        include_older: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, limit, page_token, include_older);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_load_commits not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.getLineStats`: real-time additions/deletions totals
    /// (`{ additions, deletions }`) (PROTOCOL §5.19).
    fn file_tracking_get_line_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_get_line_stats not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.stage`: stage `paths` and move their audit rows to the
    /// `staged` stage (`{ ok: true }`) (PROTOCOL §5.19).
    fn file_tracking_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_stage not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.unstage`: unstage `paths` and move their audit rows back to
    /// the `unstaged` stage (`{ ok: true }`) (PROTOCOL §5.19).
    fn file_tracking_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_unstage not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getWorkspaceStats`: the workspace's line-change `Metrics`
    /// (`{ additions, deletions, filesChanged, byAgent }`) or `null` (PROTOCOL §5.20).
    fn metrics_get_workspace_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_workspace_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getAgentStats`: one agent's `Metrics` summed across workspaces
    /// (`{ additions, deletions, filesChanged }`, `byAgent` omitted) or `null` (§5.20).
    fn metrics_get_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_agent_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getAllWorkspaceStats`: a `{ [workspaceId]: Metrics }` map across
    /// every workspace (PROTOCOL §5.20).
    fn metrics_get_all_workspace_stats(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_all_workspace_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.clearAgentStats`: reset one agent's counters across workspaces,
    /// returning `{ success: boolean }` (PROTOCOL §5.20).
    fn metrics_clear_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_clear_agent_stats not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // settings.* — BE-owned settings namespace (PROTOCOL §5.12, §9.8). Global
    // (no `workspaceId`); sensitive values are redacted in list/get and never
    // cross the wire in plaintext.
    // ------------------------------------------------------------------------

    /// `settings.list`: every setting definition with its (redacted) current
    /// value — `{ settings: SettingDefinitionWithValue[] }` (PROTOCOL §5.12).
    fn settings_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_list not implemented".to_string(),
            ))
        })
    }

    /// `settings.get`: one setting as `{ path, value, definition }`; the value is
    /// redacted when sensitive; unknown path → `-32602` (PROTOCOL §5.12).
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_get not implemented".to_string(),
            ))
        })
    }

    /// `settings.update`: atomic batch apply of `changes: [{ path, value }]`
    /// (unknown path / read-only / failed validation → `-32602`, nothing
    /// applied); returns `{ applied: [{ path, value }] }` redacted and emits
    /// `settings:changed` on success (PROTOCOL §5.12).
    fn settings_update(
        &self,
        changes: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = changes;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_update not implemented".to_string(),
            ))
        })
    }

    /// `settings.reset`: restore a setting's default, returning the redacted
    /// `{ path, value }` and emitting `settings:changed`; unknown path → `-32602`
    /// (PROTOCOL §5.12).
    fn settings_reset(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_reset not implemented".to_string(),
            ))
        })
    }

    /// Default-provider self-heal (monorepo#3044), invoked by the transport
    /// after a `host.providerDiscovery` pass with the registry-ordered ids of
    /// the providers discovery reported as installed. When no default
    /// provider is derivable from settings and at least one installed
    /// provider exists, the implementation persists the first installed
    /// provider as `providers.active` (and, when a cached model catalog
    /// exists for it, its default model as a compound `model.default`).
    /// Idempotent, and never overwrites an existing settings value. Returns
    /// `{ healed: boolean, ... }`. Default: no-op (read-only wirings).
    fn settings_heal_default_provider(
        &self,
        installed_provider_ids: Vec<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = installed_provider_ids;
        Box::pin(async { Ok(serde_json::json!({ "healed": false })) })
    }

    /// `system.capabilities`: machine-level capabilities independent of any
    /// workspace — `{ cowSupported?: boolean }` (PROTOCOL §5.7). `cowSupported`
    /// reports the `CoW` probe of the workspaces root (`true`/`false` for a
    /// supported/unsupported filesystem, omitted when the probe cannot run) —
    /// the same cached probe that fills `Workspace.cowSupported` (§5.1).
    /// Unlike the `system.status`/`system.shutdown` control fast-path, this is
    /// a router method: it needs the service layer's workspaces-root
    /// resolution and aggregate cache, not composition-root daemon state.
    fn system_capabilities(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::system_capabilities not implemented".to_string(),
            ))
        })
    }

    /// `debug.sampleStacks`: capture a point-in-time sample of the daemon's
    /// own thread stacks over a short window and return the rendered text
    /// report — `{ report, durationMs, frequencyHz, sampleCount,
    /// distinctStacks }` (PROTOCOL §5.43, monorepo#1755). Both params are
    /// optional and clamped server-side (`durationMs` 100–10000, default
    /// 1000; `frequencyHz` 1–250, default 99). Unix-only; other platforms
    /// return `Error::Unsupported`. Daemon-global — no `workspaceId`.
    fn debug_sample_stacks(
        &self,
        duration_ms: Option<i64>,
        frequency_hz: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (duration_ms, frequency_hz);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::debug_sample_stacks not implemented".to_string(),
            ))
        })
    }

    /// `providers.catalog`: the static provider registry (monorepo#928) —
    /// `{ providers: [...] }`, one row per registered provider in registry
    /// order, so clients no longer need a local copy of the provider config.
    /// Each row is `{ id, displayName, shortName, command, canBeDisabled,
    /// loginCommandHint?, loginDocsUrl?, authErrorPatterns?,
    /// requiresEnvVar?, requiresFeatureCode?, visible }`; `visible` is the
    /// daemon-evaluated gating result (env var present / no feature code
    /// required) while the raw gating fields are passed through when set. No
    /// default designation and no tier metadata — clients derive an
    /// effective default from settings (`model.default` prefix, else
    /// `providers.active`). No params, no workspaceId — static data.
    fn providers_catalog(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::providers_catalog not implemented".to_string(),
            ))
        })
    }

    /// `unsloth.status`: observe the daemon-managed singleton Unsloth server
    /// (monorepo#878) — `{ running: boolean, repoId?, port?, pid?, uptimeSecs?,
    /// phase?, cpuPercent?, memoryBytes?, attachedAgentCount? }`. `running:
    /// false` (with every other field omitted) when no managed server is up;
    /// `attachedAgentCount` counts currently-tracked agents spawned with the
    /// `unsloth` provider, regardless of `running` (a stopped-but-attached
    /// state is possible mid-restart).
    fn unsloth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::unsloth_status not implemented".to_string(),
            ))
        })
    }

    /// `unsloth.stop`: gracefully terminate the managed Unsloth server (and
    /// its process tree) if one is running — `{ stopped: boolean }`.
    /// `stopped: false` is a no-op result, not an error, when no server was
    /// running. Safe to call while agents are attached; the daemon does not
    /// block or warn — callers should check `unsloth.status`'s
    /// `attachedAgentCount` first if a live-agent warning is desired.
    fn unsloth_stop(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::unsloth_stop not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // rules.* — user-rule overrides + (internal) prompt-injection (§18.1,
    // PROTOCOL §5.21). `list`/`get` are reads; `update` upserts the user
    // override. Only user-override entries are editable; file-sourced entries
    // are read-only over the wire. The injection pipeline that assembles these
    // into an agent's prompt is internal (§6.8) — not a method here.
    // ------------------------------------------------------------------------

    /// `rules.list`: all rule sources as `{ rules: RuleSet }` — every
    /// user-override type with content plus, when `workspace_id` is given, the
    /// live workspace rule files (read-only) (PROTOCOL §5.21).
    fn rules_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_list not implemented".to_string(),
            ))
        })
    }

    /// `rules.get`: `{ enabled, content, updatedAt }` for one user-override type
    /// (PROTOCOL §5.21).
    fn rules_get(
        &self,
        workspace_id: WorkspaceId,
        rule_type: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, rule_type);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_get not implemented".to_string(),
            ))
        })
    }

    /// `rules.update`: upsert a user-override body (+ optional `enabled`),
    /// re-read the set, and emit `settings:changed`; returns `{ rules: RuleSet }`
    /// (PROTOCOL §5.21).
    fn rules_update(
        &self,
        workspace_id: WorkspaceId,
        rule_type: String,
        content: String,
        enabled: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, rule_type, content, enabled);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_update not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // specialist.* — file-backed specialist definitions (PROTOCOL §5.11,
    // §18.2). Global (no `workspaceId`); resolved 3-tier project > user >
    // bundled. `list`/`get` read the resolved view; `create`/`edit`/`delete`
    // write user/project markdown-with-frontmatter files (`bundled` is
    // read-only). Nothing is persisted in SQLite.
    // ------------------------------------------------------------------------

    /// `specialist.list` → `{ specialists: SpecialistDef[] }` (user/project
    /// files override bundled). An optional `workspace_path` adds the project
    /// tier (PROTOCOL §5.11). An optional `provider` supplies the resolution
    /// context for the additive `resolvedModel`/`resolvedProvider` preview
    /// fields (defaults to the daemon's default provider; omitted when
    /// resolution yields the provider CLI default).
    fn specialist_list(
        &self,
        workspace_path: Option<String>,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_path, provider);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_list not implemented".to_string(),
            ))
        })
    }

    /// `specialist.get` → `{ specialist: SpecialistDef }`, the resolved view;
    /// unknown id → `-32602` (PROTOCOL §5.11). An optional `provider` supplies
    /// the resolution context for the additive `resolvedModel`/
    /// `resolvedProvider` preview fields (defaults to the daemon's default
    /// provider; omitted when resolution yields the provider CLI default).
    fn specialist_get(
        &self,
        id: String,
        workspace_path: Option<String>,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, workspace_path, provider);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_get not implemented".to_string(),
            ))
        })
    }

    /// `specialist.create` → write a new user/project file (default scope
    /// `user`); returns `{ specialist: SpecialistDef }` (PROTOCOL §5.11).
    fn specialist_create(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: Option<String>,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, spec, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_create not implemented".to_string(),
            ))
        })
    }

    /// `skill.list` → discovered skills for a workspace as a bare array of
    /// `{ name, description, location, scope, allowedTools?, compatibility? }`
    /// (name-sorted, scope: "project"|"user") (PROTOCOL §5.33).
    /// Unknown `workspace_id` → `-32602` (not found).
    fn skill_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::skill_list not implemented".to_string(),
            ))
        })
    }

    /// `specialist.edit` → overwrite an existing user/project file; returns
    /// `{ specialist: SpecialistDef }`; missing file → `-32602` (PROTOCOL §5.11).
    fn specialist_edit(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, spec, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_edit not implemented".to_string(),
            ))
        })
    }

    /// `specialist.delete` → remove a user/project file; returns
    /// `{ success: true }`; missing/bundled id → `-32602` (PROTOCOL §5.11).
    fn specialist_delete(
        &self,
        id: String,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_delete not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // mcp.servers.* — external MCP-server lifecycle/config (PROTOCOL §5.22,
    // §18.3). Config lives in the **sensitive** `mcp.servers` setting (`env`/
    // `headers` redacted over the wire); runtime status is not persisted and
    // transitions push `mcp.servers:status-changed`. Distinct from the §6.8
    // agent→BE callback.
    // ------------------------------------------------------------------------

    /// `mcp.servers.list` → `{ servers: McpServerConfig[] }` — configured
    /// external servers with sensitive `env`/`headers` redacted (PROTOCOL §5.22).
    fn mcp_servers_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_list not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.create` → add a server definition; returns
    /// `{ server: McpServerConfig }` (redacted) (PROTOCOL §5.22).
    fn mcp_servers_create(
        &self,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = config;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_create not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.update` → edit an existing definition; returns
    /// `{ server: McpServerConfig }` (redacted) (PROTOCOL §5.22).
    fn mcp_servers_update(
        &self,
        server_id: String,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, config);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_update not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.delete` → remove a definition (stopping it first); returns
    /// `{ success: true }` (PROTOCOL §5.22).
    fn mcp_servers_delete(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_delete not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.toggle` → enable (start) / disable (stop) a server; returns
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_toggle(
        &self,
        server_id: String,
        enabled: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, enabled);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_toggle not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.restart` → stop-then-start a server; returns
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_restart(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_restart not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.getStatus` → point read of one server's live status as
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_get_status(
        &self,
        server_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_get_status not implemented".to_string(),
            ))
        })
    }

    /// `mcp.oauth.list` → `{ tokens: [{ serverId, value }] }` — one entry per
    /// stored OAuth bag, `value` always the redaction placeholder. The bag
    /// itself never crosses the wire (PROTOCOL §5.22 companion).
    fn mcp_oauth_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_oauth_list not implemented".to_string(),
            ))
        })
    }

    /// `mcp.oauth.get` → `{ serverId, value }`; `value` is the redaction
    /// placeholder when a bag exists and `null` when it does not (PROTOCOL
    /// §5.22 companion).
    fn mcp_oauth_get(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_oauth_get not implemented".to_string(),
            ))
        })
    }

    /// `mcp.oauth.set` → persist `tokenBag` for `serverId` and return
    /// `{ serverId, value }` with the redaction placeholder as `value` (the
    /// bag itself is never echoed). PROTOCOL §5.22 companion.
    fn mcp_oauth_set(
        &self,
        server_id: String,
        token_bag: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, token_bag);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_oauth_set not implemented".to_string(),
            ))
        })
    }

    /// `mcp.oauth.delete` → drop the persisted bag for `serverId`; idempotent
    /// `{ success: true }` (PROTOCOL §5.22 companion).
    fn mcp_oauth_delete(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_oauth_delete not implemented".to_string(),
            ))
        })
    }

    /// `mcp.testConnection` → `{ status, statusCode?, errorMessage? }` — probe
    /// an HTTP/SSE MCP endpoint from the daemon host to detect whether it is
    /// reachable and whether it requires authentication, reusing the stored
    /// OAuth bag for `serverName` when no explicit `Authorization` header is
    /// supplied (PROTOCOL §5.22.2).
    fn mcp_test_connection(
        &self,
        url: String,
        headers: Option<serde_json::Value>,
        server_name: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (url, headers, server_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_test_connection not implemented".to_string(),
            ))
        })
    }

    /// `ws.mcp.listServers` (agent bridge/hook surface only — no wire
    /// method): the configured external MCP servers projected to a
    /// non-sensitive allowlist — `{ servers: [{ id, name, transport,
    /// enabled, state, toolCount? }] }`. `env`/`headers` never appear.
    /// Gated server-side on `agentFeatures.mcpTools` and
    /// `mcp.enableUserServers`.
    fn mcp_list_servers(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_list_servers not implemented".to_string(),
            ))
        })
    }

    /// `ws.mcp.listTools` (agent bridge/hook surface only — no wire method):
    /// forward `tools/list` to one running external MCP server, returning
    /// the raw MCP result (`{ tools: [...] }`). Same settings gates as
    /// [`Self::mcp_list_servers`], plus the per-server disabled list.
    fn mcp_list_tools(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_list_tools not implemented".to_string(),
            ))
        })
    }

    /// `ws.mcp.callTool` (agent bridge/hook surface only — no wire method):
    /// forward `tools/call` to one running external MCP server, returning
    /// the raw MCP result. `timeout_ms` is a caller override the hub caps at
    /// its own bound. Same settings gates as [`Self::mcp_list_tools`].
    fn mcp_call_tool(
        &self,
        server_id: String,
        tool_name: String,
        args: serde_json::Value,
        timeout_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, tool_name, args, timeout_ms);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_call_tool not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // accept-changes.* — commit→push→PR→merge orchestration (PROTOCOL §5.18).
    // ------------------------------------------------------------------------

    /// `accept-changes.getStatus`: the `WorkspaceGitStatus` for the accept-changes
    /// panel (branch, ahead/behind trunk, remote/push state, local commits with
    /// attribution, linked PR) (PROTOCOL §5.18).
    fn accept_changes_get_status(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_get_status not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.prepare`: validate an action and return a `PrepareResult`
    /// (warnings/errors, suggested commit message/PR fields, per-file stats)
    /// (PROTOCOL §5.18).
    fn accept_changes_prepare(
        &self,
        workspace_id: WorkspaceId,
        action: String,
        files: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, action, files);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_prepare not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.execute`: run the requested action (commit, optionally
    /// chaining push + create-PR via `options`) and return an `AcceptChangesResult`
    /// with per-step status (PROTOCOL §5.18). `params` is the full request object.
    fn accept_changes_execute(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_execute not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.mergePR`: merge the linked PR via the forge and return an
    /// `AcceptChangesResult` (PROTOCOL §5.18).
    fn accept_changes_merge_pr(
        &self,
        workspace_id: WorkspaceId,
        pr_number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            pr_number,
            merge_method,
            commit_title,
            commit_message,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_merge_pr not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.addRemote`: add (and, if needed, initialize) the `origin`
    /// remote, returning the refreshed `WorkspaceGitStatus` (PROTOCOL §5.18).
    fn accept_changes_add_remote(
        &self,
        workspace_id: WorkspaceId,
        remote_url: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, remote_url);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_add_remote not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // search.* — BE-owned file/path search (PROTOCOL §5.15).
    // ------------------------------------------------------------------------

    /// `search.inFiles`: gitignore-aware ripgrep content search over the
    /// workspace worktree. `opts` is the raw `{ caseSensitive?, regex?, globs?,
    /// maxResults? }` object; a malformed `opts.regex` surfaces as
    /// `InvalidParams` ("Invalid regex"). Returns `{ requestId, matches,
    /// truncated }`, minting `request_id` when absent (PROTOCOL §5.15).
    fn search_in_files(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        opts: Option<serde_json::Value>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, query, opts, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_in_files not implemented".to_string(),
            ))
        })
    }

    /// `search.fileNames`: gitignore-aware path/glob filename search over the
    /// worktree. Returns `{ requestId, files, truncated }`, minting `request_id`
    /// when absent (PROTOCOL §5.15).
    fn search_file_names(
        &self,
        workspace_id: WorkspaceId,
        pattern: String,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, pattern, limit, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_file_names not implemented".to_string(),
            ))
        })
    }

    /// `search.cancel`: abort an in-flight search by its `requestId`. A no-op
    /// success for an unknown/already-finished id (`{ ok: true }`) (PROTOCOL §5.15).
    fn search_cancel(&self, request_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_cancel not implemented".to_string(),
            ))
        })
    }

    /// `search.messages`: full-text (FTS5, bm25-ranked) search over persisted
    /// user/assistant agent messages. `workspace_id` `None` → global across
    /// all workspaces; `Some` → hard scope filter. `prefer_workspace_id` is a
    /// soft ranking boost: results stay global but matches from that workspace
    /// outrank equally-relevant matches elsewhere. Archived-workspace matches
    /// carry a soft ranking penalty, so equally-relevant matches tier
    /// preferred → other active → archived. Returns
    /// `{ requestId, matches: MessageMatch[] }` inline, or
    /// `{ requestId, matches: [] }` (a prompt ack) when the result set is
    /// streamed via `search:result`/`search:done` (PROTOCOL §5.15 / §6.5).
    #[allow(clippy::too_many_arguments)]
    fn search_messages(
        &self,
        workspace_id: Option<WorkspaceId>,
        query: String,
        agent_id: Option<String>,
        role: Option<String>,
        limit: Option<i64>,
        prefer_workspace_id: Option<WorkspaceId>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            query,
            agent_id,
            role,
            limit,
            prefer_workspace_id,
            request_id,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_messages not implemented".to_string(),
            ))
        })
    }

    /// `search.events`: substring search over the BE event log. `workspaceId` is
    /// optional (absent → all workspaces). Returns `{ requestId, matches:
    /// EventMatch[] }` inline or a streamed prompt ack (PROTOCOL §5.15 / §6.5).
    fn search_events(
        &self,
        query: String,
        workspace_id: Option<WorkspaceId>,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, workspace_id, limit, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_events not implemented".to_string(),
            ))
        })
    }

    /// `search.notes`: GLOBAL substring search over the BE notes store (no
    /// `workspaceId`). Returns `{ requestId, matches: NoteMatch[] }` (PROTOCOL §5.15).
    fn search_notes(
        &self,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_notes not implemented".to_string(),
            ))
        })
    }

    /// `search.codebase`: v1 ripgrep/symbol-backed content search over a
    /// workspace worktree (context-engine wiring deferred, §8). Returns
    /// `{ requestId, matches: CodebaseMatch[] }` inline or a streamed prompt ack
    /// (PROTOCOL §5.15 / §6.5).
    fn search_codebase(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, query, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_codebase not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // terminal.* — interactive PTYs on the unified host (PROTOCOL §5.13, §12).
    // ------------------------------------------------------------------------

    /// `terminal.create`: spawn a PTY (default shell when `command` is absent)
    /// scoped to the workspace and start fanning its output to subscribers as
    /// `terminal:data` events. `env` is an optional overlay layered over the
    /// daemon's inherited environment so callers can pass through per-terminal
    /// variables (e.g. `FORCE_COLOR`, `PATH` additions). Returns
    /// `{ terminalId }` (PROTOCOL §5.13).
    fn terminal_create(
        &self,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, cols, rows, cwd, command, env);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_create not implemented".to_string(),
            ))
        })
    }

    /// `terminal.write`: write base64-encoded input bytes to a PTY's stdin
    /// (writes are serialized into the single master). Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_write(
        &self,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, data);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_write not implemented".to_string(),
            ))
        })
    }

    /// `terminal.resize`: resize a PTY's visible area. Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_resize(
        &self,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, cols, rows);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_resize not implemented".to_string(),
            ))
        })
    }

    /// `terminal.kill`: signal/terminate a PTY's process group; the streamer
    /// emits `terminal:exit` when the process ends. Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_kill(&self, terminal_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = terminal_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_kill not implemented".to_string(),
            ))
        })
    }

    /// `terminal.getBuffer`: snapshot a PTY's server-side scrollback for replay,
    /// base64-encoded (optionally trailing `maxBytes`). Returns
    /// `{ terminalId, data }` (PROTOCOL §5.13).
    fn terminal_get_buffer(
        &self,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, max_bytes);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_get_buffer not implemented".to_string(),
            ))
        })
    }

    /// `terminal.list`: the workspace's live terminals wrapped in the per-boot
    /// envelope `{ terminals: [{ id, name, cwd, isExecutingCommand }],
    /// daemonBootId }` (PROTOCOL §5.13; monorepo#1334). `daemonBootId` is the
    /// daemon's per-process boot id, stable within one daemon lifetime and
    /// fresh after a restart.
    fn terminal_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_list not implemented".to_string(),
            ))
        })
    }

    /// `terminal.readOutput`: a formatted, ANSI-stripped string view of a
    /// terminal's scrollback, keeping the trailing `max_lines` (default 200)
    /// (PROTOCOL §5.13).
    fn terminal_read_output(
        &self,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, terminal_id, max_lines, paginate, page_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_read_output not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // script.* — named processes on the unified PTY host (PROTOCOL §5.8, §12.2).
    // ------------------------------------------------------------------------

    /// `script.list`: the workspace's scripts with runtime state as
    /// `{ scripts: [...] }` (PROTOCOL §5.8).
    fn script_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_list not implemented".to_string(),
            ))
        })
    }

    /// `script.create`: register a script definition; returns the created
    /// [`Script`](crate::model::Script) (PROTOCOL §5.8).
    fn script_create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_create not implemented".to_string(),
            ))
        })
    }

    /// `script.remove`: stop (if running) and forget a script; returns
    /// `{ ok, scriptId }` (PROTOCOL §5.8). Workspace-scoped: the callee
    /// looks up the script under `(workspace_id, script_id)` so a script id
    /// owned by another workspace surfaces as `NotFound`.
    fn script_remove(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_remove not implemented".to_string(),
            ))
        })
    }

    /// `script.start`: spawn the script on the PTY host (service mode auto-
    /// restarts per policy); returns `{ ok, scriptId }` (PROTOCOL §5.8).
    /// Workspace-scoped.
    fn script_start(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_start not implemented".to_string(),
            ))
        })
    }

    /// `script.stop`: stop a running script (cancels pending auto-restart);
    /// returns `{ ok, scriptId }` (PROTOCOL §5.8). Workspace-scoped.
    fn script_stop(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_stop not implemented".to_string(),
            ))
        })
    }

    /// `script.restart`: stop then start, resetting the restart counter; returns
    /// `{ ok, scriptId }` (PROTOCOL §5.8). Workspace-scoped.
    fn script_restart(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_restart not implemented".to_string(),
            ))
        })
    }

    /// `script.output`: the script's current PTY scrollback as plaintext
    /// output-buffer text (optionally trailing `maxLines`, default 100); returns
    /// a bare string (`"No output yet."` when empty), not an object (§5.8).
    /// Workspace-scoped.
    fn script_output(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id, max_lines, paginate, page_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_output not implemented".to_string(),
            ))
        })
    }

    /// `script.status`: the script's [`ScriptRuntimeState`](crate::model::ScriptRuntimeState)
    /// (PROTOCOL §5.8). Workspace-scoped.
    fn script_status(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_status not implemented".to_string(),
            ))
        })
    }

    /// `script.run`: run a command-mode script to completion (optional
    /// `timeoutSeconds`), returning `{ exitCode?, output, timedOut?, warning? }`;
    /// service-mode scripts return a `warning` (PROTOCOL §5.8). Workspace-scoped.
    fn script_run(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, script_id, max_lines, timeout_seconds);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_run not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // client.hello + drafts.* — stable client identity & per-client drafts
    // (PROTOCOL §5.16/§5.17). These back the transport-level interceptors;
    // they are not routed through the JSON-RPC dispatcher, but live on
    // `WorkspaceApi` so the transport reaches persistence through services
    // without depending on `intent-store` (per the dependency-direction rules).
    // ------------------------------------------------------------------------

    /// `client.hello` persistence: upsert the logical `client` row, setting
    /// `first_seen` once and touching `last_seen`, and persisting `name` /
    /// `capabilities` (a JSON bag). The connection→client binding and the
    /// `server` capability block are transport concerns (§16) (PROTOCOL §5.17).
    fn upsert_client(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (client_id, name, capabilities);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::upsert_client not implemented".to_string(),
            ))
        })
    }

    /// `drafts.get`: the draft for the calling client (`client_id` resolved from
    /// the connection, never a param), or `None` when absent (PROTOCOL §5.16).
    fn draft_get(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<Option<Draft>>> {
        let _ = (workspace_id, agent_id, client_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_get not implemented".to_string(),
            ))
        })
    }

    /// `drafts.set`: upsert the calling client's draft. An empty `text` with no
    /// `attachments` is a clear (the row is deleted); empty text WITH
    /// attachments persists the row. `attachments` is an opaque JSON array
    /// stored verbatim (`None` ⇒ none stored). Returns `Some(updatedAt)` when a
    /// draft was stored or `None` when it was cleared, and emits
    /// `draft:changed` (carrying `hasDraft`, never the content) (PROTOCOL
    /// §5.16/§6.5).
    fn draft_set(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
        text: String,
        attachments: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<Option<String>>> {
        let _ = (workspace_id, agent_id, client_id, text, attachments);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_set not implemented".to_string(),
            ))
        })
    }

    /// `drafts.clear`: delete the calling client's draft (idempotent if none),
    /// emitting `draft:changed` with `hasDraft: false` (PROTOCOL §5.16/§6.5).
    fn draft_clear(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (workspace_id, agent_id, client_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_clear not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // file.* — workspace-scoped filesystem access (PROTOCOL §5.10). Every path
    // is validated within the resolved workspace root; access-denied and other
    // filesystem failures surface as `Error::Internal` (→ `-32603`), matching
    // the TS handler which wraps the builder errors in `INTERNAL_ERROR`.
    // ------------------------------------------------------------------------

    /// `file.read`: the file's UTF-8 contents as a **bare JSON string** (not an
    /// object), per the TS `ws.file.read` builder (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_read(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_read not implemented".to_string(),
            ))
        })
    }

    /// `file.readChunk`: one offset-windowed slice of a workspace file's raw
    /// bytes as `{ content (base64), bytesRead, size }` — the FE-ward binary
    /// counterpart of the UTF-8-only `file.read` (PROTOCOL §5.9;
    /// monorepo#2458). `length` is capped at 16 MiB decoded (over-cap →
    /// `Error::InvalidParams`); a read at/past EOF returns an empty chunk.
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_read_chunk(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        offset: u64,
        length: u64,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, offset, length, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_read_chunk not implemented".to_string(),
            ))
        })
    }

    /// `file.write`: create/overwrite a file (parent dirs created); returns
    /// `{ ok: true, path, size }` where `size` is the content byte/char length
    /// (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_write(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        content: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, content, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_write not implemented".to_string(),
            ))
        })
    }

    /// `file.list`: directory entries as a **bare array** of
    /// `{ name, type }` (`type` = `"file"`/`"directory"`); `path` defaults to
    /// `"."` (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_list(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_list not implemented".to_string(),
            ))
        })
    }

    /// `file.delete`: remove a single file (rejects directories); returns
    /// `{ ok: true, path, deleted: true }` (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_delete(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_delete not implemented".to_string(),
            ))
        })
    }

    /// `file.mkdir`: create a directory (recursive); returns
    /// `{ ok: true, path, created: true }`, or `{ ok: true, path, existed: true }`
    /// when the directory already exists (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_mkdir(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_mkdir not implemented".to_string(),
            ))
        })
    }

    /// `file.rename`: move a file/directory (destination must not exist);
    /// returns `{ ok: true, oldPath, newPath, renamed: true, isDirectory }`
    /// (PROTOCOL §5.10).
    /// `caller_agent_id` enables `CoW` sandbox containment (prefers sandbox path).
    fn file_rename(
        &self,
        workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, old_path, new_path, caller_agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_rename not implemented".to_string(),
            ))
        })
    }

    /// `file.tree`: directory entries directly under `path` (defaulting to the
    /// workspace root) as a **bare array** of `{ path, name, isDirectory }`
    /// (camelCase on the wire). The FE anchors the explorer on the root and
    /// lazy-lists children via `file.list`, so a shallow listing is sufficient.
    /// Shares the same within-workspace guard as the other file ops.
    fn file_tree(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tree not implemented".to_string(),
            ))
        })
    }

    /// `file.exists`: existence + type probe returning
    /// `{ exists, isFile, isDirectory }` (PROTOCOL §5.9). Mirrors the legacy
    /// `FileExistsResult` shape so retirement-wave consumers swap over 1:1;
    /// lookup errors collapse to `{ exists: false, isFile: false, isDirectory: false }`.
    fn file_exists(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_exists not implemented".to_string(),
            ))
        })
    }

    /// `file.stat`: file/directory metadata as
    /// `{ size, mtime, isFile, isDirectory, isSymlink, permissions }`
    /// (PROTOCOL §5.9). Mirrors the legacy `StatResult` shape. Symlinks are
    /// followed for size/type reporting; `permissions` is the octal mode
    /// string (`"0644"`).
    fn file_stat(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_stat not implemented".to_string(),
            ))
        })
    }

    /// `file.placeAttachment`: place an attachment payload into the
    /// workspace's `.intent/attachments/` directory with a collision-safe
    /// name and return `{ ok, path, fileName, size, attachmentId, mimeType?,
    /// uploadedAt }` where `path` is workspace-relative and `size` is the
    /// placed byte length (PROTOCOL §5.9; intent-hq/monorepo#1948). Exactly
    /// one of `data` (base64 payload, `data:` URL prefix tolerated) or
    /// `source_path` (absolute host-local file to copy — the daemon and
    /// caller share the host) must be provided; anything else is
    /// `Error::InvalidParams` (→ `-32602`). The directory is covered by the
    /// default `.intent/.gitignore`, so placed files never reach git
    /// tracking, auto-commit, or attribution. Placement also registers the
    /// file in the attachment registry (`attachments` table) under a
    /// daemon-minted UUID — the additive `attachmentId` / `mimeType?` /
    /// `uploadedAt` result fields (presence-detected; old clients unaffected)
    /// — so agents can retrieve it later via `ws.file.getAttachment`.
    /// `mime_type` is the optional client-supplied MIME type, recorded
    /// verbatim.
    fn file_place_attachment(
        &self,
        workspace_id: WorkspaceId,
        file_name: String,
        data: Option<String>,
        source_path: Option<String>,
        mime_type: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, file_name, data, source_path, mime_type);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_place_attachment not implemented".to_string(),
            ))
        })
    }

    /// `file.attachmentUpload.begin`: open a staged chunked attachment
    /// upload session for payloads larger than one RPC frame (PROTOCOL
    /// §5.9). Validates the header — the workspace must exist, `file_name`
    /// non-empty, `size_bytes` positive and within the 1 GiB attachment cap,
    /// `sha256` 64 hex chars — and returns `{ uploadId, maxChunkBytes }`.
    fn file_attachment_upload_begin(
        &self,
        workspace_id: WorkspaceId,
        file_name: String,
        size_bytes: u64,
        sha256: String,
        mime_type: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, file_name, size_bytes, sha256, mime_type);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_attachment_upload_begin not implemented".to_string(),
            ))
        })
    }

    /// `file.attachmentUpload.chunk`: stage one seq-numbered base64 slice of
    /// the payload; retrying a seq is idempotent and chunks may arrive in
    /// any order (PROTOCOL §5.9). Returns `{ uploadId, seq, receivedBytes }`.
    fn file_attachment_upload_chunk(
        &self,
        upload_id: String,
        seq: u64,
        data: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (upload_id, seq, data);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_attachment_upload_chunk not implemented".to_string(),
            ))
        })
    }

    /// `file.attachmentUpload.commit`: verify the assembled payload's
    /// SHA-256 and place it through the same collision-safe placement +
    /// attachment-registry path as `file.placeAttachment` (PROTOCOL §5.9).
    /// The result is byte-shape-identical to a successful
    /// `file.placeAttachment`: `{ ok, path, fileName, size, attachmentId,
    /// mimeType?, uploadedAt }`.
    fn file_attachment_upload_commit(
        &self,
        upload_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = upload_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_attachment_upload_commit not implemented".to_string(),
            ))
        })
    }

    /// `file.attachmentUpload.abort`: drop the staged upload session and
    /// delete its staging directory (PROTOCOL §5.9). Idempotent — aborting
    /// an unknown id succeeds quietly. Returns `{ uploadId, aborted }`.
    fn file_attachment_upload_abort(
        &self,
        upload_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = upload_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_attachment_upload_abort not implemented".to_string(),
            ))
        })
    }

    /// `file.getAttachmentInfo`: attachment-registry metadata lookup
    /// (PROTOCOL §5.9) → `{ attachmentId, fileName, mimeType?, size,
    /// uploadedAt, path, exists }`. `path` is the stored workspace-relative
    /// path (under `.intent/attachments/`) and `exists` reflects whether the
    /// file is still on disk (a user may delete it out-of-band; the registry
    /// row survives). Unknown id → `Error::NotFound`.
    fn file_get_attachment_info(
        &self,
        attachment_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = attachment_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_get_attachment_info not implemented".to_string(),
            ))
        })
    }

    /// MCP `ws.file.getAttachment` backing op: copy a registered attachment
    /// from the CANONICAL workspace store into the calling agent's own
    /// working directory (the canonical checkout for shared-mode agents, the
    /// sandbox clone for CoW-sandboxed agents — resolved from the caller's
    /// session like other sandbox-aware bindings) under the git-ignored
    /// `.intent/attachments/` directory, and return `{ path, fileName,
    /// mimeType?, size, uploadedAt }` with `path` relative to that working
    /// directory. The copy is skipped when an identical file is already
    /// present. Two DISTINCT failure modes (never conflated): an unknown
    /// `attachment_id` is `Error::NotFound` ("unknown attachment id"), while
    /// a registry row whose stored file is gone from disk is
    /// `Error::Internal` naming the original `fileName` and `uploadedAt` and
    /// telling the model to continue without the file rather than retry.
    fn file_get_attachment(
        &self,
        workspace_id: WorkspaceId,
        attachment_id: String,
        caller_agent_id: Option<AgentId>,
        dest_dir: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, attachment_id, caller_agent_id, dest_dir);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_get_attachment not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // primitive.* — append a fenced ```ws-block:<type>``` JSON primitive to a
    // note. Each returns `{ ok: true, primitiveId, noteId, content }` where
    // `content` is the full note text after the append, matching the TS
    // `appendPrimitiveBlock` builder (PROTOCOL §5.x). Every primitive carries
    // `version: 1` and `createdBy: "agent"`.
    // ------------------------------------------------------------------------

    /// `primitive.addReference`: append a `reference` primitive (`target.kind`
    /// = `symbol` for `#symbol:` semantic ids, else `file_range`; optional code
    /// `snapshot`).
    fn primitive_add_reference(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        semantic_id: String,
        description: String,
        snapshot: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, semantic_id, description, snapshot);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_reference not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addCli`: append a `cli` primitive (`cwd` defaults to `"./"`,
    /// `display.showCommandPrefix` = `"$"`).
    fn primitive_add_cli(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        command: String,
        description: String,
        working_directory: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            note_id,
            command,
            description,
            working_directory,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_cli not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addPatch`: append a `patch` primitive with a single-entry
    /// `patches: [{ filePath, diff }]` array.
    fn primitive_add_patch(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        file_path: String,
        diff: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, file_path, diff, description);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_patch not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addAgentAction`: append an `agent_action` primitive with empty
    /// `inputs`.
    fn primitive_add_agent_action(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
        goal: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, agent_id, goal, description);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_agent_action not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // crossWorkspace.* — read-only access to sibling workspaces that share the
    // caller's `repositoryPath`. Sibling-scope and access-control failures
    // mirror the TS builder messages ("Current workspace is not associated with
    // a repository", "Access denied: ...", "Note not found: ...") and surface
    // as `Error::Internal` (→ `-32603`), matching the TS handler which wraps the
    // builder errors in `INTERNAL_ERROR` (PROTOCOL §5.x).
    // ------------------------------------------------------------------------

    /// `crossWorkspace.listSiblings`: workspaces sharing the caller's
    /// `repositoryPath`, excluding self. Bare array of
    /// `{ id, title, branch, status, createdAt, updatedAt }` (`title` defaults
    /// to `"Untitled"`; `status` is the `PascalCase` `WorkspaceStatus`).
    fn cross_workspace_list_siblings(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_list_siblings not implemented".to_string(),
            ))
        })
    }

    /// `crossWorkspace.listNotes`: notes in a sibling workspace as a bare array
    /// of `{ id, title, createdAt, updatedAt }`.
    fn cross_workspace_list_notes(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, target_workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_list_notes not implemented".to_string(),
            ))
        })
    }

    /// `crossWorkspace.readNote`: one note in a sibling workspace as
    /// `{ id, title, content, numberedContent, sourceWorkspaceId,
    /// sourceWorkspaceTitle, branch, lineCount }`.
    fn cross_workspace_read_note(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, target_workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_read_note not implemented".to_string(),
            ))
        })
    }

    /// `browser.exec` (PROTOCOL §5.14): validate + forward a batch of CDP
    /// actions to the connected frontend and reshape the reply. The MCP
    /// binding (`ws.browser.exec`) calls this; the concrete implementation
    /// wraps a per-connection reverse channel (owned by `intent-transport`).
    /// The default returns an internal error so units of the codebase that
    /// have no reverse channel available (agent-MCP without a wired FE) fail
    /// loudly rather than silently drop the call.
    fn browser_exec(
        &self,
        workspace_id: WorkspaceId,
        actions: Vec<serde_json::Value>,
        tab_id: Option<String>,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, actions, tab_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::browser_exec not implemented".to_string(),
            ))
        })
    }

    /// `ws.host.exec`: one-shot process exec on the daemon host for the
    /// agent-JS binding, with the wire `host.exec` semantics (PROTOCOL §5.14):
    /// argv-only (no shell interpolation), `timeoutMs` reaps the whole process
    /// group, workspace-cwd containment, and secret-safe env (values never
    /// logged or returned). `params` carries the raw
    /// `{ command, args?, cwd?, env?, timeoutMs? }` object; `workspace_id`
    /// anchors the cwd containment guard — a caller-supplied `workspaceId`
    /// cannot retarget it. The concrete implementation delegates to
    /// `intent-services::host_exec::run`.
    fn host_exec(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::host_exec not implemented".to_string(),
            ))
        })
    }

    /// `ws.hook.schedule` / wire `hook.schedule`: register a background hook
    /// (an agent-owned scheduled script) after one immediate real run.
    /// `params` carries `{ name, code, delayMs }`; `agent_id` is the owning
    /// agent (the MCP caller). Returns the persisted hook on success.
    fn hook_schedule(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::hook_schedule not implemented".to_string(),
            ))
        })
    }

    /// `ws.hook.list` / wire `hook.list`: hooks in a workspace, optionally
    /// narrowed to one owning agent, as `{ hooks: [Hook] }`.
    fn hook_list(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::hook_list not implemented".to_string(),
            ))
        })
    }

    /// `ws.hook.get` (MCP-only, no wire method): one hook row by id — the
    /// full row including `code`, for active AND terminal (retired) hooks,
    /// so an agent can recover a retired hook's script to re-arm it. Hooks
    /// belonging to another workspace read as `NotFound`.
    fn hook_get(
        &self,
        workspace_id: WorkspaceId,
        hook_id: HookId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, hook_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::hook_get not implemented".to_string(),
            ))
        })
    }

    /// `ws.hook.cancel` / wire `hook.cancel`: stop an active hook. `caller`
    /// is the cancelling agent (MCP): an agent may only cancel its own hooks
    /// — a non-owner is rejected — and an owner cancel delivers no self-wake.
    /// `None` is the FE path: any hook can be cancelled and the owner is
    /// additionally woken with a cancellation notice.
    fn hook_cancel(
        &self,
        workspace_id: WorkspaceId,
        hook_id: HookId,
        caller: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, hook_id, caller);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::hook_cancel not implemented".to_string(),
            ))
        })
    }

    /// `ws.hook.runNow` / wire `hook.runNow`: trigger an immediate run of an
    /// active hook, resetting its inter-run timer.
    fn hook_run_now(
        &self,
        workspace_id: WorkspaceId,
        hook_id: HookId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, hook_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::hook_run_now not implemented".to_string(),
            ))
        })
    }

    /// `ws.pr.monitor`: register (idempotently) a centralized monitor on
    /// `pr_number` for `agent_id`, returning the monitor row plus the freshly
    /// fetched merge-requirements checklist. `repo` is an optional
    /// `"owner/name"` override; `None` resolves the workspace repo. MCP-only
    /// — monitors are agent-owned, so there is no wire registration method.
    fn pr_monitor_start(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        pr_number: u64,
        repo: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, pr_number, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_monitor_start not implemented".to_string(),
            ))
        })
    }

    /// `ws.pr.unmonitor`: cancel the calling agent's own active monitor on
    /// `(repo, pr_number)`. MCP-only, and never self-wakes the owner.
    fn pr_monitor_stop(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        pr_number: u64,
        repo: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, pr_number, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_monitor_stop not implemented".to_string(),
            ))
        })
    }

    /// `ws.pr.monitors` / wire `prMonitor.list`: monitors as
    /// `{ monitors: [...] }`. `agent_id` narrows to one owning agent (the MCP
    /// caller's own); `None` is the workspace-wide FE view. Cancelled rows are
    /// excluded, `completed` rows retained so merged PRs stay visible.
    fn pr_monitor_list(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_monitor_list not implemented".to_string(),
            ))
        })
    }

    /// Wire `prMonitor.cancel`: cancel any monitor in the workspace by id and
    /// notify the owning agent that its monitor is gone (the FE path — the
    /// agent path is `pr_monitor_stop`, which never self-wakes).
    fn pr_monitor_cancel_by_id(
        &self,
        workspace_id: WorkspaceId,
        monitor_id: PrMonitorId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, monitor_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_monitor_cancel_by_id not implemented".to_string(),
            ))
        })
    }

    /// Wire `prMonitor.flush`: deliver a monitor's pending consolidated wake
    /// now, bypassing the remaining debounce window. A no-op
    /// (`{ ok: true, flushed: false }`) when nothing is pending. With
    /// `check: true`, the daemon first re-polls the monitor on demand so the
    /// flush covers changes the poll loop has not seen yet.
    fn pr_monitor_flush_pending(
        &self,
        workspace_id: WorkspaceId,
        monitor_id: PrMonitorId,
        check: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, monitor_id, check);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_monitor_flush_pending not implemented".to_string(),
            ))
        })
    }
}

/// Whether a context engine is usable right now (§8.1). `Unavailable` is a
/// first-class, non-error state — never something that fails a request (§8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineAvailability {
    /// A working engine is present; `version` is best-effort.
    Available {
        name: String,
        version: Option<String>,
    },
    /// No usable engine (not installed, not logged in, …) — not an error.
    Unavailable { reason: String },
}

/// Natural-language code/context retrieval request scoped to a workspace (§8.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveRequest {
    pub workspace_id: WorkspaceId,
    pub workspace_path: PathBuf,
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,
}

/// A single retrieved code/context hit (§8.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievedItem {
    /// Workspace-relative file path.
    pub file: String,
    /// Detected symbol/identifier, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// 1-based line, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    /// Snippet/preview of the matched content.
    pub preview: String,
    /// Relevance score, when provided by the engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// The result of a [`ContextEngine::retrieve`] call (§8.1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveResult {
    pub items: Vec<RetrievedItem>,
}

/// Errors raised by a [`ContextEngine`] (§8.1, §11.1). Construction never fails
/// the daemon and `availability()` never errors; these surface only from
/// `retrieve()`.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    /// No usable engine for this request (maps to `ContextUnavailable`, §11.1).
    #[error("context engine unavailable: {reason}")]
    Unavailable { reason: String },
    /// Spawning the engine process failed.
    #[error("failed to spawn context engine: {0}")]
    Spawn(String),
    /// The engine did not return within the timeout.
    #[error("context engine timed out")]
    Timeout,
    /// The engine ran but exited unsuccessfully.
    #[error("context engine failed: {0}")]
    CommandFailed(String),
    /// The engine's output could not be parsed.
    #[error("failed to parse context engine output: {0}")]
    Parse(String),
}

/// Context-engine abstraction implemented by `intent-context` (§3.1, §8.1).
///
/// Code retrieval is an **optional** capability: the daemon degrades gracefully
/// when no engine is available (§8.3). `availability()` is a non-error probe;
/// only `retrieve()` can fail with [`ContextError`].
#[async_trait::async_trait]
pub trait ContextEngine: Send + Sync {
    /// Is a working engine available right now?
    async fn availability(&self) -> EngineAvailability;

    /// Natural-language code/context retrieval scoped to a workspace.
    async fn retrieve(
        &self,
        req: RetrieveRequest,
    ) -> std::result::Result<RetrieveResult, ContextError>;
}

/// Why an agent-initiated reverse RPC could not be delivered (REV-1). Kept as a
/// small named enum so the service layer can distinguish "no client connected"
/// from a transport-level failure without inspecting error strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReverseDispatchError {
    /// No client is currently registered as the sticky reverse target — no
    /// live connection to route the request to.
    NoClient,
    /// The reverse RPC could not be completed successfully — covers delivery
    /// failures (e.g. the outbound queue was closed before the request left
    /// the daemon), transport-level failures (timeout waiting for the
    /// response, connection dropped in-flight), and JSON-RPC error replies
    /// returned by the client. In other words: anything that isn't
    /// [`NoClient`](Self::NoClient) and isn't a successful `result`. The
    /// daemon does not interpret `code`; it just carries whatever the client
    /// / transport surfaced.
    Transport { code: i64, message: String },
}

impl std::fmt::Display for ReverseDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReverseDispatchError::NoClient => f.write_str("no client connected"),
            ReverseDispatchError::Transport { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for ReverseDispatchError {}

/// Agent-initiated daemon→client reverse-RPC seam (REV-1, PROTOCOL §5.14/§12.4).
///
/// Provides the sticky "first client wins" routing decision the service layer
/// needs when an agent (not a per-connection client) triggers a reverse intent
/// (currently `browser.exec`). The concrete implementation lives in
/// `intent-transport` (a shared registry of live `ReverseChannel`s ordered by
/// arrival); `intent-services` holds it as `Arc<dyn AgentReverseDispatch>` so
/// the crate graph stays acyclic (§3.2).
///
/// Semantics: `dispatch` returns the same JSON `Value` the connected client
/// echoed back verbatim, or a [`ReverseDispatchError`] describing why the
/// request could not be delivered. `is_connected` is a cheap synchronous probe
/// that lets the service surface a friendlier error before it composes the
/// forward params.
pub trait AgentReverseDispatch: Send + Sync {
    /// Whether at least one client is currently registered as a reverse target.
    fn is_connected(&self) -> bool;

    /// Dispatch a reverse JSON-RPC request to the sticky primary client and
    /// await its response.
    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: serde_json::Value,
    ) -> BoxFuture<'a, std::result::Result<serde_json::Value, ReverseDispatchError>>;
}

/// Minimal event structure for `WorkspaceApi::publish_event` (used by bindings
/// that don't import `intent_store::NewEvent`).
#[derive(Debug, Clone)]
pub struct PublishEvent {
    pub workspace_id: crate::ids::WorkspaceId,
    pub event_type: String,
    pub data: serde_json::Value,
}
