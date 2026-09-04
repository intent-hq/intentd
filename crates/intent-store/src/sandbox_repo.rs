//! Sandbox repository: CRUD for agent sandboxes (`CoW` isolation).

use intent_core::{AgentId, Result, WorkspaceId};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

/// Sandbox status lifecycle. Wire values are the `snake_case` names — the same
/// strings `to_db` writes — so clients never see undocumented spellings like
/// `mergepending` (the old `lowercase` rename collapsed the separator).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxStatus {
    /// Initial state after provisioning
    Created,
    /// Merge-back is in progress
    Merging,
    /// Successfully merged to canonical (sandbox persists for the agent's
    /// lifetime; `last_merged_commit_sha` marks the merged tip)
    Merged,
    /// Discarded without merging
    Discarded,
    /// Conflict detected, agent bounced with instructions (live retry loop)
    ConflictBounced,
    /// Merge pending manual resolution (blocked or transient failure)
    MergePending,
    /// TERMINAL: deterministic merge conflict with no live agent turn to
    /// bounce (retry cap exhausted, sweep conflict, or manual merge
    /// conflict). `conflicting_paths` names the clash; the sandbox's commits
    /// are pushed to a recovery branch in the canonical repo. Resolved only
    /// by a manual `sandbox.cow.merge` (after canonical changes) or
    /// `sandbox.cow.discard`.
    Conflict,
}

impl SandboxStatus {
    fn to_db(self) -> &'static str {
        match self {
            SandboxStatus::Created => "created",
            SandboxStatus::Merging => "merging",
            SandboxStatus::Merged => "merged",
            SandboxStatus::Discarded => "discarded",
            SandboxStatus::ConflictBounced => "conflict_bounced",
            SandboxStatus::MergePending => "merge_pending",
            SandboxStatus::Conflict => "conflict",
        }
    }

    fn from_db(s: &str) -> Result<Self> {
        match s {
            "created" => Ok(SandboxStatus::Created),
            "merging" => Ok(SandboxStatus::Merging),
            "merged" => Ok(SandboxStatus::Merged),
            "discarded" => Ok(SandboxStatus::Discarded),
            "conflict_bounced" => Ok(SandboxStatus::ConflictBounced),
            "merge_pending" => Ok(SandboxStatus::MergePending),
            // Terminal conflict; also absorbs the legacy migration-0038 value.
            "conflict" => Ok(SandboxStatus::Conflict),
            _ => Err(intent_core::Error::Internal(format!(
                "invalid sandbox status: {s}"
            ))),
        }
    }
}

/// Sandbox record for a CoW-isolated agent workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sandbox {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub path: String,
    pub branch: String,
    pub base_commit_sha: String,
    pub snapshot_commit_sha: Option<String>,
    /// Tip of the last successfully merged range. Sandboxes persist across
    /// turns (merge-on-completion no longer discards them), so repeat merges
    /// start the next cherry-pick range here instead of base/snapshot.
    pub last_merged_commit_sha: Option<String>,
    pub status: SandboxStatus,
    pub retry_count: i64,
    /// Whether the completion path auto-merges this sandbox when the agent's
    /// turn ends. `false` (parent opted out via `mergeOnTurnEnd: false`) keeps
    /// the sandbox live at turn end; merging happens only via the manual
    /// `sandbox.cow.merge` RPC. The retry sweep also skips such sandboxes.
    pub merge_on_turn_end: bool,
    /// Conflicting paths persisted whenever a merge attempt conflicts: on
    /// `conflict_bounced` (agent reconciling, next attempt overwrites them)
    /// and on the terminal `conflict` status; empty otherwise. Serialized on
    /// the wire so clients see what conflicted without re-running the merge.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicting_paths: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

const COLUMNS: &str = "id, workspace_id, agent_id, path, branch, base_commit_sha, \
    snapshot_commit_sha, last_merged_commit_sha, status, retry_count, merge_on_turn_end, \
    conflicting_paths, created_at, updated_at";

impl Store {
    /// Insert a new sandbox record.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_sandbox(&self, s: &Sandbox) -> Result<()> {
        let sql = format!(
            "INSERT INTO sandbox ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        let conflicting_paths = if s.conflicting_paths.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&s.conflicting_paths).unwrap_or_default())
        };
        sqlx::query(&sql)
            .bind(&s.id)
            .bind(&s.workspace_id.0)
            .bind(&s.agent_id.0)
            .bind(&s.path)
            .bind(&s.branch)
            .bind(&s.base_commit_sha)
            .bind(&s.snapshot_commit_sha)
            .bind(&s.last_merged_commit_sha)
            .bind(s.status.to_db())
            .bind(s.retry_count)
            .bind(s.merge_on_turn_end)
            .bind(conflicting_paths)
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("insert sandbox failed: {e}")))?;
        Ok(())
    }

    /// Get a sandbox by workspace and agent.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<Option<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox WHERE workspace_id = ? AND agent_id = ?");
        let row = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(&agent_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("get sandbox failed: {e}")))?;
        row.map(|r| sandbox_from_row(&r)).transpose()
    }

    /// Update a sandbox status and timestamp.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn update_sandbox_status(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        status: SandboxStatus,
        updated_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE sandbox SET status = ?, updated_at = ? WHERE workspace_id = ? AND agent_id = ?",
        )
        .bind(status.to_db())
        .bind(updated_at)
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("update sandbox status failed: {e}")))?;
        Ok(())
    }

    /// Persist a status together with the conflicting paths in one write, so
    /// the status row and its explanation can never disagree. Used for
    /// `conflict` (terminal) and `conflict_bounced` (agent reconciling) with
    /// the clash's paths, and for `merged` with an empty slice — which
    /// stores NULL, clearing any stale paths from an earlier bounce.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when the write fails.
    pub async fn set_sandbox_status_with_conflicts(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        status: SandboxStatus,
        conflicting_paths: &[String],
        updated_at: &str,
    ) -> Result<()> {
        let paths_json = if conflicting_paths.is_empty() {
            None
        } else {
            Some(serde_json::to_string(conflicting_paths).unwrap_or_default())
        };
        sqlx::query(
            "UPDATE sandbox SET status = ?, conflicting_paths = ?, updated_at = ? \
             WHERE workspace_id = ? AND agent_id = ?",
        )
        .bind(status.to_db())
        .bind(paths_json)
        .bind(updated_at)
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("set sandbox conflict failed: {e}")))?;
        Ok(())
    }

    /// Record the tip of the last successfully merged range for a sandbox.
    /// Persistent sandboxes merge repeatedly; the next merge cherry-picks
    /// only commits after this SHA.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when the write fails.
    pub async fn set_sandbox_last_merged_commit(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        last_merged_commit_sha: &str,
        updated_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE sandbox SET last_merged_commit_sha = ?, updated_at = ? \
             WHERE workspace_id = ? AND agent_id = ?",
        )
        .bind(last_merged_commit_sha)
        .bind(updated_at)
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| {
            intent_core::Error::Internal(format!("set sandbox last merged commit failed: {e}"))
        })?;
        Ok(())
    }

    /// Delete a sandbox by workspace and agent.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM sandbox WHERE workspace_id = ? AND agent_id = ?")
            .bind(&workspace_id.0)
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("delete sandbox failed: {e}")))?;
        Ok(())
    }

    /// List all sandboxes for a workspace.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_sandboxes(&self, workspace_id: &WorkspaceId) -> Result<Vec<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox WHERE workspace_id = ?");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("list sandboxes failed: {e}")))?;
        rows.iter().map(sandbox_from_row).collect()
    }

    /// List all sandbox records (for GC).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_all_sandboxes(&self) -> Result<Vec<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("list all sandboxes failed: {e}")))?;
        rows.iter().map(sandbox_from_row).collect()
    }

    /// List all sandbox records in a given status (across every workspace).
    /// Used by the daemon's merge retry sweep to find `merge_pending` sandboxes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_sandboxes_by_status(&self, status: SandboxStatus) -> Result<Vec<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox WHERE status = ?");
        let rows = sqlx::query(&sql)
            .bind(status.to_db())
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list sandboxes by status failed: {e}"))
            })?;
        rows.iter().map(sandbox_from_row).collect()
    }

    /// Atomically transition a sandbox status from `from` to `to`. Returns
    /// `true` when the row was still in the expected `from` status and was
    /// updated (the caller acquired the transition), `false` when another
    /// path already moved it — the compare-and-swap that lets the merge
    /// retry sweep claim `merge_pending → merging` without double-merging
    /// against a concurrent `sandbox.cow.merge` RPC or a second sweep.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn try_transition_sandbox_status(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        from: SandboxStatus,
        to: SandboxStatus,
        updated_at: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE sandbox SET status = ?, updated_at = ? \
             WHERE workspace_id = ? AND agent_id = ? AND status = ?",
        )
        .bind(to.to_db())
        .bind(updated_at)
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .bind(from.to_db())
        .execute(self.write_pool())
        .await
        .map_err(|e| {
            intent_core::Error::Internal(format!("transition sandbox status failed: {e}"))
        })?;
        Ok(res.rows_affected() > 0)
    }

    /// Get the retry count for a sandbox.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_sandbox_retry_count(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<i64> {
        let row =
            sqlx::query("SELECT retry_count FROM sandbox WHERE workspace_id = ? AND agent_id = ?")
                .bind(&workspace_id.0)
                .bind(&agent_id.0)
                .fetch_optional(self.read_pool())
                .await
                .map_err(|e| {
                    intent_core::Error::Internal(format!("get sandbox retry count failed: {e}"))
                })?;

        Ok(row.map_or(0, |r| r.try_get("retry_count").unwrap_or(0)))
    }

    /// Increment the retry count for a sandbox.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn increment_sandbox_retry_count(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("UPDATE sandbox SET retry_count = retry_count + 1 WHERE workspace_id = ? AND agent_id = ?")
            .bind(&workspace_id.0)
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("increment sandbox retry count failed: {e}")))?;
        Ok(())
    }

    /// Clear the retry count for a sandbox (on successful merge).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_sandbox_retry_count(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("UPDATE sandbox SET retry_count = 0 WHERE workspace_id = ? AND agent_id = ?")
            .bind(&workspace_id.0)
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("clear sandbox retry count failed: {e}"))
            })?;
        Ok(())
    }
}

fn sandbox_from_row(row: &SqliteRow) -> Result<Sandbox> {
    let status_str: String = row
        .try_get("status")
        .map_err(|e| intent_core::Error::Internal(format!("get status failed: {e}")))?;
    Ok(Sandbox {
        id: row
            .try_get("id")
            .map_err(|e| intent_core::Error::Internal(format!("get id failed: {e}")))?,
        workspace_id: WorkspaceId(
            row.try_get("workspace_id").map_err(|e| {
                intent_core::Error::Internal(format!("get workspace_id failed: {e}"))
            })?,
        ),
        agent_id: AgentId(
            row.try_get("agent_id")
                .map_err(|e| intent_core::Error::Internal(format!("get agent_id failed: {e}")))?,
        ),
        path: row
            .try_get("path")
            .map_err(|e| intent_core::Error::Internal(format!("get path failed: {e}")))?,
        branch: row
            .try_get("branch")
            .map_err(|e| intent_core::Error::Internal(format!("get branch failed: {e}")))?,
        base_commit_sha: row.try_get("base_commit_sha").map_err(|e| {
            intent_core::Error::Internal(format!("get base_commit_sha failed: {e}"))
        })?,
        snapshot_commit_sha: row
            .try_get::<Option<String>, _>("snapshot_commit_sha")
            .ok()
            .flatten()
            .filter(|s| !s.is_empty()),
        last_merged_commit_sha: row
            .try_get::<Option<String>, _>("last_merged_commit_sha")
            .ok()
            .flatten()
            .filter(|s| !s.is_empty()),
        status: SandboxStatus::from_db(&status_str)?,
        retry_count: row
            .try_get("retry_count")
            .map_err(|e| intent_core::Error::Internal(format!("get retry_count failed: {e}")))?,
        merge_on_turn_end: row.try_get("merge_on_turn_end").map_err(|e| {
            intent_core::Error::Internal(format!("get merge_on_turn_end failed: {e}"))
        })?,
        conflicting_paths: row
            .try_get::<Option<String>, _>("conflicting_paths")
            .ok()
            .flatten()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
        created_at: row
            .try_get("created_at")
            .map_err(|e| intent_core::Error::Internal(format!("get created_at failed: {e}")))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| intent_core::Error::Internal(format!("get updated_at failed: {e}")))?,
    })
}
