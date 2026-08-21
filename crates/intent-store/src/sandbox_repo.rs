//! Sandbox repository: CRUD for agent sandboxes (CoW isolation).

use intent_core::{AgentId, Result, WorkspaceId};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

/// Sandbox status lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxStatus {
    /// Initial state after provisioning
    Created,
    /// Merge-back is in progress
    Merging,
    /// Successfully merged to canonical and discarded
    Merged,
    /// Discarded without merging
    Discarded,
    /// Conflict detected, agent bounced with instructions
    ConflictBounced,
    /// Merge pending manual resolution (blocked or retry exhausted)
    MergePending,
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
            // Handle legacy "conflict" status from migration 0038
            "conflict" => Ok(SandboxStatus::ConflictBounced),
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
    pub status: SandboxStatus,
    pub retry_count: i64,
    pub created_at: String,
    pub updated_at: String,
}

const COLUMNS: &str = "id, workspace_id, agent_id, path, branch, base_commit_sha, \
    snapshot_commit_sha, status, retry_count, created_at, updated_at";

impl Store {
    /// Insert a new sandbox record.
    pub async fn insert_sandbox(&self, s: &Sandbox) -> Result<()> {
        let sql =
            format!("INSERT INTO sandbox ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        sqlx::query(&sql)
            .bind(&s.id)
            .bind(&s.workspace_id.0)
            .bind(&s.agent_id.0)
            .bind(&s.path)
            .bind(&s.branch)
            .bind(&s.base_commit_sha)
            .bind(&s.snapshot_commit_sha)
            .bind(s.status.to_db())
            .bind(s.retry_count)
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("insert sandbox failed: {e}")))?;
        Ok(())
    }

    /// Get a sandbox by workspace and agent.
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

    /// Delete a sandbox by workspace and agent.
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
        status: SandboxStatus::from_db(&status_str)?,
        retry_count: row
            .try_get("retry_count")
            .map_err(|e| intent_core::Error::Internal(format!("get retry_count failed: {e}")))?,
        created_at: row
            .try_get("created_at")
            .map_err(|e| intent_core::Error::Internal(format!("get created_at failed: {e}")))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| intent_core::Error::Internal(format!("get updated_at failed: {e}")))?,
    })
}
