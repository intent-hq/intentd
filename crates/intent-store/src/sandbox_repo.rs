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
    Created,
    Merged,
    Discarded,
    Conflict,
}

impl SandboxStatus {
    fn to_db(self) -> &'static str {
        match self {
            SandboxStatus::Created => "created",
            SandboxStatus::Merged => "merged",
            SandboxStatus::Discarded => "discarded",
            SandboxStatus::Conflict => "conflict",
        }
    }

    fn from_db(s: &str) -> Result<Self> {
        match s {
            "created" => Ok(SandboxStatus::Created),
            "merged" => Ok(SandboxStatus::Merged),
            "discarded" => Ok(SandboxStatus::Discarded),
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
    pub status: SandboxStatus,
    pub created_at: String,
    pub updated_at: String,
}

const COLUMNS: &str = "id, workspace_id, agent_id, path, branch, base_commit_sha, \
    snapshot_commit_sha, status, created_at, updated_at";

impl Store {
    /// Insert a new sandbox record.
    pub async fn insert_sandbox(&self, s: &Sandbox) -> Result<()> {
        let sql = format!("INSERT INTO sandbox ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        sqlx::query(&sql)
            .bind(&s.id)
            .bind(&s.workspace_id.0)
            .bind(&s.agent_id.0)
            .bind(&s.path)
            .bind(&s.branch)
            .bind(&s.base_commit_sha)
            .bind(&s.snapshot_commit_sha)
            .bind(s.status.to_db())
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .execute(&self.pool)
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
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| intent_core::Error::Internal(format!("get sandbox failed: {e}")))?;
        row.map(|r| sandbox_from_row(&r)).transpose()
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
            .execute(&self.pool)
            .await
            .map_err(|e| intent_core::Error::Internal(format!("delete sandbox failed: {e}")))?;
        Ok(())
    }

    /// List all sandboxes for a workspace.
    pub async fn list_sandboxes(&self, workspace_id: &WorkspaceId) -> Result<Vec<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox WHERE workspace_id = ?");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| intent_core::Error::Internal(format!("list sandboxes failed: {e}")))?;
        rows.iter().map(sandbox_from_row).collect()
    }

    /// List all sandbox records (for GC).
    pub async fn list_all_sandboxes(&self) -> Result<Vec<Sandbox>> {
        let sql = format!("SELECT {COLUMNS} FROM sandbox");
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| intent_core::Error::Internal(format!("list all sandboxes failed: {e}")))?;
        rows.iter().map(sandbox_from_row).collect()
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
        snapshot_commit_sha: row.try_get("snapshot_commit_sha").ok(),
        status: SandboxStatus::from_db(&status_str)?,
        created_at: row
            .try_get("created_at")
            .map_err(|e| intent_core::Error::Internal(format!("get created_at failed: {e}")))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| intent_core::Error::Internal(format!("get updated_at failed: {e}")))?,
    })
}
