//! Change-metrics repository: the durable per-workspace / per-agent line-change
//! aggregates (§9.11, §17.5). Written by the BE-internal metrics aggregator
//! (`services::metrics`, recomputed as agents edit files); read over the wire via
//! the `metrics.*` reads (PROTOCOL §5.20). There is no `metrics.calculate` RPC —
//! the rows are recomputed internally from `tracked_changes`.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

/// A persisted `workspace_metrics` row (per-workspace line-change totals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetricsRow {
    pub workspace_id: WorkspaceId,
    pub additions: i64,
    pub deletions: i64,
    pub files_changed: i64,
    pub updated_at: String,
}

/// A persisted `agent_metrics` row (per-agent, per-workspace line-change totals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMetricsRow {
    pub agent_id: String,
    pub workspace_id: WorkspaceId,
    pub additions: i64,
    pub deletions: i64,
    pub files_changed: i64,
    pub updated_at: String,
}

impl Store {
    /// Upsert the per-workspace metrics row, stamping `updated_at`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_workspace_metrics(
        &self,
        workspace_id: &WorkspaceId,
        additions: i64,
        deletions: i64,
        files_changed: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO workspace_metrics \
             (workspace_id, additions, deletions, files_changed, updated_at) \
             VALUES (?,?,?,?,?) \
             ON CONFLICT(workspace_id) DO UPDATE SET \
             additions = excluded.additions, deletions = excluded.deletions, \
             files_changed = excluded.files_changed, updated_at = excluded.updated_at",
        )
        .bind(&workspace_id.0)
        .bind(additions)
        .bind(deletions)
        .bind(files_changed)
        .bind(now_iso())
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert workspace metrics failed: {e}")))?;
        Ok(())
    }

    /// Read one workspace's metrics row (absent → `None`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_metrics(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceMetricsRow>> {
        let row = sqlx::query(
            "SELECT workspace_id, additions, deletions, files_changed, updated_at \
             FROM workspace_metrics WHERE workspace_id = ?",
        )
        .bind(&workspace_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get workspace metrics failed: {e}")))?;
        row.as_ref().map(map_workspace_metrics_row).transpose()
    }

    /// List every workspace's metrics row (for `metrics.getAllWorkspaceStats`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspace_metrics(&self) -> Result<Vec<WorkspaceMetricsRow>> {
        let rows = sqlx::query(
            "SELECT workspace_id, additions, deletions, files_changed, updated_at \
             FROM workspace_metrics ORDER BY workspace_id",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list workspace metrics failed: {e}")))?;
        rows.iter().map(map_workspace_metrics_row).collect()
    }

    /// Delete one workspace's metrics row (used when it has no tracked changes).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_workspace_metrics(&self, workspace_id: &WorkspaceId) -> Result<()> {
        sqlx::query("DELETE FROM workspace_metrics WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace metrics failed: {e}")))?;
        Ok(())
    }

    /// Upsert one per-agent, per-workspace metrics row, stamping `updated_at`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_agent_metrics(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &str,
        additions: i64,
        deletions: i64,
        files_changed: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO agent_metrics \
             (agent_id, workspace_id, additions, deletions, files_changed, updated_at) \
             VALUES (?,?,?,?,?,?) \
             ON CONFLICT(workspace_id, agent_id) DO UPDATE SET \
             additions = excluded.additions, deletions = excluded.deletions, \
             files_changed = excluded.files_changed, updated_at = excluded.updated_at",
        )
        .bind(agent_id)
        .bind(&workspace_id.0)
        .bind(additions)
        .bind(deletions)
        .bind(files_changed)
        .bind(now_iso())
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert agent metrics failed: {e}")))?;
        Ok(())
    }

    /// List the per-agent metrics rows for one workspace (powers `byAgent`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_agent_metrics_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentMetricsRow>> {
        let rows = sqlx::query(
            "SELECT agent_id, workspace_id, additions, deletions, files_changed, updated_at \
             FROM agent_metrics WHERE workspace_id = ? ORDER BY agent_id",
        )
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list agent metrics failed: {e}")))?;
        rows.iter().map(map_agent_metrics_row).collect()
    }

    /// List one agent's metrics rows across every workspace (for
    /// `metrics.getAgentStats`, whose totals are summed across workspaces).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_agent_metrics(&self, agent_id: &str) -> Result<Vec<AgentMetricsRow>> {
        let rows = sqlx::query(
            "SELECT agent_id, workspace_id, additions, deletions, files_changed, updated_at \
             FROM agent_metrics WHERE agent_id = ? ORDER BY workspace_id",
        )
        .bind(agent_id)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list agent metrics by id failed: {e}")))?;
        rows.iter().map(map_agent_metrics_row).collect()
    }

    /// Delete one workspace's per-agent metrics rows (cleared before a recompute
    /// rewrites the live per-agent breakdown).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_agent_metrics_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM agent_metrics WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace agent metrics failed: {e}")))?;
        Ok(())
    }

    /// Delete one agent's metrics rows across every workspace, returning the
    /// number of rows removed (backs `metrics.clearAgentStats`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_agent_metrics(&self, agent_id: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM agent_metrics WHERE agent_id = ?")
            .bind(agent_id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete agent metrics failed: {e}")))?;
        Ok(result.rows_affected())
    }
}

fn map_workspace_metrics_row(r: &SqliteRow) -> Result<WorkspaceMetricsRow> {
    Ok(WorkspaceMetricsRow {
        workspace_id: WorkspaceId(r.get("workspace_id")),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        files_changed: r.get("files_changed"),
        updated_at: r.get("updated_at"),
    })
}

fn map_agent_metrics_row(r: &SqliteRow) -> Result<AgentMetricsRow> {
    Ok(AgentMetricsRow {
        agent_id: r.get("agent_id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        files_changed: r.get("files_changed"),
        updated_at: r.get("updated_at"),
    })
}
