//! Row statistics for the workspace transfer manifest (`workspace.transfer.plan`).
//!
//! Enumerates every workspace-scoped table that rides in a transfer archive —
//! deliberately excluding `event` (event history stays on the source, spec
//! "Resolved Design Decisions" #3) and non-workspace-scoped tables (settings,
//! secrets, `known_repo`, usage stats, idempotency keys, clients).

use intent_core::transfer::TransferTableStat;
use intent_core::{Error, Result, WorkspaceId};

use crate::Store;

/// Workspace-scoped tables included in a transfer, each with the SQL predicate
/// that scopes its rows to one workspace (`?1` = workspace id). `agent_message`
/// and `agent_queue` have no `workspace_id` column and scope through their
/// owning `agent_session`; `completion_watch` is workspace-scoped from either
/// end of the parent/child pair.
pub const TRANSFER_TABLES: &[(&str, &str)] = &[
    ("workspace", "id = ?1"),
    ("note", "workspace_id = ?1"),
    ("note_version", "workspace_id = ?1"),
    ("note_line_attribution", "workspace_id = ?1"),
    ("comment", "workspace_id = ?1"),
    ("draft", "workspace_id = ?1"),
    ("agent_session", "workspace_id = ?1"),
    (
        "agent_message",
        "agent_id IN (SELECT id FROM agent_session WHERE workspace_id = ?1)",
    ),
    (
        "agent_queue",
        "agent_id IN (SELECT id FROM agent_session WHERE workspace_id = ?1)",
    ),
    ("interrupted_agent", "workspace_id = ?1"),
    ("delegation_group", "workspace_id = ?1"),
    (
        "completion_watch",
        "parent_workspace_id = ?1 OR child_workspace_id = ?1",
    ),
    ("event_subscription", "workspace_id = ?1"),
    ("hook", "workspace_id = ?1"),
    ("pr_monitor", "workspace_id = ?1"),
    ("script", "workspace_id = ?1"),
    ("task_agent_link", "workspace_id = ?1"),
    ("sandbox", "workspace_id = ?1"),
    ("tracked_changes", "workspace_id = ?1"),
    ("diffs", "workspace_id = ?1"),
    ("workspace_metrics", "workspace_id = ?1"),
    ("agent_metrics", "workspace_id = ?1"),
    ("workspace_context_item", "workspace_id = ?1"),
    ("workspace_ui_context", "workspace_id = ?1"),
];

impl Store {
    /// Per-table row count + approximate serialized byte size for one
    /// workspace, over [`TRANSFER_TABLES`]. `approx_bytes` sums
    /// `LENGTH(CAST(col AS BLOB))` across every column of every scoped row —
    /// an estimate of the payload carried by an export, not on-disk size.
    /// Read-only. Tables with zero rows are still listed (count 0), so the
    /// manifest shape is stable across workspaces.
    pub async fn transfer_table_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TransferTableStat>> {
        let mut stats = Vec::with_capacity(TRANSFER_TABLES.len());
        for (table, predicate) in TRANSFER_TABLES {
            let columns = self.table_columns(table).await?;
            let size_expr = if columns.is_empty() {
                "0".to_string()
            } else {
                columns
                    .iter()
                    .map(|c| format!("COALESCE(LENGTH(CAST(\"{c}\" AS BLOB)), 0)"))
                    .collect::<Vec<_>>()
                    .join(" + ")
            };
            let sql = format!(
                "SELECT COUNT(*) AS n, COALESCE(SUM({size_expr}), 0) AS b \
                 FROM \"{table}\" WHERE {predicate}"
            );
            let row = sqlx::query_as::<_, (i64, i64)>(&sql)
                .bind(&workspace_id.0)
                .fetch_one(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("transfer stats for {table} failed: {e}")))?;
            stats.push(TransferTableStat {
                name: (*table).to_string(),
                row_count: row.0,
                approx_bytes: row.1,
            });
        }
        Ok(stats)
    }

    /// Column names of `table` in declaration order (via `PRAGMA table_info`).
    async fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (i64, String)>(&format!(
            "SELECT cid, name FROM pragma_table_info('{table}') ORDER BY cid"
        ))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("table_info for {table} failed: {e}")))?;
        Ok(rows.into_iter().map(|(_, name)| name).collect())
    }
}
