//! PR-monitor repository: CRUD for agent-owned pull-request watches. Rows are
//! written through by the centralized monitor loop and rehydrated at boot via
//! [`Store::load_active_pr_monitors`].

use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const COLUMNS: &str = "monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number, \
    state, last_snapshot, pending_changes, pending_since, last_change_at, last_polled_at, \
    last_error, created_at, updated_at";

fn state_to_db(state: PrMonitorState) -> &'static str {
    match state {
        PrMonitorState::Active => "active",
        PrMonitorState::Completed => "completed",
        PrMonitorState::Cancelled => "cancelled",
    }
}

fn state_from_db(s: &str) -> Result<PrMonitorState> {
    match s {
        "active" => Ok(PrMonitorState::Active),
        "completed" => Ok(PrMonitorState::Completed),
        "cancelled" => Ok(PrMonitorState::Cancelled),
        _ => Err(intent_core::Error::Internal(format!(
            "invalid pr monitor state: {s}"
        ))),
    }
}

/// The persisted `pending_changes` column is a JSON array of change lines; a
/// NULL or unparseable value reads as "nothing pending" rather than failing
/// the row (a monitor must never become unreadable because of a bad blob).
fn pending_from_db(raw: Option<String>) -> Vec<String> {
    raw.and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .unwrap_or_default()
}

fn pending_to_db(pending: &[String]) -> Option<String> {
    if pending.is_empty() {
        return None;
    }
    serde_json::to_string(pending).ok()
}

fn monitor_from_row(r: &SqliteRow) -> Result<PrMonitor> {
    let err = |e: sqlx::Error| intent_core::Error::Internal(format!("read pr monitor row: {e}"));
    let get = |col: &str| -> Result<String> { r.try_get::<String, _>(col).map_err(err) };
    let get_opt =
        |col: &str| -> Result<Option<String>> { r.try_get::<Option<String>, _>(col).map_err(err) };
    Ok(PrMonitor {
        monitor_id: PrMonitorId(get("monitor_id")?),
        workspace_id: WorkspaceId(get("workspace_id")?),
        agent_id: AgentId(get("agent_id")?),
        repo_owner: get("repo_owner")?,
        repo_name: get("repo_name")?,
        pr_number: r.try_get::<i64, _>("pr_number").map_err(err)?,
        state: state_from_db(&get("state")?)?,
        last_snapshot: get_opt("last_snapshot")?,
        pending_changes: pending_from_db(get_opt("pending_changes")?),
        pending_since: get_opt("pending_since")?,
        last_change_at: get_opt("last_change_at")?,
        last_polled_at: get_opt("last_polled_at")?,
        last_error: get_opt("last_error")?,
        created_at: get("created_at")?,
        updated_at: get("updated_at")?,
    })
}

impl Store {
    /// Insert a new PR-monitor row.
    pub async fn insert_pr_monitor(&self, m: &PrMonitor) -> Result<()> {
        let sql = format!(
            "INSERT INTO pr_monitor ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        sqlx::query(&sql)
            .bind(&m.monitor_id.0)
            .bind(&m.workspace_id.0)
            .bind(&m.agent_id.0)
            .bind(&m.repo_owner)
            .bind(&m.repo_name)
            .bind(m.pr_number)
            .bind(state_to_db(m.state))
            .bind(&m.last_snapshot)
            .bind(pending_to_db(&m.pending_changes))
            .bind(&m.pending_since)
            .bind(&m.last_change_at)
            .bind(&m.last_polled_at)
            .bind(&m.last_error)
            .bind(&m.created_at)
            .bind(&m.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("insert pr monitor failed: {e}")))?;
        Ok(())
    }

    /// Get a PR monitor by id; `NotFound` when absent.
    pub async fn get_pr_monitor(&self, monitor_id: &PrMonitorId) -> Result<PrMonitor> {
        let sql = format!("SELECT {COLUMNS} FROM pr_monitor WHERE monitor_id = ?");
        let row = sqlx::query(&sql)
            .bind(&monitor_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("get pr monitor failed: {e}")))?;
        match row {
            Some(r) => monitor_from_row(&r),
            None => Err(intent_core::Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            ))),
        }
    }

    /// The ACTIVE monitor an agent already owns for `(owner, name, number)`,
    /// if any — the idempotent re-register lookup.
    pub async fn find_active_pr_monitor(
        &self,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
    ) -> Result<Option<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? AND repo_owner = ? \
             AND repo_name = ? AND pr_number = ? AND state = 'active'"
        );
        let row = sqlx::query(&sql)
            .bind(&agent_id.0)
            .bind(repo_owner)
            .bind(repo_name)
            .bind(pr_number)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("find pr monitor failed: {e}")))?;
        row.as_ref().map(monitor_from_row).transpose()
    }

    /// List every monitor owned by an agent, oldest first (all states — the
    /// caller filters).
    pub async fn list_pr_monitors_by_agent(&self, agent_id: &AgentId) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list pr monitors by agent failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List every monitor in a workspace, oldest first (all states).
    pub async fn list_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list pr monitors by workspace failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Every `active` monitor across all workspaces, oldest first — the poll
    /// loop's per-tick read and the boot rehydration read.
    pub async fn load_active_pr_monitors(&self) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE state = 'active' ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("load active pr monitors failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Set a monitor's lifecycle state; `NotFound` when the row is absent.
    pub async fn update_pr_monitor_state(
        &self,
        monitor_id: &PrMonitorId,
        state: PrMonitorState,
        updated_at: &str,
    ) -> Result<()> {
        let res =
            sqlx::query("UPDATE pr_monitor SET state = ?, updated_at = ? WHERE monitor_id = ?")
                .bind(state_to_db(state))
                .bind(updated_at)
                .bind(&monitor_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| {
                    intent_core::Error::Internal(format!("update pr monitor state failed: {e}"))
                })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        Ok(())
    }

    /// Write back everything one poll can change: the baseline snapshot, the
    /// accumulated pending changes and their debounce anchors, the poll
    /// timestamp, and the last forge error. One statement so a reader never
    /// observes a baseline that moved without its pending changes.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_pr_monitor_poll(
        &self,
        monitor_id: &PrMonitorId,
        last_snapshot: Option<&str>,
        pending_changes: &[String],
        pending_since: Option<&str>,
        last_change_at: Option<&str>,
        last_polled_at: Option<&str>,
        last_error: Option<&str>,
        updated_at: &str,
    ) -> Result<()> {
        let res = sqlx::query(
            "UPDATE pr_monitor SET last_snapshot = ?, pending_changes = ?, pending_since = ?, \
             last_change_at = ?, last_polled_at = ?, last_error = ?, updated_at = ? \
             WHERE monitor_id = ?",
        )
        .bind(last_snapshot)
        .bind(pending_to_db(pending_changes))
        .bind(pending_since)
        .bind(last_change_at)
        .bind(last_polled_at)
        .bind(last_error)
        .bind(updated_at)
        .bind(&monitor_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("update pr monitor poll failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        Ok(())
    }

    /// Delete a PR-monitor row; `NotFound` when absent.
    pub async fn delete_pr_monitor(&self, monitor_id: &PrMonitorId) -> Result<()> {
        let res = sqlx::query("DELETE FROM pr_monitor WHERE monitor_id = ?")
            .bind(&monitor_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("delete pr monitor failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        Ok(())
    }
}
