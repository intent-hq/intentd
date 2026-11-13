//! Workspace homes on an orchestrator (monorepo
//! `docs/protocol/methods/nodes.md`, "Workspace homes"): which machine holds
//! each workspace's home checkout, its state and its last backup.

use std::collections::HashMap;

use intent_core::{clock::parse_iso, Error, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use sqlx::{sqlite::SqliteRow, Row};

use crate::Store;

/// How long home backups may keep failing before `backupWarning` shows.
pub const BACKUP_WARNING_AFTER: time::Duration = time::Duration::minutes(15);

const SELECT: &str = "SELECT workspace_id, home_id, node_id, cloud_machine_id, name, state, \
    since, last_backup_at, backup_failing_since FROM workspace_home";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("workspace home: {error}"))
}

fn state_from_db(text: &str) -> Result<HomeState> {
    match text {
        "attached" => Ok(HomeState::Attached),
        "detached" => Ok(HomeState::Detached),
        "moving" => Ok(HomeState::Moving),
        other => Err(db(format!("unknown state {other}"))),
    }
}

fn warning(failing_since: Option<&str>, now: time::OffsetDateTime) -> bool {
    failing_since
        .and_then(parse_iso)
        .is_some_and(|since| now - since >= BACKUP_WARNING_AFTER)
}

fn map_row(row: &SqliteRow, now: time::OffsetDateTime) -> Result<(WorkspaceId, WorkspaceHome)> {
    let get = |name: &str| row.try_get::<Option<String>, _>(name).map_err(db);
    let failing = get("backup_failing_since")?;
    Ok((
        WorkspaceId(row.try_get("workspace_id").map_err(db)?),
        WorkspaceHome {
            home_id: HomeId(row.try_get("home_id").map_err(db)?),
            node_id: get("node_id")?,
            cloud_machine_id: get("cloud_machine_id")?,
            name: row.try_get("name").map_err(db)?,
            state: state_from_db(&row.try_get::<String, _>("state").map_err(db)?)?,
            since: row.try_get("since").map_err(db)?,
            last_backup_at: get("last_backup_at")?,
            backup_warning: warning(failing.as_deref(), now),
        },
    ))
}

impl Store {
    /// Record `home` as the workspace's home, replacing any other. A new
    /// `homeId` starts with no backup failure.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure (also for an unknown
    /// workspace: the foreign key refuses it).
    pub async fn set_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO workspace_home (workspace_id, home_id, node_id, cloud_machine_id, name, \
             state, since, last_backup_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(workspace_id) DO UPDATE SET home_id = excluded.home_id, \
             node_id = excluded.node_id, cloud_machine_id = excluded.cloud_machine_id, \
             name = excluded.name, state = excluded.state, since = excluded.since, \
             last_backup_at = excluded.last_backup_at, \
             backup_failing_since = CASE WHEN workspace_home.home_id = excluded.home_id \
             THEN workspace_home.backup_failing_since END",
        )
        .bind(&workspace_id.0)
        .bind(&home.home_id.0)
        .bind(&home.node_id)
        .bind(&home.cloud_machine_id)
        .bind(&home.name)
        .bind(home.state.as_str())
        .bind(&home.since)
        .bind(&home.last_backup_at)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(())
    }

    /// The workspace's home, if it has one.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn get_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceHome>> {
        let sql = format!("{SELECT} WHERE workspace_id = ?");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&workspace_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(db)?;
        let now = time::OffsetDateTime::now_utc();
        row.map(|row| map_row(&row, now).map(|(_, home)| home))
            .transpose()
    }

    /// Every workspace's home, for overlaying list rows in one query.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn list_workspace_homes(&self) -> Result<HashMap<WorkspaceId, WorkspaceHome>> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(SELECT))
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        let now = time::OffsetDateTime::now_utc();
        rows.iter().map(|row| map_row(row, now)).collect()
    }

    /// The homes the exposed machine `node_id` holds.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn homes_on_machine(
        &self,
        node_id: &str,
    ) -> Result<Vec<(WorkspaceId, WorkspaceHome)>> {
        let sql = format!("{SELECT} WHERE node_id = ? ORDER BY workspace_id");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(node_id)
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        let now = time::OffsetDateTime::now_utc();
        rows.iter().map(|row| map_row(row, now)).collect()
    }

    /// Set the state of the workspace's home `home_id`; `false` when that is
    /// not its home (any more) or the state is unchanged.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn set_workspace_home_state(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        state: HomeState,
        since: &str,
    ) -> Result<bool> {
        let done = sqlx::query(
            "UPDATE workspace_home SET state = ?, since = ? \
             WHERE workspace_id = ? AND home_id = ? AND state != ?",
        )
        .bind(state.as_str())
        .bind(since)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .bind(state.as_str())
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// Record a backup of the workspace's home `home_id` at `at`, reported
    /// by the machine `node_id`: it ends a failing streak. `false` when that
    /// is not its home, or not on that machine.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn record_workspace_home_backup(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        at: &str,
    ) -> Result<bool> {
        let done = sqlx::query(
            "UPDATE workspace_home SET last_backup_at = ?, backup_failing_since = NULL \
             WHERE workspace_id = ? AND home_id = ? AND node_id = ?",
        )
        .bind(at)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .bind(node_id)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// Record that backing up the workspace's home `home_id` failed at `at`;
    /// a streak keeps its first failure.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn record_workspace_home_backup_failure(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        at: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE workspace_home SET backup_failing_since = COALESCE(backup_failing_since, ?) \
             WHERE workspace_id = ? AND home_id = ?",
        )
        .bind(at)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
