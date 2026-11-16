//! Home final stops on an orchestrator (monorepo
//! `docs/protocol/node-link.md`, "Home final stop"): the stops of homes
//! that were retired (the workspace archived or deleted) or moved away,
//! which their machine has not acknowledged yet.

use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use intent_core::{Error, HomeId, Result, WorkspaceHome, WorkspaceId};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

use crate::Store;

const SELECT: &str = "SELECT home_id, workspace_id, node_id, stop_id, reason, orphaned_at, \
    recorded_at FROM home_stop";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("home stop: {error}"))
}

fn map_row(row: &SqliteRow) -> Result<HomeStopRecord> {
    let reason: String = row.try_get("reason").map_err(db)?;
    Ok(HomeStopRecord {
        workspace_id: WorkspaceId(row.try_get("workspace_id").map_err(db)?),
        home_id: HomeId(row.try_get("home_id").map_err(db)?),
        node_id: row.try_get("node_id").map_err(db)?,
        stop_id: row.try_get("stop_id").map_err(db)?,
        reason: HomeStopReason::parse(&reason)
            .ok_or_else(|| db(format!("unknown reason {reason}")))?,
        orphaned_at: row.try_get("orphaned_at").map_err(db)?,
        recorded_at: row.try_get("recorded_at").map_err(db)?,
    })
}

/// Insert `record` unless its home has a stop already.
pub(crate) async fn insert(conn: &mut SqliteConnection, record: &HomeStopRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO home_stop (home_id, workspace_id, node_id, stop_id, reason, orphaned_at, \
         recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(home_id) DO NOTHING",
    )
    .bind(&record.home_id.0)
    .bind(&record.workspace_id.0)
    .bind(&record.node_id)
    .bind(&record.stop_id)
    .bind(record.reason.as_str())
    .bind(&record.orphaned_at)
    .bind(&record.recorded_at)
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(())
}

/// Inside a transaction that ends a move without its switch (a rollback,
/// or the workspace archived or deleted): a `retired` stop of the move's
/// target when it is on an exposed machine (a cloud sandbox only while it
/// still exists), which may have opened it. Its
/// last push is discarded (see `discard_push`).
pub(crate) async fn stop_move_target(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    now: &str,
) -> Result<()> {
    let target: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT home_id, node_id, cloud_machine_id FROM home_move WHERE workspace_id = ?",
    )
    .bind(&workspace_id.0)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?;
    let Some((home_id, Some(node_id), cloud_machine_id)) = target else {
        return Ok(());
    };
    if cloud_machine_id.is_some() {
        // A cloud sandbox that is gone (removed, lost) is owed nothing.
        let present: Option<i64> = sqlx::query_scalar("SELECT 1 FROM exposed_machine WHERE id = ?")
            .bind(&node_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?;
        if present.is_none() {
            return Ok(());
        }
    }
    let record = HomeStopRecord::new(
        workspace_id.clone(),
        HomeId(home_id),
        node_id,
        HomeStopReason::Retired,
        now,
    );
    insert(&mut *conn, &record).await?;
    sqlx::query("UPDATE home_stop SET discard_push = 1 WHERE home_id = ?")
        .bind(&record.home_id.0)
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
}

/// The workspace's current home on an exposed machine, as `(homeId, nodeId)`.
pub(crate) async fn machine_home(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
) -> Result<Option<(String, String)>> {
    let row = sqlx::query(
        "SELECT home_id, node_id FROM workspace_home WHERE workspace_id = ? \
         AND node_id IS NOT NULL",
    )
    .bind(&workspace_id.0)
    .fetch_optional(conn)
    .await
    .map_err(db)?;
    row.map(|row| {
        Ok((
            row.try_get("home_id").map_err(db)?,
            row.try_get("node_id").map_err(db)?,
        ))
    })
    .transpose()
}

/// Inside the transaction that archives or deletes `workspace_id`: end its
/// home. The home row goes and a `retired` stop of its machine checkout is
/// recorded. A deleted workspace (`deleted`) also turns its pending `moved`
/// stops into `retired` ones: nothing of it is pushed any more.
pub(crate) async fn retire_workspace_home(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    now: &str,
    deleted: bool,
) -> Result<()> {
    if let Some((home_id, node_id)) = machine_home(conn, workspace_id).await? {
        let record = HomeStopRecord::new(
            workspace_id.clone(),
            HomeId(home_id),
            node_id,
            HomeStopReason::Retired,
            now,
        );
        insert(conn, &record).await?;
    }
    sqlx::query("DELETE FROM workspace_home WHERE workspace_id = ?")
        .bind(&workspace_id.0)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    // A move in progress ends with the home: it never switches now, and
    // its target stops too.
    stop_move_target(conn, workspace_id, now).await?;
    sqlx::query("DELETE FROM home_move WHERE workspace_id = ?")
        .bind(&workspace_id.0)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    if deleted {
        sqlx::query(
            "UPDATE home_stop SET reason = 'retired', orphaned_at = NULL \
             WHERE workspace_id = ? AND reason = 'moved'",
        )
        .bind(&workspace_id.0)
        .execute(conn)
        .await
        .map_err(db)?;
    }
    Ok(())
}

impl Store {
    /// Make `home` the workspace's home and, in the same transaction, record
    /// a `moved` stop of the machine home it replaces (another `homeId`).
    /// The stop recorded, if any. The switch step of a home move (nodes.md
    /// "Moving the home").
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure (also for an unknown
    /// workspace: the foreign key refuses it).
    pub async fn switch_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
        now: &str,
    ) -> Result<Option<HomeStopRecord>> {
        dispatch!(self, backend => backend.switch_workspace_home(workspace_id, home, now).await)
    }

    /// Record `record` unless its home has a stop already; the stop kept.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn record_home_stop(&self, record: &HomeStopRecord) -> Result<HomeStopRecord> {
        dispatch!(self, backend => backend.record_home_stop(record).await)
    }

    /// The pending stop of `home_id`.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn get_home_stop(&self, home_id: &HomeId) -> Result<Option<HomeStopRecord>> {
        dispatch!(self, backend => backend.get_home_stop(home_id).await)
    }

    /// Whether the pending stop of `home_id` discards its last push: the
    /// home was a move's target that never became the workspace's.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn home_stop_discards_push(&self, home_id: &HomeId) -> Result<bool> {
        dispatch!(self, backend => backend.home_stop_discards_push(home_id).await)
    }

    /// The pending stops of the exposed machine `node_id`, oldest first.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn list_home_stops(&self, node_id: &str) -> Result<Vec<HomeStopRecord>> {
        dispatch!(self, backend => backend.list_home_stops(node_id).await)
    }

    /// The pending stops of `workspace_id`'s homes.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn list_workspace_home_stops(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<HomeStopRecord>> {
        dispatch!(self, backend => backend.list_workspace_home_stops(workspace_id).await)
    }

    /// Delete the stop of `home_id` its ack `stop_id` answers; the deleted
    /// stop, `None` when it is gone or has another id.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn finish_home_stop(
        &self,
        home_id: &HomeId,
        stop_id: &str,
    ) -> Result<Option<HomeStopRecord>> {
        dispatch!(self, backend => backend.finish_home_stop(home_id, stop_id).await)
    }

    /// Delete every stop of the exposed machine `node_id` (it was removed);
    /// how many went.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn delete_home_stops_on_machine(&self, node_id: &str) -> Result<u64> {
        dispatch!(self, backend => backend.delete_home_stops_on_machine(node_id).await)
    }
}

impl crate::SqliteStore {
    pub async fn switch_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
        now: &str,
    ) -> Result<Option<HomeStopRecord>> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let stop = match machine_home(&mut tx, workspace_id).await? {
            Some((old, node_id)) if old != home.home_id.0 => Some(HomeStopRecord::new(
                workspace_id.clone(),
                HomeId(old),
                node_id,
                HomeStopReason::Moved,
                now,
            )),
            _ => None,
        };
        if let Some(stop) = &stop {
            insert(&mut tx, stop).await?;
        }
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
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(stop)
    }

    pub async fn record_home_stop(&self, record: &HomeStopRecord) -> Result<HomeStopRecord> {
        let mut conn = self.write_pool().acquire().await.map_err(db)?;
        insert(&mut conn, record).await?;
        drop(conn);
        self.get_home_stop(&record.home_id)
            .await?
            .ok_or_else(|| db("the recorded stop is gone"))
    }

    pub async fn get_home_stop(&self, home_id: &HomeId) -> Result<Option<HomeStopRecord>> {
        let sql = format!("{SELECT} WHERE home_id = ?");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&home_id.0)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        row.as_ref().map(map_row).transpose()
    }

    pub async fn home_stop_discards_push(&self, home_id: &HomeId) -> Result<bool> {
        let discard: Option<i64> =
            sqlx::query_scalar("SELECT discard_push FROM home_stop WHERE home_id = ?")
                .bind(&home_id.0)
                .fetch_optional(self.write_pool())
                .await
                .map_err(db)?;
        Ok(discard.is_some_and(|discard| discard != 0))
    }

    pub async fn list_home_stops(&self, node_id: &str) -> Result<Vec<HomeStopRecord>> {
        let sql = format!("{SELECT} WHERE node_id = ? ORDER BY recorded_at, home_id");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(node_id)
            .fetch_all(self.write_pool())
            .await
            .map_err(db)?;
        rows.iter().map(map_row).collect()
    }

    pub async fn list_workspace_home_stops(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<HomeStopRecord>> {
        let sql = format!("{SELECT} WHERE workspace_id = ? ORDER BY recorded_at, home_id");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&workspace_id.0)
            .fetch_all(self.write_pool())
            .await
            .map_err(db)?;
        rows.iter().map(map_row).collect()
    }

    pub async fn finish_home_stop(
        &self,
        home_id: &HomeId,
        stop_id: &str,
    ) -> Result<Option<HomeStopRecord>> {
        let sql = format!(
            "DELETE FROM home_stop WHERE home_id = ? AND stop_id = ? RETURNING {}",
            SELECT
                .trim_start_matches("SELECT ")
                .trim_end_matches(" FROM home_stop")
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&home_id.0)
            .bind(stop_id)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        row.as_ref().map(map_row).transpose()
    }

    pub async fn delete_home_stops_on_machine(&self, node_id: &str) -> Result<u64> {
        let done = sqlx::query("DELETE FROM home_stop WHERE node_id = ?")
            .bind(node_id)
            .execute(self.write_pool())
            .await
            .map_err(db)?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
mod tests;
