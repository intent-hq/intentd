//! Home moves on an orchestrator (monorepo
//! `docs/protocol/methods/nodes.md`, "Moving the home"): the move in
//! progress of each workspace, kept so it goes on after a restart.

use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use intent_core::{Error, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

use crate::Store;

const SELECT: &str = "SELECT workspace_id, home_id, node_id, cloud_machine_id, name, \
    from_home_id, from_state, from_since, pushed, started_at FROM home_move";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("home move: {error}"))
}

/// What [`Store::complete_home_move`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeMoveSwitch {
    /// The old machine home's `moved` stop, when it was on a machine.
    pub stop: Option<HomeStopRecord>,
    /// The new home finished its setup before the switch, which claimed
    /// its `autoStart`: the caller starts the scripts.
    pub auto_start: bool,
}

/// A workspace's home move in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeMove {
    pub workspace_id: WorkspaceId,
    /// The new home: its new `homeId`, machine or cloud entry, and name.
    pub target: WorkspaceHome,
    /// The home being moved away from, and its state and `since` before the
    /// move (a rollback gives them back).
    pub from_home_id: HomeId,
    pub from_state: HomeState,
    pub from_since: String,
    /// The old home's last backup landed (step 1 is done).
    pub pushed: bool,
    pub started_at: String,
}

fn map_row(row: &SqliteRow) -> Result<HomeMove> {
    let get = |name: &str| row.try_get::<Option<String>, _>(name).map_err(db);
    let from_state = match row.try_get::<String, _>("from_state").map_err(db)?.as_str() {
        "attached" => HomeState::Attached,
        "detached" => HomeState::Detached,
        other => return Err(db(format!("unknown state {other}"))),
    };
    let started_at: String = row.try_get("started_at").map_err(db)?;
    Ok(HomeMove {
        workspace_id: WorkspaceId(row.try_get("workspace_id").map_err(db)?),
        target: WorkspaceHome {
            home_id: HomeId(row.try_get("home_id").map_err(db)?),
            node_id: get("node_id")?,
            cloud_machine_id: get("cloud_machine_id")?,
            name: row.try_get("name").map_err(db)?,
            state: HomeState::Attached,
            since: started_at.clone(),
            last_backup_at: None,
            backup_warning: false,
        },
        from_home_id: HomeId(row.try_get("from_home_id").map_err(db)?),
        from_state,
        from_since: row.try_get("from_since").map_err(db)?,
        pushed: row.try_get::<i64, _>("pushed").map_err(db)? != 0,
        started_at,
    })
}

async fn move_of(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
) -> Result<Option<HomeMove>> {
    let sql = format!("{SELECT} WHERE workspace_id = ?");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(&workspace_id.0)
        .fetch_optional(conn)
        .await
        .map_err(db)?;
    row.as_ref().map(map_row).transpose()
}

impl Store {
    /// Start `home_move`: in one transaction the workspace's home
    /// `from_home_id` goes from `from_state` to `moving` (since
    /// `started_at`) and the move is recorded. `false` when that home is not
    /// the workspace's (any more), is not in `from_state`, or a move is
    /// already recorded: nothing is written then.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn begin_home_move(&self, home_move: &HomeMove) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let moving = sqlx::query(
            "UPDATE workspace_home SET state = 'moving', since = ? \
             WHERE workspace_id = ? AND home_id = ? AND state = ?",
        )
        .bind(&home_move.started_at)
        .bind(&home_move.workspace_id.0)
        .bind(&home_move.from_home_id.0)
        .bind(home_move.from_state.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if moving.rows_affected() == 0 {
            return Ok(false);
        }
        let recorded = sqlx::query(
            "INSERT INTO home_move (workspace_id, home_id, node_id, cloud_machine_id, name, \
             from_home_id, from_state, from_since, pushed, started_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(workspace_id) DO NOTHING",
        )
        .bind(&home_move.workspace_id.0)
        .bind(&home_move.target.home_id.0)
        .bind(&home_move.target.node_id)
        .bind(&home_move.target.cloud_machine_id)
        .bind(&home_move.target.name)
        .bind(&home_move.from_home_id.0)
        .bind(home_move.from_state.as_str())
        .bind(&home_move.from_since)
        .bind(i64::from(home_move.pushed))
        .bind(&home_move.started_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if recorded.rows_affected() == 0 {
            return Ok(false);
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    /// The workspace's move in progress, if any.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn get_home_move(&self, workspace_id: &WorkspaceId) -> Result<Option<HomeMove>> {
        let mut conn = self.write_pool().acquire().await.map_err(db)?;
        move_of(&mut conn, workspace_id).await
    }

    /// Every move in progress, oldest first.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn list_home_moves(&self) -> Result<Vec<HomeMove>> {
        let sql = format!("{SELECT} ORDER BY started_at, workspace_id");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(self.write_pool())
            .await
            .map_err(db)?;
        rows.iter().map(map_row).collect()
    }

    /// The moves in progress whose new home is on the exposed machine
    /// `node_id`.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn home_moves_to_machine(&self, node_id: &str) -> Result<Vec<HomeMove>> {
        let sql = format!("{SELECT} WHERE node_id = ? ORDER BY workspace_id");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(node_id)
            .fetch_all(self.write_pool())
            .await
            .map_err(db)?;
        rows.iter().map(map_row).collect()
    }

    /// Record that the move to `home_id` has the old home's last backup;
    /// `false` when that move is gone.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn mark_home_move_pushed(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<bool> {
        let done =
            sqlx::query("UPDATE home_move SET pushed = 1 WHERE workspace_id = ? AND home_id = ?")
                .bind(&workspace_id.0)
                .bind(&home_id.0)
                .execute(self.write_pool())
                .await
                .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// Record that the new home `home_id` of a move, on `node_id`, finished
    /// its setup in the `setupFinished` record `seq` of lease `lease_id`'s
    /// journal, before the switch. `false` when no such move is recorded.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn record_home_move_setup(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<bool> {
        let seq = i64::try_from(seq).map_err(db)?;
        let done = sqlx::query(
            "UPDATE home_move SET setup_lease_id = ?, setup_seq = ? \
             WHERE workspace_id = ? AND home_id = ? AND node_id = ?",
        )
        .bind(lease_id)
        .bind(seq)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .bind(node_id)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// Roll back the move to `home_id`: in one transaction the move goes and
    /// the old home gets back its state and `since`. `false` when that move
    /// is gone.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn abort_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let Some(home_move) = move_of(&mut tx, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(false);
        };
        sqlx::query("DELETE FROM home_move WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query(
            "UPDATE workspace_home SET state = ?, since = ? \
             WHERE workspace_id = ? AND home_id = ? AND state = 'moving'",
        )
        .bind(home_move.from_state.as_str())
        .bind(&home_move.from_since)
        .bind(&workspace_id.0)
        .bind(&home_move.from_home_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    /// The switch of the move to `home_id` (nodes.md "Moving the home" step
    /// 3): in one transaction the workspace's home becomes the move's new
    /// home, `attached` since `now` (its `lastBackupAt` kept: the new home
    /// starts from that backup), a `moved` stop of the old machine home is
    /// recorded and the move goes. A setup the new home finished before
    /// claims its `autoStart` here. `None` when that move is gone (rolled
    /// back, or the workspace archived or deleted): nothing is written.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn complete_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        now: &str,
    ) -> Result<Option<HomeMoveSwitch>> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let Some(home_move) = move_of(&mut tx, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(None);
        };
        let stop = match crate::home_stop_repo::machine_home(&mut tx, workspace_id).await? {
            Some((old, node_id)) if old != home_id.0 => Some(HomeStopRecord::new(
                workspace_id.clone(),
                HomeId(old),
                node_id,
                HomeStopReason::Moved,
                now,
            )),
            _ => None,
        };
        if let Some(stop) = &stop {
            crate::home_stop_repo::insert(&mut tx, stop).await?;
        }
        let setup: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT setup_lease_id, setup_seq FROM home_move WHERE workspace_id = ?",
        )
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let (setup_lease, setup_seq) = setup.unwrap_or_default();
        let target = &home_move.target;
        let switched = sqlx::query(
            "UPDATE workspace_home SET home_id = ?, node_id = ?, cloud_machine_id = ?, name = ?, \
             state = 'attached', since = ?, backup_failing_since = NULL, \
             auto_start_lease_id = ?, auto_start_seq = ? WHERE workspace_id = ?",
        )
        .bind(&target.home_id.0)
        .bind(&target.node_id)
        .bind(&target.cloud_machine_id)
        .bind(&target.name)
        .bind(now)
        .bind(&setup_lease)
        .bind(setup_seq)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if switched.rows_affected() == 0 {
            return Ok(None);
        }
        sqlx::query("DELETE FROM home_move WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Some(HomeMoveSwitch {
            stop,
            auto_start: setup_lease.is_some(),
        }))
    }
}
