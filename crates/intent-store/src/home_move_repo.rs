//! Home moves on an orchestrator (monorepo
//! `docs/protocol/methods/nodes.md`, "Moving the home"): the move in
//! progress of each workspace, kept so it goes on after a restart.

use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use std::collections::HashMap;

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

/// What [`Store::claim_home_auto_start_or_record_move`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeAutoStart {
    /// The home is the workspace's: start its `autoStart` scripts now.
    Claimed,
    /// The home is a move's new home: the switch claims them.
    AtSwitch,
    /// Neither, or claimed already.
    No,
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

impl HomeMove {
    /// The home clients see while this move runs from `home`, the
    /// workspace's home: once the old home's last backup landed, a move to
    /// a cloud home shows its new home `starting` until the switch
    /// (nodes.md "Cloud homes" 6); else `home` as is. The store keeps the
    /// old home `moving`, so every call stays refused as for any move.
    #[must_use]
    pub fn shown(&self, home: &WorkspaceHome) -> WorkspaceHome {
        if home.state != HomeState::Moving
            || home.home_id != self.from_home_id
            || !self.pushed
            || self.target.cloud_machine_id.is_none()
        {
            return home.clone();
        }
        WorkspaceHome {
            state: HomeState::Starting,
            since: home.since.clone(),
            last_backup_at: home.last_backup_at.clone(),
            backup_warning: home.backup_warning,
            ..self.target.clone()
        }
    }
}

fn map_row(row: &SqliteRow) -> Result<HomeMove> {
    let get = |name: &str| row.try_get::<Option<String>, _>(name).map_err(db);
    let from_state = match row.try_get::<String, _>("from_state").map_err(db)?.as_str() {
        "attached" => HomeState::Attached,
        "detached" => HomeState::Detached,
        "starting" => HomeState::Starting,
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

/// Inside a transaction: `owner` becomes cloud machine `node_id`'s
/// `cloud.startedBy`. Nothing for a row that is not a cloud machine.
async fn co_own(
    conn: &mut SqliteConnection,
    node_id: &str,
    owner: &intent_core::machines::CloudStartedBy,
) -> Result<()> {
    let cloud: Option<Option<String>> =
        sqlx::query_scalar("SELECT cloud_json FROM exposed_machine WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?;
    let Some(Some(cloud)) = cloud else {
        return Ok(());
    };
    let mut cloud: intent_core::machines::CloudMachine =
        serde_json::from_str(&cloud).map_err(db)?;
    if &cloud.started_by == owner {
        return Ok(());
    }
    cloud.started_by = owner.clone();
    sqlx::query("UPDATE exposed_machine SET cloud_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&cloud).map_err(db)?)
        .bind(node_id)
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
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

    /// The workspace's home as clients see it ([`HomeMove::shown`]).
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn get_shown_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceHome>> {
        let Some(home) = self.get_workspace_home(workspace_id).await? else {
            return Ok(None);
        };
        if home.state != HomeState::Moving {
            return Ok(Some(home));
        }
        Ok(Some(match self.get_home_move(workspace_id).await? {
            Some(home_move) => home_move.shown(&home),
            None => home,
        }))
    }

    /// Every workspace's home as clients see it ([`HomeMove::shown`]).
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn list_shown_workspace_homes(&self) -> Result<HashMap<WorkspaceId, WorkspaceHome>> {
        let mut homes = self.list_workspace_homes().await?;
        if homes.values().any(|home| home.state == HomeState::Moving) {
            for home_move in self.list_home_moves().await? {
                if let Some(home) = homes.get_mut(&home_move.workspace_id) {
                    *home = home_move.shown(home);
                }
            }
        }
        Ok(homes)
    }

    /// Record exposed machine `node_id` (a sandbox started for it) as the
    /// new home of the move to `home_id`; `false` when that move is gone
    /// or already has a machine.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn set_home_move_machine(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
    ) -> Result<bool> {
        let done = sqlx::query(
            "UPDATE home_move SET node_id = ? \
             WHERE workspace_id = ? AND home_id = ? AND node_id IS NULL",
        )
        .bind(node_id)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// The cloud machines of entry `cloud_machine_id` in `workspace_id`
    /// started for `owner` (a home-only start's key) that hold no home, owe
    /// no stop and are no move's target: sandboxes whose start a restart
    /// cut before anything recorded them. Oldest first.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn unclaimed_home_sandboxes(
        &self,
        workspace_id: &WorkspaceId,
        cloud_machine_id: &str,
        owner: &str,
    ) -> Result<Vec<intent_core::machines::ExposedMachine>> {
        let mut left = Vec::new();
        for row in self.list_cloud_machines().await? {
            let Some(cloud) = &row.cloud else { continue };
            if cloud.agent_id != owner
                || cloud.workspace_id != workspace_id.as_str()
                || cloud.cloud_machine_id != cloud_machine_id
            {
                continue;
            }
            if self.homes_on_machine(&row.id).await?.is_empty()
                && self.list_home_stops(&row.id).await?.is_empty()
                && self.home_moves_to_machine(&row.id).await?.is_empty()
            {
                left.push(row);
            }
        }
        Ok(left)
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

    /// A `setupFinished` record of the home `home_id` on `node_id`, seq
    /// `seq` of lease `lease_id`'s journal, in one transaction: it claims
    /// the home's `autoStart` (as
    /// [`Store::claim_workspace_home_auto_start`]) when the home is the
    /// workspace's; else, when it is the new home of a move, it is recorded
    /// on the move and the switch claims it.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn claim_home_auto_start_or_record_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<HomeAutoStart> {
        let seq = i64::try_from(seq).map_err(db)?;
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let claimed = sqlx::query(
            "UPDATE workspace_home SET auto_start_lease_id = ?, auto_start_seq = ? \
             WHERE workspace_id = ? AND home_id = ? AND node_id = ? \
             AND (auto_start_lease_id IS NULL OR auto_start_lease_id != ? \
             OR auto_start_seq IS NULL OR auto_start_seq < ?)",
        )
        .bind(lease_id)
        .bind(seq)
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .bind(node_id)
        .bind(lease_id)
        .bind(seq)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let outcome = if claimed.rows_affected() > 0 {
            HomeAutoStart::Claimed
        } else {
            let recorded = sqlx::query(
                "UPDATE home_move SET setup_lease_id = ?, setup_seq = ? \
                 WHERE workspace_id = ? AND home_id = ? AND node_id = ?",
            )
            .bind(lease_id)
            .bind(seq)
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            if recorded.rows_affected() > 0 {
                HomeAutoStart::AtSwitch
            } else {
                HomeAutoStart::No
            }
        };
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// Roll back the move to `home_id`: in one transaction the move goes,
    /// the old home gets back its state and `since`, and a target on a
    /// machine gets a `retired` stop whose last push is discarded (its
    /// machine may have opened it although the reply never came). An old
    /// cloud home whose sandbox was removed meanwhile is renewed for a
    /// rebuild in the same transaction. `false` when that move is gone.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn abort_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        now: &str,
    ) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let Some(home_move) = move_of(&mut tx, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(false);
        };
        crate::home_stop_repo::stop_move_target(&mut tx, workspace_id, now).await?;
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
        crate::home_rebuild_repo::renew_if_sandbox_gone(&mut tx, workspace_id, now).await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    /// The switch of the move to `home_id` (nodes.md "Moving the home" step
    /// 3): in one transaction the workspace's home becomes the move's new
    /// home, `attached` since `now` (its `lastBackupAt` kept: the new home
    /// starts from that backup), a `moved` stop of the old machine home is
    /// recorded and the move goes; an error when the new home's machine is
    /// gone, so the move rolls back. A setup the new home finished before
    /// claims its `autoStart` here. A new home on a cloud sandbox gets
    /// `owner` as the sandbox's `cloud.startedBy` in the same transaction
    /// (nodes.md "Cloud homes" 2). `None` when that move is gone (rolled
    /// back, or the workspace archived or deleted): nothing is written.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn complete_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        owner: &intent_core::machines::CloudStartedBy,
        now: &str,
    ) -> Result<Option<HomeMoveSwitch>> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let Some(home_move) = move_of(&mut tx, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(None);
        };
        if let Some(node_id) = &home_move.target.node_id {
            let present: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM exposed_machine WHERE id = ?")
                    .bind(node_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            if present.is_none() {
                // Its sandbox went (lost, killed, deleted) after it opened:
                // the move fails and rolls back instead.
                return Err(Error::Internal("the new home's machine is gone".into()));
            }
        }
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
        if let (Some(_), Some(node_id)) = (&target.cloud_machine_id, &target.node_id) {
            co_own(&mut tx, node_id, owner).await?;
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

#[cfg(test)]
mod tests;
