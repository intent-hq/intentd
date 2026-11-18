//! Home final stops, cloud home rebuilds and home moves (Postgres). Mirrors
//! `home_stop_repo.rs`, `home_rebuild_repo.rs` and `home_move_repo.rs`;
//! every row belongs to the store's tenant.
//!
//! Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order.

use std::collections::HashMap;

use intent_core::machines::{CloudMachine, CloudStartedBy, ExposedMachine};
use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use intent_core::{Error, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use sqlx::postgres::{PgConnection, PgRow, Postgres};
use sqlx::Row;

use super::{db, PgStore, WriteTx};
use crate::{HomeAutoStart, HomeMove, HomeMoveSwitch, HomeRebuild};

const STOP_COLUMNS: &str =
    "home_id, workspace_id, node_id, stop_id, reason, orphaned_at, recorded_at";

const MOVE_COLUMNS: &str = "workspace_id, home_id, node_id, cloud_machine_id, name, \
    from_home_id, from_state, from_since, pushed, started_at";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn stop_row(row: &PgRow) -> Result<HomeStopRecord> {
    let reason: String = col(row, "reason")?;
    Ok(HomeStopRecord {
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        home_id: HomeId(col(row, "home_id")?),
        node_id: col(row, "node_id")?,
        stop_id: col(row, "stop_id")?,
        reason: HomeStopReason::parse(&reason)
            .ok_or_else(|| db(format!("home stop: unknown reason {reason}")))?,
        orphaned_at: col(row, "orphaned_at")?,
        recorded_at: col(row, "recorded_at")?,
    })
}

fn move_row(row: &PgRow) -> Result<HomeMove> {
    let from_state = match col::<String>(row, "from_state")?.as_str() {
        "attached" => HomeState::Attached,
        "detached" => HomeState::Detached,
        "starting" => HomeState::Starting,
        other => return Err(db(format!("home move: unknown state {other}"))),
    };
    let started_at: String = col(row, "started_at")?;
    Ok(HomeMove {
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        target: WorkspaceHome {
            home_id: HomeId(col(row, "home_id")?),
            node_id: col(row, "node_id")?,
            cloud_machine_id: col(row, "cloud_machine_id")?,
            name: col(row, "name")?,
            state: HomeState::Attached,
            since: started_at.clone(),
            last_backup_at: None,
            backup_warning: false,
        },
        from_home_id: HomeId(col(row, "from_home_id")?),
        from_state,
        from_since: col(row, "from_since")?,
        pushed: col::<i64>(row, "pushed")? != 0,
        started_at,
    })
}

/// Insert `record` unless its home has a stop already.
async fn insert_stop(tx: &mut WriteTx, tenant: &str, record: &HomeStopRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO home_stop (tenant_id, home_id, workspace_id, node_id, stop_id, reason, \
         orphaned_at, recorded_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id, home_id) DO NOTHING",
    )
    .bind(tenant)
    .bind(&record.home_id.0)
    .bind(&record.workspace_id.0)
    .bind(&record.node_id)
    .bind(&record.stop_id)
    .bind(record.reason.as_str())
    .bind(&record.orphaned_at)
    .bind(&record.recorded_at)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

async fn machine_present(conn: &mut PgConnection, tenant: &str, node_id: &str) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM exposed_machine WHERE tenant_id = $1 AND id = $2)",
    )
    .bind(tenant)
    .bind(node_id)
    .fetch_one(conn)
    .await
    .map_err(db)
}

/// As `home_stop_repo::stop_move_target`.
async fn stop_move_target(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    now: &str,
) -> Result<()> {
    let target: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT home_id, node_id, cloud_machine_id FROM home_move \
         WHERE tenant_id = $1 AND workspace_id = $2",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;
    let Some((home_id, Some(node_id), cloud_machine_id)) = target else {
        return Ok(());
    };
    // A cloud sandbox that is gone (removed, lost) is owed nothing.
    if cloud_machine_id.is_some() && !machine_present(tx, tenant, &node_id).await? {
        return Ok(());
    }
    let record = HomeStopRecord::new(
        workspace_id.clone(),
        HomeId(home_id),
        node_id,
        HomeStopReason::Retired,
        now,
    );
    insert_stop(tx, tenant, &record).await?;
    sqlx::query("UPDATE home_stop SET discard_push = 1 WHERE tenant_id = $1 AND home_id = $2")
        .bind(tenant)
        .bind(&record.home_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

/// The workspace's current home on an exposed machine, as `(homeId, nodeId)`.
async fn machine_home(
    conn: &mut PgConnection,
    tenant: &str,
    workspace_id: &WorkspaceId,
) -> Result<Option<(String, String)>> {
    sqlx::query_as(
        "SELECT home_id, node_id FROM workspace_home WHERE tenant_id = $1 AND workspace_id = $2 \
         AND node_id IS NOT NULL",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .fetch_optional(conn)
    .await
    .map_err(db)
}

/// The `moved` stop of the workspace's machine home when `home_id` replaces
/// it, inserted.
async fn stop_replaced_home(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    now: &str,
) -> Result<Option<HomeStopRecord>> {
    let stop = match machine_home(tx, tenant, workspace_id).await? {
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
        insert_stop(tx, tenant, stop).await?;
    }
    Ok(stop)
}

/// As `home_stop_repo::retire_workspace_home`, inside the transaction that
/// archives or deletes `workspace_id`.
pub(super) async fn retire_workspace_home(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    now: &str,
    deleted: bool,
) -> Result<()> {
    if let Some((home_id, node_id)) = machine_home(tx, tenant, workspace_id).await? {
        let record = HomeStopRecord::new(
            workspace_id.clone(),
            HomeId(home_id),
            node_id,
            HomeStopReason::Retired,
            now,
        );
        insert_stop(tx, tenant, &record).await?;
    }
    sqlx::query("DELETE FROM workspace_home WHERE tenant_id = $1 AND workspace_id = $2")
        .bind(tenant)
        .bind(&workspace_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    stop_move_target(tx, tenant, workspace_id, now).await?;
    delete_move(tx, tenant, workspace_id).await?;
    if deleted {
        sqlx::query(
            "UPDATE home_stop SET reason = 'retired', orphaned_at = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND reason = 'moved'",
        )
        .bind(tenant)
        .bind(&workspace_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    }
    Ok(())
}

async fn delete_move(tx: &mut WriteTx, tenant: &str, workspace_id: &WorkspaceId) -> Result<()> {
    sqlx::query("DELETE FROM home_move WHERE tenant_id = $1 AND workspace_id = $2")
        .bind(tenant)
        .bind(&workspace_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

/// As `home_rebuild_repo::renew`.
async fn renew(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: WorkspaceId,
    from_home_id: HomeId,
    now: &str,
) -> Result<HomeRebuild> {
    let home_id = HomeId::new();
    sqlx::query(
        "UPDATE workspace_home SET home_id = $4, node_id = NULL, state = 'attached', \
         since = $5, backup_failing_since = NULL, auto_start_lease_id = NULL, \
         auto_start_seq = NULL WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .bind(&from_home_id.0)
    .bind(&home_id.0)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    sqlx::query(
        "INSERT INTO home_rebuild (tenant_id, workspace_id, home_id, from_home_id, recorded_at) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET \
         home_id = excluded.home_id, from_home_id = excluded.from_home_id, \
         recorded_at = excluded.recorded_at",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .bind(&home_id.0)
    .bind(&from_home_id.0)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(HomeRebuild {
        workspace_id,
        home_id,
        from_home_id,
    })
}

/// As `home_rebuild_repo::renew_if_sandbox_gone`.
async fn renew_if_sandbox_gone(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    now: &str,
) -> Result<Option<HomeRebuild>> {
    let gone: Option<String> = sqlx::query_scalar(
        "SELECT home_id FROM workspace_home WHERE tenant_id = $1 AND workspace_id = $2 \
         AND cloud_machine_id IS NOT NULL AND state = 'attached' AND node_id IS NOT NULL \
         AND node_id NOT IN (SELECT id FROM exposed_machine WHERE tenant_id = $1)",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;
    match gone {
        Some(home_id) => Ok(Some(
            renew(tx, tenant, workspace_id.clone(), HomeId(home_id), now).await?,
        )),
        None => Ok(None),
    }
}

async fn move_of(
    conn: &mut PgConnection,
    tenant: &str,
    workspace_id: &WorkspaceId,
) -> Result<Option<HomeMove>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {MOVE_COLUMNS} FROM home_move WHERE tenant_id = $1 AND workspace_id = $2"
    )))
    .bind(tenant)
    .bind(&workspace_id.0)
    .fetch_optional(conn)
    .await
    .map_err(db)?;
    row.as_ref().map(move_row).transpose()
}

/// As `home_move_repo::co_own`.
async fn co_own(
    tx: &mut WriteTx,
    tenant: &str,
    node_id: &str,
    owner: &CloudStartedBy,
) -> Result<()> {
    let cloud: Option<Option<String>> = sqlx::query_scalar(
        "SELECT cloud_json FROM exposed_machine WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;
    let Some(Some(cloud)) = cloud else {
        return Ok(());
    };
    let mut cloud: CloudMachine = serde_json::from_str(&cloud).map_err(db)?;
    if &cloud.started_by == owner {
        return Ok(());
    }
    cloud.started_by = owner.clone();
    sqlx::query("UPDATE exposed_machine SET cloud_json = $3 WHERE tenant_id = $1 AND id = $2")
        .bind(tenant)
        .bind(node_id)
        .bind(serde_json::to_string(&cloud).map_err(db)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

async fn rows_changed(
    mut tx: WriteTx,
    query: sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
) -> Result<bool> {
    let done = query.execute(&mut *tx).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    Ok(done.rows_affected() > 0)
}

// ---- home_stop_repo -----------------------------------------------------------

impl PgStore {
    pub async fn switch_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
        now: &str,
    ) -> Result<Option<HomeStopRecord>> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let stop = stop_replaced_home(&mut tx, tenant, workspace_id, &home.home_id, now).await?;
        sqlx::query(
            "INSERT INTO workspace_home (tenant_id, workspace_id, home_id, node_id, \
             cloud_machine_id, name, state, since, last_backup_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET home_id = excluded.home_id, \
             node_id = excluded.node_id, cloud_machine_id = excluded.cloud_machine_id, \
             name = excluded.name, state = excluded.state, since = excluded.since, \
             last_backup_at = excluded.last_backup_at, \
             backup_failing_since = CASE WHEN workspace_home.home_id = excluded.home_id \
             THEN workspace_home.backup_failing_since END",
        )
        .bind(tenant)
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
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        insert_stop(&mut tx, tenant, record).await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {STOP_COLUMNS} FROM home_stop WHERE tenant_id = $1 AND home_id = $2"
        )))
        .bind(tenant)
        .bind(&record.home_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref()
            .map(stop_row)
            .transpose()?
            .ok_or_else(|| db("home stop: the recorded stop is gone"))
    }

    async fn stops(&self, filter: &str, value: &str) -> Result<Vec<HomeStopRecord>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {STOP_COLUMNS} FROM home_stop WHERE tenant_id = $1 AND {filter} = $2 \
             ORDER BY recorded_at COLLATE \"C\", home_id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(value)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(stop_row).collect()
    }

    pub async fn get_home_stop(&self, home_id: &HomeId) -> Result<Option<HomeStopRecord>> {
        Ok(self.stops("home_id", &home_id.0).await?.pop())
    }

    pub async fn home_stop_discards_push(&self, home_id: &HomeId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let discard: Option<i64> = sqlx::query_scalar(
            "SELECT discard_push FROM home_stop WHERE tenant_id = $1 AND home_id = $2",
        )
        .bind(self.tenant())
        .bind(&home_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(discard.is_some_and(|discard| discard != 0))
    }

    pub async fn list_home_stops(&self, node_id: &str) -> Result<Vec<HomeStopRecord>> {
        self.stops("node_id", node_id).await
    }

    pub async fn list_workspace_home_stops(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<HomeStopRecord>> {
        self.stops("workspace_id", &workspace_id.0).await
    }

    pub async fn finish_home_stop(
        &self,
        home_id: &HomeId,
        stop_id: &str,
    ) -> Result<Option<HomeStopRecord>> {
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM home_stop WHERE tenant_id = $1 AND home_id = $2 AND stop_id = $3 \
             RETURNING {STOP_COLUMNS}"
        )))
        .bind(self.tenant())
        .bind(&home_id.0)
        .bind(stop_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(stop_row).transpose()
    }

    pub async fn delete_home_stops_on_machine(&self, node_id: &str) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query("DELETE FROM home_stop WHERE tenant_id = $1 AND node_id = $2")
            .bind(self.tenant())
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(done.rows_affected())
    }
}

// ---- home_rebuild_repo --------------------------------------------------------

impl PgStore {
    pub async fn remove_cloud_machine(
        &self,
        node_id: &str,
        rebuild_homes: bool,
        now: &str,
    ) -> Result<Vec<HomeRebuild>> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let held: Vec<(String, String)> = if rebuild_homes {
            sqlx::query_as(
                "SELECT workspace_id, home_id FROM workspace_home WHERE tenant_id = $1 \
                 AND node_id = $2 AND cloud_machine_id IS NOT NULL AND state = 'attached' \
                 ORDER BY workspace_id COLLATE \"C\"",
            )
            .bind(tenant)
            .bind(node_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?
        } else {
            Vec::new()
        };
        let mut rebuilt = Vec::with_capacity(held.len());
        for (workspace_id, from_home_id) in held {
            rebuilt.push(
                renew(
                    &mut tx,
                    tenant,
                    WorkspaceId(workspace_id),
                    HomeId(from_home_id),
                    now,
                )
                .await?,
            );
        }
        sqlx::query("DELETE FROM home_stop WHERE tenant_id = $1 AND node_id = $2")
            .bind(tenant)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM exposed_machine WHERE tenant_id = $1 AND id = $2")
            .bind(tenant)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rebuilt)
    }

    pub async fn home_rebuild_of(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<Option<HomeId>> {
        let mut tx = self.begin().await?;
        let from: Option<String> = sqlx::query_scalar(
            "SELECT from_home_id FROM home_rebuild \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(home_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(from.map(HomeId))
    }

    pub async fn finish_home_rebuild(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "DELETE FROM home_rebuild \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(home_id.as_str()),
        )
        .await
    }
}

// ---- home_move_repo -----------------------------------------------------------

impl PgStore {
    pub async fn begin_home_move(&self, home_move: &HomeMove) -> Result<bool> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let moving = sqlx::query(
            "UPDATE workspace_home SET state = 'moving', since = $5 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND state = $4",
        )
        .bind(tenant)
        .bind(&home_move.workspace_id.0)
        .bind(&home_move.from_home_id.0)
        .bind(home_move.from_state.as_str())
        .bind(&home_move.started_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if moving.rows_affected() == 0 {
            return Ok(false);
        }
        let recorded = sqlx::query(
            "INSERT INTO home_move (tenant_id, workspace_id, home_id, node_id, cloud_machine_id, \
             name, from_home_id, from_state, from_since, pushed, started_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (tenant_id, workspace_id) DO NOTHING",
        )
        .bind(tenant)
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

    pub async fn get_home_move(&self, workspace_id: &WorkspaceId) -> Result<Option<HomeMove>> {
        let mut tx = self.begin().await?;
        let found = move_of(&mut tx, self.tenant(), workspace_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    async fn moves(&self, sql: &str, node_id: Option<&str>) -> Result<Vec<HomeMove>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        if let Some(node_id) = node_id {
            query = query.bind(node_id);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(move_row).collect()
    }

    pub async fn list_home_moves(&self) -> Result<Vec<HomeMove>> {
        let sql = format!(
            "SELECT {MOVE_COLUMNS} FROM home_move WHERE tenant_id = $1 \
             ORDER BY started_at COLLATE \"C\", workspace_id COLLATE \"C\""
        );
        self.moves(&sql, None).await
    }

    pub async fn home_moves_to_machine(&self, node_id: &str) -> Result<Vec<HomeMove>> {
        let sql = format!(
            "SELECT {MOVE_COLUMNS} FROM home_move WHERE tenant_id = $1 AND node_id = $2 \
             ORDER BY workspace_id COLLATE \"C\""
        );
        self.moves(&sql, Some(node_id)).await
    }

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

    pub async fn set_home_move_machine(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE home_move SET node_id = $4 WHERE tenant_id = $1 AND workspace_id = $2 \
                 AND home_id = $3 AND node_id IS NULL",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id),
        )
        .await
    }

    pub async fn unclaimed_home_sandboxes(
        &self,
        workspace_id: &WorkspaceId,
        cloud_machine_id: &str,
        owner: &str,
    ) -> Result<Vec<ExposedMachine>> {
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

    pub async fn mark_home_move_pushed(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE home_move SET pushed = 1 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0),
        )
        .await
    }

    pub async fn claim_home_auto_start_or_record_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<HomeAutoStart> {
        let seq = i64::try_from(seq).map_err(db)?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let claimed = sqlx::query(
            "UPDATE workspace_home SET auto_start_lease_id = $5, auto_start_seq = $6 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND node_id = $4 \
             AND (auto_start_lease_id IS NULL OR auto_start_lease_id != $5 \
             OR auto_start_seq IS NULL OR auto_start_seq < $6)",
        )
        .bind(tenant)
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
                "UPDATE home_move SET setup_lease_id = $5, setup_seq = $6 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND node_id = $4",
            )
            .bind(tenant)
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id)
            .bind(lease_id)
            .bind(seq)
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

    pub async fn abort_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        now: &str,
    ) -> Result<bool> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let Some(home_move) = move_of(&mut tx, tenant, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(false);
        };
        stop_move_target(&mut tx, tenant, workspace_id, now).await?;
        delete_move(&mut tx, tenant, workspace_id).await?;
        sqlx::query(
            "UPDATE workspace_home SET state = $4, since = $5 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND state = 'moving'",
        )
        .bind(tenant)
        .bind(&workspace_id.0)
        .bind(&home_move.from_home_id.0)
        .bind(home_move.from_state.as_str())
        .bind(&home_move.from_since)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        renew_if_sandbox_gone(&mut tx, tenant, workspace_id, now).await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn complete_home_move(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        owner: &CloudStartedBy,
        now: &str,
    ) -> Result<Option<HomeMoveSwitch>> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let Some(home_move) = move_of(&mut tx, tenant, workspace_id)
            .await?
            .filter(|found| &found.target.home_id == home_id)
        else {
            return Ok(None);
        };
        if let (Some(node_id), Some(_)) = (
            &home_move.target.node_id,
            &home_move.target.cloud_machine_id,
        ) {
            if !machine_present(&mut tx, tenant, node_id).await? {
                // Its sandbox went (lost, killed, deleted) after it opened:
                // the move fails and rolls back instead.
                return Err(Error::Internal("the new home's machine is gone".into()));
            }
        }
        let stop = stop_replaced_home(&mut tx, tenant, workspace_id, home_id, now).await?;
        let setup: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT setup_lease_id, setup_seq FROM home_move \
             WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(tenant)
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let (setup_lease, setup_seq) = setup.unwrap_or_default();
        let target = &home_move.target;
        let switched = sqlx::query(
            "UPDATE workspace_home SET home_id = $3, node_id = $4, cloud_machine_id = $5, \
             name = $6, state = 'attached', since = $7, backup_failing_since = NULL, \
             auto_start_lease_id = $8, auto_start_seq = $9 \
             WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(tenant)
        .bind(&workspace_id.0)
        .bind(&target.home_id.0)
        .bind(&target.node_id)
        .bind(&target.cloud_machine_id)
        .bind(&target.name)
        .bind(now)
        .bind(&setup_lease)
        .bind(setup_seq)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if switched.rows_affected() == 0 {
            return Ok(None);
        }
        if let (Some(_), Some(node_id)) = (&target.cloud_machine_id, &target.node_id) {
            co_own(&mut tx, tenant, node_id, owner).await?;
        }
        delete_move(&mut tx, tenant, workspace_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(Some(HomeMoveSwitch {
            stop,
            auto_start: setup_lease.is_some(),
        }))
    }
}
