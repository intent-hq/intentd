//! Cloud home rebuilds on an orchestrator (monorepo
//! `docs/protocol/methods/nodes.md`, "Cloud homes" 5): a cloud home whose
//! sandbox was deleted gets a new `homeId` with no sandbox, and starts again
//! at its next use from the hub's `home/head` and `home/wip`.

use intent_core::{Error, HomeId, Result, WorkspaceId};
use sqlx::{Row, SqliteConnection};

use crate::Store;

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("home rebuild: {error}"))
}

/// A cloud home that lost its sandbox: the workspace's new home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeRebuild {
    pub workspace_id: WorkspaceId,
    /// The new home, with no sandbox.
    pub home_id: HomeId,
    /// The home the deleted sandbox held.
    pub from_home_id: HomeId,
}

/// Give the workspace's home `from_home_id` a new `homeId` with no machine,
/// still `attached`, no backup failure and no `autoStart` claimed, and
/// record the rebuild.
async fn renew(
    conn: &mut SqliteConnection,
    workspace_id: WorkspaceId,
    from_home_id: HomeId,
    now: &str,
) -> Result<HomeRebuild> {
    let home_id = HomeId::new();
    sqlx::query(
        "UPDATE workspace_home SET home_id = ?, node_id = NULL, state = 'attached', since = ?, \
         backup_failing_since = NULL, auto_start_lease_id = NULL, auto_start_seq = NULL \
         WHERE workspace_id = ? AND home_id = ?",
    )
    .bind(&home_id.0)
    .bind(now)
    .bind(&workspace_id.0)
    .bind(&from_home_id.0)
    .execute(&mut *conn)
    .await
    .map_err(db)?;
    sqlx::query(
        "INSERT INTO home_rebuild (workspace_id, home_id, from_home_id, recorded_at) \
         VALUES (?, ?, ?, ?) ON CONFLICT(workspace_id) DO UPDATE SET \
         home_id = excluded.home_id, from_home_id = excluded.from_home_id, \
         recorded_at = excluded.recorded_at",
    )
    .bind(&workspace_id.0)
    .bind(&home_id.0)
    .bind(&from_home_id.0)
    .bind(now)
    .execute(&mut *conn)
    .await
    .map_err(db)?;
    Ok(HomeRebuild {
        workspace_id,
        home_id,
        from_home_id,
    })
}

/// Inside a transaction that gives `workspace_id` its home back (a move
/// rolled back): an attached cloud home whose machine row is gone (its
/// sandbox was removed while the home was moving) is renewed for a rebuild,
/// as [`Store::remove_cloud_machine`] does.
pub(crate) async fn renew_if_sandbox_gone(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    now: &str,
) -> Result<Option<HomeRebuild>> {
    let gone: Option<String> = sqlx::query_scalar(
        "SELECT home_id FROM workspace_home WHERE workspace_id = ? \
         AND cloud_machine_id IS NOT NULL AND state = 'attached' AND node_id IS NOT NULL \
         AND node_id NOT IN (SELECT id FROM exposed_machine)",
    )
    .bind(&workspace_id.0)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?;
    match gone {
        Some(home_id) => Ok(Some(
            renew(conn, workspace_id.clone(), HomeId(home_id), now).await?,
        )),
        None => Ok(None),
    }
}

impl Store {
    /// Remove cloud machine `node_id`'s row, whose sandbox is gone, in one
    /// transaction with what goes with it: the home stops it owed (no
    /// machine is left to stop) and, with `rebuild_homes`, each attached
    /// cloud home it held, which is renewed for a rebuild at its next use
    /// (nodes.md "Cloud homes" 5). A home that is starting is left to its
    /// start, which detaches it when it fails; one that is moving is renewed
    /// when its move rolls back. The homes renewed.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn remove_cloud_machine(
        &self,
        node_id: &str,
        rebuild_homes: bool,
        now: &str,
    ) -> Result<Vec<HomeRebuild>> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let held = if rebuild_homes {
            sqlx::query(
                "SELECT workspace_id, home_id FROM workspace_home WHERE node_id = ? \
             AND cloud_machine_id IS NOT NULL AND state = 'attached' ORDER BY workspace_id",
            )
            .bind(node_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?
        } else {
            Vec::new()
        };
        let mut rebuilt = Vec::with_capacity(held.len());
        for row in held {
            let workspace_id = WorkspaceId(row.try_get("workspace_id").map_err(db)?);
            let from_home_id = HomeId(row.try_get("home_id").map_err(db)?);
            rebuilt.push(renew(&mut tx, workspace_id, from_home_id, now).await?);
        }
        sqlx::query("DELETE FROM home_stop WHERE node_id = ?")
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM exposed_machine WHERE id = ?")
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rebuilt)
    }

    /// The home that `home_id`, the workspace's home, rebuilds (its
    /// deleted sandbox held it), while that home has not opened yet.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn home_rebuild_of(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<Option<HomeId>> {
        let from: Option<String> = sqlx::query_scalar(
            "SELECT from_home_id FROM home_rebuild WHERE workspace_id = ? AND home_id = ?",
        )
        .bind(&workspace_id.0)
        .bind(home_id.as_str())
        .fetch_optional(self.read_pool())
        .await
        .map_err(db)?;
        Ok(from.map(HomeId))
    }

    /// Rebuilt home `home_id` opened: `true` once, for the call that
    /// posts its notice.
    ///
    /// # Errors
    ///
    /// `Error::Internal` on a database failure.
    pub async fn finish_home_rebuild(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<bool> {
        let done = sqlx::query("DELETE FROM home_rebuild WHERE workspace_id = ? AND home_id = ?")
            .bind(&workspace_id.0)
            .bind(home_id.as_str())
            .execute(self.write_pool())
            .await
            .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests;
