//! Hook repository: CRUD for background hooks (agent-owned scheduled
//! scripts). Rows are written through by the hook scheduler and rehydrated at
//! boot via [`Store::load_active_hooks`].

use intent_core::{AgentId, Hook, HookId, HookState, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const COLUMNS: &str = "hook_id, workspace_id, agent_id, name, code, delay_ms, state, \
    created_at, last_run_at, next_run_at, run_count, last_error, last_logs, last_state, \
    expires_at, perpetual, dispatch_count";

fn state_to_db(state: HookState) -> &'static str {
    match state {
        HookState::Scheduled => "scheduled",
        HookState::Running => "running",
        HookState::Dispatched => "dispatched",
        HookState::Evicted => "evicted",
        HookState::Cancelled => "cancelled",
        HookState::Expired => "expired",
    }
}

fn state_from_db(s: &str) -> Result<HookState> {
    match s {
        "scheduled" => Ok(HookState::Scheduled),
        "running" => Ok(HookState::Running),
        "dispatched" => Ok(HookState::Dispatched),
        "evicted" => Ok(HookState::Evicted),
        "cancelled" => Ok(HookState::Cancelled),
        "expired" => Ok(HookState::Expired),
        _ => Err(intent_core::Error::Internal(format!(
            "invalid hook state: {s}"
        ))),
    }
}

fn hook_from_row(r: &SqliteRow) -> Result<Hook> {
    let state: String = r
        .try_get("state")
        .map_err(|e| intent_core::Error::Internal(format!("read hook row failed: {e}")))?;
    let get = |col: &str| -> Result<String> {
        r.try_get::<String, _>(col)
            .map_err(|e| intent_core::Error::Internal(format!("read hook row failed: {e}")))
    };
    let get_opt = |col: &str| -> Result<Option<String>> {
        r.try_get::<Option<String>, _>(col)
            .map_err(|e| intent_core::Error::Internal(format!("read hook row failed: {e}")))
    };
    let get_i64 = |col: &str| -> Result<i64> {
        r.try_get::<i64, _>(col)
            .map_err(|e| intent_core::Error::Internal(format!("read hook row failed: {e}")))
    };
    let perpetual = get_i64("perpetual")? != 0;
    Ok(Hook {
        hook_id: HookId(get("hook_id")?),
        workspace_id: WorkspaceId(get("workspace_id")?),
        agent_id: AgentId(get("agent_id")?),
        name: get("name")?,
        code: get("code")?,
        delay_ms: get_i64("delay_ms")?,
        state: state_from_db(&state)?,
        created_at: get("created_at")?,
        last_run_at: get_opt("last_run_at")?,
        next_run_at: get_opt("next_run_at")?,
        run_count: get_i64("run_count")?,
        last_error: get_opt("last_error")?,
        last_logs: get_opt("last_logs")?,
        last_state: get_opt("last_state")?,
        expires_at: get_opt("expires_at")?,
        perpetual,
        dispatch_count: get_i64("dispatch_count")?,
    })
}

impl Store {
    /// Insert a new hook row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_hook(&self, h: &Hook) -> Result<()> {
        let sql = format!(
            "INSERT INTO hook ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        sqlx::query(&sql)
            .bind(&h.hook_id.0)
            .bind(&h.workspace_id.0)
            .bind(&h.agent_id.0)
            .bind(&h.name)
            .bind(&h.code)
            .bind(h.delay_ms)
            .bind(state_to_db(h.state))
            .bind(&h.created_at)
            .bind(&h.last_run_at)
            .bind(&h.next_run_at)
            .bind(h.run_count)
            .bind(&h.last_error)
            .bind(&h.last_logs)
            .bind(&h.last_state)
            .bind(&h.expires_at)
            .bind(i64::from(h.perpetual))
            .bind(h.dispatch_count)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("insert hook failed: {e}")))?;
        Ok(())
    }

    /// Get a hook by id; `NotFound` when absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn get_hook(&self, hook_id: &HookId) -> Result<Hook> {
        let sql = format!("SELECT {COLUMNS} FROM hook WHERE hook_id = ?");
        let row = sqlx::query(&sql)
            .bind(&hook_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("get hook failed: {e}")))?;
        match row {
            Some(r) => hook_from_row(&r),
            None => Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            ))),
        }
    }

    /// List all hooks in a workspace, oldest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_hooks_by_workspace(&self, workspace_id: &WorkspaceId) -> Result<Vec<Hook>> {
        let sql = format!("SELECT {COLUMNS} FROM hook WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list hooks by workspace failed: {e}"))
            })?;
        rows.iter().map(hook_from_row).collect()
    }

    /// Number of ACTIVE (`scheduled`/`running`) hooks owned by an agent —
    /// a count-only aggregate for per-turn surfaces (the agent state
    /// snapshot): no `code`/`last_state` blob hydration and no dependence on
    /// how many terminal rows the agent has accumulated, unlike
    /// [`Store::list_hooks_by_agent`] + in-memory filtering.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn count_active_hooks_by_agent(&self, agent_id: &AgentId) -> Result<u64> {
        let n: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM hook \
             WHERE agent_id = ? AND state IN ('scheduled', 'running')",
        )
        .bind(&agent_id.0)
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("count active hooks failed: {e}")))?
        .get::<i64, _>("n");
        Ok(n.cast_unsigned())
    }

    /// Number of ACTIVE (`scheduled`/`running`) hooks in a workspace — the
    /// count-only aggregate behind the `Workspace.waiting` hook probe: no
    /// `code`/`last_state` blob hydration and no dependence on how many
    /// terminal rows the workspace has accumulated over the daemon's
    /// lifetime, unlike [`Store::list_hooks_by_workspace`] + in-memory
    /// filtering (mirrors [`Store::count_active_hooks_by_agent`]).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn count_active_hooks_by_workspace(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        let n: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM hook \
             WHERE workspace_id = ? AND state IN ('scheduled', 'running')",
        )
        .bind(&workspace_id.0)
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| {
            intent_core::Error::Internal(format!("count workspace active hooks failed: {e}"))
        })?
        .get::<i64, _>("n");
        Ok(n.cast_unsigned())
    }

    /// List all hooks owned by an agent, oldest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_hooks_by_agent(&self, agent_id: &AgentId) -> Result<Vec<Hook>> {
        let sql = format!("SELECT {COLUMNS} FROM hook WHERE agent_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list hooks by agent failed: {e}"))
            })?;
        rows.iter().map(hook_from_row).collect()
    }

    /// Set a hook's lifecycle state; `NotFound` when the row is absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_state(&self, hook_id: &HookId, state: HookState) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET state = ? WHERE hook_id = ?")
            .bind(state_to_db(state))
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("update hook state failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Atomically persist a hook's TTL expiry: a single UPDATE sets
    /// `state = 'expired'` AND clears `next_run_at`, so no reader can ever
    /// observe an expired hook with a stale `next_run_at`; `NotFound` when
    /// the row is absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn expire_hook(&self, hook_id: &HookId) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET state = ?, next_run_at = NULL WHERE hook_id = ?")
            .bind(state_to_db(HookState::Expired))
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("expire hook failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Record a completed run: bump `run_count`, set `last_run_at`, and set
    /// (or clear) `next_run_at`; `NotFound` when the row is absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_run(
        &self,
        hook_id: &HookId,
        last_run_at: &str,
        next_run_at: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query(
            "UPDATE hook SET run_count = run_count + 1, last_run_at = ?, next_run_at = ? \
             WHERE hook_id = ?",
        )
        .bind(last_run_at)
        .bind(next_run_at)
        .bind(&hook_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("update hook run failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Bump a hook's `dispatch_count`; `NotFound` when the row is absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn increment_hook_dispatch_count(&self, hook_id: &HookId) -> Result<()> {
        let res =
            sqlx::query("UPDATE hook SET dispatch_count = dispatch_count + 1 WHERE hook_id = ?")
                .bind(&hook_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| {
                    intent_core::Error::Internal(format!("update hook dispatch count failed: {e}"))
                })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Set (or clear) a hook's `next_run_at`; `NotFound` when the row is
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_next_run(
        &self,
        hook_id: &HookId,
        next_run_at: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET next_run_at = ? WHERE hook_id = ?")
            .bind(next_run_at)
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("update hook next run failed: {e}"))
            })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Set (or clear) a hook's `last_error`; `NotFound` when the row is
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_last_error(
        &self,
        hook_id: &HookId,
        last_error: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET last_error = ? WHERE hook_id = ?")
            .bind(last_error)
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("update hook last error failed: {e}"))
            })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Set (or clear) a hook's `last_logs`; `NotFound` when the row is
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_last_logs(
        &self,
        hook_id: &HookId,
        last_logs: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET last_logs = ? WHERE hook_id = ?")
            .bind(last_logs)
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("update hook last logs failed: {e}"))
            })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Set (or clear) a hook's `last_state`; `NotFound` when the row is
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the hook does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_hook_last_state(
        &self,
        hook_id: &HookId,
        last_state: Option<&str>,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE hook SET last_state = ? WHERE hook_id = ?")
            .bind(last_state)
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("update hook last state failed: {e}"))
            })?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Delete a hook row; `NotFound` when absent.
    #[cfg(test)]
    pub(crate) async fn delete_hook(&self, hook_id: &HookId) -> Result<()> {
        let res = sqlx::query("DELETE FROM hook WHERE hook_id = ?")
            .bind(&hook_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("delete hook failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(intent_core::Error::NotFound(format!(
                "hook {} not found",
                hook_id.0
            )));
        }
        Ok(())
    }

    /// Load every active (`scheduled` or `running`) hook across all
    /// workspaces, oldest first — the boot rehydration read for the hook
    /// scheduler.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_active_hooks(&self) -> Result<Vec<Hook>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM hook WHERE state IN ('scheduled', 'running') \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("load active hooks failed: {e}")))?;
        rows.iter().map(hook_from_row).collect()
    }
}
