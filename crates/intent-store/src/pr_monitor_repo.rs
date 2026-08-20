//! PR-monitor repository: CRUD for agent-owned pull-request watches. Rows are
//! written through by the centralized monitor loop and rehydrated at boot via
//! [`Store::load_active_pr_monitors`].

use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const COLUMNS: &str = "monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number, \
    state, last_snapshot, baseline_snapshot, pending_changes, pending_since, last_change_at, \
    last_polled_at, last_error, created_at, updated_at";

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

/// Everything one poll write-back can change on a monitor row — the named
/// fields keep the two snapshot columns (and the three timestamp-ish
/// options) from being transposable at call sites. See
/// [`Store::update_pr_monitor_poll`] for the concurrency-guard semantics of
/// `expected_updated_at`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PrMonitorPollUpdate<'a> {
    /// The most recent poll's snapshot (per-poll activity anchor).
    pub last_snapshot: Option<&'a str>,
    /// The emit-baseline snapshot pending changes are recomputed against.
    pub baseline_snapshot: Option<&'a str>,
    pub pending_changes: &'a [String],
    pub pending_since: Option<&'a str>,
    pub last_change_at: Option<&'a str>,
    pub last_polled_at: Option<&'a str>,
    pub last_error: Option<&'a str>,
    pub updated_at: &'a str,
    /// The `updated_at` the caller read; the write lands only if it still
    /// matches (optimistic concurrency).
    pub expected_updated_at: &'a str,
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
        baseline_snapshot: get_opt("baseline_snapshot")?,
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
    /// Insert a new PR-monitor row. Returns `false` (without inserting) when
    /// the `idx_pr_monitor_identity` unique index rejects the row — a
    /// concurrent register already created an ACTIVE monitor for the same
    /// `(agent, repo, PR)` triple; the caller re-arms that row instead.
    pub async fn insert_pr_monitor(&self, m: &PrMonitor) -> Result<bool> {
        let sql = format!(
            "INSERT INTO pr_monitor ({COLUMNS}) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        match sqlx::query(&sql)
            .bind(&m.monitor_id.0)
            .bind(&m.workspace_id.0)
            .bind(&m.agent_id.0)
            .bind(&m.repo_owner)
            .bind(&m.repo_name)
            .bind(m.pr_number)
            .bind(state_to_db(m.state))
            .bind(&m.last_snapshot)
            .bind(&m.baseline_snapshot)
            .bind(pending_to_db(&m.pending_changes))
            .bind(&m.pending_since)
            .bind(&m.last_change_at)
            .bind(&m.last_polled_at)
            .bind(&m.last_error)
            .bind(&m.created_at)
            .bind(&m.updated_at)
            .execute(self.write_pool())
            .await
        {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_database_error()
                    .is_some_and(|d| d.is_unique_violation()) =>
            {
                Ok(false)
            }
            Err(e) => Err(intent_core::Error::Internal(format!(
                "insert pr monitor failed: {e}"
            ))),
        }
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

    /// List an agent's ACTIVE monitors only, oldest first — the SQL-filtered
    /// counterpart to [`Store::list_pr_monitors_by_agent`] for read paths
    /// that only care about active rows (idle-visibility's
    /// `waitingOnPrMonitors`), so cost is O(active monitors) rather than
    /// O(all monitor history for the agent).
    pub async fn list_active_pr_monitors_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? AND state = 'active' \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list active pr monitors by agent failed: {e}"
                ))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List a workspace's ACTIVE monitors only, oldest first — the
    /// SQL-filtered counterpart to [`Store::list_pr_monitors_by_workspace`]
    /// for `agent.list`/`agent.diagnostics`, so cost is O(active monitors in
    /// the workspace) rather than O(all monitor history in the workspace).
    pub async fn list_active_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? AND state = 'active' \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list active pr monitors by workspace failed: {e}"
                ))
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

    /// Every non-cancelled (active or completed) monitor across all
    /// workspaces, oldest first — the single bulk read backing the
    /// `workspace.list` / `workspace.subscribe` seq-0 PR merge. Completed
    /// rows are retained so merged PRs stay visible; cancelled rows are
    /// excluded (they are removed from the UI), matching the services-level
    /// per-workspace view ([`Services::pr_monitors_for_workspace`]). Unless
    /// `include_archived`, rows owned by archived workspaces are filtered in
    /// SQL so cost tracks the workspaces the list call actually returns.
    pub async fn load_non_cancelled_pr_monitors(
        &self,
        include_archived: bool,
    ) -> Result<Vec<PrMonitor>> {
        let archived_filter = if include_archived {
            ""
        } else {
            " AND workspace_id IN (SELECT id FROM workspace WHERE archived = 0)"
        };
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE state != 'cancelled'\
             {archived_filter} ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("load non-cancelled pr monitors failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Set a monitor's lifecycle state. Every legal transition starts from
    /// `active`, so the update is guarded on it; returns `false` when the row
    /// is absent or already terminal (a concurrent cancel/complete won) so
    /// the caller can skip its side effects instead of resurrecting the row.
    pub async fn update_pr_monitor_state(
        &self,
        monitor_id: &PrMonitorId,
        state: PrMonitorState,
        updated_at: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE pr_monitor SET state = ?, updated_at = ? \
             WHERE monitor_id = ? AND state = 'active'",
        )
        .bind(state_to_db(state))
        .bind(updated_at)
        .bind(&monitor_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| {
            intent_core::Error::Internal(format!("update pr monitor state failed: {e}"))
        })?;
        Ok(res.rows_affected() > 0)
    }

    /// Write back everything one poll can change: the last-poll snapshot, the
    /// emit-baseline snapshot, the pending changes and their debounce anchors,
    /// the poll timestamp, and the last forge error. One statement so a reader
    /// never observes a baseline that moved without its pending changes.
    ///
    /// Optimistic-concurrency guarded: the write only lands when the row is
    /// still `active` AND its `updated_at` still equals
    /// [`PrMonitorPollUpdate::expected_updated_at`] (the value the caller
    /// read). Returns `false` when the guard fails — a concurrent
    /// flush/cancel/re-register/poll moved the row, and the caller must
    /// discard its stale image (skip emits) rather than clobber.
    pub async fn update_pr_monitor_poll(
        &self,
        monitor_id: &PrMonitorId,
        update: PrMonitorPollUpdate<'_>,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE pr_monitor SET last_snapshot = ?, baseline_snapshot = ?, \
             pending_changes = ?, pending_since = ?, last_change_at = ?, last_polled_at = ?, \
             last_error = ?, updated_at = ? \
             WHERE monitor_id = ? AND state = 'active' AND updated_at = ?",
        )
        .bind(update.last_snapshot)
        .bind(update.baseline_snapshot)
        .bind(pending_to_db(update.pending_changes))
        .bind(update.pending_since)
        .bind(update.last_change_at)
        .bind(update.last_polled_at)
        .bind(update.last_error)
        .bind(update.updated_at)
        .bind(&monitor_id.0)
        .bind(update.expected_updated_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("update pr monitor poll failed: {e}")))?;
        Ok(res.rows_affected() > 0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use intent_core::{
        now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use uuid::Uuid;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("test-pr-monitor-{}.db", Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut sidecar = self.path.clone().into_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(&sidecar);
            }
        }
    }

    fn test_workspace(ws_id: &WorkspaceId, ts: &str) -> Workspace {
        Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn test_session(agent_id: &AgentId, ws_id: &WorkspaceId, ts: &str) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Owner".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

    /// Open a store with one workspace + agent session (the FK targets a
    /// `pr_monitor` row needs) and return them.
    async fn store_with_owner() -> (TempDb, Store, WorkspaceId, AgentId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-pr-monitor".to_string());
        store
            .insert_workspace(&test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&agent_id, &ws_id, &ts))
            .await
            .expect("insert session");
        (tmp, store, ws_id, agent_id)
    }

    fn test_monitor(ws_id: &WorkspaceId, agent_id: &AgentId, ts: &str) -> PrMonitor {
        PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: ws_id.clone(),
            agent_id: agent_id.clone(),
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: Some(r#"{"v":1}"#.to_string()),
            baseline_snapshot: Some(r#"{"v":1}"#.to_string()),
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
        }
    }

    /// `baseline_snapshot` round-trips through insert/get, and
    /// `update_pr_monitor_poll` moves it independently of `last_snapshot`
    /// (they are distinct columns: the poll baseline advances every poll,
    /// the emit baseline only on delivered wakes).
    #[tokio::test]
    async fn baseline_snapshot_round_trip() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let m = test_monitor(&ws_id, &agent_id, &ts);
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(read.baseline_snapshot.as_deref(), Some(r#"{"v":1}"#));
        assert_eq!(read.last_snapshot.as_deref(), Some(r#"{"v":1}"#));

        let now = now_iso();
        assert!(store
            .update_pr_monitor_poll(
                &m.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: Some(r#"{"v":3}"#),
                    baseline_snapshot: Some(r#"{"v":2}"#),
                    pending_changes: &["mergeable: true → false".to_string()],
                    pending_since: Some(&now),
                    last_change_at: Some(&now),
                    last_polled_at: Some(&now),
                    last_error: None,
                    updated_at: &now,
                    expected_updated_at: &m.updated_at,
                },
            )
            .await
            .expect("update poll"));
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(
            read.last_snapshot.as_deref(),
            Some(r#"{"v":3}"#),
            "poll baseline moved"
        );
        assert_eq!(
            read.baseline_snapshot.as_deref(),
            Some(r#"{"v":2}"#),
            "emit baseline written independently"
        );
        assert_eq!(read.pending_changes, vec!["mergeable: true → false"]);
    }

    /// The 0089 migration backfills `baseline_snapshot` from `last_snapshot`
    /// on pre-existing rows (simulated by dropping the column and re-running
    /// the migration file verbatim), so active monitors upgraded across the
    /// migration keep a usable emit baseline. A NULL `last_snapshot` stays
    /// NULL.
    #[tokio::test]
    async fn migration_backfills_baseline_from_last_snapshot() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        sqlx::query("ALTER TABLE pr_monitor DROP COLUMN baseline_snapshot")
            .execute(store.write_pool())
            .await
            .expect("drop 0089 column");

        let ts = now_iso();
        for (id, snapshot) in [
            ("prmon-polled", Some(r#"{"v":7}"#)),
            ("prmon-never-polled", None),
        ] {
            sqlx::query(
                "INSERT INTO pr_monitor (monitor_id, workspace_id, agent_id, repo_owner, \
                 repo_name, pr_number, state, last_snapshot, created_at, updated_at) \
                 VALUES (?, ?, ?, 'o', 'r', ?, 'active', ?, ?, ?)",
            )
            .bind(id)
            .bind(&ws_id.0)
            .bind(&agent_id.0)
            .bind(if snapshot.is_some() { 1_i64 } else { 2_i64 })
            .bind(snapshot)
            .bind(&ts)
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("insert raw pre-0089 row");
        }

        sqlx::raw_sql(include_str!("../migrations/0089_pr_monitor_baseline.sql"))
            .execute(store.write_pool())
            .await
            .expect("re-run 0089 migration");

        let polled = store
            .get_pr_monitor(&PrMonitorId("prmon-polled".to_string()))
            .await
            .expect("get polled");
        assert_eq!(
            polled.baseline_snapshot.as_deref(),
            Some(r#"{"v":7}"#),
            "baseline backfilled from last_snapshot"
        );
        let never = store
            .get_pr_monitor(&PrMonitorId("prmon-never-polled".to_string()))
            .await
            .expect("get never-polled");
        assert_eq!(never.baseline_snapshot, None, "NULL stays NULL");
    }
}
