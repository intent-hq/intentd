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

/// The narrow projection of one non-cancelled monitor row consumed by the
/// `workspace.list` / `workspace.subscribe` seq-0 PR merge: the identity /
/// lifecycle columns plus the handful of scalar fields the list decoration
/// reads out of `last_snapshot`. The snapshot scalars are extracted in SQL
/// (`json_extract`), so the row's JSON blob columns (`last_snapshot`,
/// `baseline_snapshot`, `pending_changes`) are never shipped to Rust or
/// deserialized on this hot path — deliberately not a [`PrMonitor`], so the
/// type itself guarantees the bulk read cannot regrow the blobs
/// (intent-hq/monorepo#3878).
///
/// The `snapshot_*` fields are `None` when the monitor has no snapshot yet or
/// the persisted blob is not valid JSON (mirroring the tolerant
/// `serde_json::from_str(..).ok()` parse this projection replaced);
/// `snapshot_url` / `snapshot_title` / `snapshot_is_draft` are mandatory in
/// a serialized snapshot, so `Some` on any of them means "snapshot present".
#[derive(Debug, Clone)]
pub struct PrMonitorListEntry {
    pub workspace_id: WorkspaceId,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: i64,
    pub state: PrMonitorState,
    pub created_at: String,
    pub updated_at: String,
    /// `$.url` of `last_snapshot` — the PR's HTML URL.
    pub snapshot_url: Option<String>,
    /// `$.title` of `last_snapshot`.
    pub snapshot_title: Option<String>,
    /// `$.headSha` of `last_snapshot` (optional in the snapshot itself).
    pub snapshot_head_sha: Option<String>,
    /// `$.requirements.state` of `last_snapshot` — the checklist's 4-value
    /// lifecycle word (`open` / `draft` / `closed` / `merged`).
    pub snapshot_state: Option<String>,
    /// `$.requirements.isDraft` of `last_snapshot`.
    pub snapshot_is_draft: Option<bool>,
    /// `$.requirements.mergeable` of `last_snapshot` (tri-state: omitted
    /// while the forge is still computing).
    pub snapshot_mergeable: Option<bool>,
}

fn list_entry_from_row(r: &SqliteRow) -> Result<PrMonitorListEntry> {
    let err =
        |e: sqlx::Error| intent_core::Error::Internal(format!("read pr monitor list row: {e}"));
    let get = |col: &str| -> Result<String> { r.try_get::<String, _>(col).map_err(err) };
    let get_opt =
        |col: &str| -> Result<Option<String>> { r.try_get::<Option<String>, _>(col).map_err(err) };
    Ok(PrMonitorListEntry {
        workspace_id: WorkspaceId(get("workspace_id")?),
        repo_owner: get("repo_owner")?,
        repo_name: get("repo_name")?,
        pr_number: r.try_get::<i64, _>("pr_number").map_err(err)?,
        state: state_from_db(&get("state")?)?,
        created_at: get("created_at")?,
        updated_at: get("updated_at")?,
        snapshot_url: get_opt("snapshot_url")?,
        snapshot_title: get_opt("snapshot_title")?,
        snapshot_head_sha: get_opt("snapshot_head_sha")?,
        snapshot_state: get_opt("snapshot_state")?,
        snapshot_is_draft: r
            .try_get::<Option<bool>, _>("snapshot_is_draft")
            .map_err(err)?,
        snapshot_mergeable: r
            .try_get::<Option<bool>, _>("snapshot_mergeable")
            .map_err(err)?,
    })
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the insert fails for any reason other than the unique-index rejection (which returns `Ok(false)`).
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
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation) =>
            {
                Ok(false)
            }
            Err(e) => Err(intent_core::Error::Internal(format!(
                "insert pr monitor failed: {e}"
            ))),
        }
    }

    /// Get a PR monitor by id; `NotFound` when absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the PR monitor does not exist; `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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

    /// List a workspace's ACTIVE monitors plus only its most recently updated
    /// COMPLETED monitor (`LIMIT 1`), oldest first — the displayStatus
    /// derivation read (active rows feed the open-PR signals, the latest
    /// completed row the merged signal, matching linked-PR "latest" step-6
    /// semantics). Completed rows are retained indefinitely, so the bound
    /// keeps this hot-path read O(active monitors) instead of O(all monitor
    /// history in the workspace); cancelled rows are excluded entirely.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_display_status_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? AND state = 'active' \
             UNION ALL \
             SELECT {COLUMNS} FROM (SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? \
             AND state = 'completed' ORDER BY updated_at DESC LIMIT 1) \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list display-status pr monitors by workspace failed: {e}"
                ))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Every `active` monitor across all workspaces, oldest first — the poll
    /// loop's per-tick read and the boot rehydration read.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    /// workspaces, oldest first, as narrow [`PrMonitorListEntry`] projections
    /// — the single bulk read backing the `workspace.list` /
    /// `workspace.subscribe` seq-0 PR merge. Completed rows are retained so
    /// merged PRs stay visible; cancelled rows are excluded (they are removed
    /// from the UI), matching the services-level per-workspace view
    /// ([`Services::pr_monitors_for_workspace`]). Unless `include_archived`,
    /// rows owned by archived workspaces are filtered in SQL so cost tracks
    /// the workspaces the list call actually returns.
    ///
    /// The blob columns are never selected: the few `last_snapshot` scalars
    /// the merge consumes are `json_extract`ed in SQL (guarded by
    /// `json_valid` so a malformed blob degrades to NULL scalars instead of
    /// failing the query), and `baseline_snapshot` / `pending_changes` are
    /// not touched at all — this read grows with monitor history, and
    /// hydrating full snapshot JSON per row put it at 1.26s for 440 rows on
    /// one of the hottest RPCs (intent-hq/monorepo#3878).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_non_cancelled_pr_monitor_list_entries(
        &self,
        include_archived: bool,
    ) -> Result<Vec<PrMonitorListEntry>> {
        let archived_filter = if include_archived {
            ""
        } else {
            " AND workspace_id IN (SELECT id FROM workspace WHERE archived = 0)"
        };
        let sql = format!(
            "SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
             updated_at, \
             json_extract(snapshot, '$.url') AS snapshot_url, \
             json_extract(snapshot, '$.title') AS snapshot_title, \
             json_extract(snapshot, '$.headSha') AS snapshot_head_sha, \
             json_extract(snapshot, '$.requirements.state') AS snapshot_state, \
             json_extract(snapshot, '$.requirements.isDraft') AS snapshot_is_draft, \
             json_extract(snapshot, '$.requirements.mergeable') AS snapshot_mergeable \
             FROM (SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
             updated_at, \
             CASE WHEN json_valid(last_snapshot) THEN last_snapshot END AS snapshot \
             FROM pr_monitor WHERE state != 'cancelled'{archived_filter}) \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("load pr monitor list entries failed: {e}"))
            })?;
        rows.iter().map(list_entry_from_row).collect()
    }

    /// Set a monitor's lifecycle state. Every legal transition starts from
    /// `active`, so the update is guarded on it; returns `false` when the row
    /// is absent or already terminal (a concurrent cancel/complete won) so
    /// the caller can skip its side effects instead of resurrecting the row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
            context_links: None,
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
            retired_at: None,
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

    /// The displayStatus derivation read returns every ACTIVE row plus only
    /// the most recently updated COMPLETED row (`LIMIT 1`) — never older
    /// completed rows (retained indefinitely) and never cancelled rows —
    /// so the hot-path read stays bounded.
    #[tokio::test]
    async fn display_status_read_bounds_completed_rows_to_latest() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let mk = |pr_number: i64, state: PrMonitorState, created: &str, updated: &str| {
            let mut m = test_monitor(&ws_id, &agent_id, created);
            m.pr_number = pr_number;
            m.state = state;
            m.updated_at = updated.to_string();
            m
        };
        let active_a = mk(
            1,
            PrMonitorState::Active,
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        let active_b = mk(
            2,
            PrMonitorState::Active,
            "2026-01-02T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );
        let completed_old = mk(
            3,
            PrMonitorState::Completed,
            "2026-01-03T00:00:00Z",
            "2026-01-03T00:00:00Z",
        );
        let completed_latest = mk(
            4,
            PrMonitorState::Completed,
            "2026-01-04T00:00:00Z",
            "2026-01-05T00:00:00Z",
        );
        let cancelled = mk(
            5,
            PrMonitorState::Cancelled,
            "2026-01-06T00:00:00Z",
            "2026-01-06T00:00:00Z",
        );
        for m in [
            &active_a,
            &active_b,
            &completed_old,
            &completed_latest,
            &cancelled,
        ] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }

        let rows = store
            .list_display_status_pr_monitors_by_workspace(&ws_id)
            .await
            .expect("list");
        let ids: Vec<&str> = rows.iter().map(|m| m.monitor_id.0.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                active_a.monitor_id.0.as_str(),
                active_b.monitor_id.0.as_str(),
                completed_latest.monitor_id.0.as_str(),
            ],
            "all active rows + only the latest completed row, oldest first"
        );
    }

    /// The `workspace.list` bulk read (intent-hq/monorepo#3878) returns
    /// [`PrMonitorListEntry`] projections: the snapshot scalars the list
    /// decoration consumes arrive `json_extract`ed in SQL, and the blob
    /// columns (`last_snapshot`, `baseline_snapshot`, `pending_changes`) are
    /// never returned or deserialized — the entry type carries no fields for
    /// them, so the shape is enforced at compile time. A missing or malformed
    /// `last_snapshot` degrades to NULL scalars instead of failing the query,
    /// cancelled rows are excluded, and archived-workspace rows are filtered
    /// unless `include_archived`.
    #[tokio::test]
    async fn list_entries_project_snapshot_scalars_without_blobs() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let snapshot = serde_json::json!({
            "title": "Monitored PR",
            "url": "https://github.com/o/r/pull/1",
            "headSha": "abc123",
            "conversationCount": 0,
            "reviewCommentCount": 0,
            "requirements": {
                "state": "merged",
                "isDraft": false,
                "hasConflicts": false,
                "isBehind": false,
                "mergeable": true,
                "checks": {
                    "total": 0, "passed": 0, "failed": 0, "pending": 0,
                    "items": [], "failingRequired": [], "pendingRequired": [],
                    "requiredKnown": true
                },
                "approvals": { "decision": "none", "have": 0, "changesRequested": 0 },
                "threads": { "unresolved": 0 },
                "rulesKnown": false
            }
        })
        .to_string();
        let mk = |pr_number: i64, state: PrMonitorState, snap: Option<String>, created: &str| {
            let mut m = test_monitor(&ws_id, &agent_id, created);
            m.pr_number = pr_number;
            m.state = state;
            m.last_snapshot = snap;
            m.baseline_snapshot = Some(r#"{"big":"blob"}"#.to_string());
            m.pending_changes = vec!["mergeable: true → false".to_string()];
            m
        };
        for m in [
            mk(
                1,
                PrMonitorState::Active,
                Some(snapshot),
                "2026-01-01T00:00:00Z",
            ),
            mk(2, PrMonitorState::Completed, None, "2026-01-02T00:00:00Z"),
            mk(
                3,
                PrMonitorState::Active,
                Some("{not json".to_string()),
                "2026-01-03T00:00:00Z",
            ),
            mk(4, PrMonitorState::Cancelled, None, "2026-01-04T00:00:00Z"),
        ] {
            assert!(store.insert_pr_monitor(&m).await.expect("insert"));
        }
        // A monitor in an archived workspace: excluded unless include_archived.
        let ts = now_iso();
        let archived_ws = WorkspaceId("ws-pr-monitor-archived".to_string());
        let mut w = test_workspace(&archived_ws, &ts);
        w.archived = true;
        w.status = WorkspaceStatus::Archived;
        store.insert_workspace(&w).await.expect("archived ws");
        let mut m = test_monitor(&archived_ws, &agent_id, "2026-01-05T00:00:00Z");
        m.pr_number = 5;
        assert!(store.insert_pr_monitor(&m).await.expect("insert archived"));

        let entries = store
            .load_non_cancelled_pr_monitor_list_entries(false)
            .await
            .expect("list entries");
        let numbers: Vec<i64> = entries.iter().map(|e| e.pr_number).collect();
        assert_eq!(
            numbers,
            vec![1, 2, 3],
            "cancelled + archived-workspace rows excluded, oldest first"
        );

        // Snapshot-backed row: scalars extracted from the JSON blob in SQL.
        let with_snap = &entries[0];
        assert_eq!(with_snap.workspace_id, ws_id);
        assert_eq!(with_snap.state, PrMonitorState::Active);
        assert_eq!(
            with_snap.snapshot_url.as_deref(),
            Some("https://github.com/o/r/pull/1")
        );
        assert_eq!(with_snap.snapshot_title.as_deref(), Some("Monitored PR"));
        assert_eq!(with_snap.snapshot_head_sha.as_deref(), Some("abc123"));
        assert_eq!(with_snap.snapshot_state.as_deref(), Some("merged"));
        assert_eq!(with_snap.snapshot_is_draft, Some(false));
        assert_eq!(with_snap.snapshot_mergeable, Some(true));

        // Snapshotless and malformed-snapshot rows read as NULL scalars
        // (mirroring the tolerant serde parse this projection replaced).
        for e in [&entries[1], &entries[2]] {
            assert_eq!(e.snapshot_url, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_title, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_head_sha, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_state, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_is_draft, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_mergeable, None, "pr {}", e.pr_number);
        }

        let all = store
            .load_non_cancelled_pr_monitor_list_entries(true)
            .await
            .expect("list entries incl. archived");
        let numbers: Vec<i64> = all.iter().map(|e| e.pr_number).collect();
        assert_eq!(
            numbers,
            vec![1, 2, 3, 5],
            "include_archived adds the archived workspace's row; cancelled stays excluded"
        );
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
