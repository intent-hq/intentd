//! Workspace repository: insert + list, mapping rows ↔ [`Workspace`] (§9.2).

use intent_core::{
    now_iso, AgentId, CheckoutMode, ClientId, ContextLink, Error, PullRequestInfo, Result,
    SetupScript, TokenUsage, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::agent_repo::{
    delete_in_bounded_batches, fetch_agent_usage_rows, DELETE_CASCADE_BATCH,
    UNREAD_TOP_LEVEL_SESSION_INDEX, UNREAD_TOP_LEVEL_SESSION_PREDICATE,
};
use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, AgentUsageRow, Store};

const WORKSPACE_COLUMNS: &str = "id, title, branch, base_ref, base_commit_sha, status, \
    status_message, status_image_asset_id, attention, path, repository_path, repository_owner, \
    repository_name, worktree_path, scope, skip_worktree, is_remote, default_model, pr_number, \
    pr_url, pr_status, active_pull_request, pull_requests, context_links, archived, archived_at, \
    tags, created_at, updated_at, last_activity, token_usage, setup_script, checkout_mode, \
    browser_client_id";

/// SQL behind [`Store::clear_workspace_unread_if_all_seen`], extracted so the
/// monorepo#4190 plan-shape guard runs `EXPLAIN` on the exact production
/// statement (see `SESSION_MESSAGE_STATS_SQL` for the precedent).
pub(crate) fn clear_workspace_unread_if_all_seen_sql() -> String {
    format!(
        "UPDATE workspace SET attention='none' \
         WHERE id = ? AND attention = 'unread' \
           AND NOT EXISTS(\
               SELECT 1 FROM agent_session INDEXED BY {UNREAD_TOP_LEVEL_SESSION_INDEX} \
               WHERE workspace_id = workspace.id \
                 AND {UNREAD_TOP_LEVEL_SESSION_PREDICATE}\
           )"
    )
}

impl Store {
    /// Insert a workspace row. `activity` is derived and never persisted (§9.9).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if encoding workspace fields or the insert fails.
    pub async fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        self.insert_workspace_with_auto_commit(ws, None).await
    }

    /// Insert a workspace row with the per-workspace `auto_commit_enabled`
    /// override seeded in the same INSERT (mirror-at-creation, spec Diagnosis
    /// §3b). Atomic: the row can never exist without its seed, so a created
    /// workspace never silently degrades to global-tracking semantics.
    /// `None` leaves the column NULL (resolves against the global at read
    /// time).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_workspace_with_auto_commit(
        &self,
        ws: &Workspace,
        auto_commit: Option<bool>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO workspace ({WORKSPACE_COLUMNS}, auto_commit_enabled) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&ws.id.0)
            .bind(&ws.title)
            .bind(&ws.branch)
            .bind(&ws.base_ref)
            .bind(&ws.base_commit_sha)
            .bind(enum_to_db(&ws.status)?)
            .bind(&ws.status_message)
            .bind(&ws.status_image_asset_id)
            .bind(enum_to_db(&ws.attention)?)
            .bind(&ws.path)
            .bind(&ws.repository_path)
            .bind(&ws.repository_owner)
            .bind(&ws.repository_name)
            .bind(&ws.worktree_path)
            .bind(&ws.scope)
            .bind(i64::from(ws.skip_worktree))
            .bind(i64::from(ws.is_remote))
            .bind(&ws.default_model)
            .bind(ws.pr_number.map(u64::cast_signed))
            .bind(&ws.pr_url)
            .bind(pr_status_to_db(ws)?)
            .bind(active_pr_to_db(ws)?)
            .bind(pull_requests_to_db(ws)?)
            .bind(context_links_to_db(ws)?)
            .bind(i64::from(ws.archived))
            .bind(&ws.archived_at)
            .bind(tags_to_db(&ws.tags)?)
            .bind(&ws.created_at)
            .bind(&ws.updated_at)
            .bind(&ws.last_activity)
            .bind(token_usage_to_db(ws)?)
            .bind(setup_script_to_db(ws)?)
            .bind(checkout_mode_to_db(ws)?)
            .bind(ws.browser_client_id.as_ref().map(|c| c.0.clone()))
            .bind(auto_commit.map(i64::from))
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert workspace failed: {e}")))?;
        Ok(())
    }

    /// Fetch a single workspace by id, or `NotFound`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn get_workspace(&self, id: &WorkspaceId) -> Result<Workspace> {
        let sql = format!("SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace failed: {e}")))?;
        match row {
            Some(r) => map_workspace_row(&r),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// Update an existing workspace, preserving `branch`, `id`, the
    /// guarded `last_activity` and archive lifecycle columns, or return
    /// `NotFound`. Returns the stored branch.
    /// `activity` is derived and never persisted (§9.9).
    ///
    /// Explicit branch changes use [`Self::update_workspace_with_branch`].
    /// `last_activity` is one exception to the full-row replace
    /// (monorepo#1585): it goes through the same monotonic guard as
    /// [`Self::bump_workspace_last_activity`] — the candidate writes only when
    /// it parses AND the stored value is NULL, unparseable, or strictly older.
    /// Otherwise the stored column holds, so a get → mutate → write flow whose
    /// read predated a concurrent bump can never silently revert it (the
    /// `attention` clobber shape fixed by #1481).
    ///
    /// The archive lifecycle is another exception: `archived` /
    /// `archived_at` are NEVER written here, and `status` holds whenever the
    /// row is archived or the candidate is `Archived`. Those columns move
    /// only through the scoped, fenced flips
    /// ([`Self::archive_workspace_detaching_guests`] /
    /// [`Self::unarchive_workspace_if_archived`]), so a full-row write from a
    /// snapshot read before a concurrent archive can never resurrect the
    /// workspace behind the archive's guest sweep (the `workspace.archive`
    /// fence relies on this — see `ArchiveFence` in `intent-services`).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_workspace(&self, ws: &Workspace) -> Result<String> {
        self.update_workspace_with_branch(ws, None).await
    }

    /// Update workspace fields and optionally apply an explicit branch edit.
    /// Returns the stored branch so responses cannot echo a stale snapshot.
    ///
    /// # Errors
    /// Returns `Error::NotFound` for a missing workspace or an error on database failure.
    pub async fn update_workspace_with_branch(
        &self,
        ws: &Workspace,
        branch: Option<&str>,
    ) -> Result<String> {
        let status = enum_to_db(&ws.status)?;
        let row = sqlx::query(
            "UPDATE workspace SET title=?, branch=COALESCE(?, branch), base_ref=?, base_commit_sha=?, \
             status=CASE WHEN archived = 1 OR ? = ? THEN status ELSE ? END, \
             status_message=?, status_image_asset_id=?, attention=?, path=?, repository_path=?, \
             repository_owner=?, repository_name=?, worktree_path=?, scope=?, skip_worktree=?, \
             is_remote=?, default_model=?, pr_number=?, pr_url=?, pr_status=?, \
             active_pull_request=?, pull_requests=?, context_links=?, \
             tags=?, created_at=?, updated_at=?, \
             last_activity=CASE WHEN julianday(?) IS NOT NULL \
               AND (last_activity IS NULL OR julianday(last_activity) IS NULL \
               OR julianday(last_activity) < julianday(?)) THEN ? ELSE last_activity END, \
             token_usage=?, setup_script=?, checkout_mode=? WHERE id=? RETURNING branch",
        )
        .bind(&ws.title)
        .bind(branch)
        .bind(&ws.base_ref)
        .bind(&ws.base_commit_sha)
        .bind(&status)
        .bind(enum_to_db(&WorkspaceStatus::Archived)?)
        .bind(&status)
        .bind(&ws.status_message)
        .bind(&ws.status_image_asset_id)
        .bind(enum_to_db(&ws.attention)?)
        .bind(&ws.path)
        .bind(&ws.repository_path)
        .bind(&ws.repository_owner)
        .bind(&ws.repository_name)
        .bind(&ws.worktree_path)
        .bind(&ws.scope)
        .bind(i64::from(ws.skip_worktree))
        .bind(i64::from(ws.is_remote))
        .bind(&ws.default_model)
        .bind(ws.pr_number.map(u64::cast_signed))
        .bind(&ws.pr_url)
        .bind(pr_status_to_db(ws)?)
        .bind(active_pr_to_db(ws)?)
        .bind(pull_requests_to_db(ws)?)
        .bind(context_links_to_db(ws)?)
        .bind(tags_to_db(&ws.tags)?)
        .bind(&ws.created_at)
        .bind(&ws.updated_at)
        .bind(&ws.last_activity)
        .bind(&ws.last_activity)
        .bind(&ws.last_activity)
        .bind(token_usage_to_db(ws)?)
        .bind(setup_script_to_db(ws)?)
        .bind(checkout_mode_to_db(ws)?)
        .bind(&ws.id.0)
        .fetch_optional(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace failed: {e}")))?;
        match row {
            Some(row) => col(&row, "branch"),
            None => Err(Error::NotFound(format!("workspace {}", ws.id))),
        }
    }

    /// Reconcile an observed branch without overwriting concurrent workspace edits.
    /// A renamed or switched branch is no longer eligible for automatic deletion.
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    pub async fn reconcile_workspace_branch(
        &self,
        expected: &Workspace,
        branch: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE workspace SET branch = ?, branch_auto_generated = 0 \
             WHERE id = ? AND branch = ? AND branch <> ? \
             AND worktree_path IS ? AND repository_path IS ? AND is_remote = 0",
        )
        .bind(branch)
        .bind(&expected.id.0)
        .bind(&expected.branch)
        .bind(branch)
        .bind(&expected.worktree_path)
        .bind(&expected.repository_path)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("reconcile workspace branch failed: {e}")))?;
        Ok(result.rows_affected() != 0)
    }

    /// Scoped PR-linkage write: set ONLY the PR columns (`pr_number`,
    /// `pr_url`, `pr_status`, `active_pull_request`, `pull_requests`) plus
    /// `updated_at` — never a full-row replace, so a PR refresh whose
    /// workspace read predates a concurrent mutation (archive, title edit,
    /// relink) can never clobber the other columns (same scoped-update
    /// discipline as [`Self::set_workspace_attention`] /
    /// [`Self::update_workspace_token_usage`]). The PR columns themselves are
    /// last-writer-wins by design — refreshes are idempotent against the
    /// forge and the next sweep converges. `NotFound` when the workspace
    /// does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_workspace_pr_linkage(&self, ws: &Workspace) -> Result<()> {
        let res = pr_linkage_update(ws)?
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("update workspace pr linkage failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {}", ws.id)));
        }
        Ok(())
    }

    /// [`Self::update_workspace_pr_linkage`] rebased on the row at write
    /// time (intent-hq/intent#5654): inside ONE `BEGIN IMMEDIATE` write-pool
    /// transaction, read the stored `pull_requests`, hand it to the caller's
    /// synchronous `rebase` closure together with the entity about to be
    /// written, then perform the same scoped PR-columns `UPDATE` from the
    /// (possibly amended) entity. The REST-only PR refreshes build their
    /// list from a row read that predates the forge round trip, so a
    /// signal-bearing fold landing in between would otherwise be clobbered;
    /// the closure (intent-services owns the merge rule) re-derives the
    /// `is_in_merge_queue` carry against the current row instead. Same
    /// envelope and layering as [`Self::update_workspace_token_usage`];
    /// a malformed stored list decodes to `None` (the closure then keeps
    /// the entity's list). `NotFound` when the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_workspace_pr_linkage_rebased<F>(
        &self,
        ws: &mut Workspace,
        rebase: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut Workspace, Option<Vec<PullRequestInfo>>),
    {
        let mut conn = self.write_pool().acquire().await.map_err(|e| {
            Error::Internal(format!("update workspace pr linkage acquire failed: {e}"))
        })?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                Error::Internal(format!("update workspace pr linkage begin failed: {e}"))
            })?;

        let body_result = async {
            let row = sqlx::query("SELECT pull_requests FROM workspace WHERE id = ?")
                .bind(&ws.id.0)
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| {
                    Error::Internal(format!("update workspace pr linkage read failed: {e}"))
                })?;
            let Some(row) = row else {
                return Err(Error::NotFound(format!("workspace {}", ws.id)));
            };
            let persisted = row
                .get::<Option<String>, _>("pull_requests")
                .and_then(|s| serde_json::from_str::<Vec<PullRequestInfo>>(&s).ok());
            rebase(ws, persisted);
            let res = pr_linkage_update(ws)?
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("update workspace pr linkage failed: {e}")))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!("workspace {}", ws.id)));
            }
            Ok(())
        }
        .await;

        crate::commit_with_rollback_guard(
            conn,
            body_result,
            "update workspace pr linkage commit failed",
        )
        .await
    }

    /// Project onto a workspace's persisted PR snapshots atomically
    /// (intent-hq/intent#5654, the cache-hit fold): inside ONE
    /// `BEGIN IMMEDIATE` write-pool transaction, read the stored
    /// `pull_requests` and `active_pull_request`, hand both to the caller's
    /// synchronous `project` closure, and — when it returns `true` — write
    /// back ONLY those two columns plus `updated_at`. The closure never
    /// sees a pre-read entity, so a REST refresh that committed between
    /// the caller's row lookup and this write is what gets projected onto,
    /// never rolled back; the linked scalars (`pr_number`, `pr_url`,
    /// `pr_status`) are untouched. Returns the written pair on a committed
    /// write, `None` when the closure declined. `NotFound` when the
    /// workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails or a stored snapshot is malformed.
    pub async fn project_workspace_pr_snapshots<F>(
        &self,
        id: &WorkspaceId,
        updated_at: &str,
        project: F,
    ) -> Result<Option<(Option<Vec<PullRequestInfo>>, Option<PullRequestInfo>)>>
    where
        F: FnOnce(&mut Option<Vec<PullRequestInfo>>, &mut Option<PullRequestInfo>) -> bool,
    {
        let mut conn = self.write_pool().acquire().await.map_err(|e| {
            Error::Internal(format!(
                "project workspace pr snapshots acquire failed: {e}"
            ))
        })?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                Error::Internal(format!("project workspace pr snapshots begin failed: {e}"))
            })?;

        let body_result = async {
            let row = sqlx::query(
                "SELECT pull_requests, active_pull_request FROM workspace WHERE id = ?",
            )
            .bind(&id.0)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| {
                Error::Internal(format!("project workspace pr snapshots read failed: {e}"))
            })?;
            let Some(row) = row else {
                return Err(Error::NotFound(format!("workspace {id}")));
            };
            let mut pool = pull_requests_from_db(row.get::<Option<String>, _>("pull_requests"))?;
            let mut active =
                active_pr_from_db(row.get::<Option<String>, _>("active_pull_request"))?;
            if !project(&mut pool, &mut active) {
                return Ok(None);
            }
            let pool_json = pool
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("encode pull_requests failed: {e}")))?;
            let active_json = active
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("encode active_pull_request failed: {e}")))?;
            let res = sqlx::query(
                "UPDATE workspace SET pull_requests=?, active_pull_request=?, updated_at=? \
                 WHERE id=?",
            )
            .bind(pool_json)
            .bind(active_json)
            .bind(updated_at)
            .bind(&id.0)
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("project workspace pr snapshots failed: {e}")))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!("workspace {id}")));
            }
            Ok(Some((pool, active)))
        }
        .await;

        crate::commit_with_rollback_guard(
            conn,
            body_result,
            "project workspace pr snapshots commit failed",
        )
        .await
    }

    /// Recompute-and-store a workspace's `token_usage` snapshot atomically
    /// (§5.23, monorepo#738): inside ONE write-pool transaction, read the
    /// per-session usage rows and the stored workspace `token_usage`, invoke
    /// the caller's synchronous `compute` closure with both, and — when it
    /// returns `Some(new_usage)` — perform a scoped
    /// `UPDATE workspace SET token_usage=?, updated_at=?` (never a full-row
    /// replace, so a concurrent title/status update is never clobbered).
    /// Returns the written [`TokenUsage`] on a committed write, `None` when
    /// the closure declined. `NotFound` if the workspace row is absent.
    /// Layering: aggregation stays in intent-services via the closure; the
    /// store only supplies the transactional read→write envelope.
    ///
    /// Uses raw `BEGIN IMMEDIATE` (same pattern as `insert_events`):
    /// IMMEDIATE mode acquires the exclusive write lock upfront, avoiding the
    /// DEFERRED-mode lock-upgrade race (read → write inside one transaction)
    /// that intermittently fails with `SQLITE_BUSY` (code 5). With
    /// `max_connections=1` on the write pool, concurrent recomputes serialize
    /// at `pool.acquire()` instead.
    ///
    /// Trade-off: the in-transaction row read (`fetch_agent_usage_rows`) reads
    /// `agent_message` for sessions still on the per-message fallback (no
    /// snapshot/baseline token report), and that work happens while holding
    /// the daemon's sole write connection and the `SQLite` write lock. The
    /// report-backed skip keeps the common case cheap, and the fallback read
    /// projects each message's usage object in SQL and filters to
    /// usage-bearing rows off a partial index instead of materializing message
    /// bodies (monorepo#1571) — so what a workspace of long-history fallback
    /// sessions pays for is its usage-bearing rows, not its transcript bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_workspace_token_usage<F>(
        &self,
        workspace_id: &WorkspaceId,
        compute: F,
    ) -> Result<Option<TokenUsage>>
    where
        F: FnOnce(&[AgentUsageRow], Option<&TokenUsage>) -> Option<TokenUsage>,
    {
        let mut conn =
            self.write_pool().acquire().await.map_err(|e| {
                Error::Internal(format!("token usage recompute acquire failed: {e}"))
            })?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("token usage recompute begin failed: {e}")))?;

        let body_result = async {
            let row = sqlx::query("SELECT token_usage FROM workspace WHERE id = ?")
                .bind(&workspace_id.0)
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("token usage recompute read failed: {e}")))?;
            let Some(row) = row else {
                return Err(Error::NotFound(format!("workspace {workspace_id}")));
            };
            // Best-effort decode: a malformed stored snapshot degrades to None so
            // the recompute writes a fresh one rather than failing.
            let current: Option<TokenUsage> = row
                .get::<Option<String>, _>("token_usage")
                .and_then(|s| serde_json::from_str(&s).ok());
            let usage_rows = fetch_agent_usage_rows(&mut conn, workspace_id).await?;
            let Some(new_usage) = compute(&usage_rows, current.as_ref()) else {
                return Ok(None);
            };
            let json = serde_json::to_string(&new_usage)
                .map_err(|e| Error::Internal(format!("encode token_usage failed: {e}")))?;
            let res = sqlx::query("UPDATE workspace SET token_usage=?, updated_at=? WHERE id=?")
                .bind(json)
                .bind(now_iso())
                .bind(&workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("token usage recompute write failed: {e}")))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!("workspace {workspace_id}")));
            }
            Ok(Some(new_usage))
        }
        .await;

        crate::commit_with_rollback_guard(conn, body_result, "token usage recompute commit failed")
            .await
    }

    /// Scoped, conditional attention write (monorepo#1481): set ONLY the
    /// `attention` column — plus `updated_at` when the caller intends an
    /// activity bump — guarded on the current value, so the write and the
    /// "did it change" decision are a single atomic statement and a
    /// concurrent mutation of any other column is never clobbered (same
    /// scoped-update discipline as [`Self::update_workspace_token_usage`]
    /// and [`Self::set_workspace_branch_auto_generated`]).
    ///
    /// `expected = Some(from)` writes only when the current attention equals
    /// `from` (markSeen's clear-only-when-unread; must differ from
    /// `attention` — debug-asserted — or the write degenerates to a
    /// same-value rewrite reported as a change); `None` writes whenever the
    /// current attention differs from `attention`. Returns whether a row was
    /// written (`true` ⇒ the value actually changed); `NotFound` when the
    /// workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when the workspace does not exist; `Error::Internal` if the update or presence check fails.
    pub async fn set_workspace_attention(
        &self,
        id: &WorkspaceId,
        attention: WorkspaceAttention,
        updated_at: Option<&str>,
        expected: Option<WorkspaceAttention>,
    ) -> Result<bool> {
        debug_assert!(
            expected.as_ref() != Some(&attention),
            "expected == attention degenerates to a same-value rewrite that \
             reports `changed = true`"
        );
        let target = enum_to_db(&attention)?;
        let guard = match &expected {
            Some(from) => enum_to_db(from)?,
            None => target.clone(),
        };
        let sql = match (updated_at.is_some(), expected.is_some()) {
            (true, true) => {
                "UPDATE workspace SET attention=?, updated_at=? WHERE id=? AND attention = ?"
            }
            (true, false) => {
                "UPDATE workspace SET attention=?, updated_at=? WHERE id=? AND attention <> ?"
            }
            (false, true) => "UPDATE workspace SET attention=? WHERE id=? AND attention = ?",
            (false, false) => "UPDATE workspace SET attention=? WHERE id=? AND attention <> ?",
        };
        let mut query = sqlx::query(sql).bind(&target);
        if let Some(ts) = updated_at {
            query = query.bind(ts);
        }
        let res = query
            .bind(&id.0)
            .bind(&guard)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set attention failed: {e}")))?;
        if res.rows_affected() > 0 {
            return Ok(true);
        }
        // Zero rows: either the guard declined (no change) or the workspace
        // is missing — distinguish so callers keep NotFound semantics.
        let row = sqlx::query("SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?) AS present")
            .bind(&id.0)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("set attention presence check failed: {e}")))?;
        if col::<i64>(&row, "present")? == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(false)
    }

    /// Atomic settle-clear of the stored `unread` flag (§5.1): set
    /// `attention = none` ONLY when the current value is `unread` AND the
    /// workspace has no unread top-level session — the derivation re-checked
    /// INSIDE the same guarded UPDATE (shared predicate with
    /// [`Store::workspace_has_unread_top_level_session`]), so a message that
    /// lands between a caller's probe and this write flips the NOT EXISTS
    /// and the clear declines instead of retiring a freshly-raised unread.
    /// Scoped to the attention column, `updated_at` untouched (acknowledging
    /// is not "activity", monorepo#1466). Returns whether a row was written
    /// (`true` ⇒ stored `unread` was cleared); a missing workspace reads as
    /// `false` — settle callers are best-effort and never need `NotFound`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_workspace_unread_if_all_seen(&self, id: &WorkspaceId) -> Result<bool> {
        let sql = clear_workspace_unread_if_all_seen_sql();
        let res = sqlx::query(&sql)
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("settle unread clear failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Scoped, conditional unarchive flip: set `status`/`archived`/
    /// `archived_at` (plus `updated_at`) back to Active ONLY when the row is
    /// currently archived, so the write and the "did this call flip it"
    /// decision are a single atomic statement — two concurrent unarchivers
    /// (or an unarchive racing a turn-start auto-unarchive) can both read
    /// `archived: true`, but exactly one write affects a row. Same
    /// scoped-update discipline as [`Self::set_workspace_attention`]. Returns
    /// whether a row was written (`true` ⇒ this call performed the flip);
    /// `NotFound` when the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn unarchive_workspace_if_archived(
        &self,
        id: &WorkspaceId,
        updated_at: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workspace SET status=?, archived=0, archived_at=NULL, updated_at=? \
             WHERE id=? AND archived=1",
        )
        .bind(enum_to_db(&WorkspaceStatus::Active)?)
        .bind(updated_at)
        .bind(&id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("conditional unarchive failed: {e}")))?;
        if res.rows_affected() > 0 {
            return Ok(true);
        }
        // Zero rows: either the row is already active (no flip) or the
        // workspace is missing — distinguish so callers keep NotFound
        // semantics.
        let row = sqlx::query("SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?) AS present")
            .bind(&id.0)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("conditional unarchive presence check failed: {e}"))
            })?;
        if col::<i64>(&row, "present")? == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(false)
    }

    /// Scoped, monotonic `last_activity` write (monorepo#1580): set ONLY the
    /// `last_activity` column — never `updated_at`, never a full-row replace —
    /// and only when the supplied timestamp is strictly newer than the stored
    /// one (or the column is NULL / unparseable). Same scoped-update
    /// discipline as [`Self::set_workspace_attention`].
    ///
    /// Backs the debounced `lastActivity` derivation in intent-services so the
    /// persisted column tracks the derived value and cheap read paths
    /// (`list_workspaces_lite`, the `workspace.subscribe` seq-0 snapshot) serve
    /// a fresh timestamp after a restart.
    ///
    /// Comparison runs through `SQLite`'s `julianday()` rather than raw TEXT so
    /// timestamps of differing fractional-second precision order correctly
    /// (lexicographic `…:00Z` vs `…:00.5Z` compares backwards). A malformed
    /// `last_activity` parses to NULL and is treated as "older" (overwritten);
    /// a malformed input never writes. Returns whether a row was written;
    /// `NotFound` when the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn bump_workspace_last_activity(
        &self,
        id: &WorkspaceId,
        last_activity: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workspace SET last_activity=? WHERE id=? AND julianday(?) IS NOT NULL \
             AND (last_activity IS NULL OR julianday(last_activity) IS NULL \
             OR julianday(last_activity) < julianday(?))",
        )
        .bind(last_activity)
        .bind(&id.0)
        .bind(last_activity)
        .bind(last_activity)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("bump last_activity failed: {e}")))?;
        if res.rows_affected() > 0 {
            return Ok(true);
        }
        // Zero rows: either the monotonic guard declined (not newer) or the
        // workspace is missing — distinguish so callers keep NotFound semantics.
        let row = sqlx::query("SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?) AS present")
            .bind(&id.0)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("bump last_activity presence check failed: {e}"))
            })?;
        if col::<i64>(&row, "present")? == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(false)
    }

    /// Delete a workspace by id, or `NotFound`. Records a tombstone in
    /// `deleted_workspace_id` (same transaction as the row delete) so
    /// `workspace.create` never recycles the id for a later workspace (FE
    /// `recentlyDeletedWorkspaces` parity, persisted across restarts).
    /// Also removes the workspace's `draft` rows explicitly — `draft` has no
    /// workspace FK (opaque keys, PROTOCOL §5.16), so no cascade applies.
    /// The `browser_tab` rows and their process-local `displayed`
    /// overlay entries are removed together after each committed batch (see
    /// `browser_tab_repo`).
    ///
    /// Callers must stop the workspace's runtime writers before deletion.
    /// Histories and other growing children are swept in committed batches,
    /// releasing the writer between steps (intent-hq/intent#5337). Failure or
    /// cancellation can leave a live workspace with partially removed data;
    /// retry resumes cleanup. Only the final row delete and tombstone are
    /// atomic. Cleanup errors propagate without falling back to a cascade.
    ///
    /// The final transaction uses whole-transaction retry to eliminate
    /// `SQLITE_BUSY` (code 5) lock-upgrade failures under concurrent load (STAB-7).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn delete_workspace(&self, id: &WorkspaceId) -> Result<()> {
        // In particular, a missing workspace must not delete opaque draft
        // keys. The final transaction also checks existence for racing deletes.
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?)")
                .bind(&id.0)
                .fetch_one(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("delete workspace check failed: {e}")))?;
        if !exists {
            return Err(Error::NotFound(format!("workspace {id}")));
        }

        // IDs only: never hydrate sessions or their transcripts. Each session
        // uses the same bounded payload/message cleanup as agent.delete.
        while let Some(agent_id) = sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_session WHERE workspace_id = ? LIMIT 1",
        )
        .bind(&id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list workspace deletion agents failed: {e}")))?
        {
            self.delete_agent_session(id, &AgentId(agent_id)).await?;
            tokio::task::yield_now().await;
        }

        // A note's parent-clear trigger can otherwise update every child in
        // one statement. Clear the links first, then sweep its heavy children
        // before the notes themselves. All predicates remain workspace-scoped.
        delete_in_bounded_batches(
            self.write_pool(),
            "UPDATE note SET parent_id = NULL WHERE rowid IN \
             (SELECT rowid FROM note WHERE workspace_id = ? AND parent_id IS NOT NULL LIMIT ?)",
            &id.0,
            DELETE_CASCADE_BATCH,
        )
        .await?;
        // comment's index starts with note_id, not workspace_id. Join from
        // the workspace's notes to avoid rescanning unrelated comments for
        // every batch, and preserve rows with no note (no cascade before).
        delete_in_bounded_batches(
            self.write_pool(),
            "DELETE FROM comment WHERE rowid IN \
             (SELECT c.rowid FROM note n JOIN comment c \
              ON c.note_id = n.id AND c.workspace_id = n.workspace_id \
              WHERE n.workspace_id = ? LIMIT ?)",
            &id.0,
            DELETE_CASCADE_BATCH,
        )
        .await?;
        for table in [
            "note_version",
            "note_line_attribution",
            "note",
            "tracked_changes",
            "diffs",
            "delegation_group",
            "task_agent_link",
            "agent_metrics",
            "workspace_context_item",
            "workspace_git_root",
            "workspace_invite",
            "workspace_mcp_disabled_server",
            "draft",
        ] {
            let sql = format!(
                "DELETE FROM {table} WHERE rowid IN \
                 (SELECT rowid FROM {table} WHERE workspace_id = ? LIMIT ?)"
            );
            delete_in_bounded_batches(self.write_pool(), &sql, &id.0, DELETE_CASCADE_BATCH).await?;
        }

        // Browser rows need their ids after commit to evict the process-local
        // overlay. RETURNING completes the implicit transaction before the
        // successful fetch_all returns, keeping each batch and eviction paired.
        loop {
            let tab_ids: Vec<String> = sqlx::query_scalar(
                "DELETE FROM browser_tab WHERE rowid IN \
                 (SELECT rowid FROM browser_tab WHERE workspace_id = ? LIMIT ?) RETURNING tab_id",
            )
            .bind(&id.0)
            .bind(DELETE_CASCADE_BATCH)
            .fetch_all(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace browser tabs failed: {e}")))?;
            self.browser_tab_displayed
                .forget_all(tab_ids.iter().map(String::as_str));
            if i64::try_from(tab_ids.len()) != Ok(DELETE_CASCADE_BATCH) {
                break;
            }
            tokio::task::yield_now().await;
        }

        let pool = self.write_pool();
        let id = id.clone();

        let tab_ids = crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("delete workspace tx failed: {e}")))?;
            // A newly created session after the pre-sweep must not silently
            // reintroduce a workspace-wide history cascade. Let callers retry
            // after fencing the writer that raced with deletion.
            let new_session: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_session WHERE workspace_id = ?)",
            )
            .bind(&id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("check workspace deletion remainder failed: {e}"))
            })?;
            if new_session {
                return Err(Error::Internal(format!(
                    "workspace {id} gained an agent during deletion; retry cleanup"
                )));
            }
            let tab_ids = crate::browser_tab_repo::workspace_tab_ids(&mut tx, &id).await?;
            // Child-table cleanup first (defensive ordering); on the NotFound
            // early-return below the rollback undoes it.
            sqlx::query("DELETE FROM draft WHERE workspace_id = ?")
                .bind(&id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("delete workspace drafts failed: {e}")))?;
            let res = sqlx::query("DELETE FROM workspace WHERE id = ?")
                .bind(&id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("delete workspace failed: {e}")))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!("workspace {id}")));
            }
            sqlx::query(
                "INSERT OR REPLACE INTO deleted_workspace_id (id, deleted_at) VALUES (?, ?)",
            )
            .bind(&id.0)
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("record deleted workspace id failed: {e}")))?;
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("delete workspace commit failed: {e}")))?;
            Ok(tab_ids)
        })
        .await?;
        self.browser_tab_displayed
            .forget_all(tab_ids.iter().map(String::as_str));
        Ok(())
    }

    /// Whether a workspace id was ever used — a live row exists **or** a
    /// delete tombstone is recorded. `workspace.create` uses this to uniquify
    /// derived slug ids so a deleted workspace's id is never recycled.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn workspace_id_ever_used(&self, id: &WorkspaceId) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?) \
             OR EXISTS(SELECT 1 FROM deleted_workspace_id WHERE id = ?) AS used",
        )
        .bind(&id.0)
        .bind(&id.0)
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("workspace id lookup failed: {e}")))?;
        Ok(col::<i64>(&row, "used")? != 0)
    }

    /// Record whether the workspace's branch was auto-generated by the daemon
    /// at create time (vs supplied by the caller). Read back by the
    /// `workspace.delete` cleanup guard: only an auto-generated branch is ever
    /// deleted with the worktree (TS `removeGitWorktree` parity). Store-only —
    /// the flag never appears on the wire, so it lives outside [`Workspace`].
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn set_workspace_branch_auto_generated(
        &self,
        id: &WorkspaceId,
        auto_generated: bool,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE workspace SET branch_auto_generated = ? WHERE id = ?")
            .bind(i64::from(auto_generated))
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set branch_auto_generated failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(())
    }

    /// Whether the workspace's branch was auto-generated at create time.
    /// `NotFound` when the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn workspace_branch_auto_generated(&self, id: &WorkspaceId) -> Result<bool> {
        let row = sqlx::query("SELECT branch_auto_generated FROM workspace WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get branch_auto_generated failed: {e}")))?;
        match row {
            Some(r) => Ok(col::<i64>(&r, "branch_auto_generated")? != 0),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// Set the persisted per-workspace auto-commit override (spec Diagnosis
    /// §3b). Mirrored from the global `git.autoCommit` at create time and
    /// toggled via `workspace.setAutoCommit`. Store-only column — the value
    /// is surfaced through the dedicated getter RPC, not on [`Workspace`].
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn set_workspace_auto_commit(&self, id: &WorkspaceId, enabled: bool) -> Result<()> {
        let res = sqlx::query("UPDATE workspace SET auto_commit_enabled = ? WHERE id = ?")
            .bind(i64::from(enabled))
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set auto_commit_enabled failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(())
    }

    /// The persisted per-workspace auto-commit override. `Ok(None)` for
    /// pre-migration rows (NULL column) — the caller resolves NULL against
    /// the global `git.autoCommit` setting. `NotFound` when the workspace
    /// does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn workspace_auto_commit(&self, id: &WorkspaceId) -> Result<Option<bool>> {
        let row = sqlx::query("SELECT auto_commit_enabled FROM workspace WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get auto_commit_enabled failed: {e}")))?;
        match row {
            Some(r) => Ok(col::<Option<i64>>(&r, "auto_commit_enabled")?.map(|v| v != 0)),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// Scoped write of the per-workspace browser-client pin (REV-2,
    /// `workspace.setBrowserClient`): `None` clears it. This setter is the
    /// only writer of the column after insert — `update_workspace` never
    /// touches it (like `auto_commit_enabled`), so a stale `Workspace`
    /// snapshot passed to a general update cannot revert a concurrent pin.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn set_workspace_browser_client(
        &self,
        id: &WorkspaceId,
        client_id: Option<&ClientId>,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE workspace SET browser_client_id = ? WHERE id = ?")
            .bind(client_id.map(|c| c.0.as_str()))
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set browser_client_id failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(())
    }

    /// The persisted per-workspace browser-client pin; `Ok(None)` when
    /// unpinned. `NotFound` when the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn workspace_browser_client(&self, id: &WorkspaceId) -> Result<Option<ClientId>> {
        let row = sqlx::query("SELECT browser_client_id FROM workspace WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get browser_client_id failed: {e}")))?;
        match row {
            Some(r) => Ok(col::<Option<String>>(&r, "browser_client_id")?.map(ClientId)),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// List workspaces, filtering archived rows unless `include_archived`.
    /// The seeded virtual [`CHIEF_WORKSPACE_ID`] row is always excluded — Chief
    /// is synthesized on read by the service layer and never surfaces via
    /// `workspace.list` (TS `findAll` parity, `workspace.repository.ts`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspaces(&self, include_archived: bool) -> Result<Vec<Workspace>> {
        let sql = if include_archived {
            format!("SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id <> ? ORDER BY created_at")
        } else {
            format!(
                "SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id <> ? AND archived = 0 ORDER BY created_at"
            )
        };
        let rows = sqlx::query(&sql)
            .bind(CHIEF_WORKSPACE_ID)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspaces failed: {e}")))?;
        rows.iter().map(map_workspace_row).collect()
    }

    /// Live (non-archived, non-remote) workspaces referencing a PR by URL —
    /// linked via `pr_url` or carrying a `pull_requests` pool entry with that
    /// URL — oldest first. Backs the passive `github.pulls.get` fold: the
    /// match runs in SQL (`json_each` over the pool column) so only the
    /// referencing rows are decoded, and compares `COLLATE NOCASE` because
    /// forge slugs are case-insensitive while persisted URLs may carry a
    /// client-supplied casing. The seeded virtual [`CHIEF_WORKSPACE_ID`]
    /// row is excluded like [`Self::list_workspaces`].
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspaces_referencing_pr_url(&self, url: &str) -> Result<Vec<Workspace>> {
        let sql = format!(
            "SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id <> ? AND archived = 0 \
             AND is_remote = 0 AND (pr_url = ? COLLATE NOCASE OR (pull_requests IS NOT NULL \
             AND json_valid(pull_requests) AND EXISTS (\
             SELECT 1 FROM json_each(workspace.pull_requests) AS je \
             WHERE je.value ->> '$.url' = ? COLLATE NOCASE))) ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(CHIEF_WORKSPACE_ID)
            .bind(url)
            .bind(url)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("list workspaces referencing pr url failed: {e}"))
            })?;
        rows.iter().map(map_workspace_row).collect()
    }
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

/// Encode the optional `pr_status` enum to its `PascalCase` DB word, or `None`.
fn pr_status_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.pr_status.map(|s| enum_to_db(&s)).transpose()
}

/// Encode the optional `active_pull_request` snapshot to a JSON TEXT column.
fn active_pr_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.active_pull_request
        .as_ref()
        .map(|pr| {
            serde_json::to_string(pr)
                .map_err(|e| Error::Internal(format!("encode active_pull_request failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `active_pull_request` JSON TEXT column.
fn active_pr_from_db(s: Option<String>) -> Result<Option<PullRequestInfo>> {
    s.map(|json| {
        serde_json::from_str::<PullRequestInfo>(&json)
            .map_err(|e| Error::Internal(format!("decode active_pull_request failed: {e}")))
    })
    .transpose()
}

/// The scoped PR-columns `UPDATE` (PR columns + `updated_at`, never a
/// full-row replace) shared by [`Store::update_workspace_pr_linkage`] and
/// [`Store::update_workspace_pr_linkage_rebased`].
fn pr_linkage_update(
    ws: &Workspace,
) -> Result<sqlx::query::Query<'_, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'_>>> {
    Ok(sqlx::query(
        "UPDATE workspace SET pr_number=?, pr_url=?, pr_status=?, \
         active_pull_request=?, pull_requests=?, updated_at=? WHERE id=?",
    )
    .bind(ws.pr_number.map(u64::cast_signed))
    .bind(&ws.pr_url)
    .bind(pr_status_to_db(ws)?)
    .bind(active_pr_to_db(ws)?)
    .bind(pull_requests_to_db(ws)?)
    .bind(&ws.updated_at)
    .bind(&ws.id.0))
}

/// Encode the optional `pull_requests` snapshot list to a JSON TEXT column.
fn pull_requests_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.pull_requests
        .as_ref()
        .map(|prs| {
            serde_json::to_string(prs)
                .map_err(|e| Error::Internal(format!("encode pull_requests failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `pull_requests` JSON TEXT column.
fn pull_requests_from_db(s: Option<String>) -> Result<Option<Vec<PullRequestInfo>>> {
    s.map(|json| {
        serde_json::from_str::<Vec<PullRequestInfo>>(&json)
            .map_err(|e| Error::Internal(format!("decode pull_requests failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `context_links` list to a JSON TEXT column (§5.1).
fn context_links_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.context_links
        .as_ref()
        .map(|links| {
            serde_json::to_string(links)
                .map_err(|e| Error::Internal(format!("encode context_links failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `context_links` JSON TEXT column (§5.1).
fn context_links_from_db(s: Option<String>) -> Result<Option<Vec<ContextLink>>> {
    s.map(|json| {
        serde_json::from_str::<Vec<ContextLink>>(&json)
            .map_err(|e| Error::Internal(format!("decode context_links failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `token_usage` snapshot to a JSON TEXT column (§5.23).
fn token_usage_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.token_usage
        .as_ref()
        .map(|tu| {
            serde_json::to_string(tu)
                .map_err(|e| Error::Internal(format!("encode token_usage failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `token_usage` JSON TEXT column (§5.23).
fn token_usage_from_db(s: Option<String>) -> Result<Option<TokenUsage>> {
    s.map(|json| {
        serde_json::from_str::<TokenUsage>(&json)
            .map_err(|e| Error::Internal(format!("decode token_usage failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `setup_script` record to a JSON TEXT column (§5.25).
fn setup_script_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.setup_script
        .as_ref()
        .map(|s| {
            serde_json::to_string(s)
                .map_err(|e| Error::Internal(format!("encode setup_script failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `setup_script` JSON TEXT column (§5.25).
fn setup_script_from_db(s: Option<String>) -> Result<Option<SetupScript>> {
    s.map(|json| {
        serde_json::from_str::<SetupScript>(&json)
            .map_err(|e| Error::Internal(format!("decode setup_script failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `checkout_mode` enum to a TEXT column (§5.1).
fn checkout_mode_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.checkout_mode.as_ref().map(enum_to_db).transpose()
}

fn map_workspace_row(row: &SqliteRow) -> Result<Workspace> {
    let pr_number: Option<i64> = col(row, "pr_number")?;
    let pr_status = col::<Option<String>>(row, "pr_status")?
        .map(|s| enum_from_db::<intent_core::PullRequestStatus>(&s))
        .transpose()?;
    let active_pull_request =
        active_pr_from_db(col::<Option<String>>(row, "active_pull_request")?)?;
    let pull_requests = pull_requests_from_db(col::<Option<String>>(row, "pull_requests")?)?;
    let context_links = context_links_from_db(col::<Option<String>>(row, "context_links")?)?;
    let token_usage = token_usage_from_db(col::<Option<String>>(row, "token_usage")?)?;
    let setup_script = setup_script_from_db(col::<Option<String>>(row, "setup_script")?)?;
    let checkout_mode = col::<Option<String>>(row, "checkout_mode")?
        .map(|s| enum_from_db::<CheckoutMode>(&s))
        .transpose()?;
    Ok(Workspace {
        id: WorkspaceId(col(row, "id")?),
        title: col(row, "title")?,
        branch: col(row, "branch")?,
        base_ref: col(row, "base_ref")?,
        base_commit_sha: col(row, "base_commit_sha")?,
        status: enum_from_db::<WorkspaceStatus>(&col::<String>(row, "status")?)?,
        status_message: col(row, "status_message")?,
        status_image_asset_id: col(row, "status_image_asset_id")?,
        // Derived, read-only; never persisted (§9.9).
        activity: WorkspaceActivity::Idle,
        attention: enum_from_db::<WorkspaceAttention>(&col::<String>(row, "attention")?)?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
        last_activity: col(row, "last_activity")?,
        tags: tags_from_db(&col::<String>(row, "tags")?)?,
        path: col(row, "path")?,
        repository_path: col(row, "repository_path")?,
        repository_owner: col(row, "repository_owner")?,
        repository_name: col(row, "repository_name")?,
        worktree_path: col(row, "worktree_path")?,
        scope: col(row, "scope")?,
        skip_worktree: col::<i64>(row, "skip_worktree")? != 0,
        setup_script,
        is_remote: col::<i64>(row, "is_remote")? != 0,
        default_model: col(row, "default_model")?,
        pr_number: pr_number.map(i64::cast_unsigned),
        pr_url: col(row, "pr_url")?,
        pr_status,
        active_pull_request,
        pull_requests,
        context_links,
        archived: col::<i64>(row, "archived")? != 0,
        archived_at: col(row, "archived_at")?,
        // Card aggregates are computed on the workspace.list/get emit path
        // (intent-services), never persisted.
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        display_status: None,
        waiting: false,
        token_usage,
        // cow_supported is computed on the emit path (intent-services), never persisted.
        cow_supported: None,
        checkout_mode,
        browser_client_id: col::<Option<String>>(row, "browser_client_id")?.map(ClientId),
        // disk_usage is computed on the emit path (intent-services), never persisted.
        disk_usage: None,
        pending_delete_at: None,
        // pull_requests_total is set by the list-row slimming (intent-core), never persisted.
        pull_requests_total: None,
        membership: None,
    })
}
