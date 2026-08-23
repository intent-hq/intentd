//! Workspace repository: insert + list, mapping rows ↔ [`Workspace`] (§9.2).

use intent_core::{
    now_iso, CheckoutMode, Error, PullRequestInfo, Result, SandboxType, SetupScript, TokenUsage,
    Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    CHIEF_WORKSPACE_ID,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::agent_repo::fetch_agent_usage_rows;
use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, AgentUsageRow, Store};

const WORKSPACE_COLUMNS: &str = "id, title, branch, base_ref, base_commit_sha, status, \
    status_message, status_image_asset_id, attention, path, repository_path, repository_owner, \
    repository_name, worktree_path, scope, skip_worktree, is_remote, default_model, pr_number, \
    pr_url, pr_status, active_pull_request, pull_requests, archived, archived_at, tags, \
    created_at, updated_at, last_activity, token_usage, setup_script, checkout_mode, \
    execution_environment";

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
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
            .bind(i64::from(ws.archived))
            .bind(&ws.archived_at)
            .bind(tags_to_db(&ws.tags)?)
            .bind(&ws.created_at)
            .bind(&ws.updated_at)
            .bind(&ws.last_activity)
            .bind(token_usage_to_db(ws)?)
            .bind(setup_script_to_db(ws)?)
            .bind(checkout_mode_to_db(ws)?)
            .bind(execution_environment_to_db(ws)?)
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

    /// Update an existing workspace (full row replace, except `id` and the
    /// guarded `last_activity`, see below), or `NotFound`. `activity` is
    /// derived and never persisted (§9.9).
    ///
    /// `last_activity` is the one exception to the full-row replace
    /// (monorepo#1585): it goes through the same monotonic guard as
    /// [`Self::bump_workspace_last_activity`] — the candidate writes only when
    /// it parses AND the stored value is NULL, unparseable, or strictly older.
    /// Otherwise the stored column holds, so a get → mutate → write flow whose
    /// read predated a concurrent bump can never silently revert it (the
    /// `attention` clobber shape fixed by #1481).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn update_workspace(&self, ws: &Workspace) -> Result<()> {
        let res = sqlx::query(
            "UPDATE workspace SET title=?, branch=?, base_ref=?, base_commit_sha=?, status=?, \
             status_message=?, status_image_asset_id=?, attention=?, path=?, repository_path=?, \
             repository_owner=?, repository_name=?, worktree_path=?, scope=?, skip_worktree=?, \
             is_remote=?, default_model=?, pr_number=?, pr_url=?, pr_status=?, \
             active_pull_request=?, pull_requests=?, archived=?, archived_at=?, tags=?, \
             created_at=?, updated_at=?, \
             last_activity=CASE WHEN julianday(?) IS NOT NULL \
               AND (last_activity IS NULL OR julianday(last_activity) IS NULL \
               OR julianday(last_activity) < julianday(?)) THEN ? ELSE last_activity END, \
             token_usage=?, setup_script=?, checkout_mode=?, execution_environment=? WHERE id=?",
        )
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
        .bind(i64::from(ws.archived))
        .bind(&ws.archived_at)
        .bind(tags_to_db(&ws.tags)?)
        .bind(&ws.created_at)
        .bind(&ws.updated_at)
        .bind(&ws.last_activity)
        .bind(&ws.last_activity)
        .bind(&ws.last_activity)
        .bind(token_usage_to_db(ws)?)
        .bind(setup_script_to_db(ws)?)
        .bind(checkout_mode_to_db(ws)?)
        .bind(execution_environment_to_db(ws)?)
        .bind(&ws.id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {}", ws.id)));
        }
        Ok(())
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
        let res = sqlx::query(
            "UPDATE workspace SET pr_number=?, pr_url=?, pr_status=?, \
             active_pull_request=?, pull_requests=?, updated_at=? WHERE id=?",
        )
        .bind(ws.pr_number.map(u64::cast_signed))
        .bind(&ws.pr_url)
        .bind(pr_status_to_db(ws)?)
        .bind(active_pr_to_db(ws)?)
        .bind(pull_requests_to_db(ws)?)
        .bind(&ws.updated_at)
        .bind(&ws.id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace pr linkage failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {}", ws.id)));
        }
        Ok(())
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
    ///
    /// Uses whole-transaction retry to eliminate `SQLITE_BUSY` (code 5) failures
    /// during lock upgrade under concurrent load (STAB-7).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist; `Error::Internal` if the database operation fails.
    pub async fn delete_workspace(&self, id: &WorkspaceId) -> Result<()> {
        let pool = self.write_pool();
        let id = id.clone();

        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("delete workspace tx failed: {e}")))?;
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
            Ok(())
        })
        .await
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

/// Encode the optional `execution_environment` enum to a TEXT column (§5.1).
fn execution_environment_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.execution_environment
        .as_ref()
        .map(enum_to_db)
        .transpose()
}

fn map_workspace_row(row: &SqliteRow) -> Result<Workspace> {
    let pr_number: Option<i64> = col(row, "pr_number")?;
    let pr_status = col::<Option<String>>(row, "pr_status")?
        .map(|s| enum_from_db::<intent_core::PullRequestStatus>(&s))
        .transpose()?;
    let active_pull_request =
        active_pr_from_db(col::<Option<String>>(row, "active_pull_request")?)?;
    let pull_requests = pull_requests_from_db(col::<Option<String>>(row, "pull_requests")?)?;
    let token_usage = token_usage_from_db(col::<Option<String>>(row, "token_usage")?)?;
    let setup_script = setup_script_from_db(col::<Option<String>>(row, "setup_script")?)?;
    let checkout_mode = col::<Option<String>>(row, "checkout_mode")?
        .map(|s| enum_from_db::<CheckoutMode>(&s))
        .transpose()?;
    let execution_environment = col::<Option<String>>(row, "execution_environment")?
        .map(|s| enum_from_db::<SandboxType>(&s))
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
        execution_environment,
        // disk_usage is computed on the emit path (intent-services), never persisted.
        disk_usage: None,
        pending_delete_at: None,
    })
}
