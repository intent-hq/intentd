//! Tracked-changes repository: the per-file agent-change audit trail (§9.11,
//! §17.4). Written by the BE-internal file-tracking pipeline (track-change);
//! there is one row per agent per file per git stage (monorepo#957), upserted
//! in place as the file moves
//! `unstaged → staged → committed → pushed → pr → merged`. Raw content stays
//! lazy via the `old_blob_sha`/`new_blob_sha` columns — never inlined here.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::Store;

const TRACKED_CHANGE_COLUMNS: &str = "id, workspace_id, path, stage, status, agent_id, \
    session_id, turn, commit_hash, old_blob_sha, new_blob_sha, additions, deletions, \
    created_at, updated_at";

/// Input to [`Store::upsert_tracked_change`]: a tracked change without its id /
/// timestamps. The repository mints the id (on insert) and stamps `updated_at`.
#[derive(Debug, Clone)]
pub struct NewTrackedChange {
    pub workspace_id: WorkspaceId,
    pub path: String,
    pub stage: String,
    pub status: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub turn: Option<i64>,
    pub commit_hash: Option<String>,
    pub old_blob_sha: Option<String>,
    pub new_blob_sha: Option<String>,
    pub additions: i64,
    pub deletions: i64,
}

/// A persisted `tracked_changes` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedChangeRow {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub path: String,
    pub stage: String,
    pub status: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub turn: Option<i64>,
    pub commit_hash: Option<String>,
    pub old_blob_sha: Option<String>,
    pub new_blob_sha: Option<String>,
    pub additions: i64,
    pub deletions: i64,
    pub created_at: String,
    pub updated_at: String,
}

impl Store {
    /// Upsert a tracked change keyed by `(workspace_id, path, stage, agent_id)`
    /// (monorepo#957): update the calling agent's existing row in place
    /// (preserving its id + `created_at`) or insert a new one with a minted
    /// `UUIDv7` id. Keying on the agent keeps one attribution row per agent when
    /// several agents touch the same path — a later touch by agent B no longer
    /// overwrites (and thus silently drops) agent A's attribution. `NULL`
    /// `agent_id` (unattributed pipeline) rows key separately via `IS`. There is
    /// no UNIQUE index on this quad (the audit trail keeps history across
    /// stages), so the upsert is done by hand.
    ///
    /// Returns the replaced row's `(additions, deletions)` (`None` when the row
    /// was created) so callers can derive the line-change **delta** this upsert
    /// represents (the global usage-stats recording needs the growth, not the
    /// replaced cumulative per-file counters).
    pub async fn upsert_tracked_change(&self, c: &NewTrackedChange) -> Result<Option<(i64, i64)>> {
        let now = now_iso();
        let existing: Option<(String, i64, i64)> = sqlx::query(
            "SELECT id, additions, deletions FROM tracked_changes \
             WHERE workspace_id = ? AND path = ? AND stage = ? AND agent_id IS ?",
        )
        .bind(&c.workspace_id.0)
        .bind(&c.path)
        .bind(&c.stage)
        .bind(&c.agent_id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("lookup tracked change failed: {e}")))?
        .map(|r| {
            (
                r.get::<String, _>("id"),
                r.get::<i64, _>("additions"),
                r.get::<i64, _>("deletions"),
            )
        });

        match existing {
            Some((id, prev_additions, prev_deletions)) => {
                sqlx::query(
                    "UPDATE tracked_changes SET status = ?, agent_id = ?, session_id = ?, \
                     turn = ?, commit_hash = ?, old_blob_sha = ?, new_blob_sha = ?, \
                     additions = ?, deletions = ?, updated_at = ? WHERE id = ?",
                )
                .bind(&c.status)
                .bind(&c.agent_id)
                .bind(&c.session_id)
                .bind(c.turn)
                .bind(&c.commit_hash)
                .bind(&c.old_blob_sha)
                .bind(&c.new_blob_sha)
                .bind(c.additions)
                .bind(c.deletions)
                .bind(&now)
                .bind(&id)
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("update tracked change failed: {e}")))?;
                Ok(Some((prev_additions, prev_deletions)))
            }
            None => {
                let id = Uuid::now_v7().to_string();
                let sql = format!(
                    "INSERT INTO tracked_changes ({TRACKED_CHANGE_COLUMNS}) \
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
                sqlx::query(&sql)
                    .bind(&id)
                    .bind(&c.workspace_id.0)
                    .bind(&c.path)
                    .bind(&c.stage)
                    .bind(&c.status)
                    .bind(&c.agent_id)
                    .bind(&c.session_id)
                    .bind(c.turn)
                    .bind(&c.commit_hash)
                    .bind(&c.old_blob_sha)
                    .bind(&c.new_blob_sha)
                    .bind(c.additions)
                    .bind(c.deletions)
                    .bind(&now)
                    .bind(&now)
                    .execute(self.write_pool())
                    .await
                    .map_err(|e| Error::Internal(format!("insert tracked change failed: {e}")))?;
                Ok(None)
            }
        }
    }

    /// Max cumulative `(additions, deletions)` across the **sibling** rows for
    /// the same `(workspace, path, stage)` — every row except `agent_id`'s own
    /// (`NULL` keys separately via `IS NOT`). `Ok(None)` when the path has no
    /// sibling rows. Used by the file-tracking pipeline as the usage-stats
    /// baseline for a fresh row on an already-tracked path (monorepo#1009):
    /// each row carries the file's full diff counters, so a new agent's first
    /// row must not re-record lines a sibling row already accounted for.
    pub async fn max_sibling_tracked_change_counters(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
        stage: &str,
        agent_id: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        let row = sqlx::query(
            "SELECT MAX(additions) AS additions, MAX(deletions) AS deletions \
             FROM tracked_changes \
             WHERE workspace_id = ? AND path = ? AND stage = ? AND agent_id IS NOT ?",
        )
        .bind(&workspace_id.0)
        .bind(path)
        .bind(stage)
        .bind(agent_id)
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("max sibling tracked change failed: {e}")))?;
        let additions: Option<i64> = row.get("additions");
        let deletions: Option<i64> = row.get("deletions");
        Ok(additions.zip(deletions))
    }

    /// Transition the `stage` of a workspace's tracked-change rows for `path`
    /// from `from_stage` to `to_stage` in place, stamping `updated_at` and
    /// preserving every attribution column (`agent_id`/`session_id`/`turn`) and
    /// the recorded stats/blobs. Backs `file-tracking.stage`/`unstage` (M4.8):
    /// staging/unstaging a file moves its audit row across the git stage without
    /// dropping who produced it. Returns the number of rows transitioned.
    pub async fn set_tracked_change_stage(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
        from_stage: &str,
        to_stage: &str,
    ) -> Result<u64> {
        let now = now_iso();
        let result = sqlx::query(
            "UPDATE tracked_changes SET stage = ?, updated_at = ? \
             WHERE workspace_id = ? AND path = ? AND stage = ?",
        )
        .bind(to_stage)
        .bind(&now)
        .bind(&workspace_id.0)
        .bind(path)
        .bind(from_stage)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("transition tracked change failed: {e}")))?;
        Ok(result.rows_affected())
    }

    /// List a workspace's tracked changes, oldest first. Internal read used by the
    /// pipeline + tests (the UI-facing reads land in M4.8).
    pub async fn list_tracked_changes(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TrackedChangeRow>> {
        let sql = format!(
            "SELECT {TRACKED_CHANGE_COLUMNS} FROM tracked_changes \
             WHERE workspace_id = ? ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list tracked changes failed: {e}")))?;
        rows.iter().map(map_tracked_change_row).collect()
    }
}

fn map_tracked_change_row(r: &SqliteRow) -> Result<TrackedChangeRow> {
    Ok(TrackedChangeRow {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        path: r.get("path"),
        stage: r.get("stage"),
        status: r.get("status"),
        agent_id: r.get("agent_id"),
        session_id: r.get("session_id"),
        turn: r.get("turn"),
        commit_hash: r.get("commit_hash"),
        old_blob_sha: r.get("old_blob_sha"),
        new_blob_sha: r.get("new_blob_sha"),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    })
}
