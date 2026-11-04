//! Ranked note search. Only winning rows hydrate note bodies; candidate
//! ranking reads the FTS index and its small, trigger-maintained context.

use intent_core::{Error, Result, WorkspaceId};
use sqlx::Row;

use crate::{tags_from_db, Store};

/// Filters for [`Store::search_notes_fts`]. Defaults preserve the legacy API's
/// unlimited, global search including individually archived notes. Interactive
/// callers should set a limit and exclude archived notes.
#[derive(Debug, Clone)]
pub struct NoteFtsOptions<'a> {
    pub workspace_id: Option<&'a WorkspaceId>,
    pub prefer_workspace_id: Option<&'a WorkspaceId>,
    /// Caller-visible workspace IDs, applied BEFORE ranking's final limit.
    /// `None` means unrestricted; `Some(&[])` grants no access.
    pub allowed_workspace_ids: Option<&'a [WorkspaceId]>,
    pub include_archived: bool,
    /// `None` is unlimited, zero is empty. Negative values are rejected.
    pub limit: Option<i64>,
    /// Best hits kept per workspace, applied BEFORE `limit`, so one workspace
    /// with many strong hits cannot crowd others out. `None` keeps every hit;
    /// zero is empty. Negative values are rejected.
    pub per_workspace_limit: Option<i64>,
    /// Keyset page start: only hits ordered strictly after this sort key
    /// are returned. `None` leaves the SQL unchanged.
    pub after: Option<&'a NoteFtsCursor>,
}

/// One hit's position in the ranked order (adjusted rank ascending, then
/// `updated_at` descending, then `workspace_id`, `note_id` ascending).
#[derive(Debug, Clone, PartialEq)]
pub struct NoteFtsCursor {
    pub rank: f64,
    pub updated_at: String,
    pub workspace_id: String,
    pub note_id: String,
}

/// Strictly-after test for [`NoteFtsCursor`] over the ranked columns,
/// matching the ORDER BY. Binds: rank, rank, updated_at, updated_at,
/// workspace_id, workspace_id, note_id.
const AFTER_CURSOR: &str = "(adjusted_rank > ? OR (adjusted_rank = ? AND (updated_at < ? \
     OR (updated_at = ? AND (workspace_id > ? OR (workspace_id = ? AND note_id > ?))))))";

impl Default for NoteFtsOptions<'_> {
    fn default() -> Self {
        Self {
            workspace_id: None,
            prefer_workspace_id: None,
            allowed_workspace_ids: None,
            include_archived: true,
            limit: None,
            per_workspace_limit: None,
            after: None,
        }
    }
}

/// Winning note plus the content needed to generate a matching preview.
/// Identity is always the pair `(workspace_id, note_id)`; lower rank is better.
#[derive(Debug, Clone)]
pub struct NoteFtsMatch {
    pub note_id: String,
    pub workspace_id: String,
    pub title: String,
    pub content: String,
    pub tags: Vec<String>,
    pub updated_at: String,
    pub is_archived: bool,
    pub workspace_archived: bool,
    pub rank: f64,
}

/// Shared with the query-plan regression guard. Binding order: preferred
/// workspace, MATCH expression, optional hard scope, optional permissions JSON,
/// optional per-workspace limit, optional cursor ([`AFTER_CURSOR`]), limit.
/// Values are bound, never interpolated into SQL.
pub(crate) fn search_notes_fts_sql(options: &NoteFtsOptions<'_>) -> String {
    let ranked = note_fts_ranked_sql(options);
    // Optional per-workspace cap: rank within each workspace with the same
    // tie-breaks as the global order, keep the best N, then apply the global
    // limit. Without the cap the SQL is unchanged (query-plan guard).
    let candidates = if options.per_workspace_limit.is_some() {
        let after = if options.after.is_some() {
            format!(" AND {AFTER_CURSOR}")
        } else {
            String::new()
        };
        format!(
            "SELECT note_id, workspace_id, updated_at, workspace_archived, adjusted_rank \
             FROM ( \
                 SELECT r.*, ROW_NUMBER() OVER ( \
                     PARTITION BY r.workspace_id \
                     ORDER BY r.adjusted_rank ASC, r.updated_at DESC, r.note_id ASC \
                 ) AS workspace_position \
                 FROM ({ranked}) r \
             ) \
             WHERE workspace_position <= ?{after} \
             ORDER BY adjusted_rank ASC, updated_at DESC, workspace_id ASC, note_id ASC \
             LIMIT ?"
        )
    } else if options.after.is_some() {
        format!(
            "SELECT * FROM ({ranked}) WHERE {AFTER_CURSOR} \
             ORDER BY adjusted_rank ASC, updated_at DESC, workspace_id ASC, note_id ASC \
             LIMIT ?"
        )
    } else {
        format!(
            "{ranked} \
             ORDER BY adjusted_rank ASC, c.updated_at DESC, c.workspace_id ASC, c.note_id ASC \
             LIMIT ?"
        )
    };
    // The LIMIT subquery prevents SQLite flattening it into a full note-table
    // join. Every tie-break is inside the limit as well as the outer ordering,
    // so the chosen subset is stable across insertion order/import/VACUUM.
    format!(
        "SELECT n.id AS note_id, n.workspace_id, n.title, n.content, n.tags, \
                n.updated_at, n.is_archived, top.workspace_archived, top.adjusted_rank \
         FROM ({candidates}) top \
         JOIN note n ON n.id = top.note_id AND n.workspace_id = top.workspace_id \
         ORDER BY top.adjusted_rank ASC, top.updated_at DESC, top.workspace_id ASC, top.note_id ASC"
    )
}

/// The ranked candidate rows (no order or limit). Binds: preferred
/// workspace, MATCH expression, optional hard scope, optional permissions.
fn note_fts_ranked_sql(options: &NoteFtsOptions<'_>) -> String {
    let mut filters = String::new();
    if options.workspace_id.is_some() {
        filters.push_str(" AND c.workspace_id = ?");
    }
    if options.allowed_workspace_ids.is_some() {
        filters.push_str(" AND c.workspace_id IN (SELECT value FROM json_each(?))");
    }
    if !options.include_archived {
        filters.push_str(" AND c.is_archived = 0");
    }
    format!(
        "SELECT c.note_id, c.workspace_id, c.updated_at, \
                w.archived AS workspace_archived, \
                bm25(note_fts, 5.0, 1.0, 2.0) \
                  - CASE WHEN c.workspace_id = ? THEN 1.0 ELSE 0.0 END \
                  + CASE WHEN w.archived <> 0 THEN 1.0 ELSE 0.0 END AS adjusted_rank \
         FROM note_fts \
         JOIN note_search_ctx c ON c.search_id = note_fts.rowid \
         JOIN workspace w ON w.id = c.workspace_id \
         WHERE note_fts MATCH ?{filters}"
    )
}

type SqliteQuery<'q> = sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>;

/// Bind [`note_fts_ranked_sql`]'s parameters, in its order.
fn bind_ranked<'q>(
    sql: &'q str,
    match_expr: &'q str,
    options: &NoteFtsOptions<'q>,
) -> Result<SqliteQuery<'q>> {
    let mut query = sqlx::query(sql)
        .bind(options.prefer_workspace_id.map(|id| id.0.as_str()))
        .bind(match_expr);
    if let Some(id) = options.workspace_id {
        query = query.bind(id.0.as_str());
    }
    if let Some(ids) = options.allowed_workspace_ids {
        // One JSON binding avoids SQLite's variable limit on large hosts.
        let ids: Vec<&str> = ids.iter().map(|id| id.0.as_str()).collect();
        query =
            query.bind(serde_json::to_string(&ids).map_err(|e| {
                Error::Internal(format!("encode note search permissions failed: {e}"))
            })?);
    }
    Ok(query)
}

impl Store {
    /// Current ranked positions of the given `(workspace_id, note_id)` notes
    /// under the same search and filters as [`Store::search_notes_fts`]
    /// (`limit`, `per_workspace_limit` and `after` are ignored). Notes that
    /// no longer match are absent. A keyset page uses this to re-anchor on
    /// its previous hits, whose ranks move with the index statistics.
    ///
    /// # Errors
    /// Returns an error for an invalid MATCH expression or database failure.
    pub async fn note_fts_positions(
        &self,
        match_expr: &str,
        options: &NoteFtsOptions<'_>,
        notes: &[(String, String)],
    ) -> Result<Vec<NoteFtsCursor>> {
        if match_expr.trim().is_empty()
            || notes.is_empty()
            || options
                .allowed_workspace_ids
                .is_some_and(<[WorkspaceId]>::is_empty)
        {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT note_id, workspace_id, updated_at, adjusted_rank FROM ({}) \
             WHERE (workspace_id, note_id) IN ( \
                 SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') \
                 FROM json_each(?))",
            note_fts_ranked_sql(options)
        );
        let wanted = serde_json::to_string(notes)
            .map_err(|e| Error::Internal(format!("encode note search anchors failed: {e}")))?;
        let rows = bind_ranked(&sql, match_expr, options)?
            .bind(wanted)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("note search positions failed: {e}")))?;
        rows.iter()
            .map(|row| {
                Ok(NoteFtsCursor {
                    rank: row.try_get("adjusted_rank")?,
                    updated_at: row.try_get("updated_at")?,
                    workspace_id: row.try_get("workspace_id")?,
                    note_id: row.try_get("note_id")?,
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(|e| Error::Internal(format!("decode note search position: {e}")))
    }

    /// Search the note FTS index using an already-sanitized MATCH expression
    /// (from `intent_search::fts_match_expr`). An empty expression returns no
    /// hits. Raw user text must be sanitized by the service, as for transcripts.
    ///
    /// All visibility, scope and archive filters run before the limit. Title,
    /// body and tags have BM25 weights 5/1/2; the preferred workspace gets a
    /// 1.0 boost and archived workspaces a 1.0 penalty, matching transcripts.
    /// Only selected notes' bodies are fetched for preview generation.
    ///
    /// # Errors
    /// Returns an error for a negative limit, invalid MATCH expression or
    /// database/decoding failure.
    pub async fn search_notes_fts(
        &self,
        match_expr: &str,
        options: &NoteFtsOptions<'_>,
    ) -> Result<Vec<NoteFtsMatch>> {
        if options.limit.is_some_and(|limit| limit < 0)
            || options.per_workspace_limit.is_some_and(|limit| limit < 0)
        {
            return Err(Error::InvalidParams(
                "note search limit must be nonnegative".into(),
            ));
        }
        if match_expr.trim().is_empty()
            || options.limit == Some(0)
            || options.per_workspace_limit == Some(0)
            || options
                .allowed_workspace_ids
                .is_some_and(<[WorkspaceId]>::is_empty)
        {
            return Ok(Vec::new());
        }
        let sql = search_notes_fts_sql(options);
        let mut query = bind_ranked(&sql, match_expr, options)?;
        if let Some(per_workspace) = options.per_workspace_limit {
            query = query.bind(per_workspace);
        }
        if let Some(after) = options.after {
            query = query
                .bind(after.rank)
                .bind(after.rank)
                .bind(after.updated_at.as_str())
                .bind(after.updated_at.as_str())
                .bind(after.workspace_id.as_str())
                .bind(after.workspace_id.as_str())
                .bind(after.note_id.as_str());
        }
        let rows = query
            .bind(options.limit.unwrap_or(-1))
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("search notes failed: {e}")))?;
        rows.iter()
            .map(|row| {
                let decode = || -> std::result::Result<_, sqlx::Error> {
                    Ok(NoteFtsMatch {
                        note_id: row.try_get("note_id")?,
                        workspace_id: row.try_get("workspace_id")?,
                        title: row.try_get("title")?,
                        content: row.try_get("content")?,
                        tags: Vec::new(),
                        updated_at: row.try_get("updated_at")?,
                        is_archived: row.try_get::<i64, _>("is_archived")? != 0,
                        workspace_archived: row.try_get::<i64, _>("workspace_archived")? != 0,
                        rank: row.try_get("adjusted_rank")?,
                    })
                };
                let mut hit = decode()
                    .map_err(|e| Error::Internal(format!("decode note search hit: {e}")))?;
                let tags: String = row
                    .try_get("tags")
                    .map_err(|e| Error::Internal(format!("decode note search tags: {e}")))?;
                hit.tags = tags_from_db(&tags)?;
                Ok(hit)
            })
            .collect()
    }
}
