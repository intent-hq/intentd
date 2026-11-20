//! Agent messages, their externalized payloads, transcript paging, session
//! message projections, unread probes, message search and monitor message
//! provenance (Postgres). Mirrors the message half of `agent_repo.rs` and
//! `agent_repo/monitor_message_provenance.rs`.
//!
//! The `SQLite` versions bracket transcript writes with the process-local
//! repository lifecycle observer. A Postgres store never installs one, so
//! those brackets are left out here (as in `workspaces.rs`).
//!
//! Multi-statement reads run in one `REPEATABLE READ` transaction, the
//! equivalent of the `SQLite` WAL read snapshot: a message and its payload
//! rows always come from the same state. Text ordering uses `COLLATE "C"` to
//! match `SQLite`'s byte order. `rowid` order is `row_seq`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;

use intent_core::{
    AgentId, AgentListRowScope, AgentMessage, AgentSession, Error, Result, WorkspaceId,
};
use sqlx::postgres::{PgConnection, PgRow};
use uuid::Uuid;

use super::agents::map_session;
use super::note_search::SearchQuery;
use super::notes::col;
use super::{db, PgStore, WriteTx};
use crate::agent_repo::monitor_message_provenance::envelope_digest;
use crate::agent_repo::{
    batch_content_cols_and_payload_rows, batch_preview_col_values, content_col_and_payload_rows,
    decode_last_tool_use_col, decode_preview_col, inferred_import_origin, last_tool_use_col_value,
    normalize_compound_model, preview_col_value, OwnedBatchMessage, PreparedContent,
    MESSAGE_PREVIEW_METADATA_MAX_BYTES, SESSION_COLUMNS,
};
use crate::message_payload::{
    self as payload, KIND_THUMBNAILS, KIND_TOOL_RESULT_OUTPUT, KIND_TOOL_RESULT_OUTPUT_REPLAY,
    KIND_TOOL_USE_INPUT, KIND_TOOL_USE_INPUT_REPLAY,
};
use crate::{
    project_user_message_index_metadata, FullAgentSessionRow, MessageFtsMatch,
    MonitorMessageProvenance, PrunedToolPayload, ReplaceMessage, SessionMessageProjection,
    UsageMessageOrigin, UserMessageIndexItem, UserMessageIndexPage, UserMessageIndexPageRequest,
};
use intent_core::review_monitor::ReviewMonitorBinding;

const MESSAGE_COLUMNS: &str = "id, agent_id, seq, role, content, metadata, created_at";

/// The `SQLite` search's preferred-workspace boost and archived-workspace
/// penalty (1.0 each) are soft in bm25 units: bm25 spreads over about 0-10
/// on a real corpus, so a decisively better match from another or an
/// archived workspace still wins. `ts_rank` has no IDF and spreads over
/// about 0.03-0.1, where 1.0 would make strict tiers. 0.01 keeps both soft:
/// identical matches still order preferred, other, archived, and a match
/// with more hits of the term still beats a single hit in a preferred or
/// active workspace (`message_search_boosts_are_soft`).
const PREFER_WORKSPACE_BOOST: f64 = 0.01;
const ARCHIVED_WORKSPACE_PENALTY: f64 = 0.01;

/// An unread top-level session row of `agent_session s` (see
/// `UNREAD_TOP_LEVEL_SESSION_PREDICATE` in `agent_repo.rs`).
const UNREAD_TOP_LEVEL: &str = "s.parent_agent_id IS NULL AND s.is_background = 0 \
     AND s.status <> 'deleted' AND s.last_message_id IS NOT NULL \
     AND s.last_message_role = 'assistant' AND s.retired_at IS NULL \
     AND s.notifications_muted = 0 \
     AND (sqlite_json_extract(NUL_FREE(s.metadata), '$.lastSeenMessageId') IS NULL \
          OR sqlite_json_extract(NUL_FREE(s.metadata), '$.lastSeenMessageId') <> s.last_message_id)";

/// Whether a user row is a qualified monitor wake (see the `SQLite`
/// `get_agent_user_message_index_page`): by its metadata, or by a content
/// block's `messageMetadata`.
const QUALIFIED_MONITOR: &str =
    "(COALESCE(sqlite_json_extract(NUL_FREE(m.metadata), '$.type') = 'pr_monitor_wake' \
       AND (sqlite_json_type(NUL_FREE(m.metadata), '$.resource') IS NOT NULL \
         OR sqlite_json_type(NUL_FREE(m.metadata), '$.availability') IS NOT NULL), false) \
     OR CASE WHEN sqlite_json_type(NUL_FREE(m.content)) = 'array' \
       THEN EXISTS(SELECT 1 FROM jsonb_array_elements(intent_json(NUL_FREE(m.content))) \
         AS b(block) \
         WHERE b.block #>> '{messageMetadata,type}' = 'pr_monitor_wake' \
           AND (b.block #> '{messageMetadata,resource}' IS NOT NULL \
             OR b.block #> '{messageMetadata,availability}' IS NOT NULL)) \
       ELSE false END)";

/// Expand each `NUL_FREE(col)` in `sql` to `col` without escaped NULs
/// (`\u0000`, not an escaped backslash followed by `u0000`). `jsonb`, which
/// the JSON compat functions parse into, rejects them, so such a document
/// would read as malformed; `SQLite` JSON1 accepts it. Only the NUL
/// characters are lost. `col` is a plain column reference.
pub(super) fn nul_free(sql: &str) -> String {
    const MARK: &str = "NUL_FREE(";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(MARK) {
        out.push_str(&rest[..at]);
        let after = &rest[at + MARK.len()..];
        let end = after.find(')').expect("NUL_FREE( is closed");
        let col = &after[..end];
        let _ = write!(
            out,
            r"(CASE WHEN strpos({col}, '\u0000') > 0 THEN regexp_replace({col}, '(?<!\\)((?:\\\\)*)\\u0000', '\1', 'g') ELSE {col} END)"
        );
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

fn internal(context: &str) -> impl Fn(sqlx::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{context}: {e}"))
}

fn map_message_row(row: &PgRow) -> Result<AgentMessage> {
    let content: serde_json::Value = serde_json::from_str(&col::<String>(row, "content")?)
        .map_err(|e| Error::Internal(format!("decode message content failed: {e}")))?;
    let metadata: Option<serde_json::Value> = match col::<Option<String>>(row, "metadata")? {
        Some(raw) => Some(
            serde_json::from_str(&raw)
                .map_err(|e| Error::Internal(format!("decode message metadata failed: {e}")))?,
        ),
        None => None,
    };
    Ok(AgentMessage {
        id: col(row, "id")?,
        agent_id: AgentId(col(row, "agent_id")?),
        seq: col(row, "seq")?,
        role: col(row, "role")?,
        content,
        app_message_id: intent_core::lift_app_message_id(metadata.as_ref()),
        author: None,
        metadata,
        created_at: col(row, "created_at")?,
    })
}

fn encode_metadata(metadata: Option<&serde_json::Value>) -> Result<Option<String>> {
    metadata
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| Error::Internal(format!("encode message metadata failed: {e}")))
}

pub(super) fn projection_from_row(row: &PgRow) -> Result<SessionMessageProjection> {
    let message_count: i64 = col(row, "message_count")?;
    Ok(SessionMessageProjection {
        message_count: message_count.max(0).cast_unsigned(),
        last_assistant_text_blocks: decode_preview_col(col(row, "last_assistant_preview")?),
        last_user_text_blocks: decode_preview_col(col(row, "last_user_preview")?),
        last_message_role: col(row, "last_message_role")?,
        last_message_id: col(row, "last_message_id")?,
        last_tool_use: decode_last_tool_use_col(col(row, "last_tool_use_preview")?),
    })
}

/// The `agent.list { scope }` filter over `agent_session s` and its extra
/// bind (`$3`), like `scope_predicate` in `agent_repo.rs`.
fn scope_filter(scope: &AgentListRowScope) -> (&'static str, Option<&str>) {
    match scope {
        AgentListRowScope::TopLevel => (
            " AND s.parent_agent_id IS NULL AND s.is_background = 0 AND s.retired_at IS NULL",
            None,
        ),
        AgentListRowScope::Delegated {
            parent_agent_id: None,
            orphaned_only: false,
        } => (
            " AND s.parent_agent_id IS NOT NULL AND s.retired_at IS NULL",
            None,
        ),
        AgentListRowScope::Delegated {
            parent_agent_id: None,
            orphaned_only: true,
        } => (
            " AND s.parent_agent_id IS NOT NULL AND s.retired_at IS NULL \
             AND NOT EXISTS (SELECT 1 FROM agent_session p WHERE p.tenant_id = $1 \
             AND p.id = s.parent_agent_id AND p.workspace_id = s.workspace_id \
             AND p.retired_at IS NULL)",
            None,
        ),
        AgentListRowScope::Delegated {
            parent_agent_id: Some(parent),
            ..
        } => (
            " AND s.parent_agent_id = $3 AND s.retired_at IS NULL",
            Some(parent.as_str()),
        ),
        AgentListRowScope::Background => (
            " AND s.parent_agent_id IS NULL AND s.is_background <> 0 AND s.retired_at IS NULL",
            None,
        ),
    }
}

/// Upsert one message's prepared payload rows inside the caller's write
/// transaction; a pre-staged row with the same key is overwritten.
pub(super) async fn insert_payload_rows(
    tx: &mut WriteTx,
    tenant: &str,
    message_id: &str,
    agent_id: &str,
    rows: &[payload::PayloadRow],
) -> Result<()> {
    for row in rows {
        upsert_payload(
            tx,
            tenant,
            message_id,
            agent_id,
            (
                row.block_ordinal,
                row.kind,
                row.encoding,
                row.body.as_slice(),
            ),
        )
        .await
        .map_err(|e| Error::Internal(format!("insert message payload failed: {e}")))?;
    }
    Ok(())
}

async fn upsert_payload(
    tx: &mut WriteTx,
    tenant: &str,
    message_id: &str,
    agent_id: &str,
    (block_ordinal, kind, encoding, body): (i64, &str, &str, &[u8]),
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO agent_message_payload \
         (tenant_id, message_id, agent_id, block_ordinal, kind, encoding, body) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (tenant_id, message_id, block_ordinal, kind) \
         DO UPDATE SET encoding = excluded.encoding, body = excluded.body",
    )
    .bind(tenant)
    .bind(message_id)
    .bind(agent_id)
    .bind(block_ordinal)
    .bind(kind)
    .bind(encoding)
    .bind(body)
    .execute(&mut **tx)
    .await
    .map(|_| ())
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn insert_message_row(
    tx: &mut WriteTx,
    tenant: &str,
    id: &str,
    agent_id: &str,
    seq: i64,
    role: &str,
    content_json: &str,
    metadata_json: Option<&str>,
    created_at: &str,
    usage: (&str, Option<&str>),
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO agent_message (tenant_id, id, agent_id, seq, role, content, metadata, \
         created_at, usage_model, usage_origin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(tenant)
    .bind(id)
    .bind(agent_id)
    .bind(seq)
    .bind(role)
    .bind(content_json)
    .bind(metadata_json)
    .bind(created_at)
    .bind(usage.0)
    .bind(usage.1)
    .execute(&mut **tx)
    .await
    .map(|_| ())
}

/// Overwrite the session's preview columns after a batch write.
async fn update_session_previews(
    tx: &mut WriteTx,
    tenant: &str,
    agent_id: &str,
    batch: &[OwnedBatchMessage],
    last_message_id: Option<&str>,
) -> Result<()> {
    let (assistant_preview, user_preview, last_message_role, last_tool_use) =
        batch_preview_col_values(batch)?;
    sqlx::query(
        "UPDATE agent_session SET last_assistant_preview = $3, last_user_preview = $4, \
         last_message_role = $5, last_message_id = $6, last_tool_use_preview = $7 \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind(agent_id)
    .bind(assistant_preview.as_deref())
    .bind(user_preview.as_deref())
    .bind(last_message_role.as_deref())
    .bind(last_message_id)
    .bind(last_tool_use.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(internal("session message previews write failed"))?;
    Ok(())
}

impl PgStore {
    pub async fn search_agent_messages_fts(
        &self,
        match_expr: &str,
        workspace_id: Option<&WorkspaceId>,
        agent_id: Option<&str>,
        role: Option<&str>,
        prefer_workspace_id: Option<&WorkspaceId>,
        limit: Option<i64>,
    ) -> Result<Vec<MessageFtsMatch>> {
        let Some(query) = SearchQuery::parse(match_expr) else {
            return Ok(Vec::new());
        };
        let sql = format!(
            "SELECT m.id AS message_id, m.agent_id, m.role, m.content, m.metadata, m.created_at, \
                    s.workspace_id, s.name AS agent_name, top.adjusted_rank \
             FROM (SELECT r.id, r.row_seq, \
                     (-ts_rank(r.search_tsv, q.query) \
                       - CASE WHEN rs.workspace_id = $2 THEN $4 ELSE 0.0 END \
                       + CASE WHEN w.archived <> 0 THEN $5 ELSE 0.0 END)::float8 AS adjusted_rank \
                   FROM agent_message r \
                   JOIN agent_session rs ON rs.tenant_id = $1 AND rs.id = r.agent_id \
                   JOIN workspace w ON w.tenant_id = $1 AND w.id = rs.workspace_id \
                   CROSS JOIN (SELECT {} AS query) q \
                   WHERE r.tenant_id = $1 AND r.search_tsv @@ q.query \
                     AND ($7::text IS NULL OR rs.workspace_id = $7) \
                     AND ($8::text IS NULL OR r.agent_id = $8) \
                     AND ($9::text IS NULL OR r.role = $9) \
                   ORDER BY adjusted_rank, r.row_seq DESC \
                   LIMIT $6) top \
             JOIN agent_message m ON m.tenant_id = $1 AND m.id = top.id \
             JOIN agent_session s ON s.tenant_id = $1 AND s.id = m.agent_id \
             ORDER BY top.adjusted_rank, m.created_at COLLATE \"C\" DESC, m.id COLLATE \"C\"",
            query.sql()
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(prefer_workspace_id.map(WorkspaceId::as_str))
            .bind(query.terms)
            .bind(PREFER_WORKSPACE_BOOST)
            .bind(ARCHIVED_WORKSPACE_PENALTY)
            .bind(limit.map(|n| n.max(0)))
            .bind(workspace_id.map(WorkspaceId::as_str))
            .bind(agent_id)
            .bind(role)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("search agent messages failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                let content: serde_json::Value =
                    serde_json::from_str(&col::<String>(row, "content")?).map_err(|e| {
                        Error::Internal(format!("decode message content failed: {e}"))
                    })?;
                Ok(MessageFtsMatch {
                    metadata: col::<Option<String>>(row, "metadata")?
                        .filter(|raw| raw.len() <= MESSAGE_PREVIEW_METADATA_MAX_BYTES)
                        .and_then(|raw| serde_json::from_str(&raw).ok()),
                    message_id: col(row, "message_id")?,
                    agent_id: col(row, "agent_id")?,
                    workspace_id: col(row, "workspace_id")?,
                    agent_name: col(row, "agent_name")?,
                    role: col(row, "role")?,
                    content,
                    created_at: col(row, "created_at")?,
                    rank: col(row, "adjusted_rank")?,
                })
            })
            .collect()
    }

    pub async fn append_agent_message(
        &self,
        agent_id: &AgentId,
        role: &str,
        content: &serde_json::Value,
        created_at: &str,
    ) -> Result<AgentMessage> {
        let id = Uuid::now_v7().to_string();
        self.append_agent_message_with_id(agent_id, &id, role, content, None, created_at)
            .await
    }

    pub async fn append_agent_message_with_metadata(
        &self,
        agent_id: &AgentId,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
    ) -> Result<AgentMessage> {
        let id = Uuid::now_v7().to_string();
        self.append_agent_message_with_id(agent_id, &id, role, content, metadata, created_at)
            .await
    }

    pub async fn append_agent_message_with_id(
        &self,
        agent_id: &AgentId,
        id: &str,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
    ) -> Result<AgentMessage> {
        self.append_agent_message_inner(
            agent_id,
            id,
            role,
            content,
            metadata,
            created_at,
            false,
            role_origin(role),
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn append_agent_message_with_provenance(
        &self,
        agent_id: &AgentId,
        id: &str,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
        origin: UsageMessageOrigin,
    ) -> Result<AgentMessage> {
        self.append_agent_message_inner(
            agent_id, id, role, content, metadata, created_at, false, origin,
        )
        .await
    }

    pub async fn prestage_agent_message_payload(
        &self,
        agent_id: &AgentId,
        message_id: &str,
        block_ordinal: usize,
        block: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        if !payload::block_needs_extraction(block) {
            return Ok(None);
        }
        let owned = block.clone();
        let ordinal = i64::try_from(block_ordinal).unwrap_or(i64::MAX);
        let (slim, row) =
            tokio::task::spawn_blocking(move || payload::extract_block_payload(owned, ordinal))
                .await
                .map_err(|e| Error::Internal(format!("payload extraction task failed: {e}")))??;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut tx = self.begin_write().await?;
        upsert_payload(
            &mut tx,
            self.tenant(),
            message_id,
            &agent_id.0,
            (
                row.block_ordinal,
                row.kind,
                row.encoding,
                row.body.as_slice(),
            ),
        )
        .await
        .map_err(internal("prestage message payload failed"))?;
        tx.commit()
            .await
            .map_err(internal("prestage message payload commit failed"))?;
        Ok(Some(slim))
    }

    pub async fn append_agent_message_prestaged(
        &self,
        agent_id: &AgentId,
        id: &str,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
    ) -> Result<AgentMessage> {
        self.append_agent_message_inner(
            agent_id,
            id,
            role,
            content,
            metadata,
            created_at,
            true,
            role_origin(role),
        )
        .await
    }

    pub async fn delete_prestaged_agent_message_payloads(&self, message_id: &str) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM agent_message_payload p WHERE p.tenant_id = $1 AND p.message_id = $2 \
             AND NOT EXISTS (SELECT 1 FROM agent_message m WHERE m.tenant_id = $1 AND m.id = $2)",
        )
        .bind(self.tenant())
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(internal("delete prestaged payloads failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(deleted)
    }

    /// Shared body of the appends; `prestaged` adopts payload rows staged
    /// under `id` and deletes the ones the final content no longer uses.
    #[expect(clippy::too_many_arguments)]
    async fn append_agent_message_inner(
        &self,
        agent_id: &AgentId,
        id: &str,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
        prestaged: bool,
        origin: UsageMessageOrigin,
    ) -> Result<AgentMessage> {
        let metadata_json = encode_metadata(metadata)?;
        let preview_update = match role {
            "assistant" => Some(("last_assistant_preview", preview_col_value(role, content)?)),
            "user" => Some(("last_user_preview", preview_col_value(role, content)?)),
            _ => None,
        };
        let last_tool_use = match &preview_update {
            Some(_) => last_tool_use_col_value(content)?,
            None => None,
        };
        // CPU-bound extraction and thumbnails run before the transaction.
        let PreparedContent {
            content_json,
            payload_rows,
            stamped,
        } = content_col_and_payload_rows(content).await?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(s.resolved_model, ''), NULLIF(s.model, ''), 'unknown') AS model, \
             (SELECT COALESCE(MAX(m.seq), -1) + 1 FROM agent_message m \
              WHERE m.tenant_id = $1 AND m.agent_id = $2) AS next \
             FROM agent_session s WHERE s.tenant_id = $1 AND s.id = $2",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("message provenance read failed"))?;
        let seq: i64 = col(&row, "next")?;
        let usage_model: String = col(&row, "model")?;
        let stale_keys: Vec<(i64, String)> = if prestaged {
            let staged: Vec<(i64, String)> = sqlx::query_as(
                "SELECT block_ordinal, kind FROM agent_message_payload \
                 WHERE tenant_id = $1 AND message_id = $2",
            )
            .bind(tenant)
            .bind(id)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("read prestaged payload keys failed"))?;
            let placeholders = payload::placeholder_keys(content);
            for (ordinal, kind) in &placeholders {
                if !staged.iter().any(|(o, k)| o == ordinal && k == kind)
                    && !payload_rows
                        .iter()
                        .any(|r| r.block_ordinal == *ordinal && &r.kind == kind)
                {
                    tracing::warn!(
                        message_id = id,
                        block_ordinal = ordinal,
                        kind,
                        "placeholder block has no pre-staged payload row; \
                         reads will serve the stored preview"
                    );
                }
            }
            staged
                .into_iter()
                .filter(|(o, k)| {
                    !placeholders.iter().any(|(po, pk)| po == o && pk == k)
                        && !payload_rows
                            .iter()
                            .any(|r| r.block_ordinal == *o && r.kind == k)
                })
                .collect()
        } else {
            Vec::new()
        };
        insert_message_row(
            &mut tx,
            tenant,
            id,
            &agent_id.0,
            seq,
            role,
            &content_json,
            metadata_json.as_deref(),
            created_at,
            (usage_model.as_str(), Some(origin.as_db())),
        )
        .await
        .map_err(internal("append agent message failed"))?;
        insert_payload_rows(&mut tx, tenant, id, &agent_id.0, &payload_rows).await?;
        for (ordinal, kind) in &stale_keys {
            sqlx::query(
                "DELETE FROM agent_message_payload \
                 WHERE tenant_id = $1 AND message_id = $2 AND block_ordinal = $3 AND kind = $4",
            )
            .bind(tenant)
            .bind(id)
            .bind(ordinal)
            .bind(kind)
            .execute(&mut *tx)
            .await
            .map_err(internal("delete stale prestaged payload failed"))?;
        }
        if let Some((column, value)) = &preview_update {
            // A user/assistant append is the session's newest message.
            let sql = format!(
                "UPDATE agent_session SET {column} = $3, last_message_role = $4, \
                 last_message_id = $5, last_tool_use_preview = $6 \
                 WHERE tenant_id = $1 AND id = $2"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(tenant)
                .bind(&agent_id.0)
                .bind(value.as_str())
                .bind(role)
                .bind(id)
                .bind(last_tool_use.as_deref())
                .execute(&mut *tx)
                .await
                .map_err(internal("session message preview write failed"))?;
        }
        let counts = match origin {
            UsageMessageOrigin::Human => Some((1_i64, 0_i64)),
            UsageMessageOrigin::Agent => Some((0, 1)),
            UsageMessageOrigin::Excluded => None,
        };
        if let Some((human, agent)) = counts {
            sqlx::query(
                "INSERT INTO agent_usage_cell \
                 (tenant_id, agent_id, model, human_messages, agent_messages) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
                 human_messages = agent_usage_cell.human_messages + excluded.human_messages, \
                 agent_messages = agent_usage_cell.agent_messages + excluded.agent_messages",
            )
            .bind(tenant)
            .bind(&agent_id.0)
            .bind(&usage_model)
            .bind(human)
            .bind(agent)
            .execute(&mut *tx)
            .await
            .map_err(internal("usage message cell write failed"))?;
        }
        tx.commit()
            .await
            .map_err(internal("append agent message commit failed"))?;
        Ok(AgentMessage {
            id: id.to_string(),
            agent_id: agent_id.clone(),
            seq,
            role: role.to_string(),
            content: stamped.unwrap_or_else(|| content.clone()),
            app_message_id: intent_core::lift_app_message_id(metadata),
            author: None,
            metadata: metadata.cloned(),
            created_at: created_at.to_string(),
        })
    }

    pub async fn agent_has_assistant_message(&self, agent_id: &AgentId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND role = 'assistant')",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("probe assistant message failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(exists)
    }

    pub async fn agent_has_assistant_after_last_user(&self, agent_id: &AgentId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_message a \
             WHERE a.tenant_id = $1 AND a.agent_id = $2 AND a.role = 'assistant' AND a.seq > \
             (SELECT MAX(u.seq) FROM agent_message u \
              WHERE u.tenant_id = $1 AND u.agent_id = $2 AND u.role = 'user'))",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("probe assistant after last user failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(exists)
    }

    pub async fn get_agent_messages(
        &self,
        agent_id: &AgentId,
        limit: Option<i64>,
    ) -> Result<Vec<AgentMessage>> {
        let mut tx = self.begin_read_snapshot().await?;
        let messages = self.messages_in(&mut tx, agent_id, limit).await?;
        tx.commit().await.map_err(db)?;
        Ok(messages)
    }

    /// One agent's messages (the newest `limit`, or all) in `seq` order, with
    /// their payloads spliced back in.
    async fn messages_in(
        &self,
        conn: &mut PgConnection,
        agent_id: &AgentId,
        limit: Option<i64>,
    ) -> Result<Vec<AgentMessage>> {
        let sql = match limit {
            Some(_) => format!(
                "SELECT {MESSAGE_COLUMNS} FROM (SELECT {MESSAGE_COLUMNS} FROM agent_message \
                 WHERE tenant_id = $1 AND agent_id = $2 ORDER BY seq DESC LIMIT $3) t \
                 ORDER BY seq"
            ),
            None => format!(
                "SELECT {MESSAGE_COLUMNS} FROM agent_message \
                 WHERE tenant_id = $1 AND agent_id = $2 ORDER BY seq"
            ),
        };
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0);
        if let Some(n) = limit {
            query = query.bind(n.max(0));
        }
        let rows = query
            .fetch_all(&mut *conn)
            .await
            .map_err(internal("get agent messages failed"))?;
        let mut messages: Vec<AgentMessage> =
            rows.iter().map(map_message_row).collect::<Result<_>>()?;
        self.hydrate_message_payloads(conn, &mut messages).await?;
        Ok(messages)
    }

    /// Splice the full-body payload rows back into `messages` (the read
    /// side of write-time extraction). Replay rows are ignored; an
    /// undecodable row leaves the stored preview in place.
    async fn hydrate_message_payloads(
        &self,
        conn: &mut PgConnection,
        messages: &mut [AgentMessage],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let index_by_id: HashMap<String, usize> = messages
            .iter()
            .enumerate()
            .map(|(i, m)| (m.id.clone(), i))
            .collect();
        let ids: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
        let rows = sqlx::query(
            "SELECT message_id, block_ordinal, kind, encoding, body FROM agent_message_payload \
             WHERE tenant_id = $1 AND kind IN ($2, $3) AND message_id = ANY($4)",
        )
        .bind(self.tenant())
        .bind(KIND_TOOL_USE_INPUT)
        .bind(KIND_TOOL_RESULT_OUTPUT)
        .bind(ids)
        .fetch_all(conn)
        .await
        .map_err(internal("get message payloads failed"))?;
        for row in &rows {
            let message_id: String = col(row, "message_id")?;
            let Some(&idx) = index_by_id.get(&message_id) else {
                continue;
            };
            let block_ordinal: i64 = col(row, "block_ordinal")?;
            let kind: String = col(row, "kind")?;
            let encoding: String = col(row, "encoding")?;
            let body: Vec<u8> = col(row, "body")?;
            match payload::decode_body(&encoding, &body) {
                Ok(v) => {
                    payload::splice_payload(&mut messages[idx].content, block_ordinal, &kind, v);
                }
                Err(e) => tracing::warn!(
                    message = %message_id,
                    block_ordinal,
                    kind,
                    error = %e,
                    "decode message payload failed; serving stored preview"
                ),
            }
        }
        Ok(())
    }

    pub async fn get_agent_messages_for_replay(
        &self,
        agent_id: &AgentId,
        replay_chars: usize,
    ) -> Result<Vec<AgentMessage>> {
        const ROWS_PER_PAGE: i64 = 64;
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 ORDER BY seq"
        );
        let mut tx = self.begin_read_snapshot().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("get agent messages for replay failed"))?;
        let mut messages: Vec<AgentMessage> =
            rows.iter().map(map_message_row).collect::<Result<_>>()?;
        if messages.is_empty() {
            return Ok(messages);
        }
        let index_by_id: HashMap<String, usize> = messages
            .iter()
            .enumerate()
            .map(|(i, m)| (m.id.clone(), i))
            .collect();
        let mut after: i64 = 0;
        loop {
            let rows = sqlx::query(
                "SELECT row_seq, message_id, block_ordinal, kind, encoding, body \
                 FROM agent_message_payload \
                 WHERE tenant_id = $1 AND agent_id = $2 AND row_seq > $3 \
                   AND kind IN ($5, $6, $7, $8) \
                 ORDER BY row_seq LIMIT $4",
            )
            .bind(self.tenant())
            .bind(&agent_id.0)
            .bind(after)
            .bind(ROWS_PER_PAGE)
            .bind(KIND_TOOL_USE_INPUT)
            .bind(KIND_TOOL_RESULT_OUTPUT)
            .bind(KIND_TOOL_USE_INPUT_REPLAY)
            .bind(KIND_TOOL_RESULT_OUTPUT_REPLAY)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("get replay payloads failed"))?;
            let page_len = rows.len();
            for row in &rows {
                after = col(row, "row_seq")?;
                let message_id: String = col(row, "message_id")?;
                let Some(&idx) = index_by_id.get(&message_id) else {
                    continue;
                };
                let block_ordinal: i64 = col(row, "block_ordinal")?;
                let kind: String = col(row, "kind")?;
                let encoding: String = col(row, "encoding")?;
                let body: Vec<u8> = col(row, "body")?;
                let preview = payload::decode_body(&encoding, &body).and_then(|v| {
                    if payload::is_replay_kind(&kind) {
                        payload::ReplayPreview::from_row_body(v)
                    } else {
                        Ok(payload::ReplayPreview::from_body(&v, replay_chars))
                    }
                });
                match preview {
                    Ok(preview) => payload::splice_replay_preview(
                        &mut messages[idx].content,
                        block_ordinal,
                        &kind,
                        &preview,
                        replay_chars,
                    ),
                    Err(e) => tracing::warn!(
                        message = %message_id,
                        block_ordinal,
                        kind,
                        error = %e,
                        "decode replay payload failed; serving stored preview"
                    ),
                }
            }
            if i64::try_from(page_len).unwrap_or(i64::MAX) < ROWS_PER_PAGE {
                break;
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(messages)
    }

    pub async fn compact_tool_payloads_before(
        &self,
        cutoff_iso: &str,
        replay_chars: usize,
    ) -> Result<u64> {
        const CHUNK: i64 = 500;
        type Scanned = (String, String, i64, String, String, Vec<u8>);
        let mut total: u64 = 0;
        let mut after: i64 = 0;
        loop {
            let mut read = self.begin().await?;
            let rows = sqlx::query(
                "SELECT p.row_seq, p.message_id, p.agent_id, p.block_ordinal, p.kind, \
                        p.encoding, p.body \
                 FROM agent_message_payload p \
                 JOIN agent_message m ON m.tenant_id = $1 AND m.id = p.message_id \
                 WHERE p.tenant_id = $1 AND p.row_seq > $2 AND p.kind IN ($5, $6) \
                   AND m.created_at COLLATE \"C\" < $3 \
                 ORDER BY p.row_seq LIMIT $4",
            )
            .bind(self.tenant())
            .bind(after)
            .bind(cutoff_iso)
            .bind(CHUNK)
            .bind(KIND_TOOL_USE_INPUT)
            .bind(KIND_TOOL_RESULT_OUTPUT)
            .fetch_all(&mut *read)
            .await
            .map_err(internal("scan tool payloads failed"))?;
            read.commit().await.map_err(db)?;
            if rows.is_empty() {
                break;
            }
            let page_len = rows.len();
            let mut scanned: Vec<Scanned> = Vec::with_capacity(page_len);
            for row in &rows {
                after = col(row, "row_seq")?;
                scanned.push((
                    col(row, "message_id")?,
                    col(row, "agent_id")?,
                    col(row, "block_ordinal")?,
                    col(row, "kind")?,
                    col(row, "encoding")?,
                    col(row, "body")?,
                ));
            }
            drop(rows);
            let converted = tokio::task::spawn_blocking(move || {
                let mut out = Vec::with_capacity(scanned.len());
                for (message_id, agent_id, block_ordinal, kind, encoding, body) in scanned {
                    let Some(replay_kind) = payload::replay_kind_for(&kind) else {
                        continue;
                    };
                    let encoded = payload::decode_body(&encoding, &body)
                        .map(|v| payload::ReplayPreview::from_body(&v, replay_chars))
                        .and_then(|p| p.encode());
                    match encoded {
                        Ok((replay_encoding, replay_body)) => out.push((
                            message_id,
                            agent_id,
                            block_ordinal,
                            kind,
                            replay_kind,
                            replay_encoding,
                            replay_body,
                        )),
                        Err(e) => tracing::warn!(
                            message = %message_id,
                            block_ordinal,
                            kind,
                            error = %e,
                            "decode tool payload failed; leaving full row in place"
                        ),
                    }
                }
                out
            })
            .await
            .map_err(|e| Error::Internal(format!("payload compaction task failed: {e}")))?;
            let mut tx = self.begin_write().await?;
            for (message_id, agent_id, ordinal, kind, replay_kind, encoding, body) in &converted {
                let deleted = sqlx::query(
                    "DELETE FROM agent_message_payload WHERE tenant_id = $1 \
                     AND message_id = $2 AND block_ordinal = $3 AND kind = $4",
                )
                .bind(self.tenant())
                .bind(message_id)
                .bind(ordinal)
                .bind(kind)
                .execute(&mut *tx)
                .await
                .map_err(internal("compact tool payloads delete failed"))?
                .rows_affected();
                if deleted == 0 {
                    continue;
                }
                upsert_payload(
                    &mut tx,
                    self.tenant(),
                    message_id,
                    agent_id,
                    (*ordinal, *replay_kind, *encoding, body.as_slice()),
                )
                .await
                .map_err(internal("compact tool payloads insert failed"))?;
                total += 1;
            }
            tx.commit()
                .await
                .map_err(internal("compact tool payloads commit failed"))?;
            if i64::try_from(page_len).unwrap_or(i64::MAX) < CHUNK {
                break;
            }
        }
        Ok(total)
    }

    /// The blocks of one message whose heavy body the retention sweep
    /// replaced: a replay row and no full-body row of the same field.
    async fn pruned_tool_payloads_in(
        &self,
        conn: &mut PgConnection,
        message_id: &str,
    ) -> Result<Vec<PrunedToolPayload>> {
        let present: Vec<(i64, String)> = sqlx::query_as(
            "SELECT block_ordinal, kind FROM agent_message_payload \
             WHERE tenant_id = $1 AND message_id = $2 AND block_ordinal >= 0",
        )
        .bind(self.tenant())
        .bind(message_id)
        .fetch_all(conn)
        .await
        .map_err(internal("list tool payload kinds failed"))?;
        let mut pruned: Vec<PrunedToolPayload> = present
            .iter()
            .filter_map(|(ordinal, kind)| {
                let (field, full_kind) = payload::pruned_field_for(kind)?;
                let full_retained = present.iter().any(|(o, k)| o == ordinal && k == full_kind);
                (!full_retained).then_some(PrunedToolPayload {
                    block_ordinal: *ordinal,
                    field,
                })
            })
            .collect();
        pruned.sort_by_key(|p| (p.block_ordinal, p.field));
        Ok(pruned)
    }

    pub async fn get_last_non_system_message(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<AgentMessage>> {
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND role <> 'system' \
             ORDER BY seq DESC LIMIT 1"
        );
        let mut tx = self.begin_read_snapshot().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal("get last non-system message failed"))?;
        let Some(message) = row.as_ref().map(map_message_row).transpose()? else {
            return Ok(None);
        };
        let mut messages = [message];
        self.hydrate_message_payloads(&mut tx, &mut messages)
            .await?;
        tx.commit().await.map_err(db)?;
        let [message] = messages;
        Ok(Some(message))
    }

    pub async fn persisted_queue_message_ids(&self, agent_id: &AgentId) -> Result<HashSet<String>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(nul_free(
            "SELECT sqlite_json_extract(NUL_FREE(metadata), '$.queueInfo.queuedMessageId') \
             FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 AND role = 'user' \
             AND sqlite_json_type(NUL_FREE(metadata), '$.queueInfo.queuedMessageId') = 'text'",
        )))
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("read persisted queue message IDs failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().collect())
    }

    pub async fn get_agent_message_by_id(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<AgentMessage>> {
        let mut tx = self.begin_read_snapshot().await?;
        let message = self
            .get_agent_message_by_id_in(&mut tx, agent_id, message_id)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(message)
    }

    pub async fn get_agent_message_by_id_with_pruned(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<(AgentMessage, Vec<PrunedToolPayload>)>> {
        let mut tx = self.begin_read_snapshot().await?;
        let Some(message) = self
            .get_agent_message_by_id_in(&mut tx, agent_id, message_id)
            .await?
        else {
            return Ok(None);
        };
        let pruned = self.pruned_tool_payloads_in(&mut tx, message_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(Some((message, pruned)))
    }

    async fn get_agent_message_by_id_in(
        &self,
        conn: &mut PgConnection,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<AgentMessage>> {
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND id = $3"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0)
            .bind(message_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(internal("get agent message by id failed"))?;
        let Some(message) = row.as_ref().map(map_message_row).transpose()? else {
            return Ok(None);
        };
        let mut messages = [message];
        self.hydrate_message_payloads(conn, &mut messages).await?;
        let [message] = messages;
        Ok(Some(message))
    }

    pub async fn count_agent_messages(&self, agent_id: &AgentId) -> Result<i64> {
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_message WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("count agent messages failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(n)
    }

    pub async fn get_agent_message_index(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<i64>> {
        let mut tx = self.begin().await?;
        let idx: Option<i64> = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM agent_message p \
               WHERE p.tenant_id = $1 AND p.agent_id = m.agent_id AND p.seq < m.seq) \
             FROM agent_message m WHERE m.tenant_id = $1 AND m.agent_id = $2 AND m.id = $3",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get agent message index failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(idx)
    }

    pub async fn get_agent_messages_page(
        &self,
        agent_id: &AgentId,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<AgentMessage>> {
        let mut tx = self.begin_read_snapshot().await?;
        let mut messages = self
            .select_messages_page(&mut tx, agent_id, offset, limit)
            .await?;
        self.hydrate_message_payloads(&mut tx, &mut messages)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(messages)
    }

    pub async fn get_agent_messages_page_as_stored(
        &self,
        agent_id: &AgentId,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<AgentMessage>> {
        let mut tx = self.begin().await?;
        let messages = self
            .select_messages_page(&mut tx, agent_id, offset, limit)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(messages)
    }

    async fn select_messages_page(
        &self,
        conn: &mut PgConnection,
        agent_id: &AgentId,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<AgentMessage>> {
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 \
             ORDER BY seq LIMIT $3 OFFSET $4"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0)
            .bind(limit.max(0))
            .bind(offset.max(0))
            .fetch_all(conn)
            .await
            .map_err(internal("get agent messages page failed"))?
            .iter()
            .map(map_message_row)
            .collect()
    }

    pub async fn get_agent_history_batch(
        &self,
        agent_id: &AgentId,
        before_seq: Option<i64>,
        limit: usize,
    ) -> Result<(Vec<AgentMessage>, u64)> {
        let mut tx = self.begin_read_snapshot().await?;
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT message_count FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("read history count failed"))?
        .unwrap_or(0)
        .max(0)
        .cast_unsigned();
        // Newest first, stopping once the rows before one reach the budget.
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM (SELECT {MESSAGE_COLUMNS}, \
               SUM(octet_length(content) + COALESCE(octet_length(metadata), 0)) OVER \
               (ORDER BY seq DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) \
               AS preceding_bytes \
             FROM (SELECT {MESSAGE_COLUMNS} FROM agent_message \
               WHERE tenant_id = $1 AND agent_id = $2 AND seq < $3 \
               ORDER BY seq DESC LIMIT $4) c) b \
             WHERE COALESCE(preceding_bytes, 0) < $5 ORDER BY seq DESC"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&agent_id.0)
            .bind(before_seq.unwrap_or(i64::MAX))
            .bind(i64::try_from(limit.clamp(1, 8)).unwrap_or(8))
            .bind(i64::try_from(intent_core::SLIM_PAGE_BUDGET_BYTES).unwrap_or(i64::MAX))
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("read history batch failed"))?;
        tx.commit().await.map_err(db)?;
        let messages = rows.iter().map(map_message_row).collect::<Result<_>>()?;
        Ok((messages, total))
    }

    pub async fn get_agent_user_message_index(
        &self,
        agent_id: &AgentId,
        preview_chars: usize,
    ) -> Result<Vec<UserMessageIndexItem>> {
        Ok(self
            .get_agent_user_message_index_page(agent_id, preview_chars, None)
            .await?
            .items)
    }

    pub async fn get_agent_user_message_index_page(
        &self,
        agent_id: &AgentId,
        preview_chars: usize,
        page: Option<UserMessageIndexPageRequest>,
    ) -> Result<UserMessageIndexPage> {
        let tail = match page {
            Some(UserMessageIndexPageRequest {
                before_seq: Some(_),
                ..
            }) => "AND m.seq < $4 ORDER BY m.seq DESC LIMIT $5",
            Some(_) => "ORDER BY m.seq DESC LIMIT $4",
            None => "ORDER BY m.seq",
        };
        // The metadata is projected in Rust
        // (`project_user_message_index_metadata`), as SQLite's pre-trim is.
        let sql = format!(
            "SELECT m.id, m.seq, \
                    substr(intent_message_search_text(NUL_FREE(m.content)), 1, LEAST($2, 2147483647)::int) \
                      AS preview, \
                    m.metadata, m.created_at, {QUALIFIED_MONITOR} AS qualified_monitor \
             FROM agent_message m \
             WHERE m.tenant_id = $1 AND m.agent_id = $3 AND m.role = 'user' {tail}"
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(nul_free(&sql)))
            .bind(self.tenant())
            .bind(i64::try_from(preview_chars).unwrap_or(i64::MAX))
            .bind(&agent_id.0);
        if let Some(page) = page {
            if let Some(before_seq) = page.before_seq {
                query = query.bind(before_seq);
            }
            // One extra row tells whether older history remains.
            query = query.bind(i64::try_from(page.limit).unwrap_or(i64::MAX - 1) + 1);
        }
        let mut tx = self.begin().await?;
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("get agent user message index failed"))?;
        tx.commit().await.map_err(db)?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            let metadata = match col::<Option<String>>(row, "metadata")? {
                Some(raw) => {
                    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                        Error::Internal(format!("decode message metadata failed: {e}"))
                    })?;
                    project_user_message_index_metadata(&value)
                }
                None => None,
            };
            items.push(UserMessageIndexItem {
                qualified_monitor: col(row, "qualified_monitor")?,
                seq: col(row, "seq")?,
                id: col(row, "id")?,
                preview: col::<Option<String>>(row, "preview")?.unwrap_or_default(),
                metadata,
                created_at: col(row, "created_at")?,
            });
        }
        let mut next_before_seq = None;
        if let Some(page) = page {
            if items.len() > page.limit {
                items.truncate(page.limit);
                next_before_seq = items.last().map(|item| item.seq);
            }
            items.reverse();
        }
        Ok(UserMessageIndexPage {
            items,
            next_before_seq,
        })
    }

    pub async fn get_agent_message_thumbnails(
        &self,
        agent_id: &AgentId,
        message_ids: &[String],
    ) -> Result<HashMap<String, serde_json::Value>> {
        if message_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT m.id, m.thumbnails, p.encoding, p.body FROM agent_message m \
             LEFT JOIN agent_message_payload p \
               ON p.tenant_id = $1 AND p.message_id = m.id AND p.kind = $4 \
             WHERE m.tenant_id = $1 AND m.agent_id = $2 \
               AND (m.thumbnails IS NOT NULL OR p.body IS NOT NULL) AND m.id = ANY($3)",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(message_ids)
        .bind(KIND_THUMBNAILS)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("get agent message thumbnails failed"))?;
        tx.commit().await.map_err(db)?;
        let mut map = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: String = col(row, "id")?;
            let decoded = if let Some(encoding) = col::<Option<String>>(row, "encoding")? {
                let body: Vec<u8> = col(row, "body")?;
                payload::decode_body(&encoding, &body)
            } else {
                let raw: String = col(row, "thumbnails")?;
                serde_json::from_str::<serde_json::Value>(&raw)
                    .map_err(|e| Error::Internal(e.to_string()))
            };
            match decoded {
                Ok(v) => {
                    map.insert(id, v);
                }
                Err(e) => tracing::warn!(
                    message = %id,
                    error = %e,
                    "decode message thumbnails failed; serving block with data omitted"
                ),
            }
        }
        Ok(map)
    }

    pub async fn replace_agent_messages(
        &self,
        agent_id: &AgentId,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<Vec<AgentMessage>> {
        type Identity = (String, String, Option<String>, String);
        let owned: Vec<OwnedBatchMessage> = messages
            .iter()
            .map(|m| {
                (
                    m.role.to_string(),
                    m.content.clone(),
                    m.metadata.cloned(),
                    m.created_at.to_string(),
                )
            })
            .collect();
        // CPU-bound extraction and thumbnails run before the transaction.
        let prepared = batch_content_cols_and_payload_rows(&owned).await?;
        let metadata_json: Vec<Option<String>> = owned
            .iter()
            .map(|(_, _, metadata, _)| encode_metadata(metadata.as_ref()))
            .collect::<Result<_>>()?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        // A replaced transcript cannot inherit its prior native family.
        sqlx::query("DELETE FROM review_context_enrollment WHERE tenant_id = $1 AND agent_id = $2")
            .bind(tenant)
            .bind(&agent_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Internal("private native enrollment invalidation failed".into()))?;
        let usage_model: String = sqlx::query_scalar(
            "SELECT COALESCE(NULLIF(resolved_model, ''), NULLIF(model, ''), 'unknown') \
             FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("replace provenance read failed"))?;
        let existing = sqlx::query(
            "SELECT role, content, metadata, created_at, usage_model, usage_origin \
             FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 ORDER BY seq",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("replace provenance rows failed"))?;
        let mut existing_by_identity =
            HashMap::<Identity, VecDeque<(Option<String>, Option<String>)>>::new();
        for old in &existing {
            existing_by_identity
                .entry((
                    col(old, "role")?,
                    col(old, "content")?,
                    col(old, "metadata")?,
                    col(old, "created_at")?,
                ))
                .or_default()
                .push_back((col(old, "usage_model")?, col(old, "usage_origin")?));
        }
        let identities: Vec<Identity> = owned
            .iter()
            .zip(&prepared)
            .zip(&metadata_json)
            .map(|(((role, _, _, created_at), p), metadata)| {
                (
                    role.clone(),
                    p.content_json.clone(),
                    metadata.clone(),
                    created_at.clone(),
                )
            })
            .collect();
        let mut replacement_counts = HashMap::<&Identity, usize>::new();
        for identity in &identities {
            *replacement_counts.entry(identity).or_default() += 1;
        }
        // Equal multiplicity on both sides keeps the persisted provenance;
        // a changed multiplicity is ambiguous and gets new history.
        let preservable: HashSet<Identity> = existing_by_identity
            .iter()
            .filter(|(identity, rows)| replacement_counts.get(identity) == Some(&rows.len()))
            .map(|(identity, _)| identity.clone())
            .collect();
        sqlx::query("DELETE FROM agent_message WHERE tenant_id = $1 AND agent_id = $2")
            .bind(tenant)
            .bind(&agent_id.0)
            .execute(&mut *tx)
            .await
            .map_err(internal("replace agent messages clear failed"))?;
        sqlx::query(
            "UPDATE agent_usage_cell SET human_messages = 0, agent_messages = 0 \
             WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .execute(&mut *tx)
        .await
        .map_err(internal("reset usage message cells failed"))?;
        let mut inserted = Vec::with_capacity(owned.len());
        let mut message_counts = BTreeMap::<String, (i64, i64)>::new();
        let mut last_message_id: Option<String> = None;
        for (idx, (role, content, metadata, created_at)) in owned.iter().enumerate() {
            let seq = i64::try_from(idx).unwrap_or(i64::MAX);
            let id = Uuid::now_v7().to_string();
            if role == "user" || role == "assistant" {
                last_message_id = Some(id.clone());
            }
            let identity = &identities[idx];
            let preserved = preservable
                .contains(identity)
                .then(|| existing_by_identity.get_mut(identity)?.pop_front())
                .flatten();
            let row_model = preserved
                .as_ref()
                .and_then(|old| old.0.clone())
                .filter(|model| !model.is_empty())
                .unwrap_or_else(|| usage_model.clone());
            let counts = message_counts.entry(row_model.clone()).or_default();
            let usage_origin = match preserved.as_ref().and_then(|old| old.1.as_deref()) {
                Some("agent") => {
                    counts.1 += 1;
                    "agent"
                }
                Some("human") => {
                    counts.0 += 1;
                    "human"
                }
                Some("excluded") => "excluded",
                _ => match role.as_str() {
                    "user"
                        if preserved.is_some()
                            && metadata
                                .as_ref()
                                .and_then(|m| m.get("fromAgentId"))
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|id| !id.is_empty()) =>
                    {
                        counts.1 += 1;
                        "agent"
                    }
                    "assistant" => {
                        counts.1 += 1;
                        "agent"
                    }
                    "user" => {
                        counts.0 += 1;
                        "human"
                    }
                    _ => "excluded",
                },
            };
            let PreparedContent {
                content_json,
                payload_rows,
                stamped,
            } = &prepared[idx];
            insert_message_row(
                &mut tx,
                tenant,
                &id,
                &agent_id.0,
                seq,
                role,
                content_json,
                metadata_json[idx].as_deref(),
                created_at,
                (row_model.as_str(), Some(usage_origin)),
            )
            .await
            .map_err(internal("replace agent messages insert failed"))?;
            insert_payload_rows(&mut tx, tenant, &id, &agent_id.0, payload_rows).await?;
            inserted.push(AgentMessage {
                id,
                agent_id: agent_id.clone(),
                seq,
                role: role.clone(),
                content: stamped.as_ref().unwrap_or(content).clone(),
                app_message_id: intent_core::lift_app_message_id(metadata.as_ref()),
                author: None,
                metadata: metadata.clone(),
                created_at: created_at.clone(),
            });
        }
        for (model, (human, agent)) in message_counts {
            sqlx::query(
                "INSERT INTO agent_usage_cell \
                 (tenant_id, agent_id, model, human_messages, agent_messages) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, agent_id, model) \
                 DO UPDATE SET human_messages = excluded.human_messages, \
                 agent_messages = excluded.agent_messages",
            )
            .bind(tenant)
            .bind(&agent_id.0)
            .bind(model)
            .bind(human)
            .bind(agent)
            .execute(&mut *tx)
            .await
            .map_err(internal("replace usage message cells failed"))?;
        }
        update_session_previews(
            &mut tx,
            tenant,
            &agent_id.0,
            &owned,
            last_message_id.as_deref(),
        )
        .await?;
        tx.commit()
            .await
            .map_err(internal("replace agent messages commit failed"))?;
        Ok(inserted)
    }

    pub async fn truncate_agent_messages_from(
        &self,
        agent_id: &AgentId,
        first_dropped_seq: i64,
    ) -> Result<usize> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        // Only the dropped suffix's message counts are subtracted; token
        // spend and costs survive transcript edits.
        let counts: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT COALESCE(NULLIF(usage_model, ''), 'unknown') AS model, \
             COUNT(*) FILTER (WHERE usage_origin = 'human') AS human, \
             COUNT(*) FILTER (WHERE usage_origin = 'agent') AS agent \
             FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 AND seq >= $3 \
             AND usage_origin IN ('human', 'agent') GROUP BY 1",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .bind(first_dropped_seq)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("read truncated usage counts failed"))?;
        for (model, human, agent) in counts {
            sqlx::query(
                "UPDATE agent_usage_cell SET human_messages = GREATEST(0, human_messages - $3), \
                 agent_messages = GREATEST(0, agent_messages - $4) \
                 WHERE tenant_id = $1 AND agent_id = $2 AND model = $5",
            )
            .bind(tenant)
            .bind(&agent_id.0)
            .bind(human)
            .bind(agent)
            .bind(model)
            .execute(&mut *tx)
            .await
            .map_err(internal("subtract truncated usage counts failed"))?;
        }
        let deleted = sqlx::query(
            "DELETE FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 AND seq >= $3",
        )
        .bind(tenant)
        .bind(&agent_id.0)
        .bind(first_dropped_seq)
        .execute(&mut *tx)
        .await
        .map_err(internal("truncate agent messages failed"))?
        .rows_affected();
        // Previews derive from the stored (slim) content; no hydration.
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND role = $3 ORDER BY seq DESC LIMIT 1"
        );
        let mut newest: Vec<AgentMessage> = Vec::with_capacity(2);
        for role in ["assistant", "user"] {
            let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(tenant)
                .bind(&agent_id.0)
                .bind(role)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("read newest {role} message failed: {e}")))?;
            if let Some(row) = row {
                newest.push(map_message_row(&row)?);
            }
        }
        newest.sort_by_key(|m| m.seq);
        let batch: Vec<OwnedBatchMessage> = newest
            .iter()
            .map(|m| {
                (
                    m.role.clone(),
                    m.content.clone(),
                    m.metadata.clone(),
                    m.created_at.clone(),
                )
            })
            .collect();
        let last_message_id = newest.last().map(|m| m.id.as_str());
        update_session_previews(&mut tx, tenant, &agent_id.0, &batch, last_message_id).await?;
        tx.commit()
            .await
            .map_err(internal("truncate agent messages commit failed"))?;
        Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
    }

    pub async fn get_agent_session_message_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<HashMap<String, (u64, bool, u64)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
            "SELECT s.id, s.message_count, s.assistant_message_count, s.conversation_bytes \
             FROM agent_session s WHERE s.tenant_id = $1 AND s.workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("get agent session message stats failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(id, messages, assistant, bytes)| {
                (
                    id,
                    (
                        messages.max(0).cast_unsigned(),
                        assistant > 0,
                        bytes.max(0).cast_unsigned(),
                    ),
                )
            })
            .collect())
    }

    pub async fn get_agent_session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        self.session_message_projections(workspace_id, "", None)
            .await
    }

    pub async fn get_active_agent_session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        self.session_message_projections(workspace_id, " AND s.retired_at IS NULL", None)
            .await
    }

    pub async fn get_retired_agent_session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        self.session_message_projections(workspace_id, " AND s.retired_at IS NOT NULL", None)
            .await
    }

    pub async fn get_scoped_agent_session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
        scope: &AgentListRowScope,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        let (filter, extra) = scope_filter(scope);
        self.session_message_projections(workspace_id, filter, extra)
            .await
    }

    /// `filter` is a compile-time fragment over `agent_session s`, never
    /// caller input; `extra` binds `$3`.
    async fn session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
        filter: &str,
        extra: Option<&str>,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        let sql = format!(
            "SELECT s.id AS agent_id, s.message_count, s.last_assistant_preview, \
             s.last_user_preview, s.last_message_role, s.last_message_id, \
             s.last_tool_use_preview \
             FROM agent_session s WHERE s.tenant_id = $1 AND s.workspace_id = $2{filter}"
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&workspace_id.0);
        if let Some(extra) = extra {
            query = query.bind(extra);
        }
        let mut tx = self.begin().await?;
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("session message projections failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| Ok((col(row, "agent_id")?, projection_from_row(row)?)))
            .collect()
    }

    pub async fn get_agent_session_message_projection(
        &self,
        agent_id: &AgentId,
    ) -> Result<SessionMessageProjection> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT s.last_assistant_preview, s.last_user_preview, s.last_message_role, \
             s.last_message_id, s.last_tool_use_preview, s.message_count \
             FROM agent_session s WHERE s.tenant_id = $1 AND s.id = $2",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("session message projection failed"))?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map_or_else(
            || Ok(SessionMessageProjection::default()),
            projection_from_row,
        )
    }

    pub async fn get_workspace_message_watermark(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        let mut tx = self.begin().await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_message m WHERE m.tenant_id = $1 AND m.agent_id IN \
             (SELECT s.id FROM agent_session s WHERE s.tenant_id = $1 AND s.workspace_id = $2)",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("get workspace message watermark failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(count.cast_unsigned())
    }

    pub async fn workspace_has_unread_top_level_session(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool> {
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM agent_session s \
             WHERE s.tenant_id = $1 AND s.workspace_id = $2 AND {UNREAD_TOP_LEVEL})"
        );
        let mut tx = self.begin().await?;
        let unread: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(nul_free(&sql)))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal("workspace unread probe failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(unread)
    }

    pub async fn workspaces_with_unread_top_level_sessions(&self) -> Result<HashSet<String>> {
        let sql = format!(
            "SELECT DISTINCT s.workspace_id FROM agent_session s \
             WHERE s.tenant_id = $1 AND {UNREAD_TOP_LEVEL}"
        );
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(nul_free(&sql)))
            .bind(self.tenant())
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("batch workspace unread probe failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().collect())
    }

    pub async fn workspaces_with_unread_top_level_sessions_by_workspace(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashSet<WorkspaceId>> {
        if workspace_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let sql = format!(
            "SELECT DISTINCT s.workspace_id FROM agent_session s \
             WHERE s.tenant_id = $1 AND s.workspace_id = ANY($2) AND {UNREAD_TOP_LEVEL}"
        );
        let wanted: Vec<&str> = workspace_ids.iter().map(WorkspaceId::as_str).collect();
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(nul_free(&sql)))
            .bind(self.tenant())
            .bind(wanted)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("scoped batch workspace unread probe failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn list_legacy_question_tail_candidates_by_workspace(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<Vec<(AgentId, String, String, serde_json::Value)>> {
        if workspace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<&str> = workspace_ids.iter().map(WorkspaceId::as_str).collect();
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String, String, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(nul_free(
                "SELECT s.id, m.id, m.role, m.content FROM agent_session s \
             JOIN agent_message m ON m.tenant_id = $1 AND m.id = (\
                 SELECT tail.id FROM agent_message tail \
                 WHERE tail.tenant_id = $1 AND tail.agent_id = s.id AND tail.role <> 'system' \
                 ORDER BY tail.seq DESC LIMIT 1) \
             WHERE s.tenant_id = $1 AND s.workspace_id = ANY($2) \
               AND s.parent_agent_id IS NULL AND s.is_background = 0 \
               AND s.status <> 'deleted' AND s.retired_at IS NULL \
               AND s.notifications_muted = 0 \
               AND COALESCE(sqlite_json_type(NUL_FREE(s.metadata), \
                    '$.pendingQuestionsMessageId'), '') <> 'text'",
            )))
            .bind(self.tenant())
            .bind(wanted)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal("batch legacy question tail read failed"))?;
        tx.commit().await.map_err(db)?;
        rows.into_iter()
            .map(|(agent, message, role, raw)| {
                let content = serde_json::from_str(&raw).map_err(|e| {
                    Error::Internal(format!("decode legacy question tail content failed: {e}"))
                })?;
                Ok((AgentId(agent), message, role, content))
            })
            .collect()
    }

    pub async fn list_top_level_sessions_with_unseen_last_message(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<(String, String)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(nul_free(
            "SELECT id, last_message_id FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = $2 \
               AND parent_agent_id IS NULL AND is_background = 0 AND status <> 'deleted' \
               AND last_message_id IS NOT NULL \
               AND (sqlite_json_extract(NUL_FREE(metadata), '$.lastSeenMessageId') IS NULL \
                    OR sqlite_json_extract(NUL_FREE(metadata), '$.lastSeenMessageId') \
                       <> last_message_id) \
             ORDER BY created_at COLLATE \"C\"",
        )))
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list unseen top-level sessions failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    pub async fn get_monitor_message_provenance(
        &self,
        agent: &AgentId,
        message_id: &str,
    ) -> Result<Option<MonitorMessageProvenance>> {
        let failed = |_: sqlx::Error| Error::Internal("monitor history read failed".into());
        let mut tx = self.begin_read_snapshot().await?;
        let row: Option<(String, String, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT p.workspace_id, p.binding_json, p.envelope_sha256, p.version \
             FROM pr_monitor_message_provenance p \
             JOIN agent_session s ON s.tenant_id = $1 AND s.id = p.agent_id \
               AND s.workspace_id = p.workspace_id \
             WHERE p.tenant_id = $1 AND p.message_id = $2 AND p.agent_id = $3",
        )
        .bind(self.tenant())
        .bind(message_id)
        .bind(&agent.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(failed)?;
        let Some((workspace_id, binding_json, digest, version)) = row else {
            return Ok(None);
        };
        let Ok(binding) = serde_json::from_str::<ReviewMonitorBinding>(&binding_json) else {
            return Ok(None);
        };
        if version != 1 || digest.len() != 32 {
            return Ok(None);
        }
        let workspace_id = WorkspaceId(workspace_id);
        let Some(message) = self
            .get_agent_message_by_id_in(&mut tx, agent, message_id)
            .await?
        else {
            return Ok(None);
        };
        let actual = envelope_digest(&message, &workspace_id, &binding)?;
        if digest != actual {
            return Ok(None);
        }
        tx.commit().await.map_err(failed)?;
        Ok(Some(MonitorMessageProvenance {
            message,
            workspace_id,
            binding,
            envelope_sha256: actual,
        }))
    }
}

fn role_origin(role: &str) -> UsageMessageOrigin {
    match role {
        "assistant" => UsageMessageOrigin::Agent,
        "user" => UsageMessageOrigin::Human,
        _ => UsageMessageOrigin::Excluded,
    }
}

/// Whole transcripts: a session inserted with its message log, and sessions
/// read back with theirs. Each read runs in one snapshot, so a session and
/// its messages always come from the same state.
impl PgStore {
    pub async fn insert_agent_session_with_messages(
        &self,
        s: &impl FullAgentSessionRow,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<()> {
        let s = s.full_row();
        let owned: Vec<OwnedBatchMessage> = messages
            .iter()
            .map(|m| {
                (
                    m.role.to_string(),
                    m.content.clone(),
                    m.metadata.cloned(),
                    m.created_at.to_string(),
                )
            })
            .collect();
        // CPU-bound extraction and thumbnails run before the transaction.
        let prepared = batch_content_cols_and_payload_rows(&owned).await?;
        let metadata_json: Vec<Option<String>> = owned
            .iter()
            .map(|(_, _, metadata, _)| encode_metadata(metadata.as_ref()))
            .collect::<Result<_>>()?;
        let (import_model, _) = normalize_compound_model(s.model.clone(), s.provider.clone());
        let import_model = import_model.unwrap_or_else(|| "unknown".to_string());
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        self.insert_session_row(&mut tx, s, false)
            .await?
            .map_err(internal("insert agent session failed"))?;
        let mut human_messages = 0_i64;
        let mut agent_messages = 0_i64;
        let mut last_message_id: Option<String> = None;
        for (idx, (role, _, metadata, created_at)) in owned.iter().enumerate() {
            let PreparedContent {
                content_json,
                payload_rows,
                ..
            } = &prepared[idx];
            let id = Uuid::now_v7().to_string();
            if role == "user" || role == "assistant" {
                last_message_id = Some(id.clone());
            }
            let origin = inferred_import_origin(role, metadata.as_ref());
            match origin {
                UsageMessageOrigin::Human => human_messages += 1,
                UsageMessageOrigin::Agent => agent_messages += 1,
                UsageMessageOrigin::Excluded => {}
            }
            insert_message_row(
                &mut tx,
                tenant,
                &id,
                &s.id.0,
                i64::try_from(idx).unwrap_or(i64::MAX),
                role,
                content_json,
                metadata_json[idx].as_deref(),
                created_at,
                (import_model.as_str(), Some(origin.as_db())),
            )
            .await
            .map_err(internal("append agent message failed"))?;
            insert_payload_rows(&mut tx, tenant, &id, &s.id.0, payload_rows).await?;
        }
        if human_messages != 0 || agent_messages != 0 {
            sqlx::query(
                "INSERT INTO agent_usage_cell \
                 (tenant_id, agent_id, model, human_messages, agent_messages) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, agent_id, model) \
                 DO UPDATE SET human_messages = excluded.human_messages, \
                 agent_messages = excluded.agent_messages",
            )
            .bind(tenant)
            .bind(&s.id.0)
            .bind(&import_model)
            .bind(human_messages)
            .bind(agent_messages)
            .execute(&mut *tx)
            .await
            .map_err(internal("materialize imported usage counts failed"))?;
        }
        update_session_previews(&mut tx, tenant, &s.id.0, &owned, last_message_id.as_deref())
            .await?;
        tx.commit()
            .await
            .map_err(internal("insert session with messages commit failed"))
    }

    pub async fn get_agent_session_with_transcript(&self, id: &AgentId) -> Result<AgentSession> {
        let mut tx = self.begin_read_snapshot().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SESSION_COLUMNS} FROM agent_session WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get agent session failed"))?
        .ok_or_else(|| Error::NotFound(format!("agent session {id}")))?;
        let mut session = map_session(&row, true)?;
        session.messages = self.messages_in(&mut tx, id, None).await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }

    pub async fn list_agent_sessions_with_transcripts(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSession>> {
        let mut tx = self.begin_read_snapshot().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SESSION_COLUMNS} FROM agent_session WHERE tenant_id = $1 \
             AND workspace_id = $2 ORDER BY created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list agent sessions failed"))?;
        let mut sessions = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut session = map_session(row, true)?;
            session.messages = self.messages_in(&mut tx, &session.id, None).await?;
            sessions.push(session);
        }
        tx.commit().await.map_err(db)?;
        Ok(sessions)
    }
}
