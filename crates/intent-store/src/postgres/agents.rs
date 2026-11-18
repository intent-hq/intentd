//! Agent sessions, child agents, interrupted agents, persisted queues,
//! task links, flipped completions, wake-delivery markers and review-context
//! enrollment (Postgres). Mirrors the session half of `agent_repo.rs`,
//! `agent_queue_repo.rs`, `task_agent_link_repo.rs`,
//! `agent_flipped_completion_repo.rs`, `advisory_wake_delivery_repo.rs`,
//! `completion_wake_delivery_repo.rs` and `agent_repo/review_context_enrollment.rs`.
//!
//! Session deletion takes the repository lifecycle bracket as on `SQLite`;
//! the other writers here predate the Postgres lifecycle domain and leave
//! it out. `SQLite` edits
//! session `metadata` with JSON1 inside one statement; here the same edits
//! run in Rust on the row read inside the writer-locked transaction, which
//! orders them against every other writer of the tenant.
//!
//! Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order, with
//! `row_seq` (insertion order) breaking ties.

use std::collections::{BTreeMap, HashMap};

use intent_core::prompt_breakdown::PromptBreakdown;
use intent_core::{
    AgentDelegatedCounts, AgentId, AgentListRowScope, AgentMessage, AgentParentDelegatedCounts,
    AgentScopeCounts, AgentSession, AgentStatus, Error, NoteId, Result, TaskAgentLink,
    TokenUsageTotals, UsageCost, WorkspaceId, PENDING_QUESTIONS_MESSAGE_ID_KEY,
};
use serde_json::{Map, Value};
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::Row;

use super::{db, PgStore, WriteTx};
use crate::agent_flipped_completion_repo::AGENT_FLIPPED_COMPLETIONS_CAP;
use crate::agent_queue_repo::AgentQueueRow;
use crate::agent_repo::prepared_monitor_message::{draft_monitor_message, MonitorMessageDraft};
use crate::agent_repo::UsageMessageOrigin;
use crate::agent_repo::{
    effort_levels_from_db, effort_levels_to_db, encode_metadata, json_col_from_db, json_col_to_db,
    normalize_compound_model, AgentTurnEffort, AgentUsageCellRow, AgentUsageRow, ChildAgentCounts,
    InterruptedAgent, ReviewContextEnrollment, DAEMON_OWNED_METADATA_KEYS,
    MANUAL_CHOICE_METADATA_KEY, SESSION_COLUMNS,
};
use crate::{
    enum_from_db, enum_to_db, AgentSessionSummary, FullAgentSessionRow, PreparedMonitorMessage,
    QualifiedPrMonitor,
};
use intent_core::review_monitor::{ReviewMonitorBinding, ReviewMonitorWakeMetadata};

/// [`crate::agent_repo::SESSION_SUMMARY_COLUMNS`] without the heavy columns.
const SUMMARY_COLUMNS: &str = crate::agent_repo::SESSION_SUMMARY_COLUMNS;

const INTERRUPTED_SELECT: &str = "SELECT ia.agent_id, ia.workspace_id, ia.prev_status, \
    ia.interrupted_at, ia.reason, ag.name AS agent_name, w.title AS workspace_name \
    FROM interrupted_agent ia \
    INNER JOIN agent_session ag ON ag.tenant_id = $1 AND ia.agent_id = ag.id \
    LEFT JOIN workspace w ON w.tenant_id = $1 AND ia.workspace_id = w.id \
    WHERE ia.tenant_id = $1 AND ia.resolution = 'pending'";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn not_found(id: &AgentId) -> Error {
    Error::NotFound(format!("agent session {id}"))
}

fn count(row: &PgRow, name: &str) -> Result<u64> {
    Ok(u64::try_from(col::<i64>(row, name)?).unwrap_or(0))
}

fn ids(ids: &[AgentId]) -> Vec<&str> {
    ids.iter().map(|id| id.0.as_str()).collect()
}

fn workspace_ids(ids: &[WorkspaceId]) -> Vec<&str> {
    ids.iter().map(|id| id.0.as_str()).collect()
}

pub(super) fn map_session(row: &PgRow, heavy: bool) -> Result<AgentSession> {
    let metadata = match col::<Option<String>>(row, "metadata")? {
        Some(raw) if !raw.is_empty() => {
            Some(serde_json::from_str::<Value>(&raw).map_err(|e| {
                Error::Internal(format!("decode agent session metadata failed: {e}"))
            })?)
        }
        _ => None,
    };
    let (system_prompt, image_blocks, initial_message) = if heavy {
        (
            col(row, "system_prompt")?,
            json_col_from_db(col(row, "image_blocks")?, "image_blocks")?,
            col(row, "initial_message")?,
        )
    } else {
        (None, None, None)
    };
    let (model, provider) = normalize_compound_model(col(row, "model")?, col(row, "provider")?);
    // agent-session-literal: allow — the row mapper must read every persisted column
    Ok(AgentSession {
        id: AgentId(col(row, "id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        parent_agent_id: col::<Option<String>>(row, "parent_agent_id")?.map(AgentId),
        backend_session_id: col::<Option<String>>(row, "backend_session_id")?.map(AgentId),
        acp_session_id: col(row, "acp_session_id")?,
        name: col(row, "name")?,
        name_explicitly_set: col::<i64>(row, "name_explicitly_set")? != 0,
        model,
        reasoning_effort: col(row, "reasoning_effort")?,
        effort_levels: effort_levels_from_db(col(row, "effort_levels")?)?,
        provider,
        specialist: col(row, "specialist")?,
        status: enum_from_db::<AgentStatus>(&col::<String>(row, "status")?)?,
        is_active: col::<i64>(row, "is_active")? != 0,
        system_prompt,
        messages: Vec::new(),
        stats: None,
        task_note_id: col::<Option<String>>(row, "task_note_id")?.map(NoteId::from),
        skip_auto_commit: col::<i64>(row, "skip_auto_commit")? != 0,
        completion_report: col(row, "completion_report")?,
        completion_report_timestamp: col(row, "completion_report_timestamp")?,
        attention_request_kind: col(row, "attention_request_kind")?,
        attention_request_reason: col(row, "attention_request_reason")?,
        attention_request_timestamp: col(row, "attention_request_timestamp")?,
        delegation_depth: col(row, "delegation_depth")?,
        initial_message,
        context_references: json_col_from_db(
            col(row, "context_references")?,
            "context_references",
        )?,
        image_blocks,
        file_blocks: json_col_from_db(col(row, "file_blocks")?, "file_blocks")?,
        is_background: col::<i64>(row, "is_background")? != 0,
        metadata,
        stop_reason: col(row, "stop_reason")?,
        stop_reason_timestamp: col(row, "stop_reason_timestamp")?,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: col(row, "retired_at")?,
        notifications_muted: col::<i64>(row, "notifications_muted")? != 0,
        pinned: col::<i64>(row, "pinned")? != 0,
        permission_policy: col(row, "permission_policy")?,
        harness_version: col(row, "harness_version")?,
        harness_features: json_col_from_db(col(row, "harness_features")?, "harness_features")?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

pub(super) fn map_summary(row: &PgRow) -> Result<AgentSessionSummary> {
    map_session(row, false).map(AgentSessionSummary::from_session)
}

fn map_interrupted(row: &PgRow) -> Result<InterruptedAgent> {
    Ok(InterruptedAgent {
        agent_id: AgentId(col(row, "agent_id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        prev_status: col(row, "prev_status")?,
        interrupted_at: col(row, "interrupted_at")?,
        agent_name: col(row, "agent_name")?,
        workspace_name: col(row, "workspace_name")?,
        reason: col(row, "reason")?,
    })
}

// ---- session metadata (SQLite JSON1 semantics, in Rust) -------------------

/// `CASE WHEN metadata IS NULL THEN '{}' WHEN json_type(metadata) = 'object'
/// THEN metadata ELSE json_object('priorNonObjectMetadata', json(metadata))
/// END`; malformed JSON is an error, as `json_type` raises in `SQLite`.
fn metadata_object(raw: Option<&str>) -> Result<Map<String, Value>> {
    let Some(raw) = raw else {
        return Ok(Map::new());
    };
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(other) => Ok(Map::from_iter([(
            "priorNonObjectMetadata".to_owned(),
            other,
        )])),
        Err(e) => Err(Error::Internal(format!(
            "agent session metadata is malformed JSON: {e}"
        ))),
    }
}

/// The stored document for `json_extract`, which raises on malformed JSON.
fn extractable_metadata(raw: Option<&str>) -> Result<Option<Value>> {
    raw.map(|r| {
        serde_json::from_str(r)
            .map_err(|e| Error::Internal(format!("agent session metadata is malformed JSON: {e}")))
    })
    .transpose()
}

/// The stored document when it is valid JSON (`json_valid`).
fn metadata_value(raw: Option<&str>) -> Option<Value> {
    raw.and_then(|r| serde_json::from_str(r).ok())
}

fn path(key: &str) -> impl Iterator<Item = &str> {
    key.split('.')
}

/// `json_extract(doc, '$.' || key)` as a JSON value (`None` when missing).
fn get_key<'a>(doc: &'a Value, key: &str) -> Option<&'a Value> {
    path(key).try_fold(doc, |v, k| v.as_object()?.get(k))
}

/// `json_set(doc, '$.' || key, value)`, creating missing parent objects.
fn set_key(doc: &mut Map<String, Value>, key: &str, value: Value) {
    let parts: Vec<&str> = path(key).collect();
    let (leaf, parents) = parts.split_last().expect("split yields one part");
    let mut cur = doc;
    for p in parents {
        let entry = cur
            .entry((*p).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        let Value::Object(next) = entry else {
            return;
        };
        cur = next;
    }
    cur.insert((*leaf).to_owned(), value);
}

/// `json_remove(doc, '$.' || key)`; keeps the order of the other keys.
fn remove_key(doc: &mut Map<String, Value>, key: &str) {
    let parts: Vec<&str> = path(key).collect();
    let (leaf, parents) = parts.split_last().expect("split yields one part");
    let mut cur = doc;
    for p in parents {
        let Some(Value::Object(next)) = cur.get_mut(*p) else {
            return;
        };
        cur = next;
    }
    cur.shift_remove(*leaf);
}

/// `json_extract(metadata, '$.' || key) IS expected`: a JSON string equals
/// a text bind; a missing key or JSON null is SQL NULL.
fn extract_is(doc: Option<&Value>, key: &str, expected: Option<&str>) -> bool {
    match (doc.and_then(|d| get_key(d, key)), expected) {
        (None | Some(Value::Null), None) => true,
        (Some(Value::String(s)), Some(e)) => s == e,
        _ => false,
    }
}

/// The stored document is a valid object that holds `key` (`json_type` is
/// not NULL, so a JSON null counts).
fn has_key(raw: Option<&str>, key: &str) -> bool {
    matches!(metadata_value(raw), Some(ref d @ Value::Object(_)) if get_key(d, key).is_some())
}

/// `json(?)`: the text parsed as JSON, an error when malformed.
fn json_arg(value: &str) -> Result<Value> {
    serde_json::from_str(value).map_err(|e| Error::Internal(format!("malformed JSON value: {e}")))
}

fn encode(doc: Map<String, Value>) -> Result<String> {
    serde_json::to_string(&Value::Object(doc))
        .map_err(|e| Error::Internal(format!("encode agent session metadata failed: {e}")))
}

/// RFC 7396 merge patch (`SQLite` `json_patch`).
fn merge_patch(target: &mut Value, patch: Value) {
    let Value::Object(patch) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(map) = target else {
        return;
    };
    for (k, v) in patch {
        if v.is_null() {
            map.shift_remove(&k);
        } else {
            merge_patch(map.entry(k).or_insert(Value::Null), v);
        }
    }
}

/// A whole-document `metadata` write that keeps the daemon-owned keys as
/// stored (`daemon_owned_preserving_metadata` in `agent_repo.rs`). `chief`
/// drops the caller's `chiefPromptVersion` when the stored row has none.
fn whole_metadata(
    stored: Option<&str>,
    value: Option<&Value>,
    keep_choice: bool,
    chief: bool,
) -> Result<Option<String>> {
    let stored_doc = metadata_value(stored);
    let stripped = value.map(|v| match v {
        Value::Object(map) => {
            let mut map = map.clone();
            for key in DAEMON_OWNED_METADATA_KEYS {
                map.shift_remove(key);
            }
            Value::Object(map)
        }
        other => other.clone(),
    });
    let drop_chief = chief
        && matches!(
            stored_doc
                .as_ref()
                .and_then(|d| get_key(d, "chiefPromptVersion")),
            None | Some(Value::Null)
        );
    let apply_chief = |mut doc: Value| {
        if drop_chief {
            if let Value::Object(map) = &mut doc {
                map.shift_remove("chiefPromptVersion");
            }
        }
        doc
    };
    let Some(stored_doc) = stored_doc.filter(|d| {
        d.is_object()
            && DAEMON_OWNED_METADATA_KEYS
                .iter()
                .any(|k| get_key(d, k).is_some())
    }) else {
        return encode_metadata(stripped.map(apply_chief).as_ref());
    };
    let object = match stripped {
        None => Value::Object(Map::new()),
        Some(v @ Value::Object(_)) => v,
        Some(other) => serde_json::json!({ "priorNonObjectMetadata": other }),
    };
    let mut doc = apply_chief(object);
    let mut patch = Map::new();
    for (i, key) in DAEMON_OWNED_METADATA_KEYS.iter().enumerate() {
        let keep = i != 0 || keep_choice;
        let value = get_key(&stored_doc, key)
            .filter(|_| keep)
            .cloned()
            .unwrap_or(Value::Null);
        patch.insert((*key).to_owned(), value);
    }
    merge_patch(&mut doc, Value::Object(patch));
    encode_metadata(Some(&doc))
}

// ---- scope SQL --------------------------------------------------------------

/// [`crate::agent_repo::scope_predicate`] with Postgres placeholders: the
/// extra bind, when any, is `$3`.
fn scope_predicate(scope: &AgentListRowScope) -> (&'static str, Option<&str>) {
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
             AND NOT EXISTS (SELECT 1 FROM agent_session p \
             WHERE p.tenant_id = $1 AND p.id = s.parent_agent_id \
             AND p.workspace_id = s.workspace_id AND p.retired_at IS NULL)",
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

fn running_statuses() -> Result<Vec<String>> {
    AgentStatus::ALL
        .iter()
        .filter(|s| s.is_running_turn())
        .map(enum_to_db)
        .collect()
}

fn statuses(list: &[AgentStatus]) -> Result<Vec<String>> {
    list.iter().map(enum_to_db).collect()
}

/// Bind the 40 [`SESSION_COLUMNS`] values as `$2..$41`.
fn bind_session<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    s: &'q AgentSession,
    task_graph_enabled: bool,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    Ok(query
        .bind(&s.id.0)
        .bind(&s.workspace_id.0)
        .bind(s.backend_session_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.acp_session_id)
        .bind(&s.name)
        .bind(i64::from(s.name_explicitly_set))
        .bind(&s.model)
        .bind(&s.provider)
        .bind(enum_to_db(&s.status)?)
        .bind(i64::from(s.is_active))
        .bind(&s.system_prompt)
        .bind(&s.created_at)
        .bind(&s.updated_at)
        .bind(s.parent_agent_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.specialist)
        .bind(s.task_note_id.as_ref().map(|n| n.0.clone()))
        .bind(i64::from(s.skip_auto_commit))
        .bind(&s.completion_report)
        .bind(&s.completion_report_timestamp)
        .bind(&s.attention_request_kind)
        .bind(&s.attention_request_reason)
        .bind(&s.attention_request_timestamp)
        .bind(s.delegation_depth)
        .bind(&s.initial_message)
        .bind(json_col_to_db(s.context_references.as_ref())?)
        .bind(json_col_to_db(s.image_blocks.as_ref())?)
        .bind(json_col_to_db(s.file_blocks.as_ref())?)
        .bind(i64::from(s.is_background))
        .bind(encode_metadata(s.metadata.as_ref())?)
        .bind(&s.stop_reason)
        .bind(&s.stop_reason_timestamp)
        .bind(&s.reasoning_effort)
        .bind(effort_levels_to_db(s.effort_levels.as_ref())?)
        .bind(i64::from(task_graph_enabled))
        .bind(&s.harness_version)
        .bind(json_col_to_db(s.harness_features.as_ref())?)
        .bind(&s.retired_at)
        .bind(i64::from(s.notifications_muted))
        .bind(i64::from(s.pinned))
        .bind(&s.permission_policy))
}

fn decode_totals(raw: Option<String>) -> Option<TokenUsageTotals> {
    raw.and_then(|s| serde_json::from_str(&s).ok())
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Single-statement row lookups and existence checks used by several writers.
impl PgStore {
    async fn session_exists(
        &self,
        conn: &mut PgConnection,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_one(conn)
        .await
        .map_err(db)
    }

    /// The stored `metadata` of a session in `workspace_id`, `None` when the
    /// session is missing.
    async fn stored_metadata(
        &self,
        conn: &mut PgConnection,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<Option<Option<String>>> {
        sqlx::query_scalar(
            "SELECT metadata FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 FOR UPDATE",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(conn)
        .await
        .map_err(db)
    }

    async fn write_metadata(
        &self,
        tx: &mut WriteTx,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        metadata: Option<&str>,
        updated_at: Option<&str>,
    ) -> Result<u64> {
        Ok(sqlx::query(
            "UPDATE agent_session SET metadata = $4, updated_at = COALESCE($5, updated_at) \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(metadata)
        .bind(updated_at)
        .execute(&mut **tx)
        .await
        .map_err(db)?
        .rows_affected())
    }

    async fn summaries(&self, sql: &str, binds: &[&str]) -> Result<Vec<AgentSessionSummary>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_summary).collect()
    }

    async fn session_row(&self, id: &AgentId, columns: &str) -> Result<Option<PgRow>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {columns} FROM agent_session WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(row)
    }

    /// Insert one session row inside the caller's write transaction. The
    /// outer error is a bind failure, the inner one the database's.
    pub(super) async fn insert_session_row(
        &self,
        tx: &mut WriteTx,
        s: &AgentSession,
        task_graph_enabled: bool,
    ) -> Result<std::result::Result<(), sqlx::Error>> {
        let sql = format!(
            "INSERT INTO agent_session (tenant_id, {SESSION_COLUMNS}) VALUES ($1, \
             $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, \
             $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, \
             $36, $37, $38, $39, $40, $41)"
        );
        Ok(bind_session(
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(self.tenant()),
            s,
            task_graph_enabled,
        )?
        .execute(&mut **tx)
        .await
        .map(|_| ()))
    }

    async fn remember_specialist(&self, tx: &mut WriteTx, s: &AgentSession) -> Result<()> {
        sqlx::query(
            "INSERT INTO settings (tenant_id, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, key) DO UPDATE SET value = excluded.value",
        )
        .bind(self.tenant())
        .bind(crate::settings_repo::agent_creation_preferences_key(
            &s.workspace_id,
        ))
        .bind(serde_json::json!({ "specialistId": s.specialist }).to_string())
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }
}

impl PgStore {
    // ---- create and read ----------------------------------------------------

    pub async fn insert_agent_session(&self, s: &impl FullAgentSessionRow) -> Result<()> {
        self.insert_agent_session_with_preferences(s, false, false)
            .await
    }

    pub async fn insert_agent_session_with_task_graph(
        &self,
        s: &impl FullAgentSessionRow,
        task_graph_enabled: bool,
    ) -> Result<()> {
        self.insert_agent_session_with_preferences(s, task_graph_enabled, false)
            .await
    }

    pub async fn insert_agent_session_with_preferences(
        &self,
        s: &impl FullAgentSessionRow,
        task_graph_enabled: bool,
        remember_specialist: bool,
    ) -> Result<()> {
        let s = s.full_row();
        let mut tx = self.begin_write().await?;
        self.insert_session_row(&mut tx, s, task_graph_enabled)
            .await?
            .map_err(|e| {
                if e.as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                {
                    Error::Internal(format!(
                        "server-minted agent id {} collided with an existing session",
                        s.id
                    ))
                } else {
                    Error::Internal(format!("insert agent session failed: {e}"))
                }
            })?;
        if remember_specialist {
            self.remember_specialist(&mut tx, s).await?;
        }
        tx.commit().await.map_err(db)
    }

    pub async fn get_agent_session_without_messages(&self, id: &AgentId) -> Result<AgentSession> {
        match self.session_row(id, SESSION_COLUMNS).await? {
            Some(row) => map_session(&row, true),
            None => Err(not_found(id)),
        }
    }

    pub async fn get_agent_session_summary(&self, id: &AgentId) -> Result<AgentSessionSummary> {
        match self.session_row(id, SUMMARY_COLUMNS).await? {
            Some(row) => map_summary(&row),
            None => Err(not_found(id)),
        }
    }

    pub async fn get_agent_statuses(
        &self,
        ids_in: &[AgentId],
    ) -> Result<Vec<(AgentId, AgentStatus)>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, status FROM agent_session WHERE tenant_id = $1 AND id = ANY($2) \
             ORDER BY id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(ids(ids_in))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: String = col(row, "id")?;
            match enum_from_db::<AgentStatus>(&col::<String>(row, "status")?) {
                Ok(status) => out.push((AgentId(id), status)),
                Err(e) => tracing::warn!(
                    agent = %id,
                    error = %e,
                    "decode agent status failed; omitting agent from status batch"
                ),
            }
        }
        Ok(out)
    }

    pub async fn get_agent_session_task_graph_enabled(&self, id: &AgentId) -> Result<bool> {
        let columns = "COALESCE(sqlite_json_extract(harness_features, '$.taskGraph')::bigint, \
                       task_graph_enabled) AS enabled";
        let row = self
            .session_row(id, columns)
            .await?
            .ok_or_else(|| not_found(id))?;
        Ok(col::<i64>(&row, "enabled")? != 0)
    }

    pub async fn materialize_agent_session_harness_features(
        &self,
        id: &AgentId,
        snapshot: &Value,
    ) -> Result<Option<Value>> {
        let encoded = serde_json::to_string(snapshot)
            .map_err(|e| Error::Internal(format!("encode agentFeatures snapshot failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE agent_session SET harness_features = $3 \
             WHERE tenant_id = $1 AND id = $2 AND harness_features IS NULL",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&encoded)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let raw: Option<Option<String>> = sqlx::query_scalar(
            "SELECT harness_features FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        json_col_from_db(raw.ok_or_else(|| not_found(id))?, "harness_features")
    }

    pub async fn get_agent_session_status(&self, id: &AgentId) -> Result<AgentStatus> {
        let row = self
            .session_row(id, "status")
            .await?
            .ok_or_else(|| not_found(id))?;
        enum_from_db::<AgentStatus>(&col::<String>(&row, "status")?)
    }

    pub async fn get_agent_session_updated_at(&self, id: &AgentId) -> Result<String> {
        let row = self
            .session_row(id, "updated_at")
            .await?
            .ok_or_else(|| not_found(id))?;
        col(&row, "updated_at")
    }

    pub async fn get_agent_session_updated_at_batch(
        &self,
        ids_in: &[AgentId],
    ) -> Result<HashMap<AgentId, String>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, updated_at FROM agent_session WHERE tenant_id = $1 AND id = ANY($2)",
        )
        .bind(self.tenant())
        .bind(ids(ids_in))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| Ok((AgentId(col(row, "id")?), col(row, "updated_at")?)))
            .collect()
    }

    pub async fn get_agent_session_name(&self, id: &AgentId) -> Result<Option<String>> {
        self.session_row(id, "name")
            .await?
            .map(|row| col(&row, "name"))
            .transpose()
    }

    pub async fn list_agent_session_summaries(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSessionSummary>> {
        let sql = format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.summaries(&sql, &[workspace_id.0.as_str()]).await
    }

    pub async fn list_agent_session_summaries_by_workspace(
        &self,
        ids_in: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, Vec<AgentSessionSummary>>> {
        if ids_in.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = ANY($2) \
             ORDER BY workspace_id COLLATE \"C\", created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .bind(workspace_ids(ids_in))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let mut out: HashMap<WorkspaceId, Vec<AgentSessionSummary>> = HashMap::new();
        for row in &rows {
            let session = map_summary(row)?;
            out.entry(session.workspace_id.clone())
                .or_default()
                .push(session);
        }
        Ok(out)
    }

    pub async fn list_active_agent_session_summaries(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSessionSummary>> {
        let sql = format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = $2 AND retired_at IS NULL \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.summaries(&sql, &[workspace_id.0.as_str()]).await
    }

    pub async fn list_retired_agent_session_summaries(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSessionSummary>> {
        let sql = format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = $2 AND retired_at IS NOT NULL \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.summaries(&sql, &[workspace_id.0.as_str()]).await
    }

    pub async fn count_retired_agent_sessions(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = $2 AND retired_at IS NOT NULL",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    pub async fn list_scoped_agent_session_summaries(
        &self,
        workspace_id: &WorkspaceId,
        scope: &AgentListRowScope,
    ) -> Result<Vec<AgentSessionSummary>> {
        let (predicate, extra) = scope_predicate(scope);
        let sql = format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session s \
             WHERE s.tenant_id = $1 AND s.workspace_id = $2{predicate} \
             ORDER BY s.created_at COLLATE \"C\", s.row_seq"
        );
        let mut binds = vec![workspace_id.0.as_str()];
        binds.extend(extra);
        self.summaries(&sql, &binds).await
    }

    pub async fn count_agent_sessions_by_scope(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<AgentScopeCounts> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT \
                COUNT(*) FILTER (WHERE parent_agent_id IS NULL AND is_background = 0) AS top_level, \
                COUNT(*) FILTER (WHERE parent_agent_id IS NOT NULL) AS delegated, \
                COUNT(*) FILTER (WHERE parent_agent_id IS NULL AND is_background <> 0) AS background \
             FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2 AND retired_at IS NULL",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(AgentScopeCounts {
            top_level: count(&row, "top_level")?,
            delegated: count(&row, "delegated")?,
            background: count(&row, "background")?,
        })
    }

    pub async fn count_delegated_agent_sessions_by_parent(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<AgentDelegatedCounts> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT c.parent_agent_id, COUNT(*) AS total, \
                COUNT(*) FILTER (WHERE c.status = ANY($3)) AS running, \
                bool_or(p.id IS NULL) AS orphaned \
             FROM agent_session c \
             LEFT JOIN agent_session p ON p.tenant_id = $1 AND p.id = c.parent_agent_id \
                AND p.workspace_id = c.workspace_id AND p.retired_at IS NULL \
             WHERE c.tenant_id = $1 AND c.workspace_id = $2 AND c.retired_at IS NULL \
                AND c.parent_agent_id IS NOT NULL \
             GROUP BY c.parent_agent_id",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(running_statuses()?)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let mut counts = AgentDelegatedCounts::default();
        for row in &rows {
            let entry = AgentParentDelegatedCounts {
                total: count(row, "total")?,
                running: count(row, "running")?,
            };
            counts.running += entry.running;
            if col::<bool>(row, "orphaned")? {
                counts.orphaned.total += entry.total;
                counts.orphaned.running += entry.running;
            }
            counts
                .by_parent
                .insert(AgentId::from(col::<String>(row, "parent_agent_id")?), entry);
        }
        Ok(counts)
    }

    pub async fn list_all_agent_sessions(&self) -> Result<Vec<AgentSession>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SESSION_COLUMNS} FROM agent_session WHERE tenant_id = $1 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(|r| map_session(r, true)).collect()
    }

    // ---- usage ----------------------------------------------------------------

    pub async fn set_agent_session_token_usage(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        snapshot: &TokenUsageTotals,
    ) -> Result<()> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| Error::Internal(format!("encode session token_usage failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(resolved_model, ''), NULLIF(model, ''), 'unknown') AS model, \
                    token_usage, token_usage_baseline \
             FROM agent_session WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| not_found(id))?;
        let model: String = col(&row, "model")?;
        let previous = decode_totals(col(&row, "token_usage")?).unwrap_or_default();
        let baseline = decode_totals(col(&row, "token_usage_baseline")?);
        let has_materialized_totals: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_usage_cell WHERE tenant_id = $1 AND agent_id = $2 \
             AND (input_tokens <> 0 OR output_tokens <> 0 OR cache_read_tokens <> 0 OR \
                  cache_creation_tokens <> 0 OR thought_tokens <> 0 OR costs_json <> '{}'))",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !has_materialized_totals
            && (baseline.is_some() || previous != TokenUsageTotals::default())
        {
            let mut seed_costs = BTreeMap::<String, f64>::new();
            for cost in [
                baseline.as_ref().and_then(|t| t.cost.as_ref()),
                previous.cost.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                *seed_costs.entry(cost.currency.clone()).or_default() += cost.amount;
            }
            let seed_costs_json = serde_json::to_string(&seed_costs)
                .map_err(|e| Error::Internal(format!("encode legacy usage costs failed: {e}")))?;
            let b = baseline.clone().unwrap_or_default();
            sqlx::query(
                "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, input_tokens, \
                 output_tokens, cache_read_tokens, cache_creation_tokens, thought_tokens, costs_json) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
                 input_tokens = agent_usage_cell.input_tokens + excluded.input_tokens, \
                 output_tokens = agent_usage_cell.output_tokens + excluded.output_tokens, \
                 cache_read_tokens = agent_usage_cell.cache_read_tokens + excluded.cache_read_tokens, \
                 cache_creation_tokens = agent_usage_cell.cache_creation_tokens + excluded.cache_creation_tokens, \
                 thought_tokens = agent_usage_cell.thought_tokens + excluded.thought_tokens, \
                 costs_json = excluded.costs_json",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&model)
            .bind(to_i64(b.input_tokens.saturating_add(previous.input_tokens)))
            .bind(to_i64(b.output_tokens.saturating_add(previous.output_tokens)))
            .bind(to_i64(
                b.cache_read_tokens.saturating_add(previous.cache_read_tokens),
            ))
            .bind(to_i64(
                b.cache_creation_tokens
                    .saturating_add(previous.cache_creation_tokens),
            ))
            .bind(to_i64(b.thought_tokens.saturating_add(previous.thought_tokens)))
            .bind(seed_costs_json)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        let delta = |new: u64, old: u64| {
            if new >= old {
                to_i64(new - old)
            } else {
                -to_i64(old - new)
            }
        };
        let deltas = [
            delta(snapshot.input_tokens, previous.input_tokens),
            delta(snapshot.output_tokens, previous.output_tokens),
            delta(snapshot.cache_read_tokens, previous.cache_read_tokens),
            delta(
                snapshot.cache_creation_tokens,
                previous.cache_creation_tokens,
            ),
            delta(snapshot.thought_tokens, previous.thought_tokens),
        ];
        let cost_delta = match (previous.cost.as_ref(), snapshot.cost.as_ref()) {
            (Some(old), Some(new)) if old.currency == new.currency => new.amount - old.amount,
            (_, Some(new)) => new.amount,
            _ => 0.0,
        };
        let mut cost_corrections = BTreeMap::new();
        if let (Some(old), Some(new)) = (previous.cost.as_ref(), snapshot.cost.as_ref()) {
            if old.currency != new.currency {
                cost_corrections.insert(old.currency.clone(), old.amount.max(0.0));
            } else if cost_delta < 0.0 {
                cost_corrections.insert(old.currency.clone(), -cost_delta);
            }
        }
        self.subtract_usage_cell_corrections(&mut tx, id, &model, deltas, cost_corrections)
            .await?;
        let costs_raw: Option<String> = sqlx::query_scalar(
            "SELECT costs_json FROM agent_usage_cell \
             WHERE tenant_id = $1 AND agent_id = $2 AND model = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&model)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let mut costs: BTreeMap<String, f64> = costs_raw
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        if let Some(new) = snapshot.cost.as_ref().filter(|_| cost_delta > 0.0) {
            let total = costs.entry(new.currency.clone()).or_default();
            *total = (*total + cost_delta).max(0.0);
        }
        costs.retain(|_, amount| *amount > 0.0);
        let costs_json = serde_json::to_string(&costs)
            .map_err(|e| Error::Internal(format!("encode usage-cell costs failed: {e}")))?;
        let [input, output, cache_read, cache_creation, thought] = deltas.map(|v| v.max(0));
        sqlx::query(
            "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, input_tokens, \
             output_tokens, cache_read_tokens, cache_creation_tokens, thought_tokens, costs_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
             input_tokens = GREATEST(0, agent_usage_cell.input_tokens + $4), \
             output_tokens = GREATEST(0, agent_usage_cell.output_tokens + $5), \
             cache_read_tokens = GREATEST(0, agent_usage_cell.cache_read_tokens + $6), \
             cache_creation_tokens = GREATEST(0, agent_usage_cell.cache_creation_tokens + $7), \
             thought_tokens = GREATEST(0, agent_usage_cell.thought_tokens + $8), \
             costs_json = excluded.costs_json",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&model)
        .bind(input)
        .bind(output)
        .bind(cache_read)
        .bind(cache_creation)
        .bind(thought)
        .bind(costs_json)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "UPDATE agent_session SET token_usage = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(&json)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// `subtract_usage_cell_corrections` in `agent_repo.rs`.
    async fn subtract_usage_cell_corrections(
        &self,
        tx: &mut WriteTx,
        agent_id: &AgentId,
        model: &str,
        deltas: [i64; 5],
        mut cost_corrections: BTreeMap<String, f64>,
    ) -> Result<()> {
        let mut remaining = deltas.map(|delta| delta.saturating_neg().max(0));
        if remaining == [0; 5] && cost_corrections.is_empty() {
            return Ok(());
        }
        let rows = sqlx::query(
            "SELECT model, input_tokens, output_tokens, cache_read_tokens, \
             cache_creation_tokens, thought_tokens, costs_json FROM agent_usage_cell \
             WHERE tenant_id = $1 AND agent_id = $2 \
             ORDER BY (model = $3) DESC, model COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(model)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        for row in rows {
            let mut counters = [
                col::<i64>(&row, "input_tokens")?,
                col(&row, "output_tokens")?,
                col(&row, "cache_read_tokens")?,
                col(&row, "cache_creation_tokens")?,
                col(&row, "thought_tokens")?,
            ];
            let mut changed = false;
            for (counter, remainder) in counters.iter_mut().zip(&mut remaining) {
                let correction = (*counter).max(0).min(*remainder);
                *counter -= correction;
                *remainder -= correction;
                changed |= correction != 0;
            }
            let mut costs: BTreeMap<String, f64> =
                serde_json::from_str(&col::<String>(&row, "costs_json")?).unwrap_or_default();
            for (currency, remainder) in &mut cost_corrections {
                if let Some(amount) = costs.get_mut(currency) {
                    let correction = amount.max(0.0).min(*remainder);
                    *amount -= correction;
                    *remainder -= correction;
                    changed |= correction != 0.0;
                }
            }
            if changed {
                costs.retain(|_, amount| *amount > 0.0);
                let costs_json = serde_json::to_string(&costs).map_err(|e| {
                    Error::Internal(format!("encode usage corrections failed: {e}"))
                })?;
                sqlx::query(
                    "UPDATE agent_usage_cell SET input_tokens = $4, output_tokens = $5, \
                     cache_read_tokens = $6, cache_creation_tokens = $7, thought_tokens = $8, \
                     costs_json = $9 WHERE tenant_id = $1 AND agent_id = $2 AND model = $3",
                )
                .bind(self.tenant())
                .bind(&agent_id.0)
                .bind(col::<String>(&row, "model")?)
                .bind(counters[0])
                .bind(counters[1])
                .bind(counters[2])
                .bind(counters[3])
                .bind(counters[4])
                .bind(costs_json)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
            }
            if remaining == [0; 5] && cost_corrections.values().all(|amount| *amount == 0.0) {
                break;
            }
        }
        Ok(())
    }

    pub async fn set_agent_session_resolved_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_model: Option<&str>,
        resolved: Option<&str>,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let landed = sqlx::query(
            "UPDATE agent_session SET resolved_model = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND (CASE WHEN strpos(ltrim(model, ':'), ':') > 0 \
                       THEN nullif(substr(ltrim(model, ':'), strpos(ltrim(model, ':'), ':') + 1), '') \
                       ELSE nullif(ltrim(model, ':'), '') END) IS NOT DISTINCT FROM $5",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(resolved)
        .bind(expected_model)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        if let Some(resolved) = landed
            .then_some(resolved)
            .flatten()
            .filter(|model| !model.is_empty())
        {
            let prompt = sqlx::query(
                "SELECT m.id, m.usage_model, m.usage_origin FROM agent_message m \
                 JOIN agent_session s ON s.tenant_id = $1 AND s.id = m.agent_id \
                 WHERE m.tenant_id = $1 AND m.agent_id = $2 AND m.id = s.last_message_id \
                 AND m.role = 'user' AND m.usage_origin IN ('human', 'agent')",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
            if let Some(prompt) = prompt {
                let old_model: Option<String> = col(&prompt, "usage_model")?;
                if old_model.as_deref() != Some(resolved) {
                    let (human, agent) = if col::<String>(&prompt, "usage_origin")? == "human" {
                        (1_i64, 0_i64)
                    } else {
                        (0, 1)
                    };
                    sqlx::query(
                        "UPDATE agent_message SET usage_model = $3 \
                         WHERE tenant_id = $1 AND id = $2",
                    )
                    .bind(self.tenant())
                    .bind(col::<String>(&prompt, "id")?)
                    .bind(resolved)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                    sqlx::query(
                        "UPDATE agent_usage_cell SET \
                         human_messages = GREATEST(0, human_messages - $4), \
                         agent_messages = GREATEST(0, agent_messages - $5) \
                         WHERE tenant_id = $1 AND agent_id = $2 AND model = $3",
                    )
                    .bind(self.tenant())
                    .bind(&id.0)
                    .bind(&old_model)
                    .bind(human)
                    .bind(agent)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                    sqlx::query(
                        "INSERT INTO agent_usage_cell \
                         (tenant_id, agent_id, model, human_messages, agent_messages) \
                         VALUES ($1, $2, $3, $4, $5) \
                         ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
                         human_messages = agent_usage_cell.human_messages + excluded.human_messages, \
                         agent_messages = agent_usage_cell.agent_messages + excluded.agent_messages",
                    )
                    .bind(self.tenant())
                    .bind(&id.0)
                    .bind(resolved)
                    .bind(human)
                    .bind(agent)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                    sqlx::query(
                        "DELETE FROM agent_usage_cell WHERE tenant_id = $1 AND agent_id = $2 \
                         AND model = $3 AND input_tokens = 0 AND output_tokens = 0 \
                         AND cache_read_tokens = 0 AND cache_creation_tokens = 0 \
                         AND thought_tokens = 0 AND costs_json = '{}' \
                         AND human_messages = 0 AND agent_messages = 0",
                    )
                    .bind(self.tenant())
                    .bind(&id.0)
                    .bind(&old_model)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                }
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(landed)
    }

    pub async fn clear_agent_session_resolved_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE agent_session SET resolved_model = NULL \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    async fn scoped_session_row(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        columns: &str,
    ) -> Result<PgRow> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {columns} FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3"
        )))
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.ok_or_else(|| not_found(id))
    }

    pub async fn get_agent_session_last_turn_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<(Option<String>, Option<String>)> {
        let row = self
            .scoped_session_row(workspace_id, id, "last_turn_model, last_turn_provider")
            .await?;
        Ok(normalize_compound_model(
            col(&row, "last_turn_model")?,
            col(&row, "last_turn_provider")?,
        ))
    }

    pub async fn set_agent_session_last_turn_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: Option<&str>,
        provider: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE agent_session SET last_turn_model = $4, last_turn_provider = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(model)
        .bind(provider)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_agent_session_last_turn_effort(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<Option<AgentTurnEffort>> {
        let row = self
            .scoped_session_row(workspace_id, id, "last_turn_effort")
            .await?;
        col::<Option<String>>(&row, "last_turn_effort")?
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| Error::Internal(format!("decode last-turn effort failed: {e}")))
            })
            .transpose()
    }

    pub async fn set_agent_session_last_turn_effort(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        effort: &AgentTurnEffort,
    ) -> Result<()> {
        let json = serde_json::to_string(effort)
            .map_err(|e| Error::Internal(format!("encode last-turn effort failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE agent_session SET last_turn_effort = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(json)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    async fn identity_notices(&self, agent_id: &AgentId, limit: i64) -> Result<Vec<Value>> {
        let mut tx = self.begin().await?;
        let rows: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT metadata FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND role = 'system' \
               AND sqlite_json_extract(metadata, '$.type') \
                   IN ('model_changed', 'provider_rehomed', 'provider_failover', 'provider_returned') \
             ORDER BY seq DESC LIMIT $3",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .flatten()
            .filter_map(|m| serde_json::from_str::<Value>(&m).ok())
            .collect())
    }

    pub async fn recent_agent_identity_notices(
        &self,
        agent_id: &AgentId,
        limit: u32,
    ) -> Result<Vec<Value>> {
        self.identity_notices(agent_id, i64::from(limit)).await
    }

    pub async fn latest_agent_identity_notice(&self, agent_id: &AgentId) -> Result<Option<Value>> {
        let mut tx = self.begin().await?;
        let raw: Option<Option<String>> = sqlx::query_scalar(
            "SELECT metadata FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND role = 'system' \
               AND sqlite_json_extract(metadata, '$.type') \
                   IN ('model_changed', 'provider_rehomed', 'provider_failover', 'provider_returned') \
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(raw
            .flatten()
            .and_then(|m| serde_json::from_str::<Value>(&m).ok()))
    }

    pub async fn get_agent_session_token_usage(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<TokenUsageTotals>,
    )> {
        let row = self
            .scoped_session_row(
                workspace_id,
                id,
                "model, resolved_model, provider, token_usage",
            )
            .await?;
        let (model, provider) =
            normalize_compound_model(col(&row, "model")?, col(&row, "provider")?);
        let snapshot = col::<Option<String>>(&row, "token_usage")?
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| Error::Internal(format!("decode session token_usage failed: {e}")))
            })
            .transpose()?;
        Ok((model, col(&row, "resolved_model")?, provider, snapshot))
    }

    pub async fn get_workspace_agent_usage_data(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentUsageRow>> {
        let mut tx = self.begin().await?;
        let rows = self.agent_usage_rows(&mut tx, workspace_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    /// `fetch_agent_usage_rows` in `agent_repo.rs`, on the caller's connection.
    pub(super) async fn agent_usage_rows(
        &self,
        conn: &mut PgConnection,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentUsageRow>> {
        let session_rows = sqlx::query(
            "SELECT id, name, COALESCE(NULLIF(resolved_model, ''), model) AS model, \
             token_usage, token_usage_baseline FROM agent_session \
             WHERE tenant_id = $1 AND workspace_id = $2 ORDER BY row_seq",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *conn)
        .await
        .map_err(db)?;
        let mut result: Vec<AgentUsageRow> = Vec::with_capacity(session_rows.len());
        for row in &session_rows {
            let (model, _) = normalize_compound_model(col(row, "model")?, None);
            result.push((
                col(row, "id")?,
                model,
                decode_totals(col(row, "token_usage")?),
                decode_totals(col(row, "token_usage_baseline")?),
                Vec::new(),
                Vec::new(),
                col::<Option<String>>(row, "name")?.unwrap_or_default(),
            ));
        }
        let by_agent: HashMap<String, usize> = result
            .iter()
            .enumerate()
            .map(|(i, row)| (row.0.clone(), i))
            .collect();
        let cell_rows = sqlx::query(
            "SELECT c.agent_id, c.model, c.input_tokens, c.output_tokens, \
             c.cache_read_tokens, c.cache_creation_tokens, c.thought_tokens, \
             c.costs_json, c.human_messages, c.agent_messages \
             FROM agent_usage_cell c JOIN agent_session s ON s.tenant_id = $1 AND s.id = c.agent_id \
             WHERE c.tenant_id = $1 AND s.workspace_id = $2 \
             ORDER BY c.agent_id COLLATE \"C\", c.model COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *conn)
        .await
        .map_err(db)?;
        for cell in &cell_rows {
            let Some(&idx) = by_agent.get(&col::<String>(cell, "agent_id")?) else {
                continue;
            };
            let costs: BTreeMap<String, f64> = col::<String>(cell, "costs_json")?
                .parse::<Value>()
                .ok()
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();
            let cost = costs
                .into_iter()
                .max_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.0.cmp(&a.0))
                })
                .map(|(currency, amount)| UsageCost { amount, currency });
            let n = |name: &str| col::<i64>(cell, name).map(i64::cast_unsigned);
            result[idx].5.push(AgentUsageCellRow {
                model: col(cell, "model")?,
                reported_totals: TokenUsageTotals {
                    input_tokens: n("input_tokens")?,
                    output_tokens: n("output_tokens")?,
                    cache_read_tokens: n("cache_read_tokens")?,
                    cache_creation_tokens: n("cache_creation_tokens")?,
                    thought_tokens: n("thought_tokens")?,
                    cost,
                },
                human_messages: n("human_messages")?,
                agent_messages: n("agent_messages")?,
                message_usage: Vec::new(),
            });
        }
        let fallback: Vec<&str> = result
            .iter()
            .filter(|row| !intent_core::token_usage_reported(row.3.as_ref(), row.2.as_ref()))
            .map(|row| row.0.as_str())
            .collect();
        let message_rows = if fallback.is_empty() {
            Vec::new()
        } else {
            // jsonb orders the usage object's keys unlike SQLite; readers decode it.
            sqlx::query(
                "SELECT m.agent_id, \
                 COALESCE(NULLIF(m.usage_model, ''), NULLIF(s.model, ''), 'unknown') AS model, \
                 jsonb_build_object('usage', CASE WHEN sqlite_json_type(m.content, '$.usage') IS NOT NULL \
                     THEN intent_json(m.content) -> 'usage' \
                     ELSE intent_json(m.content) -> '_meta' -> 'usage' END)::text AS usage_json \
                 FROM agent_message m JOIN agent_session s ON s.tenant_id = $1 AND s.id = m.agent_id \
                 WHERE m.tenant_id = $1 AND m.agent_id = ANY($2) \
                 AND sqlite_json_valid(m.content) \
                 AND (CASE WHEN sqlite_json_type(m.content, '$.usage') IS NOT NULL \
                     THEN sqlite_json_type(m.content, '$.usage') = 'object' \
                     ELSE sqlite_json_type(m.content, '$._meta.usage') = 'object' END) \
                 ORDER BY m.agent_id COLLATE \"C\", m.seq",
            )
            .bind(self.tenant())
            .bind(&fallback)
            .fetch_all(&mut *conn)
            .await
            .map_err(db)?
        };
        for row in &message_rows {
            let Some(&idx) = by_agent.get(&col::<String>(row, "agent_id")?) else {
                continue;
            };
            let Some(value) = col::<Option<String>>(row, "usage_json")?
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            else {
                continue;
            };
            result[idx].4.push(value.clone());
            let model: String = col(row, "model")?;
            if let Some(cell) = result[idx].5.iter_mut().find(|c| c.model == model) {
                cell.message_usage.push(value);
            } else {
                result[idx].5.push(AgentUsageCellRow {
                    model,
                    reported_totals: TokenUsageTotals::default(),
                    human_messages: 0,
                    agent_messages: 0,
                    message_usage: vec![value],
                });
            }
        }
        Ok(result)
    }

    // ---- whole-row and preference writers -----------------------------------

    pub async fn update_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        s: &impl FullAgentSessionRow,
    ) -> Result<()> {
        self.update_agent_session_with_preferences(workspace_id, s, false, None, None, false)
            .await
    }

    pub async fn update_agent_session_with_preferences(
        &self,
        workspace_id: &WorkspaceId,
        s: &impl FullAgentSessionRow,
        remember_specialist: bool,
        notifications_muted: Option<bool>,
        pinned: Option<bool>,
        explicit_effort: bool,
    ) -> Result<()> {
        let s = s.full_row();
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT workspace_id, model, provider, acp_session_id, metadata FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
        )
        .bind(self.tenant())
        .bind(&s.id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| not_found(&s.id))?;
        if WorkspaceId(col(&row, "workspace_id")?) != *workspace_id {
            return Err(not_found(&s.id));
        }
        let stored_model: Option<String> = col(&row, "model")?;
        let stored_provider: Option<String> = col(&row, "provider")?;
        let (_, current_provider) =
            normalize_compound_model(stored_model.clone(), stored_provider.clone());
        let current_acp: Option<String> = col(&row, "acp_session_id")?;
        if (current_acp.is_some() || s.acp_session_id.is_some()) && s.provider != current_provider {
            return Err(Error::Internal(
                "agent provider is immutable once set (first real use)".to_string(),
            ));
        }
        if current_acp.is_some() && s.acp_session_id != current_acp {
            return Err(Error::Internal("acpSessionId is write-once".to_string()));
        }
        let keep_choice = stored_model == s.model && stored_provider == s.provider;
        let stored_metadata: Option<String> = col(&row, "metadata")?;
        // SQLite's chiefPromptVersion check runs json_extract on the stored row.
        extractable_metadata(stored_metadata.as_deref())?;
        let metadata = whole_metadata(
            stored_metadata.as_deref(),
            s.metadata.as_ref(),
            keep_choice,
            true,
        )?;
        let rows = sqlx::query(
            "UPDATE agent_session SET backend_session_id = $4, acp_session_id = $5, name = $6, \
             name_explicitly_set = $7, model = $8, provider = $9, status = $10, is_active = $11, \
             prompt_breakdown = CASE WHEN system_prompt IS NOT DISTINCT FROM $12 \
                 THEN prompt_breakdown ELSE NULL END, \
             system_prompt = $12, updated_at = $13, parent_agent_id = $14, specialist = $15, \
             task_note_id = $16, skip_auto_commit = $17, completion_report = $18, \
             completion_report_timestamp = $19, delegation_depth = $20, initial_message = $21, \
             context_references = $22, image_blocks = $23, file_blocks = $24, \
             is_background = $25, metadata = $26, stop_reason = $27, \
             stop_reason_timestamp = $28, \
             reasoning_effort_auto = CASE WHEN $29 THEN 0 \
                 WHEN reasoning_effort IS NOT DISTINCT FROM $30 THEN reasoning_effort_auto \
                 ELSE 0 END, \
             reasoning_effort = $30 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&s.id.0)
        .bind(&workspace_id.0)
        .bind(s.backend_session_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.acp_session_id)
        .bind(&s.name)
        .bind(i64::from(s.name_explicitly_set))
        .bind(&s.model)
        .bind(&s.provider)
        .bind(enum_to_db(&s.status)?)
        .bind(i64::from(s.is_active))
        .bind(&s.system_prompt)
        .bind(&s.updated_at)
        .bind(s.parent_agent_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.specialist)
        .bind(s.task_note_id.as_ref().map(|n| n.0.clone()))
        .bind(i64::from(s.skip_auto_commit))
        .bind(&s.completion_report)
        .bind(&s.completion_report_timestamp)
        .bind(s.delegation_depth)
        .bind(&s.initial_message)
        .bind(json_col_to_db(s.context_references.as_ref())?)
        .bind(json_col_to_db(s.image_blocks.as_ref())?)
        .bind(json_col_to_db(s.file_blocks.as_ref())?)
        .bind(i64::from(s.is_background))
        .bind(metadata)
        .bind(&s.stop_reason)
        .bind(&s.stop_reason_timestamp)
        .bind(explicit_effort)
        .bind(&s.reasoning_effort)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if rows == 0 {
            return Err(not_found(&s.id));
        }
        if let Some(value) = notifications_muted {
            self.write_preference(&mut tx, workspace_id, &s.id, "notifications_muted", value)
                .await?;
        }
        if let Some(value) = pinned {
            self.write_preference(&mut tx, workspace_id, &s.id, "pinned", value)
                .await?;
        }
        if remember_specialist {
            self.remember_specialist(&mut tx, s).await?;
        }
        tx.commit().await.map_err(db)
    }

    /// `SessionPreference::update_sql`: the rows changed (0 when unchanged).
    async fn write_preference(
        &self,
        tx: &mut WriteTx,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        column: &'static str,
        value: bool,
    ) -> Result<u64> {
        let sql = match column {
            "notifications_muted" => {
                "UPDATE agent_session SET notifications_muted = $4 \
                 WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
                 AND notifications_muted <> $4"
            }
            _ => {
                "UPDATE agent_session SET pinned = $4 \
                 WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND pinned <> $4"
            }
        };
        Ok(sqlx::query(sql)
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&workspace_id.0)
            .bind(i64::from(value))
            .execute(&mut **tx)
            .await
            .map_err(db)?
            .rows_affected())
    }

    async fn set_preference(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        column: &'static str,
        value: bool,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = self
            .write_preference(&mut tx, workspace_id, id, column, value)
            .await?;
        let exists = rows > 0 || self.session_exists(&mut tx, workspace_id, id).await?;
        tx.commit().await.map_err(db)?;
        if !exists {
            return Err(not_found(id));
        }
        Ok(rows > 0)
    }

    pub async fn set_agent_notifications_muted(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        muted: bool,
    ) -> Result<bool> {
        self.set_preference(workspace_id, id, "notifications_muted", muted)
            .await
    }

    pub async fn set_agent_pinned(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        pinned: bool,
    ) -> Result<bool> {
        self.set_preference(workspace_id, id, "pinned", pinned)
            .await
    }

    /// Run a conditional single-statement write `sql` (binding the tenant,
    /// `id`, `workspace_id`, then `binds`): `Ok(true)` when it changed a row,
    /// `Ok(false)` when the session exists but the condition did not hold,
    /// `NotFound` otherwise.
    async fn guarded_write(
        &self,
        mut tx: WriteTx,
        sql: &str,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        binds: &[Option<&str>],
    ) -> Result<bool> {
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&workspace_id.0);
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query.execute(&mut *tx).await.map_err(db)?.rows_affected();
        let exists = rows > 0 || self.session_exists(&mut tx, workspace_id, id).await?;
        tx.commit().await.map_err(db)?;
        if !exists {
            return Err(not_found(id));
        }
        Ok(rows > 0)
    }

    pub async fn set_agent_permission_policy(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        policy: Option<&str>,
    ) -> Result<bool> {
        let tx = self.begin_write().await?;
        self.guarded_write(
            tx,
            "UPDATE agent_session SET permission_policy = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND permission_policy IS DISTINCT FROM $4",
            workspace_id,
            id,
            &[policy],
        )
        .await
    }

    pub async fn set_agent_session_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: &str,
        provider: Option<&str>,
        updated_at: &str,
    ) -> Result<()> {
        self.set_agent_session_model_choice(workspace_id, id, model, provider, None, updated_at)
            .await
    }

    pub async fn set_agent_session_model_choice(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: &str,
        provider: Option<&str>,
        manual_choice: Option<&str>,
        updated_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let stored = self
            .stored_metadata(&mut tx, workspace_id, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        let metadata = match manual_choice {
            Some(choice) => {
                let mut doc = metadata_object(stored.as_deref())?;
                set_key(&mut doc, MANUAL_CHOICE_METADATA_KEY, Value::from(choice));
                Some(encode(doc)?)
            }
            None => match metadata_value(stored.as_deref()) {
                Some(Value::Object(mut doc))
                    if get_key(&Value::Object(doc.clone()), MANUAL_CHOICE_METADATA_KEY)
                        .is_some() =>
                {
                    remove_key(&mut doc, MANUAL_CHOICE_METADATA_KEY);
                    Some(encode(doc)?)
                }
                _ => stored,
            },
        };
        sqlx::query(
            "UPDATE agent_session SET \
               reasoning_effort = CASE WHEN reasoning_effort_auto = 1 \
                   AND model IS DISTINCT FROM $4 THEN NULL ELSE reasoning_effort END, \
               reasoning_effort_auto = CASE WHEN reasoning_effort_auto = 1 \
                   AND model IS DISTINCT FROM $4 THEN 0 ELSE reasoning_effort_auto END, \
               model = $4, provider = $5, updated_at = $6, metadata = $7 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(model)
        .bind(provider)
        .bind(updated_at)
        .bind(metadata)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn rehome_agent_session_provider(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_provider: Option<&str>,
        provider: &str,
        model: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        let tx = self.begin_write().await?;
        self.guarded_write(
            tx,
            "UPDATE agent_session SET provider = $5, model = $6, reasoning_effort = NULL, \
             reasoning_effort_auto = 0, updated_at = $7 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND provider IS NOT DISTINCT FROM $4",
            workspace_id,
            id,
            &[expected_provider, Some(provider), model, Some(updated_at)],
        )
        .await
    }

    /// The session's `(provider, model, metadata)` when it is in
    /// `workspace_id`, locked for the caller's write.
    async fn identity_row(
        &self,
        tx: &mut WriteTx,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<Option<(Option<String>, Option<String>, Option<String>)>> {
        sqlx::query_as(
            "SELECT provider, model, metadata FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 FOR UPDATE",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn failover_agent_session_provider(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_provider: &str,
        expected_model: Option<&str>,
        provider: &str,
        metadata_key: Option<(&str, &str)>,
        yield_to_manual_choice: bool,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let (stored_provider, stored_model, stored) = self
            .identity_row(&mut tx, workspace_id, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        let matches = stored_provider.as_deref() == Some(expected_provider)
            && stored_model.as_deref() == expected_model
            && !(yield_to_manual_choice && has_key(stored.as_deref(), MANUAL_CHOICE_METADATA_KEY));
        if !matches {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        }
        let metadata = match metadata_key {
            None => match metadata_value(stored.as_deref()) {
                Some(Value::Object(mut doc))
                    if get_key(&Value::Object(doc.clone()), MANUAL_CHOICE_METADATA_KEY)
                        .is_some() =>
                {
                    remove_key(&mut doc, MANUAL_CHOICE_METADATA_KEY);
                    Some(encode(doc)?)
                }
                _ => stored,
            },
            Some((key, value)) => {
                let mut doc = metadata_object(stored.as_deref())?;
                remove_key(&mut doc, MANUAL_CHOICE_METADATA_KEY);
                set_key(&mut doc, key, json_arg(value)?);
                Some(encode(doc)?)
            }
        };
        sqlx::query(
            "UPDATE agent_session SET provider = $4, metadata = $5, updated_at = $6 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(provider)
        .bind(metadata)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn take_agent_session_manual_choice(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        token: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let stored = self.stored_metadata(&mut tx, workspace_id, id).await?;
        let Some(Value::Object(mut doc)) = metadata_value(stored.flatten().as_deref()) else {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        };
        if !extract_is(
            Some(&Value::Object(doc.clone())),
            MANUAL_CHOICE_METADATA_KEY,
            Some(token),
        ) {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        }
        remove_key(&mut doc, MANUAL_CHOICE_METADATA_KEY);
        let rows = self
            .write_metadata(&mut tx, workspace_id, id, Some(&encode(doc)?), None)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn set_agent_session_metadata_key_json_if_identity(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_provider: &str,
        expected_model: Option<&str>,
        key: &str,
        value: &str,
        yield_to_manual_choice: bool,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let (stored_provider, stored_model, stored) = self
            .identity_row(&mut tx, workspace_id, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        if stored_provider.as_deref() != Some(expected_provider)
            || stored_model.as_deref() != expected_model
            || (yield_to_manual_choice && has_key(stored.as_deref(), MANUAL_CHOICE_METADATA_KEY))
        {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        }
        let mut doc = metadata_object(stored.as_deref())?;
        set_key(&mut doc, key, json_arg(value)?);
        self.write_metadata(
            &mut tx,
            workspace_id,
            id,
            Some(&encode(doc)?),
            Some(updated_at),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn pin_agent_session_provider_if_null(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        provider: &str,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let home: Option<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let home = home.ok_or_else(|| not_found(id))?;
        let rows = sqlx::query(
            "UPDATE agent_session SET provider = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND (provider IS NULL OR provider = '')",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(provider)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        if rows == 0 && home != workspace_id.0 {
            return Err(not_found(id));
        }
        Ok(rows > 0)
    }

    pub async fn fill_agent_session_identity_if_null(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        let model = model.filter(|m| !m.trim().is_empty());
        let reasoning_effort = reasoning_effort.filter(|e| !e.trim().is_empty());
        if model.is_none() && reasoning_effort.is_none() {
            return Ok(false);
        }
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE agent_session SET \
               model = CASE WHEN (model IS NULL OR model = '') AND $4::text IS NOT NULL \
                            THEN $4 ELSE model END, \
               reasoning_effort_auto = CASE WHEN (reasoning_effort IS NULL OR reasoning_effort = '') \
                            AND $5::text IS NOT NULL THEN 1 ELSE reasoning_effort_auto END, \
               reasoning_effort = CASE WHEN (reasoning_effort IS NULL OR reasoning_effort = '') \
                            AND $5::text IS NOT NULL THEN $5 ELSE reasoning_effort END, \
               updated_at = $6 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND ( \
               ((model IS NULL OR model = '') AND $4::text IS NOT NULL) OR \
               ((reasoning_effort IS NULL OR reasoning_effort = '') AND $5::text IS NOT NULL))",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(model)
        .bind(reasoning_effort)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn fill_agent_session_effort_if_identity(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        effort: &str,
        expected_provider: Option<&str>,
        expected_model: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        if effort.trim().is_empty() {
            return Ok(false);
        }
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE agent_session SET reasoning_effort = $4, reasoning_effort_auto = 1, \
             updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
               AND (reasoning_effort IS NULL OR TRIM(reasoning_effort) = '') \
               AND provider IS NOT DISTINCT FROM $6 AND model IS NOT DISTINCT FROM $7",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(effort)
        .bind(updated_at)
        .bind(expected_provider)
        .bind(expected_model)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn set_agent_session_system_prompt(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        system_prompt: &str,
    ) -> Result<()> {
        self.set_agent_session_system_prompt_with_breakdown(workspace_id, id, system_prompt, None)
            .await
    }

    pub async fn set_agent_session_system_prompt_with_breakdown(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        system_prompt: &str,
        breakdown: Option<&PromptBreakdown>,
    ) -> Result<()> {
        let breakdown = breakdown
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("serialize prompt breakdown failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE agent_session SET system_prompt = $4, prompt_breakdown = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(system_prompt)
        .bind(breakdown)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        if rows == 0 {
            return Err(not_found(id));
        }
        Ok(())
    }

    pub async fn get_agent_session_prompt_breakdown(
        &self,
        id: &AgentId,
        system_prompt: Option<&str>,
    ) -> Result<Option<PromptBreakdown>> {
        let mut tx = self.begin().await?;
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT CASE WHEN system_prompt IS NOT DISTINCT FROM $3 THEN prompt_breakdown END \
             FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(system_prompt)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let Some((stored,)) = row else {
            return Err(not_found(id));
        };
        Ok(stored.and_then(|json| match serde_json::from_str(&json) {
            Ok(breakdown) => Some(breakdown),
            Err(e) => {
                tracing::warn!(agent = %id, error = %e, "stored prompt breakdown does not parse");
                None
            }
        }))
    }

    pub async fn update_agent_session_metadata(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        metadata: Option<&Value>,
        updated_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let stored = self
            .stored_metadata(&mut tx, workspace_id, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        let doc = whole_metadata(stored.as_deref(), metadata, true, false)?;
        self.write_metadata(&mut tx, workspace_id, id, doc.as_deref(), Some(updated_at))
            .await?;
        tx.commit().await.map_err(db)
    }

    #[expect(
        clippy::option_option,
        reason = "same signature as the public Store method"
    )]
    pub async fn set_agent_session_metadata_key(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        key: &str,
        value: &str,
        expected: Option<Option<&str>>,
        updated_at: Option<&str>,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let Some(stored) = self.stored_metadata(&mut tx, workspace_id, id).await? else {
            return Err(not_found(id));
        };
        if let Some(expected) = expected {
            if !extract_is(
                extractable_metadata(stored.as_deref())?.as_ref(),
                key,
                expected,
            ) {
                tx.commit().await.map_err(db)?;
                return Ok(false);
            }
        }
        let mut doc = metadata_object(stored.as_deref())?;
        set_key(&mut doc, key, Value::from(value));
        self.write_metadata(&mut tx, workspace_id, id, Some(&encode(doc)?), updated_at)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn take_agent_session_metadata_key(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        key: &str,
    ) -> Result<Option<String>> {
        let mut tx = self.begin_write().await?;
        let stored = self
            .stored_metadata(&mut tx, workspace_id, id)
            .await?
            .flatten();
        let Some(Value::Object(mut doc)) = metadata_value(stored.as_deref()) else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        let Some(Value::String(value)) = get_key(&Value::Object(doc.clone()), key).cloned() else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        remove_key(&mut doc, key);
        self.write_metadata(&mut tx, workspace_id, id, Some(&encode(doc)?), None)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(Some(value))
    }

    pub async fn set_agent_session_metadata_key_json(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        key: &str,
        value: &str,
        updated_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let stored = self
            .stored_metadata(&mut tx, workspace_id, id)
            .await?
            .ok_or_else(|| not_found(id))?;
        let mut doc = metadata_object(stored.as_deref())?;
        set_key(&mut doc, key, json_arg(value)?);
        self.write_metadata(
            &mut tx,
            workspace_id,
            id,
            Some(&encode(doc)?),
            Some(updated_at),
        )
        .await?;
        tx.commit().await.map_err(db)
    }

    #[expect(
        clippy::option_option,
        reason = "same signature as the public Store method"
    )]
    pub async fn set_pending_questions_marker_if_unanswered(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected: Option<Option<&str>>,
        question: &AgentMessage,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let Some(stored) = self.stored_metadata(&mut tx, workspace_id, id).await? else {
            return Err(not_found(id));
        };
        if let Some(expected) = expected {
            if !extract_is(
                extractable_metadata(stored.as_deref())?.as_ref(),
                PENDING_QUESTIONS_MESSAGE_ID_KEY,
                expected,
            ) {
                tx.commit().await.map_err(db)?;
                return Ok(false);
            }
        }
        let answered: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_message \
             WHERE tenant_id = $1 AND agent_id = $2 AND seq > $3 AND role = 'user' \
               AND sqlite_json_extract(metadata, '$.type') = 'question_answers' \
               AND sqlite_json_extract(metadata, '$.answeredQuestionsMessageId') = $4)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(question.seq)
        .bind(&question.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if answered {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        }
        let mut doc = metadata_object(stored.as_deref())?;
        set_key(
            &mut doc,
            PENDING_QUESTIONS_MESSAGE_ID_KEY,
            Value::from(question.id.as_str()),
        );
        self.write_metadata(
            &mut tx,
            workspace_id,
            id,
            Some(&encode(doc)?),
            Some(updated_at),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    #[expect(
        clippy::option_option,
        reason = "same signature as the public Store method"
    )]
    pub async fn set_agent_session_status(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        status: AgentStatus,
        is_active: bool,
        updated_at: &str,
        stop_reason: Option<Option<String>>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let rows = match stop_reason {
            None => sqlx::query(
                "UPDATE agent_session SET status = $4, is_active = $5, updated_at = $6 \
                 WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&workspace_id.0)
            .bind(enum_to_db(&status)?)
            .bind(i64::from(is_active))
            .bind(updated_at)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected(),
            Some(reason) => {
                let stamp = reason.is_some().then_some(updated_at);
                sqlx::query(
                    "UPDATE agent_session SET status = $4, is_active = $5, updated_at = $6, \
                     stop_reason = $7, stop_reason_timestamp = $8 \
                     WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
                )
                .bind(self.tenant())
                .bind(&id.0)
                .bind(&workspace_id.0)
                .bind(enum_to_db(&status)?)
                .bind(i64::from(is_active))
                .bind(updated_at)
                .bind(reason)
                .bind(stamp)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected()
            }
        };
        tx.commit().await.map_err(db)?;
        if rows == 0 {
            return Err(not_found(id));
        }
        Ok(())
    }

    /// An unconditional single-statement write: `NotFound` when no row matched.
    async fn required_write(
        &self,
        tx: WriteTx,
        sql: &str,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        binds: &[Option<&str>],
    ) -> Result<()> {
        if self.guarded_write(tx, sql, workspace_id, id, binds).await? {
            Ok(())
        } else {
            Err(not_found(id))
        }
    }

    pub async fn refresh_agent_session_timestamp(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<()> {
        let tx = self.begin_write().await?;
        self.required_write(
            tx,
            "UPDATE agent_session SET updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
            workspace_id,
            id,
            &[Some(updated_at)],
        )
        .await
    }

    pub async fn get_agent_session_retired_at(&self, id: &AgentId) -> Result<Option<String>> {
        let row = self
            .session_row(id, "retired_at")
            .await?
            .ok_or_else(|| not_found(id))?;
        col(&row, "retired_at")
    }

    pub async fn set_agent_session_retired_at(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        retired_at: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        let sql = if retired_at.is_some() {
            "UPDATE agent_session SET retired_at = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND retired_at IS NULL"
        } else {
            "UPDATE agent_session SET retired_at = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND retired_at IS NOT NULL"
        };
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(sql)
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&workspace_id.0)
            .bind(retired_at)
            .bind(updated_at)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn clear_completion_report(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<bool> {
        let tx = self.begin_write().await?;
        self.guarded_write(
            tx,
            "UPDATE agent_session SET completion_report = NULL, \
             completion_report_timestamp = NULL, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND completion_report IS NOT NULL",
            workspace_id,
            id,
            &[Some(updated_at)],
        )
        .await
    }

    pub async fn set_attention_request(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        kind: &str,
        reason: &str,
        timestamp: &str,
    ) -> Result<()> {
        let tx = self.begin_write().await?;
        self.required_write(
            tx,
            "UPDATE agent_session SET attention_request_kind = $4, \
             attention_request_reason = $5, attention_request_timestamp = $6, updated_at = $6 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
            workspace_id,
            id,
            &[Some(kind), Some(reason), Some(timestamp)],
        )
        .await
    }

    pub async fn clear_attention_request(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<bool> {
        let tx = self.begin_write().await?;
        self.guarded_write(
            tx,
            "UPDATE agent_session SET attention_request_kind = NULL, \
             attention_request_reason = NULL, attention_request_timestamp = NULL, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND attention_request_kind IS NOT NULL",
            workspace_id,
            id,
            &[Some(updated_at)],
        )
        .await
    }

    pub async fn clear_blocker_request_if_matches(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        reason: &str,
        timestamp: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE agent_session SET attention_request_kind = NULL, \
             attention_request_reason = NULL, attention_request_timestamp = NULL, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND attention_request_kind = 'blocker' AND attention_request_reason = $5 \
             AND attention_request_timestamp IS NOT DISTINCT FROM $6",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(updated_at)
        .bind(reason)
        .bind(timestamp)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows != 0)
    }

    pub async fn set_agent_effort_levels(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        levels: Option<&[String]>,
        updated_at: &str,
    ) -> Result<bool> {
        let encoded = effort_levels_to_db(levels.map(<[String]>::to_vec).as_ref())?;
        let tx = self.begin_write().await?;
        self.guarded_write(
            tx,
            "UPDATE agent_session SET effort_levels = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND effort_levels IS DISTINCT FROM $4",
            workspace_id,
            id,
            &[encoded.as_deref(), Some(updated_at)],
        )
        .await
    }

    pub async fn reset_all_active_flags(&self) -> Result<usize> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE agent_session SET is_active = 0 WHERE tenant_id = $1 AND is_active = 1",
        )
        .bind(self.tenant())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(usize::try_from(rows).unwrap_or(usize::MAX))
    }

    pub async fn set_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        acp_session_id: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT workspace_id, acp_session_id FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let (home, current) = row.ok_or_else(|| not_found(id))?;
        if home != workspace_id.0 {
            return Err(not_found(id));
        }
        match current.as_deref() {
            Some(existing) if existing == acp_session_id => return Ok(()),
            Some(_) => return Err(Error::Internal("acpSessionId is write-once".to_string())),
            None => {}
        }
        sqlx::query(
            "UPDATE agent_session SET acp_session_id = $4 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(acp_session_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// `replace_acp_session_id` plus `write_acp_session_id_in_transaction` in
    /// `agent_repo.rs`, in one writer-locked transaction.
    pub async fn replace_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_old: &str,
        new_acp_session_id: &str,
    ) -> Result<String> {
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT workspace_id, acp_session_id, token_usage, token_usage_baseline \
             FROM agent_session WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| not_found(id))?;
        if col::<String>(&row, "workspace_id")? != workspace_id.0 {
            return Err(not_found(id));
        }
        let stored: Option<String> = col(&row, "acp_session_id")?;
        match stored.as_deref() {
            Some(existing) if existing != expected_old => {
                tx.commit().await.map_err(db)?;
                return Ok(existing.to_owned());
            }
            _ => {}
        }
        let snapshot = decode_totals(col(&row, "token_usage")?);
        let baseline = decode_totals(col(&row, "token_usage_baseline")?);
        let folded = match (&baseline, &snapshot) {
            (None, None) => None,
            (b, s) => {
                let b = b.clone().unwrap_or_default();
                let s = s.clone().unwrap_or_default();
                Some(TokenUsageTotals {
                    input_tokens: b.input_tokens.saturating_add(s.input_tokens),
                    output_tokens: b.output_tokens.saturating_add(s.output_tokens),
                    cache_read_tokens: b.cache_read_tokens.saturating_add(s.cache_read_tokens),
                    cache_creation_tokens: b
                        .cache_creation_tokens
                        .saturating_add(s.cache_creation_tokens),
                    thought_tokens: b.thought_tokens.saturating_add(s.thought_tokens),
                    cost: UsageCost::merge(b.cost.as_ref(), s.cost.as_ref()),
                })
            }
        };
        let folded_json = folded
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("encode token_usage_baseline failed: {e}")))?;
        sqlx::query(
            "UPDATE agent_session SET acp_session_id = $4, token_usage_baseline = $5, \
             token_usage = NULL WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(new_acp_session_id)
        .bind(folded_json)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(new_acp_session_id.to_owned())
    }

    // ---- child agents ---------------------------------------------------------

    pub async fn count_child_agents(&self, parent_agent_id: &AgentId) -> Result<ChildAgentCounts> {
        let terminal = statuses(&[
            AgentStatus::Completed,
            AgentStatus::Error,
            AgentStatus::Deleted,
        ])?;
        let running = statuses(&[
            AgentStatus::Pending,
            AgentStatus::Active,
            AgentStatus::Processing,
            AgentStatus::Waiting,
        ])?;
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT \
               COUNT(*) FILTER (WHERE status <> ALL($3) AND is_active = 1) AS active, \
               COUNT(*) FILTER (WHERE status <> ALL($3)) AS unsettled, \
               COUNT(*) FILTER (WHERE status = ANY($4)) AS running \
             FROM agent_session \
             WHERE tenant_id = $1 AND parent_agent_id = $2 AND retired_at IS NULL",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(terminal)
        .bind(running)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ChildAgentCounts {
            active: count(&row, "active")?,
            unsettled: count(&row, "unsettled")?,
            running: count(&row, "running")?,
        })
    }

    pub async fn list_child_agent_summaries(
        &self,
        parent_agent_id: &AgentId,
    ) -> Result<Vec<AgentSessionSummary>> {
        let sql = format!(
            "SELECT {SUMMARY_COLUMNS} FROM agent_session \
             WHERE tenant_id = $1 AND parent_agent_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.summaries(&sql, &[parent_agent_id.0.as_str()]).await
    }

    pub async fn count_unsettled_child_agents(&self, parent_agent_id: &AgentId) -> Result<u64> {
        Ok(self.count_child_agents(parent_agent_id).await?.unsettled)
    }

    // ---- interrupted agents ---------------------------------------------------

    pub async fn insert_interrupted_agent(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        prev_status: &str,
        interrupted_at: &str,
    ) -> Result<bool> {
        self.insert_interrupted_agent_with_reason(
            agent_id,
            workspace_id,
            prev_status,
            interrupted_at,
            None,
        )
        .await
    }

    pub async fn insert_interrupted_agent_with_reason(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        prev_status: &str,
        interrupted_at: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "INSERT INTO interrupted_agent \
             (tenant_id, agent_id, workspace_id, prev_status, interrupted_at, reason) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, agent_id) DO UPDATE SET \
                 prev_status = excluded.prev_status, \
                 interrupted_at = excluded.interrupted_at, \
                 reason = excluded.reason, \
                 resolution = 'pending', \
                 resolved_at = NULL",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(&workspace_id.0)
        .bind(prev_status)
        .bind(interrupted_at)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn list_interrupted_agents(&self) -> Result<Vec<InterruptedAgent>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{INTERRUPTED_SELECT} ORDER BY ia.row_seq"
        )))
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_interrupted).collect()
    }

    pub async fn get_interrupted_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<InterruptedAgent>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{INTERRUPTED_SELECT} AND ia.agent_id = $2"
        )))
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(map_interrupted).transpose()
    }

    pub async fn set_interrupted_resolution(
        &self,
        agent_id: &AgentId,
        resolution: &str,
        resolved_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE interrupted_agent SET resolution = $3, resolved_at = $4 \
             WHERE tenant_id = $1 AND agent_id = $2 AND resolution = 'pending'",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(resolution)
        .bind(resolved_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn reset_interrupted_resolution(&self, agent_id: &AgentId) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "UPDATE interrupted_agent SET resolution = 'pending', resolved_at = NULL \
             WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    // ---- persisted queues -------------------------------------------------------

    async fn insert_queue_rows(
        &self,
        tx: &mut WriteTx,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let payloads = rows
            .iter()
            .map(|r| {
                serde_json::to_string(&r.payload)
                    .map_err(|e| Error::Internal(format!("encode agent queue payload failed: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        sqlx::query(
            "INSERT INTO agent_queue (tenant_id, id, agent_id, position, payload, created_at, turn_id) \
             SELECT $1, u.id, $2, u.position, u.payload, u.created_at, u.turn_id \
             FROM unnest($3::text[], $4::bigint[], $5::text[], $6::text[], $7::text[]) \
                 AS u(id, position, payload, created_at, turn_id)",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.position).collect::<Vec<_>>())
        .bind(payloads)
        .bind(rows.iter().map(|r| r.created_at.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>())
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("bulk insert agent queue failed: {e}")))?;
        Ok(())
    }

    pub async fn replace_agent_queue(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        if let Some(row) = rows.iter().find(|r| r.agent_id != *agent_id) {
            return Err(Error::Internal(format!(
                "replace agent queue row {} belongs to agent {}, not {}",
                row.id, row.agent_id.0, agent_id.0
            )));
        }
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM agent_queue WHERE tenant_id = $1 AND agent_id = $2")
            .bind(self.tenant())
            .bind(&agent_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        self.insert_queue_rows(&mut tx, agent_id, rows).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn move_agent_queue(
        &self,
        from: &AgentId,
        to: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        if let Some(row) = rows.iter().find(|r| r.agent_id != *to) {
            return Err(Error::Internal(format!(
                "move agent queue row {} belongs to agent {}, not {}",
                row.id, row.agent_id.0, to.0
            )));
        }
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM agent_queue WHERE tenant_id = $1 AND agent_id IN ($2, $3)")
            .bind(self.tenant())
            .bind(&from.0)
            .bind(&to.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        self.insert_queue_rows(&mut tx, to, rows).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn load_all_agent_queues(&self) -> Result<Vec<AgentQueueRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT q.id, q.agent_id COLLATE \"C\" AS agent_id, q.position, q.payload, q.created_at, \
                    COALESCE(q.turn_id, q.id) AS turn_id \
             FROM agent_queue q JOIN agent_session s ON s.tenant_id = $1 AND s.id = q.agent_id \
             WHERE q.tenant_id = $1 AND NOT EXISTS (SELECT 1 FROM agent_queue_admission d \
                 WHERE d.tenant_id = $1 AND d.id = q.id AND d.phase = 'completed') \
             UNION ALL \
             SELECT d.id, d.agent_id COLLATE \"C\", -1, d.payload, d.created_at, d.turn_id \
             FROM agent_queue_admission d JOIN agent_session a ON a.tenant_id = $1 AND a.id = d.agent_id \
             WHERE d.tenant_id = $1 AND d.phase <> 'completed' AND NOT EXISTS (SELECT 1 \
                 FROM agent_queue q WHERE q.tenant_id = $1 AND q.id = d.id) \
             ORDER BY agent_id, position",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                let payload_raw: String = col(row, "payload")?;
                Ok(AgentQueueRow {
                    id: col(row, "id")?,
                    agent_id: AgentId(col(row, "agent_id")?),
                    position: col(row, "position")?,
                    payload: serde_json::from_str(&payload_raw).unwrap_or(Value::Null),
                    created_at: col(row, "created_at")?,
                    turn_id: col(row, "turn_id")?,
                })
            })
            .collect()
    }

    // ---- task agent links -------------------------------------------------------

    pub async fn upsert_task_agent_link(&self, link: &TaskAgentLink) -> Result<TaskAgentLink> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO task_agent_link \
             (tenant_id, workspace_id, note_id, task_key, task_text, agent_id, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, workspace_id, note_id, task_key) DO UPDATE SET \
             task_text = excluded.task_text, agent_id = excluded.agent_id, \
             created_at = excluded.created_at",
        )
        .bind(self.tenant())
        .bind(&link.workspace_id.0)
        .bind(link.note_id.as_str())
        .bind(&link.task_key)
        .bind(&link.task_text)
        .bind(&link.agent_id)
        .bind(link.created_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(link.clone())
    }

    pub async fn delete_task_agent_link(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        task_key: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "DELETE FROM task_agent_link \
             WHERE tenant_id = $1 AND workspace_id = $2 AND note_id = $3 AND task_key = $4",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(note_id.as_str())
        .bind(task_key)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows > 0)
    }

    pub async fn list_task_agent_links(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TaskAgentLink>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT workspace_id, note_id, task_key, task_text, agent_id, created_at \
             FROM task_agent_link WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at, note_id COLLATE \"C\", task_key COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|r| {
                Ok(TaskAgentLink {
                    workspace_id: WorkspaceId::from(col::<String>(r, "workspace_id")?),
                    note_id: NoteId::from(col::<String>(r, "note_id")?),
                    task_key: col(r, "task_key")?,
                    task_text: col(r, "task_text")?,
                    agent_id: col(r, "agent_id")?,
                    created_at: col(r, "created_at")?,
                })
            })
            .collect()
    }

    // ---- agent-flipped completions ------------------------------------------------

    pub async fn record_agent_flipped_completion(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        task_note_id: &NoteId,
        recorded_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_flipped_completion \
             (tenant_id, agent_id, workspace_id, task_note_id, recorded_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, agent_id, workspace_id, task_note_id) DO UPDATE SET \
             recorded_at = excluded.recorded_at",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(&workspace_id.0)
        .bind(&task_note_id.0)
        .bind(recorded_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "DELETE FROM agent_flipped_completion \
             WHERE tenant_id = $1 AND agent_id = $2 AND row_seq NOT IN (\
                SELECT row_seq FROM agent_flipped_completion \
                 WHERE tenant_id = $1 AND agent_id = $2 \
                 ORDER BY recorded_at COLLATE \"C\" DESC, row_seq DESC LIMIT $3)",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(AGENT_FLIPPED_COMPLETIONS_CAP)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn remove_agent_flipped_completions_for_task(
        &self,
        workspace_id: &WorkspaceId,
        task_note_id: &NoteId,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM agent_flipped_completion \
             WHERE tenant_id = $1 AND workspace_id = $2 AND task_note_id = $3",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&task_note_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn take_agent_flipped_completions(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<(WorkspaceId, NoteId)>> {
        let mut tx = self.begin_write().await?;
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "DELETE FROM agent_flipped_completion WHERE tenant_id = $1 AND agent_id = $2 \
             RETURNING workspace_id, task_note_id, recorded_at, row_seq",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let mut rows = rows;
        rows.sort_by(|a, b| (&a.2, a.3).cmp(&(&b.2, b.3)));
        Ok(rows
            .into_iter()
            .map(|(ws, note, _, _)| (WorkspaceId::from(ws), NoteId::from(note)))
            .collect())
    }

    pub async fn list_agent_flipped_completions(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<(WorkspaceId, NoteId)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT workspace_id, task_note_id FROM agent_flipped_completion \
             WHERE tenant_id = $1 AND agent_id = $2 \
             ORDER BY recorded_at COLLATE \"C\" ASC, row_seq ASC",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(ws, note)| (WorkspaceId::from(ws), NoteId::from(note)))
            .collect())
    }

    // ---- wake-delivery markers ------------------------------------------------------

    pub async fn record_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        delivered_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO advisory_wake_delivery \
             (tenant_id, parent_agent_id, child_agent_id, delivered_at) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, parent_agent_id, child_agent_id) DO UPDATE SET \
             delivered_at = excluded.delivered_at",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .bind(delivered_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn has_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<bool> {
        let mut tx = self.begin().await?;
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM advisory_wake_delivery \
             WHERE tenant_id = $1 AND parent_agent_id = $2 AND child_agent_id = $3)",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn clear_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM advisory_wake_delivery \
             WHERE tenant_id = $1 AND parent_agent_id = $2 AND child_agent_id = $3",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn clear_advisory_wake_deliveries_for_child(
        &self,
        child_agent_id: &AgentId,
    ) -> Result<()> {
        self.clear_advisory_wake_deliveries_for_children(std::slice::from_ref(child_agent_id))
            .await
    }

    pub async fn clear_advisory_wake_deliveries_for_children(
        &self,
        child_agent_ids: &[AgentId],
    ) -> Result<()> {
        if child_agent_ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM advisory_wake_delivery WHERE tenant_id = $1 AND child_agent_id = ANY($2)",
        )
        .bind(self.tenant())
        .bind(ids(child_agent_ids))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn record_completion_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: &str,
        delivered_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO completion_wake_delivery \
             (tenant_id, parent_agent_id, child_agent_id, completion_identity, delivered_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, parent_agent_id, child_agent_id) DO UPDATE SET \
             completion_identity = excluded.completion_identity, \
             delivered_at = excluded.delivered_at",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .bind(completion_identity)
        .bind(delivered_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_completion_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let identity = sqlx::query_scalar(
            "SELECT completion_identity FROM completion_wake_delivery \
             WHERE tenant_id = $1 AND parent_agent_id = $2 AND child_agent_id = $3",
        )
        .bind(self.tenant())
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(identity)
    }

    // ---- review-context enrollment ----------------------------------------------------

    /// `get_review_context_enrollment` in `review_context_enrollment.rs`.
    pub async fn get_review_context_enrollment(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
        session: &str,
    ) -> Result<Option<ReviewContextEnrollment>> {
        let unavailable = || Error::Internal("private native enrollment unavailable".into());
        if session.trim().is_empty() || session.len() > 256 || session.contains('\0') {
            return Err(unavailable());
        }
        let mut tx = self.begin().await.map_err(|_| unavailable())?;
        let row: Option<(String, i64)> = sqlx::query_as(
            "SELECT e.owner_key, e.version FROM review_context_enrollment e \
             JOIN agent_session s ON s.tenant_id = $1 AND s.id = e.agent_id \
                 AND s.workspace_id = e.workspace_id AND s.acp_session_id = e.acp_session_id \
             WHERE e.tenant_id = $1 AND e.agent_id = $2 AND e.workspace_id = $3 \
                 AND e.acp_session_id = $4",
        )
        .bind(self.tenant())
        .bind(&agent.0)
        .bind(&workspace.0)
        .bind(session)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| unavailable())?;
        let Some((key, version)) = row else {
            let private: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM review_context_private_family \
                     WHERE tenant_id = $1 AND acp_session_id = $2) \
                 OR EXISTS(SELECT 1 FROM pr_monitor_message_provenance \
                     WHERE tenant_id = $1 AND agent_id = $3)",
            )
            .bind(self.tenant())
            .bind(session)
            .bind(&agent.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| unavailable())?;
            tx.commit().await.map_err(|_| unavailable())?;
            return if private {
                Err(unavailable())
            } else {
                Ok(None)
            };
        };
        tx.commit().await.map_err(|_| unavailable())?;
        let uuid = uuid::Uuid::parse_str(&key).map_err(|_| unavailable())?;
        if version != 1 || uuid.get_version_num() != 4 || uuid.to_string() != key {
            return Err(unavailable());
        }
        Ok(Some(ReviewContextEnrollment::from_parts(
            workspace.clone(),
            agent.clone(),
            session.into(),
            key,
        )))
    }
}

// ---- session deletion and monitor wake messages ---------------------------

/// The child rows `SQLite` deletes before an agent's session row, then the
/// row itself; the rest cascade through the tenant-scoped foreign keys.
/// `cleanup_scope` also removes the agent's recovery history and private
/// desktop records (workspace deletion removes those for every agent first).
async fn delete_session_rows(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    id: &AgentId,
    cleanup_scope: bool,
) -> Result<u64> {
    for sql in [
        "DELETE FROM agent_message_payload WHERE tenant_id = $1 AND agent_id = $2",
        "DELETE FROM agent_message WHERE tenant_id = $1 AND agent_id = $2",
        "DELETE FROM agent_queue WHERE tenant_id = $1 AND agent_id = $2",
        "DELETE FROM hook WHERE tenant_id = $1 AND agent_id = $2",
        "DELETE FROM pr_monitor WHERE tenant_id = $1 AND agent_id = $2",
        "DELETE FROM completion_wake_delivery WHERE tenant_id = $1 \
         AND (parent_agent_id = $2 OR child_agent_id = $2)",
        "DELETE FROM advisory_wake_delivery WHERE tenant_id = $1 \
         AND (parent_agent_id = $2 OR child_agent_id = $2)",
    ] {
        sqlx::query(sql)
            .bind(tenant)
            .bind(&id.0)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
    }
    if cleanup_scope {
        sqlx::query(
            "DELETE FROM interrupted_agent \
             WHERE tenant_id = $1 AND agent_id = $2 AND workspace_id = $3",
        )
        .bind(tenant)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        super::desktop::delete_desktop_scope(tx, tenant, workspace_id, Some(id)).await?;
    }
    Ok(sqlx::query(
        "DELETE FROM agent_session WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
    )
    .bind(tenant)
    .bind(&id.0)
    .bind(&workspace_id.0)
    .execute(&mut **tx)
    .await
    .map_err(db)?
    .rows_affected())
}

/// Workspace deletion's per-agent step, inside its transaction.
pub(super) async fn delete_workspace_session(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
    id: &AgentId,
) -> Result<()> {
    delete_session_rows(tx, tenant, workspace_id, id, false)
        .await
        .map(drop)
}

/// A monitor wake message holding the tenant's writer lock until it
/// finishes, like `SQLite`'s reserved `BEGIN IMMEDIATE` writer.
pub(crate) struct PreparedPgMonitorMessage {
    tx: WriteTx,
    tenant: String,
    expected: sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments>,
    message: AgentMessage,
    draft: MonitorMessageDraft,
    usage_model: String,
    workspace_id: WorkspaceId,
    envelope_sha256: [u8; 32],
}

fn monitor_message_error(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("qualified monitor message write: {error}"))
}

impl PreparedPgMonitorMessage {
    pub(crate) async fn finish(self) -> Result<Option<AgentMessage>> {
        let Self {
            tx,
            tenant,
            expected,
            message,
            draft,
            usage_model,
            workspace_id,
            envelope_sha256,
        } = self;
        // Typed so the writer-lock lint sees the lock this message holds.
        let mut tx: WriteTx = tx;
        if expected
            .fetch_optional(&mut *tx)
            .await
            .map_err(monitor_message_error)?
            .is_none()
        {
            // Dropping the transaction rolls it back.
            return Ok(None);
        }
        let m = &message;
        super::agent_messages::insert_message_row(
            &mut tx,
            &tenant,
            &m.id,
            &m.agent_id.0,
            m.seq,
            "user",
            &draft.content.content_json,
            Some(&draft.metadata_json),
            &m.created_at,
            (&usage_model, Some(UsageMessageOrigin::Excluded.as_db())),
        )
        .await
        .map_err(monitor_message_error)?;
        super::agent_messages::insert_payload_rows(
            &mut tx,
            &tenant,
            &m.id,
            &m.agent_id.0,
            &draft.content.payload_rows,
        )
        .await?;
        sqlx::query(
            "INSERT INTO pr_monitor_message_provenance \
             (tenant_id, message_id, agent_id, workspace_id, binding_json, envelope_sha256, version) \
             VALUES ($1, $2, $3, $4, $5, $6, 1)",
        )
        .bind(&tenant)
        .bind(&m.id)
        .bind(&m.agent_id.0)
        .bind(&workspace_id.0)
        .bind(&draft.binding_json)
        .bind(envelope_sha256.as_slice())
        .execute(&mut *tx)
        .await
        .map_err(monitor_message_error)?;
        // As on SQLite: no preview projection of private monitor text.
        sqlx::query("UPDATE agent_session SET updated_at = $3 WHERE tenant_id = $1 AND id = $2")
            .bind(&tenant)
            .bind(&m.agent_id.0)
            .bind(&m.created_at)
            .execute(&mut *tx)
            .await
            .map_err(monitor_message_error)?;
        tx.commit().await.map_err(monitor_message_error)?;
        Ok(Some(message))
    }
}

impl PgStore {
    pub async fn delete_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<bool> {
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut tx = self.begin_write().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !exists {
            return Ok(false);
        }
        lifecycle.begin(&[crate::RepositoryLifecycleKey::Agent(id.clone())])?;
        let removed = delete_session_rows(&mut tx, self.tenant(), workspace_id, id, true).await?;
        tx.commit().await.map_err(db)?;
        lifecycle.settle_after_retirement().await?;
        Ok(removed > 0)
    }

    pub async fn prepare_monitor_message(
        &self,
        expected: &QualifiedPrMonitor,
        binding: &ReviewMonitorBinding,
        id: &str,
        text: &str,
        metadata: &ReviewMonitorWakeMetadata,
        created_at: &str,
    ) -> Result<PreparedMonitorMessage> {
        let draft = draft_monitor_message(expected, binding, id, text, metadata).await?;
        let monitor = expected.monitor();
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(s.resolved_model, ''), NULLIF(s.model, ''), 'unknown') AS model, \
             (SELECT COALESCE(MAX(m.seq), -1) + 1 FROM agent_message m \
              WHERE m.tenant_id = $1 AND m.agent_id = $2) AS next \
             FROM agent_session s WHERE s.tenant_id = $1 AND s.id = $2 \
             AND s.workspace_id = $3 AND s.retired_at IS NULL",
        )
        .bind(self.tenant())
        .bind(&monitor.agent_id.0)
        .bind(&monitor.workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(monitor_message_error)?
        .ok_or_else(|| Error::NotFound("original monitor destination unavailable".into()))?;
        let message = draft.message(id, &monitor.agent_id, col(&row, "next")?, created_at);
        let envelope_sha256 = crate::agent_repo::monitor_message_provenance::envelope_digest(
            &message,
            &monitor.workspace_id,
            binding,
        )?;
        Ok(PreparedMonitorMessage::postgres(PreparedPgMonitorMessage {
            tx,
            tenant: self.tenant().to_owned(),
            expected: super::pr_monitors::captured_monitor_exists(self.tenant(), expected),
            message,
            draft,
            usage_model: col(&row, "model")?,
            workspace_id: monitor.workspace_id.clone(),
            envelope_sha256,
        }))
    }
}
