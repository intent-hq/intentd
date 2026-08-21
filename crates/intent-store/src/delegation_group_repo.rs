//! Delegation-group repository: CRUD for persisted `after_all` fan-in state.
//!
//! Write-through on every group mutation in `agent_subscriptions.rs` so a
//! restarted daemon can rehydrate undelivered groups and deliver the single
//! aggregated parent wake — including summaries from children that completed
//! before the restart.

use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

/// Persisted delegation group row (mirrors the in-memory `DelegationGroup`).
#[derive(Debug, Clone)]
pub struct PersistedDelegationGroup {
    pub group_id: String,
    pub workspace_id: WorkspaceId,
    pub parent_agent_id: AgentId,
    pub await_mode: String,
    pub expected_agent_ids: Vec<AgentId>,
    pub completed_agent_ids: Vec<AgentId>,
    pub deleted_agent_ids: Vec<AgentId>,
    pub sealed: bool,
    pub delivered: bool,
    pub event_summaries: Vec<String>,
    /// JSON-serialized Event objects retained for the aggregated wake.
    pub raw_events_json: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Store {
    /// Insert or replace a `delegation_group` row (upsert).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_delegation_group(&self, g: &PersistedDelegationGroup) -> Result<()> {
        let expected_json = serde_json::to_string(&g.expected_agent_ids)
            .map_err(|e| Error::Internal(format!("encode expected_agent_ids: {e}")))?;
        let completed_json = serde_json::to_string(&g.completed_agent_ids)
            .map_err(|e| Error::Internal(format!("encode completed_agent_ids: {e}")))?;
        let deleted_json = serde_json::to_string(&g.deleted_agent_ids)
            .map_err(|e| Error::Internal(format!("encode deleted_agent_ids: {e}")))?;
        let summaries_json = serde_json::to_string(&g.event_summaries)
            .map_err(|e| Error::Internal(format!("encode event_summaries: {e}")))?;
        let raw_events_json = serde_json::to_string(&g.raw_events_json)
            .map_err(|e| Error::Internal(format!("encode raw_events: {e}")))?;

        sqlx::query(
            "INSERT INTO delegation_group (
                group_id, workspace_id, parent_agent_id, await_mode,
                expected_agent_ids, completed_agent_ids, deleted_agent_ids,
                sealed, delivered, event_summaries, raw_events,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(group_id) DO UPDATE SET
                expected_agent_ids = excluded.expected_agent_ids,
                completed_agent_ids = excluded.completed_agent_ids,
                deleted_agent_ids = excluded.deleted_agent_ids,
                sealed = excluded.sealed,
                delivered = excluded.delivered,
                event_summaries = excluded.event_summaries,
                raw_events = excluded.raw_events,
                updated_at = excluded.updated_at",
        )
        .bind(&g.group_id)
        .bind(&g.workspace_id.0)
        .bind(&g.parent_agent_id.0)
        .bind(&g.await_mode)
        .bind(&expected_json)
        .bind(&completed_json)
        .bind(&deleted_json)
        .bind(g.sealed as i64)
        .bind(g.delivered as i64)
        .bind(&summaries_json)
        .bind(&raw_events_json)
        .bind(&g.created_at)
        .bind(&g.updated_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert delegation_group failed: {e}")))?;
        Ok(())
    }

    /// Load all undelivered `delegation_group` rows for a workspace.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_undelivered_groups(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PersistedDelegationGroup>> {
        let rows = sqlx::query(
            "SELECT group_id, workspace_id, parent_agent_id, await_mode,
                    expected_agent_ids, completed_agent_ids, deleted_agent_ids,
                    sealed, delivered, event_summaries, raw_events,
                    created_at, updated_at
             FROM delegation_group
             WHERE workspace_id = ? AND delivered = 0
             ORDER BY created_at ASC",
        )
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list undelivered delegation groups: {e}")))?;

        rows.iter().map(decode_group_row).collect()
    }

    /// Delete a `delegation_group` row (called on delivery).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_delegation_group(&self, group_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM delegation_group WHERE group_id = ?")
            .bind(group_id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete delegation_group failed: {e}")))?;
        Ok(())
    }

    /// STAB-108: Get distinct workspace IDs that have undelivered delegation groups.
    /// Used for startup rehydration sweep.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspaces_with_undelivered_groups(&self) -> Result<Vec<WorkspaceId>> {
        let rows =
            sqlx::query("SELECT DISTINCT workspace_id FROM delegation_group WHERE delivered = 0")
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| {
                    Error::Internal(format!("list workspaces with undelivered groups: {e}"))
                })?;

        Ok(rows
            .iter()
            .filter_map(|row| match row.try_get::<String, _>("workspace_id") {
                Ok(ws_id) => Some(WorkspaceId::from(ws_id.as_str())),
                Err(e) => {
                    eprintln!("WARN: Skipping delegation_group row with decode error: {e}");
                    None
                }
            })
            .collect())
    }
}

fn decode_group_row(row: &sqlx::sqlite::SqliteRow) -> Result<PersistedDelegationGroup> {
    let expected_raw: String = row
        .try_get("expected_agent_ids")
        .map_err(|e| Error::Internal(format!("decode expected_agent_ids: {e}")))?;
    let expected_ids: Vec<String> = serde_json::from_str(&expected_raw)
        .map_err(|e| Error::Internal(format!("parse expected_agent_ids: {e}")))?;

    let completed_raw: String = row
        .try_get("completed_agent_ids")
        .map_err(|e| Error::Internal(format!("decode completed_agent_ids: {e}")))?;
    let completed_ids: Vec<String> = serde_json::from_str(&completed_raw)
        .map_err(|e| Error::Internal(format!("parse completed_agent_ids: {e}")))?;

    let deleted_raw: String = row
        .try_get("deleted_agent_ids")
        .map_err(|e| Error::Internal(format!("decode deleted_agent_ids: {e}")))?;
    let deleted_ids: Vec<String> = serde_json::from_str(&deleted_raw)
        .map_err(|e| Error::Internal(format!("parse deleted_agent_ids: {e}")))?;

    let summaries_raw: String = row
        .try_get("event_summaries")
        .map_err(|e| Error::Internal(format!("decode event_summaries: {e}")))?;
    let summaries: Vec<String> = serde_json::from_str(&summaries_raw)
        .map_err(|e| Error::Internal(format!("parse event_summaries: {e}")))?;

    let raw_events_raw: String = row
        .try_get("raw_events")
        .map_err(|e| Error::Internal(format!("decode raw_events: {e}")))?;
    let raw_events_json: Vec<String> = serde_json::from_str(&raw_events_raw)
        .map_err(|e| Error::Internal(format!("parse raw_events: {e}")))?;

    Ok(PersistedDelegationGroup {
        group_id: row
            .try_get("group_id")
            .map_err(|e| Error::Internal(format!("decode group_id: {e}")))?,
        workspace_id: WorkspaceId::from(
            row.try_get::<String, _>("workspace_id")
                .map_err(|e| Error::Internal(format!("decode workspace_id: {e}")))?
                .as_str(),
        ),
        parent_agent_id: AgentId::from(
            row.try_get::<String, _>("parent_agent_id")
                .map_err(|e| Error::Internal(format!("decode parent_agent_id: {e}")))?
                .as_str(),
        ),
        await_mode: row
            .try_get("await_mode")
            .map_err(|e| Error::Internal(format!("decode await_mode: {e}")))?,
        expected_agent_ids: expected_ids
            .into_iter()
            .map(|s| AgentId::from(s.as_str()))
            .collect(),
        completed_agent_ids: completed_ids
            .into_iter()
            .map(|s| AgentId::from(s.as_str()))
            .collect(),
        deleted_agent_ids: deleted_ids
            .into_iter()
            .map(|s| AgentId::from(s.as_str()))
            .collect(),
        sealed: row
            .try_get::<i64, _>("sealed")
            .map_err(|e| Error::Internal(format!("decode sealed: {e}")))?
            != 0,
        delivered: row
            .try_get::<i64, _>("delivered")
            .map_err(|e| Error::Internal(format!("decode delivered: {e}")))?
            != 0,
        event_summaries: summaries,
        raw_events_json,
        created_at: row
            .try_get("created_at")
            .map_err(|e| Error::Internal(format!("decode created_at: {e}")))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| Error::Internal(format!("decode updated_at: {e}")))?,
    })
}
