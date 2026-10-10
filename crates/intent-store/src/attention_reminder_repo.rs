//! Internal durable reminder generations and per-person receipts, never public settings.
use std::collections::{BTreeSet, HashMap};

use intent_core::{AttentionReminderReason, Error, PrincipalId, Result, WorkspaceId};
use serde_json::Value;
use sqlx::Row;

use crate::Store;

pub(crate) fn reminder_prefix(id: &WorkspaceId) -> String {
    format!(
        "workspace.attentionReminder:{}:{}:",
        id.as_str().len(),
        id.as_str()
    )
}

pub(crate) async fn write_reminder_generation(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workspace: &WorkspaceId,
    reason: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(format!("{}{reason}", reminder_prefix(workspace)))
        .bind(serde_json::to_string(&uuid::Uuid::new_v4().to_string()).map_err(|e| Error::Internal(e.to_string()))?)
        .execute(&mut **tx).await.map_err(|e| Error::Internal(format!("save reminder generation: {e}")))?;
    Ok(())
}

impl Store {
    /// Read all internal reminder inputs in one statement for the requested workspace list.
    ///
    /// # Errors
    /// Returns an error on database failure or malformed internal state.
    pub async fn workspace_reminder_state(
        &self,
        workspaces: &[WorkspaceId],
        principal: Option<&PrincipalId>,
    ) -> Result<HashMap<WorkspaceId, HashMap<String, Value>>> {
        if workspaces.is_empty() {
            return Ok(HashMap::new());
        }
        let scopes: Vec<_> = workspaces.iter().map(|workspace| {
            serde_json::json!({"workspaceId":workspace.as_str(),"prefix":reminder_prefix(workspace)})
        }).collect();
        // Build exact keys from the requested workspaces and their currently relevant
        // sessions. Equality joins use the settings primary key; historical agent
        // generations and other people's receipts never enter this read.
        let rows = sqlx::query(
            "WITH scopes AS (
               SELECT json_extract(value,'$.workspaceId') AS workspace_id,
                      json_extract(value,'$.prefix') AS prefix FROM json_each(?)
             ), requested AS (
               SELECT workspace_id,prefix,prefix || 'review' AS key FROM scopes
               UNION ALL
               SELECT workspace_id,prefix,prefix || ? AS key FROM scopes
               UNION ALL
               SELECT scopes.workspace_id,scopes.prefix,scopes.prefix || 'discussion:' || a.id
               FROM scopes JOIN agent_session a ON a.workspace_id=scopes.workspace_id
               WHERE a.parent_agent_id IS NULL AND a.is_background=0
                 AND a.status<>'deleted' AND a.retired_at IS NULL AND a.notifications_muted=0
                 AND a.attention_request_kind IS NOT NULL AND a.attention_request_kind<>'blocker'
             )
             SELECT requested.workspace_id,requested.prefix,s.key,s.value
             FROM requested JOIN settings s ON s.key=requested.key",
        )
        .bind(serde_json::to_string(&scopes).map_err(|e| Error::Internal(e.to_string()))?)
        .bind(principal.map(|id| format!("receipt:{}", id.as_str())))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("read reminder state: {e}")))?;
        let mut result: HashMap<WorkspaceId, HashMap<String, Value>> = HashMap::new();
        for row in rows {
            let workspace = WorkspaceId::from(row.get::<String, _>("workspace_id"));
            let prefix: String = row.get("prefix");
            let key: String = row.get("key");
            let raw: String = row.get("value");
            let suffix = key
                .strip_prefix(&prefix)
                .ok_or_else(|| Error::Internal("invalid reminder state key".into()))?;
            let value = serde_json::from_str(&raw)
                .map_err(|e| Error::Internal(format!("decode reminder state: {e}")))?;
            result
                .entry(workspace)
                .or_default()
                .insert(suffix.to_string(), value);
        }
        Ok(result)
    }

    /// Atomically union exact observed reason versions into this person's receipt.
    ///
    /// # Errors
    /// Returns an error on database failure, missing workspace or malformed receipt.
    pub async fn acknowledge_workspace_reminders(
        &self,
        workspace: &WorkspaceId,
        principal: &PrincipalId,
        reasons: &[AttentionReminderReason],
    ) -> Result<bool> {
        if reasons.is_empty() {
            return Ok(false);
        }
        let key = format!(
            "{}receipt:{}",
            reminder_prefix(workspace),
            principal.as_str()
        );
        crate::with_write_txn_retry(|| async {
            let mut tx = self.write_pool().begin().await.map_err(|e| Error::Internal(e.to_string()))?;
            // Acquire the writer before reading the old receipt, so concurrent devices merge.
            sqlx::query("INSERT INTO settings (key,value) SELECT ?,'[]' WHERE EXISTS(SELECT 1 FROM workspace WHERE id=?) ON CONFLICT(key) DO NOTHING")
                .bind(&key).bind(workspace.as_str()).execute(&mut *tx).await.map_err(|e| Error::Internal(e.to_string()))?;
            let raw: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=? AND EXISTS(SELECT 1 FROM workspace WHERE id=?)")
                .bind(&key).bind(workspace.as_str()).fetch_optional(&mut *tx).await.map_err(|e| Error::Internal(e.to_string()))?;
            let raw = raw.ok_or_else(|| Error::NotFound(format!("workspace {workspace}")))?;
            let mut tokens: BTreeSet<AttentionReminderReason> = serde_json::from_str(&raw).map_err(|e| Error::Internal(format!("decode reminder receipt: {e}")))?;
            let before = tokens.len();
            tokens.extend(reasons.iter().cloned());
            let changed = tokens.len() != before;
            if changed {
                sqlx::query("UPDATE settings SET value=? WHERE key=?")
                    .bind(serde_json::to_string(&tokens).map_err(|e| Error::Internal(e.to_string()))?).bind(&key)
                    .execute(&mut *tx).await.map_err(|e| Error::Internal(e.to_string()))?;
            }
            tx.commit().await.map_err(|e| Error::Internal(e.to_string()))?;
            Ok(changed)
        }).await
    }
}
