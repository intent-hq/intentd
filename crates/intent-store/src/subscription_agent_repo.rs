//! Targeted list projections for completion-watch participants. Three batched
//! reads per chunk, independent of transcript size and workspace population.

use std::collections::HashMap;

use intent_core::{AgentId, AgentSession, Error, Result};
use serde_json::{json, Value};
use sqlx::Row;

use crate::agent_repo::{
    decode_last_tool_use_col, decode_preview_col, map_session_summary_row, SESSION_SUMMARY_COLUMNS,
};
use crate::{SessionMessageProjection, Store};

/// List-shaped inputs for one watched agent; no transcript or hook log bodies.
pub struct SubscriptionAgentProjection {
    /// Metadata-only session with its actual owning workspace.
    pub session: AgentSession,
    /// Persisted bounded previews and trigger-maintained message count.
    pub messages: SessionMessageProjection,
    /// Active hook identity and timing only.
    pub waiting_on_hooks: Vec<Value>,
    /// Active PR monitor identity and title only.
    pub waiting_on_pr_monitors: Vec<Value>,
}

// Below SQLite's bind-variable limit; chunking bounds each fetched batch.
const IDS_PER_STATEMENT: usize = 500;

fn session_sql(placeholders: &str) -> String {
    format!(
        "SELECT {SESSION_SUMMARY_COLUMNS}, message_count, last_assistant_preview, \
         last_user_preview, last_message_role, last_message_id, last_tool_use_preview \
         FROM agent_session WHERE id IN ({placeholders})"
    )
}

fn hooks_sql(placeholders: &str) -> String {
    format!(
        "SELECT agent_id, hook_id, name, next_run_at, expires_at \
         FROM hook INDEXED BY idx_hook_agent \
         WHERE agent_id IN ({placeholders}) AND state IN ('scheduled', 'running') \
         ORDER BY created_at"
    )
}

fn monitors_sql(placeholders: &str) -> String {
    format!(
        "SELECT agent_id, monitor_id, repo_owner, repo_name, pr_number, \
         CASE WHEN json_valid(last_snapshot) THEN \
         CASE WHEN json_type(last_snapshot, '$.title') = 'text' \
         THEN json_extract(last_snapshot, '$.title') END END AS title \
         FROM pr_monitor INDEXED BY idx_pr_monitor_agent \
         WHERE agent_id IN ({placeholders}) AND state = 'active' ORDER BY created_at"
    )
}

impl Store {
    /// Read only the requested IDs, including retired participants. Missing or
    /// undecodable session rows are omitted, like the best-effort status map.
    /// Heavy session prompt/image/initial-message columns and all message rows
    /// are excluded; hook code/log/state and PR snapshot bodies never leave SQL.
    ///
    /// # Errors
    /// Returns `Error::Internal` if a projected database read fails.
    pub async fn get_subscription_agent_projections(
        &self,
        ids: &[AgentId],
    ) -> Result<Vec<SubscriptionAgentProjection>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = session_sql(&placeholders);
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            let rows = query.fetch_all(self.read_pool()).await.map_err(|e| {
                Error::Internal(format!("subscription agent projection failed: {e}"))
            })?;
            let mut batch = HashMap::with_capacity(rows.len());
            for row in rows {
                let session = match map_session_summary_row(&row) {
                    Ok(session) => session,
                    Err(e) => {
                        tracing::warn!(agent = %row.get::<String, _>("id"), error = %e,
                            "decode subscription agent failed; omitting row");
                        continue;
                    }
                };
                let count: i64 = row.get("message_count");
                let messages = SessionMessageProjection {
                    message_count: count.max(0).cast_unsigned(),
                    last_assistant_text_blocks: decode_preview_col(
                        row.get("last_assistant_preview"),
                    ),
                    last_user_text_blocks: decode_preview_col(row.get("last_user_preview")),
                    last_message_role: row.get("last_message_role"),
                    last_message_id: row.get("last_message_id"),
                    last_tool_use: decode_last_tool_use_col(row.get("last_tool_use_preview")),
                };
                batch.insert(
                    session.id.0.clone(),
                    SubscriptionAgentProjection {
                        session,
                        messages,
                        waiting_on_hooks: Vec::new(),
                        waiting_on_pr_monitors: Vec::new(),
                    },
                );
            }
            let sql = hooks_sql(&placeholders);
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            for row in query
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("subscription hook projection failed: {e}")))?
            {
                if let Some(agent) = batch.get_mut(&row.get::<String, _>("agent_id")) {
                    let mut value = json!({"hookId": row.get::<String, _>("hook_id"), "name": row.get::<String, _>("name")});
                    for (column, field) in
                        [("next_run_at", "nextRunAt"), ("expires_at", "expiresAt")]
                    {
                        if let Some(text) = row.get::<Option<String>, _>(column) {
                            value[field] = json!(text);
                        }
                    }
                    agent.waiting_on_hooks.push(value);
                }
            }
            let sql = monitors_sql(&placeholders);
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            for row in query.fetch_all(self.read_pool()).await.map_err(|e| {
                Error::Internal(format!("subscription PR monitor projection failed: {e}"))
            })? {
                if let Some(agent) = batch.get_mut(&row.get::<String, _>("agent_id")) {
                    let mut value = json!({
                        "monitorId": row.get::<String, _>("monitor_id"),
                        "repo": format!("{}/{}", row.get::<String, _>("repo_owner"), row.get::<String, _>("repo_name")),
                        "prNumber": row.get::<i64, _>("pr_number"),
                    });
                    if let Some(title) = row.get::<Option<String>, _>("title") {
                        value["title"] = json!(title);
                    }
                    agent.waiting_on_pr_monitors.push(value);
                }
            }
            out.extend(batch.into_values());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plan checks exercise production SQL: indexed target IDs, no session or
    /// transcript scan, and no hook log/code or PR snapshot hydration.
    #[tokio::test]
    async fn subscription_projection_queries_use_target_indexes() {
        let dir = tempfile::Builder::new()
            .prefix("subscription-projections-")
            .tempdir()
            .unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        for (sql, index) in [
            (session_sql("?,?"), "sqlite_autoindex_agent_session_1"),
            (hooks_sql("?,?"), "idx_hook_agent"),
            (monitors_sql("?,?"), "idx_pr_monitor_agent"),
        ] {
            let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
                .bind("agent-a")
                .bind("agent-b")
                .fetch_all(store.read_pool())
                .await
                .unwrap();
            let details: Vec<String> = plan.iter().map(|row| row.get("detail")).collect();
            assert!(
                details
                    .iter()
                    .any(|line| line.contains("SEARCH") && line.contains(index)),
                "{details:?}"
            );
            assert!(
                !details.iter().any(|line| line.contains("SCAN ")),
                "{details:?}"
            );
            assert!(!sql.contains("agent_message"), "transcript access: {sql}");
        }
        assert!(store
            .get_subscription_agent_projections(&[])
            .await
            .unwrap()
            .is_empty());
    }
}
