//! Local addressing grants and transactional delivery into the ordinary queue.
use crate::{AgentQueueRow, Store};
use intent_core::discussion::{DiscussionMessage, DEFAULT_CONVERSATION};
use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::{Row, SqliteConnection};

fn db(error: &sqlx::Error) -> Error {
    Error::Internal(format!("discussion delivery: {error}"))
}

pub(super) async fn agent_eligible_in(
    conn: &mut SqliteConnection,
    ws: &WorkspaceId,
    agent: &AgentId,
) -> Result<bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session WHERE workspace_id=? AND id=? AND retired_at IS NULL AND status<>'deleted')")
        .bind(ws.as_str()).bind(agent.as_str()).fetch_one(conn).await.map_err(|e| db(&e))
}

pub(super) async fn agent_access_in(
    conn: &mut SqliteConnection,
    ws: &WorkspaceId,
    conversation: &str,
    agent: &AgentId,
) -> Result<bool> {
    if conversation != DEFAULT_CONVERSATION {
        return Ok(false);
    }
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_agent_grant g JOIN agent_session a ON a.id=g.agent_id AND a.workspace_id=g.workspace_id WHERE g.workspace_id=? AND g.conversation_id=? AND g.agent_id=? AND a.retired_at IS NULL AND a.status<>'deleted')")
        .bind(ws.as_str()).bind(conversation).bind(agent.as_str()).fetch_one(conn).await.map_err(|e| db(&e))
}

/// A pending addressed message. Its immutable content is owned by discussion history.
pub struct DiscussionDelivery {
    pub message: DiscussionMessage,
    pub agent_id: AgentId,
    pub turn_id: String,
    pub attempt: i64,
    pub updated_at: String,
}

impl Store {
    /// Check the current agent and local explicit-address grant.
    /// # Errors
    /// Returns database errors.
    pub async fn discussion_agent_access(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        agent: &AgentId,
    ) -> Result<bool> {
        let mut conn = self.read_pool().acquire().await.map_err(|e| db(&e))?;
        agent_access_in(&mut conn, ws, conversation, agent).await
    }

    /// Read bounded pending deliveries, optionally for a single committed message.
    /// # Errors
    /// Returns database or snapshot decoding errors.
    pub async fn discussion_pending_deliveries(
        &self,
        message: Option<&str>,
    ) -> Result<Vec<DiscussionDelivery>> {
        let rows = if let Some(message) = message {
            sqlx::query("SELECT m.*,d.agent_id AS recipient_agent_id,a.turn_id AS invocation_turn_id,a.attempt AS invocation_attempt,a.updated_at AS invocation_updated_at FROM discussion_agent_delivery d JOIN workspace_discussion_message m ON m.id=d.message_id JOIN discussion_agent_activity a ON a.message_id=d.message_id AND a.agent_id=d.agent_id WHERE d.message_id=? ORDER BY d.agent_id LIMIT 100")
                .bind(message).fetch_all(self.read_pool()).await.map_err(|e| db(&e))?
        } else {
            sqlx::query("SELECT m.*,d.agent_id AS recipient_agent_id,a.turn_id AS invocation_turn_id,a.attempt AS invocation_attempt,a.updated_at AS invocation_updated_at FROM discussion_agent_delivery d JOIN workspace_discussion_message m ON m.id=d.message_id JOIN discussion_agent_activity a ON a.message_id=d.message_id AND a.agent_id=d.agent_id ORDER BY d.message_id,d.agent_id LIMIT 100")
                .fetch_all(self.read_pool()).await.map_err(|e| db(&e))?
        };
        rows.iter()
            .map(|row| {
                Ok(DiscussionDelivery {
                    message: super::discussion_repo::message(row)?,
                    agent_id: AgentId(row.get("recipient_agent_id")),
                    turn_id: row.get("invocation_turn_id"),
                    attempt: row.get("invocation_attempt"),
                    updated_at: row.get("invocation_updated_at"),
                })
            })
            .collect()
    }

    /// Atomically hand a pending delivery to the normal durable queue. The caller
    /// holds the queue persistence gate and publishes in memory after this commit.
    /// Deleted/retired recipients are discarded; they must not be resurrected.
    /// # Errors
    /// Returns database errors; a failed handoff leaves the delivery pending.
    pub async fn admit_discussion_delivery(
        &self,
        message: &str,
        row: &AgentQueueRow,
    ) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        let ws: Option<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM discussion_agent_delivery WHERE message_id=? AND agent_id=?",
        )
        .bind(message)
        .bind(row.agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db(&e))?;
        let Some(ws) = ws else {
            return Ok(false);
        };
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_agent_activity WHERE turn_id=? AND state='queued')").bind(&row.turn_id).fetch_one(&mut *tx).await.map_err(|e| db(&e))?;
        let eligible = active
            && agent_access_in(
                &mut tx,
                &WorkspaceId(ws),
                DEFAULT_CONVERSATION,
                &row.agent_id,
            )
            .await?;
        if eligible {
            sqlx::query("INSERT INTO agent_queue(id,agent_id,position,payload,created_at,turn_id) VALUES(?,?,(SELECT coalesce(max(position),-1)+1 FROM agent_queue WHERE agent_id=?),?,?,?)")
                .bind(&row.id).bind(row.agent_id.as_str()).bind(row.agent_id.as_str()).bind(row.payload.to_string()).bind(&row.created_at).bind(&row.turn_id)
                .execute(&mut *tx).await.map_err(|e| db(&e))?;
        }
        sqlx::query("DELETE FROM discussion_agent_delivery WHERE message_id=? AND agent_id=?")
            .bind(message)
            .bind(row.agent_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        tx.commit().await.map_err(|e| db(&e))?;
        Ok(eligible)
    }
}
