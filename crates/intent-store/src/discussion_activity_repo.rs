//! Durable per-recipient invocation state and compare-and-swap controls.
use crate::Store;
use intent_core::discussion::DiscussionAgentActivity;
use intent_core::{now_iso, AgentId, Error, Result, WorkspaceId};
use sqlx::{Row, SqliteConnection};

fn db(e: &sqlx::Error) -> Error {
    Error::Internal(format!("discussion activity: {e}"))
}

pub(super) async fn refresh(conn: &mut SqliteConnection, message: &str) -> Result<()> {
    sqlx::query("UPDATE workspace_discussion_message SET agent_activity_json=(SELECT json_group_array(json_object('agentId',agent_id,'attempt',attempt,'version',version,'state',state,'updatedAt',updated_at,'error',error)) FROM (SELECT * FROM discussion_agent_activity WHERE message_id=? ORDER BY agent_id)) WHERE id=?")
        .bind(message).bind(message).execute(conn).await.map_err(|e| db(&e))?;
    Ok(())
}

pub(super) async fn insert(
    conn: &mut SqliteConnection,
    message: &str,
    agent: &AgentId,
    now: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO discussion_agent_activity(message_id,agent_id,attempt,version,state,updated_at,turn_id) VALUES(?,?,1,1,'queued',?,?)")
        .bind(message).bind(agent.as_str()).bind(now).bind(format!("discussion:{message}:{}", agent.as_str()))
        .execute(conn).await.map_err(|e| db(&e))?;
    Ok(())
}

/// Internal receipt used to correlate queue work without exposing transcript data.
#[derive(Clone, Debug)]
pub struct DiscussionActivityReceipt {
    pub workspace_id: WorkspaceId,
    pub conversation_id: String,
    pub message_id: String,
    pub turn_id: String,
    pub activity: DiscussionAgentActivity,
}
fn receipt(row: &sqlx::sqlite::SqliteRow) -> Result<DiscussionActivityReceipt> {
    Ok(DiscussionActivityReceipt {
        workspace_id: WorkspaceId(row.get("workspace_id")),
        conversation_id: row.get("conversation_id"),
        message_id: row.get("message_id"),
        turn_id: row.get("turn_id"),
        activity: DiscussionAgentActivity {
            agent_id: AgentId(row.get("agent_id")),
            attempt: u32::try_from(row.get::<i64, _>("attempt"))
                .map_err(|e| Error::Internal(e.to_string()))?,
            version: row.get("version"),
            state: row.get("state"),
            updated_at: row.get("updated_at"),
            error: row.get("error"),
        },
    })
}
const SELECT: &str = "SELECT a.*,m.workspace_id,m.conversation_id FROM discussion_agent_activity a JOIN workspace_discussion_message m ON m.id=a.message_id";

impl Store {
    /// Look up one activity in its authorized conversation.
    /// # Errors
    /// Returns not-found or storage errors.
    pub async fn discussion_activity(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        agent: &AgentId,
    ) -> Result<DiscussionActivityReceipt> {
        let row=sqlx::query(&format!("{SELECT} WHERE m.workspace_id=? AND m.conversation_id=? AND a.message_id=? AND a.agent_id=?"))
            .bind(ws.as_str()).bind(conversation).bind(message).bind(agent.as_str()).fetch_optional(self.read_pool()).await.map_err(|e| db(&e))?
            .ok_or_else(|| Error::NotFound("Discussion invocation not found".into()))?;
        receipt(&row)
    }

    /// Transition only this live invocation; stale worker completions cannot rewrite retries.
    /// # Errors
    /// Returns storage errors.
    pub async fn transition_discussion_activity(
        &self,
        turn: &str,
        state: &str,
        error: Option<&str>,
    ) -> Result<Option<DiscussionActivityReceipt>> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        let changed=sqlx::query("UPDATE discussion_agent_activity SET state=?,error=?,updated_at=?,version=version+1 WHERE turn_id=? AND state IN ('queued','working') AND state<>?")
            .bind(state).bind(error).bind(now_iso()).bind(turn).bind(state).execute(&mut *tx).await.map_err(|e| db(&e))?.rows_affected();
        if changed == 0 {
            return Ok(None);
        }
        let row = sqlx::query(&format!("{SELECT} WHERE a.turn_id=?"))
            .bind(turn)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        let value = receipt(&row)?;
        refresh(&mut tx, &value.message_id).await?;
        tx.commit().await.map_err(|e| db(&e))?;
        Ok(Some(value))
    }

    /// Gate an exact invocation immediately before its provider work.
    /// # Errors
    /// Returns storage errors.
    pub async fn discussion_invocation_allowed(&self, turn: &str, agent: &AgentId) -> Result<bool> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_agent_activity WHERE turn_id=? AND agent_id=? AND state IN ('queued','working'))")
            .bind(turn).bind(agent.as_str()).fetch_one(self.read_pool()).await.map_err(|e| db(&e))
    }

    /// Stop or retry exactly the observed attempt, atomically with its outbox/queue.
    /// # Errors
    /// Returns stale/future request, eligibility, and storage errors.
    pub async fn control_discussion_activity(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        agent: &AgentId,
        attempt: u32,
        retry: bool,
    ) -> Result<(DiscussionActivityReceipt, bool)> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        let row=sqlx::query(&format!("{SELECT} WHERE m.workspace_id=? AND m.conversation_id=? AND a.message_id=? AND a.agent_id=?"))
            .bind(ws.as_str()).bind(conversation).bind(message).bind(agent.as_str()).fetch_optional(&mut *tx).await.map_err(|e| db(&e))?
            .ok_or_else(|| Error::NotFound("Discussion invocation not found".into()))?;
        let old = receipt(&row)?;
        if attempt == 0 || attempt > old.activity.attempt {
            return Err(Error::InvalidParams(
                "attempt does not identify an existing invocation".into(),
            ));
        }
        let actionable = if retry {
            matches!(old.activity.state.as_str(), "failed" | "stopped")
        } else {
            matches!(old.activity.state.as_str(), "queued" | "working")
        };
        if attempt != old.activity.attempt || !actionable {
            return Ok((old, false));
        }
        if retry
            && !super::discussion_agent_repo::agent_access_in(&mut tx, ws, conversation, agent)
                .await?
        {
            return Err(Error::NotFound("Discussion recipient not found".into()));
        }
        sqlx::query("DELETE FROM discussion_agent_delivery WHERE message_id=? AND agent_id=?")
            .bind(message)
            .bind(agent.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        sqlx::query("DELETE FROM agent_queue WHERE id=? AND agent_id=?")
            .bind(&old.turn_id)
            .bind(agent.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        let next = if retry {
            attempt
                .checked_add(1)
                .ok_or_else(|| Error::InvalidParams("attempt limit reached".into()))?
        } else {
            attempt
        };
        let turn = if retry {
            format!("discussion:{message}:{}:{next}", agent.as_str())
        } else {
            old.turn_id
        };
        sqlx::query("UPDATE discussion_agent_activity SET attempt=?,version=version+1,state=?,updated_at=?,error=NULL,turn_id=? WHERE message_id=? AND agent_id=?")
            .bind(i64::from(next)).bind(if retry {"queued"} else {"stopped"}).bind(now_iso()).bind(&turn).bind(message).bind(agent.as_str()).execute(&mut *tx).await.map_err(|e| db(&e))?;
        if retry {
            sqlx::query("INSERT INTO discussion_agent_delivery(message_id,agent_id,workspace_id) VALUES(?,?,?)").bind(message).bind(agent.as_str()).bind(ws.as_str()).execute(&mut *tx).await.map_err(|e| db(&e))?;
        }
        refresh(&mut tx, message).await?;
        let row = sqlx::query(&format!("{SELECT} WHERE a.turn_id=?"))
            .bind(&turn)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        let result = receipt(&row)?;
        tx.commit().await.map_err(|e| db(&e))?;
        Ok((result, true))
    }

    /// Interrupted processes cannot report success. Keep their receipts stopped,
    /// and require an explicit retry instead of replaying old invocation queues.
    /// # Errors
    /// Returns storage errors.
    pub async fn recover_discussion_activity(&self) -> Result<()> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        // Startup healing runs first. Do not let its generic continuation replay a
        // request whose correlated recovery now requires an explicit Retry.
        sqlx::query("UPDATE interrupted_agent SET resolution='abandoned',resolved_at=? WHERE resolution='pending' AND agent_id IN (SELECT agent_id FROM discussion_agent_activity WHERE state='working' OR (state='queued' AND NOT EXISTS(SELECT 1 FROM agent_queue q WHERE q.id=discussion_agent_activity.turn_id) AND NOT EXISTS(SELECT 1 FROM discussion_agent_delivery d WHERE d.message_id=discussion_agent_activity.message_id AND d.agent_id=discussion_agent_activity.agent_id)))")
            .bind(now_iso()).execute(&mut *tx).await.map_err(|e| db(&e))?;
        let messages:Vec<String>=sqlx::query_scalar("UPDATE discussion_agent_activity SET state='stopped',error='Interrupted by daemon restart.',updated_at=?,version=version+1 WHERE state='working' OR (state='queued' AND NOT EXISTS(SELECT 1 FROM agent_queue q WHERE q.id=discussion_agent_activity.turn_id) AND NOT EXISTS(SELECT 1 FROM discussion_agent_delivery d WHERE d.message_id=discussion_agent_activity.message_id AND d.agent_id=discussion_agent_activity.agent_id)) RETURNING message_id")
            .bind(now_iso()).fetch_all(&mut *tx).await.map_err(|e| db(&e))?;
        sqlx::query("DELETE FROM agent_queue WHERE id IN (SELECT turn_id FROM discussion_agent_activity WHERE state NOT IN ('queued','working'))").execute(&mut *tx).await.map_err(|e| db(&e))?;
        for message in messages {
            refresh(&mut tx, &message).await?;
        }
        tx.commit().await.map_err(|e| db(&e))?;
        Ok(())
    }
}
