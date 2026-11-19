//! Labs discussions (Postgres): history paging, conversations, private
//! participants, agent grants and deliveries, invocation activity, reactions
//! and management. Mirrors `discussion_repo.rs`,
//! `discussion_conversation_repo.rs`, `discussion_agent_repo.rs`,
//! `discussion_activity_repo.rs`, `discussion_reaction_repo.rs` and
//! `discussion_management_repo.rs`.
//!
//! Text ordering and cursors use `COLLATE "C"` to match `SQLite`'s byte order.

use intent_core::discussion::{
    DiscussionActor, DiscussionAgentActivity, DiscussionAuthor, DiscussionConversation,
    DiscussionMessage, DiscussionPage, DiscussionReaction, DiscussionReactionState,
    DiscussionReply, DiscussionSendResult, DiscussionTarget, DEFAULT_CONVERSATION,
    DISCUSSION_PAGE_MAX, DISCUSSION_REACTIONS, DISCUSSION_REACTOR_MAX, DISCUSSION_RECIPIENT_MAX,
};
use intent_core::{now_iso, AgentId, Error, PrincipalId, Result, WorkspaceId};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::Row;

use super::read_seam::probe::between;
use super::{db, PgStore, WriteTx};
use crate::discussion_activity_repo::DiscussionActivityReceipt;
use crate::discussion_agent_repo::DiscussionDelivery;
use crate::AgentQueueRow;

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn json_err(e: impl std::fmt::Display) -> Error {
    Error::Internal(e.to_string())
}

fn to_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(json_err)
}

fn missing_discussion() -> Error {
    Error::NotFound("Discussion not found".into())
}

fn missing_message() -> Error {
    Error::NotFound("Discussion message not found".into())
}

fn missing_invocation() -> Error {
    Error::NotFound("Discussion invocation not found".into())
}

fn message(row: &PgRow) -> Result<DiscussionMessage> {
    let mut reactions: Vec<DiscussionReaction> =
        serde_json::from_str(&col::<String>(row, "reactions_json")?).map_err(json_err)?;
    let selected: Vec<String> = match row.try_get::<Option<String>, _>("viewer_reactions") {
        Ok(Some(text)) => serde_json::from_str(&text).map_err(json_err)?,
        _ => Vec::new(),
    };
    for reaction in &mut reactions {
        reaction.reacted = selected.contains(&reaction.reaction);
    }
    Ok(DiscussionMessage {
        id: col(row, "id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        conversation_id: col(row, "conversation_id")?,
        sequence: col(row, "sequence")?,
        body: col(row, "body")?,
        recipient_agent_ids: serde_json::from_str(&col::<String>(row, "recipient_agent_ids_json")?)
            .map_err(json_err)?,
        agent_mentions: serde_json::from_str(&col::<String>(row, "agent_mentions_json")?)
            .map_err(json_err)?,
        principal_mentions: serde_json::from_str(&col::<String>(row, "principal_mentions_json")?)
            .map_err(json_err)?,
        agent_activity: serde_json::from_str(&col::<String>(row, "agent_activity_json")?)
            .map_err(json_err)?,
        author: serde_json::from_str(&col::<String>(row, "author_json")?)
            .map_err(|e| Error::Internal(format!("discussion author: {e}")))?,
        created_at: col(row, "created_at")?,
        reply_to_message_id: col(row, "reply_to_message_id")?,
        reply_to: col::<Option<String>>(row, "reply_to_json")?
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(json_err)?,
        reactions,
        reaction_version: col(row, "reaction_version")?,
    })
}

fn profile(row: &PgRow) -> Result<DiscussionAuthor> {
    Ok(DiscussionAuthor {
        principal_id: Some(PrincipalId(col(row, "id")?)),
        agent_id: None,
        login: col(row, "login")?,
        display_name: col(row, "display_name")?,
        avatar_url: col(row, "avatar_url")?,
    })
}

fn conversation(row: &PgRow) -> Result<DiscussionConversation> {
    Ok(DiscussionConversation {
        name: col(row, "name")?,
        id: col(row, "id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        kind: col(row, "kind")?,
        participants: serde_json::from_str(&col::<String>(row, "participants_json")?)
            .map_err(json_err)?,
        created_at: col(row, "created_at")?,
    })
}

fn receipt(row: &PgRow) -> Result<DiscussionActivityReceipt> {
    Ok(DiscussionActivityReceipt {
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        conversation_id: col(row, "conversation_id")?,
        message_id: col(row, "message_id")?,
        turn_id: col(row, "turn_id")?,
        activity: DiscussionAgentActivity {
            agent_id: AgentId(col(row, "agent_id")?),
            attempt: u32::try_from(col::<i64>(row, "attempt")?).map_err(json_err)?,
            version: col(row, "version")?,
            state: col(row, "state")?,
            updated_at: col(row, "updated_at")?,
            error: col(row, "error")?,
        },
    })
}

/// An activity with its message's workspace and conversation; callers append
/// `AND …` conditions starting at `$2`.
const ACTIVITY: &str = "SELECT a.*, m.workspace_id, m.conversation_id \
    FROM discussion_agent_activity a \
    JOIN workspace_discussion_message m ON m.tenant_id = $1 AND m.id = a.message_id \
    WHERE a.tenant_id = $1";

async fn read_conversation(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    id: &str,
) -> Result<DiscussionConversation> {
    read_conversation_probed(c, tenant, ws, id, None).await
}

/// [`read_conversation`]; a reader names itself in `probe` for the test seam
/// that pauses it after the workspace-conversation lookup misses.
async fn read_conversation_probed(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    id: &str,
    probe: Option<&'static str>,
) -> Result<DiscussionConversation> {
    if let Some(row) = sqlx::query(
        "SELECT name, created_at, deleted_at FROM workspace_discussion_conversation \
         WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
    )
    .bind(tenant)
    .bind(ws.as_str())
    .bind(id)
    .fetch_optional(&mut *c)
    .await
    .map_err(db)?
    {
        if col::<Option<String>>(&row, "deleted_at")?.is_some() {
            return Err(missing_discussion());
        }
        return Ok(DiscussionConversation {
            id: id.into(),
            workspace_id: ws.clone(),
            kind: "workspace".into(),
            name: col(&row, "name")?,
            participants: vec![],
            created_at: col(&row, "created_at")?,
        });
    }
    if let Some(reader) = probe {
        between(tenant, reader).await;
    }
    if id == DEFAULT_CONVERSATION {
        let created_at: String =
            sqlx::query_scalar("SELECT created_at FROM workspace WHERE tenant_id = $1 AND id = $2")
                .bind(tenant)
                .bind(ws.as_str())
                .fetch_optional(&mut *c)
                .await
                .map_err(db)?
                .ok_or_else(missing_discussion)?;
        return Ok(DiscussionConversation {
            name: None,
            id: id.into(),
            workspace_id: ws.clone(),
            kind: "workspace".into(),
            participants: vec![],
            created_at,
        });
    }
    let row = sqlx::query(
        "SELECT id, workspace_id, kind, participants_json, created_at, NULL::text AS name \
         FROM discussion_conversation WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
    )
    .bind(tenant)
    .bind(ws.as_str())
    .bind(id)
    .fetch_optional(&mut *c)
    .await
    .map_err(db)?
    .ok_or_else(missing_discussion)?;
    conversation(&row)
}

async fn agent_eligible_in(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    agent: &AgentId,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2 \
         AND id = $3 AND retired_at IS NULL AND status <> 'deleted')",
    )
    .bind(tenant)
    .bind(ws.as_str())
    .bind(agent.as_str())
    .fetch_one(c)
    .await
    .map_err(db)
}

async fn agent_access_in(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation: &str,
    agent: &AgentId,
) -> Result<bool> {
    Ok(
        workspace_conversation_in(c, tenant, ws, conversation).await?
            && agent_grant_in(c, tenant, ws, conversation, agent).await?,
    )
}

/// Whether `conversation` is a live workspace conversation of `ws`.
async fn workspace_conversation_in(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation: &str,
) -> Result<bool> {
    match read_conversation(c, tenant, ws, conversation).await {
        Ok(found) => Ok(found.kind == "workspace"),
        Err(Error::NotFound(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether live agent `agent` holds a grant on `conversation`.
async fn agent_grant_in(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation: &str,
    agent: &AgentId,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM discussion_agent_grant g \
         JOIN agent_session a ON a.tenant_id = $1 AND a.id = g.agent_id AND a.workspace_id = g.workspace_id \
         WHERE g.tenant_id = $1 AND g.workspace_id = $2 AND g.conversation_id = $3 AND g.agent_id = $4 \
         AND a.retired_at IS NULL AND a.status <> 'deleted')",
    )
    .bind(tenant)
    .bind(ws.as_str())
    .bind(conversation)
    .bind(agent.as_str())
    .fetch_one(c)
    .await
    .map_err(db)
}

/// Whether `principal` may take part in workspace `ws`: the primary
/// principal, a workspace member, or (outside `__chief__`) a host member.
async fn principal_eligible(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    principal: Option<&str>,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM principal p JOIN workspace w ON w.tenant_id = $1 AND w.id = $2 \
         WHERE p.tenant_id = $1 AND p.id = $3 AND (p.is_primary = 1 \
         OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 AND m.workspace_id = $2 AND m.principal_id = p.id) \
         OR ($2 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id))))",
    )
    .bind(tenant)
    .bind(ws.as_str())
    .bind(principal)
    .fetch_one(c)
    .await
    .map_err(db)
}

/// One bounded query checks live membership and participation, including on retries.
async fn validate_principal_recipients(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation_id: &str,
    target: &DiscussionTarget,
) -> Result<()> {
    if target.principal_mentions.is_empty() {
        return Ok(());
    }
    let ids: std::collections::HashSet<&str> = target
        .principal_mentions
        .iter()
        .map(|m| m.principal_id.as_str())
        .collect();
    let list: Vec<&str> = ids.iter().copied().collect();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM principal p WHERE p.tenant_id = $1 AND p.id = ANY($2) AND (p.is_primary = 1 \
         OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 AND m.workspace_id = $3 AND m.principal_id = p.id) \
         OR ($3 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id))) \
         AND ($4 = 'default' \
         OR EXISTS(SELECT 1 FROM workspace_discussion_conversation wdc WHERE wdc.tenant_id = $1 AND wdc.workspace_id = $3 AND wdc.id = $4 AND wdc.deleted_at IS NULL) \
         OR EXISTS(SELECT 1 FROM discussion_participant d WHERE d.tenant_id = $1 AND d.workspace_id = $3 AND d.conversation_id = $4 AND d.principal_id = p.id))",
    )
    .bind(tenant)
    .bind(&list)
    .bind(ws.as_str())
    .bind(conversation_id)
    .fetch_one(c)
    .await
    .map_err(db)?;
    if count != i64::try_from(ids.len()).map_err(json_err)? {
        return Err(missing_discussion());
    }
    Ok(())
}

async fn refresh_activity(tx: &mut WriteTx, tenant: &str, message: &str) -> Result<()> {
    sqlx::query(
        "UPDATE workspace_discussion_message SET agent_activity_json = COALESCE((SELECT json_agg(json_build_object(\
         'agentId', a.agent_id, 'attempt', a.attempt, 'version', a.version, 'state', a.state, \
         'updatedAt', a.updated_at, 'error', a.error) ORDER BY a.agent_id COLLATE \"C\")::text \
         FROM discussion_agent_activity a WHERE a.tenant_id = $1 AND a.message_id = $2), '[]') \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind(message)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

async fn insert_activity(
    tx: &mut WriteTx,
    tenant: &str,
    message: &str,
    agent: &AgentId,
    now: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO discussion_agent_activity (tenant_id, message_id, agent_id, attempt, version, \
         state, updated_at, turn_id) VALUES ($1, $2, $3, 1, 1, 'queued', $4, $5)",
    )
    .bind(tenant)
    .bind(message)
    .bind(agent.as_str())
    .bind(now)
    .bind(format!("discussion:{message}:{}", agent.as_str()))
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

async fn authorize(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation: &str,
    actor: &DiscussionActor,
) -> Result<()> {
    let allowed = match actor {
        DiscussionActor::Human(principal) => sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal p JOIN workspace w ON w.tenant_id = $1 AND w.id = $2 \
             WHERE p.tenant_id = $1 AND p.id = $3 AND (p.is_primary = 1 \
             OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 AND m.workspace_id = $2 AND m.principal_id = p.id) \
             OR ($2 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id))) \
             AND ($4 = 'default' \
             OR EXISTS(SELECT 1 FROM workspace_discussion_conversation wdc WHERE wdc.tenant_id = $1 AND wdc.workspace_id = w.id AND wdc.id = $4 AND wdc.deleted_at IS NULL) \
             OR EXISTS(SELECT 1 FROM discussion_participant d WHERE d.tenant_id = $1 AND d.workspace_id = w.id AND d.conversation_id = $4 AND d.principal_id = p.id)))",
        )
        .bind(tenant)
        .bind(ws.as_str())
        .bind(principal.as_str())
        .bind(conversation)
        .fetch_one(c)
        .await
        .map_err(db)?,
        DiscussionActor::Agent(agent) => agent_access_in(c, tenant, ws, conversation, agent).await?,
    };
    if allowed {
        Ok(())
    } else {
        Err(missing_message())
    }
}

async fn reactor(
    c: &mut PgConnection,
    tenant: &str,
    actor: &DiscussionActor,
) -> Result<DiscussionAuthor> {
    match actor {
        DiscussionActor::Human(principal) => {
            let row = sqlx::query(
                "SELECT login, display_name, avatar_url FROM principal WHERE tenant_id = $1 AND id = $2",
            )
            .bind(tenant)
            .bind(principal.as_str())
            .fetch_one(c)
            .await
            .map_err(db)?;
            Ok(DiscussionAuthor {
                principal_id: Some(principal.clone()),
                agent_id: None,
                login: col(&row, "login")?,
                display_name: col(&row, "display_name")?,
                avatar_url: col(&row, "avatar_url")?,
            })
        }
        DiscussionActor::Agent(agent) => {
            let name: String = sqlx::query_scalar(
                "SELECT name FROM agent_session WHERE tenant_id = $1 AND id = $2",
            )
            .bind(tenant)
            .bind(agent.as_str())
            .fetch_one(c)
            .await
            .map_err(db)?;
            Ok(DiscussionAuthor {
                principal_id: None,
                agent_id: Some(agent.clone()),
                login: None,
                display_name: Some(name),
                avatar_url: None,
            })
        }
    }
}

async fn reaction_state(
    c: &mut PgConnection,
    tenant: &str,
    ws: &WorkspaceId,
    conversation: &str,
    message: &str,
    actor: &DiscussionActor,
) -> Result<DiscussionReactionState> {
    let row = sqlx::query(
        "SELECT m.reactions_json, m.reaction_version, \
         (SELECT COALESCE(json_agg(r.reaction), '[]'::json)::text FROM discussion_reaction r \
          WHERE r.tenant_id = $1 AND r.message_id = m.id AND (r.principal_id = $2 OR r.agent_id = $3)) AS selected \
         FROM workspace_discussion_message m \
         WHERE m.tenant_id = $1 AND m.workspace_id = $4 AND m.conversation_id = $5 AND m.id = $6",
    )
    .bind(tenant)
    .bind(actor.principal_id())
    .bind(actor.agent_id())
    .bind(ws.as_str())
    .bind(conversation)
    .bind(message)
    .fetch_optional(c)
    .await
    .map_err(db)?
    .ok_or_else(missing_message)?;
    let mut reactions: Vec<DiscussionReaction> =
        serde_json::from_str(&col::<String>(&row, "reactions_json")?).map_err(json_err)?;
    let selected: Vec<String> =
        serde_json::from_str(&col::<String>(&row, "selected")?).map_err(json_err)?;
    for reaction in &mut reactions {
        reaction.reacted = selected.contains(&reaction.reaction);
    }
    Ok(DiscussionReactionState {
        message_id: message.into(),
        reactions,
        reaction_version: col(&row, "reaction_version")?,
    })
}

impl PgStore {
    pub async fn insert_discussion_message(
        &self,
        workspace_id: &WorkspaceId,
        client_message_id: &str,
        body: &str,
        author: &DiscussionAuthor,
    ) -> Result<(DiscussionMessage, bool)> {
        let (result, inserted) = self
            .send_discussion_message(
                workspace_id,
                client_message_id,
                body,
                author,
                &DiscussionTarget::default(),
            )
            .await?;
        Ok((result.message, inserted))
    }

    pub async fn list_discussion_messages(
        &self,
        workspace_id: &WorkspaceId,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        self.list_discussion_messages_in(
            workspace_id,
            DEFAULT_CONVERSATION,
            before_sequence,
            after_sequence,
            limit,
        )
        .await
    }

    pub async fn list_discussion_messages_in(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &str,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        self.list_discussion_messages_for(
            workspace_id,
            conversation_id,
            before_sequence,
            after_sequence,
            limit,
            None,
        )
        .await
    }

    pub async fn list_discussion_messages_for(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &str,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
        viewer: Option<&DiscussionActor>,
    ) -> Result<DiscussionPage> {
        if limit == 0
            || limit > DISCUSSION_PAGE_MAX
            || before_sequence.is_some_and(|n| n <= 0)
            || after_sequence.is_some_and(|n| n < 0)
            || (before_sequence.is_some() && after_sequence.is_some())
        {
            return Err(Error::InvalidParams(
                "invalid discussion page bounds".into(),
            ));
        }
        let (comparison, order, cursor) = match after_sequence {
            Some(after) => (">", "ASC", after),
            None => ("<", "DESC", before_sequence.unwrap_or(i64::MAX)),
        };
        let sql = format!(
            "SELECT m.*, (SELECT COALESCE(json_agg(r.reaction), '[]'::json)::text FROM discussion_reaction r \
              WHERE r.tenant_id = $1 AND r.message_id = m.id AND (r.principal_id = $2 OR r.agent_id = $3)) AS viewer_reactions \
             FROM workspace_discussion_message m \
             WHERE m.tenant_id = $1 AND m.workspace_id = $4 AND m.conversation_id = $5 AND m.sequence {comparison} $6 \
             ORDER BY m.sequence {order} LIMIT $7"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(viewer.and_then(DiscussionActor::principal_id))
            .bind(viewer.and_then(DiscussionActor::agent_id))
            .bind(workspace_id.as_str())
            .bind(conversation_id)
            .bind(cursor)
            .bind(i64::from(limit) + 1)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let has_more = rows.len() > limit as usize;
        let mut messages = rows
            .iter()
            .take(limit as usize)
            .map(message)
            .collect::<Result<Vec<_>>>()?;
        if after_sequence.is_none() {
            messages.reverse();
        }
        Ok(DiscussionPage { messages, has_more })
    }

    pub async fn has_private_discussions(&self, ws: &WorkspaceId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let found = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM discussion_conversation WHERE tenant_id = $1 AND workspace_id = $2) \
             OR EXISTS(SELECT 1 FROM workspace_discussion_message m WHERE m.tenant_id = $1 AND m.workspace_id = $2 \
              AND m.conversation_id <> 'default' AND NOT EXISTS(SELECT 1 FROM workspace_discussion_conversation wdc \
              WHERE wdc.tenant_id = $1 AND wdc.workspace_id = m.workspace_id AND wdc.id = m.conversation_id))",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn discussion_conversation(
        &self,
        ws: &WorkspaceId,
        id: &str,
    ) -> Result<DiscussionConversation> {
        // `read_conversation` falls back to another table when one has no row.
        let mut tx = self.begin_read_snapshot().await?;
        let found = read_conversation_probed(
            &mut tx,
            self.tenant(),
            ws,
            id,
            Some("discussion_conversation"),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn discussion_is_participant(
        &self,
        ws: &WorkspaceId,
        id: &str,
        principal: &PrincipalId,
    ) -> Result<bool> {
        let mut tx = self.begin().await?;
        let found = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE tenant_id = $1 \
             AND workspace_id = $2 AND conversation_id = $3 AND principal_id = $4)",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(principal.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn discussion_visible_ids(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        ids: &[String],
    ) -> Result<Vec<String>> {
        let mut tx = self.begin().await?;
        let visible = sqlx::query_scalar(
            "SELECT id AS conversation_id FROM workspace_discussion_conversation \
             WHERE tenant_id = $1 AND workspace_id = $2 AND deleted_at IS NULL AND id = ANY($4) \
             UNION SELECT conversation_id FROM discussion_participant \
             WHERE tenant_id = $1 AND workspace_id = $2 AND principal_id = $3 AND conversation_id = ANY($4)",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(principal.as_str())
        .bind(ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(visible)
    }

    pub async fn discussion_conversations(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Value> {
        if !(1..=100).contains(&limit) || before == Some("") {
            return Err(Error::InvalidParams(
                "invalid conversation page bounds".into(),
            ));
        }
        let cursor = before
            .filter(|id| *id != DEFAULT_CONVERSATION)
            .unwrap_or("~");
        // The page and the default conversation come from one snapshot.
        let mut tx = self.begin_read_snapshot().await?;
        let rows = sqlx::query(
            "SELECT * FROM (\
             SELECT c.id, c.workspace_id, c.kind, c.participants_json, c.created_at, NULL::text AS name \
             FROM discussion_conversation c WHERE c.tenant_id = $1 AND c.workspace_id = $2 \
             AND c.id COLLATE \"C\" < $3 AND EXISTS(SELECT 1 FROM discussion_participant p \
             WHERE p.tenant_id = $1 AND p.conversation_id = c.id AND p.principal_id = $4) \
             UNION ALL SELECT id, workspace_id, 'workspace' AS kind, '[]' AS participants_json, created_at, name \
             FROM workspace_discussion_conversation WHERE tenant_id = $1 AND workspace_id = $2 \
             AND id <> 'default' AND id COLLATE \"C\" < $3 AND deleted_at IS NULL\
             ) listed ORDER BY id COLLATE \"C\" DESC LIMIT $5",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(cursor)
        .bind(principal.as_str())
        .bind(i64::from(limit) + 1)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        between(self.tenant(), "discussion_conversations").await;
        let mut conversations = Vec::new();
        if before.is_none() {
            match read_conversation(&mut tx, self.tenant(), ws, DEFAULT_CONVERSATION).await {
                Ok(found) => conversations.push(found),
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        tx.commit().await.map_err(db)?;
        conversations.extend(rows.iter().map(conversation).collect::<Result<Vec<_>>>()?);
        let has_more = conversations.len() > limit as usize;
        conversations.truncate(limit as usize);
        Ok(json!({"conversations":conversations,"hasMore":has_more}))
    }

    pub async fn discussion_recipients(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        query: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Value> {
        if !(1..=100).contains(&limit) || query.len() > 256 || after == Some("") {
            return Err(Error::InvalidParams("invalid recipient page bounds".into()));
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT p.id, p.login, p.display_name, p.avatar_url FROM principal p \
             WHERE p.tenant_id = $1 AND p.id <> $2 AND p.id COLLATE \"C\" > $3 AND (p.is_primary = 1 \
             OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 AND m.workspace_id = $4 AND m.principal_id = p.id) \
             OR ($4 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id))) \
             AND strpos(lower(coalesce(p.login, '') || ' ' || coalesce(p.display_name, '') || ' ' || p.id), lower($5)) > 0 \
             ORDER BY p.id COLLATE \"C\" LIMIT $6",
        )
        .bind(self.tenant())
        .bind(principal.as_str())
        .bind(after.unwrap_or(""))
        .bind(ws.as_str())
        .bind(query)
        .bind(i64::from(limit) + 1)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let has_more = rows.len() > limit as usize;
        let members = rows
            .iter()
            .take(limit as usize)
            .map(profile)
            .collect::<Result<Vec<_>>>()?;
        Ok(json!({"members":members,"hasMore":has_more}))
    }

    pub async fn send_discussion_message(
        &self,
        ws: &WorkspaceId,
        key: &str,
        body: &str,
        author: &DiscussionAuthor,
        target: &DiscussionTarget,
    ) -> Result<(DiscussionSendResult, bool)> {
        if target.create_conversation
            && (target.conversation_id.is_some()
                || target.recipient_principal_ids.is_some()
                || author.agent_id.is_some())
        {
            return Err(Error::InvalidParams(
                "createConversation requires a human and no conversation or private recipients"
                    .into(),
            ));
        }
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let principal_id = author.principal_id.as_ref();
        let agent_id = author.agent_id.as_ref();
        if principal_id.is_some() == agent_id.is_some() {
            return Err(Error::InvalidParams(
                "discussion requires exactly one author identity".into(),
            ));
        }
        let conversation_id = target
            .conversation_id
            .as_deref()
            .unwrap_or(DEFAULT_CONVERSATION);
        if (agent_id.is_some() || !target.recipient_agent_ids.is_empty())
            && (!target.create_conversation
                && read_conversation(&mut tx, tenant, ws, conversation_id)
                    .await?
                    .kind
                    != "workspace"
                || target.recipient_principal_ids.is_some())
        {
            return Err(Error::Forbidden(
                "agent messaging is available only in the workspace discussion".into(),
            ));
        }
        if agent_id.is_some() && !target.principal_mentions.is_empty() {
            return Err(Error::Forbidden("only humans may mention people".into()));
        }
        intent_core::discussion::validate_agent_mentions(body, target)?;
        intent_core::discussion::validate_principal_mentions(body, target)?;
        let mut recipients = target.recipient_agent_ids.clone();
        recipients.sort();
        if recipients.len() > DISCUSSION_RECIPIENT_MAX
            || recipients.windows(2).any(|w| w[0] == w[1])
            || recipients
                .iter()
                .any(|id| id.as_str().trim().is_empty() || Some(id) == agent_id)
        {
            return Err(Error::InvalidParams(
                "recipientAgentIds must contain at most 31 distinct other agents".into(),
            ));
        }
        if let Some(agent) = agent_id {
            if !agent_access_in(&mut tx, tenant, ws, conversation_id, agent).await? {
                return Err(missing_discussion());
            }
        } else if !principal_eligible(&mut tx, tenant, ws, principal_id.map(PrincipalId::as_str))
            .await?
        {
            return Err(missing_discussion());
        }
        if target.create_conversation {
            let deleted: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM workspace_discussion_conversation WHERE tenant_id = $1 \
                 AND workspace_id = $2 AND first_principal_id = $3 AND first_client_message_id = $4 \
                 AND deleted_at IS NOT NULL)",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(principal_id.map(PrincipalId::as_str))
            .bind(key)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            if deleted {
                return Err(missing_discussion());
            }
        }
        let old = sqlx::query(
            "SELECT * FROM workspace_discussion_message WHERE tenant_id = $1 AND workspace_id = $2 \
             AND principal_id IS NOT DISTINCT FROM $3 AND agent_id IS NOT DISTINCT FROM $4 \
             AND client_message_id = $5",
        )
        .bind(tenant)
        .bind(ws.as_str())
        .bind(principal_id.map(PrincipalId::as_str))
        .bind(agent_id.map(AgentId::as_str))
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(row) = old {
            let message = message(&row)?;
            let conversation =
                read_conversation(&mut tx, tenant, ws, &message.conversation_id).await?;
            let same_target = if target.create_conversation {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM workspace_discussion_conversation WHERE tenant_id = $1 \
                     AND workspace_id = $2 AND id = $3 AND first_message_id = $4)",
                )
                .bind(tenant)
                .bind(ws.as_str())
                .bind(&conversation.id)
                .bind(&message.id)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?
            } else if let Some(recipients) = &target.recipient_principal_ids {
                let mut ids: Vec<_> = conversation
                    .participants
                    .iter()
                    .filter_map(|p| p.principal_id.clone())
                    .collect();
                ids.sort();
                let mut wanted = recipients.clone();
                wanted.push(principal_id.ok_or_else(missing_discussion)?.clone());
                wanted.sort();
                ids == wanted && conversation.kind != "workspace"
            } else {
                conversation_id == message.conversation_id
            };
            if message.principal_mentions != target.principal_mentions
                || message.agent_mentions != target.agent_mentions
                || message.recipient_agent_ids != recipients
                || message.body != body
                || !same_target
                || message.reply_to_message_id != target.reply_to_message_id
            {
                return Err(Error::InvalidParams(
                    "messageId was already used with a different body, conversation or reply target".into(),
                ));
            }
            if conversation.kind != "workspace" {
                let allowed: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE tenant_id = $1 \
                     AND conversation_id = $2 AND principal_id = $3)",
                )
                .bind(tenant)
                .bind(&message.conversation_id)
                .bind(principal_id.map(PrincipalId::as_str))
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
                if !allowed {
                    return Err(missing_discussion());
                }
            }
            validate_principal_recipients(&mut tx, tenant, ws, &message.conversation_id, target)
                .await?;
            tx.commit().await.map_err(db)?;
            return Ok((
                DiscussionSendResult {
                    message,
                    conversation,
                },
                false,
            ));
        }
        for recipient in &recipients {
            if !agent_eligible_in(&mut tx, tenant, ws, recipient).await? {
                return Err(missing_discussion());
            }
        }
        let id = uuid::Uuid::now_v7().to_string();
        let conversation = if target.create_conversation {
            let conversation = DiscussionConversation {
                id: uuid::Uuid::now_v7().to_string(),
                workspace_id: ws.clone(),
                kind: "workspace".into(),
                name: None,
                participants: vec![],
                created_at: now_iso(),
            };
            sqlx::query(
                "INSERT INTO workspace_discussion_conversation (tenant_id, workspace_id, id, \
                 first_message_id, first_principal_id, first_client_message_id, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(&conversation.id)
            .bind(&id)
            .bind(principal_id.map(PrincipalId::as_str))
            .bind(key)
            .bind(&conversation.created_at)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            conversation
        } else if let Some(recipients) = &target.recipient_principal_ids {
            let mut ids = recipients.clone();
            ids.push(principal_id.ok_or_else(missing_discussion)?.clone());
            ids.sort();
            let list: Vec<&str> = ids.iter().map(PrincipalId::as_str).collect();
            let rows = sqlx::query(
                "SELECT p.id, p.login, p.display_name, p.avatar_url FROM principal p \
                 WHERE p.tenant_id = $1 AND p.id = ANY($2) AND (p.is_primary = 1 \
                 OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 AND m.workspace_id = $3 AND m.principal_id = p.id) \
                 OR ($3 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id))) \
                 ORDER BY p.id COLLATE \"C\"",
            )
            .bind(tenant)
            .bind(&list)
            .bind(ws.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            if rows.len() != ids.len() {
                return Err(missing_discussion());
            }
            let conversation = DiscussionConversation {
                name: None,
                id: uuid::Uuid::now_v7().to_string(),
                workspace_id: ws.clone(),
                kind: if ids.len() == 2 { "direct" } else { "group" }.into(),
                participants: rows.iter().map(profile).collect::<Result<_>>()?,
                created_at: now_iso(),
            };
            sqlx::query(
                "INSERT INTO discussion_conversation (tenant_id, id, workspace_id, kind, \
                 participants_json, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(tenant)
            .bind(&conversation.id)
            .bind(ws.as_str())
            .bind(&conversation.kind)
            .bind(to_json(&conversation.participants)?)
            .bind(&conversation.created_at)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            for participant in &ids {
                sqlx::query(
                    "INSERT INTO discussion_participant (tenant_id, conversation_id, workspace_id, \
                     source_principal_id, principal_id) VALUES ($1, $2, $3, $4, $4)",
                )
                .bind(tenant)
                .bind(&conversation.id)
                .bind(ws.as_str())
                .bind(participant.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            conversation
        } else {
            if read_conversation(&mut tx, tenant, ws, conversation_id)
                .await?
                .kind
                != "workspace"
            {
                let allowed: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE tenant_id = $1 \
                     AND workspace_id = $2 AND conversation_id = $3 AND principal_id = $4)",
                )
                .bind(tenant)
                .bind(ws.as_str())
                .bind(conversation_id)
                .bind(principal_id.map(PrincipalId::as_str))
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
                if !allowed {
                    return Err(missing_discussion());
                }
            }
            read_conversation(&mut tx, tenant, ws, conversation_id).await?
        };
        validate_principal_recipients(&mut tx, tenant, ws, &conversation.id, target).await?;
        let reply = if let Some(reply_id) = &target.reply_to_message_id {
            let row = sqlx::query(
                "SELECT * FROM workspace_discussion_message WHERE tenant_id = $1 \
                 AND workspace_id = $2 AND conversation_id = $3 AND id = $4",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(&conversation.id)
            .bind(reply_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or_else(missing_discussion)?;
            let original = message(&row)?;
            Some(to_json(&DiscussionReply {
                id: original.id,
                author: original.author,
                body: original.body,
            })?)
        } else {
            None
        };
        let now = now_iso();
        sqlx::query(
            "INSERT INTO workspace_discussion_message (tenant_id, id, workspace_id, conversation_id, \
             sequence, principal_id, agent_id, client_message_id, body, author_json, created_at, \
             reply_to_message_id, reply_to_json, recipient_agent_ids_json, agent_mentions_json, \
             principal_mentions_json) VALUES ($1, $2, $3, $4, \
             (SELECT COALESCE(MAX(sequence), 0) + 1 FROM workspace_discussion_message \
              WHERE tenant_id = $1 AND workspace_id = $3 AND conversation_id = $4), \
             $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        )
        .bind(tenant)
        .bind(&id)
        .bind(ws.as_str())
        .bind(&conversation.id)
        .bind(principal_id.map(PrincipalId::as_str))
        .bind(agent_id.map(AgentId::as_str))
        .bind(key)
        .bind(body)
        .bind(to_json(author)?)
        .bind(&now)
        .bind(&target.reply_to_message_id)
        .bind(reply)
        .bind(to_json(&recipients)?)
        .bind(to_json(&target.agent_mentions)?)
        .bind(to_json(&target.principal_mentions)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        for recipient in &recipients {
            insert_activity(&mut tx, tenant, &id, recipient, &now_iso()).await?;
            sqlx::query(
                "INSERT INTO discussion_agent_grant (tenant_id, workspace_id, conversation_id, agent_id) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (tenant_id, workspace_id, conversation_id, agent_id) DO NOTHING",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(&conversation.id)
            .bind(recipient.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            sqlx::query(
                "INSERT INTO discussion_agent_delivery (tenant_id, message_id, agent_id, workspace_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(tenant)
            .bind(&id)
            .bind(recipient.as_str())
            .bind(ws.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        refresh_activity(&mut tx, tenant, &id).await?;
        let row = sqlx::query(
            "SELECT * FROM workspace_discussion_message WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(&id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let message = message(&row)?;
        tx.commit().await.map_err(db)?;
        Ok((
            DiscussionSendResult {
                message,
                conversation,
            },
            true,
        ))
    }

    pub async fn discussion_agent_conversations(
        &self,
        ws: &WorkspaceId,
        agent: &AgentId,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Value> {
        // The granted ids and their conversations come from one snapshot: a
        // conversation deleted in between would otherwise fail the page.
        let mut tx = self.begin_read_snapshot().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT g.conversation_id FROM discussion_agent_grant g \
             JOIN agent_session a ON a.tenant_id = $1 AND a.id = g.agent_id AND a.workspace_id = g.workspace_id \
             WHERE g.tenant_id = $1 AND g.workspace_id = $2 AND g.agent_id = $3 AND a.retired_at IS NULL \
             AND a.status <> 'deleted' AND g.conversation_id COLLATE \"C\" < $4 \
             ORDER BY g.conversation_id COLLATE \"C\" DESC LIMIT $5",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(agent.as_str())
        .bind(before.unwrap_or("~"))
        .bind(i64::from(limit) + 1)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        between(self.tenant(), "discussion_agent_conversations").await;
        let has_more = ids.len() > limit as usize;
        let mut conversations = vec![];
        for id in ids.into_iter().take(limit as usize) {
            conversations.push(read_conversation(&mut tx, self.tenant(), ws, &id).await?);
        }
        tx.commit().await.map_err(db)?;
        Ok(json!({"conversations":conversations,"hasMore":has_more}))
    }

    pub async fn discussion_agent_access(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        agent: &AgentId,
    ) -> Result<bool> {
        // The conversation and the grant come from one snapshot.
        let tenant = self.tenant();
        let mut tx = self.begin_read_snapshot().await?;
        let allowed = workspace_conversation_in(&mut tx, tenant, ws, conversation).await? && {
            between(tenant, "discussion_agent_access").await;
            agent_grant_in(&mut tx, tenant, ws, conversation, agent).await?
        };
        tx.commit().await.map_err(db)?;
        Ok(allowed)
    }

    pub async fn discussion_pending_deliveries(
        &self,
        message_id: Option<&str>,
    ) -> Result<Vec<DiscussionDelivery>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT m.*, d.agent_id AS recipient_agent_id, a.turn_id AS invocation_turn_id, \
             a.attempt AS invocation_attempt, a.updated_at AS invocation_updated_at \
             FROM discussion_agent_delivery d \
             JOIN workspace_discussion_message m ON m.tenant_id = $1 AND m.id = d.message_id \
             JOIN discussion_agent_activity a ON a.tenant_id = $1 AND a.message_id = d.message_id AND a.agent_id = d.agent_id \
             WHERE d.tenant_id = $1 AND ($2::text IS NULL OR d.message_id = $2) \
             ORDER BY d.message_id COLLATE \"C\", d.agent_id COLLATE \"C\" LIMIT 100",
        )
        .bind(self.tenant())
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(DiscussionDelivery {
                    message: message(row)?,
                    agent_id: AgentId(col(row, "recipient_agent_id")?),
                    turn_id: col(row, "invocation_turn_id")?,
                    attempt: col(row, "invocation_attempt")?,
                    updated_at: col(row, "invocation_updated_at")?,
                })
            })
            .collect()
    }

    pub async fn admit_discussion_delivery(
        &self,
        message_id: &str,
        row: &AgentQueueRow,
    ) -> Result<bool> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let ws: Option<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM discussion_agent_delivery WHERE tenant_id = $1 \
             AND message_id = $2 AND agent_id = $3",
        )
        .bind(tenant)
        .bind(message_id)
        .bind(row.agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(ws) = ws else {
            return Ok(false);
        };
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM discussion_agent_activity WHERE tenant_id = $1 \
             AND turn_id = $2 AND state = 'queued')",
        )
        .bind(tenant)
        .bind(&row.turn_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let conversation: String = sqlx::query_scalar(
            "SELECT conversation_id FROM workspace_discussion_message WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let eligible = active
            && agent_access_in(
                &mut tx,
                tenant,
                &WorkspaceId(ws),
                &conversation,
                &row.agent_id,
            )
            .await?;
        if eligible {
            sqlx::query(
                "INSERT INTO agent_queue (tenant_id, id, agent_id, position, payload, created_at, turn_id) \
                 VALUES ($1, $2, $3, (SELECT COALESCE(MAX(position), -1) + 1 FROM agent_queue \
                 WHERE tenant_id = $1 AND agent_id = $3), $4, $5, $6)",
            )
            .bind(tenant)
            .bind(&row.id)
            .bind(row.agent_id.as_str())
            .bind(row.payload.to_string())
            .bind(&row.created_at)
            .bind(&row.turn_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        sqlx::query(
            "DELETE FROM discussion_agent_delivery WHERE tenant_id = $1 AND message_id = $2 AND agent_id = $3",
        )
        .bind(tenant)
        .bind(message_id)
        .bind(row.agent_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(eligible)
    }

    pub async fn discussion_activity(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        agent: &AgentId,
    ) -> Result<DiscussionActivityReceipt> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{ACTIVITY} AND m.workspace_id = $2 AND m.conversation_id = $3 AND a.message_id = $4 AND a.agent_id = $5"
        )))
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(conversation)
        .bind(message_id)
        .bind(agent.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(missing_invocation)?;
        tx.commit().await.map_err(db)?;
        receipt(&row)
    }

    pub async fn transition_discussion_activity(
        &self,
        turn: &str,
        state: &str,
        error: Option<&str>,
    ) -> Result<Option<DiscussionActivityReceipt>> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE discussion_agent_activity SET state = $2, error = $3, updated_at = $4, \
             version = version + 1 WHERE tenant_id = $1 AND turn_id = $5 \
             AND state IN ('queued', 'working') AND state <> $2",
        )
        .bind(tenant)
        .bind(state)
        .bind(error)
        .bind(now_iso())
        .bind(turn)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if changed == 0 {
            return Ok(None);
        }
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{ACTIVITY} AND a.turn_id = $2"
        )))
        .bind(tenant)
        .bind(turn)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let value = receipt(&row)?;
        refresh_activity(&mut tx, tenant, &value.message_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(Some(value))
    }

    pub async fn discussion_invocation_allowed(&self, turn: &str, agent: &AgentId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let allowed = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM discussion_agent_activity WHERE tenant_id = $1 \
             AND turn_id = $2 AND agent_id = $3 AND state IN ('queued', 'working'))",
        )
        .bind(self.tenant())
        .bind(turn)
        .bind(agent.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(allowed)
    }

    pub async fn control_discussion_activity(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        agent: &AgentId,
        attempt: u32,
        retry: bool,
    ) -> Result<(DiscussionActivityReceipt, bool)> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{ACTIVITY} AND m.workspace_id = $2 AND m.conversation_id = $3 AND a.message_id = $4 AND a.agent_id = $5"
        )))
        .bind(tenant)
        .bind(ws.as_str())
        .bind(conversation)
        .bind(message_id)
        .bind(agent.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(missing_invocation)?;
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
        if retry && !agent_access_in(&mut tx, tenant, ws, conversation, agent).await? {
            return Err(Error::NotFound("Discussion recipient not found".into()));
        }
        sqlx::query(
            "DELETE FROM discussion_agent_delivery WHERE tenant_id = $1 AND message_id = $2 AND agent_id = $3",
        )
        .bind(tenant)
        .bind(message_id)
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query("DELETE FROM agent_queue WHERE tenant_id = $1 AND id = $2 AND agent_id = $3")
            .bind(tenant)
            .bind(&old.turn_id)
            .bind(agent.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let next = if retry {
            attempt
                .checked_add(1)
                .ok_or_else(|| Error::InvalidParams("attempt limit reached".into()))?
        } else {
            attempt
        };
        let turn = if retry {
            format!("discussion:{message_id}:{}:{next}", agent.as_str())
        } else {
            old.turn_id
        };
        sqlx::query(
            "UPDATE discussion_agent_activity SET attempt = $2, version = version + 1, state = $3, \
             updated_at = $4, error = NULL, turn_id = $5 \
             WHERE tenant_id = $1 AND message_id = $6 AND agent_id = $7",
        )
        .bind(tenant)
        .bind(i64::from(next))
        .bind(if retry { "queued" } else { "stopped" })
        .bind(now_iso())
        .bind(&turn)
        .bind(message_id)
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if retry {
            sqlx::query(
                "INSERT INTO discussion_agent_delivery (tenant_id, message_id, agent_id, workspace_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(tenant)
            .bind(message_id)
            .bind(agent.as_str())
            .bind(ws.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        refresh_activity(&mut tx, tenant, message_id).await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{ACTIVITY} AND a.turn_id = $2"
        )))
        .bind(tenant)
        .bind(&turn)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let result = receipt(&row)?;
        tx.commit().await.map_err(db)?;
        Ok((result, true))
    }

    pub async fn recover_discussion_activity(&self) -> Result<()> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        // Startup healing runs first. Do not let its generic continuation replay a
        // request whose correlated recovery now requires an explicit Retry.
        sqlx::query(
            "UPDATE interrupted_agent SET resolution = 'abandoned', resolved_at = $2 \
             WHERE tenant_id = $1 AND resolution = 'pending' AND agent_id IN (\
             SELECT a.agent_id FROM discussion_agent_activity a WHERE a.tenant_id = $1 AND (a.state = 'working' \
             OR (a.state = 'queued' \
             AND NOT EXISTS(SELECT 1 FROM agent_queue q WHERE q.tenant_id = $1 AND q.id = a.turn_id) \
             AND NOT EXISTS(SELECT 1 FROM discussion_agent_delivery d WHERE d.tenant_id = $1 \
             AND d.message_id = a.message_id AND d.agent_id = a.agent_id))))",
        )
        .bind(tenant)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let messages: Vec<String> = sqlx::query_scalar(
            "UPDATE discussion_agent_activity a SET state = 'stopped', \
             error = 'Interrupted by daemon restart.', updated_at = $2, version = a.version + 1 \
             WHERE a.tenant_id = $1 AND (a.state = 'working' OR (a.state = 'queued' \
             AND NOT EXISTS(SELECT 1 FROM agent_queue q WHERE q.tenant_id = $1 AND q.id = a.turn_id) \
             AND NOT EXISTS(SELECT 1 FROM discussion_agent_delivery d WHERE d.tenant_id = $1 \
             AND d.message_id = a.message_id AND d.agent_id = a.agent_id))) \
             RETURNING a.message_id",
        )
        .bind(tenant)
        .bind(now_iso())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "DELETE FROM agent_queue WHERE tenant_id = $1 AND id IN (SELECT a.turn_id \
             FROM discussion_agent_activity a WHERE a.tenant_id = $1 AND a.state NOT IN ('queued', 'working'))",
        )
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        for message_id in messages {
            refresh_activity(&mut tx, tenant, &message_id).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    pub async fn discussion_reaction_state(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        principal: &PrincipalId,
    ) -> Result<DiscussionReactionState> {
        self.discussion_actor_reaction_state(
            ws,
            conversation,
            message_id,
            &DiscussionActor::Human(principal.clone()),
        )
        .await
    }

    pub async fn set_discussion_reaction(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        principal: &PrincipalId,
        reaction: &str,
        active: bool,
    ) -> Result<(DiscussionReactionState, bool)> {
        self.set_discussion_actor_reaction(
            ws,
            conversation,
            message_id,
            &DiscussionActor::Human(principal.clone()),
            reaction,
            active,
        )
        .await
    }

    pub async fn discussion_actor_reaction_state(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        actor: &DiscussionActor,
    ) -> Result<DiscussionReactionState> {
        let tenant = self.tenant();
        // The access check and the state come from one snapshot.
        let mut tx = self.begin_read_snapshot().await?;
        authorize(&mut tx, tenant, ws, conversation, actor).await?;
        between(tenant, "discussion_actor_reaction_state").await;
        let result = reaction_state(&mut tx, tenant, ws, conversation, message_id, actor).await?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }

    pub async fn set_discussion_actor_reaction(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message_id: &str,
        actor: &DiscussionActor,
        reaction: &str,
        active: bool,
    ) -> Result<(DiscussionReactionState, bool)> {
        if !DISCUSSION_REACTIONS.contains(&reaction) {
            return Err(Error::InvalidParams(
                "unsupported discussion reaction".into(),
            ));
        }
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        authorize(&mut tx, tenant, ws, conversation, actor).await?;
        let mut result =
            reaction_state(&mut tx, tenant, ws, conversation, message_id, actor).await?;
        if active
            && reaction == "🙂"
            && !result
                .reactions
                .iter()
                .any(|r| r.reaction == reaction && r.reacted)
        {
            return Err(Error::InvalidParams(
                "legacy smile reactions can only be removed".into(),
            ));
        }
        let changed = if active {
            let snapshot = to_json(&reactor(&mut tx, tenant, actor).await?)?;
            // Each actor kind has its own unique key; the other column is NULL.
            let sql = match actor {
                DiscussionActor::Human(_) => {
                    "INSERT INTO discussion_reaction (tenant_id, id, workspace_id, message_id, principal_id, \
                     agent_id, reaction, author_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                     ON CONFLICT (tenant_id, message_id, principal_id, reaction) DO NOTHING"
                }
                DiscussionActor::Agent(_) => {
                    "INSERT INTO discussion_reaction (tenant_id, id, workspace_id, message_id, principal_id, \
                     agent_id, reaction, author_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                     ON CONFLICT (tenant_id, message_id, agent_id, reaction) DO NOTHING"
                }
            };
            sqlx::query(sql)
                .bind(tenant)
                .bind(uuid::Uuid::now_v7().to_string())
                .bind(ws.as_str())
                .bind(message_id)
                .bind(actor.principal_id())
                .bind(actor.agent_id())
                .bind(reaction)
                .bind(snapshot)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected()
                > 0
        } else {
            sqlx::query(
                "DELETE FROM discussion_reaction WHERE tenant_id = $1 AND message_id = $2 \
                 AND (principal_id = $3 OR agent_id = $4) AND reaction = $5",
            )
            .bind(tenant)
            .bind(message_id)
            .bind(actor.principal_id())
            .bind(actor.agent_id())
            .bind(reaction)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
                > 0
        };
        if changed {
            if let Some(item) = result.reactions.iter_mut().find(|r| r.reaction == reaction) {
                item.count += if active { 1 } else { -1 };
                item.reacted = active;
            } else if active {
                result.reactions.push(DiscussionReaction {
                    reaction: reaction.into(),
                    count: 1,
                    reacted: true,
                    reactors: vec![],
                });
            }
            if let Some(item) = result.reactions.iter_mut().find(|r| r.reaction == reaction) {
                let snapshots: Vec<String> = sqlx::query_scalar(
                    "SELECT author_json FROM discussion_reaction WHERE tenant_id = $1 \
                     AND message_id = $2 AND reaction = $3 ORDER BY id COLLATE \"C\" LIMIT $4",
                )
                .bind(tenant)
                .bind(message_id)
                .bind(reaction)
                .bind(i64::from(DISCUSSION_REACTOR_MAX))
                .fetch_all(&mut *tx)
                .await
                .map_err(db)?;
                item.reactors = snapshots
                    .iter()
                    .map(|s| serde_json::from_str(s).map_err(json_err))
                    .collect::<Result<_>>()?;
            }
            result.reactions.retain(|r| r.count > 0);
            result
                .reactions
                .sort_by_key(|r| DISCUSSION_REACTIONS.iter().position(|s| *s == r.reaction));
            result.reaction_version += 1;
            let mut counts = result.reactions.clone();
            for item in &mut counts {
                item.reacted = false;
            }
            sqlx::query(
                "UPDATE workspace_discussion_message SET reactions_json = $2, reaction_version = $3 \
                 WHERE tenant_id = $1 AND id = $4",
            )
            .bind(tenant)
            .bind(to_json(&counts)?)
            .bind(result.reaction_version)
            .bind(message_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok((result, changed))
    }

    pub async fn manage_discussion(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        id: &str,
        name: Option<&str>,
    ) -> Result<DiscussionConversation> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT p.is_primary, (SELECT m.role FROM workspace_member m WHERE m.tenant_id = $1 \
             AND m.workspace_id = $2 AND m.principal_id = p.id) AS role \
             FROM principal p JOIN workspace w ON w.tenant_id = $1 AND w.id = $2 \
             WHERE p.tenant_id = $1 AND p.id = $3 AND (p.is_primary = 1 \
             OR EXISTS(SELECT 1 FROM workspace_member e WHERE e.tenant_id = $1 AND e.workspace_id = $2 AND e.principal_id = p.id) \
             OR ($2 <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id)))",
        )
        .bind(tenant)
        .bind(ws.as_str())
        .bind(principal.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound("Workspace not found".into()))?;
        let role: Option<String> = col(&row, "role")?;
        let owner = col::<i64>(&row, "is_primary")? != 0 || role.as_deref() == Some("owner");
        if !(owner || name.is_some() && role.as_deref() == Some("manager")) {
            return Err(Error::Forbidden(
                "discussion management requires the workspace owner, or a manager for rename"
                    .into(),
            ));
        }
        let name = name.map(str::trim);
        if name.is_some_and(|n| n.is_empty() || n.len() > 128) {
            return Err(Error::InvalidParams(
                "name must be nonblank and at most 128 UTF-8 bytes".into(),
            ));
        }
        let mut conversation = read_conversation(&mut tx, tenant, ws, id).await?;
        if conversation.kind != "workspace" {
            return Err(Error::Forbidden(
                "legacy private discussions cannot be managed".into(),
            ));
        }
        conversation.name = name.map(str::to_owned);
        sqlx::query(
            "INSERT INTO workspace_discussion_conversation (tenant_id, workspace_id, id, name, \
             created_at, deleted_at) VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, workspace_id, id) DO UPDATE SET name = excluded.name, \
             deleted_at = excluded.deleted_at",
        )
        .bind(tenant)
        .bind(ws.as_str())
        .bind(&conversation.id)
        .bind(&conversation.name)
        .bind(&conversation.created_at)
        .bind(name.is_none().then(now_iso))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if name.is_none() {
            sqlx::query(
                "DELETE FROM workspace_discussion_message WHERE tenant_id = $1 AND workspace_id = $2 \
                 AND conversation_id = $3",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(&conversation.id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            sqlx::query(
                "DELETE FROM discussion_agent_grant WHERE tenant_id = $1 AND workspace_id = $2 \
                 AND conversation_id = $3",
            )
            .bind(tenant)
            .bind(ws.as_str())
            .bind(&conversation.id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(conversation)
    }
}
