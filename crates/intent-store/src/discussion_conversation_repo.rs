//! Participant-scoped discovery and atomic first-message creation.
use crate::Store;
use intent_core::discussion::{
    DiscussionAuthor, DiscussionConversation, DiscussionSendResult, DiscussionTarget,
    DEFAULT_CONVERSATION,
};
use intent_core::{now_iso, Error, PrincipalId, Result, WorkspaceId};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

fn db(e: &sqlx::Error) -> Error {
    Error::Internal(format!("discussion conversation: {e}"))
}
fn missing() -> Error {
    Error::NotFound("Discussion not found".into())
}
fn profile(r: &SqliteRow) -> DiscussionAuthor {
    DiscussionAuthor {
        principal_id: Some(PrincipalId(r.get("id"))),
        agent_id: None,
        login: r.get("login"),
        display_name: r.get("display_name"),
        avatar_url: r.get("avatar_url"),
    }
}
fn conversation(r: &SqliteRow) -> Result<DiscussionConversation> {
    Ok(DiscussionConversation {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        kind: r.get("kind"),
        participants: serde_json::from_str(r.get("participants_json"))
            .map_err(|e| Error::Internal(e.to_string()))?,
        created_at: r.get("created_at"),
    })
}
async fn read_conversation(
    c: &mut SqliteConnection,
    ws: &WorkspaceId,
    id: &str,
) -> Result<DiscussionConversation> {
    if id == DEFAULT_CONVERSATION {
        let created_at = sqlx::query_scalar("SELECT created_at FROM workspace WHERE id=?")
            .bind(ws.as_str())
            .fetch_optional(&mut *c)
            .await
            .map_err(|e| db(&e))?
            .ok_or_else(missing)?;
        return Ok(DiscussionConversation {
            id: id.into(),
            workspace_id: ws.clone(),
            kind: "workspace".into(),
            participants: vec![],
            created_at,
        });
    }
    let row = sqlx::query("SELECT * FROM discussion_conversation WHERE workspace_id=? AND id=?")
        .bind(ws.as_str())
        .bind(id)
        .fetch_optional(c)
        .await
        .map_err(|e| db(&e))?
        .ok_or_else(missing)?;
    conversation(&row)
}
pub(super) const ELIGIBLE: &str = "(p.is_primary=1 OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.workspace_id=? AND m.principal_id=p.id) OR (? <> '__chief__' AND EXISTS(SELECT 1 FROM host_member h WHERE h.principal_id=p.id)))";

impl Store {
    /// Transfer preflight must not expose private history through an archive.
    /// # Errors
    /// Returns a database error.
    pub async fn has_private_discussions(&self, ws: &WorkspaceId) -> Result<bool> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_conversation WHERE workspace_id=?) OR EXISTS(SELECT 1 FROM workspace_discussion_message WHERE workspace_id=? AND conversation_id<>'default')")
            .bind(ws.as_str()).bind(ws.as_str()).fetch_one(self.read_pool()).await.map_err(|e|db(&e))
    }

    /// Read metadata after caller authorization.
    /// # Errors
    /// Returns not-found or a database error.
    pub async fn discussion_conversation(
        &self,
        ws: &WorkspaceId,
        id: &str,
    ) -> Result<DiscussionConversation> {
        let mut conn = self.read_pool().acquire().await.map_err(|e| db(&e))?;
        read_conversation(&mut conn, ws, id).await
    }

    /// Check local participant grants; imported source IDs are never authority.
    /// # Errors
    /// Returns a database error.
    pub async fn discussion_is_participant(
        &self,
        ws: &WorkspaceId,
        id: &str,
        principal: &PrincipalId,
    ) -> Result<bool> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE workspace_id=? AND conversation_id=? AND principal_id=?)").bind(ws.as_str()).bind(id).bind(principal.as_str()).fetch_one(self.read_pool()).await.map_err(|e|db(&e))
    }

    /// Filter a roster's conversation IDs in one bounded query.
    /// # Errors
    /// Returns serialization or database errors.
    pub async fn discussion_visible_ids(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        ids: &[String],
    ) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT conversation_id FROM discussion_participant WHERE workspace_id=? AND principal_id=? AND conversation_id IN (SELECT value FROM json_each(?))").bind(ws.as_str()).bind(principal.as_str()).bind(serde_json::to_string(ids).map_err(|e|Error::Internal(e.to_string()))?).fetch_all(self.read_pool()).await.map_err(|e|db(&e))
    }

    /// Bounded private conversation discovery with a leading default sentinel.
    /// # Errors
    /// Returns invalid bounds or database errors.
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
        let include_default = before.is_none();
        let cursor = before
            .filter(|id| *id != DEFAULT_CONVERSATION)
            .unwrap_or("~");
        let rows=sqlx::query("SELECT c.* FROM discussion_conversation c WHERE c.workspace_id=? AND c.id < ? AND EXISTS(SELECT 1 FROM discussion_participant p WHERE p.conversation_id=c.id AND p.principal_id=?) ORDER BY c.id DESC LIMIT ?").bind(ws.as_str()).bind(cursor).bind(principal.as_str()).bind(i64::from(limit)+1).fetch_all(self.read_pool()).await.map_err(|e|db(&e))?;
        let mut conversations = Vec::new();
        if include_default {
            conversations.push(
                self.discussion_conversation(ws, DEFAULT_CONVERSATION)
                    .await?,
            );
        }
        conversations.extend(rows.iter().map(conversation).collect::<Result<Vec<_>>>()?);
        let has_more = conversations.len() > limit as usize;
        conversations.truncate(limit as usize);
        Ok(json!({"conversations":conversations,"hasMore":has_more}))
    }

    /// Bounded effective member picker, independent of online presence.
    /// # Errors
    /// Returns invalid bounds or database errors.
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
        let sql=format!("SELECT p.id,p.login,p.display_name,p.avatar_url FROM principal p WHERE p.id<>? AND p.id>? AND {ELIGIBLE} AND instr(lower(coalesce(p.login,'') || ' ' || coalesce(p.display_name,'') || ' ' || p.id),lower(?))>0 ORDER BY p.id LIMIT ?");
        let rows = sqlx::query(&sql)
            .bind(principal.as_str())
            .bind(after.unwrap_or(""))
            .bind(ws.as_str())
            .bind(ws.as_str())
            .bind(query)
            .bind(i64::from(limit) + 1)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| db(&e))?;
        let has_more = rows.len() > limit as usize;
        let members: Vec<_> = rows.iter().take(limit as usize).map(profile).collect();
        Ok(json!({"members":members,"hasMore":has_more}))
    }

    /// Atomically create a conversation on its first message or append/retry.
    /// # Errors
    /// Rejects target/body retry mismatches, missing members, and database failures.
    pub async fn send_discussion_message(
        &self,
        ws: &WorkspaceId,
        key: &str,
        body: &str,
        author: &DiscussionAuthor,
        target: &DiscussionTarget,
    ) -> Result<(DiscussionSendResult, bool)> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        // Recheck after acquiring the serialized writer: a removal that won
        // the write race must revoke appends and retries, including default.
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
        // Ordinary agent transcripts are workspace-visible. Private discussion
        // content must never enter that wider surface.
        if (agent_id.is_some() || !target.recipient_agent_ids.is_empty())
            && (conversation_id != DEFAULT_CONVERSATION || target.recipient_principal_ids.is_some())
        {
            return Err(Error::Forbidden(
                "agent messaging is available only in the workspace discussion".into(),
            ));
        }
        intent_core::discussion::validate_agent_mentions(body, target)?;
        let mut recipients = target.recipient_agent_ids.clone();
        recipients.sort();
        if recipients.len() > intent_core::discussion::DISCUSSION_RECIPIENT_MAX
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
            if !super::discussion_agent_repo::agent_access_in(&mut tx, ws, conversation_id, agent)
                .await?
            {
                return Err(missing());
            }
        } else {
            let membership_sql = format!("SELECT EXISTS(SELECT 1 FROM principal p JOIN workspace w ON w.id=? WHERE p.id=? AND {ELIGIBLE})");
            let member: bool = sqlx::query_scalar(&membership_sql)
                .bind(ws.as_str())
                .bind(principal_id.map(PrincipalId::as_str))
                .bind(ws.as_str())
                .bind(ws.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| db(&e))?;
            if !member {
                return Err(missing());
            }
        }
        let old=sqlx::query("SELECT * FROM workspace_discussion_message WHERE workspace_id=? AND principal_id IS ? AND agent_id IS ? AND client_message_id=?").bind(ws.as_str()).bind(principal_id.map(PrincipalId::as_str)).bind(agent_id.map(intent_core::AgentId::as_str)).bind(key).fetch_optional(&mut *tx).await.map_err(|e|db(&e))?;
        if let Some(row) = old {
            let message = super::discussion_repo::message(&row)?;
            let conversation = read_conversation(&mut tx, ws, &message.conversation_id).await?;
            let same_target = if let Some(recipients) = &target.recipient_principal_ids {
                let mut ids: Vec<_> = conversation
                    .participants
                    .iter()
                    .filter_map(|p| p.principal_id.clone())
                    .collect();
                ids.sort();
                let mut wanted = recipients.clone();
                wanted.push(principal_id.ok_or_else(missing)?.clone());
                wanted.sort();
                ids == wanted && conversation.kind != "workspace"
            } else {
                target
                    .conversation_id
                    .as_deref()
                    .unwrap_or(DEFAULT_CONVERSATION)
                    == message.conversation_id
            };
            if message.agent_mentions != target.agent_mentions
                || message.recipient_agent_ids != recipients
                || message.body != body
                || !same_target
                || message.reply_to_message_id != target.reply_to_message_id
            {
                return Err(Error::InvalidParams(
                    "messageId was already used with a different body, conversation or reply target".into(),
                ));
            }
            if message.conversation_id != DEFAULT_CONVERSATION {
                let allowed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE conversation_id=? AND principal_id=?)").bind(&message.conversation_id).bind(principal_id.map(PrincipalId::as_str)).fetch_one(&mut *tx).await.map_err(|e|db(&e))?;
                if !allowed {
                    return Err(missing());
                }
            }
            tx.commit().await.map_err(|e| db(&e))?;
            return Ok((
                DiscussionSendResult {
                    message,
                    conversation,
                },
                false,
            ));
        }
        for recipient in &recipients {
            if !super::discussion_agent_repo::agent_eligible_in(&mut tx, ws, recipient).await? {
                return Err(missing());
            }
        }
        let conversation = if let Some(recipients) = &target.recipient_principal_ids {
            let mut ids = recipients.clone();
            ids.push(principal_id.ok_or_else(missing)?.clone());
            ids.sort();
            let sql=format!("SELECT p.id,p.login,p.display_name,p.avatar_url FROM principal p WHERE p.id IN (SELECT value FROM json_each(?)) AND {ELIGIBLE} ORDER BY p.id");
            let rows = sqlx::query(&sql)
                .bind(serde_json::to_string(&ids).map_err(|e| Error::Internal(e.to_string()))?)
                .bind(ws.as_str())
                .bind(ws.as_str())
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| db(&e))?;
            if rows.len() != ids.len() {
                return Err(missing());
            }
            let conversation = DiscussionConversation {
                id: uuid::Uuid::now_v7().to_string(),
                workspace_id: ws.clone(),
                kind: if ids.len() == 2 { "direct" } else { "group" }.into(),
                participants: rows.iter().map(profile).collect(),
                created_at: now_iso(),
            };
            sqlx::query("INSERT INTO discussion_conversation(id,workspace_id,kind,participants_json,created_at) VALUES(?,?,?,?,?)").bind(&conversation.id).bind(ws.as_str()).bind(&conversation.kind).bind(serde_json::to_string(&conversation.participants).map_err(|e|Error::Internal(e.to_string()))?).bind(&conversation.created_at).execute(&mut *tx).await.map_err(|e|db(&e))?;
            for id in ids {
                sqlx::query("INSERT INTO discussion_participant(conversation_id,workspace_id,source_principal_id,principal_id) VALUES(?,?,?,?)").bind(&conversation.id).bind(ws.as_str()).bind(id.as_str()).bind(id.as_str()).execute(&mut *tx).await.map_err(|e|db(&e))?;
            }
            conversation
        } else {
            let id = target
                .conversation_id
                .as_deref()
                .unwrap_or(DEFAULT_CONVERSATION);
            if id != DEFAULT_CONVERSATION {
                let allowed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM discussion_participant WHERE workspace_id=? AND conversation_id=? AND principal_id=?)").bind(ws.as_str()).bind(id).bind(principal_id.map(PrincipalId::as_str)).fetch_one(&mut *tx).await.map_err(|e|db(&e))?;
                if !allowed {
                    return Err(missing());
                }
            }
            read_conversation(&mut tx, ws, id).await?
        };
        let reply = if let Some(reply_id) = &target.reply_to_message_id {
            let row = sqlx::query("SELECT * FROM workspace_discussion_message WHERE workspace_id=? AND conversation_id=? AND id=?")
                .bind(ws.as_str()).bind(&conversation.id).bind(reply_id).fetch_optional(&mut *tx).await.map_err(|e|db(&e))?.ok_or_else(missing)?;
            let original = super::discussion_repo::message(&row)?;
            Some(
                serde_json::to_string(&intent_core::discussion::DiscussionReply {
                    id: original.id,
                    author: original.author,
                    body: original.body,
                })
                .map_err(|e| Error::Internal(e.to_string()))?,
            )
        } else {
            None
        };
        let id = uuid::Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO workspace_discussion_message(id,workspace_id,conversation_id,sequence,principal_id,client_message_id,body,author_json,created_at,reply_to_message_id,reply_to_json) VALUES(?,?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM workspace_discussion_message WHERE workspace_id=? AND conversation_id=?),?,?,?,?,?,?,?)").bind(&id).bind(ws.as_str()).bind(&conversation.id).bind(ws.as_str()).bind(&conversation.id).bind(principal_id.map(PrincipalId::as_str)).bind(key).bind(body).bind(serde_json::to_string(author).map_err(|e|Error::Internal(e.to_string()))?).bind(now_iso()).bind(&target.reply_to_message_id).bind(reply).execute(&mut *tx).await.map_err(|e|db(&e))?;
        sqlx::query("UPDATE workspace_discussion_message SET agent_id=?,recipient_agent_ids_json=?,agent_mentions_json=? WHERE id=?")
            .bind(agent_id.map(intent_core::AgentId::as_str))
            .bind(serde_json::to_string(&recipients).map_err(|e| Error::Internal(e.to_string()))?)
            .bind(serde_json::to_string(&target.agent_mentions).map_err(|e| Error::Internal(e.to_string()))?).bind(&id).execute(&mut *tx).await.map_err(|e| db(&e))?;
        for recipient in &recipients {
            super::discussion_activity_repo::insert(&mut tx, &id, recipient, &now_iso()).await?;
            sqlx::query("INSERT OR IGNORE INTO discussion_agent_grant(workspace_id,conversation_id,agent_id) VALUES(?,?,?)")
                .bind(ws.as_str()).bind(&conversation.id).bind(recipient.as_str())
                .execute(&mut *tx).await.map_err(|e| db(&e))?;
            sqlx::query("INSERT INTO discussion_agent_delivery(message_id,agent_id,workspace_id) VALUES(?,?,?)")
                .bind(&id).bind(recipient.as_str()).bind(ws.as_str())
                .execute(&mut *tx).await.map_err(|e| db(&e))?;
        }
        super::discussion_activity_repo::refresh(&mut tx, &id).await?;
        let row = sqlx::query("SELECT * FROM workspace_discussion_message WHERE id=?")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        let message = super::discussion_repo::message(&row)?;
        tx.commit().await.map_err(|e| db(&e))?;
        Ok((
            DiscussionSendResult {
                message,
                conversation,
            },
            true,
        ))
    }
}
