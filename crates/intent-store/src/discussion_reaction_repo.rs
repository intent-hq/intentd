//! Durable caller-bound reactions with bounded, viewer-specific projections.
use crate::Store;
use intent_core::discussion::{
    DiscussionActor, DiscussionAuthor, DiscussionReaction, DiscussionReactionState,
    DISCUSSION_REACTIONS, DISCUSSION_REACTOR_MAX,
};
use intent_core::{Error, PrincipalId, Result, WorkspaceId};
use sqlx::{Row, SqliteConnection};

fn db(e: &sqlx::Error) -> Error {
    Error::Internal(format!("discussion reaction: {e}"))
}
fn missing() -> Error {
    Error::NotFound("Discussion message not found".into())
}

async fn authorize_human(
    c: &mut SqliteConnection,
    ws: &WorkspaceId,
    conversation: &str,
    principal: &PrincipalId,
) -> Result<()> {
    let sql = format!("SELECT EXISTS(SELECT 1 FROM principal p JOIN workspace w ON w.id=? WHERE p.id=? AND {} AND (?='default' OR EXISTS(SELECT 1 FROM discussion_participant d WHERE d.workspace_id=w.id AND d.conversation_id=? AND d.principal_id=p.id)))", super::discussion_conversation_repo::ELIGIBLE);
    let allowed: bool = sqlx::query_scalar(&sql)
        .bind(ws.as_str())
        .bind(principal.as_str())
        .bind(ws.as_str())
        .bind(ws.as_str())
        .bind(conversation)
        .bind(conversation)
        .fetch_one(c)
        .await
        .map_err(|e| db(&e))?;
    if allowed {
        Ok(())
    } else {
        Err(missing())
    }
}

async fn authorize(
    c: &mut SqliteConnection,
    ws: &WorkspaceId,
    conversation: &str,
    actor: &DiscussionActor,
) -> Result<()> {
    match actor {
        DiscussionActor::Human(principal) => authorize_human(c, ws, conversation, principal).await,
        DiscussionActor::Agent(agent) => {
            if super::discussion_agent_repo::agent_access_in(c, ws, conversation, agent).await? {
                Ok(())
            } else {
                Err(missing())
            }
        }
    }
}

async fn author(c: &mut SqliteConnection, actor: &DiscussionActor) -> Result<DiscussionAuthor> {
    match actor {
        DiscussionActor::Human(principal) => {
            let row = sqlx::query("SELECT login,display_name,avatar_url FROM principal WHERE id=?")
                .bind(principal.as_str())
                .fetch_one(c)
                .await
                .map_err(|e| db(&e))?;
            Ok(DiscussionAuthor {
                principal_id: Some(principal.clone()),
                agent_id: None,
                login: row.get("login"),
                display_name: row.get("display_name"),
                avatar_url: row.get("avatar_url"),
            })
        }
        DiscussionActor::Agent(agent) => {
            let name = sqlx::query_scalar("SELECT name FROM agent_session WHERE id=?")
                .bind(agent.as_str())
                .fetch_one(c)
                .await
                .map_err(|e| db(&e))?;
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

async fn state(
    c: &mut SqliteConnection,
    ws: &WorkspaceId,
    conversation: &str,
    message: &str,
    actor: &DiscussionActor,
) -> Result<DiscussionReactionState> {
    let row = sqlx::query("SELECT m.reactions_json,m.reaction_version,(SELECT json_group_array(reaction) FROM discussion_reaction r WHERE r.message_id=m.id AND (r.principal_id=? OR r.agent_id=?)) AS selected FROM workspace_discussion_message m WHERE m.workspace_id=? AND m.conversation_id=? AND m.id=?")
        .bind(actor.principal_id()).bind(actor.agent_id()).bind(ws.as_str()).bind(conversation).bind(message).fetch_optional(c).await.map_err(|e| db(&e))?.ok_or_else(missing)?;
    let mut reactions: Vec<DiscussionReaction> = serde_json::from_str(row.get("reactions_json"))
        .map_err(|e| Error::Internal(e.to_string()))?;
    let selected: Vec<String> =
        serde_json::from_str(row.get("selected")).map_err(|e| Error::Internal(e.to_string()))?;
    for reaction in &mut reactions {
        reaction.reacted = selected.contains(&reaction.reaction);
    }
    Ok(DiscussionReactionState {
        message_id: message.into(),
        reactions,
        reaction_version: row.get("reaction_version"),
    })
}

impl Store {
    /// Read one human viewer's selected reactions.
    /// # Errors
    /// Returns not-found for inaccessible targets or database errors.
    pub async fn discussion_reaction_state(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        principal: &PrincipalId,
    ) -> Result<DiscussionReactionState> {
        self.discussion_actor_reaction_state(
            ws,
            conversation,
            message,
            &DiscussionActor::Human(principal.clone()),
        )
        .await
    }

    /// Set a human viewer's selection; identity snapshots come from the store.
    /// # Errors
    /// Returns invalid reaction, inaccessible target or database errors.
    pub async fn set_discussion_reaction(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        principal: &PrincipalId,
        reaction: &str,
        active: bool,
    ) -> Result<(DiscussionReactionState, bool)> {
        self.set_discussion_actor_reaction(
            ws,
            conversation,
            message,
            &DiscussionActor::Human(principal.clone()),
            reaction,
            active,
        )
        .await
    }
    /// Read counts, version and this viewer's choices from one snapshot.
    /// # Errors
    /// Returns not-found for inaccessible targets or database errors.
    pub async fn discussion_actor_reaction_state(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        actor: &DiscussionActor,
    ) -> Result<DiscussionReactionState> {
        let mut tx = self.read_pool().begin().await.map_err(|e| db(&e))?;
        authorize(&mut tx, ws, conversation, actor).await?;
        let result = state(&mut tx, ws, conversation, message, actor).await?;
        tx.commit().await.map_err(|e| db(&e))?;
        Ok(result)
    }

    /// Set one authenticated caller's desired reaction state atomically.
    /// No-op retries do not advance the version or require a notification.
    /// # Errors
    /// Returns invalid reaction, inaccessible target or database errors.
    pub async fn set_discussion_actor_reaction(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        actor: &DiscussionActor,
        reaction: &str,
        active: bool,
    ) -> Result<(DiscussionReactionState, bool)> {
        if !DISCUSSION_REACTIONS.contains(&reaction) {
            return Err(Error::InvalidParams(
                "unsupported discussion reaction".into(),
            ));
        }
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        authorize(&mut tx, ws, conversation, actor).await?;
        let mut result = state(&mut tx, ws, conversation, message, actor).await?;
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
            let snapshot = serde_json::to_string(&author(&mut tx, actor).await?)
                .map_err(|e| Error::Internal(e.to_string()))?;
            sqlx::query("INSERT INTO discussion_reaction(id,workspace_id,message_id,principal_id,agent_id,reaction,author_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING")
                .bind(uuid::Uuid::now_v7().to_string()).bind(ws.as_str()).bind(message).bind(actor.principal_id()).bind(actor.agent_id()).bind(reaction).bind(snapshot).execute(&mut *tx).await.map_err(|e| db(&e))?.rows_affected()>0
        } else {
            sqlx::query("DELETE FROM discussion_reaction WHERE message_id=? AND (principal_id=? OR agent_id=?) AND reaction=?")
                .bind(message).bind(actor.principal_id()).bind(actor.agent_id()).bind(reaction).execute(&mut *tx).await.map_err(|e| db(&e))?.rows_affected()>0
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
            // Only refresh the changed sample, using an index and a bounded fetch.
            // Inert imported rows participate in display but never in local selection.
            if let Some(item) = result.reactions.iter_mut().find(|r| r.reaction == reaction) {
                let snapshots: Vec<String> = sqlx::query_scalar("SELECT author_json FROM discussion_reaction WHERE message_id=? AND reaction=? ORDER BY id LIMIT ?")
                    .bind(message).bind(reaction).bind(i64::from(DISCUSSION_REACTOR_MAX))
                    .fetch_all(&mut *tx).await.map_err(|e| db(&e))?;
                item.reactors = snapshots
                    .iter()
                    .map(|s| serde_json::from_str(s).map_err(|e| Error::Internal(e.to_string())))
                    .collect::<Result<_>>()?;
            }
            result.reactions.retain(|r| r.count > 0);
            result
                .reactions
                .sort_by_key(|r| DISCUSSION_REACTIONS.iter().position(|s| *s == r.reaction));
            result.reaction_version += 1;
            // Counts are write-maintained; never aggregate an unbounded reactor
            // roster on history reads. Personal selection is projected separately.
            let mut counts = result.reactions.clone();
            for item in &mut counts {
                item.reacted = false;
            }
            sqlx::query("UPDATE workspace_discussion_message SET reactions_json=?,reaction_version=? WHERE id=?")
                .bind(serde_json::to_string(&counts).map_err(|e|Error::Internal(e.to_string()))?).bind(result.reaction_version).bind(message).execute(&mut *tx).await.map_err(|e| db(&e))?;
        }
        tx.commit().await.map_err(|e| db(&e))?;
        Ok((result, changed))
    }
}
