//! Human-only workspace discussion operations; all transports share these gates.
use intent_core::discussion::{
    DiscussionAuthor, DiscussionSendResult, DiscussionTarget, DEFAULT_CONVERSATION,
    DISCUSSION_BODY_MAX_BYTES, DISCUSSION_RECIPIENT_MAX,
};
use intent_core::events::{DISCUSSION_MESSAGE, DISCUSSION_REACTION};
use intent_core::{
    current_caller, ActorType, Caller, Error, EventActor, PrincipalId, Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};

use crate::Services;

impl Services {
    async fn discussion_human(&self, workspace_id: &WorkspaceId) -> Result<PrincipalId> {
        let Some(Caller::Wire { principal_id, .. }) = current_caller() else {
            return Err(Error::Forbidden(
                "workspace discussions require a human connection".into(),
            ));
        };
        self.require_member(workspace_id).await?;
        self.store.get_workspace(workspace_id).await?;
        Ok(principal_id)
    }

    #[cfg(test)]
    pub(crate) async fn discussion_list_op(
        &self,
        workspace_id: &WorkspaceId,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<intent_core::discussion::DiscussionPage> {
        self.discussion_human(workspace_id).await?;
        self.store
            .list_discussion_messages(workspace_id, before_sequence, after_sequence, limit)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn discussion_send_op(
        &self,
        workspace_id: &WorkspaceId,
        message_id: &str,
        body: &str,
    ) -> Result<intent_core::discussion::DiscussionMessage> {
        Ok(self
            .discussion_send_to_op(workspace_id, message_id, body, &DiscussionTarget::default())
            .await?
            .message)
    }

    pub(crate) async fn discussion_access(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &str,
    ) -> Result<PrincipalId> {
        let principal = self.discussion_human(workspace_id).await?;
        if conversation_id != DEFAULT_CONVERSATION
            && !self
                .store
                .discussion_is_participant(workspace_id, conversation_id, &principal)
                .await?
        {
            return Err(Error::NotFound("Discussion not found".into()));
        }
        Ok(principal)
    }

    pub(crate) async fn discussion_history_op(
        &self,
        ws: &WorkspaceId,
        id: &str,
        before: Option<i64>,
        after: Option<i64>,
        limit: u32,
    ) -> Result<Value> {
        let principal = self.discussion_access(ws, id).await?;
        let page = self
            .store
            .list_discussion_messages_for(ws, id, before, after, limit, Some(&principal))
            .await?;
        let conversation = self.store.discussion_conversation(ws, id).await?;
        Ok(json!({"messages":page.messages,"hasMore":page.has_more,"conversation":conversation}))
    }

    pub(crate) async fn discussion_conversations_op(
        &self,
        ws: &WorkspaceId,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Value> {
        let principal = self.discussion_human(ws).await?;
        self.store
            .discussion_conversations(ws, &principal, before, limit)
            .await
    }

    pub(crate) async fn discussion_recipients_op(
        &self,
        ws: &WorkspaceId,
        query: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Value> {
        let principal = self.discussion_human(ws).await?;
        self.store
            .discussion_recipients(ws, &principal, query, after, limit)
            .await
    }

    pub(crate) async fn discussion_send_to_op(
        &self,
        workspace_id: &WorkspaceId,
        message_id: &str,
        body: &str,
        target: &DiscussionTarget,
    ) -> Result<DiscussionSendResult> {
        let principal_id = self.discussion_human(workspace_id).await?;
        if message_id.trim().is_empty() || message_id.len() > 128 {
            return Err(Error::InvalidParams(
                "messageId must be nonblank and at most 128 UTF-8 bytes".into(),
            ));
        }
        if body.trim().is_empty() || body.len() > DISCUSSION_BODY_MAX_BYTES {
            return Err(Error::InvalidParams(
                "body must be nonblank and at most 16384 UTF-8 bytes".into(),
            ));
        }
        if target
            .reply_to_message_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty() || id.len() > 128)
        {
            return Err(Error::InvalidParams(
                "replyToMessageId must be nonblank and at most 128 UTF-8 bytes".into(),
            ));
        }
        if target.conversation_id.is_some() && target.recipient_principal_ids.is_some() {
            return Err(Error::InvalidParams(
                "conversationId and recipientPrincipalIds are mutually exclusive".into(),
            ));
        }
        if let Some(recipients) = &target.recipient_principal_ids {
            let unique: std::collections::HashSet<_> = recipients.iter().collect();
            if recipients.is_empty()
                || recipients.len() > DISCUSSION_RECIPIENT_MAX
                || unique.len() != recipients.len()
                || recipients.contains(&principal_id)
                || recipients.iter().any(|p| p.as_str().is_empty())
            {
                return Err(Error::InvalidParams(
                    "recipientPrincipalIds must contain 1..31 distinct other workspace members"
                        .into(),
                ));
            }
        } else {
            self.discussion_access(
                workspace_id,
                target
                    .conversation_id
                    .as_deref()
                    .unwrap_or(DEFAULT_CONVERSATION),
            )
            .await?;
        }
        let principal = self.store.get_principal(&principal_id).await?;
        let actor_name = crate::principal_ops::principal_attribution_name(&principal);
        let author = DiscussionAuthor {
            principal_id,
            login: principal.login,
            display_name: principal.display_name,
            avatar_url: principal.avatar_url,
        };
        let (mut result, inserted) = self
            .store
            .send_discussion_message(workspace_id, message_id, body, &author, target)
            .await?;
        if inserted {
            // History is committed above. Broadcast its full snapshot without
            // duplicating human message bodies in the durable event log.
            crate::publish_event_transient(
                self.event_bus.as_ref(),
                &NewEvent {
                    workspace_id: workspace_id.clone(),
                    timestamp: result.message.created_at.clone(),
                    event_type: DISCUSSION_MESSAGE.into(),
                    actor: EventActor {
                        actor_type: ActorType::User,
                        id: Some(author.principal_id.0.clone()),
                        name: Some(actor_name),
                        ..EventActor::default()
                    },
                    session_id: None,
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: json!({"workspaceId":workspace_id,"conversationId":result.conversation.id,"conversation":result.conversation,"message":result.message}),
                },
            );
        }
        let state = self
            .store
            .discussion_reaction_state(
                workspace_id,
                &result.message.conversation_id,
                &result.message.id,
                &author.principal_id,
            )
            .await?;
        result.message.reactions = state.reactions;
        result.message.reaction_version = state.reaction_version;
        Ok(result)
    }

    pub(crate) async fn discussion_react_op(
        &self,
        ws: &WorkspaceId,
        conversation: &str,
        message: &str,
        reaction: &str,
        active: bool,
    ) -> Result<intent_core::discussion::DiscussionReactionState> {
        let principal = self.discussion_access(ws, conversation).await?;
        let (state, changed) = self
            .store
            .set_discussion_reaction(ws, conversation, message, &principal, reaction, active)
            .await?;
        if changed {
            // Only the target identity crosses the bus. Delivery projects the
            // current committed version and personal selection for each viewer.
            crate::publish_event_transient(
                self.event_bus.as_ref(),
                &NewEvent {
                    workspace_id: ws.clone(),
                    timestamp: intent_core::now_iso(),
                    event_type: DISCUSSION_REACTION.into(),
                    actor: EventActor {
                        actor_type: ActorType::User,
                        ..EventActor::default()
                    },
                    session_id: None,
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: json!({"workspaceId":ws,"conversationId":conversation,"messageId":message}),
                },
            );
        }
        Ok(state)
    }
    /// Projection is applied for every human transport, including owners.
    pub(crate) async fn discussion_project_event_op(
        &self,
        mut event: intent_core::Event,
    ) -> Result<Option<intent_core::Event>> {
        if event.event_type.starts_with("discussion:") {
            let id = event
                .data
                .get("conversationId")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_CONVERSATION);
            let Ok(principal) = self.discussion_access(&event.workspace_id, id).await else {
                return Ok(None);
            };
            let message_id = if event.event_type == DISCUSSION_REACTION {
                event.data.get("messageId").and_then(Value::as_str)
            } else {
                event
                    .data
                    .get("message")
                    .and_then(|m| m.get("id"))
                    .and_then(Value::as_str)
            };
            if let Some(message_id) = message_id {
                let Ok(state) = self
                    .store
                    .discussion_reaction_state(&event.workspace_id, id, message_id, &principal)
                    .await
                else {
                    return Ok(None);
                };
                if event.event_type == DISCUSSION_REACTION {
                    event.data = json!({"workspaceId":event.workspace_id,"conversationId":id,"messageId":state.message_id,"reactions":state.reactions,"reactionVersion":state.reaction_version});
                } else {
                    event.data["message"]["reactions"] = json!(state.reactions);
                    event.data["message"]["reactionVersion"] = json!(state.reaction_version);
                }
            }
        } else if event.event_type == intent_core::events::PRESENCE_CHANGED {
            if !matches!(current_caller(), Some(Caller::Wire { .. })) {
                return Ok(None);
            }
            self.require_member(&event.workspace_id).await?;
            self.project_discussion_typing(&event.workspace_id, &mut event.data)
                .await?;
        }
        Ok(Some(event))
    }

    pub(crate) async fn project_discussion_typing(
        &self,
        ws: &WorkspaceId,
        data: &mut Value,
    ) -> Result<()> {
        let Some(members) = data.get_mut("members").and_then(Value::as_array_mut) else {
            return Ok(());
        };
        let Some(Caller::Wire { principal_id, .. }) = current_caller() else {
            for member in members {
                if let Some(obj) = member.as_object_mut() {
                    obj.remove("discussionTyping");
                }
            }
            return Ok(());
        };
        let ids: Vec<String> = members
            .iter()
            .flat_map(|m| m["discussionTyping"].as_array().into_iter().flatten())
            .filter_map(|t| t["conversationId"].as_str())
            .filter(|id| *id != DEFAULT_CONVERSATION)
            .map(str::to_string)
            .collect();
        let visible = if ids.is_empty() {
            Vec::new()
        } else {
            self.store
                .discussion_visible_ids(ws, &principal_id, &ids)
                .await?
        };
        for member in members {
            if let Some(entries) = member
                .get_mut("discussionTyping")
                .and_then(Value::as_array_mut)
            {
                entries.retain(|entry| {
                    entry["conversationId"].as_str().is_some_and(|id| {
                        id == DEFAULT_CONVERSATION || visible.iter().any(|v| v == id)
                    })
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
