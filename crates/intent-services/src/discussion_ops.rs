//! Human-only workspace discussion operations; all transports share these gates.
use intent_core::discussion::{
    DiscussionAuthor, DiscussionMessage, DiscussionPage, DISCUSSION_BODY_MAX_BYTES,
};
use intent_core::events::DISCUSSION_MESSAGE;
use intent_core::{
    current_caller, ActorType, Caller, Error, EventActor, PrincipalId, Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::json;

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

    pub(crate) async fn discussion_list_op(
        &self,
        workspace_id: &WorkspaceId,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        self.discussion_human(workspace_id).await?;
        self.store
            .list_discussion_messages(workspace_id, before_sequence, after_sequence, limit)
            .await
    }

    pub(crate) async fn discussion_send_op(
        &self,
        workspace_id: &WorkspaceId,
        message_id: &str,
        body: &str,
    ) -> Result<DiscussionMessage> {
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
        let principal = self.store.get_principal(&principal_id).await?;
        let actor_name = crate::principal_ops::principal_attribution_name(&principal);
        let author = DiscussionAuthor {
            principal_id,
            login: principal.login,
            display_name: principal.display_name,
            avatar_url: principal.avatar_url,
        };
        let (message, inserted) = self
            .store
            .insert_discussion_message(workspace_id, message_id, body, &author)
            .await?;
        if inserted {
            // History is committed above. Broadcast its full snapshot without
            // duplicating human message bodies in the durable event log.
            crate::publish_event_transient(
                self.event_bus.as_ref(),
                &NewEvent {
                    workspace_id: workspace_id.clone(),
                    timestamp: message.created_at.clone(),
                    event_type: DISCUSSION_MESSAGE.into(),
                    actor: EventActor {
                        actor_type: ActorType::User,
                        id: Some(author.principal_id.0),
                        name: Some(actor_name),
                        ..EventActor::default()
                    },
                    session_id: None,
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: json!({"workspaceId":workspace_id,"message":message}),
                },
            );
        }
        Ok(message)
    }
}

#[cfg(test)]
mod tests;
