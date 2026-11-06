//! Correlated controls; human discussion access never grants general agent control.
use crate::{agent_manager::TurnOptions, Services};
use intent_core::{discussion::DiscussionAgentControl, Error, EventActor, Result};
use intent_store::{DiscussionActivityReceipt, NewEvent};
use serde_json::{json, Value};

pub(crate) fn is_discussion(metadata: Option<&Value>) -> bool {
    metadata.and_then(|m| m.get("type")).and_then(Value::as_str) == Some("discussion_message")
}

impl Services {
    pub(crate) fn publish_discussion_activity(&self, receipt: &DiscussionActivityReceipt) {
        crate::publish_event_transient(
            self.event_bus.as_ref(),
            &NewEvent {
                workspace_id: receipt.workspace_id.clone(),
                timestamp: receipt.activity.updated_at.clone(),
                event_type: intent_core::events::DISCUSSION_ACTIVITY.into(),
                actor: EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({"workspaceId":receipt.workspace_id,"conversationId":receipt.conversation_id,"messageId":receipt.message_id,"activity":receipt.activity}),
            },
        );
    }

    pub(crate) async fn set_discussion_activity(
        &self,
        turn: &str,
        state: &str,
        error: Option<&str>,
    ) {
        match self
            .store
            .transition_discussion_activity(turn, state, error)
            .await
        {
            Ok(Some(receipt)) => self.publish_discussion_activity(&receipt),
            Ok(None) => {}
            Err(error) => tracing::error!(%error, %turn, "discussion activity persistence failed"),
        }
    }

    pub(crate) async fn settle_discussion_options(
        &self,
        options: &TurnOptions,
        state: &str,
        error: Option<&str>,
    ) {
        if is_discussion(options.message_metadata.as_ref()) {
            if let Some(turn) = &options.turn_id {
                self.set_discussion_activity(turn, state, error).await;
            }
        }
    }

    pub(crate) async fn discussion_control_op(
        &self,
        request: DiscussionAgentControl,
        retry: bool,
    ) -> Result<Value> {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Forbidden("human discussion connection required".into()))?;
        let services = self.clone();
        let task = self
            .delivery_tasks
            .spawn_draining(async move {
                intent_core::with_caller(
                    caller,
                    services.instruction_admission(async {
                        let principal = services
                            .discussion_access(&request.workspace_id, &request.conversation_id)
                            .await?;
                        if services
                            .store
                            .discussion_conversation(
                                &request.workspace_id,
                                &request.conversation_id,
                            )
                            .await?
                            .kind
                            != "workspace"
                        {
                            return Err(Error::Forbidden(
                                "private discussion agent control is unavailable".into(),
                            ));
                        }
                        services
                            .require_agent_member_in(&request.agent_id, &request.workspace_id)
                            .await?;
                        if services
                            .store
                            .principal_instructions_revoked(&principal)
                            .await?
                        {
                            return Err(Error::Forbidden(
                                "instruction authority was revoked".into(),
                            ));
                        }
                        services.apply_discussion_control(request, retry).await
                    }),
                )
                .await
            })
            .ok_or_else(|| Error::Internal("daemon is shutting down".into()))?;
        task.await
            .map_err(|e| Error::Internal(format!("discussion control: {e}")))?
    }

    async fn apply_discussion_control(
        &self,
        request: DiscussionAgentControl,
        retry: bool,
    ) -> Result<Value> {
        let control = self.discussion_control_gates.for_agent(&request.agent_id);
        let _control = control.lock().await;
        let gate = self.agent_queue_persist_gate.lock().await;
        let (receipt, changed) = self
            .store
            .control_discussion_activity(
                &request.workspace_id,
                &request.conversation_id,
                &request.message_id,
                &request.agent_id,
                request.attempt,
                retry,
            )
            .await?;
        if changed && !retry {
            if let Some(queue) = self
                .agent_queues
                .lock()
                .expect("agent queues poisoned")
                .get_mut(&request.agent_id)
            {
                queue.retain(|m| m.turn_id != receipt.turn_id);
            }
        }
        drop(gate);
        if changed {
            self.publish_discussion_activity(&receipt);
        }
        if !retry && receipt.activity.state == "stopped" {
            if let Some(manager) = self.agent_manager() {
                manager
                    .stop_discussion_turn(&request.agent_id, &receipt.turn_id)
                    .await;
                manager
                    .clone()
                    .try_drain_queue(request.agent_id.clone(), request.workspace_id.clone())
                    .await;
            }
            self.publish_queue_updated(&request.agent_id).await;
        }
        // Repeated Retry can finish a committed handoff, but never re-create a completed attempt.
        if retry && receipt.activity.state == "queued" {
            self.dispatch_discussion_deliveries(Some(request.message_id), false)
                .await?;
            if self
                .find_queued_message(&request.agent_id, &receipt.turn_id)
                .is_some()
            {
                self.mark_parked_recovery_send(&request.agent_id, receipt.turn_id.clone());
                self.publish_queue_updated(&request.agent_id).await;
                if let Some(manager) = self.agent_manager() {
                    manager
                        .redrive_parked_recovery_send(&request.agent_id, &request.workspace_id)
                        .await;
                }
            }
        }
        Ok(json!({"activity":receipt.activity}))
    }
}
