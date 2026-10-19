//! Durable explicit delivery; ordinary discussion traffic never enters agent queues.
use crate::{agent_ops::QueuedMessage, Services};
use intent_core::{discussion::DiscussionMessage, Error, Result};
use serde_json::json;

impl Services {
    pub(crate) async fn dispatch_discussion_deliveries(
        &self,
        message: Option<String>,
        kick: bool,
    ) -> Result<usize> {
        let services = self.clone();
        // SQL may commit after its awaiting request is cancelled. Retain the
        // handoff until the committed queue row is visible in memory too.
        let task = self
            .delivery_tasks
            .spawn_draining(async move {
                services
                    .admit_discussion_deliveries(message.as_deref(), kick)
                    .await
            })
            .ok_or_else(|| {
                Error::Internal(
                    "daemon is shutting down; discussion delivery remains pending".into(),
                )
            })?;
        task.await
            .map_err(|e| Error::Internal(format!("discussion delivery: {e}")))?
    }

    async fn admit_discussion_deliveries(
        &self,
        message: Option<&str>,
        kick: bool,
    ) -> Result<usize> {
        let mut count = 0;
        loop {
            // One shared gate orders concurrent retries and prevents queue
            // snapshots from erasing a committed row before memory adopts it.
            let gate = self.agent_queue_persist_gate.lock().await;
            let pending = self.store.discussion_pending_deliveries(message).await?;
            if pending.is_empty() {
                return Ok(count);
            }
            let mut destinations = Vec::new();
            let mut failure = None;
            for delivery in pending {
                let m = delivery.message;
                let agent = delivery.agent_id;
                let id = format!("discussion:{}:{}", m.id, agent.as_str());
                let author = m
                    .author
                    .display_name
                    .as_deref()
                    .or(m.author.login.as_deref())
                    .unwrap_or("Workspace member");
                let conversation = serde_json::to_string(&m.conversation_id)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                let content = format!("[EXPLICIT DISCUSSION MESSAGE]\n{author} addressed you in the workspace discussion.\nConversation ID: {conversation}\nMessage ID: {}\n\n{}\n\nRead context with ws.discussion.list({conversation}). Reply to this discussion with ws.discussion.send({conversation}, body, {{ messageId: aUniqueRetryKey }}). Only explicit send calls post replies; your final response is not posted. Other discussion messages are not delivered automatically.", m.id, m.body);
                let mut metadata = json!({"type":"discussion_message","workspaceId":m.workspace_id,"conversationId":m.conversation_id,"discussionMessageId":m.id});
                if let Some(sender) = &m.author.agent_id {
                    metadata["fromAgentId"] = json!(sender);
                    metadata["fromAgentName"] = json!(author);
                }
                if let Some(principal) = &m.author.principal_id {
                    // Restore from the authenticated immutable author, never
                    // the retry caller or ambient task-local caller.
                    metadata["fromPrincipalId"] = json!(principal);
                }
                let order = self
                    .queue_submission_order
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                let payload = json!({"id":id,"turnId":id,"content":content,"imageBlocks":null,"fileBlocks":null,"queuedAt":m.created_at,"messageMetadata":metadata,"userOrigin":m.author.principal_id.is_some(),"submissionOrder":order,"correlationOrderKnown":true});
                let queued: QueuedMessage = serde_json::from_value(payload.clone())
                    .map_err(|e| Error::Internal(e.to_string()))?;
                let row = intent_store::AgentQueueRow {
                    id: id.clone(),
                    agent_id: agent.clone(),
                    position: 0,
                    payload,
                    created_at: queued.queued_at.clone(),
                    turn_id: id,
                };
                let admitted = match self.store.admit_discussion_delivery(&m.id, &row).await {
                    Ok(admitted) => admitted,
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                };
                if admitted {
                    count += 1;
                    // No await between durable commit and in-memory adoption.
                    self.agent_queues
                        .lock()
                        .expect("agent queue registry poisoned")
                        .entry(agent.clone())
                        .or_default()
                        .push(queued);
                    destinations.push((agent, m.workspace_id));
                }
            }
            drop(gate);
            for (agent, workspace) in destinations {
                if kick {
                    self.publish_queue_updated(&agent).await;
                    if let Some(manager) = self.agent_manager() {
                        manager.clone().try_drain_queue(agent, workspace).await;
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
        }
    }

    /// A restored handoff already consumed its outbox row. An explicit retry
    /// may drain that pending submission, but must never reconstruct a completed
    /// message or wake an agent just because it has unrelated queued work.
    pub(crate) async fn retry_pending_discussion_message(
        &self,
        message: &DiscussionMessage,
    ) -> Result<()> {
        for agent in &message.recipient_agent_ids {
            let id = format!("discussion:{}:{}", message.id, agent.as_str());
            if self.find_queued_message(agent, &id).is_some()
                && self
                    .store
                    .discussion_agent_access(&message.workspace_id, &message.conversation_id, agent)
                    .await?
            {
                if let Some(manager) = self.agent_manager() {
                    manager
                        .clone()
                        .try_drain_queue(agent.clone(), message.workspace_id.clone())
                        .await;
                }
            }
        }
        Ok(())
    }
}
