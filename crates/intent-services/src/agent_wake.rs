//! The agents side of [`AgentWakePort`]: wake delivery, the message queue,
//! the queue drainer, retirement gates and the agent status and provider
//! predicates that runtime code reaches through the port instead of naming
//! `agent_ops` / `agent_manager` items. [`Services::new`] registers it.

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_core::{AgentId, AgentStatus, BoxFuture, MessageOrigin, Result, WorkspaceId};
use serde_json::Value;

use crate::agent_manager::AgentManager;
use crate::feature_ports::{AgentQueueDrainer, AgentWakePort};
use crate::Services;

/// The in-crate agent-wake feature. Stateless: everything it reads lives on
/// `Services`.
#[derive(Default)]
pub(crate) struct AgentWakeFeature;

/// Register the in-crate agent-wake feature's port.
pub(crate) fn register(services: Services) -> Services {
    services.with_agent_wake_port(Arc::new(AgentWakeFeature))
}

impl AgentWakePort for AgentWakeFeature {
    fn deliver_wake_message<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
        content: &'a str,
        metadata: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(svc.deliver_wake_message(workspace_id, agent_id, content, metadata))
    }

    fn enqueue_automatic_message(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        message_id: Option<String>,
        content: String,
        metadata: Option<Value>,
    ) -> Result<intent_store::AgentQueueRow> {
        let (queued, position) = svc.enqueue_message_with_id(
            agent_id,
            message_id,
            content,
            None,
            None,
            metadata,
            None,
            false,
            MessageOrigin::Automatic,
        );
        Ok(intent_store::AgentQueueRow {
            id: queued.id.clone(),
            agent_id: agent_id.clone(),
            position: i64::try_from(position).unwrap_or(i64::MAX),
            payload: serde_json::to_value(&queued)
                .map_err(|e| intent_core::Error::Internal(e.to_string()))?,
            created_at: queued.queued_at.clone(),
            turn_id: queued.turn_id.clone(),
        })
    }

    fn publish_queue_updated<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.publish_queue_updated(agent_id))
    }

    fn queue_drainer(&self, svc: &Services) -> Option<Arc<dyn AgentQueueDrainer>> {
        svc.agent_manager()
            .map(|manager| manager as Arc<dyn AgentQueueDrainer>)
    }

    fn retirement_gate(&self, svc: &Services, agent_id: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        svc.agent_retirement_gates.for_agent(agent_id)
    }

    fn is_terminal_status(&self, status: AgentStatus) -> bool {
        crate::agent_ops::is_terminal_status(status)
    }

    fn provider_is_disabled(
        &self,
        provider_id: &str,
        enabled: Option<&BTreeMap<String, bool>>,
    ) -> bool {
        crate::agent_ops::provider_is_disabled(provider_id, enabled)
    }
}

impl AgentQueueDrainer for AgentManager {
    fn try_drain_queue(
        self: Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'static, ()> {
        Box::pin(AgentManager::try_drain_queue(self, agent_id, workspace_id))
    }
}
