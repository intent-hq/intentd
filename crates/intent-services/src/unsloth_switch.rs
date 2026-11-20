//! Confirmation gate for Unsloth model switches (PROTOCOL §5.5).
//!
//! The managed Unsloth server holds one model. A turn for an agent on another
//! model restarts the server, and every other agent on it loses its model. A
//! user send that would do this while other agents use the server is refused
//! with `unsloth-model-switch-confirmation-required`, which names those
//! agents. The client asks the user, then resends with
//! `messageMetadata.confirmUnslothModelSwitch: true`; the daemon consumes that
//! flag and never persists it.

use intent_core::{AgentId, Error, MessageOrigin, Result};
use serde_json::{json, Value};

use crate::Services;

/// Reserved `messageMetadata` key confirming the switch for one send.
pub(crate) const CONFIRM_METADATA_KEY: &str = "confirmUnslothModelSwitch";

/// Remove the confirmation flag. An object left empty becomes `None`.
fn take_confirmation(metadata: Option<Value>) -> (Option<Value>, bool) {
    let Some(Value::Object(mut fields)) = metadata else {
        return (metadata, false);
    };
    let confirmed = fields.remove(CONFIRM_METADATA_KEY) == Some(Value::Bool(true));
    let rest = (!fields.is_empty()).then_some(Value::Object(fields));
    (rest, confirmed)
}

impl Services {
    /// Admit a send to `agent_id`, or refuse it when its next turn would
    /// make the managed server unload the model other agents use. Returns
    /// the metadata to persist, without the confirmation flag.
    pub(crate) async fn admit_unsloth_model_switch(
        &self,
        agent_id: &AgentId,
        origin: MessageOrigin,
        metadata: Option<Value>,
    ) -> Result<Option<Value>> {
        let (metadata, confirmed) = take_confirmation(metadata);
        if confirmed || !origin.is_user() {
            return Ok(metadata);
        }
        let Some(manager) = self.agent_manager() else {
            return Ok(metadata);
        };
        let Some(current) = manager.unsloth_manager().owned_serving_repo() else {
            return Ok(metadata);
        };
        let session = self.store.get_agent_session_summary(agent_id).await?;
        // A remote agent runs on its machine's own server.
        if intent_core::remote_launch::RemotePlacementView::from_metadata(session.metadata.as_ref())
            .is_some()
        {
            return Ok(metadata);
        }
        let settings = self.effective_settings();
        let provider = crate::agent_session::resolve_base_provider_id(
            session.provider.as_deref(),
            crate::agent_session::derived_default_provider(&settings).as_deref(),
        );
        let Some(requested) = session.model.clone() else {
            return Ok(metadata);
        };
        if provider.as_deref() != Some("unsloth") || requested == current {
            return Ok(metadata);
        }
        // Only agents running on the served model lose it.
        let others: Vec<AgentId> = manager
            .agents_with_provider("unsloth")
            .into_iter()
            .filter(|(id, model)| id != agent_id && model.as_deref() == Some(current.as_str()))
            .map(|(id, _)| id)
            .collect();
        if others.is_empty() {
            return Ok(metadata);
        }
        let mut agents = Vec::with_capacity(others.len());
        for id in others {
            let Ok(other) = self.store.get_agent_session_summary(&id).await else {
                continue;
            };
            let workspace_title = self
                .store
                .get_workspace(&other.workspace_id)
                .await
                .ok()
                .map(|ws| ws.title);
            agents.push(json!({
                "agentId": id.as_str(),
                "agentName": other.name,
                "workspaceId": other.workspace_id.as_str(),
                "workspaceTitle": workspace_title,
            }));
        }
        agents.sort_by(|a, b| a["agentId"].as_str().cmp(&b["agentId"].as_str()));
        let count = agents.len();
        Err(Error::confirmation_required(
            format!(
                "switching the Unsloth server from {current} to {requested} unloads the model for {count} other agent{}; resend with messageMetadata.{CONFIRM_METADATA_KEY} to continue",
                if count == 1 { "" } else { "s" }
            ),
            json!({
                "code": "unsloth-model-switch-confirmation-required",
                "currentModel": current,
                "requestedModel": requested,
                "agents": agents,
            }),
        ))
    }
}
