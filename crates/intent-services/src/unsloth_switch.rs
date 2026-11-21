//! Confirmation gate for Unsloth model switches (PROTOCOL §5.5).
//!
//! The daemon runs one Unsloth server per model while memory allows. When a
//! turn needs a model that does not fit, the pool stops other servers to make
//! room, and every agent on them loses its model. A user send that would stop
//! a server other agents use is refused with
//! `unsloth-model-switch-confirmation-required`, which names those agents. The client asks the user, then resends with
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
        if provider.as_deref() != Some("unsloth") {
            return Ok(metadata);
        }
        // The servers that starting this model would stop to make room. A
        // running model, or one that fits, stops nothing.
        let attached = manager.unsloth_agents_by_model(Some(agent_id));
        let unloaded = manager
            .unsloth_manager()
            .plan_for(&requested, &attached)
            .await;
        let Some(current) = unloaded.first().cloned() else {
            return Ok(metadata);
        };
        let others: Vec<AgentId> = manager
            .agents_with_provider("unsloth")
            .into_iter()
            .filter(|(id, model)| {
                id != agent_id && model.as_ref().is_some_and(|model| unloaded.contains(model))
            })
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
                "starting Unsloth model {requested} stops the server for {} to free memory, unloading the model for {count} other agent{}; resend with messageMetadata.{CONFIRM_METADATA_KEY} to continue",
                unloaded.join(", "),
                if count == 1 { "" } else { "s" }
            ),
            json!({
                "code": "unsloth-model-switch-confirmation-required",
                "currentModel": current,
                "requestedModel": requested,
                "unloadedModels": unloaded,
                "agents": agents,
            }),
        ))
    }
}
