//! Change-aware catalog delivery for persistent first-turn-prepend providers.

use base64::Engine;
use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use intent_providers::InjectionMechanism;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{session_provider_id, AgentManager};

const DELIVERY_KEY: &str = "skillCatalogDelivery";

impl AgentManager {
    /// A loaded conversation already has its system prompt, but its catalog may
    /// predate the current discovery policy or files. Compare a stable digest
    /// instead of replaying the whole prompt or accumulating unchanged catalogs.
    pub(super) async fn build_skill_catalog_update(
        &self,
        agent_id: &AgentId,
        first_turn_prepend: Option<&str>,
    ) -> Option<String> {
        self.skill_catalog_pending.lock().unwrap().remove(agent_id);
        let session = self.services.store.get_agent_session(agent_id).await.ok()?;
        let default =
            crate::agent_session::derived_default_provider(&self.services.effective_settings());
        let provider_id = session_provider_id(&session, default.as_deref())?;
        let provider = intent_providers::find_provider(&provider_id)?;
        if provider.injection_mechanism != InjectionMechanism::FirstTurnPrepend {
            return None;
        }
        let acp_session_id = session.acp_session_id.as_deref()?;
        let workspace = self
            .services
            .store
            .get_workspace(&session.workspace_id)
            .await
            .ok()?;
        let catalog = crate::rules::skill_catalog_for_workspace(&workspace).await;
        let mut digest = Sha256::new();
        digest.update(b"intent-skill-catalog-v1\0");
        digest.update(acp_session_id.as_bytes());
        digest.update(b"\0");
        digest.update(catalog.as_bytes());
        let fingerprint =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize());
        if session
            .metadata
            .as_ref()
            .and_then(|meta| meta.get(DELIVERY_KEY))
            .and_then(Value::as_str)
            == Some(fingerprint.as_str())
        {
            return None;
        }
        self.skill_catalog_pending
            .lock()
            .unwrap()
            .insert(agent_id.clone(), fingerprint);
        // Fresh/recreated sessions normally already carry the current catalog.
        // A change between spawn-time assembly and this turn still gets a
        // replacement, including removal of the final skill.
        let already_in_prompt = first_turn_prepend.is_some_and(|prompt| {
            if catalog.is_empty() {
                !prompt.contains("<available_skills>")
            } else {
                prompt.contains(&catalog)
            }
        });
        (!already_in_prompt).then(|| crate::rules::skill_catalog_update(&catalog))
    }

    pub(super) async fn acknowledge_skill_catalog(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        fingerprint: Option<String>,
    ) {
        let Some(fingerprint) = fingerprint else {
            return;
        };
        // A narrow metadata write preserves concurrent session preferences and
        // does not make bookkeeping look like user activity. If it fails, the
        // next turn safely retries the catalog update.
        if let Err(error) = self
            .services
            .store
            .set_agent_session_metadata_key(
                workspace_id,
                agent_id,
                DELIVERY_KEY,
                &fingerprint,
                None,
                None,
            )
            .await
        {
            tracing::warn!(agent = %agent_id, %error, "failed to record delivered skill catalog");
        }
    }
}

#[cfg(test)]
mod tests;
