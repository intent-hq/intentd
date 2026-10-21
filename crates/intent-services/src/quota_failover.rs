//! Automatic quota failover to backup provider profiles (PROTOCOL §5.54,
//! "Backup failover").
//!
//! A turn whose provider rejects it for quota (exactly the failures
//! `agent:failed` stamps `errorCode: "quota-exceeded"`) moves the agent onto
//! the first untried backup profile of its own base provider, in saved
//! order, and the turn worker redrives the same user message there. The
//! chain is bounded per user turn: every reference tried is recorded and
//! never picked again, so a turn tries each backup at most once.
//!
//! Flow: the turn worker arms the chain before each attempt
//! ([`Services::arm_quota_failover`]); the quota seam of the failed step
//! (`run_prompt_turn` for a turn that ran, the worker's spawn-failure arm for
//! a rejected session setup) calls [`Services::try_quota_failover`]. On a
//! switch the failed attempt is swallowed (no `agent:failed`, no error row)
//! and the worker loops back through its spawn with the same content.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use intent_core::{now_iso, AgentId, WorkspaceId};
use serde_json::json;

use crate::provider_profiles::{self, ProfileMeta};
use crate::Services;

/// Prefix of the error `run_prompt_turn` returns for a quota failure it
/// swallowed after switching the agent onto a backup: the worker redrives the
/// same message on a fresh child instead of taking the terminal path.
pub(crate) const PROMPT_QUOTA_FAILOVER_PREFIX: &str = "session/prompt quota failover:";

/// `metadata.type` of the transcript notice persisted for each switch.
pub(crate) const PROVIDER_FAILOVER_NOTICE_TYPE: &str = "provider_failover";

/// Per-agent failover chain of the user turn in flight: the references
/// already tried. Present only while a turn worker owns the agent's turn
/// (armed before each attempt), so a quota failure outside a worker never
/// fails over without a redrive. In-memory only.
pub(crate) type QuotaFailoverChains = Arc<Mutex<HashMap<AgentId, Vec<String>>>>;

/// Whether `err` is the swallowed-attempt marker from `run_prompt_turn`.
pub(crate) fn is_quota_failover_error(err: &intent_core::Error) -> bool {
    matches!(err, intent_core::Error::Internal(msg) if msg.starts_with(PROMPT_QUOTA_FAILOVER_PREFIX))
}

/// The next backup for an agent whose `current` selection just failed for
/// quota: the first profile, in saved order, with `backup: true` whose base
/// provider is `current`'s base, that is neither `current` nor already in
/// `tried`. `None` when the base provider is disabled or no candidate is
/// left. `current` may be the base's Default configuration (a bare id) or a
/// profile reference; failover never crosses base providers. Profiles that
/// no longer exist are not in `profiles`, so they are skipped by
/// construction.
pub(crate) fn next_backup(
    current: &str,
    tried: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Option<String> {
    let current = provider_profiles::canonical_selection(current);
    let base = if provider_profiles::is_qualified_ref(&current) {
        provider_profiles::parse_profile_ref(&current)?.0.to_owned()
    } else {
        current.clone()
    };
    if crate::agent_ops::provider_is_disabled(&base, enabled) {
        return None;
    }
    profiles
        .iter()
        .filter(|p| p.backup && p.base_provider == base)
        .map(ProfileMeta::provider_ref)
        .find(|candidate| *candidate != current && !tried.contains(candidate))
}

impl Services {
    /// Arm the failover chain for the attempt the turn worker is about to
    /// run. `keep` carries the references tried so far over to a redrive of
    /// the same message (a failover or silent redrive); otherwise the chain
    /// restarts empty for a new message.
    pub(crate) fn arm_quota_failover(&self, agent_id: &AgentId, keep: bool) {
        let mut chains = self.quota_failover_chains.lock().unwrap();
        if keep {
            chains.entry(agent_id.clone()).or_default();
        } else {
            chains.insert(agent_id.clone(), Vec::new());
        }
    }

    /// Drop the agent's chain when its turn worker exits.
    pub(crate) fn disarm_quota_failover(&self, agent_id: &AgentId) {
        self.quota_failover_chains.lock().unwrap().remove(agent_id);
    }

    /// After a quota failure, switch the agent onto its next backup profile.
    /// Returns `true` when the switch landed — the caller then swallows the
    /// failed attempt and the worker redrives the same message — and `false`
    /// when the failure must stay terminal (no armed worker, no candidate,
    /// an unreadable session or profile list, or a concurrent provider
    /// change that won the compare-and-set).
    ///
    /// The switch persists the backup reference as the agent's provider with
    /// the same model, emits `agent:updated`, and persists one
    /// `provider_failover` system row (emitting `agent:message`). It changes
    /// no profile, so no `providers:profiles-changed` is sent.
    pub(crate) async fn try_quota_failover(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> bool {
        if !self
            .quota_failover_chains
            .lock()
            .unwrap()
            .contains_key(agent_id)
        {
            return false;
        }
        let session = match self.store.get_agent_session(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover skipped: session read failed");
                return false;
            }
        };
        let Some(from) = session.provider.clone().filter(|p| !p.trim().is_empty()) else {
            return false;
        };
        let profiles = match self.provider_profiles.profiles() {
            Ok(profiles) => profiles,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover skipped: profiles unreadable");
                return false;
            }
        };
        let settings = self.effective_settings();
        let from_selection = provider_profiles::canonical_selection(&from);
        let target = {
            let mut chains = self.quota_failover_chains.lock().unwrap();
            let Some(tried) = chains.get_mut(agent_id) else {
                return false;
            };
            if !tried.contains(&from_selection) {
                tried.push(from_selection.clone());
            }
            let target = next_backup(
                &from_selection,
                tried,
                &profiles,
                settings.providers.enabled.as_ref(),
            );
            if let Some(target) = &target {
                tried.push(target.clone());
            }
            target
        };
        let Some(target) = target else {
            return false;
        };
        let landed = match self
            .store
            .failover_agent_session_provider(workspace_id, agent_id, &from, &target, &now_iso())
            .await
        {
            Ok(landed) => landed,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover switch failed");
                return false;
            }
        };
        if !landed {
            tracing::info!(agent = %agent_id, "provider changed concurrently; quota failover skipped");
            return false;
        }
        tracing::info!(
            agent = %agent_id,
            from_provider = %from_selection,
            to_provider = %target,
            "provider quota exhausted; switching to backup profile"
        );
        let model = session.model.clone().filter(|m| !m.is_empty());
        self.invalidate_agent_list_cache(workspace_id);
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "modelId": model, "providerId": target }),
        )
        .await;
        self.persist_provider_failover_notice(
            workspace_id,
            agent_id,
            &from_selection,
            &target,
            model.as_deref(),
            &profiles,
        )
        .await;
        true
    }

    /// Persist the `provider_failover` system row for one switch and emit
    /// `agent:message`. Best-effort like the other identity notices: the
    /// switch already landed, so a failed append is logged and the turn
    /// proceeds on the backup.
    async fn persist_provider_failover_notice(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        from: &str,
        to: &str,
        model: Option<&str>,
        profiles: &[ProfileMeta],
    ) {
        let content = json!([{ "type": "text", "text": failover_notice_text(from, to, profiles) }]);
        let metadata = json!({
            "type": PROVIDER_FAILOVER_NOTICE_TYPE,
            "fromProvider": from,
            "toProvider": to,
            "model": model,
            "reason": crate::agent_manager::QUOTA_EXCEEDED_ERROR_CODE,
        });
        match self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "system",
                &content,
                Some(&metadata),
                &now_iso(),
            )
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                    .await;
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to persist provider failover notice");
            }
        }
    }
}

/// The notice text: both configurations by readable label, never by
/// internal reference.
fn failover_notice_text(from: &str, to: &str, profiles: &[ProfileMeta]) -> String {
    let from = provider_profiles::readable_selection(from, Some(profiles));
    let to = provider_profiles::readable_selection(to, Some(profiles));
    format!("{from} ran out of quota. Switched to the backup {to}.")
}

#[cfg(test)]
#[path = "quota_failover_tests.rs"]
mod tests;
