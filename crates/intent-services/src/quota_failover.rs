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

use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Per-agent failover chain of the user turn in flight. Present only while a
/// turn worker owns the agent's turn (armed before each attempt), so a quota
/// failure outside a worker never fails over without a redrive. In-memory
/// only.
pub(crate) type QuotaFailoverChains = Arc<Mutex<HashMap<AgentId, QuotaFailoverChain>>>;

/// One message's failover state.
#[derive(Clone, Debug, Default)]
pub(crate) struct QuotaFailoverChain {
    /// References already tried for this message.
    tried: Vec<String>,
    /// The identity the current attempt started with: its canonical
    /// selection and the session's `model` column as read at turn start.
    /// A failure is only failed over while the session still carries exactly
    /// this identity — a user switch of provider OR model during the attempt
    /// always wins.
    attempt: Option<(String, Option<String>)>,
}

/// Agents whose current message is a turn a quota failover cut short (set
/// when a switch lands, cleared with the message): every history replay for
/// it shows the backup the partial turn's persisted work instead of starting
/// over.
pub(crate) type QuotaFailoverReplays = Arc<Mutex<HashSet<AgentId>>>;

/// Sent to the backup in place of the original user message when the failed
/// attempt already produced output: the user message and the partial work
/// are in the replayed history above, so re-sending the message as new would
/// invite the backup to redo work (duplicate edits or commands).
pub(crate) const QUOTA_FAILOVER_CONTINUATION: &str = "The previous provider ran out of quota \
partway through your reply to the last user message above. Continue that reply from where it \
stopped. The tool calls and results shown above have already happened: do not repeat them.";

/// Whether `err` is the swallowed-attempt marker from `run_prompt_turn`.
pub(crate) fn is_quota_failover_error(err: &intent_core::Error) -> bool {
    matches!(err, intent_core::Error::Internal(msg) if msg.starts_with(PROMPT_QUOTA_FAILOVER_PREFIX))
}

/// The next backup for an agent whose `current` selection just failed for
/// quota, with no home to return to: [`next_candidate`] without a home.
#[cfg(test)]
pub(crate) fn next_backup(
    current: &str,
    tried: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Option<String> {
    next_candidate(current, None, tried, profiles, enabled)
}

/// The next configuration to try for an agent whose `current` selection just
/// failed for quota. The rotation is the `backup: true` profiles of
/// `current`'s base provider in saved order, then the conversation's `home`
/// (when it is away and home is not itself a backup), taken in a cycle
/// starting after `current` and skipping `current` and every reference in
/// `tried`. `None` when the base provider is disabled or nothing is left.
/// `current` and `home` may be the base's Default configuration (a bare id)
/// or a profile reference; the rotation never crosses base providers.
/// Profiles that no longer exist are not in `profiles`, so a backup is
/// skipped by construction; a profile home that no longer exists is skipped
/// too.
pub(crate) fn next_candidate(
    current: &str,
    home: Option<&str>,
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
    let mut rotation: Vec<String> = profiles
        .iter()
        .filter(|p| p.backup && p.base_provider == base)
        .map(ProfileMeta::provider_ref)
        .collect();
    if let Some(home) = home.map(provider_profiles::canonical_selection) {
        let home_base = intent_providers::base_provider_id(&home).to_owned();
        let home_exists = !provider_profiles::is_qualified_ref(&home)
            || profiles.iter().any(|p| p.provider_ref() == home);
        if home_base == base && home_exists && !rotation.contains(&home) {
            rotation.push(home);
        }
    }
    let start = rotation
        .iter()
        .position(|candidate| *candidate == current)
        .map_or(0, |index| index + 1);
    (0..rotation.len())
        .map(|offset| &rotation[(start + offset) % rotation.len()])
        .find(|candidate| **candidate != current && !tried.contains(candidate))
        .cloned()
}

impl Services {
    /// Arm the failover chain for the attempt the turn worker is about to
    /// run. `keep` carries the references tried so far over to a redrive of
    /// the same message (a failover or silent redrive); otherwise the chain
    /// restarts empty for a new message.
    pub(crate) fn arm_quota_failover(&self, agent_id: &AgentId, keep: bool) {
        let mut chains = self.quota_failover_chains.lock().unwrap();
        if keep {
            chains.entry(agent_id.clone()).or_default().attempt = None;
        } else {
            chains.insert(agent_id.clone(), QuotaFailoverChain::default());
            self.quota_failover_replays.lock().unwrap().remove(agent_id);
            self.cancel_provider_return_announcement(agent_id);
        }
    }

    /// Record the identity the armed attempt starts with (called at turn
    /// start, once the spawn identity is resolved). A no-op when no worker
    /// armed a chain.
    pub(crate) fn note_quota_failover_attempt(
        &self,
        agent_id: &AgentId,
        selection: &str,
        model: Option<&str>,
    ) {
        if let Some(chain) = self.quota_failover_chains.lock().unwrap().get_mut(agent_id) {
            chain.attempt = Some((
                provider_profiles::canonical_selection(selection),
                model.map(str::to_owned),
            ));
        }
    }

    /// The session-open fill of a turn that started on the provider default
    /// (no stored model) landed `filled` in the `model` column: that is the
    /// identity the attempt now runs under, so the recorded attempt adopts
    /// it. Only an attempt recorded with no model is updated, from the value
    /// the fill itself persisted — never from a re-read of the row, which
    /// could pick up a user's concurrent change.
    pub(crate) fn note_quota_failover_filled_model(&self, agent_id: &AgentId, filled: &str) {
        if let Some(chain) = self.quota_failover_chains.lock().unwrap().get_mut(agent_id) {
            if let Some((_, model)) = chain.attempt.as_mut() {
                if model.is_none() {
                    *model = Some(filled.to_owned());
                }
            }
        }
    }

    /// The armed chain's tried references, when the session still carries
    /// the exact identity the failed attempt started with (and, when given,
    /// the failed step's own provider record agrees). `None` otherwise: no
    /// armed worker, an unrecorded attempt, or a user switch of provider or
    /// model since the attempt started.
    fn chain_for_failed_attempt(
        &self,
        agent_id: &AgentId,
        session: &intent_core::AgentSession,
        failed_provider: Option<&str>,
    ) -> Option<(String, Vec<String>)> {
        let chain = self
            .quota_failover_chains
            .lock()
            .unwrap()
            .get(agent_id)
            .cloned()?;
        let (selection, model) = chain.attempt?;
        let current = failed_attempt_selection(session.provider.as_deref(), Some(&selection))?;
        if let Some(failed) = failed_provider {
            if provider_profiles::canonical_selection(failed) != current {
                return None;
            }
        }
        (session.model == model).then_some((current, chain.tried))
    }

    /// Drop the agent's chain when its turn worker is torn down.
    pub(crate) fn disarm_quota_failover(&self, agent_id: &AgentId) {
        self.quota_failover_chains.lock().unwrap().remove(agent_id);
        self.quota_failover_replays.lock().unwrap().remove(agent_id);
        self.cancel_provider_return_announcement(agent_id);
    }

    /// Whether the agent's current message is a turn a quota failover cut
    /// short (see [`QuotaFailoverReplays`]). The mark lives for the whole
    /// message — so a same-message redrive on the backup (a silent transport
    /// redrive, a further failover) still continues the turn — and is cleared
    /// when the worker arms a new message or is torn down.
    pub(crate) fn quota_failover_replay_pending(&self, agent_id: &AgentId) -> bool {
        self.quota_failover_replays
            .lock()
            .unwrap()
            .contains(agent_id)
    }

    /// Whether a quota failure right now would fail over: the agent's turn
    /// worker armed a chain and an untried backup exists for the session's
    /// provider. Read-only. The spawn retry loop uses it to skip the
    /// same-provider retries a spent quota cannot benefit from; with no
    /// candidate those retries run as before.
    pub(crate) async fn has_quota_failover_candidate(
        &self,
        agent_id: &AgentId,
        failed_provider: Option<&str>,
    ) -> bool {
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return false;
        };
        let Some((from, tried)) =
            self.chain_for_failed_attempt(agent_id, &session, failed_provider)
        else {
            return false;
        };
        let Ok(profiles) = self.provider_profiles.profiles() else {
            return false;
        };
        let settings = self.effective_settings();
        let home = crate::provider_return::read_state(&session).map(|s| s.home);
        next_candidate(
            &from,
            home.as_deref(),
            &tried,
            &profiles,
            settings.providers.enabled.as_ref(),
        )
        .is_some()
    }

    /// After a quota failure, switch the agent onto the next configuration of
    /// its rotation ([`next_candidate`]). `error_text` is the rendered quota
    /// error, read for a reported reset time (the return-home timer, see
    /// [`crate::provider_return`]). `failed_provider` is the configuration the failed attempt actually ran
    /// on (the committed turn identity, or the spawn attempt's record). When
    /// the session already names another configuration — the user switched
    /// while the attempt was in flight — nothing fails over: the user's
    /// choice is never overwritten. Returns `true` when the switch landed — the caller then swallows the
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
        failed_provider: Option<&str>,
        error_text: &str,
    ) -> bool {
        let session = match self.store.get_agent_session(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover skipped: session read failed");
                return false;
            }
        };
        if self
            .chain_for_failed_attempt(agent_id, &session, failed_provider)
            .is_none()
        {
            tracing::info!(agent = %agent_id, "no armed attempt, or provider/model changed since the failed attempt; quota failover skipped");
            return false;
        }
        let Some(from) = session.provider.clone() else {
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
        let return_state = crate::provider_return::read_state(&session);
        let home = return_state.as_ref().map(|s| s.home.clone());
        let target = {
            let mut chains = self.quota_failover_chains.lock().unwrap();
            let Some(chain) = chains.get_mut(agent_id) else {
                return false;
            };
            let tried = &mut chain.tried;
            if !tried.contains(&from_selection) {
                tried.push(from_selection.clone());
            }
            let target = next_candidate(
                &from_selection,
                home.as_deref(),
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
        let model = session.model.clone().filter(|m| !m.is_empty());
        let landed = match self
            .store
            .failover_agent_session_provider(
                workspace_id,
                agent_id,
                &from,
                session.model.as_deref(),
                &target,
                &now_iso(),
            )
            .await
        {
            Ok(landed) => landed,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover switch failed");
                return false;
            }
        };
        if !landed {
            tracing::info!(agent = %agent_id, "provider or model changed concurrently; quota failover skipped");
            return false;
        }
        tracing::info!(
            agent = %agent_id,
            from_provider = %from_selection,
            to_provider = %target,
            "provider quota exhausted; switching to backup profile"
        );
        self.quota_failover_replays
            .lock()
            .unwrap()
            .insert(agent_id.clone());
        // Leaving the configuration this message started on: a timed return
        // home that has not produced output yet is not announced.
        self.cancel_provider_return_announcement(agent_id);
        let now = chrono::Utc::now();
        let next_state = crate::provider_return::on_switch(
            return_state,
            &from_selection,
            &target,
            crate::provider_return::parse_reset_time(error_text, now),
            now,
        );
        self.persist_provider_return_state(workspace_id, agent_id, Some(&next_state))
            .await;
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

/// Upper bound on the identity notices the `model_changed` suppression reads
/// back: comfortably above one turn's failover chain (at most one hop per
/// saved backup, 32 profiles per base).
pub(crate) const FAILOVER_CHAIN_READ_LIMIT: u32 = 64;

/// Whether `notices` (identity-change rows, newest first) announce the hop
/// `from` → `to` as one unbroken chain of `provider_failover` rows: the
/// newest lands on `to`, each row starts where the next-older one landed,
/// and the oldest in the chain starts at `from`. A turn whose first backup
/// also failed (A → B → C, last committed turn on A) is announced by its two
/// rows; any other identity row in between — a manual switch's
/// `model_changed`, a re-home — breaks the chain, so a genuine change still
/// gets its own notice.
pub(crate) fn failover_chain_announces(
    notices: &[serde_json::Value],
    from: &str,
    to: &str,
) -> bool {
    let selection = provider_profiles::canonical_selection;
    let from = selection(from);
    let mut landing = selection(to);
    for notice in notices {
        if notice["type"].as_str() != Some(PROVIDER_FAILOVER_NOTICE_TYPE) {
            return false;
        }
        let (Some(hop_from), Some(hop_to)) = (
            notice["fromProvider"].as_str(),
            notice["toProvider"].as_str(),
        ) else {
            return false;
        };
        if selection(hop_to) != landing {
            return false;
        }
        let hop_from = selection(hop_from);
        if hop_from == from {
            return true;
        }
        landing = hop_from;
    }
    false
}

/// The canonical selection the quota failure belongs to, when the session
/// still runs it: `None` when the failed attempt's provider is unknown, the
/// session has no provider, or the session already names a different
/// configuration (a concurrent user switch, which always wins).
fn failed_attempt_selection(
    session_provider: Option<&str>,
    failed_provider: Option<&str>,
) -> Option<String> {
    let session = session_provider.filter(|p| !p.trim().is_empty())?;
    let session = provider_profiles::canonical_selection(session);
    (provider_profiles::canonical_selection(failed_provider?) == session).then_some(session)
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
