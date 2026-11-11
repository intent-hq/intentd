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
    /// The identity the current attempt started with. A failure is only
    /// failed over while the session still carries exactly this identity — a
    /// user switch of provider OR model during the attempt always wins.
    attempt: Option<FailoverAttempt>,
}

/// One attempt of the message in flight.
#[derive(Clone, Debug)]
struct FailoverAttempt {
    /// Canonical selection the attempt runs on.
    selection: String,
    /// The session's `model` column as read at turn start.
    model: Option<String>,
    /// When the attempt started: the shared quota cache only clears an
    /// exhausted mark recorded before this (see
    /// [`crate::provider_quota_cache::ProviderQuotaCache::mark_working`]).
    started_at: chrono::DateTime<chrono::Utc>,
    /// Whether this attempt already reported working to the shared cache.
    working_noted: bool,
    /// Whether a remote agent runs this attempt on its machine's own
    /// sign-in: another account, so the attempt never marks or clears this
    /// backend's reference in the shared cache (intent-hq/intent#9100).
    machine_sign_in: bool,
}

/// Agents whose current message is a turn a quota failover cut short (set
/// when a switch lands, cleared with the message): every history replay for
/// it shows the backup the partial turn's persisted work instead of starting
/// over.
pub(crate) type QuotaFailoverReplays = Arc<Mutex<HashSet<AgentId>>>;

/// What [`Services::try_quota_failover`] did with a quota failure.
#[derive(Debug)]
pub(crate) enum QuotaFailoverOutcome {
    /// The agent moved to another configuration: swallow the failed attempt
    /// and redrive the message there.
    Switched,
    /// Nothing moved: the failure stays terminal as reported.
    Terminal,
    /// Nothing moved and the failure stays terminal, with this actionable
    /// error in place of the raw quota error (a remote agent whose only
    /// backups cannot run on its machine).
    Refused(intent_core::Error),
}

impl QuotaFailoverOutcome {
    pub(crate) fn switched(&self) -> bool {
        matches!(self, Self::Switched)
    }

    pub(crate) fn into_refusal(self) -> Option<intent_core::Error> {
        match self {
            Self::Refused(error) => Some(error),
            Self::Switched | Self::Terminal => None,
        }
    }
}

/// Code prefixing the error a remote agent fails with when quota failover
/// cannot move it (the backup cannot run on, or did not start on, its
/// machine). The turn worker never fails such an error over again.
pub(crate) const REMOTE_FAILOVER_UNAVAILABLE: &str = "remote-failover-unavailable:";

/// Whether `err` is a [`REMOTE_FAILOVER_UNAVAILABLE`] failure.
pub(crate) fn is_remote_failover_unavailable(err: &intent_core::Error) -> bool {
    matches!(err, intent_core::Error::InvalidParams(msg) if msg.starts_with(REMOTE_FAILOVER_UNAVAILABLE))
}

/// Which automatic switch a [`PendingRemoteFailover`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RemoteSwitchKind {
    /// A quota failover onto a backup: announced by a `provider_failover`
    /// row once the node runs it.
    Failover,
    /// A timed return home ([`crate::provider_return`]): announced by its
    /// own `provider_returned` row at home's first output.
    Return,
}

/// A remote agent's automatic provider switch (a quota failover, or the
/// timed return home after one) that is persisted but not confirmed yet:
/// the node replaces the agent's run with one on the new configuration, and
/// the switch is undone if the node cannot start it (intent-hq/intent#8317).
/// In-memory only.
#[derive(Clone, Debug)]
pub(crate) struct PendingRemoteFailover {
    kind: RemoteSwitchKind,
    /// The session's provider before the switch, as stored.
    from: String,
    /// The backup reference the session now names.
    to: String,
    /// The session's `model` column, unchanged by the switch.
    model: Option<String>,
    /// The return state the switch replaced.
    previous_return_state: Option<crate::provider_return::ProviderReturnState>,
    /// The machine the agent runs on.
    node_name: String,
    profiles: Vec<ProfileMeta>,
}

pub(crate) type PendingRemoteFailovers = Arc<Mutex<HashMap<AgentId, PendingRemoteFailover>>>;

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
#[cfg(test)]
pub(crate) fn next_candidate(
    current: &str,
    home: Option<&str>,
    tried: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Option<String> {
    next_candidate_avoiding(current, home, tried, &[], profiles, enabled)
}

/// [`next_candidate`], preferring configurations the shared quota cache does
/// not mark exhausted: the walk takes the first eligible candidate not in
/// `exhausted`, and only when every eligible candidate is marked falls back
/// to the first of them — a mark is advisory, so a turn still tries every
/// candidate once.
pub(crate) fn next_candidate_avoiding(
    current: &str,
    home: Option<&str>,
    tried: &[String],
    exhausted: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Option<String> {
    let mut eligible = candidate_walk(current, home, tried, profiles, enabled).into_iter();
    let first = eligible.next()?;
    if !exhausted.contains(&first) {
        return Some(first);
    }
    eligible
        .find(|candidate| !exhausted.contains(candidate))
        .or(Some(first))
}

/// The first candidate of `current`'s rotation (see [`next_candidate`])
/// that the shared quota cache does not mark exhausted, with nothing tried
/// yet: where a turn starts instead of a configuration already known to be
/// out of quota. `None` when every candidate is marked (the turn then still
/// tries its current configuration) or there is none.
pub(crate) fn first_unmarked_candidate(
    current: &str,
    home: Option<&str>,
    exhausted: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Option<String> {
    candidate_walk(current, home, &[], profiles, enabled)
        .into_iter()
        .find(|candidate| !exhausted.contains(candidate))
}

/// The eligible candidates of `current`'s rotation in walk order (see
/// [`next_candidate`]), `current` and `tried` excluded.
fn candidate_walk(
    current: &str,
    home: Option<&str>,
    tried: &[String],
    profiles: &[ProfileMeta],
    enabled: Option<&BTreeMap<String, bool>>,
) -> Vec<String> {
    let current = provider_profiles::canonical_selection(current);
    let base = if provider_profiles::is_qualified_ref(&current) {
        let Some((base, _)) = provider_profiles::parse_profile_ref(&current) else {
            return Vec::new();
        };
        base.to_owned()
    } else {
        current.clone()
    };
    if crate::agent_ops::provider_is_disabled(&base, enabled) {
        return Vec::new();
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
        .filter(|candidate| **candidate != current && !tried.contains(candidate))
        .cloned()
        .collect()
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
        machine_sign_in: bool,
    ) {
        if let Some(chain) = self.quota_failover_chains.lock().unwrap().get_mut(agent_id) {
            chain.attempt = Some(FailoverAttempt {
                selection: provider_profiles::canonical_selection(selection),
                model: model.map(str::to_owned),
                started_at: chrono::Utc::now(),
                working_noted: false,
                machine_sign_in,
            });
        }
    }

    /// Correct the recorded attempt's machine sign-in answer to the one of
    /// the live remote run it reuses (see
    /// [`Self::note_quota_failover_attempt`]).
    pub(crate) fn set_quota_failover_attempt_machine_sign_in(
        &self,
        agent_id: &AgentId,
        machine_sign_in: bool,
    ) {
        if let Some(attempt) = self
            .quota_failover_chains
            .lock()
            .unwrap()
            .get_mut(agent_id)
            .and_then(|c| c.attempt.as_mut())
        {
            attempt.machine_sign_in = machine_sign_in;
        }
    }

    /// The armed attempt produced output (or completed without any): its
    /// configuration has quota right now, so the shared cache marks it
    /// working for every conversation (see
    /// [`crate::provider_quota_cache`]). Once per attempt; a no-op when no
    /// worker armed a chain or the attempt identity is not recorded yet.
    pub(crate) fn note_provider_working(&self, agent_id: &AgentId) {
        let noted = {
            let mut chains = self.quota_failover_chains.lock().unwrap();
            let Some(attempt) = chains.get_mut(agent_id).and_then(|c| c.attempt.as_mut()) else {
                return;
            };
            if attempt.working_noted || attempt.machine_sign_in {
                return;
            }
            attempt.working_noted = true;
            (attempt.selection.clone(), attempt.started_at)
        };
        let read = self.profile_activation().process_read(agent_id, &noted.0);
        self.provider_quota_cache
            .mark_working(&read, noted.1, chrono::Utc::now());
    }

    /// Record `agent_id`'s quota failure on `selection` in the shared cache,
    /// so every other conversation skips it until `reported` (the error's
    /// reset time, when usable) or the default mark duration. Ignored when
    /// the agent's process runs settings edited or deleted since it started
    /// (see [`crate::profile_activation::ProfileActivation::process_read`]).
    pub(crate) fn note_provider_exhausted(
        &self,
        agent_id: &AgentId,
        selection: &str,
        reported: Option<chrono::DateTime<chrono::Utc>>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        let read = self.profile_activation().process_read(agent_id, selection);
        self.provider_quota_cache
            .mark_exhausted(&read, reported, now);
    }

    /// [`Self::note_provider_exhausted`] for a failed attempt, except one a
    /// remote agent ran on its machine's own sign-in: that account is not
    /// this backend's, so its quota says nothing about this backend's
    /// reference (intent-hq/intent#9100). Uses the answer recorded when the
    /// attempt started, so a settings change during the turn cannot flip it.
    fn note_provider_exhausted_unless_machine_sign_in(
        &self,
        agent_id: &AgentId,
        selection: &str,
        reported: Option<chrono::DateTime<chrono::Utc>>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        let machine_sign_in = self
            .quota_failover_chains
            .lock()
            .unwrap()
            .get(agent_id)
            .and_then(|c| c.attempt.as_ref())
            .is_some_and(|a| {
                a.machine_sign_in
                    && a.selection == provider_profiles::canonical_selection(selection)
            });
        if machine_sign_in {
            tracing::info!(agent = %agent_id, provider = %selection, "quota failure on the machine's own sign-in; this backend's reference not marked");
            return;
        }
        self.note_provider_exhausted(agent_id, selection, reported, now);
    }

    /// Configurations the shared cache marks exhausted right now.
    pub(crate) fn exhausted_providers(&self) -> Vec<String> {
        self.provider_quota_cache.exhausted(chrono::Utc::now())
    }

    /// The session-open fill of a turn that started on the provider default
    /// (no stored model) landed `filled` in the `model` column: that is the
    /// identity the attempt now runs under, so the recorded attempt adopts
    /// it. Only an attempt recorded with no model is updated, from the value
    /// the fill itself persisted — never from a re-read of the row, which
    /// could pick up a user's concurrent change.
    pub(crate) fn note_quota_failover_filled_model(&self, agent_id: &AgentId, filled: &str) {
        if let Some(chain) = self.quota_failover_chains.lock().unwrap().get_mut(agent_id) {
            if let Some(attempt) = chain.attempt.as_mut() {
                if attempt.model.is_none() {
                    attempt.model = Some(filled.to_owned());
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
        let FailoverAttempt {
            selection, model, ..
        } = chain.attempt?;
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
        self.pending_remote_failovers
            .lock()
            .unwrap()
            .remove(agent_id);
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
        let Ok(session) = self
            .store
            .get_agent_session_without_messages(agent_id)
            .await
        else {
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
        next_candidate_avoiding(
            &from,
            home.as_deref(),
            &tried,
            &self.exhausted_providers(),
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
    /// choice is never overwritten. Returns [`QuotaFailoverOutcome::Switched`]
    /// when the switch landed — the caller then swallows the failed attempt
    /// and the worker redrives the same message — and otherwise leaves the
    /// failure terminal (no armed worker, no candidate, an unreadable
    /// session or profile list, or a concurrent provider change that won the
    /// compare-and-set). A remote agent only moves to a configuration that
    /// can run on its machine; when every remaining backup is tied to this
    /// backend the outcome is [`QuotaFailoverOutcome::Refused`] with the
    /// actionable error to fail with instead.
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
    ) -> QuotaFailoverOutcome {
        use QuotaFailoverOutcome::Terminal;
        let session = match self
            .store
            .get_agent_session_without_messages(agent_id)
            .await
        {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover skipped: session read failed");
                return Terminal;
            }
        };
        let now = chrono::Utc::now();
        let reported = crate::provider_return::parse_reset_time(error_text, now);
        // Every other conversation learns of the spent quota now, whether or
        // not this one can fail over. The failed attempt's own provider
        // record is authoritative; without one, the session's provider is
        // only used once the identity check below proves it is the one that
        // failed (never a configuration the user switched to meanwhile).
        let recorded = failed_provider.filter(|p| !p.trim().is_empty());
        if let Some(failed) = recorded {
            self.note_provider_exhausted_unless_machine_sign_in(agent_id, failed, reported, now);
        }
        if self
            .chain_for_failed_attempt(agent_id, &session, failed_provider)
            .is_none()
        {
            tracing::info!(agent = %agent_id, "no armed attempt, or provider/model changed since the failed attempt; quota failover skipped");
            return Terminal;
        }
        if recorded.is_none() {
            if let Some(failed) = session.provider.as_deref().filter(|p| !p.trim().is_empty()) {
                self.note_provider_exhausted_unless_machine_sign_in(
                    agent_id, failed, reported, now,
                );
            }
        }
        let Some(from) = session.provider.clone() else {
            return Terminal;
        };
        let profiles = match self.provider_profiles.profiles() {
            Ok(profiles) => profiles,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "quota failover skipped: profiles unreadable");
                return Terminal;
            }
        };
        let settings = self.effective_settings();
        let from_selection = provider_profiles::canonical_selection(&from);
        let return_state = crate::provider_return::read_state(&session);
        let home = return_state.as_ref().map(|s| s.home.clone());
        let exhausted = self.exhausted_providers();
        // A remote agent only moves to a configuration that can run on its
        // machine (`Services::remote_compatibility`); the others are counted
        // as tried and passed over.
        let remote = intent_core::remote_launch::RemotePlacementView::from_metadata(
            session.metadata.as_ref(),
        );
        let mut passed_over: Option<(String, String)> = None;
        let target = loop {
            let candidate = {
                let mut chains = self.quota_failover_chains.lock().unwrap();
                let Some(chain) = chains.get_mut(agent_id) else {
                    return Terminal;
                };
                let tried = &mut chain.tried;
                if !tried.contains(&from_selection) {
                    tried.push(from_selection.clone());
                }
                let candidate = next_candidate_avoiding(
                    &from_selection,
                    home.as_deref(),
                    tried,
                    &exhausted,
                    &profiles,
                    settings.providers.enabled.as_ref(),
                );
                if let Some(candidate) = &candidate {
                    tried.push(candidate.clone());
                }
                candidate
            };
            let Some(candidate) = candidate else {
                break None;
            };
            if remote.is_some() {
                if let Some(why) = self.remote_refusal(agent_id, &candidate).await {
                    passed_over.get_or_insert((candidate, why));
                    continue;
                }
            }
            break Some(candidate);
        };
        let Some(target) = target else {
            // Nothing left to try. When this was home rejecting a return
            // before any output, it still counts as a failed return: the
            // timer moves to the reported reset or the next backoff step.
            // Guarded on the identity read above, so a concurrent manual
            // choice (which clears the state) is never undone.
            if let Some(state) = return_state.filter(|s| s.home == from_selection) {
                let failed = crate::provider_return::on_failed_return(state, reported, now);
                self.persist_provider_return_state_if_identity(
                    workspace_id,
                    agent_id,
                    &from,
                    session.model.as_deref(),
                    &failed,
                    false,
                )
                .await;
            }
            return match (remote, passed_over) {
                (Some(view), Some((backup, why))) => QuotaFailoverOutcome::Refused(
                    intent_core::Error::InvalidParams(remote_backup_incompatible_text(
                        &from_selection,
                        &backup,
                        &why,
                        &view.node_name,
                        &profiles,
                    )),
                ),
                _ => Terminal,
            };
        };
        if self
            .switch_provider_for_quota(
                workspace_id,
                agent_id,
                &session,
                &target,
                return_state,
                reported,
                &profiles,
                true,
            )
            .await
        {
            QuotaFailoverOutcome::Switched
        } else {
            Terminal
        }
    }

    /// Whether `reference` can run on a remote agent's machine.
    async fn runs_remotely(&self, agent_id: &AgentId, reference: &str) -> bool {
        self.remote_refusal(agent_id, reference).await.is_none()
    }

    /// Why `reference` cannot run on a remote agent's machine under the
    /// launch rules for where it runs (`None` when it can): a sandbox has no
    /// sign-in, so there a candidate must send a set variable. An
    /// unresolvable reference cannot run anywhere, so it is refused too.
    async fn remote_refusal(&self, agent_id: &AgentId, reference: &str) -> Option<String> {
        let cloud = match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => intent_core::remote_launch::RemotePlacementView::from_metadata(
                session.metadata.as_ref(),
            )
            .is_some_and(|view| view.is_cloud()),
            Err(e) => return Some(format!("the agent could not be read: {e}")),
        };
        let target = if cloud {
            crate::RemoteTarget::Cloud
        } else {
            crate::RemoteTarget::Machine
        };
        let why = match self.remote_launch_plan(reference, target).await {
            Ok(plan)
                if cloud
                    && !plan
                        .env
                        .as_ref()
                        .is_some_and(|env| env.entries().any(|(_, value)| value.is_some())) =>
            {
                "It sends no sign-in, and a sandbox has none of its own.".to_owned()
            }
            Ok(_) => return None,
            Err(e) => e.to_string(),
        };
        tracing::info!(agent = %agent_id, provider = %reference, why = %why, "backup the launch rules refuse on the remote agent's machine; passed over");
        Some(why)
    }

    /// Move the agent from its current configuration (`session.provider`)
    /// onto `target` because the current one is out of quota, and announce
    /// it: the shared tail of a failover after a failed attempt
    /// ([`Self::try_quota_failover`]) and of a switch ahead of a turn whose
    /// configuration the shared cache marks exhausted
    /// ([`Self::avoid_exhausted_provider`]). `reported` is the reset time
    /// feeding the return timer. `after_failed_attempt` is set for a
    /// failover: the message is a turn the failure interrupted (its redrive
    /// continues the partial work) and leaving home counts as a failed
    /// return. A switch ahead of the turn has neither: the backup receives
    /// the original message, and the return state follows
    /// [`crate::provider_return::on_skip`]. Returns whether the switch
    /// landed (a concurrent provider or model change wins the
    /// compare-and-set).
    ///
    /// The switch persists the target as the agent's provider with the same
    /// model and the return state in one statement, emits `agent:updated`,
    /// and persists one `provider_failover` system row (emitting
    /// `agent:message`).
    #[expect(clippy::too_many_arguments, reason = "one switch's full context")]
    pub(crate) async fn switch_provider_for_quota(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        session: &intent_core::AgentSession,
        target: &str,
        return_state: Option<crate::provider_return::ProviderReturnState>,
        reported: Option<chrono::DateTime<chrono::Utc>>,
        profiles: &[ProfileMeta],
        after_failed_attempt: bool,
    ) -> bool {
        let Some(from) = session.provider.clone() else {
            return false;
        };
        // A remote agent runs on its machine: failover is the one provider
        // switch it allows (`agent.setModel` refuses any other), and only to
        // a configuration that can run there.
        let remote = intent_core::remote_launch::RemotePlacementView::from_metadata(
            session.metadata.as_ref(),
        );
        if remote.is_some() && !self.runs_remotely(agent_id, target).await {
            return false;
        }
        let from_selection = provider_profiles::canonical_selection(&from);
        let now = chrono::Utc::now();
        let model = session.model.clone().filter(|m| !m.is_empty());
        let previous_return_state = return_state.clone();
        let next_state = match reported.filter(|_| !after_failed_attempt) {
            Some(until) => {
                crate::provider_return::on_skip(return_state, &from_selection, target, until, now)
            }
            None => crate::provider_return::on_switch(
                return_state,
                &from_selection,
                target,
                reported,
                now,
            ),
        };
        let next_state_json = json!(next_state).to_string();
        // The move and its return bookkeeping land in one statement, so a
        // concurrent `agent.setModel` (which clears the state) either wins
        // the compare-and-set or comes after — never in between.
        let landed = match self
            .store
            .failover_agent_session_provider(
                workspace_id,
                agent_id,
                &from,
                session.model.as_deref(),
                target,
                Some((
                    crate::provider_return::PROVIDER_RETURN_METADATA_KEY,
                    &next_state_json,
                )),
                // A switch ahead of the turn yields to a manual choice
                // saved since the caller's read (PROTOCOL §5.54).
                !after_failed_attempt,
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
            after_failed_attempt,
            "provider quota exhausted; switching to backup profile"
        );
        if after_failed_attempt {
            self.quota_failover_replays
                .lock()
                .unwrap()
                .insert(agent_id.clone());
        }
        // Leaving the configuration this message started on: a timed return
        // home that has not produced output yet is not announced. A rotation
        // that reaches home is a return attempt of its own: home's first
        // output accepts it (no `provider_returned` row — the failover row
        // below announces the move).
        self.cancel_provider_return_announcement(agent_id);
        if next_state.returning.is_some() {
            self.arm_provider_return_announcement(agent_id);
        }
        self.invalidate_agent_list_cache(workspace_id);
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "modelId": model, "providerId": target }),
        )
        .await;
        if let Some(view) = remote {
            // Announced once the agent produces output (or completes a turn)
            // on `target` ([`Self::complete_remote_failover`]); undone if the
            // node cannot run it ([`Self::fail_remote_failover`]).
            self.pending_remote_failovers.lock().unwrap().insert(
                agent_id.clone(),
                PendingRemoteFailover {
                    kind: RemoteSwitchKind::Failover,
                    from,
                    to: target.to_owned(),
                    model: session.model.clone(),
                    previous_return_state,
                    node_name: view.node_name,
                    profiles: profiles.to_vec(),
                },
            );
            return true;
        }
        self.persist_provider_failover_notice(
            workspace_id,
            agent_id,
            &from_selection,
            target,
            model.as_deref(),
            profiles,
        )
        .await;
        true
    }

    /// Record a remote agent's timed return home (`from` → `to`), persisted
    /// with `previous_return_state` replaced, as pending its node's start.
    pub(crate) fn note_remote_return_pending(
        &self,
        agent_id: &AgentId,
        session: &intent_core::AgentSession,
        to: &str,
        previous_return_state: crate::provider_return::ProviderReturnState,
    ) {
        let Some(view) = intent_core::remote_launch::RemotePlacementView::from_metadata(
            session.metadata.as_ref(),
        ) else {
            return;
        };
        let Some(from) = session.provider.clone() else {
            return;
        };
        self.pending_remote_failovers.lock().unwrap().insert(
            agent_id.clone(),
            PendingRemoteFailover {
                kind: RemoteSwitchKind::Return,
                from,
                to: to.to_owned(),
                model: session.model.clone(),
                previous_return_state: Some(previous_return_state),
                node_name: view.node_name,
                profiles: self
                    .provider_profiles
                    .profiles()
                    .map(|p| p.to_vec())
                    .unwrap_or_default(),
            },
        );
    }

    /// Whether `reference` can run on a remote agent's machine (see
    /// [`Self::runs_remotely`]).
    pub(crate) async fn remote_agent_can_run(&self, agent_id: &AgentId, reference: &str) -> bool {
        self.runs_remotely(agent_id, reference).await
    }

    /// Whether a remote agent's session names the backup of a quota switch
    /// not yet announced: the one provider change its live run may be
    /// replaced for instead of refused.
    pub(crate) fn remote_failover_pending(
        &self,
        agent_id: &AgentId,
        session: &intent_core::AgentSession,
    ) -> bool {
        self.pending_remote_failovers
            .lock()
            .unwrap()
            .get(agent_id)
            .is_some_and(|p| {
                session.provider.as_deref() == Some(p.to.as_str()) && session.model == p.model
            })
    }

    /// Whether a remote agent has an automatic switch awaiting its node.
    pub(crate) fn remote_switch_pending(&self, agent_id: &AgentId) -> bool {
        self.pending_remote_failovers
            .lock()
            .unwrap()
            .contains_key(agent_id)
    }

    /// The node runs the remote agent on its new configuration — it produced
    /// output there, or completed a turn: confirm the pending switch, and
    /// announce a quota failover now, ahead of that output. A no-op without
    /// one. Confirmation waits for this because the node starts the agent
    /// asynchronously: a start failure only surfaces as the first prompt
    /// failing (`node-agent-not-running`, see
    /// [`Self::fail_remote_failover`]).
    pub(crate) async fn complete_remote_failover(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        let Some(pending) = self
            .pending_remote_failovers
            .lock()
            .unwrap()
            .remove(agent_id)
        else {
            return;
        };
        if pending.kind != RemoteSwitchKind::Failover {
            return;
        }
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return;
        };
        if session.provider.as_deref() != Some(pending.to.as_str()) {
            return;
        }
        let model = pending.model.clone().filter(|m| !m.is_empty());
        self.persist_provider_failover_notice(
            workspace_id,
            agent_id,
            &provider_profiles::canonical_selection(&pending.from),
            &pending.to,
            model.as_deref(),
            &pending.profiles,
        )
        .await;
    }

    /// The node could not run the remote agent on its new configuration
    /// (`reason`): the start was refused, or the first prompt failed before
    /// any output. With an automatic switch pending, undo it — the previous
    /// provider and return state come back, unless the session changed
    /// since — and return the actionable [`REMOTE_FAILOVER_UNAVAILABLE`]
    /// error the agent fails with; nothing is retried. `None` without one.
    pub(crate) async fn fail_remote_failover(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        reason: &str,
    ) -> Option<intent_core::Error> {
        let pending = self
            .pending_remote_failovers
            .lock()
            .unwrap()
            .remove(agent_id)?;
        // A failed return home is a failed return: back on the backup with
        // the next backoff step, so the next turn does not retry it at once.
        let previous_return_state = match pending.kind {
            RemoteSwitchKind::Failover => pending.previous_return_state.clone(),
            RemoteSwitchKind::Return => pending.previous_return_state.clone().map(|state| {
                crate::provider_return::on_failed_return(state, None, chrono::Utc::now())
            }),
        };
        // Home's earlier output (what made the return due early) is stale
        // once it failed to start here: without dropping it, the next turn
        // would bypass the backoff and retry home at once.
        if pending.kind == RemoteSwitchKind::Return {
            self.provider_quota_cache.clear_working(&pending.to);
        }
        let restored_state = json!(previous_return_state).to_string();
        let restored = self
            .store
            .failover_agent_session_provider(
                workspace_id,
                agent_id,
                &pending.to,
                pending.model.as_deref(),
                &pending.from,
                Some((
                    crate::provider_return::PROVIDER_RETURN_METADATA_KEY,
                    &restored_state,
                )),
                false,
                &now_iso(),
            )
            .await;
        match restored {
            Ok(true) => {
                self.quota_failover_replays.lock().unwrap().remove(agent_id);
                self.cancel_provider_return_announcement(agent_id);
                self.invalidate_agent_list_cache(workspace_id);
                let model = pending.model.clone().filter(|m| !m.is_empty());
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    intent_core::events::AGENT_UPDATED,
                    json!({ "agentId": agent_id.0, "modelId": model, "providerId": pending.from }),
                )
                .await;
            }
            Ok(false) => {
                tracing::info!(agent = %agent_id, "session changed since the remote quota switch; not restored");
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "restoring the provider after a failed remote failover failed");
            }
        }
        let from = provider_profiles::canonical_selection(&pending.from);
        let placement = match self
            .store
            .get_agent_session_without_messages(agent_id)
            .await
        {
            Ok(session) => intent_core::remote_launch::RemotePlacementView::from_metadata(
                session.metadata.as_ref(),
            )
            .and_then(|view| view.placement_error),
            Err(_) => None,
        };
        let reason = &with_placement_error(reason, placement.as_ref());
        Some(intent_core::Error::InvalidParams(match pending.kind {
            RemoteSwitchKind::Failover => remote_backup_failed_text(
                &from,
                &pending.to,
                &pending.node_name,
                reason,
                &pending.profiles,
            ),
            RemoteSwitchKind::Return => remote_return_failed_text(
                &from,
                &pending.to,
                &pending.node_name,
                reason,
                &pending.profiles,
            ),
        }))
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

/// The error a remote agent fails with when its only backups cannot run on
/// its machine: `why` is the launch rules' refusal, which names the fix.
fn remote_backup_incompatible_text(
    from: &str,
    backup: &str,
    why: &str,
    machine: &str,
    profiles: &[ProfileMeta],
) -> String {
    let from = provider_profiles::readable_selection(from, Some(profiles));
    let backup = provider_profiles::readable_selection(backup, Some(profiles));
    format!(
        "{REMOTE_FAILOVER_UNAVAILABLE} {from} ran out of quota on {machine}. The backup {backup} \
         can't run there: {why}"
    )
}

/// The error a remote agent fails with when its machine could not start the
/// backup it was switched to.
fn remote_backup_failed_text(
    from: &str,
    backup: &str,
    machine: &str,
    reason: &str,
    profiles: &[ProfileMeta],
) -> String {
    let from = provider_profiles::readable_selection(from, Some(profiles));
    let backup = provider_profiles::readable_selection(backup, Some(profiles));
    let reason = reason.trim().trim_end_matches('.');
    let not_sign_in = |cause: String| {
        format!(
            "{cause}, which is not a sign-in problem. Retry the message, or create a new agent."
        )
    };
    let has = |text: &str| reason.contains(text);
    let advice = if let Some(cause) = non_account_start_cause(reason, machine) {
        not_sign_in(cause)
    } else if has(provider_profiles::PROFILE_NOT_REMOTE_COMPATIBLE) {
        format!(
            "{backup} can't run on other machines: it isn't marked for remote machines, or it \
             uses an account folder on this backend. Turn on Use on remote machines for it in \
             Settings → Providers, or create a new agent with a configuration that is marked."
        )
    } else if has("request `initialize` timed out") {
        not_sign_in(format!(
            "the backup {backup} started on {machine}, but its provider did not respond in time"
        ))
    } else if has("exited with status") {
        format!(
            "the provider stopped on {machine} before it was ready. Retry the message; if it \
             stops again, check that {backup} is signed in on {machine}, or create a new agent."
        )
    } else {
        format!("Sign in to {backup} on {machine}, or create a new agent with another account.")
    };
    format!(
        "{REMOTE_FAILOVER_UNAVAILABLE} {from} ran out of quota on {machine}, and the backup \
         {backup} couldn't start there: {reason}. {advice}"
    )
}

/// `reason` completed with the agent's recorded `placementError` when it
/// carries no detail of its own: an older machine's refusal says only "the
/// agent has exited". A reason with its own detail is kept as it is, so an
/// error recorded for an earlier start cannot reword it.
fn with_placement_error(
    reason: &str,
    placement: Option<&intent_core::remote_launch::PlacementError>,
) -> String {
    match placement {
        Some(error)
            if !error.detail.is_empty()
                && reason
                    .trim()
                    .trim_end_matches('.')
                    .ends_with("the agent has exited") =>
        {
            format!("{reason} ({}: {})", error.code, error.detail)
        }
        _ => reason.to_owned(),
    }
}

/// What stopped a remote start when it was not the provider or its account
/// (intent-hq/intent#8383): the node error codes in `reason` for the
/// checkout, the machine's capacity, its shutdown or the link. `None` for
/// an account or provider start failure, or a reason without such a code.
fn non_account_start_cause(reason: &str, machine: &str) -> Option<String> {
    let has = |code: &str| reason.contains(code);
    if has("node-checkout-failed") || has("node-git-") {
        Some(format!(
            "{machine} couldn't fetch the agent's code from this backend"
        ))
    } else if has("node-capacity") {
        Some(format!("{machine} has no room for another agent"))
    } else if has("node-shutting-down") {
        Some(format!("{machine} is shutting down"))
    } else if has("node-link-") {
        Some(format!("the connection to {machine} was lost"))
    } else {
        None
    }
}

/// The error a remote agent fails with when its machine could not start
/// home on a timed return; it stays on the backup.
fn remote_return_failed_text(
    away: &str,
    home: &str,
    machine: &str,
    reason: &str,
    profiles: &[ProfileMeta],
) -> String {
    let away = provider_profiles::readable_selection(away, Some(profiles));
    let home = provider_profiles::readable_selection(home, Some(profiles));
    let reason = reason.trim().trim_end_matches('.');
    format!(
        "{REMOTE_FAILOVER_UNAVAILABLE} {home} couldn't start on {machine} when going back from \
         the backup {away}: {reason}. The agent stays on {away}; send your message again to \
         continue there, or create a new agent with another account."
    )
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
