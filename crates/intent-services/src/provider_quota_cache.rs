//! Daemon-wide quota health of provider configurations (PROTOCOL §5.54
//! Backup failover, "Shared quota state").
//!
//! Every conversation's own failover chain and return timer
//! ([`crate::quota_failover`], [`crate::provider_return`]) only knows what
//! that conversation saw. This cache shares the latest observation per
//! configuration (a base provider's Default configuration by its bare id, a
//! profile by its reference) across every conversation on the daemon:
//!
//! - A quota failure marks the configuration *exhausted* until the reset time
//!   the error reports (when usable) or [`MARK_DURATION`] later. Other
//!   conversations then skip it — at the start of their next turn and in the
//!   failover rotation — instead of each spending a failed turn on it.
//! - A configuration's first output on an attempt marks it *working*. A
//!   conversation away from home whose home is marked working goes back on
//!   its next turn without waiting for its own timer.
//!
//! Exhausted marks are advisory: when every candidate is marked the turn
//! still tries them (see `next_candidate_avoiding`). In-memory only — after a
//! daemon restart each conversation's persisted return timer applies as
//! before.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use intent_core::AgentId;

use crate::provider_profiles;

/// How long an exhausted mark lasts when the quota error reports no usable
/// reset time (the same 5 hours as the per-conversation return timer).
pub(crate) const MARK_DURATION: Duration = crate::provider_return::FALLBACK_RETURN_AFTER;

/// The latest quota observation for one configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuotaHealth {
    /// A quota failure was seen at `marked_at`; skip until `until`.
    Exhausted {
        marked_at: DateTime<Utc>,
        until: DateTime<Utc>,
    },
    /// The latest attempt produced output.
    Working,
}

/// Shared across `Services` clones (see the module docs).
#[derive(Clone, Debug, Default)]
pub(crate) struct ProviderQuotaCache {
    entries: Arc<Mutex<HashMap<String, QuotaHealth>>>,
    /// Per agent, the configuration the user chose by hand that no turn has
    /// started on yet: the first turn that starts on exactly that
    /// configuration tries it even when it is marked (a manual choice is the
    /// new starting point), see [`Self::take_manual_choice`].
    manual_choices: Arc<Mutex<ManualChoices>>,
}

/// Manual choices per agent (see [`ProviderQuotaCache::begin_manual_choice`]).
/// `generation` is one counter for every record and every turn-start read,
/// so a committed choice can be ordered against the read that checks it.
#[derive(Debug, Default)]
struct ManualChoices {
    generation: u64,
    by_agent: HashMap<AgentId, AgentChoices>,
}

#[derive(Debug, Default)]
struct AgentChoices {
    /// `agent.setModel` calls whose persist is in flight, by token. Never
    /// pruned by a turn start: the persist may land after its read.
    pending: HashMap<u64, PendingChoice>,
    /// Choices whose persist succeeded and that no turn has started on.
    committed: Vec<CommittedChoice>,
}

impl AgentChoices {
    fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.committed.is_empty()
    }
}

#[derive(Debug)]
struct PendingChoice {
    selection: String,
    /// A turn already started on it while the persist was in flight.
    consumed: bool,
}

#[derive(Debug)]
struct CommittedChoice {
    selection: String,
    /// When the persist was known to have landed.
    generation: u64,
}

/// One in-flight manual choice, from [`ProviderQuotaCache::begin_manual_choice`].
/// [`Self::commit`] after the persist succeeds; dropping it uncommitted (a
/// failed persist, `?`, or a cancelled `agent.setModel`) discards only this
/// call's record, so an earlier successful choice and other in-flight calls
/// are untouched however the calls overlap.
#[derive(Debug)]
#[must_use = "commit after the persist succeeds"]
pub(crate) struct ManualChoiceGuard {
    cache: ProviderQuotaCache,
    agent: AgentId,
    token: u64,
    finished: bool,
}

impl ManualChoiceGuard {
    /// The persist succeeded: the choice is the agent's selection.
    pub(crate) fn commit(mut self) {
        self.finished = true;
        self.cache
            .finish_manual_choice(&self.agent, self.token, true);
    }
}

impl Drop for ManualChoiceGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.cache
                .finish_manual_choice(&self.agent, self.token, false);
        }
    }
}

impl ProviderQuotaCache {
    /// Record a quota failure on `selection` at `now`. The mark lasts until
    /// `reported` (the reset time parsed from the error) when usable, else
    /// [`MARK_DURATION`]. The newest failure always replaces the entry.
    pub(crate) fn mark_exhausted(
        &self,
        selection: &str,
        reported: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) {
        let until =
            crate::provider_return::usable_reset(reported, now).unwrap_or(now + MARK_DURATION);
        let selection = provider_profiles::canonical_selection(selection);
        tracing::info!(provider = %selection, until = %until, "provider marked out of quota for all conversations");
        self.entries.lock().unwrap().insert(
            selection,
            QuotaHealth::Exhausted {
                marked_at: now,
                until,
            },
        );
    }

    /// Record that an attempt on `selection` which started at
    /// `attempt_started` produced output at `now`. An exhausted mark is only
    /// replaced when it was recorded before the attempt started: a turn
    /// already in flight when another one hit the quota proves nothing about
    /// the quota now.
    pub(crate) fn mark_working(
        &self,
        selection: &str,
        attempt_started: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        let selection = provider_profiles::canonical_selection(selection);
        let mut entries = self.entries.lock().unwrap();
        match entries.get(&selection) {
            Some(QuotaHealth::Working) => {}
            Some(QuotaHealth::Exhausted { marked_at, until })
                if *marked_at >= attempt_started && now < *until => {}
            previous => {
                if previous.is_some() {
                    tracing::info!(provider = %selection, "provider quota available again for all conversations");
                }
                entries.insert(selection, QuotaHealth::Working);
            }
        }
    }

    /// The user is choosing `selection` for `agent` by hand. Recorded BEFORE
    /// the choice is persisted, so any turn start that reads the persisted
    /// choice also sees this record (see [`Self::take_manual_choice`]).
    pub(crate) fn begin_manual_choice(
        &self,
        agent: &AgentId,
        selection: &str,
    ) -> ManualChoiceGuard {
        let mut choices = self.manual_choices.lock().unwrap();
        choices.generation += 1;
        let token = choices.generation;
        choices
            .by_agent
            .entry(agent.clone())
            .or_default()
            .pending
            .insert(
                token,
                PendingChoice {
                    selection: provider_profiles::canonical_selection(selection),
                    consumed: false,
                },
            );
        ManualChoiceGuard {
            cache: self.clone(),
            agent: agent.clone(),
            token,
            finished: false,
        }
    }

    /// End the in-flight choice `token`: on success it becomes a committed
    /// choice (unless a turn already started on it); on failure only its own
    /// record goes.
    fn finish_manual_choice(&self, agent: &AgentId, token: u64, persisted: bool) {
        let mut guard = self.manual_choices.lock().unwrap();
        let choices = &mut *guard;
        let Some(agent_choices) = choices.by_agent.get_mut(agent) else {
            return;
        };
        if let Some(pending) = agent_choices.pending.remove(&token) {
            if persisted && !pending.consumed {
                choices.generation += 1;
                agent_choices
                    .committed
                    .retain(|c| c.selection != pending.selection);
                agent_choices.committed.push(CommittedChoice {
                    selection: pending.selection,
                    generation: choices.generation,
                });
            }
        }
        if agent_choices.is_empty() {
            choices.by_agent.remove(agent);
        }
    }

    /// Stamp a turn start's session read: call BEFORE reading the session,
    /// and pass the result to [`Self::take_manual_choice`] with the
    /// configuration that read returned.
    pub(crate) fn manual_choice_read(&self) -> u64 {
        let mut choices = self.manual_choices.lock().unwrap();
        choices.generation += 1;
        choices.generation
    }

    /// Whether a turn starting on `current` (read after `read`) is the first
    /// on one of `agent`'s manual choices: it then tries `current` and is
    /// never moved away ahead of the attempt. Every record of `current` is
    /// used up. A committed choice of another configuration that landed
    /// before the read was replaced by a later write and is dropped; one
    /// that landed after, and every in-flight one, is kept for the turn that
    /// reads it.
    pub(crate) fn take_manual_choice(&self, agent: &AgentId, current: &str, read: u64) -> bool {
        let current = provider_profiles::canonical_selection(current);
        let mut choices = self.manual_choices.lock().unwrap();
        let Some(agent_choices) = choices.by_agent.get_mut(agent) else {
            return false;
        };
        let mut matched = false;
        agent_choices.committed.retain(|c| {
            if c.selection == current {
                matched = true;
                false
            } else {
                c.generation > read
            }
        });
        for pending in agent_choices.pending.values_mut() {
            if pending.selection == current && !pending.consumed {
                pending.consumed = true;
                matched = true;
            }
        }
        if agent_choices.is_empty() {
            choices.by_agent.remove(agent);
        }
        matched
    }

    /// Whether any manual-choice record is held for `agent`.
    #[cfg(test)]
    pub(crate) fn is_pending_or_committed(&self, agent: &AgentId) -> bool {
        self.manual_choices
            .lock()
            .unwrap()
            .by_agent
            .contains_key(agent)
    }

    /// The time `selection`'s exhausted mark lasts until, while it is live at
    /// `now`. An expired mark is pruned and reads as unknown.
    pub(crate) fn exhausted_until(
        &self,
        selection: &str,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let selection = provider_profiles::canonical_selection(selection);
        let mut entries = self.entries.lock().unwrap();
        match entries.get(&selection) {
            Some(QuotaHealth::Exhausted { until, .. }) if now < *until => Some(*until),
            Some(QuotaHealth::Exhausted { .. }) => {
                entries.remove(&selection);
                None
            }
            _ => None,
        }
    }

    /// Whether `selection` is marked exhausted at `now`.
    pub(crate) fn is_exhausted(&self, selection: &str, now: DateTime<Utc>) -> bool {
        self.exhausted_until(selection, now).is_some()
    }

    /// Whether the latest observation of `selection` is a working attempt.
    pub(crate) fn is_known_working(&self, selection: &str) -> bool {
        let selection = provider_profiles::canonical_selection(selection);
        matches!(
            self.entries.lock().unwrap().get(&selection),
            Some(QuotaHealth::Working)
        )
    }

    /// Every configuration marked exhausted at `now` (expired marks pruned).
    pub(crate) fn exhausted(&self, now: DateTime<Utc>) -> Vec<String> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, health| match health {
            QuotaHealth::Exhausted { until, .. } => now < *until,
            QuotaHealth::Working => true,
        });
        entries
            .iter()
            .filter(|(_, health)| matches!(health, QuotaHealth::Exhausted { .. }))
            .map(|(selection, _)| selection.clone())
            .collect()
    }
}

#[cfg(test)]
#[path = "provider_quota_cache_tests.rs"]
mod tests;
