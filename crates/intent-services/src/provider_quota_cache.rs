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
//! - Saving an edit to a configuration (or deleting a profile) forgets its
//!   entry, so every conversation tries it again (see
//!   [`ProviderQuotaCache::forget`]). A process still running on the
//!   settings from before an environment edit or a delete keeps reporting on
//!   the old account or key, so its observations are dropped: each mark
//!   takes the [`SettingsRead`] the run started with, and the cache checks it
//!   against the saved settings under its lock
//!   ([`ProfileActivation::is_current`]). The edit moves the saved settings
//!   fingerprint before it forgets, so an observation racing the edit is
//!   either rejected by the check or cleared by the forget. Lock order: this
//!   cache, then the profile activation registry.
//!
//! Exhausted marks are advisory: when every candidate is marked the turn
//! still tries them (see `next_candidate_avoiding`). In-memory only — after a
//! daemon restart each conversation's persisted return timer applies as
//! before.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};

use crate::profile_activation::{ProfileActivation, SettingsRead};
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
#[derive(Clone, Debug)]
pub(crate) struct ProviderQuotaCache {
    entries: Arc<Mutex<HashMap<String, QuotaHealth>>>,
    /// The registry of saved settings every mark is checked against.
    activation: Arc<ProfileActivation>,
}

impl ProviderQuotaCache {
    /// A cache whose marks are checked against `activation`'s saved
    /// settings (the daemon's one registry).
    pub(crate) fn new(activation: Arc<ProfileActivation>) -> Self {
        Self {
            entries: Arc::default(),
            activation,
        }
    }

    /// Record a quota failure on the configuration `read` belongs to at
    /// `now`. The mark lasts until `reported` (the reset time parsed from
    /// the error) when usable, else [`MARK_DURATION`]. The newest failure
    /// always replaces the entry. Nothing is recorded when `read` is no
    /// longer the saved settings (edited or deleted since).
    pub(crate) fn mark_exhausted(
        &self,
        read: &SettingsRead,
        reported: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) {
        let until =
            crate::provider_return::usable_reset(reported, now).unwrap_or(now + MARK_DURATION);
        let selection = read.selection().to_owned();
        let mut entries = self.entries.lock().unwrap();
        if !self.activation.is_current(read) {
            tracing::info!(provider = %selection, "quota failure on earlier provider settings; shared quota state unchanged");
            return;
        }
        tracing::info!(provider = %selection, until = %until, "provider marked out of quota for all conversations");
        entries.insert(
            selection,
            QuotaHealth::Exhausted {
                marked_at: now,
                until,
            },
        );
    }

    /// Record that an attempt on the configuration `read` belongs to, which
    /// started at `attempt_started`, produced output at `now`. An exhausted
    /// mark is only replaced when it was recorded before the attempt
    /// started: a turn already in flight when another one hit the quota
    /// proves nothing about the quota now. Like [`Self::mark_exhausted`],
    /// nothing is recorded when `read` is no longer the saved settings.
    pub(crate) fn mark_working(
        &self,
        read: &SettingsRead,
        attempt_started: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        let selection = read.selection().to_owned();
        let mut entries = self.entries.lock().unwrap();
        if !self.activation.is_current(read) {
            return;
        }
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

    /// Drop whatever is known about `selection`'s quota, so every
    /// conversation treats it as unknown again: no longer skipped, and no
    /// longer a reason to return early. Called when an edit to the
    /// configuration is saved or a profile is deleted, after the saved
    /// settings fingerprint moved (see the module docs).
    pub(crate) fn forget(&self, selection: &str) {
        let selection = provider_profiles::canonical_selection(selection);
        if self.entries.lock().unwrap().remove(&selection).is_some() {
            tracing::info!(provider = %selection, "provider settings changed; quota state cleared for all conversations");
        }
    }

    /// Drop a working observation of `selection` (an exhausted mark is
    /// kept): an attempt that never got going proves the earlier output no
    /// longer says it is usable, so a return to it waits for its backoff
    /// instead of being retried on the next turn.
    pub(crate) fn clear_working(&self, selection: &str) {
        let selection = provider_profiles::canonical_selection(selection);
        let mut entries = self.entries.lock().unwrap();
        if matches!(entries.get(&selection), Some(QuotaHealth::Working)) {
            entries.remove(&selection);
        }
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
