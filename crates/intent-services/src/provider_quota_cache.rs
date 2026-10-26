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
