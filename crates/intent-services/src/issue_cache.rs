//! The daemon-side issue cache behind `github.issues.get` — the issue
//! hover card's read — kept next to the shared PR cache
//! ([`crate::pr_monitor::PrCache`]) and governed by the same knob: an entry
//! younger than `prCache.maxAgeSeconds` is served with no forge call, an
//! older or absent one costs exactly one `get_issue`, whose result is stored
//! for the next read. Errors (including quota exhaustion) propagate to the
//! caller and are never cached.
//!
//! Retention mirrors the PR cache's unmonitored policy — nothing keeps an
//! issue alive, so every entry expires at [`ISSUE_CACHE_MAX_IDLE`] and the
//! map is bounded by [`ISSUE_CACHE_MAX_ENTRIES`] (oldest fetch evicted
//! first), enforced on every write. In-memory only; a daemon restart starts
//! cold. Shared across [`Services`] clones.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::Result;
use intent_sourcecontrol::{Issue, RepoRef};

use crate::pr_monitor::{PR_CACHE_MAX_ENTRIES, PR_CACHE_MAX_IDLE};
use crate::{pr_ops, Services};

/// Age past which an entry is dropped by the retention pass — the PR
/// cache's unmonitored idle expiry.
pub(crate) const ISSUE_CACHE_MAX_IDLE: Duration = PR_CACHE_MAX_IDLE;

/// Hard bound on the entries the cache holds, enforced on every write — the
/// PR cache's unmonitored cap.
pub(crate) const ISSUE_CACHE_MAX_ENTRIES: usize = PR_CACHE_MAX_ENTRIES;

/// The `(owner, repo, issue)` identity an entry is keyed on: the [`RepoRef`]
/// identity ([`RepoRef::identity_parts`]), so case-variant slugs share one
/// entry like they share one PR cache slot.
type IssueKey = (String, String, u64);

fn issue_key(repo_ref: &RepoRef, number: u64) -> IssueKey {
    let (owner, name) = repo_ref.identity_parts();
    (owner, name, number)
}

/// One issue's last forge read.
#[derive(Debug, Clone)]
pub(crate) struct IssueCacheEntry {
    pub(crate) issue: Issue,
    /// When `issue` was read — the freshness anchor and the retention
    /// pass's order key.
    pub(crate) fetched_at: Instant,
}

/// The issue cache; see the module docs.
pub(crate) type IssueCache = Arc<Mutex<HashMap<IssueKey, IssueCacheEntry>>>;

/// The cached issue for `key` when it was read less than `max_age` ago;
/// `None` when the entry is absent or older.
fn cached_issue_within(cache: &IssueCache, key: &IssueKey, max_age: Duration) -> Option<Issue> {
    let now = Instant::now();
    cache
        .lock()
        .unwrap()
        .get(key)
        .filter(|entry| now.saturating_duration_since(entry.fetched_at) < max_age)
        .map(|entry| entry.issue.clone())
}

/// Store a fresh read, then run the retention pass.
fn store_issue(cache: &IssueCache, key: IssueKey, issue: Issue) {
    let now = Instant::now();
    let mut cache = cache.lock().unwrap();
    cache.insert(
        key,
        IssueCacheEntry {
            issue,
            fetched_at: now,
        },
    );
    retain_issue_cache(&mut cache, now);
}

/// The retention pass, run under the cache lock after every write: entries
/// read [`ISSUE_CACHE_MAX_IDLE`] ago or earlier are dropped, then the rest
/// are bounded by [`ISSUE_CACHE_MAX_ENTRIES`], evicting the oldest
/// `fetched_at` first.
fn retain_issue_cache(cache: &mut HashMap<IssueKey, IssueCacheEntry>, now: Instant) {
    cache.retain(|_, entry| now.saturating_duration_since(entry.fetched_at) < ISSUE_CACHE_MAX_IDLE);
    let excess = cache.len().saturating_sub(ISSUE_CACHE_MAX_ENTRIES);
    if excess == 0 {
        return;
    }
    let mut by_age: Vec<(Instant, IssueKey)> = cache
        .iter()
        .map(|(key, entry)| (entry.fetched_at, key.clone()))
        .collect();
    by_age.sort();
    for (_, key) in by_age.into_iter().take(excess) {
        cache.remove(&key);
    }
}

impl Services {
    /// Read one issue through the cache under the on-demand readers'
    /// policy: served without a forge call when read within
    /// `prCache.maxAgeSeconds` ([`Self::pr_cache_max_age`]), else fetched
    /// once via `get_issue` and stored.
    ///
    /// # Errors
    ///
    /// Returns an error when no source-control provider is configured or
    /// the forge read fails; a failed read stores nothing.
    pub(crate) async fn read_issue(&self, repo_ref: &RepoRef, number: u64) -> Result<Issue> {
        let key = issue_key(repo_ref, number);
        if let Some(issue) = cached_issue_within(&self.issue_cache, &key, self.pr_cache_max_age()) {
            tracing::trace!(
                issue_number = number,
                "issue cache: serving the cached read"
            );
            return Ok(issue);
        }
        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        let issue = sc
            .get_issue(repo_ref, number)
            .await
            .map_err(pr_ops::map_sc_err)?;
        store_issue(&self.issue_cache, key, issue.clone());
        Ok(issue)
    }

    /// Age every cache entry by `by`, so a test can cross the `Serve`
    /// window or [`ISSUE_CACHE_MAX_IDLE`] without sleeping.
    #[cfg(test)]
    pub(crate) fn backdate_issue_cache(&self, by: Duration) {
        for entry in self.issue_cache.lock().unwrap().values_mut() {
            entry.fetched_at = entry.fetched_at.checked_sub(by).unwrap_or(entry.fetched_at);
        }
    }

    /// The number of issues the cache currently holds.
    #[cfg(test)]
    pub(crate) fn issue_cache_len(&self) -> usize {
        self.issue_cache.lock().unwrap().len()
    }

    /// Whether the cache holds `number` in `repo_ref`.
    #[cfg(test)]
    pub(crate) fn issue_cached(&self, repo_ref: &RepoRef, number: u64) -> bool {
        self.issue_cache
            .lock()
            .unwrap()
            .contains_key(&issue_key(repo_ref, number))
    }
}
