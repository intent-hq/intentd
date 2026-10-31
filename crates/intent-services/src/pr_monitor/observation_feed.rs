//! The PR observation feed: the bridge from every OTHER fresh PR read
//! (workspace PR refresh / discovery, `github.pulls.get`, `ws.pr.snapshot`)
//! into the monitor sweep.
//!
//! The monitor polls on its own cadence, which idles out to minutes for a
//! quiet or settled workspace, while the workspace PR pill is updated by
//! whichever path reads the PR first. Without this feed the two could show
//! different lifecycles for one PR until the monitor's next scheduled poll:
//! a merge the pill already shows would still read "open; queued to merge"
//! in the monitor card, and the agent would not be woken.
//!
//! The feed remembers the PR signature each monitored PR had when the
//! monitor sweep last read it ([`ObservationFeed::record_poll`]). Only the
//! sweep advances it, because only the sweep polls every sibling monitor on
//! a PR; registration and check-now update one monitor, so they only seed a
//! missing baseline and otherwise observe ([`ObservationFeed::seed_or_observe`]).
//! Any other fresh
//! read whose signature differs ([`ObservationFeed::observe`]) marks the PR
//! changed; the due-sweep polls marked PRs ahead of their idle interval and
//! the loop is kicked so that happens immediately. A mark is taken when the
//! sweep starts the PR's fetch and restored if the fetch fails.
//!
//! The signature is limited to what both reads report identically —
//! lifecycle state, draft flag, head SHA — so a REST record and the
//! monitor's folded GraphQL observation never disagree on formatting alone
//! (`updatedAt` is deliberately excluded for that reason).

use std::collections::{HashMap, HashSet};

use intent_sourcecontrol::{PrState, PullRequest};

use super::PrKey;

/// The lifecycle facts the monitor card and the workspace PR pill both show.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Signature {
    state: PrState,
    draft: bool,
    head_sha: Option<String>,
}

impl Signature {
    fn of(pr: &PullRequest) -> Self {
        Self {
            state: pr.state,
            draft: pr.draft,
            head_sha: pr.head_sha.clone(),
        }
    }

    /// Whether `observed` reports something the monitor has not seen. A head
    /// SHA either read omits is unknown, not a change.
    fn differs_from(&self, observed: &Self) -> bool {
        self.state != observed.state
            || self.draft != observed.draft
            || matches!(
                (&self.head_sha, &observed.head_sha),
                (Some(seen), Some(now)) if seen != now
            )
    }
}

#[derive(Debug, Default)]
pub(crate) struct ObservationFeed {
    /// What the monitor last read, per monitored PR.
    seen: HashMap<PrKey, Signature>,
    /// PRs another read saw move since the monitor's last read.
    changed: HashSet<PrKey>,
}

impl ObservationFeed {
    /// The monitor read `pr`: it is now the baseline other reads compare to.
    pub(crate) fn record_poll(&mut self, key: PrKey, pr: &PullRequest) {
        self.seen.insert(key, Signature::of(pr));
    }

    /// Another path read `pr` fresh from the forge. Returns `true` when this
    /// observation newly marks a monitored PR changed. A PR the monitor has
    /// not read yet (no baseline: unmonitored, or not polled since boot,
    /// which the restart catch-up covers) is ignored.
    pub(crate) fn observe(&mut self, key: PrKey, pr: &PullRequest) -> bool {
        let Some(seen) = self.seen.get(&key) else {
            return false;
        };
        seen.differs_from(&Signature::of(pr)) && self.changed.insert(key)
    }

    /// A read that updates only ONE monitor on the PR (registration,
    /// check-now). With no baseline yet it seeds one, so a fresh monitor is
    /// covered before its first sweep poll. With a baseline it must not
    /// advance it — sibling monitors (other workspaces) on the PR were not
    /// updated — so it is an observation: it marks the PR when it moved and
    /// returns whether that mark is new.
    pub(crate) fn seed_or_observe(&mut self, key: PrKey, pr: &PullRequest) -> bool {
        if self.seen.contains_key(&key) {
            return self.observe(key, pr);
        }
        self.record_poll(key, pr);
        false
    }

    /// Whether `key` is marked changed.
    pub(crate) fn is_changed(&self, key: &PrKey) -> bool {
        self.changed.contains(key)
    }

    /// Clear the mark as the sweep starts fetching `key`. Returns whether it
    /// was marked, so a failed fetch can [`Self::restore`] it.
    pub(crate) fn take(&mut self, key: &PrKey) -> bool {
        self.changed.remove(key)
    }

    /// Put back a mark whose fetch failed.
    pub(crate) fn restore(&mut self, key: PrKey) {
        if self.seen.contains_key(&key) {
            self.changed.insert(key);
        }
    }

    /// Forget PRs no active monitor watches any more.
    pub(crate) fn retain(&mut self, monitored: &HashSet<PrKey>) {
        self.seen.retain(|key, _| monitored.contains(key));
        self.changed.retain(|key| monitored.contains(key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: i64) -> PrKey {
        ("acme".into(), "widgets".into(), n)
    }

    fn pr(state: PrState, draft: bool, head: Option<&str>) -> PullRequest {
        PullRequest {
            number: 1,
            url: String::new(),
            title: String::new(),
            body: None,
            state,
            draft,
            source_branch: "feature".into(),
            target_branch: "main".into(),
            author: String::new(),
            mergeable: None,
            mergeable_state: None,
            head_sha: head.map(str::to_string),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn unmonitored_or_unpolled_prs_are_ignored() {
        let mut feed = ObservationFeed::default();
        assert!(!feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
        assert!(!feed.is_changed(&key(1)));
    }

    #[test]
    fn a_merge_seen_elsewhere_marks_the_monitored_pr_once() {
        let mut feed = ObservationFeed::default();
        feed.record_poll(key(1), &pr(PrState::Open, false, Some("a")));
        assert!(feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
        assert!(feed.is_changed(&key(1)));
        // The same news again is not a new mark (no second kick).
        assert!(!feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
    }

    #[test]
    fn identical_or_unknown_signals_do_not_mark() {
        let mut feed = ObservationFeed::default();
        feed.record_poll(key(1), &pr(PrState::Open, false, Some("a")));
        assert!(!feed.observe(key(1), &pr(PrState::Open, false, Some("a"))));
        assert!(!feed.observe(key(1), &pr(PrState::Open, false, None)));
        assert!(!feed.is_changed(&key(1)));
    }

    #[test]
    fn draft_close_and_push_each_mark() {
        for observed in [
            pr(PrState::Open, true, Some("a")),
            pr(PrState::Closed, false, Some("a")),
            pr(PrState::Open, false, Some("b")),
        ] {
            let mut feed = ObservationFeed::default();
            feed.record_poll(key(1), &pr(PrState::Open, false, Some("a")));
            assert!(feed.observe(key(1), &observed), "{observed:?}");
        }
    }

    #[test]
    fn take_and_restore_follow_the_fetch_outcome() {
        let mut feed = ObservationFeed::default();
        feed.record_poll(key(1), &pr(PrState::Open, false, Some("a")));
        feed.observe(key(1), &pr(PrState::Merged, false, Some("a")));
        assert!(feed.take(&key(1)));
        assert!(!feed.is_changed(&key(1)));
        feed.restore(key(1));
        assert!(feed.is_changed(&key(1)));
        // A successful poll records the new state; the same news is then old.
        feed.take(&key(1));
        feed.record_poll(key(1), &pr(PrState::Merged, false, Some("a")));
        assert!(!feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
    }

    #[test]
    fn a_single_monitor_read_seeds_but_never_advances_the_baseline() {
        let mut feed = ObservationFeed::default();
        assert!(!feed.seed_or_observe(key(1), &pr(PrState::Open, false, Some("a"))));
        assert!(!feed.is_changed(&key(1)));
        // Seeded: a later merge seen elsewhere marks.
        assert!(feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
        feed.take(&key(1));
        // A check-now that reads the merge marks for the siblings and leaves
        // the baseline at open, so the next fresh read still disagrees.
        assert!(feed.seed_or_observe(key(1), &pr(PrState::Merged, false, Some("a"))));
        feed.take(&key(1));
        assert!(feed.observe(key(1), &pr(PrState::Merged, false, Some("a"))));
    }

    #[test]
    fn retain_drops_prs_no_longer_monitored() {
        let mut feed = ObservationFeed::default();
        feed.record_poll(key(1), &pr(PrState::Open, false, None));
        feed.record_poll(key(2), &pr(PrState::Open, false, None));
        feed.observe(key(1), &pr(PrState::Merged, false, None));
        feed.retain(&HashSet::from([key(2)]));
        assert!(!feed.is_changed(&key(1)));
        assert!(!feed.observe(key(1), &pr(PrState::Closed, false, None)));
        feed.restore(key(1));
        assert!(!feed.is_changed(&key(1)));
    }
}
