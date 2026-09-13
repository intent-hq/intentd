//! Centralized PR monitoring (`ws.pr.monitor`). An agent registers a watch on
//! one pull request and the daemon polls it from a SINGLE loop, diffing each
//! refreshed merge-requirements checklist against the persisted EMIT baseline
//! — the PR state as of the last delivered wake (or registration).
//!
//! The pending set (`pendingChanges`, so the UI can surface "changes awaiting
//! emit") is RECOMPUTED each poll as `diff(baseline, fresh)` — a coalesced
//! net diff, never an accumulated log: a field that reverts to its baseline
//! value drops out of the set (a full revert goes silent, no wake at all)
//! and A→B→C renders as a single A→C line. The set is delivered as ONE
//! consolidated wake once the PR has been quiet for the debounce window —
//! via the automatic-delivery `agent.sendMessage` path, so a wake queues
//! behind an in-flight turn and never interrupts. A merged/closed PR is
//! terminal: monitoring stops with an IMMEDIATE (undebounced) final wake and
//! the row is retained in `completed` state so merged PRs stay visible.
//! Cancellation (`cancelled`) is excluded from list surfaces; a user/FE
//! cancel notifies the owning agent, an agent's own cancel does not.
//!
//! Monitors persist to the `pr_monitor` table and rehydrate at boot
//! ([`Services::rehydrate_pr_monitors`]): every resumed monitor polls
//! promptly, and anything that changed while the daemon was down — including
//! a pending emit that was persisted but never delivered — fires immediately,
//! without debounce. Debounce applies again from the next change onward.
//!
//! The loop's forge request rate is planned against
//! `prMonitor.hourlyRequestBudget` rather than growing with the number of
//! monitored PRs: the loop keeps ticking every `prMonitor.pollSeconds`, but
//! each distinct PR is revisited on the **effective** interval
//! ([`effective_pr_monitor_interval_secs`]) and each tick fetches at most a
//! capped, oldest-first subset of the due PRs
//! ([`pr_monitor_fetches_per_tick`]), so a large monitor set is spread
//! across ticks instead of burst-fetched. The budget is a **cost model**
//! that derives the cadence (each PR poll costed at
//! [`PR_MONITOR_REQUESTS_PER_POLL`] REST calls), not a hard ceiling: no
//! request is counted or blocked against it, and actual spend can differ
//! (the 3-call unit is a single-page estimate: a multi-page review list or
//! a degraded-path REST fallback costs more; GraphQL reads ride their own
//! quota). Ahead of genuine exhaustion, each due-sweep tick also spends one
//! quota-free `rate_limit` probe and stretches the interval further when
//! the projected spend to the window's reset would exceed
//! `prMonitor.quotaSharePercent` of the REMAINING quota
//! ([`plan_quota_cadence`]) — and defers every poll until the window
//! resets once that share cannot pay for a single fetch. The quota is
//! shared with agents' own `gh` use and the PR-refresh sweep, so the
//! monitor slows down before the shared rate-limit gate has to stop it; a
//! host without the signal plans on the hourly budget alone. Genuine
//! quota exhaustion is handled by the shared rate-limit gate instead. The
//! debounce window is evaluated at that effective cadence, so a wake may
//! arrive up to one effective interval late.
//!
//! Actual spend on a quiet PR is lower than the cost model: every sweep
//! poll issues `get_pr`, but the sub-reads (merge-requirements probe,
//! reviews, review threads, conversation comments) are skipped while the
//! PR's change fingerprint — `updatedAt`, head SHA, lifecycle/draft flags,
//! mergeability — is unchanged since the last full fetch, bounded by
//! [`PR_MONITOR_MAX_CHEAP_POLLS`] and [`PR_MONITOR_MAX_CHEAP_AGE`] so
//! signals the fingerprint does not cover (check runs, merge-queue events)
//! are still re-read regularly ([`PrMonitorFetchCache`]). A forge that
//! reports no `updatedAt` gets no cheap polls at all: without it the
//! fingerprint is blind to the comment / review / thread movement the
//! monitor exists to report.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::events::{
    PR_MONITOR_CANCELLED, PR_MONITOR_CHANGED, PR_MONITOR_COMPLETED, PR_MONITOR_EMITTED,
    PR_MONITOR_REGISTERED,
};
use intent_core::{
    now_iso, parse_iso, AgentId, AgentStatus, Error, PrMonitor, PrMonitorId, PrMonitorState,
    PullRequestInfo, PullRequestStatus, Result, WorkspaceId,
};
use intent_sourcecontrol::{
    PrObservation, PrState, PullRequest, RateLimitStatus, RepoRef, SourceControl,
};
use intent_store::{NewEvent, PrMonitorListEntry, PrMonitorPollUpdate};
use serde_json::{json, Value};

use crate::pr_ops::{self, MergeRequirements};
use crate::rate_limit::RATE_LIMIT_MAX_PAUSE;
use crate::workspace_status::MonitorPrSignals;
use crate::{publish_event, system_actor, Services};

use intent_core::config::{
    MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET, MAX_PR_MONITOR_QUOTA_SHARE_PERCENT,
    MIN_PR_MONITOR_DEBOUNCE_SECONDS, MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET,
    MIN_PR_MONITOR_POLL_SECONDS, MIN_PR_MONITOR_QUOTA_SHARE_PERCENT,
};

/// Cap on concurrently ACTIVE monitors per agent (mirrors the background-hook
/// `maxPerAgent` convention).
pub(crate) const DEFAULT_PR_MONITORS_MAX_PER_AGENT: u32 = 5;

/// Forge REST calls one distinct-PR poll is COSTED at on the steady-state
/// GitHub path: the PR read, the reviews list, and the conversation-comment
/// list — a single-page estimate. The merge-requirements probe, review
/// decision, and review threads ride GraphQL (its own quota); the REST
/// check-runs read is a fallback taken only when the probe fails. This is
/// the unit of the `prMonitor.hourlyRequestBudget` cost model — a planning
/// constant for the cadence math, not a measured or enforced per-poll spend
/// (a PR whose review list spans several REST pages, or a degraded poll
/// taking a REST fallback, issues more calls than this; nothing counts them
/// against the budget).
pub(crate) const PR_MONITOR_REQUESTS_PER_POLL: u64 = 3;

/// The effective per-PR poll interval (seconds) for `distinct_prs` monitored
/// PRs: the configured `poll_secs`, stretched so that revisiting every PR
/// at that interval is MODELLED to spend at most `hourly_budget` REST calls
/// per hour — `max(poll_secs, ceil(distinct_prs × PR_MONITOR_REQUESTS_PER_POLL
/// × 3600 / hourly_budget))`. Both inputs are clamped to their floors first
/// (the budget's ceiling is applied by the settings getter). At the defaults
/// (30s, 1500/h) up to 4 PRs keep the configured 30s; 10 PRs → 72s, 20 PRs
/// → 144s. The budget is a cost model that sets the cadence; it is not a
/// limiter that counts or blocks requests.
pub(crate) fn effective_pr_monitor_interval_secs(
    distinct_prs: usize,
    poll_secs: u64,
    hourly_budget: u64,
) -> u64 {
    let poll_secs = poll_secs.max(MIN_PR_MONITOR_POLL_SECONDS);
    let hourly_budget = hourly_budget.max(MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET);
    let needed = (distinct_prs as u64)
        .saturating_mul(PR_MONITOR_REQUESTS_PER_POLL)
        .saturating_mul(3600)
        .div_ceil(hourly_budget);
    poll_secs.max(needed)
}

/// The forge's remaining quota as read by the tick's quota-free probe,
/// reduced to what the cadence math needs: the requests left in the window
/// and how long the window still runs. Absent whenever the probe failed or
/// the host lacks either signal, in which case the cadence falls back to
/// the hourly-budget model alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaWindow {
    /// Requests left in the current window.
    pub(crate) remaining: u64,
    /// Seconds until the window resets, clamped into
    /// `[1, RATE_LIMIT_MAX_PAUSE]` — a reset already in the past (the
    /// window turned over between the forge's stamp and our read) is not a
    /// usable projection horizon and reads as absent.
    pub(crate) reset_in_secs: u64,
}

impl QuotaWindow {
    /// Reduce a probe result to a window, given the current unix time.
    pub(crate) fn from_status(status: &RateLimitStatus, now_unix: u64) -> Option<Self> {
        let remaining = status.remaining?;
        let reset_in_secs = status.reset_at?.saturating_sub(now_unix);
        (reset_in_secs > 0).then_some(Self {
            remaining,
            reset_in_secs: reset_in_secs.min(RATE_LIMIT_MAX_PAUSE.as_secs()),
        })
    }
}

/// What the remaining forge quota lets one due-sweep tick plan
/// ([`plan_quota_cadence`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaCadence {
    /// Poll each PR no more often than this many seconds (tick-aligned).
    Interval(u64),
    /// The allowed share cannot pay for a single fetch: nothing is due —
    /// however stale, catch-up-marked or never-polled — until the window
    /// resets, `reset_in_secs` from this tick's probe.
    DeferUntilReset { reset_in_secs: u64 },
}

/// The cadence the monitor loop last logged, so a change (interval to
/// interval, interval to deferral and back) is logged once — never per tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoggedCadence {
    /// The effective per-PR poll interval, in seconds.
    Interval(u64),
    /// Polling deferred until the quota window resets.
    Deferred,
}

/// Plan the per-PR cadence that keeps the PROJECTED spend until the window
/// resets — `distinct_prs × PR_MONITOR_REQUESTS_PER_POLL × reset_in_secs /
/// interval` — within `share_percent` of the remaining quota (the share is
/// clamped into its catalog range first). With `allowed = remaining ×
/// share / 100` requests:
///
/// - `allowed < PR_MONITOR_REQUESTS_PER_POLL` — the share cannot cover even
///   one fetch: [`QuotaCadence::DeferUntilReset`]. A deferral is measured
///   from NOW (this tick's probe), never from a monitor's `lastPolledAt`:
///   an interval anchored on a stale stamp would fall due inside the
///   window as the horizon shrinks, and would not bind catch-up rows at
///   all.
/// - otherwise [`QuotaCadence::Interval`]`(ceil(distinct_prs × 3 ×
///   reset_in_secs / allowed))`, rounded UP to a whole number of
///   `poll_secs` ticks (the revisit lands on a tick boundary anyway, and
///   tick alignment keeps the once-per-change cadence INFO from re-logging
///   every tick as `remaining` drifts).
///
/// The plan is monotone non-increasing in `remaining`: less quota never
/// polls sooner (a deferral counts as slower than any interval). The
/// `pollSeconds` floor is the caller's: it takes the max with the
/// hourly-budget cadence, which is never below the configured cadence. A
/// `None` window (probe failed, host without the signal, reset already
/// passed) or no monitored PR plans nothing — the caller keeps today's
/// hourly-budget formula.
pub(crate) fn plan_quota_cadence(
    distinct_prs: usize,
    poll_secs: u64,
    window: Option<QuotaWindow>,
    share_percent: u64,
) -> Option<QuotaCadence> {
    let window = window?;
    if distinct_prs == 0 {
        return None;
    }
    let poll_secs = poll_secs.max(MIN_PR_MONITOR_POLL_SECONDS);
    let share_percent = share_percent.clamp(
        MIN_PR_MONITOR_QUOTA_SHARE_PERCENT,
        MAX_PR_MONITOR_QUOTA_SHARE_PERCENT,
    );
    let allowed = window.remaining.saturating_mul(share_percent) / 100;
    if allowed < PR_MONITOR_REQUESTS_PER_POLL {
        return Some(QuotaCadence::DeferUntilReset {
            reset_in_secs: window.reset_in_secs,
        });
    }
    let needed = (distinct_prs as u64)
        .saturating_mul(PR_MONITOR_REQUESTS_PER_POLL)
        .saturating_mul(window.reset_in_secs)
        .div_ceil(allowed);
    Some(QuotaCadence::Interval(
        needed.div_ceil(poll_secs).saturating_mul(poll_secs),
    ))
}

/// How many distinct PRs one due-sweep tick may fetch so that `distinct_prs`
/// PRs each get revisited about once per `effective_secs` while the loop
/// ticks every `poll_secs`: `ceil(distinct_prs × poll_secs / effective_secs)`,
/// never below 1. Equals `distinct_prs` whenever the interval is not
/// stretched, so small monitor sets keep polling everything due each tick.
///
/// Once the interval spaces successive fetches further apart than one tick
/// (`effective_secs / distinct_prs > poll_secs`), the minimum of one would
/// let a stale backlog — a restart, a long outage, a quota-stretched
/// interval — drain one PR per tick and spend the whole planned interval's
/// worth of requests in a few minutes. So the tick fetches NOTHING while
/// the newest `lastPolledAt` across the active monitors
/// (`newest_anchor_age_secs`; `None` = nothing ever polled) is younger than
/// that spacing: the backlog drains one fetch per spacing, i.e. within the
/// budget the interval was planned against. The hold is measured from the
/// stamps, so it survives a restart and needs no spend counter.
pub(crate) fn pr_monitor_fetches_per_tick(
    distinct_prs: usize,
    poll_secs: u64,
    effective_secs: u64,
    newest_anchor_age_secs: Option<u64>,
) -> usize {
    let effective_secs = effective_secs.max(1);
    let spacing = effective_secs.div_ceil((distinct_prs as u64).max(1));
    if spacing > poll_secs && newest_anchor_age_secs.is_some_and(|age| age < spacing) {
        return 0;
    }
    (distinct_prs as u64)
        .saturating_mul(poll_secs)
        .div_ceil(effective_secs)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

/// The distinct `(owner, repo, pr)` identity a sweep dedupes fetches on.
/// Forge slugs are case-insensitive, so the key is the [`RepoRef`] identity
/// ([`RepoRef::identity_parts`], matching the store's `COLLATE NOCASE`
/// identity) and case-variant siblings share a fetch.
type PrKey = (String, String, i64);

fn pr_key(m: &PrMonitor) -> PrKey {
    pr_key_for(&m.repo(), m.pr_number)
}

fn pr_key_for(repo_ref: &RepoRef, pr_number: i64) -> PrKey {
    let (owner, name) = repo_ref.identity_parts();
    (owner, name, pr_number)
}

/// One active monitor as the due-sweep sees it: its staleness anchor (parsed
/// `lastPolledAt`; registration and re-registration fetch their own baseline
/// and stamp the field; `None` = never polled, sorts oldest) and whether its
/// restart catch-up marker is still unattempted.
#[derive(Clone)]
pub(crate) struct DueCandidate {
    pub(crate) anchor: Option<time::OffsetDateTime>,
    pub(crate) catch_up: bool,
    pub(crate) monitor: PrMonitor,
}

/// Whether a monitor's catch-up marker still exempts it from the freshness
/// check: marked by boot rehydration at `marked_at` and not yet ATTEMPTED
/// since (its `lastPolledAt` predates the mark). A failed post-restart
/// attempt stamps `lastPolledAt` past the mark, so the monitor rejoins the
/// normal cadence — the marker itself survives until a successful poll
/// consumes it, keeping the undebounced-delivery guarantee for that poll.
fn catch_up_unattempted(
    monitor: &PrMonitor,
    catch_up: &HashMap<PrMonitorId, time::OffsetDateTime>,
) -> bool {
    catch_up.get(&monitor.monitor_id).is_some_and(|marked_at| {
        monitor
            .last_polled_at
            .as_deref()
            .and_then(parse_iso)
            .is_none_or(|at| at <= *marked_at)
    })
}

/// Pick the monitors one due-sweep tick polls. Monitors are grouped per
/// distinct PR: a PR's anchor is the OLDEST anchor among its sibling
/// monitors and it is due when that anchor is older than `interval` (or
/// missing, or any sibling is catch-up-unattempted). Due PRs are ordered
/// oldest anchor first (PR key breaks ties), the first `cap` are kept, and
/// EVERY active monitor on a kept PR is returned in that order — siblings
/// share the one fetch and leave the tick with aligned `lastPolledAt`
/// stamps, so a PR is fetched once per interval however many agents watch
/// it and however staggered their registrations were.
pub(crate) fn select_due_pr_monitors(
    candidates: Vec<DueCandidate>,
    now: time::OffsetDateTime,
    interval: time::Duration,
    cap: usize,
) -> Vec<PrMonitor> {
    struct Group {
        anchor: Option<time::OffsetDateTime>,
        catch_up: bool,
        monitors: Vec<PrMonitor>,
    }
    let mut index: HashMap<PrKey, usize> = HashMap::new();
    let mut groups: Vec<(PrKey, Group)> = Vec::new();
    for c in candidates {
        let key = pr_key(&c.monitor);
        if let Some(&i) = index.get(&key) {
            let group = &mut groups[i].1;
            if c.anchor < group.anchor {
                group.anchor = c.anchor;
            }
            group.catch_up |= c.catch_up;
            group.monitors.push(c.monitor);
        } else {
            index.insert(key.clone(), groups.len());
            groups.push((
                key,
                Group {
                    anchor: c.anchor,
                    catch_up: c.catch_up,
                    monitors: vec![c.monitor],
                },
            ));
        }
    }
    let mut due: Vec<(Option<time::OffsetDateTime>, PrKey, Vec<PrMonitor>)> = groups
        .into_iter()
        .filter(|(_, g)| g.catch_up || g.anchor.is_none_or(|at| now - at >= interval))
        .map(|(key, g)| (g.anchor, key, g.monitors))
        .collect();
    due.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    due.into_iter()
        .take(cap)
        .flat_map(|(_, _, monitors)| monitors)
        .collect()
}

/// Upper bound on one shared `(repo, pr)` forge fetch within a sweep —
/// defense in depth above the client-level network timeouts, so a fetch
/// that pends indefinitely (e.g. a TCP connection that went dark) surfaces
/// as a recorded `lastError` on the affected monitors instead of wedging
/// the single serialized sweep loop for every monitor.
///
/// This is an *aggregate* budget over the whole multi-request snapshot fetch
/// (PR read, merge requirements, reviews, check runs, paged review threads /
/// comments), not a per-request bound — a legitimately slow forge or a very
/// large PR could exceed it without any dead connection, in which case the
/// monitor stays `active` with a visible "timed out" `lastError` each tick.
/// Accepted tradeoff: widen it (or make it per-request) only if dogfooding
/// shows repeated timeouts on healthy PRs.
///
/// Sweep-only by design: the registration path (`fetch_snapshot`) is caller-
/// scoped — a hang there blocks only the registering RPC, which the client-
/// level network timeouts already bound — so it is deliberately not wrapped
/// in this timeout.
pub(crate) const PR_MONITOR_FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Max-latency bound on the debounce hold, in debounce windows: a PR that
/// never goes quiet (a long CI matrix flipping one check at a time, an active
/// review conversation) still gets its consolidated wake once the OLDEST
/// pending change (`pendingSince`) has waited this many windows — standard
/// debounce-with-max-wait, so a wake can be late but never starved.
pub(crate) const PR_MONITOR_DEBOUNCE_MAX_WAIT_FACTOR: i32 = 5;

/// Monitors whose next poll must deliver WITHOUT waiting out the debounce
/// window, keyed to the instant they were marked: populated by boot
/// rehydration so a baseline that moved (or a pending emit that was persisted
/// but never delivered) while the daemon was down fires immediately. Shared
/// across [`Services`] clones; an entry is consumed by the first poll that
/// acts on it. The mark time lets the due-sweep exempt a monitor from the
/// freshness check only until its first post-restart ATTEMPT
/// ([`catch_up_unattempted`]), so a failing fetch cannot keep it perpetually
/// due and starve the rotation.
pub(crate) type PrMonitorCatchUp = Arc<Mutex<HashMap<PrMonitorId, time::OffsetDateTime>>>;

/// The diffable state of one monitored PR: the merge-requirements checklist
/// plus the identity/comment-count fields the checklist itself does not
/// carry. Persisted (JSON) as the monitor's baseline.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PrMonitorSnapshot {
    pub title: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    pub conversation_count: i64,
    pub review_comment_count: i64,
    pub requirements: MergeRequirements,
    /// Whether this snapshot's `mergeQueueEjection` field reflects an actual
    /// merge-requirements probe answer (directly, or inherited through the
    /// degraded-probe hold in [`SharedPrSnapshot::materialize`]).
    /// Serde-defaulted so a baseline persisted before ejection tracking
    /// existed reads as UNTRACKED: any event the first tracked poll sees is
    /// history relative to such a baseline, not news, and the poll adopts it
    /// silently instead of emitting a false post-upgrade wake.
    #[serde(default)]
    pub ejection_tracked: bool,
    /// When the forge read that produced this snapshot SUCCEEDED (RFC 3339).
    /// The freshness anchor for superseding the snapshot with a workspace
    /// copy ([`superseded_by_terminal_copy`]): the row's `last_polled_at`
    /// also advances on failed polls, which do not re-observe the PR.
    /// Absent on snapshots persisted before the field existed (unknown
    /// freshness: such a snapshot never holds a terminal copy off); never
    /// participates in the change diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,
}

impl PrMonitorSnapshot {
    /// Whether the PR reached a terminal lifecycle (merged or closed) — the
    /// monitor's automatic-stop condition.
    fn is_terminal(&self) -> bool {
        matches!(self.requirements.state.as_str(), "merged" | "closed")
    }
}

/// One PR's freshly fetched state, shared by every monitor watching that
/// `(repo, pr)` within a single sweep. `conversation_count` is `None` when
/// the comment read degraded, so each monitor substitutes ITS OWN previous
/// count at materialization time rather than fabricating a "comments
/// removed" change from a sibling monitor's baseline.
#[derive(Debug, Clone)]
pub(crate) struct SharedPrSnapshot {
    title: String,
    url: String,
    head_sha: Option<String>,
    conversation_count: Option<i64>,
    review_comment_count: i64,
    requirements: MergeRequirements,
    /// Whether the merge-requirements probe answered this poll. The probe is
    /// the only source of `mergeQueueEjection`, so `false` means that field
    /// is "unknown", not "no ejection" (see [`Self::materialize`]).
    ejection_known: bool,
    /// Whether EVERY checklist sub-read answered
    /// ([`pr_ops::MergeRequirementsRead::complete`]): a degraded review,
    /// review-decision, check-run or review-thread read leaves a default in
    /// the checklist that the forge may answer on the next read.
    requirements_complete: bool,
}

impl SharedPrSnapshot {
    /// Materialize a per-monitor snapshot: a degraded conversation-comment
    /// read keeps the monitor's previous count rather than fabricating a
    /// "comments removed" change, and a degraded merge-requirements probe
    /// keeps the monitor's previously observed merge-queue ejection event
    /// rather than silently dropping it from the pending set (the event is
    /// monotonic — always reaches the wake — per intent-hq/monorepo#3479).
    pub(crate) fn materialize(&self, previous: Option<&PrMonitorSnapshot>) -> PrMonitorSnapshot {
        let mut requirements = self.requirements.clone();
        let ejection_tracked = if self.ejection_known {
            true
        } else {
            if requirements.merge_queue_ejection.is_none() {
                requirements.merge_queue_ejection =
                    previous.and_then(|p| p.requirements.merge_queue_ejection.clone());
            }
            // The hold carries the observation forward, so tracked-ness
            // carries with it; an untracked previous stays untracked until
            // a probe actually answers.
            previous.is_some_and(|p| p.ejection_tracked)
        };
        PrMonitorSnapshot {
            title: self.title.clone(),
            url: self.url.clone(),
            head_sha: self.head_sha.clone(),
            conversation_count: self
                .conversation_count
                .unwrap_or_else(|| previous.map_or(0, |p| p.conversation_count)),
            review_comment_count: self.review_comment_count,
            requirements,
            ejection_tracked,
            observed_at: None,
        }
    }
}

impl SharedPrSnapshot {
    /// Whether every read behind this snapshot answered — the precondition
    /// for a later poll to REUSE it instead of re-fetching: a degraded
    /// comment count, merge-requirements probe or any other checklist
    /// sub-read (reviews, review decision, check runs, review threads) must
    /// be retried on the next poll, not carried forward for as long as the
    /// PR stays quiet.
    fn is_complete(&self) -> bool {
        self.conversation_count.is_some() && self.ejection_known && self.requirements_complete
    }
}

/// The fields of the load-bearing `get_pr` read that move whenever the PR
/// changes in a way the monitor reports on: `updatedAt` (bumped by the forge
/// on every review, comment, thread, label, title, or push), the head SHA,
/// the lifecycle/draft flags and the forge's mergeability verdict. A poll
/// whose fingerprint equals the previous FULL fetch's reuses that fetch's
/// sub-reads ([`fetch_shared_snapshot_cached`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrFingerprint {
    updated_at: String,
    head_sha: Option<String>,
    state: PrState,
    draft: bool,
    mergeable: Option<bool>,
    mergeable_state: Option<String>,
}

impl PrFingerprint {
    fn of(pr: &PullRequest) -> Self {
        Self {
            updated_at: pr.updated_at.clone(),
            head_sha: pr.head_sha.clone(),
            state: pr.state,
            draft: pr.draft,
            mergeable: pr.mergeable,
            mergeable_state: pr.mergeable_state.clone(),
        }
    }

    /// Whether this fingerprint can stand in for the sub-reads at all. The
    /// head SHA, lifecycle flags and mergeability verdict do not move on a
    /// comment, review or thread — only `updatedAt` does — so a forge that
    /// does not report `updatedAt` leaves the fingerprint blind to exactly
    /// the movement the monitor reports on, and no poll may be cheap for it.
    fn detects_changes(&self) -> bool {
        !self.updated_at.is_empty()
    }
}

/// Consecutive fingerprint-unchanged polls that may reuse one full fetch
/// before the next poll re-fetches everything regardless. Check runs and
/// merge-queue events do not bump the PR's `updatedAt` on GitHub, so the
/// bound is what keeps those signals from going stale on a quiet PR.
pub(crate) const PR_MONITOR_MAX_CHEAP_POLLS: u32 = 5;

/// Age past which a full fetch is never reused, whatever the poll count —
/// so a long rate-limit pause or a stretched effective interval cannot
/// combine with the poll-count bound into an arbitrarily old checklist.
pub(crate) const PR_MONITOR_MAX_CHEAP_AGE: Duration = Duration::from_secs(15 * 60);

/// One PR's last FULL sweep fetch, remembered between sweeps.
#[derive(Debug, Clone)]
pub(crate) struct PrMonitorFetchCacheEntry {
    fingerprint: PrFingerprint,
    snapshot: SharedPrSnapshot,
    fetched_at: Instant,
    /// Polls that reused `snapshot` since `fetched_at`.
    cheap_polls: u32,
}

impl PrMonitorFetchCacheEntry {
    /// Whether a poll that just read `fingerprint` at `now` may reuse this
    /// entry's sub-fetches: a fingerprint that can detect changes at all
    /// ([`PrFingerprint::detects_changes`]) and is unchanged, a complete
    /// snapshot, and both the poll-count and age bounds still open.
    fn reusable(&self, fingerprint: &PrFingerprint, now: Instant) -> bool {
        fingerprint.detects_changes()
            && self.fingerprint == *fingerprint
            && self.snapshot.is_complete()
            && self.cheap_polls < PR_MONITOR_MAX_CHEAP_POLLS
            && now.saturating_duration_since(self.fetched_at) < PR_MONITOR_MAX_CHEAP_AGE
    }
}

/// One PR's slot in the sweep's fetch cache: the last full sweep fetch (if
/// any is held) plus an invalidation generation bumped by every on-demand
/// full fetch of the PR (registration, re-registration, check-now). A sweep
/// fetch records the generation before its `get_pr` and only stores its
/// result if the generation is unchanged when it finishes, so an on-demand
/// fetch that completed mid-sweep — and persisted a NEWER snapshot — can
/// never be shadowed by the sweep's older result on later polls.
#[derive(Debug, Clone, Default)]
pub(crate) struct PrMonitorFetchCacheSlot {
    generation: u64,
    entry: Option<PrMonitorFetchCacheEntry>,
}

/// The sweep's per-PR memory of its last full fetch, keyed like the
/// in-sweep dedupe ([`PrKey`]). Consulted ONLY by the sweep: registration,
/// re-registration and the explicit check-now path always fetch fully and
/// then INVALIDATE the PR's slot ([`invalidate_fetch_cache`]), so the sweep
/// after an on-demand fetch fetches fully too rather than reusing a
/// checklist older than the snapshot that fetch persisted. Slots for PRs no
/// longer under any active monitor are pruned at the top of each sweep.
/// In-memory only — a daemon restart starts with a full fetch per PR.
/// Shared across [`Services`] clones.
pub(crate) type PrMonitorFetchCache = Arc<Mutex<HashMap<PrKey, PrMonitorFetchCacheSlot>>>;

/// Drop any cached sweep fetch for `key` and bump its generation, so a
/// sweep fetch already in flight for the PR does not repopulate the slot
/// with its (possibly older) result. Called by every on-demand full fetch
/// once that fetch has completed.
fn invalidate_fetch_cache(cache: &PrMonitorFetchCache, key: &PrKey) {
    let mut cache = cache.lock().unwrap();
    let slot = cache.entry(key.clone()).or_default();
    slot.generation += 1;
    slot.entry = None;
}

/// Fetch the current shared state of one PR: the merge-requirements
/// checklist (which already degrades per-signal) plus the
/// conversation-comment count (`None` when that read fails). Quota
/// exhaustion is the one non-degrading failure: [`Error::RateLimited`] from
/// ANY forge read — the load-bearing `get_pr`, a checklist sub-read, or the
/// comment count — propagates so the sweep pauses the shared gate instead of
/// persisting a degraded snapshot as a successful poll.
///
/// Forge requests per fetch, measured with the stub forge (trait-level
/// reads; the GitHub HTTP count in parentheses where it differs):
///
/// | path | before | after |
/// |---|---|---|
/// | happy (host folds the read) | 6 — `get_pr`, `merge_requirements` (GraphQL + branch-rules REST = 2 HTTP), `list_reviews`, `review_decision`, `get_review_threads`, `list_comments` (7 HTTP) | 2 — `pr_observation` (1 GraphQL request, 1 rate-limit point), `branch_rules` |
/// | host without a folded read | 6 (7 HTTP) | 6 (7 HTTP), unchanged |
/// | REST fallback (probe + threads down), host without a folded read | 8 | 8, unchanged |
/// | REST fallback, host with a folded read whose GraphQL is down | 8 | 9 — the failed `pr_observation` attempt, then the 8 |
/// | cached cheap poll (fingerprint unchanged) | 1 (`get_pr`) | 1 (`pr_observation`) |
pub(crate) async fn fetch_shared_snapshot(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
) -> Result<SharedPrSnapshot> {
    if let Some(observation) = observe_pr(sc, repo_ref, number).await? {
        return shared_snapshot_from_observation(sc, repo_ref, number, observation).await;
    }
    let (pr, read) = pr_ops::fetch_merge_requirements_detailed(sc, repo_ref, number).await?;
    finish_shared_snapshot(sc, repo_ref, number, pr, read).await
}

/// The host's folded one-round-trip read
/// ([`SourceControl::pr_observation`]), or `None` when the host has none —
/// the per-signal reads are then issued instead. A failing folded read
/// (other than quota exhaustion, which propagates) also yields `None`: the
/// per-signal path's load-bearing `get_pr` then decides whether the forge
/// is reachable, so the observation never changes WHICH error a poll fails
/// with, only how many requests a successful one costs.
async fn observe_pr(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
) -> Result<Option<PrObservation>> {
    match sc.pr_observation(repo_ref, number).await {
        Ok(observation) => Ok(observation),
        Err(intent_sourcecontrol::Error::RateLimited(detail)) => Err(Error::RateLimited(detail)),
        Err(e) => {
            tracing::debug!(
                error = %e,
                pr_number = number,
                "pr monitor: folded PR observation failed, falling back to per-signal reads"
            );
            Ok(None)
        }
    }
}

/// [`finish_shared_snapshot`] for a folded observation: the checklist is
/// composed from it (see [`pr_ops::merge_requirements_from_observation`])
/// and the conversation-comment count it carries needs no further read.
async fn shared_snapshot_from_observation(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
    observation: PrObservation,
) -> Result<SharedPrSnapshot> {
    let read =
        pr_ops::merge_requirements_from_observation(sc, repo_ref, number, &observation).await?;
    Ok(SharedPrSnapshot {
        title: observation.pr.title,
        url: observation.pr.url,
        head_sha: observation.pr.head_sha,
        conversation_count: Some(observation.conversation_count),
        review_comment_count: read.review_comment_count,
        requirements: read.requirements,
        ejection_known: read.ejection_known,
        requirements_complete: read.complete,
    })
}

/// [`fetch_shared_snapshot`] for a PR the sweep has already read: the PR
/// record is always read (it is the change detector — the folded
/// observation where the host has one, else `get_pr`), but the remaining
/// reads — branch rules after a folded observation; merge-requirements
/// probe, reviews, review threads, conversation comments after a `get_pr`
/// — are skipped when `cache` holds a reusable full fetch for `key` with
/// the same [`PrFingerprint`] (see [`PrMonitorFetchCacheEntry::reusable`]).
/// A full fetch replaces the cache entry unless an on-demand fetch
/// invalidated the slot meanwhile (see [`PrMonitorFetchCacheSlot`]); a
/// failed one leaves it untouched (the next poll decides again from a fresh
/// read).
pub(crate) async fn fetch_shared_snapshot_cached(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
    cache: &PrMonitorFetchCache,
    key: &PrKey,
) -> Result<SharedPrSnapshot> {
    let generation = cache
        .lock()
        .unwrap()
        .get(key)
        .map_or(0, |slot| slot.generation);
    let observation = observe_pr(sc, repo_ref, number).await?;
    let pr = match &observation {
        Some(observation) => observation.pr.clone(),
        None => sc
            .get_pr(repo_ref, number)
            .await
            .map_err(pr_ops::map_sc_err)?,
    };
    let fingerprint = PrFingerprint::of(&pr);
    let now = Instant::now();
    let reused = {
        let mut cache = cache.lock().unwrap();
        match cache.get_mut(key).and_then(|slot| slot.entry.as_mut()) {
            Some(entry) if entry.reusable(&fingerprint, now) => {
                entry.cheap_polls += 1;
                Some(entry.snapshot.clone())
            }
            _ => None,
        }
    };
    if let Some(snapshot) = reused {
        tracing::trace!(
            pr_number = number,
            "pr monitor: PR fingerprint unchanged; reusing previous full fetch"
        );
        return Ok(snapshot);
    }
    let snapshot = match observation {
        Some(observation) => {
            shared_snapshot_from_observation(sc, repo_ref, number, observation).await?
        }
        None => fetch_shared_snapshot_for(sc, repo_ref, number, pr).await?,
    };
    if fingerprint.detects_changes() {
        let mut cache = cache.lock().unwrap();
        let slot = cache.entry(key.clone()).or_default();
        if slot.generation == generation {
            slot.entry = Some(PrMonitorFetchCacheEntry {
                fingerprint,
                snapshot: snapshot.clone(),
                fetched_at: now,
                cheap_polls: 0,
            });
        } else {
            tracing::trace!(
                pr_number = number,
                "pr monitor: on-demand fetch superseded this sweep fetch; not caching it"
            );
        }
    }
    Ok(snapshot)
}

/// The sub-reads of [`fetch_shared_snapshot`] for an already-read `pr`.
async fn fetch_shared_snapshot_for(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
    pr: PullRequest,
) -> Result<SharedPrSnapshot> {
    let read = pr_ops::merge_requirements_for_pr_detailed(sc, repo_ref, number, &pr).await?;
    finish_shared_snapshot(sc, repo_ref, number, pr, read).await
}

/// The conversation-comment read that completes a [`SharedPrSnapshot`]
/// once the checklist is composed.
async fn finish_shared_snapshot(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
    pr: PullRequest,
    read: pr_ops::MergeRequirementsRead,
) -> Result<SharedPrSnapshot> {
    let conversation_count = match sc.list_comments(repo_ref, number).await {
        Ok(comments) => Some(i64::try_from(comments.len()).expect("value fits in i64")),
        Err(intent_sourcecontrol::Error::RateLimited(detail)) => {
            return Err(Error::RateLimited(detail));
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                pr_number = number,
                "pr monitor: conversation comments unavailable, keeping previous count"
            );
            None
        }
    };
    Ok(SharedPrSnapshot {
        title: pr.title,
        url: pr.url,
        head_sha: pr.head_sha,
        conversation_count,
        review_comment_count: read.review_comment_count,
        requirements: read.requirements,
        ejection_known: read.ejection_known,
        requirements_complete: read.complete,
    })
}

/// Fetch + materialize in one step — the registration path, where exactly
/// one monitor consumes the read.
pub(crate) async fn fetch_snapshot(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
    previous: Option<&PrMonitorSnapshot>,
) -> Result<PrMonitorSnapshot> {
    Ok(fetch_shared_snapshot(sc, repo_ref, number)
        .await?
        .materialize(previous))
}

/// One human-readable line per detected change between two snapshots, in a
/// stable order (lifecycle → review → comments → checks → mergeability). An
/// empty result means "nothing moved" and the monitor stays quiet.
///
/// Per-check success transitions are NOT reported individually (see
/// [`diff_checks`]); instead, the moment the suite finishes — the old
/// snapshot still had pending checks and the new one has none — ONE
/// aggregate completion line summarizes the outcome. A poll whose only
/// movement is intermediate successes therefore produces an empty diff.
///
/// Per-check `required` flags only participate when BOTH snapshots report
/// `requiredKnown` — a degraded probe flips every flag to `false`, and
/// reporting that as "no longer required" would be a lie about the branch
/// rules rather than an observation about the PR.
pub(crate) fn diff_snapshots(old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String> {
    crate::harness::latest().pr_diff_lines(old, new)
}

/// Whether a row's persisted pending set is what the coalescing poll would
/// recompute anyway — `diff(baseline, last_snapshot)`. A set that does NOT
/// survive is a legacy accumulated log (pre-coalescing rows whose upgrade
/// migration backfilled `baseline_snapshot = last_snapshot`, making their
/// recomputed diff empty): boot rehydration delivers those as-is instead of
/// letting the first poll silently discard them.
fn pending_survives_recompute(m: &PrMonitor) -> bool {
    let parse = |s: &Option<String>| -> Option<PrMonitorSnapshot> {
        s.as_deref().and_then(|s| serde_json::from_str(s).ok())
    };
    let (Some(baseline), Some(last)) = (parse(&m.baseline_snapshot), parse(&m.last_snapshot))
    else {
        // No baseline/snapshot to recompute from: the poll preserves the
        // set until it can, so nothing is at risk.
        return true;
    };
    diff_snapshots(&baseline, &last) == m.pending_changes
}

/// The `<owner>/<name>#<number>` label every wake and event payload uses.
/// Wording owned by the harness (H6).
pub(crate) fn monitor_label(m: &PrMonitor) -> String {
    crate::harness::latest().pr_monitor_label(&m.repo_owner, &m.repo_name, m.pr_number)
}

/// Outcome of [`Services::pr_monitor_try_register`]: the monitor was
/// registered (re-armed, or adopted from a dead owner), or the call was
/// refused because another LIVE agent in the workspace already holds the
/// PR's active monitor.
#[derive(Debug, Clone)]
pub enum PrMonitorRegistration {
    /// The caller now owns an active monitor on the PR.
    Registered {
        monitor: Box<PrMonitor>,
        requirements: MergeRequirements,
        /// `Some(previous owner)` when the row was ADOPTED from an agent
        /// that can no longer receive wakes (intent-hq/intent#5079);
        /// `None` for a fresh registration or the owner's own re-arm.
        adopted_from: Option<AgentId>,
    },
    /// Refused: one monitor per PR per workspace, and another agent holds it.
    Refused(PrMonitorRefusal),
}

/// Who holds the workspace's ACTIVE monitor on a PR when it is not the
/// caller: a live agent (the call is refused), an agent that can no longer
/// receive wakes — terminal status, soft-retired, or its session row gone —
/// whose monitor is orphaned and adoptable (intent-hq/intent#5079), or a
/// live DIRECT sub-agent of the caller that has settled (its task is
/// `complete`/`cancelled`, or it sits `RuntimeIdle` with no waiting reason
/// other than PR monitors) whose monitor the parent may take over — the
/// child is still woken with a transfer notice.
enum PrMonitorHolder {
    Live(PrMonitorRefusal),
    Orphaned(PrMonitor),
    SettledChild(PrMonitor),
}

/// A refused `pr.monitor` registration — the ACTIVE monitor another agent in
/// the same workspace already holds on the PR, plus that owner's session
/// name when it has one. `child_of_caller` marks an owner that is the
/// caller's own direct sub-agent, still mid-work: the refusal instruction
/// then names the settlement conditions under which a retry adopts.
#[derive(Debug, Clone)]
pub struct PrMonitorRefusal {
    pub owner: PrMonitor,
    pub owner_agent_name: Option<String>,
    pub child_of_caller: bool,
}

impl PrMonitorRefusal {
    /// The structured `ws.pr.monitor` refusal payload: `ok: false` with
    /// `refused: true` and `reason: "already-monitored"`, naming the owner
    /// (`ownerAgentId`, `ownerAgentName` when known, `monitorId`) and
    /// carrying an `instruction` telling the model how to proceed.
    #[must_use]
    pub fn to_wire(&self) -> Value {
        let label = monitor_label(&self.owner);
        let owner_id = self.owner.agent_id.to_string();
        let owner_display = match &self.owner_agent_name {
            Some(name) => format!("{name} ({owner_id})"),
            None => owner_id.clone(),
        };
        let instruction = if self.child_of_caller {
            format!(
                "{label} is already monitored in this workspace by your sub-agent \
                 {owner_display}, which is still working; one monitor per PR per \
                 workspace. A working sub-agent keeps its monitor and receives the \
                 PR's wakes; do not register a second one. Retry ws.pr.monitor once \
                 the sub-agent settles — its task is complete or cancelled, or it is \
                 idle with nothing pending but this monitor (a ws.agent.watch on it \
                 delivers that as its monitoring-idle advisory): the retry adopts the \
                 monitor instead of being refused and the sub-agent is notified of the \
                 transfer. For a one-shot read of the PR's current state use \
                 ws.pr.snapshot. Only if you need the monitor now, use ws.agent.send \
                 to ask the sub-agent to relay the events you care about or, as a last \
                 resort, to relinquish the monitor via ws.pr.unmonitor so you can \
                 register your own."
            )
        } else {
            format!(
                "{label} is already monitored in this workspace by agent {owner_display}; \
                 one monitor per PR per workspace. That agent receives the PR's wakes. \
                 Instead of registering a second monitor, use ws.agent.send to ask the \
                 owner either to relay the events you care about to you, or to relinquish \
                 the monitor via ws.pr.unmonitor so you can register your own; for a \
                 one-shot read of the PR's current state use ws.pr.snapshot. Retry \
                 ws.pr.monitor only after the owner cancels its monitor or finishes."
            )
        };
        let mut payload = json!({
            "ok": false,
            "refused": true,
            "reason": "already-monitored",
            "repo": format!("{}/{}", self.owner.repo_owner, self.owner.repo_name),
            "prNumber": self.owner.pr_number,
            "ownerAgentId": owner_id,
            "monitorId": self.owner.monitor_id,
            "instruction": instruction,
        });
        if let Some(name) = &self.owner_agent_name {
            payload["ownerAgentName"] = json!(name);
        }
        payload
    }
}

/// Whether a snapshot's merge-requirements checklist reads as truly
/// mergeable — the `ready` signal gate. GitHub's `mergeable` flag alone
/// only means "no merge conflicts": a PR blocked by required checks,
/// missing reviews, or branch protection still reports `mergeable: true`,
/// so every checklist blocker must be clear — no failing/pending required
/// checks, no changes-requested/review-required approvals decision (nor a
/// `none` decision while the branch rules still demand approvals the PR
/// does not have), no unresolved threads when resolution is required (an
/// unreadable thread count — `threads.unresolved == None` — never promotes
/// while resolution is required, since the state is unknown, not clear), no
/// `merge_blocked_reason`, and no blocked/behind/dirty/unknown
/// `merge_state_status` (`UNKNOWN` means the forge has not established
/// mergeability yet, so it never promotes). A PR already queued in the
/// merge queue is being handled by the queue, not awaiting action, so a
/// CLEAN-but-queued snapshot stays non-ready too.
fn requirements_ready(req: &MergeRequirements) -> bool {
    req.state == "open"
        && !req.is_draft
        && req.mergeable == Some(true)
        && !req.has_conflicts
        && !req.is_behind
        && req.merge_blocked_reason.is_none()
        && req.checks.failing_required.is_empty()
        && req.checks.pending_required.is_empty()
        && !matches!(
            req.approvals.decision.as_str(),
            "changes_requested" | "review_required"
        )
        && !(req.approvals.decision == "none"
            && req
                .approvals
                .needed
                .is_some_and(|needed| req.approvals.have < i64::from(needed)))
        && !(req.threads.unresolved != Some(0) && req.threads.resolution_required == Some(true))
        && !matches!(
            req.merge_state_status.as_deref(),
            Some("BLOCKED" | "BEHIND" | "DIRTY" | "UNKNOWN")
        )
        && req.is_in_merge_queue != Some(true)
}

/// Fold a workspace's monitor rows into the displayStatus PR signals
/// (§6.5): an ACTIVE row whose persisted `last_snapshot` shows the PR
/// open/draft raises `open` — `queued` when the PR is open (not draft) and
/// sits in the merge queue (`isInMergeQueue`), and `ready` only when the
/// snapshot's full merge-requirements checklist is clear
/// ([`requirements_ready`]; truly mergeable, not just conflict-free — which
/// already excludes queued PRs) — while the LATEST
/// (most recently updated) COMPLETED row raises `merged` when its final
/// snapshot shows `merged` — matching linked-PR step-6 "latest" semantics,
/// so an older merged monitor never shadows a newer closed-unmerged one.
/// A row with no snapshot or an unparseable blob contributes nothing
/// (never fails the derivation), and cancelled rows are excluded by the
/// caller's SQL filter (which also bounds completed rows to the latest
/// one). An ACTIVE row already showing a terminal snapshot (a poll
/// observed the merge but lost its guarded terminalize write) contributes
/// nothing — the next tick re-detects and completes it.
///
/// `terminal_prs` are the workspace's own PR copies already persisted
/// merged/closed ([`crate::workspace_status::terminal_pr_copies`]): an
/// ACTIVE row whose snapshot names the same PR URL ([`pr_ops::same_pr_url`])
/// contributes nothing when that copy is fresher than the row's last poll
/// ([`superseded_by_terminal_copy`]) — the passive `github.pulls.get` fold
/// writes the copy straight from the forge, so the sidebar must not wait
/// for the monitor sweep to re-observe the merge. Only the derivation
/// yields; the row's snapshot, pending changes, and debounce state are
/// untouched, so the monitor's own terminal report still fires.
pub(crate) fn fold_monitor_pr_signals(
    monitors: &[PrMonitor],
    terminal_prs: &[&PullRequestInfo],
) -> MonitorPrSignals {
    let mut signals = MonitorPrSignals::default();
    let mut latest_completed: Option<&PrMonitor> = None;
    for m in monitors {
        match m.state {
            PrMonitorState::Active => {
                let Some(snapshot) = m
                    .last_snapshot
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
                else {
                    continue;
                };
                if superseded_by_terminal_copy(&snapshot, terminal_prs) {
                    continue;
                }
                let req = &snapshot.requirements;
                if matches!(req.state.as_str(), "open" | "draft") {
                    signals.open = true;
                    if req.state == "open" && !req.is_draft && req.is_in_merge_queue == Some(true) {
                        signals.queued = true;
                    }
                    if requirements_ready(req) {
                        signals.ready = true;
                    }
                }
            }
            PrMonitorState::Completed => {
                if latest_completed.is_none_or(|prev| m.updated_at > prev.updated_at) {
                    latest_completed = Some(m);
                }
            }
            PrMonitorState::Cancelled => {}
        }
    }
    if let Some(m) = latest_completed {
        let merged = m
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
            .is_some_and(|s| s.requirements.state == "merged");
        signals.merged = merged;
    }
    signals
}

/// Whether an ACTIVE monitor's snapshot is superseded by a workspace-owned
/// terminal copy of the same PR (by URL). A `Merged` copy always wins —
/// merged is the one irreversible forge state. A `Closed` copy wins only
/// when its forge `updatedAt` is later than the snapshot's own observation
/// time ([`snapshot_observed_at`]; unknown → the copy wins): a closed PR can
/// be reopened, and a monitor that re-observed the PR open after the copy's
/// timestamp is then the fresher observation. An unparseable copy timestamp
/// never supersedes.
fn superseded_by_terminal_copy(
    snapshot: &PrMonitorSnapshot,
    terminal_prs: &[&PullRequestInfo],
) -> bool {
    terminal_prs.iter().any(|pr| {
        pr_ops::same_pr_url(&pr.url, &snapshot.url)
            && match pr.status {
                PullRequestStatus::Merged => true,
                PullRequestStatus::Closed => parse_iso(&pr.updated_at).is_some_and(|updated| {
                    snapshot_observed_at(snapshot).is_none_or(|observed| updated > observed)
                }),
                PullRequestStatus::Open | PullRequestStatus::Draft => false,
            }
    })
}

/// When the monitor's persisted snapshot was actually read off the forge:
/// the snapshot's own `observed_at`, and nothing else. The row's
/// `last_polled_at` is NOT a stand-in for a snapshot persisted before the
/// field existed: a failed poll ([`Services::record_pr_monitor_error`])
/// advances it while keeping the previous snapshot, and the flush
/// ([`Services::emit_pending_changes`]) then clears `last_error` without
/// touching either, so neither column can vouch for the snapshot's age.
/// A legacy snapshot has unknown freshness and yields to any terminal copy.
fn snapshot_observed_at(snapshot: &PrMonitorSnapshot) -> Option<time::OffsetDateTime> {
    snapshot.observed_at.as_deref().and_then(parse_iso)
}

/// Light metadata for one ACTIVE PR monitor — the idle-visibility
/// `waitingOnPrMonitors` entry shape: `{ monitorId, repo, prNumber, title? }`.
/// `title` is read off the persisted baseline snapshot (absent until the
/// first successful poll) and omitted when unknown; no requirements
/// hydration otherwise, keeping payloads light (mirrors the hook manager's
/// `waiting_on_hooks_entry`).
pub(crate) fn waiting_on_pr_monitors_entry(m: &PrMonitor) -> Value {
    let mut v = json!({
        "monitorId": m.monitor_id,
        "repo": format!("{}/{}", m.repo_owner, m.repo_name),
        "prNumber": m.pr_number,
    });
    let title = m
        .last_snapshot
        .as_deref()
        .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
        .map(|s| s.title);
    if let Some(title) = title {
        v["title"] = Value::String(title);
    }
    v
}

/// Synthesize a [`PullRequestInfo`] from a monitor list-entry projection —
/// the monitor-derived entry the `workspace.list` / `workspace.subscribe`
/// seq-0 PR merge appends when no persisted source already carries the PR.
/// Everything is read off the persisted row's [`PrMonitorListEntry`]
/// projection (the snapshot scalars were extracted in SQL, never re-fetched
/// and never hydrated as a blob — intent-hq/monorepo#3878).
/// Mirrors the FE's `mergeMonitoredPRs` fallbacks: URL/title synthesized from
/// the repo identity when the monitor has no snapshot yet, and status
/// resolved as snapshot state → draft flag → `completed` ⇒ closed (terminal
/// covers both merged and closed; don't falsely claim merged) → open.
pub(crate) fn pr_monitor_pr_info(m: &PrMonitorListEntry) -> PullRequestInfo {
    let status = match m.snapshot_state.as_deref() {
        Some("merged") => PullRequestStatus::Merged,
        Some("closed") => PullRequestStatus::Closed,
        Some("draft") => PullRequestStatus::Draft,
        _ if m.snapshot_is_draft == Some(true) => PullRequestStatus::Draft,
        _ if m.state == PrMonitorState::Completed => PullRequestStatus::Closed,
        _ => PullRequestStatus::Open,
    };
    let url = m.snapshot_url.clone().unwrap_or_else(|| {
        format!(
            "https://github.com/{}/{}/pull/{}",
            m.repo_owner, m.repo_name, m.pr_number
        )
    });
    let title = m
        .snapshot_title
        .clone()
        .unwrap_or_else(|| format!("{}/{}#{}", m.repo_owner, m.repo_name, m.pr_number));
    PullRequestInfo {
        id: m.pr_number.to_string(),
        number: m.pr_number.cast_unsigned(),
        url,
        title,
        status,
        // Monitor-row timestamps stand in for the PR's own (the snapshot
        // does not carry them), mirroring the FE merge.
        created_at: m.created_at.clone(),
        updated_at: m.updated_at.clone(),
        base_ref: None,
        head_ref: None,
        head_sha: m.snapshot_head_sha.clone(),
        author: None,
        mergeable: m.snapshot_mergeable,
        mergeable_state: None,
        is_draft: m.snapshot_is_draft,
    }
}

/// The `messageMetadata` payload attached to every PR-monitor wake delivery
/// (PROTOCOL §5.42): `{ type: "pr_monitor_wake", monitorId, repo, prNumber,
/// reason, url? }`. `url` is the PR's HTML URL read off the monitor's
/// persisted baseline snapshot; the key is OMITTED (never null) when the
/// monitor has no baseline yet. The `transferred` wake
/// ([`Services::wake_former_owner_after_transfer`]) adds `adoptedBy`.
/// `paused_until` is the global rate-limit pause deadline
/// ([`Services::sweep_rate_limit_paused_until`]): the key `pausedUntil` is
/// present only while the gate is closed (never null).
fn pr_monitor_wake_metadata(m: &PrMonitor, reason: &str, paused_until: Option<&str>) -> Value {
    let mut metadata = json!({
        "type": "pr_monitor_wake",
        "monitorId": m.monitor_id,
        "repo": format!("{}/{}", m.repo_owner, m.repo_name),
        "prNumber": m.pr_number,
        "reason": reason,
    });
    let url = m
        .last_snapshot
        .as_deref()
        .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
        .map(|s| s.url);
    if let Some(url) = url {
        metadata["url"] = Value::String(url);
    }
    if let Some(until) = paused_until {
        metadata["pausedUntil"] = Value::String(until.to_string());
    }
    metadata
}

/// The list-surface projection of one monitor: identity + lifecycle plus the
/// hover/click fields the FE needs — PR title/URL and a compact summary of
/// the last-refresh snapshot, whether changes are accumulated awaiting the
/// debounce emit, and when the last change landed. Everything is read off the
/// persisted row (the baseline snapshot column is parsed, never re-fetched),
/// so a list stays O(rows returned).
///
/// `paused_until` is the global rate-limit pause deadline
/// ([`Services::sweep_rate_limit_paused_until`]), read once per call off the
/// in-memory gate: an ACTIVE row carries it as `pausedUntil` while the gate
/// is closed (its checklist is not being refreshed), and the key is absent
/// otherwise — never null, and never on a terminal row.
fn pr_monitor_wire(m: &PrMonitor, paused_until: Option<&str>) -> Value {
    let snapshot: Option<PrMonitorSnapshot> = m
        .last_snapshot
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let mut out = json!({
        "monitorId": m.monitor_id,
        "workspaceId": m.workspace_id,
        "agentId": m.agent_id,
        "repo": format!("{}/{}", m.repo_owner, m.repo_name),
        "prNumber": m.pr_number,
        "state": m.state,
        "pendingChanges": m.pending_changes,
        "hasPendingChanges": !m.pending_changes.is_empty(),
        "createdAt": m.created_at,
        "updatedAt": m.updated_at,
    });
    let obj = out.as_object_mut().expect("json object");
    for (key, value) in [
        ("pendingSince", &m.pending_since),
        ("lastChangeAt", &m.last_change_at),
        ("lastPolledAt", &m.last_polled_at),
        ("lastError", &m.last_error),
    ] {
        if let Some(v) = value {
            obj.insert(key.to_string(), Value::String(v.clone()));
        }
    }
    if let Some(until) = paused_until.filter(|_| m.state == PrMonitorState::Active) {
        obj.insert("pausedUntil".to_string(), Value::String(until.to_string()));
    }
    if let Some(s) = snapshot {
        let r = &s.requirements;
        obj.insert("title".to_string(), Value::String(s.title.clone()));
        obj.insert("url".to_string(), Value::String(s.url.clone()));
        let mut last = json!({
            "state": r.state,
            "isDraft": r.is_draft,
            "hasConflicts": r.has_conflicts,
            "isBehind": r.is_behind,
            "mergeable": r.mergeable,
            "mergeBlockedReason": r.merge_blocked_reason,
            "checks": {
                "total": r.checks.total,
                "passed": r.checks.passed,
                "failed": r.checks.failed,
                "pending": r.checks.pending,
                "failingRequired": r.checks.failing_required,
                "pendingRequired": r.checks.pending_required,
                "requiredKnown": r.checks.required_known,
            },
            "approvals": {
                "decision": r.approvals.decision,
                "have": r.approvals.have,
                "needed": r.approvals.needed,
                "changesRequested": r.approvals.changes_requested,
            },
            "threads": {
                "resolutionRequired": r.threads.resolution_required,
            },
            "rulesKnown": r.rules_known,
        });
        // Presence-detected: the count appears only when the thread
        // resolution state was readable (never null).
        if let Some(unresolved) = r.threads.unresolved {
            last["threads"]["unresolved"] = json!(unresolved);
        }
        // Presence-detected: the key appears only when the host reported
        // the PR queued (never null).
        if let Some(queued) = r.is_in_merge_queue {
            last["isInMergeQueue"] = json!(queued);
        }
        // Same presence rule: only when the host reported an ejection event.
        if let Some(ejection) = &r.merge_queue_ejection {
            last["mergeQueueEjection"] = serde_json::to_value(ejection).expect("serialize");
        }
        obj.insert("lastSnapshot".to_string(), last);
    }
    out
}

/// Render the refreshed merge-requirements checklist as the wake's
/// "where the PR stands now" section. Wording owned by the harness (H6);
/// production callers ride [`render_change_wake`], which composes the
/// checklist inside the harness — this delegator remains for the golden
/// fixtures.
#[cfg(test)]
pub(crate) fn render_checklist(s: &PrMonitorSnapshot) -> String {
    crate::harness::latest().pr_checklist(s)
}

/// The consolidated change wake: what moved since the last emit, followed by
/// the refreshed checklist. Wording owned by the harness (H6).
pub(crate) fn render_change_wake(
    m: &PrMonitor,
    changes: &[String],
    snapshot: &PrMonitorSnapshot,
) -> String {
    crate::harness::latest().pr_change_wake(&monitor_label(m), changes, snapshot)
}

/// The terminal wake: the PR merged or closed, so monitoring stopped. States
/// that explicitly, with the reason, so the model does not keep waiting.
/// Wording owned by the harness (H6).
pub(crate) fn render_terminal_wake(
    m: &PrMonitor,
    changes: &[String],
    snapshot: &PrMonitorSnapshot,
) -> String {
    crate::harness::latest().pr_terminal_wake(&monitor_label(m), changes, snapshot)
}

impl Services {
    /// The tick cadence of the centralized monitor loop and the per-PR poll
    /// interval floor (`prMonitor.pollSeconds`), clamped to
    /// [`MIN_PR_MONITOR_POLL_SECONDS`]. Read live from the settings registry
    /// on every tick so a config change applies without a restart; an
    /// explicit override wins when wired. The interval a PR is actually
    /// revisited on is [`effective_pr_monitor_interval_secs`].
    pub(crate) fn pr_monitor_poll_interval(&self) -> Duration {
        let secs = self
            .pr_monitor_poll_seconds
            .unwrap_or_else(|| self.effective_settings().pr_monitor.poll_seconds)
            .max(MIN_PR_MONITOR_POLL_SECONDS);
        Duration::from_secs(secs)
    }

    /// The hourly forge request budget the monitor loop's cadence is
    /// modelled on (`prMonitor.hourlyRequestBudget`), clamped into
    /// [[`MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET`],
    /// [`MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET`]] so a hand-edited config
    /// file can neither divide by zero nor plan more polling than the forge
    /// quota serves. Read live on every tick like the poll cadence; an
    /// explicit override wins when wired.
    pub(crate) fn pr_monitor_hourly_request_budget(&self) -> u64 {
        self.pr_monitor_hourly_request_budget
            .unwrap_or_else(|| self.effective_settings().pr_monitor.hourly_request_budget)
            .clamp(
                MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET,
                MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET,
            )
    }

    /// The share of the forge's remaining quota the monitor loop may plan to
    /// spend before the window resets (`prMonitor.quotaSharePercent`),
    /// clamped into [[`MIN_PR_MONITOR_QUOTA_SHARE_PERCENT`],
    /// [`MAX_PR_MONITOR_QUOTA_SHARE_PERCENT`]]. Read live on every tick like
    /// the poll cadence; an explicit override wins when wired.
    pub(crate) fn pr_monitor_quota_share_percent(&self) -> u64 {
        self.pr_monitor_quota_share_percent
            .unwrap_or_else(|| self.effective_settings().pr_monitor.quota_share_percent)
            .clamp(
                MIN_PR_MONITOR_QUOTA_SHARE_PERCENT,
                MAX_PR_MONITOR_QUOTA_SHARE_PERCENT,
            )
    }

    /// The tick's one quota-free `rate_limit` probe, reduced to the window
    /// the cadence plans on ([`QuotaWindow`]). `probed` is a status an
    /// earlier step of the same tick already paid for (the early lift,
    /// [`Services::maybe_lift_rate_limit_pause`]) — reused rather than
    /// probed again, so a tick never spends more than one probe. A failed
    /// probe is logged at DEBUG and reads as no window: the cadence falls
    /// back to the hourly-budget model, exactly as before the probe existed.
    async fn pr_monitor_quota_window(
        &self,
        sc: &Arc<dyn SourceControl>,
        probed: Option<RateLimitStatus>,
    ) -> Option<QuotaWindow> {
        let status = match probed {
            Some(status) => status,
            None => match sc.rate_limit_status().await {
                Ok(status) => status,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "pr monitor sweep: quota probe failed; planning the cadence on the hourly budget alone"
                    );
                    return None;
                }
            },
        };
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        QuotaWindow::from_status(&status, now_unix)
    }

    /// Log a quota deferral at INFO once per deferral run (never per tick):
    /// the share of the remaining quota cannot pay for one fetch, so no PR
    /// is polled until the window resets. The next interval cadence logs
    /// again when polling resumes.
    fn note_pr_monitor_deferral(&self, distinct_prs: usize, window: QuotaWindow) {
        let mut last = self.pr_monitor_logged_interval.lock().unwrap();
        if *last == Some(LoggedCadence::Deferred) {
            return;
        }
        *last = Some(LoggedCadence::Deferred);
        tracing::info!(
            distinct_prs,
            quota_remaining = window.remaining,
            quota_reset_in_secs = window.reset_in_secs,
            quota_share_percent = self.pr_monitor_quota_share_percent(),
            "pr monitor: {distinct_prs} distinct PRs monitored, polling deferred until the forge \
             quota window resets in {}s ({} requests left; the configured share does not cover \
             one fetch)",
            window.reset_in_secs,
            window.remaining
        );
    }

    /// Log the effective per-PR interval at INFO once per change (never per
    /// tick), so the daemon log shows when the monitored-PR count — or a
    /// running-low forge quota (`quota`, present only when the remaining
    /// quota is what stretched the interval past the hourly-budget cadence
    /// `budget_secs`) — stretched the cadence and back.
    fn note_pr_monitor_cadence(
        &self,
        distinct_prs: usize,
        effective_secs: u64,
        poll_secs: u64,
        budget_secs: u64,
        quota: Option<QuotaWindow>,
    ) {
        let mut last = self.pr_monitor_logged_interval.lock().unwrap();
        if *last == Some(LoggedCadence::Interval(effective_secs)) {
            return;
        }
        *last = Some(LoggedCadence::Interval(effective_secs));
        if let Some(window) = quota.filter(|_| effective_secs > budget_secs) {
            tracing::info!(
                distinct_prs,
                effective_secs,
                poll_secs,
                budget_secs,
                quota_remaining = window.remaining,
                quota_reset_in_secs = window.reset_in_secs,
                "pr monitor: {distinct_prs} distinct PRs monitored, effective poll interval \
                 {effective_secs}s (configured {poll_secs}s, hourly budget {budget_secs}s) — \
                 stretched by the remaining forge quota ({} requests left, window resets in {}s)",
                window.remaining,
                window.reset_in_secs
            );
        } else {
            tracing::info!(
                distinct_prs,
                effective_secs,
                poll_secs,
                "pr monitor: {distinct_prs} distinct PRs monitored, effective poll interval \
                 {effective_secs}s (configured {poll_secs}s)"
            );
        }
    }

    /// The effective debounce quiet window (`prMonitor.debounceSeconds`),
    /// clamped to [`MIN_PR_MONITOR_DEBOUNCE_SECONDS`]. Read live from the
    /// settings registry per evaluation so a config change applies to the
    /// next window; an explicit override wins when wired. Evaluated at the
    /// effective poll cadence, so a wake may arrive up to one effective
    /// interval after the window elapses.
    pub(crate) fn pr_monitor_debounce(&self) -> Duration {
        let secs = self
            .pr_monitor_debounce_seconds
            .unwrap_or_else(|| self.effective_settings().pr_monitor.debounce_seconds)
            .max(MIN_PR_MONITOR_DEBOUNCE_SECONDS);
        Duration::from_secs(secs)
    }

    /// Register (or idempotently re-arm) a monitor on `(repo, pr_number)` for
    /// `agent_id`, returning the row plus the freshly fetched checklist. A
    /// re-register of an existing ACTIVE monitor never duplicates the row: it
    /// refreshes the baseline and clears any pending changes, so the agent's
    /// next wake reports only what moves from here.
    ///
    /// The direct-service convenience over [`Services::pr_monitor_try_register`]:
    /// a workspace-level refusal (another live agent already holds the PR's
    /// active monitor) surfaces as `Error::InvalidParams` naming the owner.
    /// The MCP op ([`Services::pr_monitor_start_op`]) uses the structured
    /// outcome instead so the model can act on it. An adoption from a dead
    /// owner is an ordinary success here.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` when the agent is already at its monitor cap or another live agent in the workspace already monitors the PR, and propagates store or forge failures (e.g. when the PR cannot be fetched).
    pub async fn pr_monitor_register(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: u64,
    ) -> Result<(PrMonitor, MergeRequirements)> {
        match self
            .pr_monitor_try_register(workspace_id, agent_id, repo_owner, repo_name, pr_number)
            .await?
        {
            PrMonitorRegistration::Registered {
                monitor,
                requirements,
                ..
            } => Ok((*monitor, requirements)),
            PrMonitorRegistration::Refused(refusal) => Err(Error::InvalidParams(format!(
                "pr.monitor: {} is already monitored by agent {} in this workspace",
                monitor_label(&refusal.owner),
                refusal.owner.agent_id
            ))),
        }
    }

    /// Register (or idempotently re-arm) a monitor, or REFUSE when another
    /// LIVE agent in `workspace_id` already holds the ACTIVE monitor on the
    /// PR — one monitor per PR per workspace
    /// (`idx_pr_monitor_workspace_identity`). The refusal is decided before
    /// the forge fetch, so a refused call costs no forge request and
    /// persists nothing; the caller's OWN re-register is never refused (it
    /// re-arms).
    ///
    /// When the holder can no longer receive wakes — terminal status
    /// (`error` / `deleted` / `completed`), soft-retired, or its session row
    /// gone — the monitor is ORPHANED and the caller ADOPTS it instead
    /// (intent-hq/intent#5079): the same row is re-parented and re-armed
    /// under the caller (no second row, so the workspace-identity index
    /// holds), the outcome carries `adopted_from`, and the
    /// `prMonitor:registered` event marks the adoption. Adoption counts
    /// against the adopter's own cap exactly like a fresh registration.
    ///
    /// A LIVE holder that is the caller's DIRECT sub-agent and has SETTLED
    /// ([`Services::pr_monitor_child_settled`]: task `complete`/`cancelled`,
    /// or `RuntimeIdle` with no waiting reason other than PR monitors) is
    /// adopted the same way — parent takeover — and, unlike a dead owner,
    /// the child is woken once with a `transferred` notice naming the
    /// adopter. A grandparent, sibling, or the child itself (once the parent
    /// holds the row) is refused as before.
    ///
    /// The initial fetch is load-bearing — a forge that cannot read the PR
    /// (unsupported host, missing PR, no token) fails registration rather
    /// than persisting a monitor that could never poll.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` when the agent is already at its monitor cap, and propagates store or forge failures (e.g. when the PR cannot be fetched).
    pub async fn pr_monitor_try_register(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: u64,
    ) -> Result<PrMonitorRegistration> {
        let existing = self
            .store
            .find_active_pr_monitor(agent_id, repo_owner, repo_name, pr_number.cast_signed())
            .await?;
        let mut orphan = None;
        // The pre-adoption row image of a settled child's monitor: the
        // former owner is woken with the transfer notice after the adoption
        // lands (an orphan's dead owner is never woken).
        let mut transferred_from: Option<PrMonitor> = None;
        if existing.is_none() {
            match self
                .pr_monitor_holder(workspace_id, agent_id, repo_owner, repo_name, pr_number)
                .await?
            {
                Some(PrMonitorHolder::Live(refusal)) => {
                    return Ok(PrMonitorRegistration::Refused(refusal));
                }
                Some(PrMonitorHolder::Orphaned(m)) => orphan = Some(m),
                Some(PrMonitorHolder::SettledChild(m)) => {
                    transferred_from = Some(m.clone());
                    orphan = Some(m);
                }
                None => {}
            }
            let cap = self.pr_monitors_max_per_agent as usize;
            let active = self
                .store
                .list_pr_monitors_by_agent(agent_id)
                .await?
                .into_iter()
                .filter(|m| m.state == PrMonitorState::Active)
                .count();
            if active >= cap {
                return Err(Error::InvalidParams(format!(
                    "pr.monitor: agent already monitors {active} PRs (max {cap})"
                )));
            }
        }

        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        let repo_ref = RepoRef::new(repo_owner, repo_name);
        let mut snapshot = fetch_snapshot(sc.as_ref(), &repo_ref, pr_number, None).await?;
        invalidate_fetch_cache(
            &self.pr_monitor_fetch_cache,
            &pr_key_for(&repo_ref, pr_number.cast_signed()),
        );
        let now = now_iso();
        snapshot.observed_at = Some(now.clone());
        let baseline = serde_json::to_string(&snapshot).ok();

        let mut adopted_from = None;
        let mut monitor = match existing {
            Some(m) => self.rearm_pr_monitor(m, baseline.clone(), &now).await?,
            None => None,
        };
        if monitor.is_none() {
            let mut adoptable = orphan.take();
            if transferred_from.is_some() {
                // The settled-child verdict predates the forge fetch, and
                // a child that picked up work meanwhile (a queued message,
                // a new turn, a fresh hook) never touches the monitor row,
                // so the adoption CAS below cannot see it: re-evaluate the
                // holder at the write. Only the CAS itself remains as a
                // window between this check and the re-parenting.
                adoptable = None;
                transferred_from = None;
                match self
                    .pr_monitor_holder(workspace_id, agent_id, repo_owner, repo_name, pr_number)
                    .await?
                {
                    Some(PrMonitorHolder::Live(refusal)) => {
                        return Ok(PrMonitorRegistration::Refused(refusal));
                    }
                    Some(PrMonitorHolder::Orphaned(o)) => adoptable = Some(o),
                    Some(PrMonitorHolder::SettledChild(o)) => {
                        transferred_from = Some(o.clone());
                        adoptable = Some(o);
                    }
                    None => {}
                }
            }
            if let Some(o) = adoptable {
                let from = o.agent_id.clone();
                monitor = self
                    .adopt_pr_monitor(o, agent_id, baseline.clone(), &now)
                    .await?;
                adopted_from = monitor.is_some().then_some(from);
            }
        }
        if monitor.is_none() {
            let m = PrMonitor {
                monitor_id: PrMonitorId::new(),
                workspace_id: workspace_id.clone(),
                agent_id: agent_id.clone(),
                repo_owner: repo_owner.to_string(),
                repo_name: repo_name.to_string(),
                pr_number: pr_number.cast_signed(),
                state: PrMonitorState::Active,
                last_snapshot: baseline.clone(),
                baseline_snapshot: baseline.clone(),
                pending_changes: Vec::new(),
                pending_since: None,
                last_change_at: None,
                last_polled_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            if self.store.insert_pr_monitor(&m).await? {
                monitor = Some(m);
            } else if let Some(winner) = self
                .store
                .find_active_pr_monitor(agent_id, repo_owner, repo_name, pr_number.cast_signed())
                .await?
            {
                // Lost an insert race against a concurrent register of the
                // same triple: re-arm the winner's row instead of surfacing
                // the unique-index violation (the call stays idempotent).
                monitor = self.rearm_pr_monitor(winner, baseline, &now).await?;
            } else {
                match self
                    .pr_monitor_holder(workspace_id, agent_id, repo_owner, repo_name, pr_number)
                    .await?
                {
                    // Lost the insert race to ANOTHER live agent's
                    // registration in this workspace: the same refusal as
                    // the pre-fetch check.
                    Some(PrMonitorHolder::Live(refusal)) => {
                        return Ok(PrMonitorRegistration::Refused(refusal));
                    }
                    // The orphan's row moved under us (a poll tick landed
                    // between the read and the adoption CAS): adopt the
                    // fresh image once more before giving up.
                    Some(PrMonitorHolder::Orphaned(o)) => {
                        transferred_from = None;
                        let from = o.agent_id.clone();
                        monitor = self.adopt_pr_monitor(o, agent_id, baseline, &now).await?;
                        adopted_from = monitor.is_some().then_some(from);
                    }
                    Some(PrMonitorHolder::SettledChild(o)) => {
                        transferred_from = Some(o.clone());
                        let from = o.agent_id.clone();
                        monitor = self.adopt_pr_monitor(o, agent_id, baseline, &now).await?;
                        adopted_from = monitor.is_some().then_some(from);
                    }
                    None => {}
                }
            }
        }
        let monitor = monitor.ok_or_else(|| {
            Error::Internal(
                "pr.monitor: registration raced a concurrent monitor mutation; retry".to_string(),
            )
        })?;
        let extra = adopted_from
            .as_ref()
            .map(|from| json!({ "adoptedFrom": from }));
        self.emit_pr_monitor_event(PR_MONITOR_REGISTERED, &monitor, extra)
            .await;
        if let Some(former) = transferred_from.filter(|_| adopted_from.is_some()) {
            self.wake_former_owner_after_transfer(&former, agent_id)
                .await;
        }
        // A newly persisted active monitor on an open PR can move the
        // derived displayStatus to `pr_open`/`pr_ready` (§6.5) and raise
        // the orthogonal `waiting` flag (§5.1).
        self.maybe_emit_display_status_changed(workspace_id).await;
        self.maybe_emit_waiting_changed(workspace_id).await;
        Ok(PrMonitorRegistration::Registered {
            monitor: Box::new(monitor),
            requirements: snapshot.requirements,
            adopted_from,
        })
    }

    /// The workspace-level duplicate check behind [`Services::pr_monitor_try_register`]:
    /// `Some(holder)` when an agent OTHER than `agent_id` holds the ACTIVE
    /// monitor on `(repo, pr_number)` in `workspace_id` — [`PrMonitorHolder::Live`]
    /// (a refusal naming that owner, session name included when it has one)
    /// while the owner can still receive wakes, [`PrMonitorHolder::Orphaned`]
    /// once it cannot (terminal status, soft-retired, or session row gone;
    /// intent-hq/intent#5079), or [`PrMonitorHolder::SettledChild`] when the
    /// live owner is the caller's DIRECT sub-agent that has settled
    /// ([`Services::pr_monitor_child_settled`]). `None` when the PR is
    /// unmonitored in the workspace or the holder is the caller itself. Any
    /// other session lookup error fails closed (propagated) rather than
    /// adopting a monitor whose owner might be live.
    async fn pr_monitor_holder(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: u64,
    ) -> Result<Option<PrMonitorHolder>> {
        let Some(owner) = self
            .store
            .find_active_pr_monitor_in_workspace(
                workspace_id,
                repo_owner,
                repo_name,
                pr_number.cast_signed(),
            )
            .await?
        else {
            return Ok(None);
        };
        if owner.agent_id == *agent_id {
            return Ok(None);
        }
        let session = match self.store.get_agent_session_summary(&owner.agent_id).await {
            Ok(session)
                if session.retired_at.is_some()
                    || crate::agent_ops::is_terminal_status(session.status) =>
            {
                return Ok(Some(PrMonitorHolder::Orphaned(owner)));
            }
            Ok(session) => session,
            Err(Error::NotFound(_)) => return Ok(Some(PrMonitorHolder::Orphaned(owner))),
            Err(e) => return Err(e),
        };
        let child_of_caller = session.parent_agent_id.as_ref() == Some(agent_id);
        if child_of_caller && self.pr_monitor_child_settled(&session).await {
            return Ok(Some(PrMonitorHolder::SettledChild(owner)));
        }
        let owner_agent_name = Some(session.name).filter(|n| !n.trim().is_empty());
        Ok(Some(PrMonitorHolder::Live(PrMonitorRefusal {
            owner,
            owner_agent_name,
            child_of_caller,
        })))
    }

    /// The parent-takeover predicate behind [`PrMonitorHolder::SettledChild`]:
    /// a live direct sub-agent has SETTLED when its linked task note is
    /// `complete` or `cancelled`, or when the session is `RuntimeIdle` with
    /// no waiting reason other than its active PR monitors
    /// ([`Services::agent_has_non_monitor_waiting_reason`] — the same set the
    /// idle-target watch guard consults). A child that is still running,
    /// has a queued message, an unresolved attention request, pending
    /// questions, live watches/subscriptions, or active hooks keeps its
    /// monitor. Every store probe fails CLOSED (not settled → the ordinary
    /// refusal): a takeover is a re-parenting write on a live agent's row,
    /// so uncertainty must never adopt — including the pending-question
    /// read, which the shared helper's convenience API collapses to "none
    /// pending" and is therefore probed here first in its propagating form
    /// ([`Services::try_pending_question_count`]). Registration evaluates
    /// it twice: at the pre-fetch precheck (so a still-working child's
    /// refusal costs no forge request) and again immediately before the
    /// adoption write, since none of those waiting reasons touch the
    /// monitor row the CAS guards; the window left is the CAS itself.
    async fn pr_monitor_child_settled(&self, child: &intent_core::AgentSession) -> bool {
        if let Some(task_note_id) = child.task_note_id.as_ref() {
            match self.store.get_note(&child.workspace_id, task_note_id).await {
                Ok(note) => {
                    if matches!(
                        note.metadata.task.as_ref().map(|t| t.status),
                        Some(
                            intent_core::TaskStatus::Complete | intent_core::TaskStatus::Cancelled
                        )
                    ) {
                        return true;
                    }
                }
                Err(Error::NotFound(_)) => {}
                Err(e) => {
                    tracing::warn!(
                        agent = %child.id.0,
                        error = %e,
                        "pr monitor takeover: task note lookup failed; refusing"
                    );
                    return false;
                }
            }
        }
        if !matches!(child.status, AgentStatus::RuntimeIdle) {
            return false;
        }
        match self.try_pending_question_count(&child.id).await {
            Ok(0) => {}
            Ok(_) => return false,
            Err(e) => {
                tracing::warn!(
                    agent = %child.id.0,
                    error = %e,
                    "pr monitor takeover: pending-question probe failed; refusing"
                );
                return false;
            }
        }
        match self.agent_has_non_monitor_waiting_reason(child).await {
            Ok(waiting) => !waiting,
            Err(e) => {
                tracing::warn!(
                    agent = %child.id.0,
                    error = %e,
                    "pr monitor takeover: waiting-reason probe failed; refusing"
                );
                false
            }
        }
    }

    /// Adopt an ORPHANED monitor for `agent_id` (intent-hq/intent#5079): the
    /// row is re-parented and re-armed in one guarded write (baseline
    /// refreshed, pending state cleared, debounce anchors reset — the
    /// [`Services::rearm_pr_monitor`] semantics), so the adopter's first wake
    /// reports only what moves from here rather than the dead owner's
    /// backlog. Returns `None` when the guarded write loses — the row was
    /// cancelled/completed/polled/adopted concurrently — so the caller can
    /// re-read instead of clobbering.
    async fn adopt_pr_monitor(
        &self,
        mut m: PrMonitor,
        agent_id: &AgentId,
        baseline: Option<String>,
        now: &str,
    ) -> Result<Option<PrMonitor>> {
        let updated = self
            .store
            .adopt_pr_monitor(
                &m.monitor_id,
                &m.agent_id,
                agent_id,
                PrMonitorPollUpdate {
                    last_snapshot: baseline.as_deref(),
                    baseline_snapshot: baseline.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(now),
                    updated_at: now,
                    expected_updated_at: &m.updated_at,
                    ..Default::default()
                },
            )
            .await?;
        if !updated {
            return Ok(None);
        }
        // A restart catch-up marker ([`Services::rehydrate_pr_monitors`])
        // belongs to the dead owner's pre-restart backlog, which the fresh
        // baseline just discarded; consume it so the adopter's first change
        // is debounced like any other, not fired immediately.
        self.consume_pr_monitor_catch_up(&m.monitor_id);
        tracing::info!(
            monitor = %m.monitor_id,
            from = %m.agent_id.0,
            to = %agent_id.0,
            label = %monitor_label(&m),
            "pr monitor: orphaned monitor adopted"
        );
        m.agent_id = agent_id.clone();
        m.last_snapshot = baseline.clone();
        m.baseline_snapshot = baseline;
        m.pending_changes = Vec::new();
        m.pending_since = None;
        m.last_change_at = None;
        m.last_polled_at = Some(now.to_string());
        m.last_error = None;
        m.updated_at = now.to_string();
        Ok(Some(m))
    }

    /// Re-arm an existing ACTIVE monitor row for an idempotent re-register:
    /// refresh the baseline, clear the pending state, and reset the debounce
    /// anchors. Returns `None` when the guarded write loses — the row was
    /// cancelled/completed/re-registered concurrently — so the caller can
    /// fall back instead of clobbering.
    async fn rearm_pr_monitor(
        &self,
        mut m: PrMonitor,
        baseline: Option<String>,
        now: &str,
    ) -> Result<Option<PrMonitor>> {
        let updated = self
            .store
            .update_pr_monitor_poll(
                &m.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: baseline.as_deref(),
                    baseline_snapshot: baseline.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(now),
                    updated_at: now,
                    expected_updated_at: &m.updated_at,
                    ..Default::default()
                },
            )
            .await?;
        if !updated {
            return Ok(None);
        }
        m.last_snapshot = baseline.clone();
        m.baseline_snapshot = baseline;
        m.pending_changes = Vec::new();
        m.pending_since = None;
        m.last_change_at = None;
        m.last_polled_at = Some(now.to_string());
        m.last_error = None;
        m.updated_at = now.to_string();
        Ok(Some(m))
    }

    /// Monitors owned by an agent, oldest first. Cancelled rows are excluded
    /// (they are removed from the UI); completed rows are retained so merged
    /// PRs stay visible.
    pub(crate) async fn pr_monitors_for_agent(&self, agent_id: &AgentId) -> Result<Vec<PrMonitor>> {
        Ok(self
            .store
            .list_pr_monitors_by_agent(agent_id)
            .await?
            .into_iter()
            .filter(|m| m.state != PrMonitorState::Cancelled)
            .collect())
    }

    /// Monitors in a workspace, oldest first, with the same cancelled-row
    /// exclusion as the per-agent view.
    pub(crate) async fn pr_monitors_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        Ok(self
            .store
            .list_pr_monitors_by_workspace(workspace_id)
            .await?
            .into_iter()
            .filter(|m| m.state != PrMonitorState::Cancelled)
            .collect())
    }

    /// Whether the workspace owns any ACTIVE PR monitor — a
    /// `Workspace.waiting` signal (§5.1, via
    /// [`Services::workspace_is_waiting`]): an idle agent still watching a
    /// PR via a monitor reads as waiting. (The monitored PR's own state
    /// separately feeds the displayStatus PR rungs via
    /// [`Services::workspace_monitor_pr_signals`].) SQL-filtered to active
    /// rows so the hot list/get enrichment cost is O(active monitors),
    /// never O(all monitor history in the workspace). Best-effort: a store
    /// read failure is logged and fails open to `false` (mirrors
    /// [`Services::workspace_has_active_hooks`]) so list/get emission is
    /// never wedged and activity is never fabricated.
    pub(crate) async fn workspace_has_active_pr_monitors(
        &self,
        workspace_id: &WorkspaceId,
    ) -> bool {
        match self
            .store
            .list_active_pr_monitors_by_workspace(workspace_id)
            .await
        {
            Ok(monitors) => !monitors.is_empty(),
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "active-pr-monitors displayStatus lookup failed; reads as none"
                );
                false
            }
        }
    }

    /// Probe the workspace's agent-monitored PRs for the displayStatus PR
    /// rungs (§6.5, [`MonitorPrSignals`]): ACTIVE monitors whose persisted
    /// `last_snapshot` shows the PR open/draft raise `open` (and `ready`
    /// when the snapshot says mergeable and not draft); the LATEST COMPLETED
    /// monitor raises `merged` when its final snapshot shows the PR merged.
    /// Purely snapshot-derived — no forge calls — and SQL-bounded to active
    /// rows plus the single most recently updated completed row, so the cost
    /// stays O(active monitors) even though completed rows are retained
    /// indefinitely. Best-effort: a store read failure is logged and reads
    /// as no signals (mirrors
    /// [`Services::workspace_has_active_pr_monitors`]) so list/get emission
    /// is never wedged and PR stages are never fabricated. `terminal_prs`
    /// are the workspace's own merged/closed PR copies an active monitor's
    /// open snapshot yields to ([`fold_monitor_pr_signals`]).
    pub(crate) async fn workspace_monitor_pr_signals(
        &self,
        workspace_id: &WorkspaceId,
        terminal_prs: &[&PullRequestInfo],
    ) -> MonitorPrSignals {
        match self
            .store
            .list_display_status_pr_monitors_by_workspace(workspace_id)
            .await
        {
            Ok(monitors) => fold_monitor_pr_signals(&monitors, terminal_prs),
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "monitor-pr displayStatus lookup failed; reads as no signals"
                );
                MonitorPrSignals::default()
            }
        }
    }

    /// Cancel an active monitor. `caller` is the cancelling agent
    /// (`ws.pr.unmonitor`): a non-owner is rejected and the owner gets no
    /// self-wake. The FE path (`caller = None`, `prMonitor.cancel`) cancels
    /// any monitor and notifies the owning agent that its monitor is gone.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the monitor does not exist in the workspace; `Error::InvalidParams` if the caller does not own the monitor or the monitor is not active.
    pub async fn pr_monitor_cancel(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &PrMonitorId,
        caller: Option<&AgentId>,
    ) -> Result<PrMonitor> {
        let monitor = self.store.get_pr_monitor(monitor_id).await?;
        if &monitor.workspace_id != workspace_id {
            return Err(Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        if let Some(caller) = caller {
            if caller != &monitor.agent_id {
                return Err(Error::InvalidParams(format!(
                    "pr.unmonitor: monitor {} is owned by agent {} — you can only cancel your \
                     own monitors",
                    monitor_id.0, monitor.agent_id.0
                )));
            }
        }
        if monitor.state != PrMonitorState::Active {
            return Err(Error::InvalidParams(format!(
                "pr.unmonitor: monitor {} is not active",
                monitor_id.0
            )));
        }
        // FE-cancel (no agent caller) wakes the owner with a notice;
        // owner-side cancel (`ws.pr.unmonitor`) delivers no wake.
        let notice = caller.is_none().then(|| {
            crate::harness::latest().pr_monitor_cancelled_from_app_notice(&monitor_label(&monitor))
        });
        match self
            .cancel_active_pr_monitor(monitor, notice.as_deref())
            .await?
        {
            Some(monitor) => Ok(monitor),
            // A concurrent cancel/complete won between our read and the
            // guarded write; the monitor is no longer active either way.
            None => Err(Error::InvalidParams(format!(
                "pr.unmonitor: monitor {} is not active",
                monitor_id.0
            ))),
        }
    }

    /// Core cancel transition shared by [`Services::pr_monitor_cancel`] and
    /// the archive sweep ([`Services::cancel_workspace_pr_monitors`]),
    /// mirroring [`Services::cancel_active_hook`]: guarded CAS write to
    /// `cancelled`, catch-up-marker removal, `prMonitor:cancelled` emit.
    /// With a `wake_notice` the owner is woken (the wake runs the deferral
    /// backstop itself, inside `wake_pr_monitor_owner`, after the delivery
    /// attempt); without one, no wake is delivered — a deferred completion
    /// watch on the (idle) owner would otherwise never settle when this was
    /// its last active monitor, so the backstop runs directly. Ends with the
    /// transition-only displayStatus recompute (§6.5). Returns `Ok(None)`
    /// when a concurrent cancel/complete won the CAS — the monitor is no
    /// longer active either way. The caller must have verified the monitor
    /// is ACTIVE.
    async fn cancel_active_pr_monitor(
        &self,
        mut monitor: PrMonitor,
        wake_notice: Option<&str>,
    ) -> Result<Option<PrMonitor>> {
        let now = now_iso();
        if !self
            .store
            .update_pr_monitor_state(&monitor.monitor_id, PrMonitorState::Cancelled, &now)
            .await?
        {
            return Ok(None);
        }
        monitor.state = PrMonitorState::Cancelled;
        monitor.updated_at = now;
        self.pr_monitor_catch_up
            .lock()
            .unwrap()
            .remove(&monitor.monitor_id);
        self.emit_pr_monitor_event(PR_MONITOR_CANCELLED, &monitor, None)
            .await;
        match wake_notice {
            Some(notice) => {
                self.wake_pr_monitor_owner(&monitor, notice, "cancelled")
                    .await;
            }
            None => {
                self.resettle_owner_after_pr_monitor_terminal(&monitor)
                    .await;
            }
        }
        // A cancelled monitor's open-PR signal lapses — the derived
        // displayStatus can drop off `pr_open`/`pr_ready` (§6.5) — and the
        // last active monitor settling drops the `waiting` flag (§5.1);
        // best-effort, transition-only emission.
        self.maybe_emit_display_status_changed(&monitor.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&monitor.workspace_id).await;
        Ok(Some(monitor))
    }

    /// Archive sweep (`workspace.archive`): cancel every ACTIVE PR monitor
    /// in the workspace through the shared cancel transition
    /// ([`Services::cancel_active_pr_monitor`]), mirroring the hook sweep
    /// ([`Services::cancel_workspace_hooks`]) — state persisted to
    /// `cancelled`, `prMonitor:cancelled` emitted, owner woken with a notice
    /// so the agent learns why its watch stopped. Runs AFTER the archived
    /// row is persisted: the wake rides the archived gate in
    /// [`Services::deliver_wake_message`], so it parks in the queue (at
    /// most) and never starts a turn while the workspace is archived.
    /// Terminal monitors are untouched, and unarchive does NOT resurrect
    /// cancelled monitors — the notice tells the owner to re-register if the
    /// PR still matters. Best-effort per monitor: a store failure is logged
    /// and the sweep moves on — archiving must not fail because one monitor
    /// row would not update.
    pub(crate) async fn cancel_workspace_pr_monitors(&self, workspace_id: &WorkspaceId) {
        let monitors = match self
            .store
            .list_active_pr_monitors_by_workspace(workspace_id)
            .await
        {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "archive pr-monitor sweep: monitor list failed; skipping"
                );
                return;
            }
        };
        for monitor in monitors {
            let monitor_id = monitor.monitor_id.clone();
            let notice = crate::harness::latest()
                .pr_monitor_cancelled_workspace_archived_notice(&monitor_label(&monitor));
            // `Ok(None)` = a concurrent cancel/complete won the CAS between
            // the list read and the guarded write; no longer active either way.
            if let Err(e) = self.cancel_active_pr_monitor(monitor, Some(&notice)).await {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    monitor = %monitor_id.0,
                    error = %e,
                    "archive pr-monitor sweep: cancel failed; continuing"
                );
            }
        }
    }

    /// Retire sweep (`ws.agent.retire`): cancel every ACTIVE PR monitor
    /// owned by the retiring agent through the shared cancel transition
    /// ([`Services::cancel_active_pr_monitor`]), mirroring the hook sweep
    /// ([`Services::cancel_agent_hooks`]) — state persisted to `cancelled`,
    /// `prMonitor:cancelled` emitted, waiting recomputed (§5.1). NO wake
    /// notice: the owner retired itself and is inert, so parking a notice
    /// in its queue is noise (the backstop in `cancel_active_pr_monitor`
    /// settles any deferred watches directly). Restore does NOT resurrect
    /// cancelled monitors (mirrors the unarchive precedent) — the agent
    /// re-registers if the PR still matters. Best-effort per monitor: a
    /// store failure is logged and the sweep moves on — retiring must not
    /// fail because one monitor row would not update.
    pub(crate) async fn cancel_agent_pr_monitors(&self, agent_id: &AgentId) {
        let monitors = match self.store.list_active_pr_monitors_by_agent(agent_id).await {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "retire pr-monitor sweep: monitor list failed; skipping"
                );
                return;
            }
        };
        for monitor in monitors {
            let monitor_id = monitor.monitor_id.clone();
            // `Ok(None)` = a concurrent cancel/complete won the CAS between
            // the list read and the guarded write; no longer active either way.
            if let Err(e) = self.cancel_active_pr_monitor(monitor, None).await {
                tracing::warn!(
                    agent = %agent_id.0,
                    monitor = %monitor_id.0,
                    error = %e,
                    "retire pr-monitor sweep: cancel failed; continuing"
                );
            }
        }
    }

    /// Deliver a monitor's pending consolidated wake right now, bypassing the
    /// remaining debounce window, and reset the debounce state. A no-op
    /// (`Ok(false)`) when nothing is pending.
    pub(crate) async fn pr_monitor_flush(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &PrMonitorId,
    ) -> Result<bool> {
        let monitor = self.store.get_pr_monitor(monitor_id).await?;
        if &monitor.workspace_id != workspace_id {
            return Err(Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        if monitor.state != PrMonitorState::Active || monitor.pending_changes.is_empty() {
            return Ok(false);
        }
        self.emit_pending_changes(&monitor).await
    }

    /// `check: true` variant of [`Services::pr_monitor_flush`]: first re-poll
    /// the one monitor on demand — fresh shared snapshot, recomputed
    /// coalesced pending set against the emit baseline, persisted through
    /// the same guarded CAS write as the sweep, terminalizing if the PR
    /// merged/closed — then deliver whatever is pending immediately,
    /// bypassing the debounce window. `Ok(false)` with no wake when the
    /// re-poll finds nothing changed vs. the emit baseline. A forge fetch
    /// failure records `lastError` (like a sweep poll) and propagates the
    /// error.
    pub(crate) async fn pr_monitor_check_and_flush(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &PrMonitorId,
    ) -> Result<bool> {
        let monitor = self.store.get_pr_monitor(monitor_id).await?;
        if &monitor.workspace_id != workspace_id {
            return Err(Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            )));
        }
        if monitor.state != PrMonitorState::Active {
            return Ok(false);
        }
        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        let repo_ref = monitor.repo();
        let shared =
            match fetch_shared_snapshot(sc.as_ref(), &repo_ref, monitor.pr_number.cast_unsigned())
                .await
            {
                Ok(shared) => shared,
                Err(e) => {
                    self.record_pr_monitor_error(&monitor, &e.to_string()).await;
                    return Err(e);
                }
            };
        invalidate_fetch_cache(&self.pr_monitor_fetch_cache, &pr_key(&monitor));
        // The poll itself can deliver the wake (the terminal final wake, or
        // a debounce window that had already elapsed).
        if self.poll_one_pr_monitor(&monitor, &shared).await? {
            return Ok(true);
        }
        // Otherwise flush the recomputed pending set (if any) immediately.
        let monitor = self.store.get_pr_monitor(monitor_id).await?;
        if monitor.state != PrMonitorState::Active || monitor.pending_changes.is_empty() {
            return Ok(false);
        }
        self.emit_pending_changes(&monitor).await
    }

    /// Spawn the ONE centralized poll loop: every `[prMonitor] pollSeconds`
    /// (re-read each tick), poll the DUE active monitors — each PR on its
    /// effective interval, at most a capped subset per tick (see the module
    /// docs). Returns the task handle so the composition root can
    /// hold/abort it.
    #[must_use]
    pub fn spawn_pr_monitor_loop(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(services.pr_monitor_poll_interval()).await;
                services.poll_due_pr_monitors().await;
            }
        })
    }

    /// One pass over every active monitor, regardless of freshness.
    ///
    /// `pub` so integration tests can drive a deterministic single sweep
    /// instead of racing [`Self::spawn_pr_monitor_loop`]'s timer; the loop
    /// itself goes through [`Self::poll_due_pr_monitors`], which also skips
    /// monitors polled within the current interval.
    pub async fn poll_pr_monitors(&self) {
        self.sweep_pr_monitors(false).await;
    }

    /// The loop-driven sweep: like [`Self::poll_pr_monitors`] but skips
    /// PRs whose oldest sibling `lastPolledAt` is fresher than the
    /// **effective** poll interval — typically a PR whose monitor was just
    /// registered or re-registered, whose registration fetch already stamped
    /// a current baseline — and fetches at most
    /// [`pr_monitor_fetches_per_tick`] distinct due PRs, oldest
    /// `lastPolledAt` first, so the rest roll over to later ticks. A due PR
    /// polls EVERY sibling monitor on it. Catch-up-marked monitors (boot
    /// rehydration) skip the freshness check for their first post-restart
    /// attempt and sort first.
    ///
    /// `pub` for the same reason as [`Self::poll_pr_monitors`]: integration
    /// tests drive one deterministic due-sweep instead of racing the loop's
    /// timer.
    pub async fn poll_due_pr_monitors(&self) {
        self.sweep_pr_monitors(true).await;
    }

    /// Age every fetch-cache entry by `by`, so a test can cross
    /// [`PR_MONITOR_MAX_CHEAP_AGE`] without sleeping.
    #[cfg(test)]
    pub(crate) fn backdate_pr_monitor_fetch_cache(&self, by: Duration) {
        for slot in self.pr_monitor_fetch_cache.lock().unwrap().values_mut() {
            if let Some(entry) = slot.entry.as_mut() {
                entry.fetched_at = entry.fetched_at.checked_sub(by).unwrap_or(entry.fetched_at);
            }
        }
    }

    /// The number of PRs the sweep's fetch cache currently holds a full
    /// fetch for (invalidated slots do not count).
    #[cfg(test)]
    pub(crate) fn pr_monitor_fetch_cache_len(&self) -> usize {
        self.pr_monitor_fetch_cache
            .lock()
            .unwrap()
            .values()
            .filter(|slot| slot.entry.is_some())
            .count()
    }

    /// One sweep over the active monitors. Per-monitor failures are logged
    /// and persisted as `lastError` — a forge outage must never kill the
    /// loop or terminalize a monitor.
    ///
    /// Forge fetches are deduplicated per distinct `(repo, pr)` WITHIN the
    /// sweep: the first monitor on a PR fetches its shared snapshot, every
    /// sibling monitor reuses it and diffs against its own baseline. A
    /// failed fetch is cached the same way and recorded on each affected
    /// monitor, so an unreachable PR costs one fetch attempt per tick, not
    /// one per monitor.
    ///
    /// Across sweeps, each fetch goes through the per-PR fetch cache
    /// ([`fetch_shared_snapshot_cached`]): `get_pr` is always issued, and
    /// the sub-reads are skipped while the PR's change fingerprint is
    /// unchanged (bounded by [`PR_MONITOR_MAX_CHEAP_POLLS`] and
    /// [`PR_MONITOR_MAX_CHEAP_AGE`]), so a quiet PR costs one forge call per
    /// poll instead of five or six.
    ///
    /// The sweep honours the global forge rate-limit gate shared with the
    /// PR-refresh and git-root sweeps (monorepo#2961): while the gate is
    /// paused the tick spends one quota-free `rate_limit` probe
    /// ([`Services::maybe_lift_rate_limit_pause`]) and, unless that lifts
    /// the pause early, is skipped before any forge call (no `lastError`
    /// churn; catch-up markers survive for the post-pause sweep), and a
    /// fetch that fails with [`Error::RateLimited`] opens the pause — which
    /// annotates `lastError` with the pause on every active monitor, this
    /// sweep's and every other workspace's alike, keeping any genuine error
    /// a monitor recorded earlier in the tick — and stops fetching further
    /// PRs in this sweep; monitors not yet reached keep their baseline and
    /// `lastPolledAt`. The shared gate is re-consulted
    /// before EVERY vacant-cache fetch, not just at the top of the sweep, so
    /// a pause opened mid-sweep by a sibling sweep (PR refresh, git roots)
    /// stops this sweep's remaining fetches too.
    ///
    /// With the gate open, a due-sweep tick spends the same single probe on
    /// the cadence instead ([`Services::pr_monitor_quota_window`]): the
    /// effective interval is stretched ahead of exhaustion so the projected
    /// spend to the window's reset stays within `prMonitor.quotaSharePercent`
    /// of the remaining quota ([`plan_quota_cadence`]) — or, once that share
    /// cannot pay for one fetch, nothing is polled until the window resets. A tick
    /// that lifted the pause reuses the lift's probe — one probe per tick
    /// either way; a full sweep (`skip_fresh == false`) plans no cadence and
    /// spends none.
    async fn sweep_pr_monitors(&self, skip_fresh: bool) {
        let mut probed = None;
        if self.sweeps_rate_limited() {
            let lifted = match pr_ops::resolve_source_control(self.source_control.clone()).await {
                Ok(sc) => self.maybe_lift_rate_limit_pause(&sc).await,
                Err(_) => None,
            };
            if lifted.is_none() {
                tracing::debug!("pr monitor sweep: forge rate limit pause active; skipping tick");
                return;
            }
            probed = lifted;
        }
        let monitors = match self.store.load_active_pr_monitors().await {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(error = %e, "pr monitor sweep: load failed; skipping tick");
                return;
            }
        };
        if monitors.is_empty() {
            self.pr_monitor_fetch_cache.lock().unwrap().clear();
            return;
        }
        {
            let active = monitors.iter().map(pr_key).collect::<HashSet<_>>();
            self.pr_monitor_fetch_cache
                .lock()
                .unwrap()
                .retain(|key, _| active.contains(key));
        }
        let sc = match pr_ops::resolve_source_control(self.source_control.clone()).await {
            Ok(sc) => sc,
            Err(e) => {
                tracing::debug!(error = %e, "pr monitor sweep: no source control; skipping tick");
                return;
            }
        };
        let monitors = if skip_fresh {
            let quota = self.pr_monitor_quota_window(&sc, probed).await;
            self.select_due_pr_monitors(monitors, quota)
        } else {
            monitors
        };
        let mut shared: HashMap<PrKey, std::result::Result<SharedPrSnapshot, String>> =
            HashMap::new();
        let mut rate_limited = false;
        for monitor in monitors {
            let key = pr_key(&monitor);
            let fetched = match shared.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                // The gate closed mid-sweep — by this sweep's own fetch or by
                // a sibling sweep sharing the gate: PRs not fetched yet stay
                // untouched until the pause window elapses.
                std::collections::hash_map::Entry::Vacant(_)
                    if rate_limited || self.sweeps_rate_limited() =>
                {
                    if !rate_limited {
                        tracing::debug!(
                            "pr monitor sweep: forge rate limit pause opened mid-sweep; skipping remaining fetches"
                        );
                        rate_limited = true;
                    }
                    continue;
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let repo_ref = monitor.repo();
                    // The timeout is defense in depth above the client-level
                    // network timeouts: a fetch that pends indefinitely maps
                    // to an error (recorded as `lastError` below) instead of
                    // wedging the sweep for every other monitor.
                    let fetched = match tokio::time::timeout(
                        self.pr_monitor_fetch_timeout,
                        fetch_shared_snapshot_cached(
                            sc.as_ref(),
                            &repo_ref,
                            monitor.pr_number.cast_unsigned(),
                            &self.pr_monitor_fetch_cache,
                            &key,
                        ),
                    )
                    .await
                    {
                        Ok(Err(Error::RateLimited(detail))) => {
                            self.pause_sweeps_for_rate_limit(&sc, &detail).await;
                            rate_limited = true;
                            Err(self.rate_limit_pause_error())
                        }
                        Ok(result) => result.map_err(|e| e.to_string()),
                        Err(_) => Err(format!(
                            "PR fetch timed out after {:?}",
                            self.pr_monitor_fetch_timeout
                        )),
                    };
                    entry.insert(fetched).clone()
                }
            };
            match fetched {
                Ok(snapshot) => {
                    if let Err(e) = self.poll_one_pr_monitor(&monitor, &snapshot).await {
                        tracing::warn!(
                            monitor = %monitor.monitor_id.0,
                            error = %e,
                            "pr monitor poll failed; will retry next tick"
                        );
                    }
                }
                Err(error) => {
                    // A forge error records `lastError` without touching the
                    // baseline — the next tick retries against the same
                    // baseline, so a transient outage never fabricates or
                    // loses a change (backoff is the poll interval itself).
                    self.record_pr_monitor_error(&monitor, &error).await;
                }
            }
            tokio::time::sleep(crate::SWEEP_INTER_WORKSPACE_PAUSE).await;
        }
    }

    /// The due-sweep selection: compute the effective per-PR interval from
    /// the distinct active PR count (siblings on one PR count once) — the
    /// hourly-budget cadence, stretched further when the remaining forge
    /// quota (`quota`, this tick's probe) would not cover the projected
    /// spend to the window's reset — then keep the oldest-polled due PRs —
    /// every sibling monitor included — up to this tick's fetch cap
    /// ([`select_due_pr_monitors`]). When the quota share cannot pay for
    /// one fetch the tick selects NOTHING: the deferral is decided here,
    /// before the stale-anchor and catch-up rules, so neither an old
    /// `lastPolledAt` nor a catch-up marker spends against an exhausted
    /// window; both are honoured on the first tick after the reset. The
    /// fetch cap is likewise zero while a stretched interval's fetch
    /// spacing has not elapsed since the newest poll
    /// ([`pr_monitor_fetches_per_tick`]), so a stale or catch-up backlog
    /// drains within the planned budget instead of one PR per tick.
    fn select_due_pr_monitors(
        &self,
        monitors: Vec<PrMonitor>,
        quota: Option<QuotaWindow>,
    ) -> Vec<PrMonitor> {
        let distinct_prs = monitors.iter().map(pr_key).collect::<HashSet<_>>().len();
        let poll_secs = self.pr_monitor_poll_interval().as_secs();
        let budget_secs = effective_pr_monitor_interval_secs(
            distinct_prs,
            poll_secs,
            self.pr_monitor_hourly_request_budget(),
        );
        let effective_secs = match plan_quota_cadence(
            distinct_prs,
            poll_secs,
            quota,
            self.pr_monitor_quota_share_percent(),
        ) {
            Some(QuotaCadence::DeferUntilReset { .. }) => {
                if let Some(window) = quota {
                    self.note_pr_monitor_deferral(distinct_prs, window);
                }
                return Vec::new();
            }
            Some(QuotaCadence::Interval(quota_secs)) => quota_secs.max(budget_secs),
            None => budget_secs,
        };
        self.note_pr_monitor_cadence(distinct_prs, effective_secs, poll_secs, budget_secs, quota);
        let interval = time::Duration::seconds(effective_secs.cast_signed());
        let now = time::OffsetDateTime::now_utc();
        let catch_up = self.pr_monitor_catch_up.lock().unwrap().clone();
        let candidates: Vec<DueCandidate> = monitors
            .into_iter()
            .map(|monitor| DueCandidate {
                anchor: monitor.last_polled_at.as_deref().and_then(parse_iso),
                catch_up: catch_up_unattempted(&monitor, &catch_up),
                monitor,
            })
            .collect();
        let newest_anchor_age_secs = candidates
            .iter()
            .filter_map(|c| c.anchor)
            .max()
            .map(|at| u64::try_from((now - at).whole_seconds()).unwrap_or(0));
        select_due_pr_monitors(
            candidates,
            now,
            interval,
            pr_monitor_fetches_per_tick(
                distinct_prs,
                poll_secs,
                effective_secs,
                newest_anchor_age_secs,
            ),
        )
    }

    /// Poll one monitor against the sweep's shared snapshot: RECOMPUTE the
    /// coalesced pending set against the persisted emit baseline, and either
    /// terminalize (PR merged/closed → immediate final wake) or evaluate the
    /// debounce window. Returns whether a wake was delivered (the terminal
    /// final wake, or the consolidated change wake on an elapsed window).
    async fn poll_one_pr_monitor(
        &self,
        monitor: &PrMonitor,
        shared: &SharedPrSnapshot,
    ) -> Result<bool> {
        let mut previous: Option<PrMonitorSnapshot> = monitor
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let now = now_iso();
        let mut fresh = shared.materialize(previous.as_ref());
        fresh.observed_at = Some(now.clone());

        // Upgrade backfill: an anchor persisted before ejection tracking
        // existed has no event field at all, so the first tracked poll would
        // misread whatever historical removal event the probe now reports as
        // news and emit a false post-upgrade wake. Adopt the fresh event
        // into UNTRACKED anchors silently (persisted via the baseline
        // write-back below) — only ejections observed after tracking began
        // are reportable.
        let backfill = |s: &mut PrMonitorSnapshot| {
            if fresh.ejection_tracked && !s.ejection_tracked {
                s.requirements
                    .merge_queue_ejection
                    .clone_from(&fresh.requirements.merge_queue_ejection);
                s.ejection_tracked = true;
            }
        };
        if let Some(prev) = previous.as_mut() {
            backfill(prev);
        }

        // Per-poll activity (fresh vs the LAST POLL's snapshot) anchors the
        // debounce quiet-window; the pending set below is computed against
        // the EMIT baseline instead, so the two diffs serve distinct roles.
        let poll_activity = previous
            .as_ref()
            .is_some_and(|prev| !diff_snapshots(prev, &fresh).is_empty());

        // The emit baseline: the PR state as of the last delivered wake (or
        // registration). A row missing one (unparseable column) anchors on
        // the last poll's snapshot; a row with neither adopts the fresh
        // snapshot below, with nothing pending.
        let mut baseline: Option<PrMonitorSnapshot> = monitor
            .baseline_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .or(previous);
        if let Some(base) = baseline.as_mut() {
            backfill(base);
        }

        // The coalesced net set: REPLACED (never accumulated) each poll, so
        // a field that reverted to its baseline value drops out and A→B→C
        // renders as a single A→C line.
        let pending = baseline
            .as_ref()
            .map(|base| diff_snapshots(base, &fresh))
            .unwrap_or_default();

        // The catch-up marker is set by boot rehydration; the first poll
        // after a restart skips the debounce window. The marker is only
        // PEEKED here and consumed after the write-back/emit succeed, so a
        // transient store failure keeps the restart guarantee for the retry.
        let catch_up = self
            .pr_monitor_catch_up
            .lock()
            .unwrap()
            .contains_key(&monitor.monitor_id);

        // Anchors: `pending_since` marks when the coalesced set first became
        // non-empty; both anchors reset when it empties (a full revert
        // leaves nothing pending — and nothing to wake about).
        let (pending_since, last_change_at) = if pending.is_empty() {
            (None, None)
        } else {
            (
                monitor.pending_since.clone().or_else(|| Some(now.clone())),
                if poll_activity {
                    Some(now.clone())
                } else {
                    monitor.last_change_at.clone()
                },
            )
        };
        let fresh_json = serde_json::to_string(&fresh).ok();
        let baseline_json = match &baseline {
            Some(base) => serde_json::to_string(base).ok(),
            None => fresh_json.clone(),
        };
        // A success clears the genuine error — the store keeps the pause
        // annotation the row carries while the global rate-limit gate is
        // closed (this poll was in flight when the pause opened, or rode a
        // sibling's cached fetch): the first post-pause refresh clears it.
        if !self
            .store
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: fresh_json.as_deref(),
                    baseline_snapshot: baseline_json.as_deref(),
                    pending_changes: &pending,
                    pending_since: pending_since.as_deref(),
                    last_change_at: last_change_at.as_deref(),
                    last_polled_at: Some(&now),
                    last_error: None,
                    updated_at: &now,
                    expected_updated_at: &monitor.updated_at,
                },
            )
            .await?
        {
            // The row moved under this sweep's stale image (a concurrent
            // flush, cancel, or re-register): discard the write and its
            // side effects; the next tick re-reads and retries.
            return Ok(false);
        }
        let mut updated = monitor.clone();
        updated.last_snapshot = fresh_json;
        updated.baseline_snapshot = baseline_json;
        updated.pending_changes = pending;
        updated.pending_since = pending_since;
        updated.last_change_at = last_change_at;
        updated.last_polled_at = Some(now.clone());
        // What landed: the store dropped the genuine error and kept the
        // row's pause annotation — this image names the one the read saw.
        updated.last_error = monitor.last_error.as_deref().and_then(|e| {
            e.find(crate::rate_limit::PAUSE_ERROR_MARKER)
                .map(|at| e[at..].to_string())
        });
        updated.updated_at = now;

        // The FE's `pendingChanges` tracks the NET set: fire on any change
        // to it, including shrinking to empty on a revert.
        if updated.pending_changes != monitor.pending_changes {
            self.emit_pr_monitor_event(
                PR_MONITOR_CHANGED,
                &updated,
                Some(json!({ "changes": updated.pending_changes })),
            )
            .await;
        }

        // Terminal fast-path: a merged/closed PR stops monitoring with an
        // immediate, undebounced final wake. A lost guarded write inside
        // (a concurrent flush/cancel won the row) skips the wake, and that
        // outcome propagates so callers never report a delivery that did
        // not happen.
        if fresh.is_terminal() {
            let delivered = self.complete_pr_monitor(&updated, &fresh).await?;
            self.consume_pr_monitor_catch_up(&monitor.monitor_id);
            // The PR just merged/closed: refresh the owning workspace's PR
            // linkage right away (best-effort) so the persisted
            // `prStatus`/`activePullRequest` flip within the monitor's poll
            // cadence instead of waiting for the slower background sweep
            // tier (intent-hq/monorepo#2094). Runs on BOTH complete
            // outcomes — a lost guarded write only skips the wake, the PR
            // is terminal either way.
            self.refresh_workspace_pr_after_terminal(&monitor.workspace_id)
                .await;
            return Ok(delivered);
        }
        if updated.pending_changes.is_empty() {
            self.consume_pr_monitor_catch_up(&monitor.monitor_id);
            return Ok(false);
        }
        // Restart catch-up: anything accumulated across the downtime fires
        // now. Otherwise hold until the PR has been quiet for the window
        // (or the max-latency bound trips on a PR that never goes quiet).
        if (catch_up || self.pr_monitor_debounce_elapsed(&updated))
            && self.emit_pending_changes(&updated).await?
        {
            self.consume_pr_monitor_catch_up(&monitor.monitor_id);
            return Ok(true);
        }
        Ok(false)
    }

    /// Consume a monitor's restart catch-up marker once its post-restart
    /// state has been fully handled (delivered, terminalized, or found to
    /// have nothing pending).
    fn consume_pr_monitor_catch_up(&self, monitor_id: &PrMonitorId) {
        self.pr_monitor_catch_up.lock().unwrap().remove(monitor_id);
    }

    /// Whether a monitor's pending changes are due for delivery: the PR has
    /// been quiet for the configured debounce window since its most recent
    /// change, OR the oldest un-emitted change has waited out the max-latency
    /// bound ([`PR_MONITOR_DEBOUNCE_MAX_WAIT_FACTOR`] debounce windows since
    /// `pending_since`) — a busy PR whose coalesced set stays CONTINUOUSLY
    /// non-empty still gets its consolidated wake, late but never starved.
    /// (Coalescing weakens the bound: a full revert empties the set and
    /// resets `pending_since`, so churn that keeps netting out to nothing
    /// re-arms the clock rather than accruing toward the max-latency bound —
    /// by design, since a PR back at its baseline has nothing to report.)
    /// An unparseable/absent anchor emits immediately rather than stranding
    /// a pending wake forever.
    fn pr_monitor_debounce_elapsed(&self, monitor: &PrMonitor) -> bool {
        let window = time::Duration::seconds(self.pr_monitor_debounce().as_secs().cast_signed());
        let now = time::OffsetDateTime::now_utc();
        if let Some(since) = monitor.pending_since.as_deref().and_then(parse_iso) {
            if now - since >= window * PR_MONITOR_DEBOUNCE_MAX_WAIT_FACTOR {
                return true;
            }
        }
        let Some(anchor) = monitor
            .last_change_at
            .as_deref()
            .or(monitor.pending_since.as_deref())
            .and_then(parse_iso)
        else {
            return true;
        };
        now - anchor >= window
    }

    /// Deliver the consolidated wake for a monitor's coalesced pending set,
    /// advance the emit baseline to the delivered snapshot, and reset the
    /// debounce state (pending cleared, anchors dropped). Returns `false`
    /// without waking when the guarded clear loses — the row moved (a
    /// concurrent poll recomputed the set, or a flush/cancel/re-register
    /// landed) between the caller's read and the clear — so no change line is
    /// ever cleared without having been rendered into a delivered wake; the
    /// surviving pending state re-emits on a later tick. An EMPTY coalesced
    /// set (a PR that fully reverted to its baseline) also returns `false`:
    /// there is nothing to report, so no wake is sent.
    async fn emit_pending_changes(&self, monitor: &PrMonitor) -> Result<bool> {
        if monitor.pending_changes.is_empty() {
            return Ok(false);
        }
        let snapshot: Option<PrMonitorSnapshot> = monitor
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let Some(snapshot) = snapshot else {
            // No snapshot to describe: keep the pending changes rather than
            // dropping them; the next poll writes a snapshot and emits.
            return Ok(false);
        };
        let message = render_change_wake(monitor, &monitor.pending_changes, &snapshot);
        let now = now_iso();
        // The delivered snapshot becomes the new emit baseline: the next
        // wake reports only what moves from here.
        if !self
            .store
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: monitor.last_snapshot.as_deref(),
                    baseline_snapshot: monitor.last_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: monitor.last_polled_at.as_deref(),
                    updated_at: &now,
                    expected_updated_at: &monitor.updated_at,
                    ..Default::default()
                },
            )
            .await?
        {
            return Ok(false);
        }
        let mut emitted = monitor.clone();
        emitted.baseline_snapshot = monitor.last_snapshot.clone();
        emitted.pending_changes = Vec::new();
        emitted.pending_since = None;
        emitted.last_change_at = None;
        emitted.updated_at = now;
        self.wake_pr_monitor_owner(&emitted, &message, "changed")
            .await;
        self.emit_pr_monitor_event(PR_MONITOR_EMITTED, &emitted, None)
            .await;
        Ok(true)
    }

    /// Terminalize a monitor whose PR merged or closed: persist `completed`
    /// (the row is RETAINED so merged PRs stay visible), clear the pending
    /// state, and deliver the immediate final wake. Its "Changes since the
    /// last report" section coalesces the same way as a change wake —
    /// `diff(baseline, final)` — so the journey to terminal never replays
    /// intermediate transitions. Returns whether the final wake was
    /// delivered — `false` when the lost guarded write (a concurrent
    /// flush/cancel/re-register/adoption moved the row, or a cancel already
    /// terminalized it and delivered its own notice) skipped it.
    async fn complete_pr_monitor(
        &self,
        monitor: &PrMonitor,
        snapshot: &PrMonitorSnapshot,
    ) -> Result<bool> {
        let changes = monitor
            .baseline_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
            .map_or_else(
                || monitor.pending_changes.clone(),
                |base| diff_snapshots(&base, snapshot),
            );
        let message = render_terminal_wake(monitor, &changes, snapshot);
        let now = now_iso();
        // ONE guarded write for the state flip and the pending clear: the
        // final wake below goes to `monitor.agent_id`, so the image it was
        // rendered from must still be the row's owner when `completed`
        // lands — a two-statement terminalization left a window in which
        // an adoption (intent-hq/intent#5079) could re-parent the row
        // between them and have its only completion wake delivered to the
        // dead previous owner.
        if !self
            .store
            .complete_pr_monitor(&monitor.monitor_id, &now, &monitor.updated_at)
            .await?
        {
            // The row moved under us; skip the wake — the next tick
            // re-detects the terminal state under the row's current owner.
            return Ok(false);
        }
        let mut completed = monitor.clone();
        completed.state = PrMonitorState::Completed;
        completed.pending_changes = Vec::new();
        completed.pending_since = None;
        completed.last_change_at = None;
        completed.updated_at = now;
        self.pr_monitor_catch_up
            .lock()
            .unwrap()
            .remove(&completed.monitor_id);
        self.wake_pr_monitor_owner(&completed, &message, "completed")
            .await;
        self.emit_pr_monitor_event(PR_MONITOR_COMPLETED, &completed, None)
            .await;
        // Completion flips the monitor's PR signal from open to merged, so
        // the derived displayStatus can transition (e.g. `pr_open` →
        // `pr_merged`, §6.5), and the last active monitor settling drops
        // the `waiting` flag (§5.1) — best-effort, transition-only emission.
        self.maybe_emit_display_status_changed(&completed.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&completed.workspace_id)
            .await;
        Ok(true)
    }

    /// Best-effort refresh of the owning workspace's PR linkage after its
    /// monitored PR reached a terminal state (merged/closed), through
    /// [`Services::refresh_workspace_pr`] — which persists the delta and
    /// emits `pr:updated`/`pr:linked`/`pr:unlinked` itself. Bounded by
    /// `pr_refresh_fetch_timeout` — the refresh sweep's *aggregate* budget
    /// over one workspace's whole refresh (the linked-PR re-fetch, possible
    /// relink discovery via `list_prs`, and the store writes; not a
    /// per-request bound) — so a hung forge call can never wedge the
    /// serialized monitor sweep; errors and timeouts are logged, never
    /// propagated — the monitor's own terminal transition already persisted,
    /// and the background refresh sweep remains the backstop. Timeout caveat
    /// (shared with the sweep's wrap): the dropped future can land between
    /// the store write and the event publish, persisting the delta without
    /// `pr:updated` — rare (the client-level network timeouts fire first)
    /// and self-limiting, since clients re-read on the next snapshot.
    async fn refresh_workspace_pr_after_terminal(&self, workspace_id: &WorkspaceId) {
        match tokio::time::timeout(
            self.pr_refresh_fetch_timeout,
            self.refresh_workspace_pr(workspace_id),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(
                workspace = %workspace_id.0,
                error = %e,
                "pr monitor terminal: workspace PR refresh failed"
            ),
            Err(_) => tracing::warn!(
                workspace = %workspace_id.0,
                timeout = ?self.pr_refresh_fetch_timeout,
                "pr monitor terminal: workspace PR refresh timed out"
            ),
        }
    }

    /// Persist a failed poll's error without disturbing the baseline or the
    /// pending state; the store keeps the pause annotation the row carries
    /// while the global rate-limit gate is closed (an `error` that is itself
    /// the pause annotation — [`Services::rate_limit_pause_error`] — lands
    /// as no genuine error). Best-effort — a store failure on the error path
    /// is logged, never propagated.
    async fn record_pr_monitor_error(&self, monitor: &PrMonitor, error: &str) {
        let now = now_iso();
        if let Err(e) = self
            .store
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: monitor.last_snapshot.as_deref(),
                    baseline_snapshot: monitor.baseline_snapshot.as_deref(),
                    pending_changes: &monitor.pending_changes,
                    pending_since: monitor.pending_since.as_deref(),
                    last_change_at: monitor.last_change_at.as_deref(),
                    last_polled_at: Some(&now),
                    last_error: Some(error),
                    updated_at: &now,
                    expected_updated_at: &monitor.updated_at,
                },
            )
            .await
        {
            tracing::warn!(
                monitor = %monitor.monitor_id.0,
                error = %e,
                "pr monitor: failed to persist lastError"
            );
        }
    }

    /// Boot rehydration: every `active` monitor resumes. Rows whose owning
    /// agent is gone are cancelled instead. Each resumed monitor is marked
    /// for catch-up so its first poll delivers immediately — a baseline that
    /// moved during downtime, or a pending emit persisted but never
    /// delivered, must not wait out another debounce window. A pending set
    /// the recomputing poll could not reproduce (a pre-coalescing
    /// accumulated log left intact by the upgrade migration, which
    /// backfilled the baseline to the last poll's snapshot) is delivered
    /// as-is BEFORE that first poll — a wake awaiting delivery at upgrade
    /// time is never dropped. Returns the number of resumed monitors.
    ///
    /// A rate-limit pause annotation persisted by the previous process
    /// (monorepo#2961) outlives the in-memory gate it described: with the
    /// gate open — always, at boot — the stale annotations are cleared
    /// first ([`Services::clear_pr_monitor_pause_annotations`]), so the
    /// surfaces do not report a pause the new process is not observing;
    /// a still-exhausted quota re-pauses (and re-stamps) on the first poll.
    /// A caller running while the gate IS paused (a transfer import) leaves
    /// them in place — the check and the clear run under the gate's
    /// reconcile lock, so a pause opening between the two cannot have its
    /// fresh stamp erased.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if loading the persisted monitors, an owner status lookup, or re-emitting pending changes fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn rehydrate_pr_monitors(&self) -> Result<usize> {
        {
            let _reconcile = self.sweep_rate_limit.reconcile().await;
            if !self.sweeps_rate_limited() {
                self.clear_pr_monitor_pause_annotations(None).await;
            }
        }
        let monitors = self.store.load_active_pr_monitors().await?;
        let mut resumed = 0;
        for mut monitor in monitors {
            // Owner deleted, session row gone, or soft-retired — the
            // retire-time sweep ([`Services::cancel_agent_pr_monitors`])
            // could have been missed by a crash window.
            let owner_gone = match self
                .store
                .get_agent_session_summary(&monitor.agent_id)
                .await
            {
                Ok(session) => {
                    session.status == AgentStatus::Deleted || session.retired_at.is_some()
                }
                Err(Error::NotFound(_)) => true,
                Err(e) => return Err(e),
            };
            if owner_gone {
                let now = now_iso();
                let _ = self
                    .store
                    .update_pr_monitor_state(&monitor.monitor_id, PrMonitorState::Cancelled, &now)
                    .await;
                monitor.state = PrMonitorState::Cancelled;
                monitor.updated_at = now;
                self.emit_pr_monitor_event(PR_MONITOR_CANCELLED, &monitor, None)
                    .await;
                // The cancelled monitor's open-PR signal lapses (§6.5) and
                // the last active monitor settling drops the `waiting`
                // flag (§5.1).
                self.maybe_emit_display_status_changed(&monitor.workspace_id)
                    .await;
                self.maybe_emit_waiting_changed(&monitor.workspace_id).await;
                continue;
            }
            // Upgrade path: deliver a pending set the recompute would lose.
            // Coalesced-era rows are a fixed point of the recompute and stay
            // on the normal catch-up poll, which folds downtime changes into
            // one consolidated wake.
            if !monitor.pending_changes.is_empty() && !pending_survives_recompute(&monitor) {
                self.emit_pending_changes(&monitor).await?;
            }
            self.pr_monitor_catch_up
                .lock()
                .unwrap()
                .insert(monitor.monitor_id.clone(), time::OffsetDateTime::now_utc());
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Idle-visibility deferral backstop (mirrors
    /// [`Services::resettle_owner_after_hook_terminal`](crate::Services::resettle_owner_after_hook_terminal)):
    /// after a PR monitor reaches a terminal state, re-run the
    /// deferred-completion redelivery for the owner. A completion watch on
    /// an idle owner defers while it owns active PR monitors; the
    /// wake-carrying transitions (changed/completed/FE-cancel) resolve via
    /// the owner's wake turn ending, but a terminal transition whose wake
    /// was not delivered (owner-side `ws.pr.unmonitor` of the last monitor,
    /// or a failed wake delivery) would otherwise strand the deferred watch
    /// forever. Routes through
    /// [`Services::redeliver_completion_after_queue_mutation`], whose guards
    /// make this a no-op in every other situation.
    async fn resettle_owner_after_pr_monitor_terminal(&self, monitor: &PrMonitor) {
        self.redeliver_completion_after_queue_mutation(&monitor.agent_id)
            .await;
    }

    /// Wake a monitor's owning agent via the automatic-delivery
    /// `agent.sendMessage` path (queued behind an in-flight turn, never
    /// interrupts). Best-effort: a delivery failure is logged, never
    /// propagated — the monitor's own state transition already persisted.
    ///
    /// Every wake reason (`changed` / `completed` / `cancelled`) marks a
    /// terminal-or-progressing monitor transition; `cancelled` and
    /// `completed` are terminal, so the deferral backstop runs after the
    /// delivery attempt — a FAILED wake on an idle owner whose last monitor
    /// just terminated must still settle the owner's deferred completion
    /// watches (a successful wake makes the backstop a no-op — the
    /// queued/running wake turn owns the settlement).
    async fn wake_pr_monitor_owner(&self, monitor: &PrMonitor, message: &str, reason: &str) {
        let paused_until = self.sweep_rate_limit_paused_until();
        let metadata = pr_monitor_wake_metadata(monitor, reason, paused_until.as_deref());
        if let Err(e) = self
            .deliver_wake_message(
                &monitor.workspace_id,
                &monitor.agent_id,
                message,
                Some(&metadata),
            )
            .await
        {
            tracing::warn!(
                monitor = %monitor.monitor_id.0,
                agent = %monitor.agent_id.0,
                error = %e,
                "pr monitor owner wake delivery failed"
            );
        }
        if reason == "cancelled" || reason == "completed" {
            self.resettle_owner_after_pr_monitor_terminal(monitor).await;
        }
    }

    /// Wake the FORMER owner of a monitor its parent just took over
    /// ([`PrMonitorHolder::SettledChild`]): `former` is the pre-adoption
    /// row image (still naming the child), `reason: "transferred"`, and the
    /// metadata carries `adoptedBy`. The transfer is terminal for the child
    /// — it no longer owns the monitor — so the same deferral backstop as
    /// `cancelled`/`completed` runs afterwards: a child whose last monitor
    /// just left it must settle its parent's deferred completion watch.
    async fn wake_former_owner_after_transfer(&self, former: &PrMonitor, adopter: &AgentId) {
        let label = monitor_label(former);
        let message =
            crate::harness::latest().pr_monitor_transferred_to_parent_notice(&label, &adopter.0);
        let paused_until = self.sweep_rate_limit_paused_until();
        let mut metadata = pr_monitor_wake_metadata(former, "transferred", paused_until.as_deref());
        metadata["adoptedBy"] = json!(adopter);
        if let Err(e) = self
            .deliver_wake_message(
                &former.workspace_id,
                &former.agent_id,
                &message,
                Some(&metadata),
            )
            .await
        {
            tracing::warn!(
                monitor = %former.monitor_id.0,
                agent = %former.agent_id.0,
                error = %e,
                "pr monitor former-owner transfer wake delivery failed"
            );
        }
        self.resettle_owner_after_pr_monitor_terminal(former).await;
    }

    /// Resolve the `(owner, name)` a monitor call targets: an explicit
    /// `"owner/name"` override wins, otherwise the workspace's own repo.
    async fn resolve_monitor_repo(
        &self,
        workspace_id: &WorkspaceId,
        repo: Option<String>,
    ) -> Result<(String, String)> {
        if let Some(slug) = repo {
            pr_ops::parse_repo_slug(&slug)
        } else {
            let ws = self.store.get_workspace(workspace_id).await?;
            let RepoRef { owner, name } = pr_ops::repo_of(&ws)?;
            Ok((owner, name))
        }
    }

    /// `ws.pr.monitor`: register (idempotently) a monitor and return
    /// `{ ok, monitor, requirements }` — the row the UI lists plus the
    /// freshly fetched merge-requirements checklist the model acts on.
    /// When another agent in the workspace already holds the PR's active
    /// monitor the call is REFUSED with a structured, non-error payload
    /// (`ok: false, refused: true, reason: "already-monitored"`) naming the
    /// owner, so the model can coordinate instead of retrying.
    pub(crate) async fn pr_monitor_start_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        pr_number: u64,
        repo: Option<String>,
    ) -> Result<Value> {
        let (owner, name) = self.resolve_monitor_repo(workspace_id, repo).await?;
        match self
            .pr_monitor_try_register(workspace_id, agent_id, &owner, &name, pr_number)
            .await?
        {
            PrMonitorRegistration::Registered {
                monitor,
                requirements,
                adopted_from,
            } => {
                let paused_until = self.sweep_rate_limit_paused_until();
                let mut payload = json!({
                    "ok": true,
                    "monitor": pr_monitor_wire(&monitor, paused_until.as_deref()),
                    "requirements": requirements,
                });
                if let Some(from) = adopted_from {
                    payload["adoptedFrom"] = json!(from);
                }
                Ok(payload)
            }
            PrMonitorRegistration::Refused(refusal) => Ok(refusal.to_wire()),
        }
    }

    /// `ws.pr.unmonitor`: cancel the caller's own active monitor on
    /// `(repo, pr_number)`. Unknown/foreign PRs surface as `NotFound` naming
    /// the label, and the owner is never self-woken.
    pub(crate) async fn pr_monitor_stop_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        pr_number: u64,
        repo: Option<String>,
    ) -> Result<Value> {
        let (owner, name) = self.resolve_monitor_repo(workspace_id, repo).await?;
        let existing = self
            .store
            .find_active_pr_monitor(agent_id, &owner, &name, pr_number.cast_signed())
            .await?
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "pr.unmonitor: no active monitor on {owner}/{name}#{pr_number}"
                ))
            })?;
        let monitor = self
            .pr_monitor_cancel(workspace_id, &existing.monitor_id, Some(agent_id))
            .await?;
        let paused_until = self.sweep_rate_limit_paused_until();
        Ok(json!({ "ok": true, "monitor": pr_monitor_wire(&monitor, paused_until.as_deref()) }))
    }

    /// `ws.pr.monitors` / wire `prMonitor.list`: `{ monitors: [...] }`.
    /// `agent_id` narrows to one owner (the MCP caller's own view); `None` is
    /// the workspace-wide FE view.
    pub(crate) async fn pr_monitor_list_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: Option<&AgentId>,
    ) -> Result<Value> {
        let monitors = match agent_id {
            Some(a) => self.pr_monitors_for_agent(a).await?,
            None => self.pr_monitors_for_workspace(workspace_id).await?,
        };
        let paused_until = self.sweep_rate_limit_paused_until();
        let monitors: Vec<Value> = monitors
            .into_iter()
            .filter(|m| &m.workspace_id == workspace_id)
            .map(|m| pr_monitor_wire(&m, paused_until.as_deref()))
            .collect();
        Ok(json!({ "monitors": monitors }))
    }

    /// Wire `prMonitor.cancel`: the FE cancel path — any monitor in the
    /// workspace, and the owning agent is notified.
    pub(crate) async fn pr_monitor_cancel_by_id_op(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &PrMonitorId,
    ) -> Result<Value> {
        let monitor = self
            .pr_monitor_cancel(workspace_id, monitor_id, None)
            .await?;
        let paused_until = self.sweep_rate_limit_paused_until();
        Ok(json!({ "ok": true, "monitor": pr_monitor_wire(&monitor, paused_until.as_deref()) }))
    }

    /// Wire `prMonitor.flush`: emit the pending debounced changes now.
    /// `flushed: false` when nothing was pending (a no-op, not an error).
    /// With `check: true`, an immediate on-demand re-poll of the monitor
    /// runs first, so the flush covers changes the loop has not seen yet.
    pub(crate) async fn pr_monitor_flush_op(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &PrMonitorId,
        check: bool,
    ) -> Result<Value> {
        let flushed = if check {
            self.pr_monitor_check_and_flush(workspace_id, monitor_id)
                .await?
        } else {
            self.pr_monitor_flush(workspace_id, monitor_id).await?
        };
        Ok(json!({ "ok": true, "flushed": flushed }))
    }

    /// Idle-visibility deferral (mirrors
    /// [`Services::active_hooks_for_agent`](crate::Services::active_hooks_for_agent)):
    /// the caller's ACTIVE PR monitors, oldest first. Empty when the agent
    /// owns no active monitor; a store failure is logged and reads as empty
    /// (visibility is best-effort and must never block an idle emit or wake
    /// delivery).
    pub(crate) async fn active_pr_monitors_for_agent(&self, agent_id: &AgentId) -> Vec<PrMonitor> {
        match self.store.list_active_pr_monitors_by_agent(agent_id).await {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "active-pr-monitors lookup failed; pr-monitor-waiting reads as empty"
                );
                Vec::new()
            }
        }
    }

    /// Workspace-batched variant of
    /// [`active_pr_monitors_for_agent`](Self::active_pr_monitors_for_agent)
    /// for `agent.list`: one store query for the whole workspace, grouped by
    /// owning agent id as light `waitingOnPrMonitors` entries (agents with no
    /// active monitor are absent). A store failure is logged and reads as
    /// empty, mirroring
    /// [`Services::active_hooks_by_agent`](crate::Services::active_hooks_by_agent).
    pub(crate) async fn active_pr_monitors_by_agent(
        &self,
        workspace_id: &WorkspaceId,
    ) -> HashMap<String, Vec<Value>> {
        let monitors = match self
            .store
            .list_active_pr_monitors_by_workspace(workspace_id)
            .await
        {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "active-pr-monitors workspace lookup failed; waitingOnPrMonitors reads as empty"
                );
                return HashMap::new();
            }
        };
        let mut by_agent: HashMap<String, Vec<Value>> = HashMap::new();
        for m in monitors {
            let agent = m.agent_id.0.clone();
            by_agent
                .entry(agent)
                .or_default()
                .push(waiting_on_pr_monitors_entry(&m));
        }
        by_agent
    }

    /// Stamp `waitingOnPrMonitors` onto an `agent:idle`-style event `data`
    /// object when `agent_id` owns at least one active PR monitor (the field
    /// is omitted — never `[]` — otherwise, and an existing stamp is left
    /// untouched). Returns the stamped list (empty when nothing was stamped
    /// and no stamp was present). Mirrors
    /// [`Services::annotate_waiting_on_hooks`](crate::Services::annotate_waiting_on_hooks).
    pub(crate) async fn annotate_waiting_on_pr_monitors(
        &self,
        agent_id: &AgentId,
        data: &mut Value,
    ) -> Vec<Value> {
        if let Some(existing) = data.get("waitingOnPrMonitors").and_then(Value::as_array) {
            return existing.clone();
        }
        let monitors = self.active_pr_monitors_for_agent(agent_id).await;
        let entries: Vec<Value> = monitors.iter().map(waiting_on_pr_monitors_entry).collect();
        if !entries.is_empty() {
            if let Some(obj) = data.as_object_mut() {
                obj.insert(
                    "waitingOnPrMonitors".to_string(),
                    Value::Array(entries.clone()),
                );
            }
        }
        entries
    }

    /// The per-turn snapshot's `prMonitors` field: one
    /// `"<owner>/<name>#<number>"` label per ACTIVE monitor this agent owns,
    /// suffixed with `" (changes pending)"` while a debounced emit is
    /// accumulating. O(this agent's monitors) — one indexed per-agent read,
    /// no snapshot parsing. Best-effort: a store failure reads as empty so a
    /// snapshot build never fails on it.
    pub(crate) async fn active_pr_monitor_labels(&self, agent_id: &AgentId) -> Vec<String> {
        let monitors = match self.store.list_pr_monitors_by_agent(agent_id).await {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "pr-monitor snapshot lookup failed; prMonitors reads as empty"
                );
                return Vec::new();
            }
        };
        monitors
            .into_iter()
            .filter(|m| m.state == PrMonitorState::Active)
            .map(|m| {
                let label = monitor_label(&m);
                if m.pending_changes.is_empty() {
                    label
                } else {
                    format!("{label} (changes pending)")
                }
            })
            .collect()
    }

    /// Emit one `prMonitor:*` lifecycle event with the canonical
    /// `{ workspaceId, agentId, monitorId, repo, prNumber, state }` payload
    /// plus any event-specific `extra` fields.
    async fn emit_pr_monitor_event(
        &self,
        event_type: &str,
        monitor: &PrMonitor,
        extra: Option<Value>,
    ) {
        let mut data = json!({
            "workspaceId": monitor.workspace_id,
            "agentId": monitor.agent_id,
            "monitorId": monitor.monitor_id,
            "repo": format!("{}/{}", monitor.repo_owner, monitor.repo_name),
            "prNumber": monitor.pr_number,
            "state": monitor.state,
        });
        if let (Some(obj), Some(Value::Object(extra))) = (data.as_object_mut(), extra) {
            obj.extend(extra);
        }
        let event = NewEvent {
            workspace_id: monitor.workspace_id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            actor: system_actor(),
            session_id: Some(monitor.agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        publish_event(self.event_bus.as_ref(), event).await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use async_trait::async_trait;
    use intent_core::{
        AgentSession, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use intent_sourcecontrol::{
        AuthStatus, Branch, BranchRules, CheckRun, CheckState, Comment, CommentAnchor, Issue,
        IssueQuery, MergeMethod, MergeOptions, MergeOutcome, MergeRequirementSignals, Mergeability,
        NewPullRequest, Page, PageParams, PrObservation, PrPatch, PrQuery, PrState, PullRequest,
        RateLimitStatus, Repo, Review, ReviewComment, ReviewDecision, ReviewThread,
        ReviewThreadComment, ReviewThreadTally, ReviewVerdict, RollupCheck, RollupCheckKind,
        ScCapabilities, UserIdentity,
    };
    use intent_store::Store;

    use super::*;
    use crate::events::EventBus;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-prmon-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    /// Mutable forge state one test can advance between polls.
    #[derive(Clone)]
    #[expect(clippy::struct_excessive_bools)]
    struct ForgeState {
        pr_state: PrState,
        draft: bool,
        head_sha: String,
        mergeable: Option<bool>,
        mergeable_state: String,
        conversation_comments: usize,
        approvals: Vec<String>,
        threads: Vec<ReviewThread>,
        checks: Vec<RollupCheck>,
        merge_queue_removal: Option<intent_sourcecontrol::MergeQueueRemoval>,
        fail_get_pr: bool,
        fail_list_comments: bool,
        fail_merge_requirements: bool,
        /// `list_reviews` fails with an ordinary (degrading) error.
        fail_list_reviews: bool,
        /// `get_review_threads` fails with an ordinary (degrading) error.
        fail_get_review_threads: bool,
        /// `get_pr` fails with the forge's quota-exhausted error.
        rate_limit_get_pr: bool,
        /// `list_reviews` (a checklist sub-read) fails with the forge's
        /// quota-exhausted error while `get_pr` still answers.
        rate_limit_list_reviews: bool,
        /// `list_comments` (the conversation count) fails with the forge's
        /// quota-exhausted error while every other read still answers.
        rate_limit_list_comments: bool,
        /// PR number whose `get_pr` pends forever (hung-connection regression).
        hang_get_pr: Option<u64>,
        /// The forge-reported quota reset (unix seconds) answered by
        /// `rate_limit_status`; `None` is the host-without-signal default.
        rate_limit_reset_at: Option<u64>,
        /// The `remaining` / `limit` answered by `rate_limit_status`;
        /// `None` is the host-without-signal default (no early lift).
        rate_limit_remaining: Option<u64>,
        rate_limit_limit: Option<u64>,
        /// `rate_limit_status` itself fails (the free probe erroring).
        fail_rate_limit_status: bool,
        /// A real RFC 3339 `updatedAt` for the PR record, overriding the
        /// opaque `rev-N` stand-in when a test needs a comparable timestamp.
        updated_at: Option<String>,
        /// How `pr_observation` answers; `None` (the default) is a host
        /// without a folded read, so every existing test keeps exercising
        /// the per-signal reads.
        folded: Option<FoldedRead>,
        /// Stands in for the forge's `updatedAt`: bumped by every
        /// [`StubForge::edit`] (as GitHub bumps it on reviews, comments,
        /// threads and pushes), left alone by [`StubForge::edit_quiet`] (as
        /// GitHub leaves it on check-run and merge-queue movement).
        revision: u64,
    }

    /// The ceiling the GitHub adapter's reads share, mirrored by the stub so
    /// the two fetch paths can be checked for parity past it: `list_comments`
    /// is one `per_page=100` page, a thread carries `comments(first: 100)`,
    /// and the folded `totalCount`s saturate to match (`observed_count`).
    const FORGE_PAGE_CEILING: usize = 100;

    /// The threads as a read returns them: each carrying at most
    /// [`FORGE_PAGE_CEILING`] comments.
    fn thread_page(threads: &[ReviewThread]) -> Vec<ReviewThread> {
        threads
            .iter()
            .map(|t| {
                let mut t = t.clone();
                t.comments.truncate(FORGE_PAGE_CEILING);
                t
            })
            .collect()
    }

    /// The stub's folded `pr_observation` answer, built from the same
    /// [`ForgeState`] the per-signal reads serve.
    #[derive(Clone, Copy, Default)]
    #[expect(clippy::struct_excessive_bools)]
    struct FoldedRead {
        /// The observation fails with an ordinary (degrading) error.
        fail: bool,
        /// The observation fails with the forge's quota-exhausted error.
        rate_limited: bool,
        /// The PR outgrew the reviews window (`reviews: None`).
        overflow_reviews: bool,
        /// The PR outgrew the review-threads window (`threads: None`).
        overflow_threads: bool,
    }

    impl Default for ForgeState {
        fn default() -> Self {
            Self {
                revision: 0,
                pr_state: PrState::Open,
                draft: false,
                head_sha: "aaaaaaaa".into(),
                mergeable: Some(true),
                mergeable_state: "clean".into(),
                conversation_comments: 0,
                approvals: vec![],
                threads: vec![],
                checks: vec![RollupCheck {
                    name: "build".into(),
                    kind: RollupCheckKind::CheckRun,
                    state: CheckState::Pending,
                    is_required: true,
                    url: None,
                    started_at: None,
                }],
                merge_queue_removal: None,
                fail_get_pr: false,
                fail_list_comments: false,
                fail_merge_requirements: false,
                fail_list_reviews: false,
                fail_get_review_threads: false,
                rate_limit_get_pr: false,
                rate_limit_list_reviews: false,
                rate_limit_list_comments: false,
                hang_get_pr: None,
                rate_limit_reset_at: None,
                rate_limit_remaining: None,
                rate_limit_limit: None,
                fail_rate_limit_status: false,
                updated_at: None,
                folded: None,
            }
        }
    }

    impl ForgeState {
        /// The PR record `get_pr` and the folded observation both serve.
        fn pr_record(&self, number: u64) -> PullRequest {
            PullRequest {
                number,
                url: format!("https://github.com/o/r/pull/{number}"),
                title: "Add thing".into(),
                body: None,
                state: self.pr_state,
                draft: self.draft,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                author: "octocat".into(),
                mergeable: self.mergeable,
                mergeable_state: Some(self.mergeable_state.clone()),
                head_sha: Some(self.head_sha.clone()),
                created_at: String::new(),
                updated_at: self
                    .updated_at
                    .clone()
                    .unwrap_or_else(|| format!("rev-{}", self.revision)),
            }
        }

        /// The reviews `list_reviews` and the folded observation both serve.
        fn reviews(&self) -> Vec<Review> {
            self.approvals
                .iter()
                .map(|a| Review {
                    author: a.clone(),
                    verdict: ReviewVerdict::Approve,
                    body: None,
                    submitted_at: "2026-01-01T00:00:00Z".into(),
                })
                .collect()
        }

        /// The probe signals `merge_requirements` and the folded observation
        /// both serve — minus the branch rules, which `merge_requirements`
        /// folds in and the observation leaves to `branch_rules`.
        fn signals(&self) -> MergeRequirementSignals {
            MergeRequirementSignals {
                merge_state_status: Some(self.mergeable_state.to_uppercase()),
                review_decision: (!self.approvals.is_empty()).then_some(ReviewDecision::Approved),
                checks: self.checks.clone(),
                checks_known: true,
                branch_rules: None,
                is_in_merge_queue: None,
                merge_queue_removal: self.merge_queue_removal.clone(),
            }
        }
    }

    /// The base branch's rules every stub read reports.
    fn stub_branch_rules() -> BranchRules {
        BranchRules {
            required_approving_review_count: Some(1),
            required_conversation_resolution: Some(true),
            required_status_checks: vec!["build".into()],
        }
    }

    /// Side effect run at the start of every `get_pr` (with the PR number),
    /// standing in for work that happens elsewhere in the daemon while a
    /// sweep is mid-flight — e.g. a sibling sweep pausing the shared gate.
    type GetPrHook = Box<dyn Fn(u64) + Send + Sync>;

    #[derive(Clone)]
    struct StubForge {
        state: Arc<Mutex<ForgeState>>,
        get_pr_calls: Arc<std::sync::atomic::AtomicUsize>,
        get_pr_numbers: Arc<Mutex<Vec<u64>>>,
        on_get_pr: Arc<Mutex<Option<GetPrHook>>>,
        /// Calls per sub-read method name (the reads a fingerprint-unchanged
        /// poll is expected to skip).
        sub_fetch_calls: Arc<Mutex<HashMap<&'static str, usize>>>,
    }

    impl StubForge {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(ForgeState::default())),
                get_pr_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                get_pr_numbers: Arc::new(Mutex::new(Vec::new())),
                on_get_pr: Arc::new(Mutex::new(None)),
                sub_fetch_calls: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        /// Mutate the forge state AND bump the PR's `updatedAt` stand-in.
        fn edit(&self, f: impl FnOnce(&mut ForgeState)) {
            let mut s = self.state.lock().unwrap();
            f(&mut s);
            s.revision += 1;
        }

        /// Mutate the forge state WITHOUT bumping `updatedAt` — check-run
        /// and merge-queue movement, which the forge's PR record does not
        /// reflect.
        fn edit_quiet(&self, f: impl FnOnce(&mut ForgeState)) {
            f(&mut self.state.lock().unwrap());
        }

        fn count_sub_fetch(&self, method: &'static str) {
            *self
                .sub_fetch_calls
                .lock()
                .unwrap()
                .entry(method)
                .or_default() += 1;
        }

        /// Calls to one sub-read method so far.
        fn sub_fetches(&self, method: &'static str) -> usize {
            self.sub_fetch_calls
                .lock()
                .unwrap()
                .get(method)
                .copied()
                .unwrap_or_default()
        }

        /// Install (or clear) the per-`get_pr` side effect.
        fn set_on_get_pr(&self, hook: Option<GetPrHook>) {
            *self.on_get_pr.lock().unwrap() = hook;
        }

        /// Snapshot-fetch attempts so far: `get_pr` is called exactly once
        /// per [`fetch_shared_snapshot`] attempt (successful or not).
        fn fetches(&self) -> usize {
            self.get_pr_calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Drain the PR numbers fetched since the last drain, in call order.
        fn take_fetched_numbers(&self) -> Vec<u64> {
            std::mem::take(&mut *self.get_pr_numbers.lock().unwrap())
        }
    }

    fn unsupported<T>(what: &str) -> intent_sourcecontrol::Result<T> {
        Err(intent_sourcecontrol::Error::Unsupported(what.to_string()))
    }

    #[async_trait]
    impl SourceControl for StubForge {
        fn provider_id(&self) -> &'static str {
            "stub"
        }
        fn capabilities(&self) -> ScCapabilities {
            ScCapabilities {
                draft_prs: true,
                squash_merge: true,
                rebase_merge: true,
                review_required_changes: true,
                check_runs: true,
                issues: true,
            }
        }
        async fn check_auth(&self) -> intent_sourcecontrol::Result<AuthStatus> {
            Ok(AuthStatus {
                authenticated: true,
                login: Some("octocat".into()),
                scopes: vec![],
            })
        }
        async fn get_user(&self) -> intent_sourcecontrol::Result<UserIdentity> {
            unsupported("get_user")
        }
        async fn list_repos(&self, _: PageParams) -> intent_sourcecontrol::Result<Page<Repo>> {
            unsupported("list_repos")
        }
        async fn search_repos(
            &self,
            _: &str,
            _: PageParams,
        ) -> intent_sourcecontrol::Result<Page<Repo>> {
            unsupported("search_repos")
        }
        async fn get_repo(&self, _: &str, _: &str) -> intent_sourcecontrol::Result<Repo> {
            unsupported("get_repo")
        }
        async fn list_remote_branches(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: PageParams,
        ) -> intent_sourcecontrol::Result<Page<Branch>> {
            unsupported("list_remote_branches")
        }
        async fn get_file_content(
            &self,
            _: &RepoRef,
            _: &str,
            _: Option<&str>,
        ) -> intent_sourcecontrol::Result<Option<String>> {
            Ok(None)
        }
        async fn create_pr(
            &self,
            _: &RepoRef,
            _: NewPullRequest,
        ) -> intent_sourcecontrol::Result<PullRequest> {
            unsupported("create_pr")
        }
        async fn rate_limit_status(&self) -> intent_sourcecontrol::Result<RateLimitStatus> {
            self.count_sub_fetch("rate_limit_status");
            let s = self.state.lock().unwrap();
            if s.fail_rate_limit_status {
                return Err(intent_sourcecontrol::Error::Api(
                    "rate_limit probe down".into(),
                ));
            }
            Ok(RateLimitStatus {
                reset_at: s.rate_limit_reset_at,
                remaining: s.rate_limit_remaining,
                limit: s.rate_limit_limit,
            })
        }
        async fn get_pr(
            &self,
            _: &RepoRef,
            number: u64,
        ) -> intent_sourcecontrol::Result<PullRequest> {
            self.get_pr_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.get_pr_numbers.lock().unwrap().push(number);
            if let Some(hook) = self.on_get_pr.lock().unwrap().as_ref() {
                hook(number);
            }
            let s = self.state.lock().unwrap().clone();
            if s.hang_get_pr == Some(number) {
                // A TCP connection that went dark: the future never resolves.
                std::future::pending::<()>().await;
            }
            if s.fail_get_pr {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "forge down".into(),
                ));
            }
            if s.rate_limit_get_pr {
                return Err(intent_sourcecontrol::Error::RateLimited(
                    "API rate limit exceeded".into(),
                ));
            }
            Ok(s.pr_record(number))
        }
        async fn list_prs(
            &self,
            _: &RepoRef,
            _: PrQuery,
        ) -> intent_sourcecontrol::Result<Page<PullRequest>> {
            // Empty page (not `Unsupported`): the terminal-refresh path runs
            // relink discovery for merged/closed linked PRs, and an empty
            // page exercises the clean "no matching open PR" branch instead
            // of the discovery-failure degrade arm.
            Ok(Page {
                items: vec![],
                next_cursor: None,
            })
        }
        async fn update_pr(
            &self,
            _: &RepoRef,
            _: u64,
            _: PrPatch,
        ) -> intent_sourcecontrol::Result<PullRequest> {
            unsupported("update_pr")
        }
        async fn merge_pr(
            &self,
            _: &RepoRef,
            _: u64,
            _: MergeMethod,
            _: MergeOptions,
        ) -> intent_sourcecontrol::Result<MergeOutcome> {
            unsupported("merge_pr")
        }
        async fn mergeability(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<Mergeability> {
            unsupported("mergeability")
        }
        async fn update_branch(&self, _: &RepoRef, _: u64) -> intent_sourcecontrol::Result<()> {
            unsupported("update_branch")
        }
        async fn submit_review(
            &self,
            _: &RepoRef,
            _: u64,
            _: ReviewVerdict,
            _: Option<String>,
        ) -> intent_sourcecontrol::Result<Review> {
            unsupported("submit_review")
        }
        async fn list_reviews(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<Vec<Review>> {
            self.count_sub_fetch("list_reviews");
            let s = self.state.lock().unwrap();
            if s.rate_limit_list_reviews {
                return Err(intent_sourcecontrol::Error::RateLimited(
                    "API rate limit exceeded".into(),
                ));
            }
            if s.fail_list_reviews {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "reviews down".into(),
                ));
            }
            Ok(s.reviews())
        }
        async fn merge_requirements(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<MergeRequirementSignals> {
            self.count_sub_fetch("merge_requirements");
            let s = self.state.lock().unwrap().clone();
            if s.fail_merge_requirements {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "probe down".into(),
                ));
            }
            let mut signals = s.signals();
            signals.branch_rules = Some(stub_branch_rules());
            Ok(signals)
        }
        async fn branch_rules(
            &self,
            _: &RepoRef,
            _: &str,
        ) -> intent_sourcecontrol::Result<BranchRules> {
            self.count_sub_fetch("branch_rules");
            Ok(stub_branch_rules())
        }
        async fn pr_observation(
            &self,
            _: &RepoRef,
            number: u64,
        ) -> intent_sourcecontrol::Result<Option<PrObservation>> {
            let s = self.state.lock().unwrap().clone();
            let Some(folded) = s.folded else {
                return Ok(None);
            };
            self.count_sub_fetch("pr_observation");
            if folded.rate_limited {
                return Err(intent_sourcecontrol::Error::RateLimited(
                    "API rate limit exceeded".into(),
                ));
            }
            if folded.fail {
                return Err(intent_sourcecontrol::Error::Api("folded read down".into()));
            }
            let (review_comment_count, unresolved) =
                crate::pr_ops::count_thread_comments(&thread_page(&s.threads));
            Ok(Some(PrObservation {
                pr: s.pr_record(number),
                signals: s.signals(),
                reviews: (!folded.overflow_reviews).then(|| s.reviews()),
                threads: (!folded.overflow_threads).then_some(ReviewThreadTally {
                    review_comment_count,
                    unresolved,
                }),
                conversation_count: i64::try_from(s.conversation_comments.min(FORGE_PAGE_CEILING))
                    .unwrap(),
            }))
        }
        async fn list_comments(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<Vec<Comment>> {
            self.count_sub_fetch("list_comments");
            let (n, fail, rate_limited) = {
                let s = self.state.lock().unwrap();
                (
                    s.conversation_comments,
                    s.fail_list_comments,
                    s.rate_limit_list_comments,
                )
            };
            if rate_limited {
                return Err(intent_sourcecontrol::Error::RateLimited(
                    "API rate limit exceeded".into(),
                ));
            }
            if fail {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "comments down".into(),
                ));
            }
            Ok((0..n.min(FORGE_PAGE_CEILING))
                .map(|i| Comment {
                    id: i.to_string(),
                    author: "octocat".into(),
                    body: "hi".into(),
                    path: None,
                    line: None,
                    created_at: String::new(),
                    url: None,
                })
                .collect())
        }
        async fn add_comment(
            &self,
            _: &RepoRef,
            _: u64,
            _: &str,
            _: Option<CommentAnchor>,
        ) -> intent_sourcecontrol::Result<Comment> {
            unsupported("add_comment")
        }
        async fn review_decision(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<Option<ReviewDecision>> {
            self.count_sub_fetch("review_decision");
            Ok(None)
        }
        async fn list_review_comments(
            &self,
            _: &RepoRef,
            _: u64,
            _: PageParams,
        ) -> intent_sourcecontrol::Result<Page<ReviewComment>> {
            self.count_sub_fetch("list_review_comments");
            unsupported("list_review_comments")
        }
        async fn reply_to_review_comment(
            &self,
            _: &RepoRef,
            _: u64,
            _: u64,
            _: &str,
        ) -> intent_sourcecontrol::Result<ReviewComment> {
            unsupported("reply_to_review_comment")
        }
        async fn get_review_threads(
            &self,
            _: &RepoRef,
            _: u64,
            _: PageParams,
        ) -> intent_sourcecontrol::Result<Page<ReviewThread>> {
            self.count_sub_fetch("get_review_threads");
            let s = self.state.lock().unwrap();
            if s.fail_get_review_threads {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "threads down".into(),
                ));
            }
            Ok(Page {
                items: thread_page(&s.threads),
                next_cursor: None,
            })
        }
        async fn resolve_thread(&self, _: &str) -> intent_sourcecontrol::Result<bool> {
            unsupported("resolve_thread")
        }
        async fn unresolve_thread(&self, _: &str) -> intent_sourcecontrol::Result<bool> {
            unsupported("unresolve_thread")
        }
        async fn check_runs(
            &self,
            _: &RepoRef,
            _: &str,
        ) -> intent_sourcecontrol::Result<Vec<CheckRun>> {
            self.count_sub_fetch("check_runs");
            Ok(Vec::new())
        }
        async fn create_issue(
            &self,
            _: &RepoRef,
            _: &str,
            _: Option<&str>,
        ) -> intent_sourcecontrol::Result<Issue> {
            unsupported("create_issue")
        }
        async fn get_issue(&self, _: &RepoRef, _: u64) -> intent_sourcecontrol::Result<Issue> {
            unsupported("get_issue")
        }
        async fn list_issues(
            &self,
            _: &RepoRef,
            _: IssueQuery,
        ) -> intent_sourcecontrol::Result<Page<Issue>> {
            unsupported("list_issues")
        }
    }

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: Some("o".into()),
            repository_name: Some("r".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            browser_client_id: None,
            pull_requests_total: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
            membership: None,
        }
    }

    fn agent(ws: &WorkspaceId, id: &str) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Owner".to_string(),
            name_explicitly_set: true,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            notifications_muted: false,
        }
    }

    /// Store + Services (event bus wired, stub forge injected) + workspace +
    /// owning agent. The debounce defaults to its floor so a test can drive
    /// coalescing without sleeping a minute.
    async fn setup() -> (
        TempDb,
        tempfile::TempDir,
        Services,
        StubForge,
        WorkspaceId,
        AgentId,
    ) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let owner = AgentId::from("agent-prmon");
        store
            .insert_agent_session(&agent(&ws, "agent-prmon"))
            .await
            .expect("agent");
        let bus = EventBus::new(store.clone());
        let forge = StubForge::new();
        let root = tempfile::tempdir().expect("temp workspaces root");
        let services = Services::new(store)
            .with_event_bus(bus)
            .with_workspaces_root(root.path().to_path_buf())
            .with_source_control(Arc::new(forge.clone()));
        (tmp, root, services, forge, ws, owner)
    }

    /// Register a monitor on PR 42 for the setup fixture's owner.
    async fn register(svc: &Services, ws: &WorkspaceId, owner: &AgentId) -> PrMonitor {
        svc.pr_monitor_register(ws, owner, "o", "r", 42)
            .await
            .expect("register")
            .0
    }

    /// The owner's persisted messages, serialized (wake assertions).
    async fn owner_messages(svc: &Services, owner: &AgentId) -> String {
        let session = svc.store().get_agent_session(owner).await.unwrap();
        serde_json::to_string(&session.messages).unwrap()
    }

    /// A baseline snapshot the diff tests mutate one field at a time.
    fn snapshot(f: impl FnOnce(&mut PrMonitorSnapshot)) -> PrMonitorSnapshot {
        let mut s = PrMonitorSnapshot {
            title: "Add thing".into(),
            url: "https://github.com/o/r/pull/42".into(),
            head_sha: Some("aaaaaaaa".into()),
            conversation_count: 1,
            review_comment_count: 2,
            requirements: MergeRequirements {
                state: "open".into(),
                is_draft: false,
                has_conflicts: false,
                is_behind: false,
                mergeable: Some(true),
                checks: pr_ops::MergeRequirementsChecks {
                    total: 1,
                    passed: 0,
                    failed: 0,
                    pending: 1,
                    items: vec![pr_ops::MergeRequirementCheck {
                        name: "build".into(),
                        status: "pending".into(),
                        required: true,
                        url: None,
                    }],
                    failing_required: vec![],
                    pending_required: vec!["build".into()],
                    required_known: true,
                },
                approvals: pr_ops::MergeRequirementsApprovals {
                    decision: "review_required".into(),
                    have: 0,
                    needed: Some(1),
                    changes_requested: 0,
                },
                threads: pr_ops::MergeRequirementsThreads {
                    unresolved: Some(1),
                    resolution_required: Some(true),
                },
                merge_state_status: Some("BLOCKED".into()),
                merge_blocked_reason: None,
                rules_known: true,
                is_in_merge_queue: None,
                merge_queue_ejection: None,
            },
            ejection_tracked: true,
            observed_at: None,
        };
        f(&mut s);
        s
    }

    /// Clear every merge-requirements blocker on the [`snapshot`] fixture —
    /// the truly-mergeable checklist shape [`requirements_ready`] accepts.
    fn ready_requirements(req: &mut MergeRequirements) {
        req.checks.passed = 1;
        req.checks.pending = 0;
        req.checks.items[0].status = "passed".into();
        req.checks.pending_required.clear();
        req.approvals.decision = "approved".into();
        req.approvals.have = 1;
        req.threads.unresolved = Some(0);
        req.merge_state_status = Some("CLEAN".into());
    }

    /// A merge-queued PR is being handled by the queue, not awaiting agent
    /// action: even a CLEAN, fully clear checklist must not read as ready,
    /// and the queued flag projects into the `lastSnapshot` wire summary
    /// (presence-detected — absent when not queued or unknown).
    #[test]
    fn a_queued_pr_is_not_ready_and_projects_into_last_snapshot() {
        let mut s = snapshot(|s| ready_requirements(&mut s.requirements));
        assert!(requirements_ready(&s.requirements), "fixture starts ready");
        s.requirements.is_in_merge_queue = Some(true);
        assert!(
            !requirements_ready(&s.requirements),
            "a queued PR never reads ready"
        );

        let ts = "2026-01-01T00:00:00Z".to_string();
        let mut m = PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: WorkspaceId::from("ws-1"),
            agent_id: AgentId::from("agent-1"),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: Some(serde_json::to_string(&s).unwrap()),
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: ts.clone(),
            updated_at: ts,
        };
        let wire = pr_monitor_wire(&m, None);
        assert_eq!(wire["lastSnapshot"]["isInMergeQueue"], json!(true));

        // Not queued / unknown: the key is absent, never null.
        s.requirements.is_in_merge_queue = None;
        m.last_snapshot = Some(serde_json::to_string(&s).unwrap());
        let wire = pr_monitor_wire(&m, None);
        assert!(wire["lastSnapshot"].get("isInMergeQueue").is_none());
        // Same presence rule for the ejection event.
        assert!(wire["lastSnapshot"].get("mergeQueueEjection").is_none());
        s.requirements.merge_queue_ejection = Some(pr_ops::MergeQueueEjection {
            at: "2026-01-02T03:04:05Z".into(),
            reason: Some("failed_checks".into()),
        });
        m.last_snapshot = Some(serde_json::to_string(&s).unwrap());
        let wire = pr_monitor_wire(&m, None);
        assert_eq!(
            wire["lastSnapshot"]["mergeQueueEjection"],
            json!({ "at": "2026-01-02T03:04:05Z", "reason": "failed_checks" })
        );
    }

    /// `pausedUntil` follows the same presence rule: an ACTIVE row carries
    /// the global rate-limit pause deadline while the gate is closed, and
    /// the key is absent (never null) when the gate is open or the row is
    /// terminal — a completed monitor is not being polled either way.
    #[test]
    fn paused_until_projects_only_onto_active_rows_while_paused() {
        let ts = "2026-01-01T00:00:00Z".to_string();
        let mut m = PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: WorkspaceId::from("ws-1"),
            agent_id: AgentId::from("agent-1"),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: None,
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: Some(
                "rate limited; PR monitor polling paused until 2026-09-17T02:39:15Z".into(),
            ),
            created_at: ts.clone(),
            updated_at: ts,
        };
        let wire = pr_monitor_wire(&m, Some("2026-09-17T02:39:15Z"));
        assert_eq!(wire["pausedUntil"], json!("2026-09-17T02:39:15Z"));
        assert_eq!(
            wire["lastError"],
            json!("rate limited; PR monitor polling paused until 2026-09-17T02:39:15Z")
        );

        let wire = pr_monitor_wire(&m, None);
        assert!(wire.get("pausedUntil").is_none(), "{wire}");

        m.state = PrMonitorState::Completed;
        let wire = pr_monitor_wire(&m, Some("2026-09-17T02:39:15Z"));
        assert!(wire.get("pausedUntil").is_none(), "{wire}");
    }

    /// An unreadable thread count (`threads.unresolved == None`) is unknown,
    /// not clear: it never promotes to ready while the branch requires
    /// resolution, reads ready when resolution is not required (the count is
    /// irrelevant to merging), and the `lastSnapshot` summary omits the key
    /// rather than serving `null` or `0`.
    #[test]
    fn an_unknown_thread_count_is_not_ready_when_resolution_is_required() {
        let mut s = snapshot(|s| ready_requirements(&mut s.requirements));
        assert!(requirements_ready(&s.requirements), "fixture starts ready");
        s.requirements.threads.unresolved = None;
        assert_eq!(s.requirements.threads.resolution_required, Some(true));
        assert!(
            !requirements_ready(&s.requirements),
            "unknown resolution state never reads ready while resolution is required"
        );
        s.requirements.threads.resolution_required = None;
        assert!(
            requirements_ready(&s.requirements),
            "unknown resolution state does not block when rules are unreadable"
        );
        s.requirements.threads.resolution_required = Some(false);
        assert!(requirements_ready(&s.requirements));

        s.requirements.threads.resolution_required = Some(true);
        let ts = "2026-01-01T00:00:00Z".to_string();
        let mut m = PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: WorkspaceId::from("ws-1"),
            agent_id: AgentId::from("agent-1"),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: Some(serde_json::to_string(&s).unwrap()),
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: ts.clone(),
            updated_at: ts,
        };
        let wire = pr_monitor_wire(&m, None);
        assert!(wire["lastSnapshot"]["threads"].get("unresolved").is_none());
        assert_eq!(
            wire["lastSnapshot"]["threads"]["resolutionRequired"],
            json!(true)
        );

        s.requirements.threads.unresolved = Some(3);
        m.last_snapshot = Some(serde_json::to_string(&s).unwrap());
        let wire = pr_monitor_wire(&m, None);
        assert_eq!(wire["lastSnapshot"]["threads"]["unresolved"], json!(3));
    }

    /// A thread-count delta is never reported when either side is unknown.
    /// Baseline `Some(1)` → degraded `None` → recovered `Some(1)`: the
    /// degraded diff carries exactly one readability line and no
    /// `thread(s)` delta, the recovery diff a neutral "readable again" line
    /// (no `thread(s) resolved` verb), the baseline→recovered diff (same
    /// count) no thread line at all, and unknown→unknown is silent.
    #[test]
    fn diff_never_fabricates_a_thread_delta_across_an_unknown_count() {
        let baseline = snapshot(|_| {});
        assert_eq!(baseline.requirements.threads.unresolved, Some(1));
        let degraded = snapshot(|s| s.requirements.threads.unresolved = None);
        let recovered = snapshot(|_| {});

        let changes = diff_snapshots(&baseline, &degraded);
        assert!(
            changes.iter().all(|c| !c.starts_with("thread(s)")),
            "no thread delta on degradation: {changes:?}"
        );
        assert_eq!(
            changes
                .iter()
                .filter(|c| c.as_str() == "review threads unreadable (resolution state unavailable)")
                .count(),
            1,
            "exactly one readability line: {changes:?}"
        );

        let changes = diff_snapshots(&degraded, &recovered);
        assert!(
            changes.iter().all(|c| !c.starts_with("thread(s)")),
            "no thread delta on recovery: {changes:?}"
        );
        assert!(changes
            .iter()
            .any(|c| c == "review threads readable again: 1 unresolved"));

        let changes = diff_snapshots(&baseline, &recovered);
        assert!(
            changes.iter().all(|c| !c.contains("thread")),
            "same count, no thread line: {changes:?}"
        );

        assert!(diff_snapshots(&degraded, &degraded).is_empty());
    }

    #[test]
    fn diff_reports_nothing_when_the_snapshot_is_unchanged() {
        let a = snapshot(|_| {});
        assert!(diff_snapshots(&a, &a).is_empty());
    }

    #[test]
    fn diff_detects_each_field_class() {
        let base = snapshot(|_| {});

        let merged = snapshot(|s| s.requirements.state = "merged".into());
        assert!(diff_snapshots(&base, &merged)
            .iter()
            .any(|c| c == "state: open → merged"));

        let draft = snapshot(|s| s.requirements.is_draft = true);
        assert!(diff_snapshots(&base, &draft)
            .iter()
            .any(|c| c == "marked as draft"));

        let pushed = snapshot(|s| s.head_sha = Some("bbbbbbbb".into()));
        assert!(diff_snapshots(&base, &pushed)
            .iter()
            .any(|c| c.contains("new commits pushed") && c.contains("bbbbbbbb")));

        let approved = snapshot(|s| {
            s.requirements.approvals.decision = "approved".into();
            s.requirements.approvals.have = 1;
        });
        let changes = diff_snapshots(&base, &approved);
        assert!(changes
            .iter()
            .any(|c| c == "review decision: review_required → approved"));
        assert!(changes.iter().any(|c| c.contains("new approval")));

        let withdrawn = snapshot(|s| {
            s.requirements.approvals.decision = "approved".into();
            s.requirements.approvals.have = 0;
        });
        assert!(diff_snapshots(&approved, &withdrawn)
            .iter()
            .any(|c| c.contains("approval withdrawn")));

        let requested = snapshot(|s| s.requirements.approvals.changes_requested = 1);
        assert!(diff_snapshots(&base, &requested)
            .iter()
            .any(|c| c == "changes-requested reviews: 0 → 1"));

        let commented = snapshot(|s| s.conversation_count = 3);
        assert!(diff_snapshots(&base, &commented)
            .iter()
            .any(|c| c.starts_with("+2 conversation comments")));

        let reviewed = snapshot(|s| s.review_comment_count = 3);
        assert!(diff_snapshots(&base, &reviewed)
            .iter()
            .any(|c| c.starts_with("+1 review comment ")));

        let resolved = snapshot(|s| s.requirements.threads.unresolved = Some(0));
        assert!(diff_snapshots(&base, &resolved)
            .iter()
            .any(|c| c.starts_with("thread(s) resolved")));

        let unresolved = snapshot(|s| s.requirements.threads.unresolved = Some(2));
        assert!(diff_snapshots(&base, &unresolved)
            .iter()
            .any(|c| c.starts_with("thread(s) unresolved/opened")));

        let conflicted = snapshot(|s| s.requirements.has_conflicts = true);
        assert!(diff_snapshots(&base, &conflicted)
            .iter()
            .any(|c| c == "merge conflicts appeared"));

        let behind = snapshot(|s| s.requirements.is_behind = true);
        assert!(diff_snapshots(&base, &behind)
            .iter()
            .any(|c| c == "branch is now behind its base"));

        let queued = snapshot(|s| s.requirements.is_in_merge_queue = Some(true));
        assert!(diff_snapshots(&base, &queued)
            .iter()
            .any(|c| c == "entered the merge queue"));
        assert!(diff_snapshots(&queued, &base)
            .iter()
            .any(|c| c == "left the merge queue"));

        let unmergeable = snapshot(|s| s.requirements.mergeable = Some(false));
        assert!(diff_snapshots(&base, &unmergeable)
            .iter()
            .any(|c| c == "mergeable: true → false"));

        let restated = snapshot(|s| s.requirements.merge_state_status = Some("CLEAN".into()));
        assert!(diff_snapshots(&base, &restated)
            .iter()
            .any(|c| c == "merge state: BLOCKED → CLEAN"));

        let blocked =
            snapshot(|s| s.requirements.merge_blocked_reason = Some("merge conflicts".into()));
        assert!(diff_snapshots(&base, &blocked)
            .iter()
            .any(|c| c == "merge blocked: merge conflicts"));
        assert!(diff_snapshots(&blocked, &base)
            .iter()
            .any(|c| c == "merge is no longer blocked"));
    }

    /// `unknown` is a transient GitHub state ("still recomputing"), never an
    /// actionable signal: `mergeable`/`mergeStateStatus` transitions TO
    /// unknown are suppressed unconditionally — queued or not — so the
    /// merge-queue processing blip (mergeable → null, mergeStateStatus →
    /// UNKNOWN) never wakes the agent. Transitions FROM unknown to a known
    /// value still report.
    #[test]
    fn diff_suppresses_mergeable_and_merge_state_transitions_to_unknown() {
        // While queued: the recomputation blip produces no lines at all,
        // whether the merge state reads as the literal UNKNOWN enum…
        let queued = snapshot(|s| s.requirements.is_in_merge_queue = Some(true));
        let queued_blip = snapshot(|s| {
            s.requirements.is_in_merge_queue = Some(true);
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = Some("UNKNOWN".into());
        });
        assert!(diff_snapshots(&queued, &queued_blip).is_empty());
        // …or as absent entirely.
        let queued_none = snapshot(|s| {
            s.requirements.is_in_merge_queue = Some(true);
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = None;
        });
        assert!(diff_snapshots(&queued, &queued_none).is_empty());

        // Not queued: suppression is unconditional — the same blip stays
        // quiet.
        let base = snapshot(|_| {});
        let blip = snapshot(|s| {
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = Some("UNKNOWN".into());
        });
        assert!(diff_snapshots(&base, &blip).is_empty());

        // Settling FROM unknown at a known value still reports.
        let known = snapshot(|s| s.requirements.mergeable = Some(false));
        let changes = diff_snapshots(&blip, &known);
        assert!(changes.iter().any(|c| c == "mergeable: unknown → false"));
        assert!(changes
            .iter()
            .any(|c| c == "merge state: UNKNOWN → BLOCKED"));
    }

    /// The same transient recomputation also clears the DERIVED fields
    /// (`hasConflicts` / `isBehind` / `mergeBlockedReason`): while the NEW
    /// snapshot's mergeability is unknown, the clearing direction of those
    /// lines is suppressed too — a DIRTY/BEHIND/blocked PR blipping to
    /// UNKNOWN stays fully silent. A real clear to a known state still
    /// reports, and the appearing direction reports even while unknown.
    #[test]
    fn diff_suppresses_derived_clears_while_mergeability_is_unknown() {
        let dirty = snapshot(|s| {
            s.requirements.has_conflicts = true;
            s.requirements.is_behind = true;
            s.requirements.mergeable = Some(false);
            s.requirements.merge_state_status = Some("DIRTY".into());
            s.requirements.merge_blocked_reason = Some("merge conflicts".into());
        });
        // DIRTY → UNKNOWN blip: the recomputation resets the derived fields
        // alongside the raw ones; nothing reports.
        let blip = snapshot(|s| {
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = Some("UNKNOWN".into());
        });
        assert!(diff_snapshots(&dirty, &blip).is_empty());
        // Same with the merge state absent entirely.
        let blip_none = snapshot(|s| {
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = None;
        });
        assert!(diff_snapshots(&dirty, &blip_none).is_empty());

        // A real clear to a known state still reports all three.
        let cleared = snapshot(|s| s.requirements.merge_state_status = Some("CLEAN".into()));
        let changes = diff_snapshots(&dirty, &cleared);
        assert!(changes.iter().any(|c| c == "merge conflicts resolved"));
        assert!(changes
            .iter()
            .any(|c| c == "branch is no longer behind its base"));
        assert!(changes.iter().any(|c| c == "merge is no longer blocked"));

        // The appearing direction keeps reporting even while unknown.
        let base = snapshot(|_| {});
        let appearing = snapshot(|s| {
            s.requirements.has_conflicts = true;
            s.requirements.is_behind = true;
            s.requirements.merge_blocked_reason = Some("blocked".into());
            s.requirements.mergeable = None;
            s.requirements.merge_state_status = None;
        });
        let changes = diff_snapshots(&base, &appearing);
        assert!(changes.iter().any(|c| c == "merge conflicts appeared"));
        assert!(changes.iter().any(|c| c == "branch is now behind its base"));
        assert!(changes.iter().any(|c| c == "merge blocked: blocked"));
    }

    /// The ejection diff is keyed on the event identity (`at`): a new event
    /// fires (with the reason humanized, underscores → spaces), an unchanged
    /// event stays quiet, and an enter→eject pair that nets out on
    /// `isInMergeQueue` still yields a reportable change.
    #[test]
    fn diff_reports_merge_queue_ejection_keyed_on_event_identity() {
        let base = snapshot(|_| {});
        let ejected = snapshot(|s| {
            s.requirements.merge_queue_ejection = Some(pr_ops::MergeQueueEjection {
                at: "2026-01-02T03:04:05Z".into(),
                reason: Some("failed_checks".into()),
            });
        });
        assert!(diff_snapshots(&base, &ejected)
            .iter()
            .any(|c| c == "removed from the merge queue (failed checks)"));

        // Unchanged event: nothing to report.
        assert!(diff_snapshots(&ejected, &ejected).is_empty());

        // A later ejection is a new event and fires again; no reason drops
        // the parenthetical.
        let re_ejected = snapshot(|s| {
            s.requirements.merge_queue_ejection = Some(pr_ops::MergeQueueEjection {
                at: "2026-01-03T00:00:00Z".into(),
                reason: None,
            });
        });
        assert!(diff_snapshots(&ejected, &re_ejected)
            .iter()
            .any(|c| c == "removed from the merge queue"));

        // Enter→eject within one window: `isInMergeQueue` nets out to its
        // baseline (absent on both sides), so no entered/left line — but the
        // fresh ejection event still yields a reportable change.
        let changes = diff_snapshots(&base, &ejected);
        assert!(!changes
            .iter()
            .any(|c| c == "entered the merge queue" || c == "left the merge queue"));
        assert_eq!(
            changes,
            vec!["removed from the merge queue (failed checks)"]
        );
    }

    /// A persisted baseline written before `mergeQueueEjection` existed still
    /// parses (serde default) and produces no phantom diff line against a
    /// fresh snapshot that also has no event — and reads as UNTRACKED, so
    /// the poll's upgrade backfill knows to adopt history silently.
    #[test]
    fn old_baseline_without_ejection_field_parses_without_phantom_diff() {
        let mut wire = serde_json::to_value(snapshot(|_| {})).unwrap();
        wire.as_object_mut().unwrap().remove("ejectionTracked");
        let req = wire["requirements"].as_object_mut().unwrap();
        assert!(
            !req.contains_key("mergeQueueEjection"),
            "omitted when absent"
        );
        let old: PrMonitorSnapshot = serde_json::from_value(wire).unwrap();
        assert_eq!(old.requirements.merge_queue_ejection, None);
        assert!(!old.ejection_tracked, "pre-upgrade rows read as untracked");
        assert!(diff_snapshots(&old, &snapshot(|_| {})).is_empty());
    }

    /// [`SharedPrSnapshot`] with the [`snapshot`] fixture's fields; the
    /// materialize tests vary `ejection_known` and the previous snapshot.
    fn shared_from(s: &PrMonitorSnapshot, ejection_known: bool) -> SharedPrSnapshot {
        SharedPrSnapshot {
            title: s.title.clone(),
            url: s.url.clone(),
            head_sha: s.head_sha.clone(),
            conversation_count: Some(s.conversation_count),
            review_comment_count: s.review_comment_count,
            requirements: s.requirements.clone(),
            ejection_known,
            requirements_complete: ejection_known,
        }
    }

    /// A degraded merge-requirements probe (`ejection_known == false`) must
    /// not read as "no ejection": materialize holds the monitor's previously
    /// observed event (and its tracked-ness), while an answering probe is
    /// authoritative on both.
    #[test]
    fn materialize_holds_previous_ejection_through_a_degraded_probe() {
        let event = pr_ops::MergeQueueEjection {
            at: "2026-01-02T03:04:05Z".into(),
            reason: Some("failed_checks".into()),
        };
        let prev = snapshot(|s| {
            s.requirements.merge_queue_ejection = Some(event.clone());
        });
        let degraded = shared_from(&snapshot(|_| {}), false);
        let m = degraded.materialize(Some(&prev));
        assert_eq!(
            m.requirements.merge_queue_ejection,
            Some(event.clone()),
            "the held event survives the degraded poll"
        );
        assert!(m.ejection_tracked, "tracked-ness carries with the hold");

        // An untracked previous (pre-upgrade baseline) stays untracked
        // through a degraded probe — only a real probe answer flips it.
        let untracked = snapshot(|s| s.ejection_tracked = false);
        assert!(!degraded.materialize(Some(&untracked)).ejection_tracked);
        assert!(!degraded.materialize(None).ejection_tracked);

        // An answering probe is authoritative: its (absent) event replaces
        // the previous one and the result is tracked.
        let known = shared_from(&snapshot(|_| {}), true);
        let m = known.materialize(Some(&prev));
        assert_eq!(m.requirements.merge_queue_ejection, None);
        assert!(m.ejection_tracked);
    }

    #[test]
    fn diff_detects_check_transitions_additions_and_removals() {
        let base = snapshot(|_| {});
        let failed = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.items[0].status = "failed".into();
            c.pending = 0;
            c.failed = 1;
            c.failing_required = vec!["build".into()];
            c.pending_required.clear();
        });
        assert!(diff_snapshots(&base, &failed)
            .iter()
            .any(|c| c == "check build: pending → failed"));

        // A failed → passed recovery resolves a previously reported failure
        // and IS reported (unlike a normal pending → passed success).
        let recovered = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.items[0].status = "passed".into();
            c.pending = 0;
            c.passed = 1;
            c.pending_required.clear();
        });
        assert!(diff_snapshots(&failed, &recovered)
            .iter()
            .any(|c| c == "check build: failed → passed"));

        let added = snapshot(|s| {
            s.requirements
                .checks
                .items
                .push(pr_ops::MergeRequirementCheck {
                    name: "lint".into(),
                    status: "failed".into(),
                    required: false,
                    url: None,
                });
        });
        assert!(diff_snapshots(&base, &added)
            .iter()
            .any(|c| c == "check started: lint (failed)"));
        assert!(diff_snapshots(&added, &base)
            .iter()
            .any(|c| c == "check removed: lint"));
    }

    #[test]
    fn intermediate_check_successes_are_suppressed() {
        let pending_lint = pr_ops::MergeRequirementCheck {
            name: "lint".into(),
            status: "pending".into(),
            required: false,
            url: None,
        };
        let two_pending = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.total = 2;
            c.pending = 2;
            c.items.push(pending_lint.clone());
        });
        let one_done = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.total = 2;
            c.pending = 1;
            c.passed = 1;
            c.items[0].status = "passed".into();
            c.items.push(pending_lint.clone());
            c.pending_required.clear();
        });
        assert!(
            diff_snapshots(&two_pending, &one_done).is_empty(),
            "an intermediate pending → passed transition must stay quiet"
        );
    }

    #[test]
    fn a_check_appearing_already_green_is_suppressed() {
        let base = snapshot(|_| {});
        let added_green = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.total = 2;
            c.passed = 1;
            c.items.push(pr_ops::MergeRequirementCheck {
                name: "lint".into(),
                status: "passed".into(),
                required: false,
                url: None,
            });
        });
        assert!(
            diff_snapshots(&base, &added_green).is_empty(),
            "a check that appears already passed must stay quiet"
        );
    }

    #[test]
    fn suite_completion_reports_one_aggregate_line() {
        // Everything green: the last pending check finishing produces exactly
        // one aggregate line and no per-check success line.
        let base = snapshot(|_| {});
        let all_passed = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.items[0].status = "passed".into();
            c.pending = 0;
            c.passed = 1;
            c.pending_required.clear();
        });
        assert_eq!(
            diff_snapshots(&base, &all_passed),
            vec!["all checks passed (1)".to_string()]
        );

        // Mixed outcome: the failure line still reports, plus the completion
        // summary — but no line for the check that merely passed.
        let two_pending = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.total = 2;
            c.pending = 2;
            c.items.push(pr_ops::MergeRequirementCheck {
                name: "lint".into(),
                status: "pending".into(),
                required: false,
                url: None,
            });
        });
        let mixed = snapshot(|s| {
            let c = &mut s.requirements.checks;
            c.total = 2;
            c.pending = 0;
            c.passed = 1;
            c.failed = 1;
            c.items[0].status = "passed".into();
            c.items.push(pr_ops::MergeRequirementCheck {
                name: "lint".into(),
                status: "failed".into(),
                required: false,
                url: None,
            });
            c.pending_required.clear();
        });
        let changes = diff_snapshots(&two_pending, &mixed);
        assert!(changes.iter().any(|c| c == "check lint: pending → failed"));
        assert!(changes
            .iter()
            .any(|c| c == "all checks completed: 1 passed, 1 failed"));
        assert!(!changes.iter().any(|c| c.contains("check build")));
    }

    #[test]
    fn required_flag_flips_only_count_when_both_sides_know_them() {
        let base = snapshot(|_| {});
        // A degraded probe zeroes every `required` flag: that is missing
        // information, not the branch dropping the requirement.
        let degraded = snapshot(|s| {
            s.requirements.checks.required_known = false;
            s.requirements.checks.items[0].required = false;
            s.requirements.checks.pending_required.clear();
        });
        assert!(
            !diff_snapshots(&base, &degraded)
                .iter()
                .any(|c| c.contains("required to merge")),
            "a degraded probe must not report a requirement change"
        );
        // Both sides trustworthy: a genuine flip IS reported.
        let optional = snapshot(|s| {
            s.requirements.checks.items[0].required = false;
            s.requirements.checks.pending_required.clear();
        });
        assert!(diff_snapshots(&base, &optional)
            .iter()
            .any(|c| c == "check build is no longer required to merge"));
    }

    #[tokio::test]
    async fn register_captures_a_baseline_and_re_registers_idempotently() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (first, requirements) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 42)
            .await
            .expect("register");
        assert_eq!(first.state, PrMonitorState::Active);
        assert_eq!(requirements.state, "open");
        assert!(first.last_snapshot.is_some(), "baseline captured");

        // A change lands, then the SAME (agent, repo, pr) re-registers: the
        // row is reused and its baseline refreshed, so nothing is pending.
        forge.edit(|s| s.conversation_comments = 2);
        let (second, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 42)
            .await
            .expect("re-register");
        assert_eq!(second.monitor_id, first.monitor_id, "no duplicate row");
        assert!(second.pending_changes.is_empty());
        assert_ne!(second.last_snapshot, first.last_snapshot, "baseline moved");
        assert_eq!(svc.pr_monitors_for_agent(&owner).await.unwrap().len(), 1);
    }

    /// Regression (intent-hq/intent#5372): a head carrying a live
    /// `completed/success` run AND an earlier `concurrency`-cancelled
    /// duplicate of the same workflow (whose gate job reports a genuine
    /// `failure`) lists every check name twice in the rollup — plus, here, an
    /// untimed run and a green legacy commit status under the gate's name.
    /// The same forge data poll after poll, in whichever order the host
    /// happens to list the nodes each time, must be a quiet poll — no
    /// `passed → failed` burst, nothing pending, no wake — and the checklist
    /// reports each name once as passed. The legacy status is independent
    /// evidence, though: when it turns red the gate reports `passed → failed`
    /// exactly once, however the live run's twins are ordered.
    #[tokio::test]
    async fn a_concurrency_cancelled_duplicate_run_does_not_flap_the_checks() {
        let run = |name: &str, state: CheckState, started_at: Option<&str>| RollupCheck {
            name: name.into(),
            kind: RollupCheckKind::CheckRun,
            state,
            is_required: name == "CI Gate",
            url: None,
            started_at: started_at.map(String::from),
        };
        let status = |state: CheckState| RollupCheck {
            kind: RollupCheckKind::StatusContext,
            ..run("CI Gate", state, None)
        };
        let cancelled_first = vec![
            run("CI Gate", CheckState::Failure, Some("2026-09-18T11:08:02Z")),
            run("route", CheckState::Cancelled, Some("2026-09-18T11:08:02Z")),
            run("CI Gate", CheckState::Success, Some("2026-09-18T11:32:04Z")),
            run("route", CheckState::Success, Some("2026-09-18T11:32:04Z")),
            run("CI Gate", CheckState::Failure, None),
            status(CheckState::Success),
        ];
        let cancelled_last = cancelled_first.iter().rev().cloned().collect::<Vec<_>>();
        let assert_one_passed_each = |checks: &pr_ops::MergeRequirementsChecks| {
            assert_eq!(checks.total, 2, "{checks:?}");
            assert_eq!((checks.passed, checks.failed), (2, 0), "{checks:?}");
            assert!(checks.failing_required.is_empty(), "{checks:?}");
        };

        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(0);
        forge.edit_quiet(|s| s.checks = cancelled_first.clone());
        let (monitor, requirements) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 42)
            .await
            .expect("register");
        assert_one_passed_each(&requirements.checks);

        // Same data, then reordered, then back: every poll is quiet. The
        // edits bump the PR's fingerprint so each poll is a FULL fetch that
        // re-reduces the rollup rather than reusing the cached checklist.
        for checks in [&cancelled_first, &cancelled_last, &cancelled_first] {
            let probes_before = forge.sub_fetches("merge_requirements");
            forge.edit(|s| s.checks = checks.clone());
            svc.poll_pr_monitors().await;
            assert_eq!(
                forge.sub_fetches("merge_requirements"),
                probes_before + 1,
                "the poll re-read the rollup"
            );
            let row = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            assert!(
                row.pending_changes.is_empty(),
                "identical forge data must be a quiet poll: {:?}",
                row.pending_changes
            );
            assert!(row.last_change_at.is_none(), "{row:?}");
            let snapshot: PrMonitorSnapshot =
                serde_json::from_str(row.last_snapshot.as_deref().expect("snapshot")).unwrap();
            assert_one_passed_each(&snapshot.requirements.checks);
        }
        let text = owner_messages(&svc, &owner).await;
        assert!(!text.contains("passed → failed"), "{text}");
        assert!(!text.contains("pr_monitor_wake"), "no wake: {text}");

        // The legacy status turns red: a real change, reported once, and the
        // live run's success no longer hides it in either twin order.
        let mut red = cancelled_last.clone();
        red.retain(|c| c.kind != RollupCheckKind::StatusContext);
        red.insert(0, status(CheckState::Failure));
        forge.edit(|s| s.checks = red.clone());
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let snapshot: PrMonitorSnapshot =
            serde_json::from_str(row.last_snapshot.as_deref().expect("snapshot")).unwrap();
        let checks = &snapshot.requirements.checks;
        assert_eq!(
            (checks.total, checks.passed, checks.failed),
            (2, 1, 1),
            "{checks:?}"
        );
        assert_eq!(checks.failing_required, vec!["CI Gate".to_string()]);
        let text = owner_messages(&svc, &owner).await;
        let reported = row
            .pending_changes
            .iter()
            .filter(|c| c.as_str() == "check CI Gate: passed → failed")
            .count()
            + text.matches("check CI Gate: passed → failed").count();
        assert_eq!(reported, 1, "{:?}\n{text}", row.pending_changes);

        red.reverse();
        forge.edit(|s| s.checks = red.clone());
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let snapshot: PrMonitorSnapshot =
            serde_json::from_str(row.last_snapshot.as_deref().expect("snapshot")).unwrap();
        let checks = &snapshot.requirements.checks;
        assert_eq!((checks.passed, checks.failed), (1, 1), "{checks:?}");
        let text = owner_messages(&svc, &owner).await;
        assert!(!text.contains("failed → passed"), "{text}");
        assert!(
            !row.pending_changes
                .iter()
                .any(|c| c.contains("CI Gate: failed")),
            "{:?}",
            row.pending_changes
        );
    }

    #[tokio::test]
    async fn register_enforces_the_per_agent_cap() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitors_max_per_agent(2);
        svc.pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("first");
        svc.pr_monitor_register(&ws, &owner, "o", "r", 2)
            .await
            .expect("second");
        let err = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 3)
            .await
            .expect_err("third exceeds the cap");
        assert!(err.to_string().contains("max 2"), "{err}");
        // Re-registering an EXISTING monitor is exempt from the cap.
        svc.pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("re-register at cap");
    }

    /// One monitor per PR per workspace: a second agent's `pr.monitor` on a
    /// PR another agent already watches is REFUSED with the structured
    /// payload naming the owner — no error, no forge fetch, no row, no
    /// `prMonitor:registered` event — while the owner's own re-register
    /// still re-arms idempotently.
    #[tokio::test]
    async fn a_second_agent_is_refused_and_told_who_owns_the_monitor() {
        async fn registered_events(svc: &Services, ws: &WorkspaceId) -> usize {
            svc.store()
                .query_events(&intent_store::EventQuery {
                    workspace_id: Some(ws.clone()),
                    event_types: vec![PR_MONITOR_REGISTERED.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap()
                .len()
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        let registered_before = registered_events(&svc, &ws).await;

        let fetches_before = forge.fetches();
        let refused = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("a refusal is a payload, not an error");
        assert_eq!(refused["ok"], json!(false), "{refused}");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["reason"], json!("already-monitored"), "{refused}");
        assert_eq!(
            refused["ownerAgentId"],
            json!(owner.to_string()),
            "{refused}"
        );
        assert_eq!(refused["ownerAgentName"], json!("Owner"), "{refused}");
        assert_eq!(refused["monitorId"], json!(first.monitor_id), "{refused}");
        assert_eq!(refused["repo"], json!("o/r"), "{refused}");
        assert_eq!(refused["prNumber"], json!(42), "{refused}");
        let instruction = refused["instruction"].as_str().expect("instruction");
        assert!(instruction.contains("o/r#42"), "{instruction}");
        assert!(instruction.contains("Owner (agent-prmon)"), "{instruction}");
        assert!(instruction.contains("ws.agent.send"), "{instruction}");
        assert!(instruction.contains("relay the events"), "{instruction}");
        assert!(
            instruction.contains("relinquish the monitor via ws.pr.unmonitor"),
            "{instruction}"
        );
        assert!(
            instruction.contains("one-shot read of the PR's current state use ws.pr.snapshot"),
            "{instruction}"
        );
        assert!(refused.get("monitor").is_none(), "{refused}");
        assert!(refused.get("requirements").is_none(), "{refused}");

        assert_eq!(
            forge.fetches(),
            fetches_before,
            "refused before the forge fetch"
        );
        assert!(
            svc.pr_monitors_for_agent(&sibling)
                .await
                .unwrap()
                .is_empty(),
            "no row persisted for the refused caller"
        );
        let ws_view = svc.pr_monitor_list_op(&ws, None).await.expect("ws list");
        let rows = ws_view["monitors"].as_array().expect("array");
        assert_eq!(rows.len(), 1, "the workspace list stays single: {ws_view}");
        assert_eq!(rows[0]["agentId"], json!(owner.to_string()));
        assert_eq!(
            registered_events(&svc, &ws).await,
            registered_before,
            "no prMonitor:registered event for the refused attempt"
        );

        // The direct-service path surfaces the same refusal as InvalidParams.
        let err = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
            .await
            .expect_err("service path refuses too");
        assert!(matches!(err, Error::InvalidParams(_)), "{err}");
        assert!(err.to_string().contains("agent-prmon"), "{err}");

        // The OWNER's own re-register is not a duplicate: it re-arms.
        let rearmed = svc
            .pr_monitor_start_op(&ws, &owner, 42, None)
            .await
            .expect("owner re-register");
        assert_eq!(rearmed["ok"], json!(true), "{rearmed}");
        assert_eq!(rearmed["monitor"]["monitorId"], json!(first.monitor_id));
    }

    /// An owner without a session name still yields a refusal —
    /// `ownerAgentName` is simply omitted and the instruction names the id.
    /// (A deleted owner session cascades its monitor rows away, so it can
    /// never be the holder.)
    #[tokio::test]
    async fn a_refusal_omits_the_owner_name_when_the_owner_is_unnamed() {
        let (_db, _root, svc, _forge, ws, _named) = setup().await;
        let mut unnamed = agent(&ws, "agent-unnamed");
        unnamed.name = String::new();
        unnamed.name_explicitly_set = false;
        svc.store()
            .insert_agent_session(&unnamed)
            .await
            .expect("unnamed owner");
        let owner = unnamed.id.clone();
        register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;

        let refused = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("refusal");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(owner.to_string()));
        assert!(refused.get("ownerAgentName").is_none(), "{refused}");
        assert!(
            refused["instruction"]
                .as_str()
                .unwrap()
                .contains("by agent agent-unnamed;"),
            "{refused}"
        );
    }

    /// How the fixture owner "dies" in the orphaned-monitor tests
    /// (intent-hq/intent#5079): a terminal session status, or soft-retire.
    #[derive(Clone, Copy, Debug)]
    enum OwnerDeath {
        Error,
        Deleted,
        Retired,
    }

    async fn kill_owner(svc: &Services, ws: &WorkspaceId, owner: &AgentId, how: OwnerDeath) {
        let now = now_iso();
        match how {
            OwnerDeath::Error | OwnerDeath::Deleted => {
                let status = match how {
                    OwnerDeath::Error => AgentStatus::Error,
                    _ => AgentStatus::Deleted,
                };
                svc.store()
                    .set_agent_session_status(ws, owner, status, false, &now, None)
                    .await
                    .expect("owner status");
            }
            OwnerDeath::Retired => {
                assert!(svc
                    .store()
                    .set_agent_session_retired_at(ws, owner, Some(&now), &now)
                    .await
                    .expect("owner retired"));
            }
        }
    }

    /// The `prMonitor:registered` event payloads in the workspace, newest first.
    async fn registered_event_data(svc: &Services, ws: &WorkspaceId) -> Vec<Value> {
        svc.store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![PR_MONITOR_REGISTERED.to_string()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.data)
            .collect()
    }

    /// A monitor whose owner can no longer receive wakes is ORPHANED: a
    /// live sibling's `ws.pr.monitor` adopts it (intent-hq/intent#5079) —
    /// the same row is re-armed under the caller (baseline refreshed,
    /// pending changes cleared), the result is a success payload carrying
    /// `adoptedFrom`, and `prMonitor:registered` marks the adoption. While
    /// the owner is still live, the same call is refused exactly as before.
    async fn assert_orphan_adopted_after(how: OwnerDeath) {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        let before = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert!(
            !before.pending_changes.is_empty(),
            "a change is pending: {before:?}"
        );

        let refused = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "live owner: {refused}");

        kill_owner(&svc, &ws, &owner, how).await;
        let registered_before = registered_event_data(&svc, &ws).await.len();

        let adopted = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("adoption is a success payload");
        assert_eq!(adopted["ok"], json!(true), "{how:?}: {adopted}");
        assert!(adopted.get("refused").is_none(), "{how:?}: {adopted}");
        assert_eq!(
            adopted["adoptedFrom"],
            json!(owner.to_string()),
            "{how:?}: {adopted}"
        );
        assert_eq!(
            adopted["monitor"]["monitorId"],
            json!(first.monitor_id),
            "same row"
        );
        assert_eq!(adopted["monitor"]["agentId"], json!(sibling.to_string()));
        assert_eq!(adopted["monitor"]["state"], json!("active"));
        assert_eq!(adopted["requirements"]["state"], json!("open"), "{adopted}");

        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, sibling, "{how:?}: owner re-parented");
        assert_eq!(row.state, PrMonitorState::Active);
        assert!(row.pending_changes.is_empty(), "pending cleared: {row:?}");
        assert_eq!(
            row.baseline_snapshot, row.last_snapshot,
            "baseline refreshed"
        );
        assert_ne!(
            row.baseline_snapshot, first.baseline_snapshot,
            "baseline moved"
        );

        let events = registered_event_data(&svc, &ws).await;
        assert_eq!(events.len(), registered_before + 1, "one registered event");
        let newest = events.first().unwrap();
        assert_eq!(newest["agentId"], json!(sibling.to_string()), "{newest}");
        assert_eq!(newest["adoptedFrom"], json!(owner.to_string()), "{newest}");
        assert_eq!(newest["monitorId"], json!(first.monitor_id), "{newest}");

        assert!(svc.pr_monitors_for_agent(&owner).await.unwrap().is_empty());
        assert_eq!(svc.pr_monitors_for_agent(&sibling).await.unwrap().len(), 1);
        let ws_view = svc.pr_monitor_list_op(&ws, None).await.expect("ws list");
        let rows = ws_view["monitors"].as_array().expect("array");
        assert_eq!(rows.len(), 1, "no second row: {ws_view}");
        assert_eq!(rows[0]["agentId"], json!(sibling.to_string()));
        assert!(
            !owner_messages(&svc, &owner).await.contains("transferred"),
            "{how:?}: a dead owner gets no transfer notice"
        );

        // The new owner's own re-register is the ordinary idempotent re-arm.
        let rearmed = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("re-register");
        assert_eq!(rearmed["ok"], json!(true), "{rearmed}");
        assert!(rearmed.get("adoptedFrom").is_none(), "{rearmed}");
        assert_eq!(rearmed["monitor"]["monitorId"], json!(first.monitor_id));
        assert_eq!(svc.pr_monitors_for_agent(&sibling).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_monitor_owned_by_a_failed_agent_is_adopted() {
        assert_orphan_adopted_after(OwnerDeath::Error).await;
    }

    #[tokio::test]
    async fn a_monitor_owned_by_a_deleted_agent_is_adopted() {
        assert_orphan_adopted_after(OwnerDeath::Deleted).await;
    }

    #[tokio::test]
    async fn a_monitor_owned_by_a_retired_agent_is_adopted() {
        assert_orphan_adopted_after(OwnerDeath::Retired).await;
    }

    /// Insert a live agent whose `parent_agent_id` is `parent`, in `status`.
    async fn child_agent(
        svc: &Services,
        ws: &WorkspaceId,
        id: &str,
        parent: &AgentId,
        status: AgentStatus,
    ) -> AgentId {
        let mut child = agent(ws, id);
        child.name = "Child".to_string();
        child.parent_agent_id = Some(parent.clone());
        child.status = status;
        svc.store()
            .insert_agent_session(&child)
            .await
            .expect("child agent");
        AgentId::from(id)
    }

    /// Insert a task note in `status` and link it to `agent` as its task.
    async fn link_task_note(
        svc: &Services,
        ws: &WorkspaceId,
        agent_id: &AgentId,
        status: intent_core::TaskStatus,
    ) {
        let ts = now_iso();
        let note_id = intent_core::NoteId::from(format!("task-{}", agent_id.0));
        let note = intent_core::Note {
            id: note_id.clone(),
            workspace_id: ws.clone(),
            title: "Task".to_string(),
            content: "body".to_string(),
            content_type: intent_core::ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: intent_core::NoteVisibility::Workspace,
            metadata: intent_core::NoteMetadata {
                task: Some(intent_core::TaskMetadata {
                    status,
                    ..Default::default()
                }),
            },
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        };
        svc.store().insert_note(&note).await.expect("task note");
        let mut session = svc.store().get_agent_session(agent_id).await.unwrap();
        session.task_note_id = Some(note_id);
        svc.store()
            .update_agent_session(ws, &session)
            .await
            .expect("link task");
    }

    /// The parent-takeover half of the holder decision: a live DIRECT
    /// sub-agent's monitor is adoptable by its parent once the child has
    /// settled — same re-arm semantics as orphan adoption, same
    /// `adoptedFrom` payload/event — and, unlike an orphan's dead owner,
    /// the child is woken exactly once with a `transferred` notice naming
    /// the adopter.
    async fn assert_parent_takeover(
        svc: &Services,
        ws: &WorkspaceId,
        parent: &AgentId,
        child: &AgentId,
        first: &PrMonitor,
    ) {
        let registered_before = registered_event_data(svc, ws).await.len();
        let adopted = svc
            .pr_monitor_start_op(ws, parent, 42, None)
            .await
            .expect("takeover is a success payload");
        assert_eq!(adopted["ok"], json!(true), "{adopted}");
        assert!(adopted.get("refused").is_none(), "{adopted}");
        assert_eq!(
            adopted["adoptedFrom"],
            json!(child.to_string()),
            "{adopted}"
        );
        assert_eq!(adopted["monitor"]["monitorId"], json!(first.monitor_id));
        assert_eq!(adopted["monitor"]["agentId"], json!(parent.to_string()));

        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, *parent, "re-parented");
        assert_eq!(row.state, PrMonitorState::Active);
        assert!(row.pending_changes.is_empty(), "pending cleared: {row:?}");
        assert_eq!(
            row.baseline_snapshot, row.last_snapshot,
            "baseline refreshed"
        );
        assert_ne!(
            row.baseline_snapshot, first.baseline_snapshot,
            "baseline moved"
        );

        let events = registered_event_data(svc, ws).await;
        assert_eq!(events.len(), registered_before + 1, "one registered event");
        assert_eq!(
            events[0]["adoptedFrom"],
            json!(child.to_string()),
            "{}",
            events[0]
        );
        assert!(svc.pr_monitors_for_agent(child).await.unwrap().is_empty());
        assert_eq!(svc.pr_monitors_for_agent(parent).await.unwrap().len(), 1);

        let child_session = svc.store().get_agent_session(child).await.unwrap();
        assert_eq!(
            child_session.messages.len(),
            1,
            "one wake: {child_session:?}"
        );
        let text = owner_messages(svc, child).await;
        assert!(text.contains(r#""reason":"transferred""#), "{text}");
        assert!(
            text.contains(&format!(r#""adoptedBy":"{}""#, parent.0)),
            "{text}"
        );
        assert!(
            text.contains(&format!(r#""monitorId":"{}""#, first.monitor_id.0)),
            "{text}"
        );
        assert!(
            text.contains(r#""url":"https://github.com/o/r/pull/42""#),
            "{text}"
        );
        let notice = format!(
            "[PR monitor o/r#42] Your parent agent ({}) took over this monitor because \
             your work had settled — it now receives the PR's wakes and this monitor will \
             not report to you again. Do not re-register a monitor on this PR \
             (ws.pr.monitor would be refused while your parent holds it); no other action \
             is needed.",
            parent.0
        );
        assert!(text.contains(&notice), "{text}");
        assert!(
            !owner_messages(svc, parent)
                .await
                .contains("pr_monitor_wake"),
            "the adopter is not woken by its own takeover"
        );
    }

    #[tokio::test]
    async fn a_parent_adopts_the_monitor_of_a_child_whose_task_is_complete() {
        let (_db, _root, svc, forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::Active).await;
        let first = register(&svc, &ws, &child).await;
        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        link_task_note(&svc, &ws, &child, intent_core::TaskStatus::Complete).await;
        assert_parent_takeover(&svc, &ws, &parent, &child, &first).await;
    }

    #[tokio::test]
    async fn a_parent_adopts_the_monitor_of_a_child_whose_task_is_cancelled() {
        let (_db, _root, svc, _forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::Active).await;
        let first = register(&svc, &ws, &child).await;
        link_task_note(&svc, &ws, &child, intent_core::TaskStatus::Cancelled).await;
        assert_parent_takeover(&svc, &ws, &parent, &child, &first).await;
    }

    #[tokio::test]
    async fn a_parent_adopts_the_monitor_of_an_idle_child_with_nothing_else_pending() {
        let (_db, _root, svc, _forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::RuntimeIdle).await;
        let first = register(&svc, &ws, &child).await;
        assert_parent_takeover(&svc, &ws, &parent, &child, &first).await;
    }

    /// A child still mid-work keeps its monitor: task `in_progress`, or idle
    /// with a waiting reason other than the monitor (a busy worker here).
    /// The refusal names the sub-agent relationship and the settlement
    /// conditions under which a retry adopts.
    #[tokio::test]
    async fn a_parent_is_refused_while_its_child_is_still_working() {
        let (_db, _root, svc, _forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::Active).await;
        let first = register(&svc, &ws, &child).await;
        link_task_note(&svc, &ws, &child, intent_core::TaskStatus::InProgress).await;

        let refused = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["reason"], json!("already-monitored"), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(child.to_string()));
        assert_eq!(refused["monitorId"], json!(first.monitor_id));
        let instruction = refused["instruction"].as_str().unwrap();
        assert!(
            instruction.contains("by your sub-agent Child (agent-child), which is still working"),
            "{instruction}"
        );
        assert!(
            instruction.contains("its task is complete or cancelled, or it is idle with nothing pending but this monitor"),
            "{instruction}"
        );
        // Contract first (keep + retry), relinquish only as the explicit
        // "need it now" fallback.
        assert!(
            instruction.contains("A working sub-agent keeps its monitor"),
            "{instruction}"
        );
        let retry_at = instruction.find("Retry ws.pr.monitor").expect("retry");
        let fallback_at = instruction
            .find("Only if you need the monitor now")
            .expect("fallback");
        let relinquish_at = instruction
            .find("relinquish the monitor via ws.pr.unmonitor")
            .expect("relinquish");
        assert!(
            retry_at < fallback_at && fallback_at < relinquish_at,
            "{instruction}"
        );
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, child, "not re-parented");
        assert!(
            !owner_messages(&svc, &child)
                .await
                .contains("pr_monitor_wake"),
            "no wake without a transfer"
        );

        // Idle, but a busy worker is a waiting reason: still refused.
        svc.store()
            .set_agent_session_status(
                &ws,
                &child,
                AgentStatus::RuntimeIdle,
                false,
                &now_iso(),
                None,
            )
            .await
            .unwrap();
        svc.set_test_busy(&child, true);
        let refused = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "busy child: {refused}");
        svc.set_test_busy(&child, false);

        // Idle holding an active hook: the hook is a waiting reason too.
        let out = svc
            .hook_schedule_op(
                &ws,
                &child,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook_id = intent_core::HookId::from(out["hook"]["hookId"].as_str().expect("hookId"));
        let refused = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("payload");
        assert_eq!(
            refused["refused"],
            json!(true),
            "hook-holding child: {refused}"
        );
        svc.hook_cancel_op(&ws, &hook_id, Some(&child))
            .await
            .expect("cancel hook");

        // Task still `in_progress`, but idle with nothing pending: the
        // predicates are OR'd, so the takeover proceeds.
        let adopted = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("payload");
        assert_eq!(adopted["ok"], json!(true), "settled child: {adopted}");
        assert_eq!(adopted["adoptedFrom"], json!(child.to_string()));

        // The child's own re-register after the takeover is the ordinary
        // refusal — the parent is a live holder, not the child's child.
        let refused = svc
            .pr_monitor_start_op(&ws, &child, 42, None)
            .await
            .expect("payload");
        assert_eq!(
            refused["refused"],
            json!(true),
            "child after takeover: {refused}"
        );
        assert_eq!(refused["ownerAgentId"], json!(parent.to_string()));
        assert!(
            !refused["instruction"]
                .as_str()
                .unwrap()
                .contains("sub-agent"),
            "{refused}"
        );
    }

    /// The settled-child verdict is re-evaluated at the adoption write, not
    /// only at the pre-fetch precheck: a child that goes busy DURING the
    /// parent's forge fetch (a new turn, which never touches the monitor
    /// row the adoption CAS guards) is refused after the fetch, keeps its
    /// monitor, and receives no transfer notice.
    #[tokio::test]
    async fn a_child_that_resumes_work_during_the_parents_fetch_keeps_its_monitor() {
        let (_db, _root, svc, forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::RuntimeIdle).await;
        let first = register(&svc, &ws, &child).await;
        let fetches_before = forge.fetches();

        // Settled at precheck; the child starts a turn while `get_pr` runs.
        let svc_in_fetch = svc.clone();
        let child_in_fetch = child.clone();
        forge.set_on_get_pr(Some(Box::new(move |_| {
            svc_in_fetch.set_test_busy(&child_in_fetch, true);
        })));
        let refused = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("a refusal is a payload, not an error");
        forge.set_on_get_pr(None);
        assert_eq!(
            forge.fetches(),
            fetches_before + 1,
            "the precheck saw a settled child, so the fetch ran"
        );
        assert_eq!(refused["ok"], json!(false), "{refused}");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["reason"], json!("already-monitored"), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(child.to_string()));
        assert_eq!(refused["monitorId"], json!(first.monitor_id));
        assert!(
            refused["instruction"]
                .as_str()
                .unwrap()
                .contains("still working"),
            "{refused}"
        );
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, child, "not re-parented");
        assert_eq!(row.state, PrMonitorState::Active);
        assert!(
            !owner_messages(&svc, &child)
                .await
                .contains("pr_monitor_wake"),
            "no transfer notice without a transfer"
        );

        // Once the child is idle again the same call adopts.
        svc.set_test_busy(&child, false);
        assert_parent_takeover(&svc, &ws, &parent, &child, &first).await;
    }

    /// Store probes on the takeover path fail CLOSED: an idle child whose
    /// pending-question state cannot be read (its newest transcript row no
    /// longer decodes, so the question derivation errors while every other
    /// probe succeeds) is refused, and the row keeps its owner. The
    /// convenience count would have collapsed that error to "none pending"
    /// and adopted.
    #[tokio::test]
    async fn a_parent_is_refused_when_the_childs_pending_question_state_is_unreadable() {
        let (_db, _root, svc, _forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::RuntimeIdle).await;
        let first = register(&svc, &ws, &child).await;
        let msg = svc
            .store()
            .append_agent_message(&child, "assistant", &json!([]), &now_iso())
            .await
            .expect("assistant row");
        sqlx::query("UPDATE agent_message SET content = '{bad' WHERE id = ?")
            .bind(&msg.id)
            .execute(svc.store().write_pool())
            .await
            .expect("corrupt message content");
        assert!(
            svc.try_pending_question_count(&child).await.is_err(),
            "the propagating probe surfaces the decode error"
        );
        assert_eq!(
            svc.pending_question_count(&child).await,
            0,
            "the convenience count still fails open"
        );

        let refused = svc
            .pr_monitor_start_op(&ws, &parent, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["reason"], json!("already-monitored"), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(child.to_string()));
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, child, "not re-parented");
        assert_eq!(row.state, PrMonitorState::Active);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_message WHERE agent_id = ?")
            .bind(&child.0)
            .fetch_one(svc.store().write_pool())
            .await
            .expect("count child rows");
        assert_eq!(rows, 1, "no wake without a transfer");
    }

    /// Only the DIRECT parent may take over: the parent's own parent is
    /// refused even though the holder has settled.
    #[tokio::test]
    async fn a_grandparent_is_refused_a_settled_grandchilds_monitor() {
        let (_db, _root, svc, _forge, ws, grandparent) = setup().await;
        let parent = child_agent(
            &svc,
            &ws,
            "agent-parent",
            &grandparent,
            AgentStatus::RuntimeIdle,
        )
        .await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::RuntimeIdle).await;
        let first = register(&svc, &ws, &child).await;

        let refused = svc
            .pr_monitor_start_op(&ws, &grandparent, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(child.to_string()));
        assert!(
            !refused["instruction"]
                .as_str()
                .unwrap()
                .contains("sub-agent"),
            "{refused}"
        );
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, child, "not re-parented");
    }

    /// Settlement only opens the monitor to the DIRECT parent: a sibling
    /// (or any non-parent) gets the ordinary refusal, no sub-agent wording.
    #[tokio::test]
    async fn a_non_parent_is_refused_a_settled_childs_monitor() {
        let (_db, _root, svc, _forge, ws, parent) = setup().await;
        let child = child_agent(&svc, &ws, "agent-child", &parent, AgentStatus::RuntimeIdle).await;
        let first = register(&svc, &ws, &child).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;

        let refused = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("payload");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        let instruction = refused["instruction"].as_str().unwrap();
        assert!(
            instruction.contains("by agent Child (agent-child);"),
            "{instruction}"
        );
        assert!(!instruction.contains("sub-agent"), "{instruction}");
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.agent_id, child, "not re-parented");
    }

    /// Adoption counts against the adopter's own cap, and the direct-service
    /// path adopts too (no `InvalidParams` for a dead owner).
    #[tokio::test]
    async fn adoption_honors_the_adopter_cap_and_the_service_path() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitors_max_per_agent(1);
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        svc.pr_monitor_register(&ws, &sibling, "o", "r", 7)
            .await
            .expect("sibling's own monitor");
        kill_owner(&svc, &ws, &owner, OwnerDeath::Error).await;

        let err = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
            .await
            .expect_err("at cap");
        assert!(err.to_string().contains("max 1"), "{err}");
        let still = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(still.agent_id, owner, "not adopted while at cap");

        let third = second_agent(&svc, &ws, "agent-third").await;
        let (adopted, _) = svc
            .pr_monitor_register(&ws, &third, "o", "r", 42)
            .await
            .expect("service path adopts");
        assert_eq!(adopted.monitor_id, first.monitor_id);
        assert_eq!(adopted.agent_id, third);
    }

    /// Terminalization is ONE guarded write (`Store::complete_pr_monitor`,
    /// whose guard has its own store test): a sweep holding the dead
    /// owner's pre-adoption image cannot complete the row an adoption
    /// re-parented (its final wake would go to the dead owner). The stale
    /// image's completion is a no-op — row still active under the adopter,
    /// no `prMonitor:completed`, nobody woken — and the next real poll
    /// completes the monitor under the adopter, waking the adopter.
    #[tokio::test]
    async fn a_stale_sweep_image_cannot_complete_an_adopted_monitor() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        // The sweep's image: the row as the poll loop read it.
        let stale = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();

        kill_owner(&svc, &ws, &owner, OwnerDeath::Error).await;
        let (adopted, _) = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
            .await
            .expect("adopt");
        assert_eq!(adopted.agent_id, sibling);
        assert_ne!(adopted.updated_at, stale.updated_at, "the row moved");

        let merged = snapshot(|s| s.requirements.state = "merged".into());
        assert!(
            !svc.complete_pr_monitor(&stale, &merged)
                .await
                .expect("stale complete"),
            "the stale image loses the guarded write"
        );
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.state, PrMonitorState::Active, "not completed: {row:?}");
        assert_eq!(row.agent_id, sibling, "still the adopter's");
        assert!(
            !owner_messages(&svc, &owner)
                .await
                .contains("[PR monitor o/r#42]"),
            "the dead owner is never woken"
        );
        assert!(
            !owner_messages(&svc, &sibling)
                .await
                .contains("[PR monitor o/r#42]"),
            "no wake without a completion"
        );
        let completed_events = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![PR_MONITOR_COMPLETED.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(completed_events.is_empty(), "{completed_events:?}");

        // The next real sweep completes it under the adopter.
        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(row.state, PrMonitorState::Completed);
        assert_eq!(row.agent_id, sibling);
        assert!(
            owner_messages(&svc, &sibling)
                .await
                .contains("[PR monitor o/r#42]"),
            "the adopter gets the final wake"
        );
        assert!(!owner_messages(&svc, &owner)
            .await
            .contains("[PR monitor o/r#42]"));
    }

    /// Adoption consumes the boot-rehydration catch-up marker: the marker
    /// belongs to the dead owner's pre-restart backlog, which the adoption's
    /// fresh baseline discards, so the adopter's first change is debounced
    /// like any other instead of firing on the next poll.
    #[tokio::test]
    async fn adoption_consumes_the_restart_catch_up_marker() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        kill_owner(&svc, &ws, &owner, OwnerDeath::Error).await;

        // A restart: the failed owner's monitor is rehydrated (not swept —
        // only deleted/retired/gone owners are) and marked for catch-up.
        assert_eq!(svc.rehydrate_pr_monitors().await.expect("rehydrate"), 1);
        assert!(
            svc.pr_monitor_catch_up
                .lock()
                .unwrap()
                .contains_key(&first.monitor_id),
            "marked for catch-up"
        );

        let (adopted, _) = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
            .await
            .expect("adopt");
        assert_eq!(adopted.agent_id, sibling);
        assert!(
            !svc.pr_monitor_catch_up
                .lock()
                .unwrap()
                .contains_key(&first.monitor_id),
            "adoption consumed the marker"
        );

        // The adopter's first change holds for the debounce window.
        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        let row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert!(!row.pending_changes.is_empty(), "pending: {row:?}");
        assert!(
            !owner_messages(&svc, &sibling)
                .await
                .contains("[PR monitor o/r#42]"),
            "debounced, not fired as catch-up"
        );
    }

    /// Only ACTIVE monitors block: once the owner cancels (or the monitor
    /// completes on merge), another agent registers successfully.
    #[tokio::test]
    async fn a_cancelled_or_completed_monitor_no_longer_blocks_another_agent() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;

        // Cancelled by the owner (`ws.pr.unmonitor`) → sibling registers.
        register(&svc, &ws, &owner).await;
        svc.pr_monitor_stop_op(&ws, &owner, 42, None)
            .await
            .expect("owner unmonitor");
        let taken = svc
            .pr_monitor_start_op(&ws, &sibling, 42, None)
            .await
            .expect("sibling register after cancel");
        assert_eq!(taken["ok"], json!(true), "{taken}");
        assert_eq!(taken["monitor"]["agentId"], json!(sibling.to_string()));
        // ...and now the roles are reversed: the owner is refused.
        let refused = svc
            .pr_monitor_start_op(&ws, &owner, 42, None)
            .await
            .expect("refusal");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(sibling.to_string()));

        // Completed on merge → the PR is unmonitored again in the workspace.
        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.pr_state = PrState::Open);
        let again = svc
            .pr_monitor_start_op(&ws, &owner, 42, None)
            .await
            .expect("register after completion");
        assert_eq!(again["ok"], json!(true), "{again}");
        assert_eq!(again["monitor"]["agentId"], json!(owner.to_string()));
    }

    /// The uniqueness is per WORKSPACE: the same PR monitored from two
    /// workspaces is two independent monitors.
    #[tokio::test]
    async fn the_same_pr_in_another_workspace_is_not_a_duplicate() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        register(&svc, &ws, &owner).await;
        let (ws2, other) = sibling_workspace(&svc, "agent-elsewhere").await;

        let started = svc
            .pr_monitor_start_op(&ws2, &other, 42, None)
            .await
            .expect("cross-workspace register");
        assert_eq!(started["ok"], json!(true), "{started}");
        assert_eq!(started["monitor"]["agentId"], json!(other.to_string()));
        assert_eq!(
            svc.pr_monitor_list_op(&ws, None).await.unwrap()["monitors"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            svc.pr_monitor_list_op(&ws2, None).await.unwrap()["monitors"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    /// Forge repo slugs are case-insensitive: a `repo` override that differs
    /// from the monitored slug only by case is the SAME PR — the owner's
    /// re-register re-arms the existing row (no second monitor, stored
    /// casing untouched), a sibling's register is refused naming the owner,
    /// and the owner's `ws.pr.unmonitor` under the variant cancels the row.
    #[tokio::test]
    async fn repo_slug_case_variants_identify_the_same_monitor() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;

        let rearmed = svc
            .pr_monitor_start_op(&ws, &owner, 42, Some("O/R".into()))
            .await
            .expect("owner re-register under a case variant");
        assert_eq!(rearmed["ok"], json!(true), "{rearmed}");
        assert_eq!(rearmed["monitor"]["monitorId"], json!(first.monitor_id));
        assert_eq!(
            rearmed["monitor"]["repo"],
            json!("o/r"),
            "the stored casing is what the row reports"
        );
        assert_eq!(
            svc.pr_monitors_for_agent(&owner).await.unwrap().len(),
            1,
            "no second monitor"
        );

        let refused = svc
            .pr_monitor_start_op(&ws, &sibling, 42, Some("O/r".into()))
            .await
            .expect("refusal is a payload");
        assert_eq!(refused["refused"], json!(true), "{refused}");
        assert_eq!(refused["ownerAgentId"], json!(owner.to_string()));
        assert_eq!(refused["monitorId"], json!(first.monitor_id));

        let stopped = svc
            .pr_monitor_stop_op(&ws, &owner, 42, Some("o/R".into()))
            .await
            .expect("owner unmonitor under a case variant");
        assert_eq!(stopped["monitor"]["monitorId"], json!(first.monitor_id));
        assert_eq!(stopped["monitor"]["state"], json!("cancelled"));
        assert!(svc
            .store()
            .find_active_pr_monitor_in_workspace(&ws, "o", "r", 42)
            .await
            .unwrap()
            .is_none());
    }

    /// The sweep's per-PR fetch key folds slug case, so monitors on case
    /// variants of one PR (two workspaces here — one workspace never holds
    /// two) share a single forge fetch per tick.
    #[tokio::test]
    async fn case_variant_monitors_share_one_fetch_per_sweep() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        svc.pr_monitor_register(&ws, &owner, "o", "r", 42)
            .await
            .expect("register");
        let (ws2, other) = sibling_workspace(&svc, "agent-elsewhere").await;
        svc.pr_monitor_register(&ws2, &other, "O", "R", 42)
            .await
            .expect("register case variant");

        let before = forge.fetches();
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.fetches() - before,
            1,
            "one shared fetch for the case-variant pair"
        );
    }

    #[tokio::test]
    async fn multiple_changes_coalesce_into_one_debounced_wake() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // A long window so the first two polls only recompute the net set.
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        forge.edit(|s| {
            s.approvals.push("reviewer".into());
            s.checks[0].state = CheckState::Success;
        });
        svc.poll_pr_monitors().await;

        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            held.pending_changes.len() >= 3,
            "the coalesced set covers both polls' changes: {:?}",
            held.pending_changes
        );
        assert!(held.pending_since.is_some());
        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "no wake while the PR is still churning"
        );

        // The window closes: exactly ONE consolidated wake carries everything.
        let svc = svc.with_pr_monitor_debounce_seconds(MIN_PR_MONITOR_DEBOUNCE_SECONDS);
        let stale = now_iso();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: held.last_snapshot.as_deref(),
                    baseline_snapshot: held.baseline_snapshot.as_deref(),
                    pending_changes: &held.pending_changes,
                    pending_since: Some("2020-01-01T00:00:00Z"),
                    last_change_at: Some("2020-01-01T00:00:00Z"),
                    last_polled_at: Some(&stale),
                    last_error: None,
                    updated_at: &stale,
                    expected_updated_at: &held.updated_at,
                },
            )
            .await
            .unwrap());
        svc.poll_pr_monitors().await;

        let text = owner_messages(&svc, &owner).await;
        assert_eq!(
            text.matches("[PR monitor o/r#42]").count(),
            1,
            "exactly one consolidated wake: {text}"
        );
        assert!(text.contains("new approval"), "{text}");
        assert!(text.contains("conversation comment"), "{text}");
        assert!(text.contains("Where the PR stands now"), "{text}");
        let drained = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(drained.pending_changes.is_empty(), "debounce state reset");
        assert!(drained.pending_since.is_none());
        assert_eq!(
            drained.baseline_snapshot, drained.last_snapshot,
            "emit advanced the baseline to the delivered snapshot"
        );
    }

    /// The coalescing property itself: a PR that churns A→B→A within one
    /// debounce window nets to an EMPTY pending set — no wake, anchors reset
    /// — and the FE's `PR_MONITOR_CHANGED` stream reflects the shrink to
    /// empty. Covers comment-count fluctuations that net to zero and a check
    /// removed then re-added with the same status.
    #[tokio::test]
    async fn a_full_revert_within_the_window_nets_to_no_wake() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // B: comments +2 and the only check removed.
        forge.edit(|s| {
            s.conversation_comments = 2;
            s.checks.clear();
        });
        svc.poll_pr_monitors().await;
        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(!held.pending_changes.is_empty(), "changes pending after B");
        assert!(held.pending_since.is_some());

        // Back to A: comments deleted, check re-added with the same status.
        forge.edit(|s| {
            s.conversation_comments = 0;
            s.checks = ForgeState::default().checks;
        });
        svc.poll_pr_monitors().await;
        let reverted = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            reverted.pending_changes.is_empty(),
            "a full revert empties the coalesced set: {:?}",
            reverted.pending_changes
        );
        assert!(reverted.pending_since.is_none(), "anchors reset");
        assert!(reverted.last_change_at.is_none(), "anchors reset");

        // Even with the window elapsed nothing emits — there is no pending
        // state left by construction.
        svc.clone()
            .with_pr_monitor_debounce_seconds(MIN_PR_MONITOR_DEBOUNCE_SECONDS)
            .poll_pr_monitors()
            .await;
        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "no wake for a PR that ended up back where it started"
        );

        // The changed-event stream tracked the net set, including the final
        // shrink to empty.
        let events = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![PR_MONITOR_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "one event per net-set change");
        assert_eq!(
            events[0].data["changes"],
            json!([]),
            "the newest event reports the shrink to empty"
        );
    }

    /// A field that moves A→B→C within one window reports a single
    /// `A → C` line — never the intermediate transitions. The intermediate
    /// state here is a (suppressed) success plus its completion aggregate;
    /// the recompute against the baseline drops both once the check fails.
    #[tokio::test]
    async fn a_field_that_moves_twice_reports_a_single_net_line() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.checks[0].state = CheckState::Success);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.checks[0].state = CheckState::Failure);
        svc.poll_pr_monitors().await;

        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let check_lines: Vec<_> = held
            .pending_changes
            .iter()
            .filter(|c| c.starts_with("check build"))
            .collect();
        assert_eq!(
            check_lines,
            vec!["check build: pending → failed"],
            "single net line, no intermediate transitions: {:?}",
            held.pending_changes
        );
        assert!(
            !held
                .pending_changes
                .iter()
                .any(|c| c.contains("all checks passed")),
            "the intermediate all-green aggregate is dropped on recompute: {:?}",
            held.pending_changes
        );

        // The delivered wake renders the same net line.
        let stale = now_iso();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: held.last_snapshot.as_deref(),
                    baseline_snapshot: held.baseline_snapshot.as_deref(),
                    pending_changes: &held.pending_changes,
                    pending_since: Some("2020-01-01T00:00:00Z"),
                    last_change_at: Some("2020-01-01T00:00:00Z"),
                    last_polled_at: Some(&stale),
                    last_error: None,
                    updated_at: &stale,
                    expected_updated_at: &held.updated_at,
                },
            )
            .await
            .unwrap());
        svc.poll_pr_monitors().await;
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("check build: pending → failed"), "{text}");
        assert!(!text.contains("pending → passed"), "{text}");
        assert!(!text.contains("passed → failed"), "{text}");
        assert!(!text.contains("all checks passed"), "{text}");
    }

    /// Suppression composed with coalescing: a suppressed intermediate
    /// success contributes only the completion aggregate to the net set,
    /// and that aggregate survives recomputation when a later poll adds
    /// an unrelated change.
    #[tokio::test]
    async fn completion_aggregate_survives_recompute_alongside_later_changes() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.checks[0].state = CheckState::Success);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;

        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            held.pending_changes
                .iter()
                .any(|c| c == "all checks passed (1)"),
            "the aggregate line persists across recomputes: {:?}",
            held.pending_changes
        );
        assert!(
            held.pending_changes
                .iter()
                .any(|c| c.contains("conversation comment")),
            "the later change joins the same net set: {:?}",
            held.pending_changes
        );
        assert!(
            !held.pending_changes.iter().any(|c| c.starts_with("check ")),
            "no per-check success line anywhere in the set: {:?}",
            held.pending_changes
        );
    }

    /// A flush racing a revert: the coalesced set already emptied, so the
    /// flush is a no-op (`Ok(false)`) and no wake is sent.
    #[tokio::test]
    async fn flush_with_an_empty_coalesced_set_is_a_noop() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.conversation_comments = 0);
        svc.poll_pr_monitors().await;

        assert!(
            !svc.pr_monitor_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "nothing pending after the revert"
        );
        assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
    }

    /// The terminal wake's "Changes since the last report" section coalesces
    /// against the emit baseline: churn that reverted before the merge does
    /// not replay in the final wake.
    #[tokio::test]
    async fn terminal_wake_coalesces_changes_since_the_last_report() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        register(&svc, &ws, &owner).await;

        // Churn that fully reverts, then the check flips twice and the PR
        // merges: the final wake nets to the completion aggregate (the
        // per-check success is suppressed) and never mentions the reverted
        // comments or the intermediate failure.
        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        forge.edit(|s| {
            s.conversation_comments = 0;
            s.checks[0].state = CheckState::Failure;
        });
        svc.poll_pr_monitors().await;
        forge.edit(|s| {
            s.checks[0].state = CheckState::Success;
            s.pr_state = PrState::Merged;
        });
        svc.poll_pr_monitors().await;

        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("was MERGED"), "{text}");
        assert!(text.contains("Changes since the last report"), "{text}");
        assert!(text.contains("state: open → merged"), "{text}");
        assert!(text.contains("all checks passed (1)"), "{text}");
        assert!(!text.contains("check build"), "{text}");
        assert!(!text.contains("conversation comment"), "{text}");
        assert!(!text.contains("pending → failed"), "{text}");
    }

    #[tokio::test]
    async fn merge_stops_monitoring_with_an_immediate_final_wake() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // A window long enough that a debounced path would emit nothing.
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;

        let completed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(
            completed.state,
            PrMonitorState::Completed,
            "row is retained in completed state"
        );
        assert!(completed.pending_changes.is_empty());
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("was MERGED"), "{text}");
        assert!(text.contains("Monitoring has STOPPED"), "{text}");
        // The wake's messageMetadata carries the PR url from the baseline.
        assert!(text.contains("pr_monitor_wake"), "{text}");
        assert!(
            text.contains(r#""url":"https://github.com/o/r/pull/42""#),
            "{text}"
        );
        // Completed rows stay visible; the loop no longer polls them.
        assert_eq!(svc.pr_monitors_for_agent(&owner).await.unwrap().len(), 1);
        assert!(svc
            .store()
            .load_active_pr_monitors()
            .await
            .unwrap()
            .is_empty());
    }

    /// Terminalizing a monitor also refreshes the owning workspace's PR
    /// linkage (intent-hq/monorepo#2094): a linked PR that merges flips the
    /// persisted `prStatus` within the monitor's poll cadence — no explicit
    /// `pr.refresh` call — instead of waiting for the slower background
    /// refresh sweep tier.
    #[tokio::test]
    async fn terminal_completion_refreshes_the_workspace_pr_linkage() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // Link the fixture workspace to the monitored PR; the branch matches
        // the stub PR's head ref so the refresh takes the update path.
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        row.branch = "feature".into();
        row.pr_number = Some(42);
        row.pr_url = Some("https://github.com/o/r/pull/42".into());
        row.pr_status = Some(intent_core::PullRequestStatus::Open);
        svc.store().update_workspace(&row).await.unwrap();
        register(&svc, &ws, &owner).await;

        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;

        let after = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(
            after.pr_status,
            Some(intent_core::PullRequestStatus::Merged),
            "terminal wake refreshed the linkage without an explicit pr.refresh"
        );
        assert_eq!(after.pr_number, Some(42), "link retained");
        assert_eq!(
            after
                .active_pull_request
                .expect("active PR persisted")
                .status,
            intent_core::PullRequestStatus::Merged
        );
        // The refresh emitted the linkage delta on the event bus.
        let evs = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![intent_core::events::PR_UPDATED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query pr:updated events");
        assert!(
            !evs.is_empty(),
            "pr:updated emitted by the terminal refresh"
        );
    }

    #[test]
    fn wake_metadata_carries_the_pr_url_and_omits_it_without_a_baseline() {
        let now = now_iso();
        let mut m = PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: WorkspaceId::from("ws-1"),
            agent_id: AgentId::from("agent-1"),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: Some(serde_json::to_string(&snapshot(|_| {})).unwrap()),
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        };

        let metadata = pr_monitor_wake_metadata(&m, "changed", None);
        assert_eq!(metadata["type"], json!("pr_monitor_wake"));
        assert_eq!(metadata["repo"], json!("o/r"));
        assert_eq!(metadata["prNumber"], json!(42));
        assert_eq!(metadata["reason"], json!("changed"));
        assert_eq!(metadata["url"], json!("https://github.com/o/r/pull/42"));
        assert!(metadata.get("pausedUntil").is_none(), "{metadata}");

        // No baseline yet: the key is ABSENT, never null.
        m.last_snapshot = None;
        let metadata = pr_monitor_wake_metadata(&m, "cancelled", None);
        assert!(metadata.get("url").is_none(), "{metadata}");
        assert_eq!(metadata["reason"], json!("cancelled"));

        // While the global rate-limit pause is active the wake names it.
        let metadata = pr_monitor_wake_metadata(&m, "cancelled", Some("2026-09-17T02:39:15Z"));
        assert_eq!(metadata["pausedUntil"], json!("2026-09-17T02:39:15Z"));
    }

    #[tokio::test]
    async fn close_stops_monitoring_and_names_the_reason() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        forge.edit(|s| s.pr_state = PrState::Closed);
        svc.poll_pr_monitors().await;
        let completed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(completed.state, PrMonitorState::Completed);
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("was CLOSED without merging"), "{text}");
    }

    #[tokio::test]
    async fn flush_emits_the_pending_wake_immediately_and_is_a_noop_when_idle() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        assert!(
            !svc.pr_monitor_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "nothing pending yet"
        );

        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        assert!(svc
            .pr_monitor_flush(&ws, &monitor.monitor_id)
            .await
            .unwrap());

        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("[PR monitor o/r#42]"), "{text}");
        let drained = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(drained.pending_changes.is_empty());
        assert!(
            !svc.pr_monitor_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "second flush is a no-op"
        );
    }

    /// `check: true` re-polls on demand: a change the loop has NOT seen yet
    /// is fetched fresh and the wake delivered immediately, bypassing the
    /// debounce window entirely.
    #[tokio::test]
    async fn check_and_flush_repolls_and_delivers_unseen_changes_immediately() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // The change lands AFTER the last sweep — a plain flush sees nothing.
        forge.edit(|s| s.conversation_comments = 1);
        assert!(
            !svc.pr_monitor_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "plain flush has no pending set to deliver"
        );

        assert!(svc
            .pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
            .await
            .unwrap());
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("[PR monitor o/r#42]"), "{text}");
        assert!(text.contains("conversation comment"), "{text}");

        // The emit baseline advanced: pending drained, nothing left to flush.
        let drained = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(drained.pending_changes.is_empty());
        assert!(drained.last_polled_at.is_some());
    }

    /// `check: true` with nothing changed vs. the emit baseline: no wake,
    /// `Ok(false)` — but the poll still stamps `lastPolledAt`.
    #[tokio::test]
    async fn check_and_flush_with_no_changes_is_a_noop() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        assert!(
            !svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "nothing changed vs. the baseline"
        );
        assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(row.pending_changes.is_empty());
        assert!(row.last_polled_at.is_some(), "the on-demand poll stamped");
    }

    /// `check: true` on a PR that merged since the last sweep terminalizes
    /// through the normal path: `completed` state, immediate final wake.
    #[tokio::test]
    async fn check_and_flush_terminalizes_a_merged_pr() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.pr_state = PrState::Merged);
        assert!(
            svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "the terminal final wake counts as flushed"
        );
        let completed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(completed.state, PrMonitorState::Completed);
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("was MERGED"), "{text}");

        // A later check on the completed row is a plain no-op.
        assert!(!svc
            .pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
            .await
            .unwrap());
    }

    /// A forge fetch failure during the check records `lastError` and
    /// propagates the error — matching the wire layer's error-shape
    /// conventions — without touching the baseline.
    #[tokio::test]
    async fn check_and_flush_records_last_error_on_forge_failure() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let baseline = monitor.last_snapshot.clone();

        forge.edit(|s| s.fail_get_pr = true);
        let err = svc
            .pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
            .await
            .expect_err("forge down surfaces as an error");
        assert!(err.to_string().contains("forge down"), "{err}");
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Active, "still active");
        assert!(row.last_error.is_some(), "lastError recorded");
        assert_eq!(row.last_snapshot, baseline, "baseline untouched");
        assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
    }

    /// The wire op: `check: false` preserves the exact existing semantics,
    /// `check: true` folds the on-demand poll in.
    #[tokio::test]
    async fn flush_op_with_check_repolls_and_without_check_is_unchanged() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| s.conversation_comments = 1);
        // No check: the unseen change stays invisible.
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
                .await
                .unwrap(),
            json!({ "ok": true, "flushed": false })
        );
        // Check: the re-poll picks it up and the wake goes out now.
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, true)
                .await
                .unwrap(),
            json!({ "ok": true, "flushed": true })
        );
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, true)
                .await
                .unwrap(),
            json!({ "ok": true, "flushed": false })
        );
    }

    #[tokio::test]
    async fn cancel_is_agent_owned_and_only_the_app_path_notifies() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let mine = register(&svc, &ws, &owner).await;

        let stranger = AgentId::from("agent-other");
        let err = svc
            .pr_monitor_cancel(&ws, &mine.monitor_id, Some(&stranger))
            .await
            .expect_err("non-owner rejected");
        assert!(err.to_string().contains("owned by agent"), "{err}");

        // An agent cancelling its OWN monitor gets no self-wake.
        let cancelled = svc
            .pr_monitor_cancel(&ws, &mine.monitor_id, Some(&owner))
            .await
            .expect("owner cancel");
        assert_eq!(cancelled.state, PrMonitorState::Cancelled);
        assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
        // Cancelled rows leave the list surfaces.
        assert!(svc.pr_monitors_for_agent(&owner).await.unwrap().is_empty());
        assert!(svc.pr_monitors_for_workspace(&ws).await.unwrap().is_empty());

        // The FE path notifies the owning agent.
        let second = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .expect("register")
            .0;
        svc.pr_monitor_cancel(&ws, &second.monitor_id, None)
            .await
            .expect("app cancel");
        let text = owner_messages(&svc, &owner).await;
        assert!(text.contains("cancelled from the app"), "{text}");
    }

    /// Insert a second agent in the fixture workspace (for a monitor on a
    /// DIFFERENT PR — a workspace holds one active monitor per PR).
    async fn second_agent(svc: &Services, ws: &WorkspaceId, id: &str) -> AgentId {
        svc.store()
            .insert_agent_session(&agent(ws, id))
            .await
            .expect("second agent");
        AgentId::from(id)
    }

    /// A second workspace on the same `o/r` repo with its own agent, so a
    /// sibling monitor can watch the SAME PR: monitors are unique per
    /// (workspace, repo, pr), and the shared-fetch sweep still groups
    /// siblings across workspaces.
    async fn sibling_workspace(svc: &Services, id: &str) -> (WorkspaceId, AgentId) {
        let ws = WorkspaceId::new();
        svc.store()
            .insert_workspace(&workspace(&ws))
            .await
            .expect("sibling workspace");
        svc.store()
            .insert_agent_session(&agent(&ws, id))
            .await
            .expect("sibling agent");
        (ws, AgentId::from(id))
    }

    #[tokio::test]
    async fn sweep_fetches_each_pr_once_across_sibling_monitors() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let first = register(&svc, &ws, &owner).await;
        let (ws2, sibling) = sibling_workspace(&svc, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws2, &sibling, "o", "r", 42)
            .await
            .expect("sibling register")
            .0;
        // A third monitor on a DIFFERENT PR still gets its own fetch.
        let other = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .expect("other pr")
            .0;

        forge.edit(|s| s.conversation_comments = 1);
        let before = forge.fetches();
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.fetches() - before,
            2,
            "one fetch for o/r#42 shared by both monitors, one for o/r#7"
        );

        // Both siblings advanced their own baselines from the shared fetch.
        for id in [&first.monitor_id, &second.monitor_id, &other.monitor_id] {
            let row = svc.store().get_pr_monitor(id).await.unwrap();
            assert!(
                !row.pending_changes.is_empty(),
                "monitor {} saw the change: {:?}",
                id.0,
                row.pending_changes
            );
        }
    }

    /// The sub-reads a fingerprint-unchanged poll is expected to skip.
    const SUB_FETCHES: [&str; 4] = [
        "merge_requirements",
        "list_reviews",
        "get_review_threads",
        "list_comments",
    ];

    fn sub_fetch_totals(forge: &StubForge) -> Vec<(&'static str, usize)> {
        SUB_FETCHES
            .iter()
            .map(|m| (*m, forge.sub_fetches(m)))
            .collect()
    }

    /// Quiet PR: three consecutive sweeps issue three `get_pr` reads but
    /// exactly ONE set of sub-reads — the first sweep fetches fully and the
    /// next two reuse it because the fingerprint did not move.
    #[tokio::test]
    async fn unchanged_fingerprint_polls_reuse_the_previous_sub_fetches() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        let get_pr_before = forge.fetches();
        let subs_before = sub_fetch_totals(&forge);
        for _ in 0..3 {
            svc.poll_pr_monitors().await;
        }
        assert_eq!(forge.fetches() - get_pr_before, 3, "get_pr every poll");
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(after - before, 1, "{method}: one full fetch, two reused");
        }
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Active);
        assert!(row.pending_changes.is_empty(), "nothing moved");
        assert!(row.last_error.is_none());
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 1);
    }

    /// A change the forge reflects in the PR record (a review, which bumps
    /// `updatedAt`) forces the full fetch on the very next poll, and the
    /// monitor sees the change.
    #[tokio::test]
    async fn a_moved_fingerprint_refetches_fully_and_the_monitor_sees_the_change() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;
        svc.poll_pr_monitors().await;

        forge.edit(|s| s.approvals = vec!["reviewer".into()]);
        let subs_before = sub_fetch_totals(&forge);
        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(
                after - before,
                1,
                "{method}: re-fetched after the fingerprint moved"
            );
        }
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            row.pending_changes
                .iter()
                .any(|l| l.to_lowercase().contains("approv")),
            "approval reported: {:?}",
            row.pending_changes
        );
    }

    /// Check-run movement does not bump the PR's `updatedAt`, so a cheap
    /// poll cannot see it; the poll-count bound guarantees a full fetch after
    /// at most [`PR_MONITOR_MAX_CHEAP_POLLS`] reused polls, which picks the
    /// change up.
    #[tokio::test]
    async fn the_poll_count_bound_forces_a_full_fetch_on_a_quiet_pr() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        // Full fetch seeding the cache.
        svc.poll_pr_monitors().await;

        forge.edit_quiet(|s| s.checks[0].state = CheckState::Failure);
        let subs_before = sub_fetch_totals(&forge);
        for i in 0..PR_MONITOR_MAX_CHEAP_POLLS {
            svc.poll_pr_monitors().await;
            let row = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            assert!(
                row.pending_changes.is_empty(),
                "cheap poll {i} reuses the cached checklist: {:?}",
                row.pending_changes
            );
        }
        assert_eq!(
            sub_fetch_totals(&forge),
            subs_before,
            "no sub-read during the reused polls"
        );

        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(after - before, 1, "{method}: the bound forced a full fetch");
        }
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            row.pending_changes.iter().any(|l| l.contains("build")),
            "the check failure surfaced: {:?}",
            row.pending_changes
        );
    }

    /// The age bound: a cached full fetch older than
    /// [`PR_MONITOR_MAX_CHEAP_AGE`] is not reused even when the fingerprint
    /// is unchanged and the poll count is under its cap.
    #[tokio::test]
    async fn the_age_bound_forces_a_full_fetch_on_a_quiet_pr() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;

        svc.poll_pr_monitors().await;
        let subs_after_cheap = sub_fetch_totals(&forge);
        svc.backdate_pr_monitor_fetch_cache(PR_MONITOR_MAX_CHEAP_AGE + Duration::from_secs(1));
        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_after_cheap.iter().zip(sub_fetch_totals(&forge))
        {
            assert_eq!(
                after - before,
                1,
                "{method}: the age bound forced a full fetch"
            );
        }
    }

    /// A degraded full fetch (a sub-read that failed) is never reused: the
    /// next poll re-issues the sub-reads even though the fingerprint is
    /// unchanged, so a transient degradation cannot persist on a quiet PR.
    #[tokio::test]
    async fn a_degraded_full_fetch_is_not_reused() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        register(&svc, &ws, &owner).await;
        forge.edit(|s| s.fail_list_comments = true);
        svc.poll_pr_monitors().await;

        let before = forge.sub_fetches("list_comments");
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.sub_fetches("list_comments") - before,
            1,
            "the degraded comment read is retried, not carried forward"
        );
    }

    /// A forge that reports no `updatedAt` never gets a cheap poll: the
    /// remaining fingerprint fields do not move on a comment, so reusing the
    /// sub-reads would hide it. Every poll re-issues the sub-reads and a
    /// quiet comment bump surfaces on the very next one (regression:
    /// intent-hq/intentd#1923 merge-queue ejection — the WSS e2e forge
    /// serves an empty `updated_at`).
    #[tokio::test]
    async fn a_forge_without_updated_at_never_gets_a_cheap_poll() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        forge.edit(|s| s.updated_at = Some(String::new()));
        let monitor = register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;

        let subs_before = sub_fetch_totals(&forge);
        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(
                after - before,
                1,
                "{method}: re-fetched although the fingerprint is unchanged"
            );
        }

        forge.edit_quiet(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(
            row.pending_changes,
            vec!["+1 conversation comment (1 total)".to_string()],
            "the quiet comment surfaced on the next poll"
        );
    }

    /// A checklist sub-read that degraded while the probe still answered —
    /// `list_reviews` (zero approvals) or `get_review_threads` (unknown
    /// resolution) — makes the fetch incomplete: the next poll re-issues
    /// every sub-read even though the fingerprint is unchanged, and once the
    /// read recovers (quietly — no `updatedAt` bump) the poll sees the
    /// signal the degraded checklist lacked.
    #[tokio::test]
    async fn a_fetch_with_a_degraded_review_or_thread_read_is_not_reused() {
        for toggle in [
            (|s: &mut ForgeState, on: bool| s.fail_list_reviews = on) as fn(&mut ForgeState, bool),
            |s: &mut ForgeState, on: bool| s.fail_get_review_threads = on,
        ] {
            let (_db, _root, svc, forge, ws, owner) = setup().await;
            let svc = svc.with_pr_monitor_debounce_seconds(3600);
            forge.edit(|s| {
                s.approvals = vec!["reviewer".into()];
                toggle(s, true);
            });
            let monitor = register(&svc, &ws, &owner).await;
            svc.poll_pr_monitors().await;
            let baseline = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            assert!(baseline.pending_changes.is_empty(), "degraded but quiet");

            forge.edit_quiet(|s| toggle(s, false));
            let subs_before = sub_fetch_totals(&forge);
            svc.poll_pr_monitors().await;
            for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
                assert_eq!(after - before, 1, "{method}: degraded fetch not reused");
            }
            let row = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            let last: PrMonitorSnapshot =
                serde_json::from_str(row.last_snapshot.as_deref().unwrap()).unwrap();
            assert_eq!(
                last.requirements.approvals.have, 1,
                "the recovered review read is reflected"
            );
            assert!(
                last.requirements.threads.unresolved.is_some(),
                "the recovered thread read is reflected"
            );

            // Complete now: the following poll is cheap again.
            let subs_before = sub_fetch_totals(&forge);
            svc.poll_pr_monitors().await;
            assert_eq!(
                sub_fetch_totals(&forge),
                subs_before,
                "complete fetch reused"
            );
        }
    }

    /// An on-demand full fetch (check-now) invalidates the sweep's cached
    /// fetch: a check that moved quietly (no `updatedAt` bump) and was
    /// delivered by check-now must not be "reversed" on the next sweep by
    /// the older cached checklist — that sweep fetches fully instead.
    #[tokio::test]
    async fn check_now_invalidates_the_sweep_fetch_cache() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 1, "seeded");

        forge.edit_quiet(|s| s.checks[0].state = CheckState::Failure);
        svc.poll_pr_monitors().await;
        assert!(
            svc.store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap()
                .pending_changes
                .is_empty(),
            "the cheap poll cannot see the quiet check movement"
        );

        assert!(
            svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "check-now fetches fully and delivers the failure"
        );
        assert!(owner_messages(&svc, &owner).await.contains("build"));
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 0, "invalidated");

        let subs_before = sub_fetch_totals(&forge);
        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(after - before, 1, "{method}: full fetch after check-now");
        }
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            row.pending_changes.is_empty(),
            "no false reversal from the stale cache: {:?}",
            row.pending_changes
        );
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 1, "re-seeded");
    }

    /// Re-registration (the same agent re-arming its monitor) is an
    /// on-demand full fetch too, and invalidates the slot the same way.
    #[tokio::test]
    async fn re_registration_invalidates_the_sweep_fetch_cache() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 1);

        register(&svc, &ws, &owner).await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 0, "invalidated");
        let subs_before = sub_fetch_totals(&forge);
        svc.poll_pr_monitors().await;
        for ((method, before), (_, after)) in subs_before.iter().zip(sub_fetch_totals(&forge)) {
            assert_eq!(after - before, 1, "{method}: full fetch after re-register");
        }
    }

    /// The generation guard: a sweep fetch that was in flight when an
    /// on-demand fetch invalidated the slot must not repopulate it with
    /// its own (potentially older) result.
    #[tokio::test]
    async fn an_in_flight_sweep_fetch_does_not_repopulate_an_invalidated_slot() {
        let forge = StubForge::new();
        let cache: PrMonitorFetchCache = Arc::default();
        let repo = RepoRef::new("o", "r");
        let key = pr_key_for(&repo, 42);
        let key_for_hook = key.clone();
        let cache_for_hook = cache.clone();
        // The on-demand fetch "completes" while the sweep's `get_pr` runs.
        forge.set_on_get_pr(Some(Box::new(move |_| {
            invalidate_fetch_cache(&cache_for_hook, &key_for_hook);
        })));
        fetch_shared_snapshot_cached(&forge, &repo, 42, &cache, &key)
            .await
            .expect("fetch");
        let guard = cache.lock().unwrap();
        let slot = guard.get(&key).expect("slot");
        assert!(slot.entry.is_none(), "superseded result not cached");
        assert_eq!(slot.generation, 1);
    }

    // -----------------------------------------------------------------------
    // Forge requests per fetch — the folded observation vs the per-signal
    // reads (the table on `fetch_shared_snapshot`).
    // -----------------------------------------------------------------------

    /// Every forge read one full fetch can issue, in the order the fetch
    /// path tries them.
    const ALL_FORGE_READS: [&str; 10] = [
        "pr_observation",
        "branch_rules",
        "get_pr",
        "merge_requirements",
        "list_reviews",
        "review_decision",
        "check_runs",
        "get_review_threads",
        "list_review_comments",
        "list_comments",
    ];

    /// The reads issued so far, as `method → count`, omitting the zero rows
    /// so an assertion spells out exactly the reads a path costs.
    fn forge_reads(forge: &StubForge) -> Vec<(&'static str, usize)> {
        ALL_FORGE_READS
            .iter()
            .map(|m| {
                let n = if *m == "get_pr" {
                    forge.fetches()
                } else {
                    forge.sub_fetches(m)
                };
                (*m, n)
            })
            .filter(|(_, n)| *n > 0)
            .collect()
    }

    /// A review thread with `comments` placeholder comments.
    fn thread(id: &str, is_resolved: bool, comments: usize) -> ReviewThread {
        ReviewThread {
            id: id.into(),
            is_resolved,
            comments: (0..comments)
                .map(|i| ReviewThreadComment {
                    id: format!("{id}-c{i}"),
                    body: "nit".into(),
                    author: "reviewer".into(),
                    path: "src/lib.rs".into(),
                    line: Some(1),
                    created_at: "2026-01-01T00:00:00Z".into(),
                })
                .collect(),
        }
    }

    /// A PR with every kind of signal populated, so the two paths have
    /// something to disagree on.
    fn busy_pr(s: &mut ForgeState) {
        s.approvals = vec!["reviewer".into()];
        s.conversation_comments = 3;
        s.threads = vec![
            thread("t1", false, 2),
            thread("t2", true, 1),
            thread("t3", false, 1),
        ];
        s.checks.push(RollupCheck {
            name: "lint".into(),
            kind: RollupCheckKind::CheckRun,
            state: CheckState::Failure,
            is_required: false,
            url: None,
            started_at: None,
        });
        s.merge_queue_removal = Some(intent_sourcecontrol::MergeQueueRemoval {
            at: "2026-08-26T22:26:36Z".into(),
            reason: Some("failed_checks".into()),
        });
    }

    /// A host without a folded read (the per-signal path): the baseline the
    /// folded read is measured against. Six reads on the happy path — and
    /// the probe's `None` review decision costs a seventh-read-worthy
    /// standalone `review_decision` — eight on the REST fallback.
    #[tokio::test]
    async fn per_signal_fetch_costs_six_reads_and_the_rest_fallback_eight() {
        let repo = RepoRef::new("o", "r");
        let forge = StubForge::new();
        forge.edit(busy_pr);
        forge.edit(|s| s.approvals.clear());
        fetch_shared_snapshot(&forge, &repo, 42)
            .await
            .expect("fetch");
        assert_eq!(
            forge_reads(&forge),
            vec![
                ("get_pr", 1),
                ("merge_requirements", 1),
                ("list_reviews", 1),
                ("review_decision", 1),
                ("get_review_threads", 1),
                ("list_comments", 1),
            ]
        );

        let forge = StubForge::new();
        forge.edit(|s| {
            s.fail_merge_requirements = true;
            s.fail_get_review_threads = true;
        });
        fetch_shared_snapshot(&forge, &repo, 42)
            .await
            .expect("fetch");
        assert_eq!(
            forge_reads(&forge),
            vec![
                ("get_pr", 1),
                ("merge_requirements", 1),
                ("list_reviews", 1),
                ("review_decision", 1),
                ("check_runs", 1),
                ("get_review_threads", 1),
                ("list_review_comments", 1),
                ("list_comments", 1),
            ]
        );
    }

    /// A host with a folded read: one full fetch is the observation plus
    /// the branch rules — two reads — and yields the SAME snapshot, byte for
    /// byte, as the per-signal path composes for the same forge state.
    #[tokio::test]
    async fn a_folded_fetch_costs_two_reads_and_matches_the_per_signal_snapshot() {
        let repo = RepoRef::new("o", "r");
        let per_signal = StubForge::new();
        per_signal.edit(busy_pr);
        let expected = fetch_shared_snapshot(&per_signal, &repo, 42)
            .await
            .expect("per-signal fetch");

        let folded = StubForge::new();
        folded.edit(busy_pr);
        folded.edit(|s| s.folded = Some(FoldedRead::default()));
        let snapshot = fetch_shared_snapshot(&folded, &repo, 42)
            .await
            .expect("folded fetch");
        assert_eq!(
            forge_reads(&folded),
            vec![("pr_observation", 1), ("branch_rules", 1)]
        );
        assert_eq!(
            serde_json::to_string(&snapshot.materialize(None)).unwrap(),
            serde_json::to_string(&expected.materialize(None)).unwrap(),
            "the folded snapshot is byte-identical to the per-signal one"
        );
        assert!(snapshot.requirements_complete);
        assert!(snapshot.ejection_known);
        assert_eq!(snapshot.conversation_count, Some(3));
        assert_eq!(snapshot.review_comment_count, 4);
        assert_eq!(snapshot.requirements.threads.unresolved, Some(2));
    }

    /// The sweep's cached path on a folded host: the observation IS the
    /// change detector, so a fingerprint-unchanged poll costs exactly one
    /// read (as the per-signal `get_pr` did) and a moved fingerprint costs
    /// the observation plus the branch rules.
    #[tokio::test]
    async fn a_folded_cheap_poll_costs_one_read_and_a_full_refetch_two() {
        let repo = RepoRef::new("o", "r");
        let forge = StubForge::new();
        forge.edit(busy_pr);
        forge.edit(|s| s.folded = Some(FoldedRead::default()));
        let cache: PrMonitorFetchCache = Arc::default();
        let key = pr_key_for(&repo, 42);
        for expected in [
            vec![("pr_observation", 1), ("branch_rules", 1)],
            vec![("pr_observation", 2), ("branch_rules", 1)],
            vec![("pr_observation", 3), ("branch_rules", 1)],
        ] {
            fetch_shared_snapshot_cached(&forge, &repo, 42, &cache, &key)
                .await
                .expect("fetch");
            assert_eq!(forge_reads(&forge), expected);
        }
        forge.edit(|s| s.conversation_comments += 1);
        let snapshot = fetch_shared_snapshot_cached(&forge, &repo, 42, &cache, &key)
            .await
            .expect("fetch");
        assert_eq!(
            forge_reads(&forge),
            vec![("pr_observation", 4), ("branch_rules", 2)]
        );
        assert_eq!(snapshot.conversation_count, Some(4));
    }

    /// A PR that outgrew the observation's windows falls back to the paged
    /// reads for THAT piece only, and still composes the per-signal snapshot.
    #[tokio::test]
    async fn an_overflowing_observation_pages_only_the_exhausted_window() {
        let repo = RepoRef::new("o", "r");
        let per_signal = StubForge::new();
        per_signal.edit(busy_pr);
        let expected = fetch_shared_snapshot(&per_signal, &repo, 42)
            .await
            .expect("per-signal fetch");

        let forge = StubForge::new();
        forge.edit(busy_pr);
        forge.edit(|s| {
            s.folded = Some(FoldedRead {
                overflow_reviews: true,
                ..FoldedRead::default()
            });
        });
        let snapshot = fetch_shared_snapshot(&forge, &repo, 42).await.unwrap();
        assert_eq!(
            forge_reads(&forge),
            vec![
                ("pr_observation", 1),
                ("branch_rules", 1),
                ("list_reviews", 1),
            ]
        );
        assert_eq!(snapshot.materialize(None), expected.materialize(None));

        let forge = StubForge::new();
        forge.edit(busy_pr);
        forge.edit(|s| {
            s.folded = Some(FoldedRead {
                overflow_threads: true,
                ..FoldedRead::default()
            });
        });
        let snapshot = fetch_shared_snapshot(&forge, &repo, 42).await.unwrap();
        assert_eq!(
            forge_reads(&forge),
            vec![
                ("pr_observation", 1),
                ("branch_rules", 1),
                ("get_review_threads", 1),
            ]
        );
        assert_eq!(snapshot.materialize(None), expected.materialize(None));
    }

    /// A failing folded read (other than quota exhaustion) falls back to
    /// the per-signal reads, so a host whose GraphQL is down but whose REST
    /// answers still gets its snapshot — at the cost of the failed attempt
    /// on top of the fallback's own reads (nine when GraphQL is entirely
    /// down); quota exhaustion on the folded read propagates like a
    /// rate-limited `get_pr` (no further read is issued).
    #[tokio::test]
    async fn a_failing_folded_read_falls_back_and_a_rate_limited_one_propagates() {
        let repo = RepoRef::new("o", "r");
        let forge = StubForge::new();
        forge.edit(|s| {
            s.folded = Some(FoldedRead {
                fail: true,
                ..FoldedRead::default()
            });
        });
        fetch_shared_snapshot(&forge, &repo, 42)
            .await
            .expect("per-signal fallback");
        assert_eq!(
            forge_reads(&forge)[..2],
            [("pr_observation", 1), ("get_pr", 1)]
        );
        assert_eq!(forge.sub_fetches("merge_requirements"), 1);

        // GraphQL entirely down (folded read, probe and threads all fail):
        // the failed observation attempt precedes the per-signal REST
        // fallback's eight reads, so this host pays nine, not eight.
        let forge = StubForge::new();
        forge.edit(|s| {
            s.folded = Some(FoldedRead {
                fail: true,
                ..FoldedRead::default()
            });
            s.fail_merge_requirements = true;
            s.fail_get_review_threads = true;
        });
        fetch_shared_snapshot(&forge, &repo, 42)
            .await
            .expect("REST fallback");
        assert_eq!(
            forge_reads(&forge),
            vec![
                ("pr_observation", 1),
                ("get_pr", 1),
                ("merge_requirements", 1),
                ("list_reviews", 1),
                ("review_decision", 1),
                ("check_runs", 1),
                ("get_review_threads", 1),
                ("list_review_comments", 1),
                ("list_comments", 1),
            ]
        );

        let forge = StubForge::new();
        forge.edit(|s| {
            s.folded = Some(FoldedRead {
                rate_limited: true,
                ..FoldedRead::default()
            });
        });
        let err = fetch_shared_snapshot(&forge, &repo, 42)
            .await
            .expect_err("quota exhaustion propagates");
        assert!(matches!(err, Error::RateLimited(_)), "{err:?}");
        assert_eq!(forge_reads(&forge), vec![("pr_observation", 1)]);
    }

    /// Count parity past the per-signal ceilings: a PR with more than 100
    /// conversation comments and more than 100 replies in one thread reports
    /// the SAME (saturated) counts from the folded read and the per-signal
    /// fallback, so a transient folded failure on an unchanged PR — folded →
    /// fallback → folded — composes identical snapshots and the monitor
    /// records no comment change.
    #[tokio::test]
    async fn an_unchanged_pr_past_the_count_ceilings_survives_a_folded_fallback_round_trip() {
        let repo = RepoRef::new("o", "r");
        let forge = StubForge::new();
        forge.edit(busy_pr);
        forge.edit(|s| {
            s.conversation_comments = 2426;
            s.threads = vec![thread("t1", false, 250), thread("t2", true, 1)];
            s.folded = Some(FoldedRead::default());
        });
        let folded = fetch_shared_snapshot(&forge, &repo, 42).await.unwrap();
        forge.edit(|s| s.folded.as_mut().unwrap().fail = true);
        let fallback = fetch_shared_snapshot(&forge, &repo, 42).await.unwrap();
        forge.edit(|s| s.folded.as_mut().unwrap().fail = false);
        let folded_again = fetch_shared_snapshot(&forge, &repo, 42).await.unwrap();
        assert_eq!(forge.sub_fetches("pr_observation"), 3);
        assert_eq!(forge.sub_fetches("list_comments"), 1);
        assert_eq!(folded.conversation_count, Some(100), "saturated, not 2426");
        assert_eq!(folded.review_comment_count, 101, "100 + 1, not 251");
        assert_eq!(fallback.materialize(None), folded.materialize(None));
        assert_eq!(folded_again.materialize(None), folded.materialize(None));

        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        forge.edit(|s| {
            s.conversation_comments = 2426;
            s.threads = vec![thread("t1", false, 250), thread("t2", true, 1)];
            s.folded = Some(FoldedRead::default());
        });
        let monitor = register(&svc, &ws, &owner).await;
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.folded.as_mut().unwrap().fail = true);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.folded.as_mut().unwrap().fail = false);
        svc.poll_pr_monitors().await;
        assert!(
            !svc.pr_monitor_flush(&ws, &monitor.monitor_id)
                .await
                .unwrap(),
            "no comment delta pending after the round trip"
        );
        assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
    }

    /// Cache hygiene: an entry outlives its monitors only until the next
    /// sweep, which prunes PRs no longer under any active monitor.
    #[tokio::test]
    async fn the_fetch_cache_is_pruned_to_active_prs() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        let other = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .expect("other pr")
            .0;
        svc.poll_pr_monitors().await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 2);

        svc.pr_monitor_cancel(&ws, &other.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        svc.poll_pr_monitors().await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 1, "o/r#7 pruned");

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        svc.poll_pr_monitors().await;
        assert_eq!(svc.pr_monitor_fetch_cache_len(), 0, "no active monitors");
        let _ = &forge;
    }

    #[tokio::test]
    async fn sweep_dedupes_failed_fetches_and_records_the_error_on_every_sibling() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let (ws2, sibling) = sibling_workspace(&svc, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws2, &sibling, "o", "r", 42)
            .await
            .expect("sibling register")
            .0;

        forge.edit(|s| s.fail_get_pr = true);
        let before = forge.fetches();
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.fetches() - before,
            1,
            "an unreachable PR costs ONE fetch attempt per sweep, not one per monitor"
        );
        for id in [&first.monitor_id, &second.monitor_id] {
            let row = svc.store().get_pr_monitor(id).await.unwrap();
            assert_eq!(row.state, PrMonitorState::Active);
            assert!(row.last_error.is_some(), "error recorded on {}", id.0);
        }
    }

    /// Regression for intent-hq/monorepo#1988: a forge fetch that pends
    /// forever (a TCP connection gone dark) must not wedge the sweep. The
    /// per-fetch timeout maps the hang to an error — `lastError` set,
    /// `lastPolledAt` stamped, baseline untouched, monitor still active —
    /// and the sweep proceeds to poll the remaining monitors.
    #[tokio::test]
    async fn a_hung_fetch_times_out_and_the_sweep_still_polls_other_monitors() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_fetch_timeout(Duration::from_millis(50));
        // The hung monitor registers FIRST so the sweep (created_at order)
        // hits the hang before the healthy monitor — forward progress past
        // the hang is exactly what the test proves.
        let hung = register(&svc, &ws, &owner).await;
        let baseline = hung.last_snapshot.clone();
        let healthy = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 43)
            .await
            .expect("register healthy")
            .0;

        forge.edit(|s| s.hang_get_pr = Some(42));
        let before = forge.fetches();
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.fetches() - before,
            2,
            "the sweep completes: one hung attempt on PR 42, one healthy fetch on PR 43"
        );

        let hung_row = svc.store().get_pr_monitor(&hung.monitor_id).await.unwrap();
        assert_eq!(hung_row.state, PrMonitorState::Active, "the loop survives");
        assert!(
            hung_row
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("timed out")),
            "timeout recorded as lastError: {:?}",
            hung_row.last_error
        );
        assert!(hung_row.last_polled_at.is_some(), "lastPolledAt stamped");
        assert_eq!(hung_row.last_snapshot, baseline, "baseline untouched");

        let healthy_row = svc
            .store()
            .get_pr_monitor(&healthy.monitor_id)
            .await
            .unwrap();
        assert_eq!(healthy_row.state, PrMonitorState::Active);
        assert!(
            healthy_row.last_error.is_none(),
            "the healthy monitor polled cleanly: {:?}",
            healthy_row.last_error
        );
    }

    /// The property `SharedPrSnapshot` exists for: when the comment read
    /// degrades, each sibling materializes the shared snapshot against ITS
    /// OWN previous count. Siblings register around a comment bump so their
    /// baselines DIVERGE (0 vs 2); an implementation that materialized once
    /// with the first sibling's baseline and reused the result would
    /// fabricate a "comments removed" change on the other.
    #[tokio::test]
    async fn a_degraded_comment_read_keeps_each_siblings_own_baseline() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let first = register(&svc, &ws, &owner).await;
        // Comments move BETWEEN the registrations: first's baseline stays at
        // 0 comments, the sibling's registration fetch stamps 2.
        forge.edit(|s| s.conversation_comments = 2);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws2, &sibling, "o", "r", 42)
            .await
            .expect("sibling register")
            .0;

        // The shared comment read degrades: each sibling keeps its own
        // previous count, so neither fabricates a comment change (first
        // does NOT see the +2 through the degraded read either).
        forge.edit(|s| s.fail_list_comments = true);
        svc.poll_pr_monitors().await;
        for id in [&first.monitor_id, &second.monitor_id] {
            let row = svc.store().get_pr_monitor(id).await.unwrap();
            assert!(
                !row.pending_changes.iter().any(|c| c.contains("comment")),
                "no fabricated comment change on {}: {:?}",
                id.0,
                row.pending_changes
            );
        }

        // Recovery diffs each monitor against ITS OWN kept count: the first
        // sees the +2 it never observed, the sibling sees nothing.
        forge.edit(|s| s.fail_list_comments = false);
        svc.poll_pr_monitors().await;
        let first_row = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert!(
            first_row
                .pending_changes
                .iter()
                .any(|c| c.contains("+2 conversation comment")),
            "first monitor catches up from its own baseline: {:?}",
            first_row.pending_changes
        );
        let second_row = svc
            .store()
            .get_pr_monitor(&second.monitor_id)
            .await
            .unwrap();
        assert!(
            !second_row
                .pending_changes
                .iter()
                .any(|c| c.contains("comment")),
            "the sibling already had the comments in its baseline: {:?}",
            second_row.pending_changes
        );
    }

    #[tokio::test]
    async fn due_sweep_skips_freshly_polled_monitors_but_never_catch_up_ones() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;

        // Registration just stamped `lastPolledAt`: the loop-driven sweep
        // skips the monitor (no fetch), while the explicit test-driven sweep
        // still polls everything.
        let before = forge.fetches();
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.fetches(), before, "fresh monitor skipped");
        svc.poll_pr_monitors().await;
        assert_eq!(forge.fetches(), before + 1, "explicit sweep never skips");

        // Backdate `lastPolledAt` beyond the poll interval: due again.
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let stale = now_iso();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.baseline_snapshot.as_deref(),
                    pending_changes: &row.pending_changes,
                    pending_since: row.pending_since.as_deref(),
                    last_change_at: row.last_change_at.as_deref(),
                    last_polled_at: Some("2020-01-01T00:00:00Z"),
                    last_error: None,
                    updated_at: &stale,
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .unwrap());
        let before = forge.fetches();
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.fetches(), before + 1, "stale monitor polled");

        // A catch-up-marked monitor (boot rehydration) is never skipped,
        // however fresh its `lastPolledAt` — downtime changes must deliver
        // on the first post-restart tick.
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        let before = forge.fetches();
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.fetches(), before + 1, "catch-up monitor polled");
    }

    /// Interval math table: the configured cadence holds until the monitored
    /// PR count would overspend the hourly budget, then stretches linearly;
    /// a higher configured cadence wins over the derived value, a smaller
    /// budget stretches further, and a sub-floor budget clamps to the floor.
    #[test]
    fn effective_interval_scales_with_distinct_prs_and_budget() {
        for (prs, expected) in [(0, 30), (1, 30), (4, 30), (5, 36), (10, 72), (20, 144)] {
            assert_eq!(
                effective_pr_monitor_interval_secs(prs, 30, 1500),
                expected,
                "{prs} PRs at 30s / 1500 per hour"
            );
        }
        // A configured cadence above the derived value is the floor.
        assert_eq!(effective_pr_monitor_interval_secs(10, 120, 1500), 120);
        // A smaller budget stretches the interval further.
        assert_eq!(effective_pr_monitor_interval_secs(10, 30, 500), 216);
        // A sub-floor budget clamps to the floor (60/h), never divides by 0.
        assert_eq!(
            effective_pr_monitor_interval_secs(10, 30, 0),
            effective_pr_monitor_interval_secs(10, 30, MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET)
        );
        assert_eq!(effective_pr_monitor_interval_secs(10, 30, 0), 1800);
        // A sub-floor cadence clamps to its floor too.
        assert_eq!(
            effective_pr_monitor_interval_secs(1, 0, 1500),
            MIN_PR_MONITOR_POLL_SECONDS
        );

        // The per-tick fetch cap spreads the stretched set across ticks.
        // A fetch spacing within one tick never holds, however fresh the
        // newest poll.
        assert_eq!(pr_monitor_fetches_per_tick(4, 30, 30, Some(0)), 4);
        assert_eq!(pr_monitor_fetches_per_tick(10, 30, 72, Some(0)), 5);
        assert_eq!(pr_monitor_fetches_per_tick(20, 30, 144, Some(0)), 5);
        assert_eq!(pr_monitor_fetches_per_tick(0, 30, 30, None), 1);
        // A spacing beyond one tick (1800s / 1 PR; 25,200s / 14 PRs =
        // 1800s) holds the tick until it has elapsed since the newest poll;
        // nothing ever polled never holds.
        assert_eq!(pr_monitor_fetches_per_tick(1, 30, 1800, None), 1);
        assert_eq!(pr_monitor_fetches_per_tick(1, 30, 1800, Some(1800)), 1);
        assert_eq!(pr_monitor_fetches_per_tick(1, 30, 1800, Some(1799)), 0);
        assert_eq!(pr_monitor_fetches_per_tick(14, 30, 25_200, Some(30)), 0);
        assert_eq!(pr_monitor_fetches_per_tick(14, 30, 25_200, Some(1800)), 1);
        // A spacing of exactly one tick is the tick itself: no hold.
        assert_eq!(pr_monitor_fetches_per_tick(3, 30, 90, Some(0)), 1);
    }

    /// Quota-window math table: a full window plans nothing beyond the
    /// hourly budget (the caller takes the max), a low one stretches the
    /// interval so the projected spend to reset fits the share, the interval
    /// is tick-aligned, a share that cannot pay for one fetch defers until
    /// the window resets (and the plan is monotone non-increasing in the
    /// remaining quota — less quota never polls sooner), and no usable
    /// window (probe failed, host without the signal, reset already passed,
    /// no PRs) plans nothing at all.
    #[test]
    fn quota_cadence_scales_with_remaining_quota_and_defers_below_one_fetch() {
        use QuotaCadence::{DeferUntilReset, Interval};
        let window = |remaining, reset_in_secs| {
            Some(QuotaWindow {
                remaining,
                reset_in_secs,
            })
        };
        // The 2026-09-16 shape: 14 PRs at the 30s floor. A full 5,000
        // window an hour out is plenty — 90s, below the budget's 101s.
        assert_eq!(
            plan_quota_cadence(14, 30, window(4_000, 3_600), 50),
            Some(Interval(90))
        );
        assert_eq!(effective_pr_monitor_interval_secs(14, 30, 1500), 101);
        // 300 left with half an hour to go: 14 × 3 × 1800 / 150 = 504 → the
        // 510s tick.
        assert_eq!(
            plan_quota_cadence(14, 30, window(300, 1_800), 50),
            Some(Interval(510))
        );
        // A larger share stretches less; the floor share stretches most.
        assert_eq!(
            plan_quota_cadence(14, 30, window(300, 1_800), 100),
            Some(Interval(270))
        );
        assert_eq!(
            plan_quota_cadence(14, 30, window(300, 1_800), 1),
            Some(Interval(25_200))
        );
        // Out-of-range shares clamp into the catalog range.
        assert_eq!(
            plan_quota_cadence(14, 30, window(300, 1_800), 0),
            plan_quota_cadence(14, 30, window(300, 1_800), 1)
        );
        assert_eq!(
            plan_quota_cadence(14, 30, window(300, 1_800), 250),
            plan_quota_cadence(14, 30, window(300, 1_800), 100)
        );
        // The share must pay for a whole fetch (3 requests): 6 left at 50%
        // is the last interval regime (3 allowed → 14 × 3 × 1800 / 3);
        // 5, 2, 1 and 0 left all defer to the reset — never an interval
        // shorter than the one more quota planned.
        assert_eq!(
            plan_quota_cadence(14, 30, window(6, 1_800), 50),
            Some(Interval(25_200))
        );
        for remaining in [5, 2, 1, 0] {
            assert_eq!(
                plan_quota_cadence(14, 30, window(remaining, 1_800), 50),
                Some(DeferUntilReset {
                    reset_in_secs: 1_800
                }),
                "remaining {remaining}"
            );
        }
        assert_eq!(
            plan_quota_cadence(14, 30, window(1, 1_000), 50),
            Some(DeferUntilReset {
                reset_in_secs: 1_000
            })
        );
        // Monotone non-increasing in the remaining quota across the whole
        // window (a deferral counts as slower than any interval).
        let slowness = |remaining| match plan_quota_cadence(14, 30, window(remaining, 1_800), 50) {
            Some(Interval(secs)) => secs,
            Some(DeferUntilReset { .. }) => u64::MAX,
            None => unreachable!("a window and PRs always plan"),
        };
        let mut previous = slowness(0);
        for remaining in 1..=5_000 {
            let current = slowness(remaining);
            assert!(
                current <= previous,
                "remaining {remaining} plans {current}s after {} planned {previous}s",
                remaining - 1
            );
            previous = current;
        }
        // Tick alignment follows the configured cadence (floor-clamped).
        assert_eq!(
            plan_quota_cadence(14, 100, window(300, 1_800), 50),
            Some(Interval(600))
        );
        assert_eq!(
            plan_quota_cadence(1, 0, window(3, 15), 100),
            Some(Interval(MIN_PR_MONITOR_POLL_SECONDS * 2))
        );
        // No window, or no PRs: nothing planned.
        assert_eq!(plan_quota_cadence(14, 30, None, 50), None);
        assert_eq!(plan_quota_cadence(0, 30, window(300, 1_800), 50), None);

        // The window reduces the probe: both signals required, a reset in
        // the past is unusable, a nonsense reset clamps to the pause cap.
        let status = |remaining, reset_at| RateLimitStatus {
            remaining,
            reset_at,
            limit: Some(5_000),
        };
        assert_eq!(
            QuotaWindow::from_status(&status(Some(300), Some(1_000 + 1_800)), 1_000),
            window(300, 1_800)
        );
        assert_eq!(
            QuotaWindow::from_status(&status(None, Some(2_800)), 1_000),
            None
        );
        assert_eq!(
            QuotaWindow::from_status(&status(Some(300), None), 1_000),
            None
        );
        assert_eq!(
            QuotaWindow::from_status(&status(Some(300), Some(1_000)), 1_000),
            None
        );
        assert_eq!(
            QuotaWindow::from_status(&status(Some(300), Some(999)), 1_000),
            None
        );
        assert_eq!(
            QuotaWindow::from_status(&status(Some(300), Some(u64::MAX)), 1_000),
            window(300, RATE_LIMIT_MAX_PAUSE.as_secs())
        );
    }

    /// Backdate a monitor's `lastPolledAt` so the due-sweep sees it as stale.
    async fn backdate(svc: &Services, id: &PrMonitorId, last_polled_at: &str) {
        let row = svc.store().get_pr_monitor(id).await.unwrap();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                id,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.baseline_snapshot.as_deref(),
                    pending_changes: &row.pending_changes,
                    pending_since: row.pending_since.as_deref(),
                    last_change_at: row.last_change_at.as_deref(),
                    last_polled_at: Some(last_polled_at),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .unwrap());
    }

    /// Simulate `secs` of wall-clock passing for the due-sweep: shift every
    /// active monitor's `lastPolledAt` back by that much (a missing stamp
    /// stays missing).
    async fn age_all(svc: &Services, secs: i64) {
        for row in svc.store().load_active_pr_monitors().await.unwrap() {
            let Some(at) = row.last_polled_at.as_deref().and_then(parse_iso) else {
                continue;
            };
            let aged = (at - time::Duration::seconds(secs))
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap();
            assert!(svc
                .store()
                .update_pr_monitor_poll(
                    &row.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: row.last_snapshot.as_deref(),
                        baseline_snapshot: row.baseline_snapshot.as_deref(),
                        pending_changes: &row.pending_changes,
                        pending_since: row.pending_since.as_deref(),
                        last_change_at: row.last_change_at.as_deref(),
                        last_polled_at: Some(&aged),
                        last_error: row.last_error.as_deref(),
                        updated_at: &now_iso(),
                        expected_updated_at: &row.updated_at,
                    },
                )
                .await
                .unwrap());
        }
    }

    /// Ten distinct PRs at the defaults stretch the interval to 72s, so one
    /// tick fetches only ceil(10 × 30 / 72) = 5 PRs — the five with the
    /// oldest `lastPolledAt`, in strictly oldest-first forge-call order and
    /// regardless of registration order — and successive ticks rotate
    /// through the rest before any PR repeats.
    #[tokio::test]
    async fn due_sweep_fetches_the_oldest_capped_subset_and_rotates() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitors_max_per_agent(20)
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        for pr in 1..=10_u64 {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            // PR 10 is the oldest-polled, PR 1 the freshest (still stale) —
            // the reverse of registration order.
            let stamp = format!("2020-01-01T00:00:{:02}Z", 10 - pr);
            backdate(&svc, &m.monitor_id, &stamp).await;
        }
        forge.take_fetched_numbers();

        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![10, 9, 8, 7, 6],
            "oldest five PRs first, oldest-first call order"
        );

        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![5, 4, 3, 2, 1],
            "the rest on the next tick, oldest-first call order"
        );

        // Every PR was polled once within the effective interval, so the
        // next tick has nothing due — no PR is fetched twice before all
        // were fetched once.
        svc.poll_due_pr_monitors().await;
        assert!(forge.take_fetched_numbers().is_empty(), "all fresh");
    }

    /// Catch-up markers (boot rehydration) exempt a monitor from the
    /// freshness check only until its first post-restart ATTEMPT: when the
    /// forge is down the marked set still rotates through the cap oldest
    /// first, a failed attempt rejoins the normal cadence instead of staying
    /// perpetually due, and the marker itself survives until a successful
    /// poll consumes it — so the changed-state catch-up wake is still
    /// delivered (undebounced) once the forge answers again.
    #[tokio::test]
    async fn catch_up_rotation_rate_limits_failed_attempts() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // A window that would suppress the wake if debounce still applied.
        let svc = svc
            .with_pr_monitors_max_per_agent(20)
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500)
            .with_pr_monitor_debounce_seconds(3600);
        let mut ids = Vec::new();
        for pr in 1..=10_u64 {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            backdate(
                &svc,
                &m.monitor_id,
                &format!("2020-01-01T00:00:{:02}Z", 10 - pr),
            )
            .await;
            ids.push(m.monitor_id);
        }
        forge.take_fetched_numbers();
        // The PRs move while the daemon is "down", then the daemon boots
        // into a failing forge.
        forge.edit(|s| {
            s.approvals.push("reviewer".into());
            s.fail_get_pr = true;
        });
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 10);
        let marked = || svc.pr_monitor_catch_up.lock().unwrap().len();
        assert_eq!(marked(), 10);

        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![10, 9, 8, 7, 6],
            "catch-up attempts rotate oldest first under the cap"
        );
        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![5, 4, 3, 2, 1],
            "the rest are attempted before any PR is retried"
        );
        // Every attempt failed and stamped `lastPolledAt`: nothing is due
        // again inside the effective interval, so the failing forge is not
        // hammered — but the markers survive for the eventual delivery.
        svc.poll_due_pr_monitors().await;
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "failed attempts are rate-limited"
        );
        assert_eq!(marked(), 10, "markers survive failed attempts");
        assert!(
            !owner_messages(&svc, &owner).await.contains("[PR monitor"),
            "no wake while every attempt fails"
        );

        // Forge back: the next due tick delivers and consumes the markers
        // of the PRs it reached.
        forge.edit(|s| s.fail_get_pr = false);
        for (i, id) in ids.iter().enumerate() {
            backdate(&svc, id, &format!("2020-01-01T00:00:{:02}Z", 9 - i)).await;
        }
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.take_fetched_numbers(), vec![10, 9, 8, 7, 6]);
        assert_eq!(marked(), 5, "successful polls consume their markers");
        let text = owner_messages(&svc, &owner).await;
        for pr in 6..=10 {
            assert!(
                text.contains(&format!("[PR monitor o/r#{pr}]")),
                "the surviving marker delivers the changed-state wake undebounced for #{pr}: {text}"
            );
        }
        for pr in 1..=5 {
            assert!(
                !text.contains(&format!("[PR monitor o/r#{pr}]")),
                "PRs not yet reached keep their marker and have not woken: {text}"
            );
        }
    }

    /// The `lastError` every active monitor carries while the global
    /// rate-limit pause is active, naming the gate's wall-clock deadline.
    fn expected_pause_error(svc: &Services) -> String {
        let until = svc
            .sweep_rate_limit_paused_until()
            .expect("the gate is paused");
        assert!(
            parse_iso(&until).is_some(),
            "pausedUntil is RFC 3339: {until}"
        );
        format!("rate limited; PR monitor polling paused until {until}")
    }

    /// Simulate the pause window elapsing without waiting out the minimum
    /// pause: the gate re-opens, and every active row's pause annotation
    /// is rewritten to name a deadline in the past — in production the same
    /// text simply ages past `now`, which is all the store's write-back
    /// floor looks at when deciding whether an annotation still stands.
    async fn elapse_pause(svc: &Services) {
        svc.sweep_rate_limit.lift();
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .cast_signed();
        let elapsed =
            crate::rate_limit::pause_error(Some(&intent_core::iso_from_unix_secs(now_unix - 60)));
        sqlx::query(
            "UPDATE pr_monitor SET last_error = substr(last_error, 1, instr(last_error, ?2) - 1) || ?1 \
             WHERE state = 'active' AND instr(COALESCE(last_error, ''), ?2) > 0",
        )
        .bind(&elapsed)
        .bind(crate::rate_limit::PAUSE_ERROR_MARKER)
        .execute(svc.store().write_pool())
        .await
        .expect("age the pause annotations");
    }

    /// A forge fetch failing with the quota-exhausted error pauses the
    /// global sweep rate-limit gate (monorepo#2961): the sweep stops
    /// fetching further PRs, EVERY active monitor — the rate-limited PR's
    /// and the ones not reached alike — records the pause as `lastError`
    /// naming the deadline, later sweeps make zero forge calls while paused,
    /// and the first successful post-pause poll clears the error.
    #[tokio::test]
    async fn a_rate_limited_fetch_pauses_the_gate_and_skips_the_rest_of_the_sweep() {
        async fn row(svc: &Services, id: &PrMonitorId) -> PrMonitor {
            svc.store().get_pr_monitor(id).await.unwrap()
        }
        async fn last_error(svc: &Services, id: &PrMonitorId) -> Option<String> {
            row(svc, id).await.last_error
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        let mut ids = Vec::new();
        for (pr, ws, who) in [
            (1_u64, &ws, &owner),
            (1, &ws2, &sibling),
            (2, &ws, &owner),
            (3, &ws, &owner),
        ] {
            let (m, _) = svc
                .pr_monitor_register(ws, who, "o", "r", pr)
                .await
                .expect("register");
            backdate(&svc, &m.monitor_id, &format!("2020-01-01T00:00:0{pr}Z")).await;
            ids.push(m.monitor_id);
        }
        forge.take_fetched_numbers();
        let before: Vec<PrMonitor> = {
            let mut rows = Vec::new();
            for id in &ids {
                rows.push(row(&svc, id).await);
            }
            rows
        };

        forge.edit(|s| s.rate_limit_get_pr = true);
        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![1],
            "the sweep stops at the rate-limited PR"
        );
        assert!(
            svc.sweep_rate_limit.paused_remaining().is_some(),
            "the global gate is paused"
        );
        let pause_error = expected_pause_error(&svc);
        let mut paused_rows = Vec::new();
        for id in &ids[..2] {
            let paused = row(&svc, id).await;
            assert_eq!(
                paused.last_error.as_deref(),
                Some(pause_error.as_str()),
                "both monitors on the rate-limited PR record the pause"
            );
            paused_rows.push(paused);
        }
        for (id, previous) in ids[2..].iter().zip(&before[2..]) {
            let paused = row(&svc, id).await;
            assert_eq!(
                paused.last_error.as_deref(),
                Some(pause_error.as_str()),
                "monitors not reached carry the pause too"
            );
            assert_eq!(
                paused.last_polled_at, previous.last_polled_at,
                "the stamp is not a poll"
            );
            assert_eq!(paused.last_snapshot, previous.last_snapshot);
            paused_rows.push(paused);
        }

        // While paused, sweeps skip the forge entirely — even a due sweep
        // over backdated rows, and even once the forge would answer again —
        // and never rewrite the paused rows (no lastError / timestamp churn).
        svc.poll_due_pr_monitors().await;
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.rate_limit_get_pr = false);
        svc.poll_due_pr_monitors().await;
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "no forge calls while the gate is paused"
        );
        for (id, paused) in ids.iter().zip(&paused_rows) {
            assert_eq!(
                &row(&svc, id).await,
                paused,
                "the pause error is recorded once; paused sweeps leave the row untouched"
            );
        }

        // Pause window over: polling resumes and the first successful poll
        // clears the pause error.
        elapse_pause(&svc).await;
        svc.poll_pr_monitors().await;
        let mut fetched = forge.take_fetched_numbers();
        fetched.sort_unstable();
        assert_eq!(fetched, vec![1, 2, 3]);
        for id in &ids {
            assert_eq!(last_error(&svc, id).await, None);
        }
    }

    /// The shared gate is re-consulted before EVERY vacant-cache fetch, not
    /// only at the top of the sweep: when a sibling sweep (PR refresh, git
    /// roots) pauses the gate while this sweep is mid-flight, the PRs not
    /// fetched yet are skipped — no further forge calls, rows untouched —
    /// while the PR fetched before the pause still completes its poll. The
    /// sibling's transition is simulated bare (no stamp): the poll landing
    /// on the still-bare row introduces no annotation of its own — the
    /// stamp is the sibling's, landed right behind its transition under
    /// the reconcile lock — and composes with it once it lands.
    #[tokio::test]
    async fn a_gate_paused_mid_sweep_by_a_sibling_stops_the_remaining_fetches() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let mut ids = Vec::new();
        for pr in [1_u64, 2, 3] {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            backdate(&svc, &m.monitor_id, &format!("2020-01-01T00:00:0{pr}Z")).await;
            ids.push(m.monitor_id);
        }
        let before: Vec<PrMonitor> = {
            let mut rows = Vec::new();
            for id in &ids {
                rows.push(svc.store().get_pr_monitor(id).await.unwrap());
            }
            rows
        };
        forge.take_fetched_numbers();

        // The first PR's fetch succeeds, but a sibling sweep pauses the
        // shared gate while it is in flight.
        let gate = Arc::clone(&svc.sweep_rate_limit);
        forge.set_on_get_pr(Some(Box::new(move |_| {
            gate.pause_for(Duration::from_secs(300));
        })));
        forge.edit(|s| s.conversation_comments = 7);
        svc.poll_due_pr_monitors().await;
        forge.set_on_get_pr(None);

        assert_eq!(
            forge.take_fetched_numbers(),
            vec![1],
            "the sweep stops at the first vacant fetch after the gate closed"
        );
        let first = svc.store().get_pr_monitor(&ids[0]).await.unwrap();
        assert_ne!(
            first.last_polled_at, before[0].last_polled_at,
            "the PR fetched before the pause completes its poll"
        );
        assert_eq!(
            first.last_error, None,
            "a poll landing on a row the sibling's stamp has not reached introduces no annotation"
        );
        for (id, previous) in ids[1..].iter().zip(&before[1..]) {
            assert_eq!(
                &svc.store().get_pr_monitor(id).await.unwrap(),
                previous,
                "PRs not reached before the pause keep their previous row"
            );
        }
        // The sibling's stamp lands: every active row, the polled one
        // included, names the deadline.
        let pause_error = expected_pause_error(&svc);
        assert_eq!(
            svc.store()
                .annotate_active_pr_monitors_pause(&pause_error)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            svc.store()
                .get_pr_monitor(&ids[0])
                .await
                .unwrap()
                .last_error,
            Some(pause_error)
        );

        // Pause window over: the skipped PRs are polled on the next sweep.
        elapse_pause(&svc).await;
        svc.poll_pr_monitors().await;
        let mut fetched = forge.take_fetched_numbers();
        fetched.sort_unstable();
        assert_eq!(fetched, vec![1, 2, 3]);
    }

    /// Quota exhaustion on a SECONDARY read — a checklist sub-read
    /// (`list_reviews`) or the conversation-comment count (`list_comments`)
    /// — is not a degraded-but-successful poll: it propagates as
    /// `RateLimited`, pauses the shared gate, records the pause `lastError`
    /// and leaves the baseline untouched, exactly like a rate-limited
    /// `get_pr`. Ordinary secondary failures still degrade (covered by
    /// `list_comments_failure_keeps_previous_conversation_count` and the
    /// probe-degradation tests).
    #[tokio::test]
    async fn a_rate_limited_secondary_read_pauses_the_gate_like_get_pr() {
        for rate_limit_read in ["list_reviews", "list_comments"] {
            let (_db, _root, svc, forge, ws, owner) = setup().await;
            let svc = svc
                .with_pr_monitor_poll_seconds(30)
                .with_pr_monitor_hourly_request_budget(1500);
            let mut ids = Vec::new();
            for pr in [1_u64, 2] {
                let (m, _) = svc
                    .pr_monitor_register(&ws, &owner, "o", "r", pr)
                    .await
                    .expect("register");
                backdate(&svc, &m.monitor_id, &format!("2020-01-01T00:00:0{pr}Z")).await;
                ids.push(m.monitor_id);
            }
            let baseline = svc.store().get_pr_monitor(&ids[0]).await.unwrap();
            forge.take_fetched_numbers();

            // A real change lands alongside the quota hit: it must NOT be
            // persisted as a successful poll.
            forge.edit(|s| {
                s.approvals = vec!["reviewer".into()];
                match rate_limit_read {
                    "list_reviews" => s.rate_limit_list_reviews = true,
                    _ => s.rate_limit_list_comments = true,
                }
            });
            svc.poll_due_pr_monitors().await;

            assert_eq!(
                forge.take_fetched_numbers(),
                vec![1],
                "{rate_limit_read}: the sweep stops at the rate-limited PR"
            );
            assert!(
                svc.sweep_rate_limit.paused_remaining().is_some(),
                "{rate_limit_read}: the global gate is paused"
            );
            let pause_error = expected_pause_error(&svc);
            let paused = svc.store().get_pr_monitor(&ids[0]).await.unwrap();
            assert_eq!(
                paused.last_error.as_deref(),
                Some(pause_error.as_str()),
                "{rate_limit_read}: the pause is recorded as lastError"
            );
            assert_eq!(
                paused.last_snapshot, baseline.last_snapshot,
                "{rate_limit_read}: the baseline is untouched"
            );
            let not_reached = svc.store().get_pr_monitor(&ids[1]).await.unwrap();
            assert_eq!(
                not_reached.last_error.as_deref(),
                Some(pause_error.as_str()),
                "{rate_limit_read}: the monitor not reached carries the pause too"
            );
            assert_eq!(
                not_reached.last_polled_at.as_deref(),
                Some("2020-01-01T00:00:02Z"),
                "{rate_limit_read}: the stamp is not a poll"
            );

            // Pause window over and quota back: the poll succeeds, clears
            // the pause error and only now records the change.
            elapse_pause(&svc).await;
            forge.edit(|s| {
                s.rate_limit_list_reviews = false;
                s.rate_limit_list_comments = false;
            });
            svc.poll_pr_monitors().await;
            let recovered = svc.store().get_pr_monitor(&ids[0]).await.unwrap();
            assert_eq!(recovered.last_error, None, "{rate_limit_read}");
            assert_ne!(
                recovered.last_snapshot, baseline.last_snapshot,
                "{rate_limit_read}: the change is observed once quota is back"
            );
        }
    }

    /// Regression (2026-09-17 incident): the pause was opened by the
    /// PR-REFRESH sweep, not by a monitor fetch, so no monitor's own fetch
    /// ever recorded it — every row sat on `lastError = NULL` with a frozen
    /// `lastPolledAt` and a checklist going stale for an hour. Whichever
    /// sweep trips the limit, EVERY active monitor across workspaces must
    /// carry the pause `lastError` naming the deadline, `ws.pr.monitors`
    /// rows must expose `pausedUntil` while the gate is closed, and the
    /// first post-pause sweep clears the error — completing a PR that
    /// merged during the blackout and waking its owner.
    #[tokio::test]
    async fn a_pause_opened_by_the_pr_refresh_sweep_is_surfaced_on_every_active_monitor() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        let (first, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let (second, _) = svc
            .pr_monitor_register(&ws2, &sibling, "o", "r", 2)
            .await
            .expect("register");
        backdate(&svc, &first.monitor_id, "2020-01-01T00:00:01Z").await;
        backdate(&svc, &second.monitor_id, "2020-01-01T00:00:02Z").await;
        forge.take_fetched_numbers();

        // The workspace is linked to PR 42 on a feature branch, so the
        // PR-refresh sweep re-fetches it — and hits the exhausted quota.
        let mut linked = svc.store().get_workspace(&ws).await.unwrap();
        linked.branch = "feature".into();
        linked.pr_number = Some(42);
        linked.pr_url = Some("https://github.com/o/r/pull/42".into());
        svc.store().update_workspace(&linked).await.unwrap();
        forge.edit(|s| s.rate_limit_get_pr = true);
        svc.refresh_all_workspace_prs(0).await;
        assert!(
            svc.sweep_rate_limit.paused_remaining().is_some(),
            "the PR-refresh sweep opened the global pause"
        );
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![42],
            "no monitor fetch was involved"
        );

        let pause_error = expected_pause_error(&svc);
        let paused_until = svc.sweep_rate_limit_paused_until().unwrap();
        for (monitor, polled) in [
            (&first, "2020-01-01T00:00:01Z"),
            (&second, "2020-01-01T00:00:02Z"),
        ] {
            let row = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            assert_eq!(
                row.last_error.as_deref(),
                Some(pause_error.as_str()),
                "every active monitor, in every workspace, carries the pause"
            );
            assert_eq!(row.last_polled_at.as_deref(), Some(polled), "not a poll");
            assert_eq!(
                row.last_snapshot, monitor.last_snapshot,
                "baseline untouched"
            );
        }

        // `ws.pr.monitors` labels the stale checklist with the deadline.
        for (ws, who, monitor) in [(&ws, &owner, &first), (&ws2, &sibling, &second)] {
            let listed = svc.pr_monitor_list_op(ws, Some(who)).await.unwrap();
            let rows = listed["monitors"].as_array().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["monitorId"], json!(monitor.monitor_id));
            assert_eq!(rows[0]["pausedUntil"], json!(paused_until));
            assert_eq!(rows[0]["lastError"], json!(pause_error));
        }

        // The monitor sweep honours the pause: no forge calls.
        svc.poll_due_pr_monitors().await;
        assert!(forge.take_fetched_numbers().is_empty());

        // Pause window over and quota back: PR 1 merged during the blackout.
        // The first post-pause sweep clears the pause on every monitor,
        // completes PR 1's monitor and wakes its owner; `pausedUntil` is
        // gone from the rows.
        elapse_pause(&svc).await;
        forge.edit(|s| {
            s.rate_limit_get_pr = false;
            s.pr_state = PrState::Merged;
        });
        svc.poll_pr_monitors().await;
        let completed = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        assert_eq!(completed.state, PrMonitorState::Completed);
        assert_eq!(completed.last_error, None);
        assert!(
            owner_messages(&svc, &owner)
                .await
                .contains("[PR monitor o/r#1]"),
            "the owner gets the final wake once polling resumes"
        );
        let recovered = svc
            .store()
            .get_pr_monitor(&second.monitor_id)
            .await
            .unwrap();
        assert_eq!(recovered.last_error, None);
        let listed = svc.pr_monitor_list_op(&ws2, Some(&sibling)).await.unwrap();
        let row = &listed["monitors"].as_array().unwrap()[0];
        assert!(row.get("pausedUntil").is_none(), "{row}");
        assert!(row.get("lastError").is_none(), "{row}");
    }

    /// A pause opened mid-sweep must not hide a genuine fetch error from the
    /// same tick: PR 1 fails with an ordinary forge error, then PR 2's fetch
    /// hits the quota and opens the pause. PR 1's row keeps its error with
    /// the pause appended, PR 2's carries the bare pause, the paused ticks
    /// touch neither, and after resume each `lastError` follows its own
    /// fetch again — PR 1 still failing keeps only the genuine error, PR 2
    /// succeeding clears.
    #[tokio::test]
    async fn a_pause_opened_mid_sweep_preserves_a_genuine_fetch_error_from_that_tick() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (first, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let (second, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 2)
            .await
            .expect("register");
        backdate(&svc, &first.monitor_id, "2020-01-01T00:00:01Z").await;
        backdate(&svc, &second.monitor_id, "2020-01-01T00:00:02Z").await;
        forge.take_fetched_numbers();

        // PR 1 answers "forge down"; PR 2 answers with the exhausted quota.
        let state = Arc::clone(&forge.state);
        forge.set_on_get_pr(Some(Box::new(move |number| {
            let mut s = state.lock().unwrap();
            s.fail_get_pr = number == 1;
            s.rate_limit_get_pr = number == 2;
        })));
        svc.poll_due_pr_monitors().await;
        forge.set_on_get_pr(None);
        assert_eq!(forge.take_fetched_numbers(), vec![1, 2]);
        assert!(svc.sweep_rate_limit.paused_remaining().is_some());

        let pause_error = expected_pause_error(&svc);
        let failed = svc.store().get_pr_monitor(&first.monitor_id).await.unwrap();
        let annotated = failed.last_error.clone().expect("PR 1 recorded its error");
        let genuine = annotated
            .strip_suffix(&format!("; {pause_error}"))
            .unwrap_or_else(|| panic!("the pause is appended to the genuine error: {annotated}"));
        assert!(genuine.contains("forge down"), "{genuine}");
        assert!(!genuine.contains("rate limited"), "{genuine}");
        let paused = svc
            .store()
            .get_pr_monitor(&second.monitor_id)
            .await
            .unwrap();
        assert_eq!(paused.last_error.as_deref(), Some(pause_error.as_str()));

        // Paused ticks fetch nothing and leave both rows as they are.
        svc.poll_pr_monitors().await;
        assert!(forge.take_fetched_numbers().is_empty());
        assert_eq!(
            svc.store().get_pr_monitor(&first.monitor_id).await.unwrap(),
            failed
        );
        assert_eq!(
            svc.store()
                .get_pr_monitor(&second.monitor_id)
                .await
                .unwrap(),
            paused
        );

        // Resume: PR 1 still fails, PR 2 answers — each `lastError` is its
        // own fetch's again, with no pause annotation left over.
        elapse_pause(&svc).await;
        let state = Arc::clone(&forge.state);
        forge.set_on_get_pr(Some(Box::new(move |number| {
            let mut s = state.lock().unwrap();
            s.fail_get_pr = number == 1;
            s.rate_limit_get_pr = false;
        })));
        svc.poll_pr_monitors().await;
        forge.set_on_get_pr(None);
        let mut fetched = forge.take_fetched_numbers();
        fetched.sort_unstable();
        assert_eq!(fetched, vec![1, 2]);
        assert_eq!(
            svc.store()
                .get_pr_monitor(&first.monitor_id)
                .await
                .unwrap()
                .last_error
                .as_deref(),
            Some(genuine),
            "the genuine error stands on its own merits after resume"
        );
        assert_eq!(
            svc.store()
                .get_pr_monitor(&second.monitor_id)
                .await
                .unwrap()
                .last_error,
            None
        );
    }

    /// An in-flight poll must not strip the pause: the production stamp
    /// ([`Services::pause_sweeps_for_rate_limit`], as the PR-refresh sweep
    /// calls it) leaves the guard token alone, so a poll that read the row
    /// before the stamp still lands — and lands WITH the pause annotation
    /// while the gate is closed, so `lastError` and `pausedUntil` both name
    /// the deadline until the first post-pause poll. A genuine error
    /// recorded mid-pause composes the same way.
    #[tokio::test]
    async fn an_in_flight_poll_landing_mid_pause_keeps_the_annotation() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (monitor, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        // The row image a sweep read, and the fetch it completed, before the
        // pause opened.
        let in_flight = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        forge.edit(|s| s.conversation_comments = 3);
        let shared = fetch_shared_snapshot(sc.as_ref(), &monitor.repo(), 1)
            .await
            .expect("fetch");

        // Another sweep's forge call trips the limit: the pause opens and is
        // stamped on the row without moving its guard token.
        svc.pause_sweeps_for_rate_limit(&sc, "API rate limit exceeded")
            .await;
        let pause_error = expected_pause_error(&svc);
        let paused_until = svc.sweep_rate_limit_paused_until().unwrap();
        let stamped = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(stamped.last_error.as_deref(), Some(pause_error.as_str()));
        assert_eq!(stamped.updated_at, in_flight.updated_at);

        // The in-flight write-back lands against the pre-stamp image — and
        // keeps the annotation, since the gate is still closed.
        svc.poll_one_pr_monitor(&in_flight, &shared)
            .await
            .expect("the guarded write-back lands");
        let landed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_ne!(landed.updated_at, in_flight.updated_at, "the poll landed");
        assert_ne!(landed.last_snapshot, in_flight.last_snapshot);
        assert_eq!(
            landed.last_error.as_deref(),
            Some(pause_error.as_str()),
            "a success mid-pause does not strip the pause"
        );
        let listed = svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap();
        let row = &listed["monitors"].as_array().unwrap()[0];
        assert_eq!(row["pausedUntil"], json!(paused_until));
        assert_eq!(row["lastError"], json!(pause_error));

        // A genuine error recorded mid-pause keeps the annotation too.
        svc.record_pr_monitor_error(&landed, "forge down").await;
        assert_eq!(
            svc.store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap()
                .last_error,
            Some(format!("forge down; {pause_error}"))
        );

        // The first post-pause poll clears everything the pause left.
        elapse_pause(&svc).await;
        svc.poll_pr_monitors().await;
        let resumed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(resumed.last_error, None);
        let listed = svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap();
        let row = &listed["monitors"].as_array().unwrap()[0];
        assert!(row.get("pausedUntil").is_none(), "{row}");
        assert!(row.get("lastError").is_none(), "{row}");
    }

    /// The gate-to-SQL schedules: `poll_one_pr_monitor` and
    /// `record_pr_monitor_error` read the row in Rust, then issue a guarded
    /// UPDATE the bulk stamp cannot fail (it leaves `updated_at` alone). A
    /// pause that OPENS between the two (the row was read bare) or EXTENDS
    /// between the two (the row was read at T1, is at T2 by the write) must
    /// land as the row now has it — the write-back carries no annotation of
    /// its own and must neither strip nor roll back the row's. Driven
    /// deterministically: the gate is held at what the read saw while the
    /// production stamp is landed on the row ahead of the write-back — to
    /// the UPDATE, indistinguishable from the stamp racing in after the
    /// read.
    #[tokio::test]
    async fn a_stamp_landing_between_the_gate_read_and_the_write_back_is_kept() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (monitor, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .cast_signed();
        let read = |svc: &Services| {
            let store = svc.store().clone();
            let id = monitor.monitor_id.clone();
            async move { store.get_pr_monitor(&id).await.unwrap() }
        };
        let pause_at = |secs_from_now: i64| {
            crate::rate_limit::pause_error(Some(&intent_core::iso_from_unix_secs(
                now_unix + secs_from_now,
            )))
        };

        // Schedule 1 — the pause OPENS after the gate read: the gate is
        // open (the capture is `None`), the row carries the fresh stamp.
        let t1 = pause_at(600);
        let in_flight = read(&svc).await;
        assert!(svc.sweep_rate_limit_paused_until().is_none());
        forge.edit(|s| s.conversation_comments = 3);
        let shared = fetch_shared_snapshot(sc.as_ref(), &monitor.repo(), 1)
            .await
            .expect("fetch");
        assert_eq!(
            svc.store()
                .annotate_active_pr_monitors_pause(&t1)
                .await
                .unwrap(),
            1
        );
        svc.poll_one_pr_monitor(&in_flight, &shared)
            .await
            .expect("the guarded write-back lands");
        let landed = read(&svc).await;
        assert_ne!(landed.updated_at, in_flight.updated_at, "the poll landed");
        assert_eq!(
            landed.last_error.as_deref(),
            Some(t1.as_str()),
            "a `None` captured before the pause opened must not clobber it"
        );
        svc.record_pr_monitor_error(&landed, "forge down").await;
        assert_eq!(
            read(&svc).await.last_error,
            Some(format!("forge down; {t1}")),
            "an error captured before the pause opened composes with it"
        );

        // Schedule 2 — the pause EXTENDS after the gate read: the gate says
        // T1 (the capture composes T1), the row already names T2.
        svc.sweep_rate_limit.pause_for(Duration::from_secs(600));
        let gate_t1 = expected_pause_error(&svc);
        assert!(gate_t1 >= t1, "{gate_t1} >= {t1}");
        let t2 = pause_at(1800);
        assert!(t2 > gate_t1, "{t2} > {gate_t1}");
        let in_flight = read(&svc).await;
        forge.edit(|s| s.conversation_comments = 4);
        let shared = fetch_shared_snapshot(sc.as_ref(), &monitor.repo(), 1)
            .await
            .expect("fetch");
        assert_eq!(
            svc.store()
                .annotate_active_pr_monitors_pause(&t2)
                .await
                .unwrap(),
            1
        );
        svc.poll_one_pr_monitor(&in_flight, &shared)
            .await
            .expect("the guarded write-back lands");
        let landed = read(&svc).await;
        assert_ne!(landed.updated_at, in_flight.updated_at, "the poll landed");
        assert_eq!(
            landed.last_error.as_deref(),
            Some(t2.as_str()),
            "a T1 captured before the extension must not roll the row back"
        );
        svc.record_pr_monitor_error(&landed, "forge down").await;
        assert_eq!(
            read(&svc).await.last_error,
            Some(format!("forge down; {t2}")),
            "an error composed with T1 lands in front of the row's T2"
        );

        // The first post-pause poll still clears everything.
        elapse_pause(&svc).await;
        svc.poll_pr_monitors().await;
        assert_eq!(read(&svc).await.last_error, None);
    }

    /// A trigger that EXTENDS an active pause re-annotates every active row
    /// with the new deadline (the WARN stays coalesced to the opening
    /// trigger): no row keeps naming a deadline `pausedUntil` has moved past.
    /// A genuine error survives the re-stamp, a trigger that does not move
    /// the deadline leaves the rows alone, and terminal rows are never
    /// touched.
    #[tokio::test]
    async fn an_extended_pause_re_stamps_every_active_monitor_with_the_new_deadline() {
        async fn errors(
            svc: &Services,
            first: &PrMonitorId,
            second: &PrMonitorId,
        ) -> (Option<String>, Option<String>) {
            (
                svc.store().get_pr_monitor(first).await.unwrap().last_error,
                svc.store().get_pr_monitor(second).await.unwrap().last_error,
            )
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        let (first, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let (second, _) = svc
            .pr_monitor_register(&ws2, &sibling, "o", "r", 2)
            .await
            .expect("register");
        let mut completed = first.clone();
        completed.monitor_id = PrMonitorId::new();
        completed.pr_number = 3;
        completed.state = PrMonitorState::Completed;
        completed.last_error = Some("old failure".into());
        assert!(svc.store().insert_pr_monitor(&completed).await.unwrap());
        // PR 1 carries a genuine error from before the pause.
        svc.record_pr_monitor_error(&first, "forge down").await;

        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ids = (&first.monitor_id, &second.monitor_id);

        // The window opens at T1.
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 600));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        let t1 = svc.sweep_rate_limit_paused_until().unwrap();
        let pause_t1 = expected_pause_error(&svc);
        assert_eq!(
            errors(&svc, ids.0, ids.1).await,
            (
                Some(format!("forge down; {pause_t1}")),
                Some(pause_t1.clone())
            )
        );

        // A later reset extends the window to T2: every active row is
        // re-annotated with T2, the genuine error still in front.
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 1800));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        let t2 = svc.sweep_rate_limit_paused_until().unwrap();
        assert!(t2 > t1, "{t2} > {t1}");
        let pause_t2 = expected_pause_error(&svc);
        assert_eq!(
            errors(&svc, ids.0, ids.1).await,
            (
                Some(format!("forge down; {pause_t2}")),
                Some(pause_t2.clone())
            )
        );
        for (ws, who) in [(&ws, &owner), (&ws2, &sibling)] {
            let listed = svc.pr_monitor_list_op(ws, Some(who)).await.unwrap();
            for row in listed["monitors"].as_array().unwrap() {
                if row["state"] == json!("active") {
                    assert_eq!(row["pausedUntil"], json!(t2), "{row}");
                } else {
                    assert!(row.get("pausedUntil").is_none(), "{row}");
                }
            }
        }

        // An earlier reset does not move the deadline: nothing is re-stamped.
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 300));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        assert_eq!(svc.sweep_rate_limit_paused_until().unwrap(), t2);
        assert_eq!(
            errors(&svc, ids.0, ids.1).await,
            (Some(format!("forge down; {pause_t2}")), Some(pause_t2))
        );

        let terminal = svc
            .store()
            .get_pr_monitor(&completed.monitor_id)
            .await
            .unwrap();
        assert_eq!(terminal.last_error.as_deref(), Some("old failure"));
    }

    /// While paused, each sweep tick spends exactly one quota-free
    /// `rate_limit` probe and lifts the gate EARLY — before the `reset +
    /// margin` deadline — once the probe reports the quota recovered
    /// (`remaining ≥ max(500, 10% of limit)`): the lifted tick polls right
    /// away, and the pause annotation every active monitor carried (which
    /// the guarded write-back would otherwise keep until its now-stale
    /// deadline aged out) is cleared, so `pausedUntil` / the pause
    /// `lastError` stop being reported. A failing probe, a host without
    /// the signal, or a quota still below the floor keep the deadline: no
    /// forge fetch, rows untouched.
    #[tokio::test]
    async fn a_paused_sweep_probes_once_per_tick_and_lifts_early_once_the_quota_recovered() {
        async fn errors(svc: &Services, ids: &[PrMonitorId]) -> Vec<Option<String>> {
            let mut out = Vec::new();
            for id in ids {
                out.push(svc.store().get_pr_monitor(id).await.unwrap().last_error);
            }
            out
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        let mut ids = Vec::new();
        for (pr, ws, who) in [(1_u64, &ws, &owner), (2, &ws2, &sibling), (3, &ws, &owner)] {
            let (m, _) = svc
                .pr_monitor_register(ws, who, "o", "r", pr)
                .await
                .expect("register");
            backdate(&svc, &m.monitor_id, &format!("2020-01-01T00:00:0{pr}Z")).await;
            ids.push(m.monitor_id);
        }
        forge.take_fetched_numbers();
        let probes = || forge.sub_fetches("rate_limit_status");
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // The window opens an hour out; the open-gate tick spends its
        // cadence probe (#1), the fetch trips the gate, and the trigger's
        // own probe reads the reset (#2); every active row is stamped.
        forge.edit(|s| {
            s.rate_limit_get_pr = true;
            s.rate_limit_reset_at = Some(now_unix + 3600);
        });
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.take_fetched_numbers(), vec![1]);
        assert!(svc.sweeps_rate_limited(), "the gate is paused");
        assert_eq!(probes(), 2);
        let pause_error = expected_pause_error(&svc);
        let deadline = svc.sweep_rate_limit_paused_until().unwrap();
        assert_eq!(
            errors(&svc, &ids).await,
            vec![Some(pause_error.clone()); 3],
            "every active monitor names the deadline"
        );
        // The forge would answer again; only the probe decides when.
        forge.edit(|s| s.rate_limit_get_pr = false);

        // No `remaining` signal: one probe, deadline kept, no fetch.
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 3, "a paused tick spends exactly one probe");
        assert!(forge.take_fetched_numbers().is_empty());
        assert_eq!(svc.sweep_rate_limit_paused_until().unwrap(), deadline);

        // The probe itself failing: deadline kept, no fetch.
        forge.edit(|s| s.fail_rate_limit_status = true);
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 4);
        assert!(forge.take_fetched_numbers().is_empty());
        assert!(svc.sweeps_rate_limited());

        // Quota reported but below the floor (499 < max(500, 10% of 5000)).
        forge.edit(|s| {
            s.fail_rate_limit_status = false;
            s.rate_limit_remaining = Some(499);
            s.rate_limit_limit = Some(5_000);
        });
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 5);
        assert!(forge.take_fetched_numbers().is_empty());
        assert_eq!(svc.sweep_rate_limit_paused_until().unwrap(), deadline);
        assert_eq!(
            errors(&svc, &ids).await,
            vec![Some(pause_error.clone()); 3],
            "the rows are untouched while the deadline stands"
        );

        // The evidence case: a full window while the deadline is still an
        // hour out → the gate lifts, this very tick polls every monitor,
        // and the persisted annotations are gone.
        forge.edit(|s| s.rate_limit_remaining = Some(5_000));
        svc.poll_pr_monitors().await;
        assert_eq!(probes(), 6);
        assert!(
            svc.sweep_rate_limit_paused_until().is_none(),
            "the pause lifted before its deadline"
        );
        let mut fetched = forge.take_fetched_numbers();
        fetched.sort_unstable();
        assert_eq!(fetched, vec![1, 2, 3], "the lifted tick polls right away");
        assert_eq!(errors(&svc, &ids).await, vec![None, None, None]);
        for (ws, who) in [(&ws, &owner), (&ws2, &sibling)] {
            let listed = svc.pr_monitor_list_op(ws, Some(who)).await.unwrap();
            for row in listed["monitors"].as_array().unwrap() {
                assert!(row.get("pausedUntil").is_none(), "{row}");
                assert!(row.get("lastError").is_none(), "{row}");
            }
        }

        // An open gate spends no probes on a FULL sweep: the next tick just
        // polls (the due-sweep's cadence probe is the other test's subject).
        svc.poll_pr_monitors().await;
        assert_eq!(probes(), 6);
        assert_eq!(forge.take_fetched_numbers().len(), 3);
    }

    /// The due-sweep plans its cadence on the tick's one quota probe: with
    /// plenty of quota the hourly-budget cadence stands (every due PR is
    /// polled); with the quota running low the interval stretches so the
    /// projected spend to reset fits the configured share (PRs due on the
    /// budget cadence are no longer due, and even far-overdue ones are
    /// spread across ticks by the fetch cap); a failed probe or a host
    /// without the signal falls back to the hourly-budget formula alone. A
    /// tick that lifted the pause reuses the lift's probe — never two probes
    /// per tick.
    #[tokio::test]
    async fn the_due_sweep_stretches_the_cadence_on_the_remaining_quota() {
        async fn backdate_all(svc: &Services, ids: &[PrMonitorId], at: &str) {
            for id in ids {
                backdate(svc, id, at).await;
            }
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500)
            .with_pr_monitor_quota_share_percent(50);
        let mut ids = Vec::new();
        for pr in 1_u64..=3 {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            ids.push(m.monitor_id);
        }
        forge.take_fetched_numbers();
        let probes = || forge.sub_fetches("rate_limit_status");
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Ten minutes stale: due on the 30s budget cadence (3 PRs at
        // 1500/h keep the configured 30s), not on a quota-stretched one.
        let ten_minutes_ago = intent_core::iso_from_unix_secs((now_unix - 600).cast_signed());
        let stale = || backdate_all(&svc, &ids, &ten_minutes_ago);

        // Host without the signal (the stub default): one probe, today's
        // formula, every due PR polled.
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 1, "a due-sweep tick spends exactly one probe");
        assert_eq!(forge.take_fetched_numbers().len(), 3);

        // Plenty of quota: a full window an hour out plans 3 × 3 × 3600 /
        // 2500 = 13s → the 30s tick; the budget cadence stands.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(5_000);
            s.rate_limit_limit = Some(5_000);
            s.rate_limit_reset_at = Some(now_unix + 3600);
        });
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 2);
        assert_eq!(forge.take_fetched_numbers().len(), 3, "no stretch");

        // Low quota: 20 requests left with an hour to go → half of them
        // cover 3 × 3 × 3600 / 10 = 3240s of polling; ten-minute-stale PRs
        // are not due anymore.
        forge.edit(|s| s.rate_limit_remaining = Some(20));
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 3);
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "the quota-stretched interval made the stale PRs not due"
        );
        // Far-overdue PRs are due, but the fetch cap (ceil(3 × 30 / 3240)
        // = 1) spreads them across ticks.
        for id in &ids {
            backdate(&svc, id, "2020-01-01T00:00:00Z").await;
        }
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 4);
        assert_eq!(forge.take_fetched_numbers().len(), 1, "one PR per tick");

        // Never below the configured cadence, however much quota is left.
        forge.edit(|s| s.rate_limit_remaining = Some(u64::MAX / 8));
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.take_fetched_numbers().len(), 3);

        // The probe failing: the budget formula alone, every due PR polled.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(20);
            s.fail_rate_limit_status = true;
        });
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 6);
        assert_eq!(
            forge.take_fetched_numbers().len(),
            3,
            "a failed probe falls back to the hourly-budget cadence"
        );
        // A `remaining` without a reset is no projection horizon either.
        forge.edit(|s| {
            s.fail_rate_limit_status = false;
            s.rate_limit_reset_at = None;
        });
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 7);
        assert_eq!(forge.take_fetched_numbers().len(), 3);

        // A paused tick that lifts the pause plans on the lift's probe: one
        // probe for the tick, and the plenty-of-quota cadence applies.
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 3600));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        assert!(svc.sweeps_rate_limited());
        assert_eq!(probes(), 8, "the trigger reads the reset");
        forge.edit(|s| s.rate_limit_remaining = Some(5_000));
        stale().await;
        svc.poll_due_pr_monitors().await;
        assert!(svc.sweep_rate_limit_paused_until().is_none(), "lifted");
        assert_eq!(probes(), 9, "the lift's probe is reused for the cadence");
        assert_eq!(forge.take_fetched_numbers().len(), 3);
    }

    /// A share of the remaining quota that cannot pay for one fetch defers
    /// EVERY poll until the window resets — measured from each tick's probe,
    /// not from `lastPolledAt`: rows older than the reset horizon and
    /// catch-up-marked rows (which bypass the interval) are not fetched
    /// either, and the deferral holds as the horizon shrinks tick after
    /// tick. Each tick still spends its single quota-free probe, the
    /// catch-up markers survive, and the first tick under a fresh window
    /// polls the stale and catch-up rows.
    #[tokio::test]
    async fn a_zero_budget_defers_every_poll_until_the_window_resets() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500)
            .with_pr_monitor_quota_share_percent(50);
        let mut ids = Vec::new();
        for pr in 1_u64..=3 {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            ids.push(m.monitor_id);
        }
        forge.take_fetched_numbers();
        let probes = || forge.sub_fetches("rate_limit_status");
        let marked = || svc.pr_monitor_catch_up.lock().unwrap().len();
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Older than the whole reset horizon: an interval anchored on this
        // stamp would already have elapsed.
        let forty_minutes_ago = intent_core::iso_from_unix_secs((now_unix - 2_400).cast_signed());
        for id in &ids {
            backdate(&svc, id, &forty_minutes_ago).await;
        }
        // And catch-up marked, as after a daemon restart.
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 3);
        assert_eq!(marked(), 3);

        // 2 requests left at 50%: one allowed request cannot cover a fetch.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(2);
            s.rate_limit_limit = Some(5_000);
            s.rate_limit_reset_at = Some(now_unix + 1_800);
        });
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 1, "a deferred tick still spends its one probe");
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "neither the stale anchors nor the catch-up markers are fetched"
        );
        // The horizon shrinks across the following ticks: still deferred.
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 900));
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 2);
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "a shrinking horizon does not make the stale rows due"
        );
        forge.edit(|s| {
            s.rate_limit_remaining = Some(0);
            s.rate_limit_reset_at = Some(now_unix + 60);
        });
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 3);
        assert!(
            forge.take_fetched_numbers().is_empty(),
            "nothing left near the reset: still nothing fetched"
        );
        assert_eq!(marked(), 3, "the catch-up markers survive the deferral");
        for id in &ids {
            let row = svc.store().get_pr_monitor(id).await.unwrap();
            assert_eq!(
                row.last_polled_at.as_deref(),
                Some(forty_minutes_ago.as_str()),
                "a deferred tick stamps nothing"
            );
        }

        // A fresh window: the stale, catch-up rows are polled on the next
        // tick.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(5_000);
            s.rate_limit_reset_at = Some(now_unix + 3_600);
        });
        svc.poll_due_pr_monitors().await;
        assert_eq!(probes(), 4);
        let mut fetched = forge.take_fetched_numbers();
        fetched.sort_unstable();
        assert_eq!(fetched, vec![1, 2, 3]);
        assert_eq!(marked(), 0, "the catch-up polls cleared the markers");
    }

    /// A stale backlog drains WITHIN the quota share, not one PR per tick:
    /// fourteen far-overdue PRs with 6 requests left at 50% and 1800s to
    /// the reset get one fetch (3 requests) before the reset — the planned
    /// 25,200s interval spaces successive fetches 1800s apart, so the ticks
    /// up to the reset fetch exactly one PR however overdue the other
    /// thirteen are (the per-tick cap alone would drain them one per 30s
    /// tick: 14 fetches, 42 requests against an allowance of 3). The
    /// spacing is measured from the newest poll, so the next PR is fetched
    /// once it has elapsed.
    #[tokio::test]
    async fn a_stale_backlog_drains_within_the_quota_share() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitors_max_per_agent(20)
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500)
            .with_pr_monitor_quota_share_percent(50);
        for pr in 1_u64..=14 {
            let (m, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            backdate(&svc, &m.monitor_id, &format!("2020-01-01T00:00:{pr:02}Z")).await;
        }
        forge.take_fetched_numbers();
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        forge.edit(|s| {
            s.rate_limit_remaining = Some(6);
            s.rate_limit_limit = Some(5_000);
            s.rate_limit_reset_at = Some(now_unix + 1_800);
        });

        // Sixty ticks 30s apart span the 1800s to the reset.
        let mut fetched = Vec::new();
        for tick in 0..60 {
            if tick > 0 {
                age_all(&svc, 30).await;
            }
            svc.poll_due_pr_monitors().await;
            fetched.extend(forge.take_fetched_numbers());
        }
        assert_eq!(
            fetched,
            vec![1],
            "the share pays for one fetch before the reset, the oldest PR"
        );
        // 1800s after that poll the spacing has elapsed: the next-oldest
        // PR is fetched.
        age_all(&svc, 30).await;
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.take_fetched_numbers(), vec![2]);
    }

    /// The early lift clears the annotation even on a monitor the lifted
    /// tick does NOT poll (not due yet): the reconciliation is the bulk
    /// clear, not a side effect of the post-lift poll — and a genuine
    /// error the row carried in front of the annotation survives it.
    #[tokio::test]
    async fn an_early_lift_clears_the_annotation_on_monitors_it_does_not_poll() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(3600)
            .with_pr_monitor_hourly_request_budget(1500);
        let (fresh, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let (failing, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 2)
            .await
            .expect("register");
        svc.record_pr_monitor_error(&failing, "forge down").await;
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 3600));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        let pause_error = expected_pause_error(&svc);
        let stamped = svc.store().get_pr_monitor(&fresh.monitor_id).await.unwrap();
        assert_eq!(stamped.last_error.as_deref(), Some(pause_error.as_str()));
        forge.take_fetched_numbers();

        // Both rows were polled at registration and the cadence is an hour:
        // the lifted due-sweep has nothing due, yet the annotations go.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(5_000);
            s.rate_limit_limit = Some(5_000);
        });
        svc.poll_due_pr_monitors().await;
        assert!(svc.sweep_rate_limit_paused_until().is_none());
        assert!(forge.take_fetched_numbers().is_empty(), "nothing was due");
        let cleared = svc.store().get_pr_monitor(&fresh.monitor_id).await.unwrap();
        assert_eq!(cleared.last_error, None);
        assert_eq!(
            cleared.last_polled_at, stamped.last_polled_at,
            "the clear is not a poll"
        );
        assert_eq!(
            cleared.updated_at, stamped.updated_at,
            "the guard token is untouched"
        );
        let kept = svc
            .store()
            .get_pr_monitor(&failing.monitor_id)
            .await
            .unwrap();
        assert_eq!(kept.last_error.as_deref(), Some("forge down"));
    }

    /// A pause annotation persisted by the previous process outlives the
    /// in-memory gate: boot rehydration, running with an open gate, clears
    /// it from every active row (a genuine error in front survives, a
    /// terminal row is untouched), so the surfaces do not report a pause
    /// the new process is not observing. Rehydration while the gate IS
    /// paused (a transfer import mid-pause) leaves the annotations alone.
    #[tokio::test]
    async fn rehydration_clears_a_persisted_pause_annotation_when_the_gate_is_open() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (clean, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let (failing, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 2)
            .await
            .expect("register");
        svc.record_pr_monitor_error(&failing, "forge down").await;
        let mut completed = clean.clone();
        completed.monitor_id = PrMonitorId::new();
        completed.pr_number = 3;
        completed.state = PrMonitorState::Completed;
        // The previous process stamped a deadline still an hour out.
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .cast_signed();
        let persisted =
            crate::rate_limit::pause_error(Some(&intent_core::iso_from_unix_secs(now_unix + 3600)));
        completed.last_error = Some(persisted.clone());
        assert!(svc.store().insert_pr_monitor(&completed).await.unwrap());
        assert_eq!(
            svc.store()
                .annotate_active_pr_monitors_pause(&persisted)
                .await
                .unwrap(),
            2
        );
        let errors = |svc: &Services| {
            let ids = [
                clean.monitor_id.clone(),
                failing.monitor_id.clone(),
                completed.monitor_id.clone(),
            ];
            let svc = svc.clone();
            async move {
                let mut out = Vec::new();
                for id in &ids {
                    out.push(svc.store().get_pr_monitor(id).await.unwrap().last_error);
                }
                out
            }
        };
        assert_eq!(
            errors(&svc).await,
            vec![
                Some(persisted.clone()),
                Some(format!("forge down; {persisted}")),
                Some(persisted.clone()),
            ]
        );

        // The new process boots with an open gate.
        assert!(svc.sweep_rate_limit_paused_until().is_none());
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 2);
        assert_eq!(
            errors(&svc).await,
            vec![
                None,
                Some("forge down".to_string()),
                Some(persisted.clone())
            ]
        );
        let listed = svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap();
        for row in listed["monitors"].as_array().unwrap() {
            assert!(row.get("pausedUntil").is_none(), "{row}");
        }

        // Rehydrating while the gate is paused keeps the stamps in place.
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix.cast_unsigned() + 3600));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        let pause_error = expected_pause_error(&svc);
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 2);
        assert_eq!(
            errors(&svc).await,
            vec![
                Some(pause_error.clone()),
                Some(format!("forge down; {pause_error}")),
                Some(persisted),
            ]
        );
    }

    /// intent-hq/intentd#1945 (review r4033765063): a write-back that read
    /// the gate closed BEFORE an early lift lands after the lift's clear —
    /// the clear moves no guard token — carrying the lifted, unexpired
    /// annotation. It must not resurrect it: the row was cleared, so the
    /// landed `lastError` is empty (a genuine error lands alone), and the
    /// next sweep stays clean. Driven deterministically like
    /// [`a_stamp_landing_between_the_gate_read_and_the_write_back_is_kept`]:
    /// the gate is held at what the capture saw while the production clear
    /// is landed ahead of the write-back — to the UPDATE, indistinguishable
    /// from the lift racing in after the gate read.
    #[tokio::test]
    async fn a_stale_pre_lift_capture_landing_after_the_clear_introduces_no_pause() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (monitor, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 3600));
        svc.pause_sweeps_for_rate_limit(&sc, "quota").await;
        let pause_error = expected_pause_error(&svc);
        let read = |svc: &Services| {
            let store = svc.store().clone();
            let id = monitor.monitor_id.clone();
            async move { store.get_pr_monitor(&id).await.unwrap() }
        };

        // The row image a sweep read mid-pause, and the fetch it completed.
        let in_flight = read(&svc).await;
        assert_eq!(in_flight.last_error.as_deref(), Some(pause_error.as_str()));
        forge.edit(|s| s.conversation_comments = 3);
        let shared = fetch_shared_snapshot(sc.as_ref(), &monitor.repo(), 1)
            .await
            .expect("fetch");

        // The lift's reconciliation lands first; the write-back still
        // captures the closed gate.
        assert_eq!(
            svc.store()
                .clear_active_pr_monitors_pause(Some(&pause_error))
                .await
                .unwrap(),
            1
        );
        assert!(
            svc.sweeps_rate_limited(),
            "the capture reads the gate closed"
        );
        svc.poll_one_pr_monitor(&in_flight, &shared)
            .await
            .expect("the guarded write-back lands");
        let landed = read(&svc).await;
        assert_ne!(landed.updated_at, in_flight.updated_at, "the poll landed");
        assert_ne!(landed.last_snapshot, in_flight.last_snapshot);
        assert_eq!(
            landed.last_error, None,
            "the stale capture does not resurrect the cleared pause"
        );

        // A genuine error captured with the stale annotation lands alone.
        svc.record_pr_monitor_error(&landed, "forge down").await;
        assert_eq!(read(&svc).await.last_error.as_deref(), Some("forge down"));

        // The gate is open (the lift the clear belonged to); the next sweep
        // finds nothing to clear and reports no pause.
        assert!(svc.sweep_rate_limit.lift());
        svc.poll_pr_monitors().await;
        assert_eq!(read(&svc).await.last_error, None);
        let listed = svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap();
        let row = &listed["monitors"].as_array().unwrap()[0];
        assert!(row.get("pausedUntil").is_none(), "{row}");
        assert!(row.get("lastError").is_none(), "{row}");
    }

    /// intent-hq/intentd#1945 (review r4033765051): a gate transition and
    /// the statement reconciling the rows to it are one critical section
    /// ([`crate::rate_limit::RateLimitGate::reconcile`]). A trigger's
    /// pause + stamp, a lift's lift + clear, and boot's check + clear each
    /// wait for a held section as a whole — the gate does not move and no
    /// row changes until it is released — so a lift's clear can no longer
    /// land between a fresh trigger's transition and its stamp (nor the
    /// reverse), the schedule in which the delayed clear erased the new
    /// pause while the gate held it.
    #[tokio::test]
    async fn gate_transitions_and_their_reconciliation_serialize_on_the_gate() {
        async fn settle() {
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let (monitor, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 1)
            .await
            .expect("register");
        let sc: Arc<dyn SourceControl> = Arc::new(forge.clone());
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        forge.edit(|s| s.rate_limit_reset_at = Some(now_unix + 3600));
        let last_error = |svc: &Services| {
            let store = svc.store().clone();
            let id = monitor.monitor_id.clone();
            async move { store.get_pr_monitor(&id).await.unwrap().last_error }
        };

        // A trigger: neither the gate nor the row moves while the section
        // is held; both move once it is released.
        let held = svc.sweep_rate_limit.reconcile().await;
        let trigger = tokio::spawn({
            let (svc, sc) = (svc.clone(), sc.clone());
            async move { svc.pause_sweeps_for_rate_limit(&sc, "quota").await }
        });
        settle().await;
        assert!(!trigger.is_finished(), "the trigger waits for the section");
        assert!(
            svc.sweep_rate_limit_paused_until().is_none(),
            "the gate does not move under a held section"
        );
        assert_eq!(last_error(&svc).await, None, "nor is a stamp landed");
        drop(held);
        trigger.await.unwrap();
        let pause_error = expected_pause_error(&svc);
        assert_eq!(last_error(&svc).await, Some(pause_error.clone()));

        // A lift: the probe runs, then the lift + clear wait for the section.
        forge.edit(|s| {
            s.rate_limit_remaining = Some(5_000);
            s.rate_limit_limit = Some(5_000);
        });
        let held = svc.sweep_rate_limit.reconcile().await;
        let lift = tokio::spawn({
            let (svc, sc) = (svc.clone(), sc.clone());
            async move { svc.maybe_lift_rate_limit_pause(&sc).await }
        });
        settle().await;
        assert!(!lift.is_finished(), "the lift waits for the section");
        assert!(svc.sweeps_rate_limited(), "the gate stays closed");
        assert_eq!(last_error(&svc).await, Some(pause_error.clone()));
        drop(held);
        assert!(lift.await.unwrap().is_some(), "the pause lifted");
        assert!(!svc.sweeps_rate_limited());
        assert_eq!(last_error(&svc).await, None);

        // Boot's check + clear wait too.
        assert_eq!(
            svc.store()
                .annotate_active_pr_monitors_pause(&pause_error)
                .await
                .unwrap(),
            1
        );
        let held = svc.sweep_rate_limit.reconcile().await;
        let boot = tokio::spawn({
            let svc = svc.clone();
            async move { svc.rehydrate_pr_monitors().await }
        });
        settle().await;
        assert!(!boot.is_finished(), "rehydration waits for the section");
        assert_eq!(last_error(&svc).await, Some(pause_error.clone()));
        drop(held);
        assert_eq!(boot.await.unwrap().unwrap(), 1);
        assert_eq!(last_error(&svc).await, None);
    }

    /// Sibling monitors on one PR count once toward the effective interval
    /// and share the one fetch: two agents each watching the same four PRs
    /// keep the configured 30s cadence and one tick fetches all four.
    #[tokio::test]
    async fn sibling_monitors_on_one_pr_count_once_toward_the_cadence() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        for pr in 1..=4_u64 {
            for (ws, who) in [(&ws, &owner), (&ws2, &sibling)] {
                let (m, _) = svc
                    .pr_monitor_register(ws, who, "o", "r", pr)
                    .await
                    .expect("register");
                backdate(&svc, &m.monitor_id, "2020-01-01T00:00:00Z").await;
            }
        }
        forge.take_fetched_numbers();

        // 8 monitors, 4 distinct PRs: not stretched (4 × 3 × 3600 / 1500 =
        // 28.8 < 30), so the cap is 4 and every PR is fetched exactly once.
        svc.poll_due_pr_monitors().await;
        assert_eq!(
            forge.take_fetched_numbers(),
            vec![1, 2, 3, 4],
            "one fetch per distinct PR, PR key breaks the anchor tie"
        );
    }

    /// Staggered siblings: two agents watching the same ten PRs, one
    /// sibling stale and the other fresh. A due PR polls BOTH siblings off
    /// the one fetch (aligning their `lastPolledAt`), so over a simulated
    /// stretch the distinct-PR fetch count stays within the hourly budget —
    /// per-monitor freshness would have polled the two siblings on separate
    /// ticks and spent twice the budget.
    #[tokio::test]
    async fn staggered_siblings_share_fetches_and_respect_the_budget() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc
            .with_pr_monitors_max_per_agent(20)
            .with_pr_monitor_poll_seconds(30)
            .with_pr_monitor_hourly_request_budget(1500);
        let (ws2, sibling) = sibling_workspace(&svc, "agent-prmon-sibling").await;
        let fresh_stamp = (time::OffsetDateTime::now_utc() - time::Duration::seconds(50))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        for pr in 1..=10_u64 {
            let (stale, _) = svc
                .pr_monitor_register(&ws, &owner, "o", "r", pr)
                .await
                .expect("register");
            backdate(
                &svc,
                &stale.monitor_id,
                &format!("2020-01-01T00:00:{:02}Z", 10 - pr),
            )
            .await;
            let (fresh, _) = svc
                .pr_monitor_register(&ws2, &sibling, "o", "r", pr)
                .await
                .expect("register sibling");
            backdate(&svc, &fresh.monitor_id, &fresh_stamp).await;
        }
        forge.take_fetched_numbers();
        let started = time::OffsetDateTime::now_utc();

        // 10 distinct PRs → 72s effective interval, cap 5 per tick. The
        // first tick reaches the five stalest PRs and stamps BOTH siblings.
        svc.poll_due_pr_monitors().await;
        assert_eq!(forge.take_fetched_numbers(), vec![10, 9, 8, 7, 6]);
        for row in svc.store().load_active_pr_monitors().await.unwrap() {
            let polled_at = row.last_polled_at.as_deref().and_then(parse_iso).unwrap();
            if row.pr_number >= 6 {
                assert!(polled_at >= started, "PR {} sibling stamped", row.pr_number);
            } else {
                assert!(polled_at < started, "PR {} untouched", row.pr_number);
            }
        }

        // Eight more ticks 30s apart (4.5 simulated minutes in total): the
        // budget allows 1500 × 4.5 / 60 = 112 REST calls = 37 fetches.
        let mut fetches = 5;
        for _ in 0..8 {
            age_all(&svc, 30).await;
            svc.poll_due_pr_monitors().await;
            fetches += forge.take_fetched_numbers().len();
        }
        let per_poll = usize::try_from(PR_MONITOR_REQUESTS_PER_POLL).unwrap();
        assert!(
            fetches * per_poll <= 1500 * 9 * 30 / 3600,
            "{fetches} fetches over 9 ticks exceed the hourly budget"
        );
    }

    /// The pure selection helper: PRs are grouped across siblings (oldest
    /// sibling anchor wins, a `None` anchor sorts oldest), a PR whose only
    /// stale sibling is fresh but catch-up-unattempted is still due, fresh
    /// PRs are skipped, due PRs are ordered oldest anchor then PR key, the
    /// first `cap` are kept, and every monitor on a kept PR is returned in
    /// that order so siblings share one fetch.
    #[test]
    fn select_due_pr_monitors_orders_by_oldest_anchor_and_keeps_siblings() {
        let mk = |pr: i64| PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: WorkspaceId::from("ws-1"),
            agent_id: AgentId::from("agent-1"),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: pr,
            state: PrMonitorState::Active,
            last_snapshot: None,
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };
        let now = time::OffsetDateTime::from_unix_timestamp(1_600_000_100).unwrap();
        let at = |secs: i64| {
            Some(time::OffsetDateTime::from_unix_timestamp(1_600_000_000 + secs).unwrap())
        };
        let candidate = |anchor, catch_up, pr| DueCandidate {
            anchor,
            catch_up,
            monitor: mk(pr),
        };
        let interval = time::Duration::seconds(60);
        assert!(select_due_pr_monitors(vec![], now, interval, 1).is_empty());
        let candidates = vec![
            candidate(at(30), false, 1),
            candidate(at(10), false, 2),
            // PR 3: a fresh sibling (20s old) plus a sibling whose own
            // anchor is the oldest of all — it drags PR 3 to the front and
            // both PR-3 monitors come along.
            candidate(at(80), false, 3),
            candidate(at(0), false, 3),
            candidate(None, false, 4),
            // PR 5 is fresh but catch-up-unattempted: due regardless.
            candidate(at(90), true, 5),
            // PR 6 is fresh: skipped.
            candidate(at(90), false, 6),
            // PR 7 ties PR 2's anchor: the PR key breaks the tie.
            candidate(at(10), false, 7),
        ];
        let kept = |cap| -> Vec<i64> {
            select_due_pr_monitors(candidates.clone(), now, interval, cap)
                .into_iter()
                .map(|m| m.pr_number)
                .collect()
        };
        assert_eq!(kept(2), vec![4, 3, 3]);
        assert_eq!(kept(usize::MAX), vec![4, 3, 3, 2, 7, 1, 5]);
    }

    #[tokio::test]
    async fn a_forge_error_records_last_error_without_touching_the_baseline() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let baseline = monitor.last_snapshot.clone();

        forge.edit(|s| s.fail_get_pr = true);
        svc.poll_pr_monitors().await;
        let failed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(failed.state, PrMonitorState::Active, "the loop survives");
        assert!(failed.last_error.is_some(), "error recorded");
        assert_eq!(failed.last_snapshot, baseline, "baseline untouched");
        assert!(
            svc.sweep_rate_limit.paused_remaining().is_none(),
            "an ordinary forge error never pauses the rate-limit gate"
        );

        // Recovery clears the error and resumes diffing from that baseline.
        forge.edit(|s| {
            s.fail_get_pr = false;
            s.conversation_comments = 1;
        });
        svc.poll_pr_monitors().await;
        let recovered = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(recovered.last_error.is_none());
        assert!(!recovered.pending_changes.is_empty());
    }

    /// Regression (intentd#1923 re-review, round 2): a snapshot persisted
    /// before `observedAt` existed has UNKNOWN freshness, so it must never
    /// hold a workspace-owned Closed copy off. `last_error == None` is not a
    /// stand-in for "the last poll succeeded": the flush
    /// (`emit_pending_changes`) clears the error while keeping both the
    /// stale snapshot and the failed attempt's `last_polled_at`.
    #[tokio::test]
    async fn a_flushed_legacy_row_never_blocks_a_newer_closed_copy() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // Forge the pre-upgrade row: open snapshot without `observedAt`, a
        // pending set awaiting its debounced wake, last successful poll T1.
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let strip = |col: &Option<String>| -> Option<String> {
            let mut v: Value = serde_json::from_str(col.as_deref()?).ok()?;
            v.as_object_mut()?.remove("observedAt");
            serde_json::to_string(&v).ok()
        };
        let (last, baseline) = (strip(&row.last_snapshot), strip(&row.baseline_snapshot));
        assert!(!last.as_deref().unwrap().contains("observedAt"));
        let pending = vec!["conversation comments: 0 → 1".to_string()];
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: last.as_deref(),
                    baseline_snapshot: baseline.as_deref(),
                    pending_changes: &pending,
                    pending_since: Some("2026-01-03T00:00:00Z"),
                    last_change_at: Some("2026-01-03T00:00:00Z"),
                    last_polled_at: Some("2026-01-03T00:00:00Z"),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .unwrap());

        // The PR closes at T2; the hover fold writes the workspace copy.
        let closed_copy = PullRequestInfo {
            id: "42".into(),
            number: 42,
            url: "https://github.com/o/r/pull/42".into(),
            title: "Add thing".into(),
            status: PullRequestStatus::Closed,
            created_at: String::new(),
            updated_at: "2026-01-04T00:00:00Z".into(),
            base_ref: None,
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: None,
            mergeable_state: None,
            is_draft: None,
        };

        // T3: a failed poll advances `last_polled_at` past T2 with an error.
        forge.edit(|s| s.fail_get_pr = true);
        svc.poll_pr_monitors().await;
        let errored = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(errored.last_error.is_some());
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&errored), &[&closed_copy]),
            MonitorPrSignals::default(),
            "under a recorded error the legacy row yields to the copy"
        );

        // The flush delivers the pending set and clears `last_error` while
        // keeping the stale snapshot and the failed attempt's poll time.
        assert!(svc
            .pr_monitor_flush(&ws, &monitor.monitor_id)
            .await
            .unwrap());
        let flushed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(flushed.last_error.is_none());
        assert!(flushed.pending_changes.is_empty());
        assert_eq!(flushed.last_polled_at, errored.last_polled_at);
        assert_eq!(flushed.last_snapshot, errored.last_snapshot);
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&flushed), &[&closed_copy]),
            MonitorPrSignals::default(),
            "a flushed legacy row still yields to the newer closed copy"
        );
    }

    #[tokio::test]
    async fn rehydration_resumes_active_monitors_and_delivers_downtime_changes_immediately() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // A window that would suppress the wake if debounce still applied.
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // The PR moves while the daemon is "down", then the daemon boots.
        forge.edit(|s| s.approvals.push("reviewer".into()));
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        svc.poll_pr_monitors().await;

        let text = owner_messages(&svc, &owner).await;
        assert!(
            text.contains("[PR monitor o/r#42]"),
            "the catch-up wake fires without debounce: {text}"
        );
        let drained = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(drained.pending_changes.is_empty());

        // Debounce applies again from the next change onward.
        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(!held.pending_changes.is_empty(), "held for the window");
        assert_eq!(
            owner_messages(&svc, &owner)
                .await
                .matches("[PR monitor o/r#42]")
                .count(),
            1,
            "no second wake yet"
        );
    }

    /// Restart catch-up only fires on a NON-EMPTY net diff: downtime churn
    /// that reverted before the daemon came back nets to nothing pending,
    /// so the first post-restart poll stays silent.
    #[tokio::test]
    async fn rehydration_catch_up_stays_silent_when_the_net_diff_is_empty() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // Churn lands and reverts across two polls, then the daemon
        // "restarts": the net diff against the emit baseline is empty.
        forge.edit(|s| s.conversation_comments = 2);
        svc.poll_pr_monitors().await;
        forge.edit(|s| s.conversation_comments = 0);
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        svc.poll_pr_monitors().await;

        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "no catch-up wake for a net-empty diff"
        );
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(row.pending_changes.is_empty());
        assert!(row.pending_since.is_none());
    }

    /// The upgrade path: a pre-coalescing row carries an accumulated
    /// pending log, and the migration backfills `baseline_snapshot =
    /// last_snapshot` — so the first recomputing poll would find an empty
    /// net diff and silently discard the wake awaiting delivery. Boot
    /// rehydration must deliver that legacy set as-is instead.
    #[tokio::test]
    async fn rehydration_delivers_a_legacy_pending_set_the_recompute_would_drop() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        // Forge a post-migration legacy row: an accumulated log that the
        // (baseline == last_snapshot) recompute cannot reproduce.
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let legacy = vec![
            "check build: pending → failed".to_string(),
            "check build: failed → passed".to_string(),
        ];
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.last_snapshot.as_deref(),
                    pending_changes: &legacy,
                    pending_since: Some(&now_iso()),
                    last_change_at: Some(&now_iso()),
                    last_polled_at: row.last_polled_at.as_deref(),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .unwrap());

        // Boot: the legacy set is delivered by rehydration itself, before
        // any poll gets a chance to recompute it away.
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        let text = owner_messages(&svc, &owner).await;
        assert!(
            text.contains("check build: failed → passed"),
            "the legacy accumulated lines are delivered, not dropped: {text}"
        );

        // The first poll then finds nothing pending and stays silent.
        svc.poll_pr_monitors().await;
        let drained = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(drained.pending_changes.is_empty());
        assert_eq!(
            owner_messages(&svc, &owner)
                .await
                .matches("[PR monitor o/r#42]")
                .count(),
            1,
            "exactly one wake: the legacy delivery"
        );
    }

    /// Strip the ejection-tracking fields from a monitor's persisted
    /// snapshot columns, forging a row written before the upgrade.
    async fn strip_ejection_tracking(svc: &Services, id: &PrMonitorId) {
        let row = svc.store().get_pr_monitor(id).await.unwrap();
        let strip = |col: &Option<String>| -> Option<String> {
            let mut v: Value = serde_json::from_str(col.as_deref()?).ok()?;
            let obj = v.as_object_mut()?;
            obj.remove("ejectionTracked");
            obj.get_mut("requirements")?
                .as_object_mut()?
                .remove("mergeQueueEjection");
            serde_json::to_string(&v).ok()
        };
        let (last, baseline) = (strip(&row.last_snapshot), strip(&row.baseline_snapshot));
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                id,
                PrMonitorPollUpdate {
                    last_snapshot: last.as_deref(),
                    baseline_snapshot: baseline.as_deref(),
                    pending_changes: &row.pending_changes,
                    pending_since: row.pending_since.as_deref(),
                    last_change_at: row.last_change_at.as_deref(),
                    last_polled_at: row.last_polled_at.as_deref(),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .unwrap());
    }

    /// The upgrade path for ejection tracking: a baseline persisted before
    /// `mergeQueueEjection` existed carries no event, so the first
    /// post-upgrade poll would misread whatever HISTORICAL removal event the
    /// probe reports as news and emit a false wake. The poll must adopt the
    /// event into the pre-upgrade baseline silently; only a LATER event (new
    /// `at`) is reportable.
    #[tokio::test]
    async fn a_pre_upgrade_baseline_adopts_a_historical_ejection_silently() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(MIN_PR_MONITOR_DEBOUNCE_SECONDS);
        let monitor = register(&svc, &ws, &owner).await;
        strip_ejection_tracking(&svc, &monitor.monitor_id).await;

        // The PR was ejected long before the upgrade: the probe reports the
        // stale event on the first post-upgrade poll.
        forge.edit(|s| {
            s.merge_queue_removal = Some(intent_sourcecontrol::MergeQueueRemoval {
                at: "2020-01-01T00:00:00Z".into(),
                reason: Some("failed_checks".into()),
            });
        });
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            !row.pending_changes
                .iter()
                .any(|c| c.contains("merge queue")),
            "the historical event is adopted, not reported: {:?}",
            row.pending_changes
        );
        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "no false post-upgrade wake"
        );

        // A NEW ejection after adoption is real news: it enters the pending
        // set (the debounce window then carries it to the wake as usual).
        forge.edit(|s| {
            s.merge_queue_removal = Some(intent_sourcecontrol::MergeQueueRemoval {
                at: "2026-02-03T04:05:06Z".into(),
                reason: Some("failed_checks".into()),
            });
        });
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            row.pending_changes
                .iter()
                .any(|c| c == "removed from the merge queue (failed checks)"),
            "a post-adoption ejection is reportable: {:?}",
            row.pending_changes
        );
    }

    /// A transient merge-requirements probe failure after an ejection was
    /// observed must not read as "no ejection": the pending line survives
    /// the degraded poll, reaches the wake exactly once, and does not
    /// re-report after the probe recovers with the same event.
    #[tokio::test]
    async fn a_degraded_probe_holds_an_observed_ejection_until_the_wake() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;

        forge.edit(|s| {
            s.merge_queue_removal = Some(intent_sourcecontrol::MergeQueueRemoval {
                at: "2026-01-02T03:04:05Z".into(),
                reason: Some("failed_checks".into()),
            });
        });
        svc.poll_pr_monitors().await;

        // The probe degrades (get_pr still answers): the observed event
        // must survive the recompute instead of emptying the pending set.
        forge.edit(|s| s.fail_merge_requirements = true);
        svc.poll_pr_monitors().await;
        let held = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            held.pending_changes
                .iter()
                .any(|c| c == "removed from the merge queue (failed checks)"),
            "the ejection line survives the degraded probe: {:?}",
            held.pending_changes
        );

        // Recovery reports the SAME event: still pending, not duplicated.
        forge.edit(|s| s.fail_merge_requirements = false);
        svc.poll_pr_monitors().await;
        let recovered = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(
            recovered
                .pending_changes
                .iter()
                .filter(|c| c.contains("merge queue"))
                .count(),
            1,
            "one ejection line, no duplicate: {:?}",
            recovered.pending_changes
        );

        // The window closes: the wake carries the ejection exactly once.
        let svc = svc.with_pr_monitor_debounce_seconds(MIN_PR_MONITOR_DEBOUNCE_SECONDS);
        let stale = now_iso();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: recovered.last_snapshot.as_deref(),
                    baseline_snapshot: recovered.baseline_snapshot.as_deref(),
                    pending_changes: &recovered.pending_changes,
                    pending_since: Some("2020-01-01T00:00:00Z"),
                    last_change_at: Some("2020-01-01T00:00:00Z"),
                    last_polled_at: Some(&stale),
                    last_error: None,
                    updated_at: &stale,
                    expected_updated_at: &recovered.updated_at,
                },
            )
            .await
            .unwrap());
        svc.poll_pr_monitors().await;
        let text = owner_messages(&svc, &owner).await;
        assert_eq!(
            text.matches("removed from the merge queue (failed checks)")
                .count(),
            1,
            "the held event reaches the wake exactly once: {text}"
        );

        // Post-wake polls with the same event stay quiet.
        svc.poll_pr_monitors().await;
        let text = owner_messages(&svc, &owner).await;
        assert_eq!(
            text.matches("removed from the merge queue (failed checks)")
                .count(),
            1,
            "no re-report after the wake: {text}"
        );
    }

    #[tokio::test]
    async fn rehydration_cancels_monitors_whose_owner_is_gone() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        svc.store()
            .set_agent_session_status(&ws, &owner, AgentStatus::Deleted, false, &now_iso(), None)
            .await
            .expect("delete owner");

        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 0);
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Cancelled);
    }

    /// Retire sweep (`ws.agent.retire`): the retiring agent's ACTIVE PR
    /// monitors are cancelled with NO wake notice (the owner retired itself
    /// and is inert); a sibling agent's monitor survives, and
    /// `agent.restore` does NOT resurrect the cancelled monitor.
    #[tokio::test]
    async fn retire_cancels_active_pr_monitors_without_waking_the_owner() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        let sibling_monitor = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 7)
            .await
            .expect("sibling register")
            .0;

        let res = svc
            .agent_retire_op(owner.clone(), None, None)
            .await
            .expect("retire");
        assert_eq!(res["success"], json!(true));

        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Cancelled);
        let row = svc
            .store()
            .get_pr_monitor(&sibling_monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Active, "sibling untouched");
        // NO wake notice for the retired owner (contrast `pr.unmonitor`'s
        // app-path notify and the archive sweep's notice).
        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "no cancellation wake for the retired owner"
        );

        // Restore does NOT resurrect: the row stays cancelled and boot
        // rehydration resumes only the sibling's active monitor.
        svc.agent_restore_op(owner.clone(), None)
            .await
            .expect("restore");
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Cancelled);
    }

    /// Restart backstop: boot rehydration cancels active monitors whose
    /// owner is soft-retired — a crash window could have missed the
    /// retire-time sweep ([`Services::cancel_agent_pr_monitors`]).
    #[tokio::test]
    async fn rehydration_cancels_monitors_of_retired_owner() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        assert!(svc
            .store()
            .set_agent_session_retired_at(&ws, &owner, Some(&now_iso()), &now_iso())
            .await
            .unwrap());

        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 0);
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Cancelled);
    }

    /// The MCP/wire op surface: `pr.monitor` defaults the repo to the
    /// workspace's own, `pr.monitors` projects the list-surface fields the FE
    /// hover needs, and `pr.unmonitor` resolves the caller's monitor by
    /// `(repo, prNumber)`.
    #[tokio::test]
    async fn monitor_ops_resolve_the_workspace_repo_and_project_the_list_payload() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;

        // No `repo` override → the workspace's own `o/r`.
        let started = svc
            .pr_monitor_start_op(&ws, &owner, 42, None)
            .await
            .expect("start");
        assert_eq!(started["ok"], json!(true));
        assert_eq!(started["monitor"]["repo"], json!("o/r"));
        assert_eq!(started["monitor"]["state"], json!("active"));
        assert_eq!(started["requirements"]["state"], json!("open"));

        // An accumulated (undelivered) change surfaces on the list payload.
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;

        let listed = svc
            .pr_monitor_list_op(&ws, Some(&owner))
            .await
            .expect("list");
        let rows = listed["monitors"].as_array().expect("array");
        assert_eq!(rows.len(), 1, "one monitor: {listed}");
        let row = &rows[0];
        assert_eq!(row["prNumber"], json!(42));
        assert_eq!(row["title"], json!("Add thing"));
        assert_eq!(row["url"], json!("https://github.com/o/r/pull/42"));
        assert_eq!(row["hasPendingChanges"], json!(true));
        assert!(row["lastChangeAt"].is_string(), "{row}");
        assert_eq!(row["lastSnapshot"]["state"], json!("open"));
        assert_eq!(row["lastSnapshot"]["checks"]["total"], json!(1));
        assert_eq!(row["lastSnapshot"]["approvals"]["needed"], json!(1));
        // Workspace-scoped view sees the same row.
        let ws_view = svc.pr_monitor_list_op(&ws, None).await.expect("ws list");
        assert_eq!(ws_view["monitors"].as_array().map(Vec::len), Some(1));

        // Flush delivers the held wake now; a second flush is a no-op.
        let monitor_id = PrMonitorId::from(row["monitorId"].as_str().unwrap());
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor_id, false)
                .await
                .unwrap(),
            json!({ "ok": true, "flushed": true })
        );
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor_id, false)
                .await
                .unwrap(),
            json!({ "ok": true, "flushed": false })
        );

        // `pr.unmonitor` resolves by (repo, prNumber) and drops the row from
        // the list surfaces; a second call reports NotFound.
        let stopped = svc
            .pr_monitor_stop_op(&ws, &owner, 42, Some("o/r".into()))
            .await
            .expect("stop");
        assert_eq!(stopped["monitor"]["state"], json!("cancelled"));
        assert_eq!(
            svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap(),
            json!({ "monitors": [] })
        );
        let err = svc
            .pr_monitor_stop_op(&ws, &owner, 42, None)
            .await
            .expect_err("no active monitor");
        assert!(err.to_string().contains("no active monitor"), "{err}");
    }

    /// `prMonitor.cancel` (the FE path, no agent caller) notifies the owner.
    #[tokio::test]
    async fn cancel_by_id_op_notifies_the_owning_agent() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let out = svc
            .pr_monitor_cancel_by_id_op(&ws, &monitor.monitor_id)
            .await
            .expect("cancel");
        assert_eq!(out["monitor"]["state"], json!("cancelled"));
        assert!(owner_messages(&svc, &owner)
            .await
            .contains("cancelled from the app"));
    }

    /// Idle-visibility gating: the `waitingOnPrMonitors` stamp applied by
    /// every `agent:idle` emit site carries the owner's ACTIVE monitors only
    /// — light `{ monitorId, repo, prNumber, title? }` metadata, no
    /// requirements/pendingChanges — and is omitted entirely (never `[]`)
    /// when the agent owns no active monitor. Mirrors
    /// `annotate_waiting_on_hooks_stamps_only_when_active_hooks_exist` in
    /// `hook_manager.rs`.
    #[tokio::test]
    async fn annotate_waiting_on_pr_monitors_stamps_only_when_active_monitors_exist() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        // No monitors at all: nothing stamped.
        let mut data = json!({ "agentId": owner.0 });
        let stamped = svc.annotate_waiting_on_pr_monitors(&owner, &mut data).await;
        assert!(stamped.is_empty());
        assert!(
            data.get("waitingOnPrMonitors").is_none(),
            "field omitted when no active monitors: {data}"
        );

        // An active monitor stamps the light entry.
        let monitor = register(&svc, &ws, &owner).await;
        let mut data = json!({ "agentId": owner.0 });
        let stamped = svc.annotate_waiting_on_pr_monitors(&owner, &mut data).await;
        assert_eq!(stamped.len(), 1);
        let entry = &data["waitingOnPrMonitors"][0];
        assert_eq!(entry["monitorId"], json!(monitor.monitor_id));
        assert_eq!(entry["repo"], json!("o/r"));
        assert_eq!(entry["prNumber"], json!(42));
        assert_eq!(entry["title"], json!("Add thing"), "{entry}");
        // Payloads stay light: no requirements/pendingChanges.
        assert!(entry.get("lastSnapshot").is_none());
        assert!(entry.get("pendingChanges").is_none());

        // A cancelled monitor is not active: nothing stamped.
        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        let mut data = json!({ "agentId": owner.0 });
        svc.annotate_waiting_on_pr_monitors(&owner, &mut data).await;
        assert!(
            data.get("waitingOnPrMonitors").is_none(),
            "cancelled monitors never stamp: {data}"
        );

        // Another agent's idle is unaffected by this owner's monitors.
        register(&svc, &ws, &owner).await;
        let other = AgentId::from("agent-other");
        let mut data = json!({ "agentId": other.0 });
        svc.annotate_waiting_on_pr_monitors(&other, &mut data).await;
        assert!(data.get("waitingOnPrMonitors").is_none());
    }

    /// Workspace-batched variant used by `agent.list`/`agent.diagnostics`:
    /// one query groups active monitors by owning agent, and an agent with
    /// none is absent from the map.
    #[tokio::test]
    async fn active_pr_monitors_by_agent_groups_by_owner() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        assert!(svc.active_pr_monitors_by_agent(&ws).await.is_empty());

        let monitor = register(&svc, &ws, &owner).await;
        let by_agent = svc.active_pr_monitors_by_agent(&ws).await;
        assert_eq!(by_agent.len(), 1);
        let entries = &by_agent[&owner.0];
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["monitorId"], json!(monitor.monitor_id));

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        assert!(svc.active_pr_monitors_by_agent(&ws).await.is_empty());
    }

    /// The per-turn snapshot's `prMonitors` labels: active monitors only, with
    /// the pending-changes marker while a debounced emit is accumulating.
    #[tokio::test]
    async fn snapshot_labels_cover_active_monitors_and_mark_pending_changes() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        assert!(svc.active_pr_monitor_labels(&owner).await.is_empty());

        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        assert_eq!(
            svc.active_pr_monitor_labels(&owner).await,
            vec!["o/r#42".to_string()]
        );

        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        assert_eq!(
            svc.active_pr_monitor_labels(&owner).await,
            vec!["o/r#42 (changes pending)".to_string()]
        );

        // Cancelled monitors leave the snapshot.
        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        assert!(svc.active_pr_monitor_labels(&owner).await.is_empty());
    }

    /// The labels reach the wire: `ws.agent.snapshot()` serializes them as
    /// `prMonitors`, an active monitor alone makes the snapshot non-trivial
    /// (so the turn-prompt line injects), and the field is omitted entirely
    /// once no monitor is active.
    #[tokio::test]
    async fn snapshot_serializes_pr_monitors_and_injects_the_turn_line() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;

        // No monitors → field omitted and the snapshot stays trivial.
        let empty = svc
            .agent_snapshot_op(ws.clone(), owner.clone())
            .await
            .expect("snapshot");
        assert!(
            !empty
                .as_object()
                .expect("object")
                .contains_key("prMonitors"),
            "empty prMonitors omitted: {empty}"
        );
        assert!(
            svc.agent_state_snapshot_line(&owner).await.is_none(),
            "trivial snapshot must not inject"
        );

        let monitor = register(&svc, &ws, &owner).await;
        let v = svc
            .agent_snapshot_op(ws.clone(), owner.clone())
            .await
            .expect("snapshot");
        assert_eq!(v["prMonitors"], json!(["o/r#42"]), "serialized: {v}");

        let line = svc
            .agent_state_snapshot_line(&owner)
            .await
            .expect("an active monitor makes the snapshot non-trivial");
        let json_part = line
            .strip_prefix("current ws.agent.snapshot() => ")
            .expect("JSON payload");
        let parsed: Value = serde_json::from_str(json_part).expect("valid JSON");
        assert_eq!(parsed["prMonitors"], json!(["o/r#42"]), "line: {line}");

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        let after = svc
            .agent_snapshot_op(ws.clone(), owner.clone())
            .await
            .expect("snapshot");
        assert!(
            !after
                .as_object()
                .expect("object")
                .contains_key("prMonitors"),
            "cancelled monitor leaves the snapshot: {after}"
        );
    }

    /// Direct child task note of the spec, so it counts into `taskStats`.
    fn task_note(ws: &WorkspaceId, id: &str, status: intent_core::TaskStatus) -> intent_core::Note {
        let ts = now_iso();
        intent_core::Note {
            id: intent_core::NoteId::from(id),
            workspace_id: ws.clone(),
            title: format!("Task {id}"),
            content: String::new(),
            content_type: intent_core::ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: Some(intent_core::NoteId::from("spec")),
            visibility: intent_core::NoteVisibility::Workspace,
            metadata: intent_core::NoteMetadata {
                task: Some(intent_core::TaskMetadata {
                    status,
                    ..Default::default()
                }),
            },
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    /// An ACTIVE PR monitor on an open PR both sets the orthogonal
    /// `waiting` flag on the list/get enrichment path AND feeds the PR
    /// rungs of the derived `displayStatus`: with every task done the
    /// rollup reads `pr_ready` (the stub PR is open, not draft, and its
    /// merge-requirements checklist is clear once the required check
    /// passes) instead of falling through to `complete`. Cancelling the
    /// monitor lapses both — the flag drops and the rollup returns to the
    /// base `complete`.
    #[tokio::test]
    async fn active_pr_monitor_sets_waiting_and_feeds_the_pr_rungs() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        svc.store()
            .insert_note(&task_note(&ws, "t1", intent_core::TaskStatus::Complete))
            .await
            .expect("insert task");
        forge.edit(|s| {
            s.checks[0].state = CheckState::Success;
            s.approvals.push("reviewer".into());
        });
        let monitor = register(&svc, &ws, &owner).await;

        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert!(
            row.waiting,
            "idle owner with an active PR monitor must read waiting"
        );
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrReady),
            "an active monitor on an open mergeable PR reads pr_ready"
        );

        // Settle the monitor: the waiting flag lapses and the rollup
        // returns to the base `complete`.
        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert!(!row.waiting, "terminal monitors never read waiting");
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::Complete),
            "a cancelled monitor's open-PR signal lapses"
        );
    }

    /// Regression: an active monitor on an open PR that is merely
    /// conflict-free (`mergeable: true`) but still blocked by its
    /// merge-requirements checklist (the default stub: a pending required
    /// check) reads `pr_open`, never `pr_ready`.
    #[tokio::test]
    async fn active_pr_monitor_blocked_checklist_reads_pr_open() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        register(&svc, &ws, &owner).await;
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrOpen),
            "a blocked checklist keeps the rollup at pr_open"
        );
    }

    /// The snapshot→signal fold: active open/draft rows raise `open` (and
    /// `ready` only when the full merge-requirements checklist is clear +
    /// not draft), completed merged rows raise `merged`, and rows with
    /// no/unparseable snapshots, non-merged completed rows, or active rows
    /// already showing a terminal snapshot contribute nothing.
    #[test]
    fn fold_monitor_pr_signals_maps_rows_to_signals() {
        let ws = WorkspaceId::new();
        let owner = AgentId::from("agent-fold");
        let ts = "2026-01-01T00:00:00Z".to_string();
        let mk = |state: PrMonitorState, snap: Option<String>| PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            pr_number: 42,
            state,
            last_snapshot: snap,
            baseline_snapshot: None,
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
        };
        let snap = |f: fn(&mut PrMonitorSnapshot)| {
            let mut s = snapshot(|_| {});
            f(&mut s);
            Some(serde_json::to_string(&s).unwrap())
        };

        // Active + open + clear checklist + not draft → open and ready.
        let ready = mk(
            PrMonitorState::Active,
            snap(|s| ready_requirements(&mut s.requirements)),
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&ready), &[]),
            MonitorPrSignals {
                queued: false,
                open: true,
                ready: true,
                merged: false
            }
        );
        // Active + open + in the merge queue + not draft → open and queued
        // (never ready: `requirements_ready` excludes queued PRs, even on
        // an otherwise clear checklist).
        let queued = mk(
            PrMonitorState::Active,
            snap(|s| s.requirements.is_in_merge_queue = Some(true)),
        );
        let queued_clear = mk(
            PrMonitorState::Active,
            snap(|s| {
                ready_requirements(&mut s.requirements);
                s.requirements.is_in_merge_queue = Some(true);
            }),
        );
        for m in [&queued, &queued_clear] {
            assert_eq!(
                fold_monitor_pr_signals(std::slice::from_ref(m), &[]),
                MonitorPrSignals {
                    queued: true,
                    open: true,
                    ready: false,
                    merged: false
                }
            );
        }
        // A draft never reads queued, whatever the flag says.
        let queued_draft = mk(
            PrMonitorState::Active,
            snap(|s| {
                s.requirements.state = "draft".into();
                s.requirements.is_draft = true;
                s.requirements.is_in_merge_queue = Some(true);
            }),
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&queued_draft), &[]),
            MonitorPrSignals {
                queued: false,
                open: true,
                ready: false,
                merged: false
            }
        );
        // Draft, not mergeable, or a blocked checklist (the default
        // fixture: pending required check, review required, unresolved
        // thread, BLOCKED merge state — despite `mergeable: Some(true)`,
        // the intent-hq/intentd#1350 regression shape) → open only.
        let draft = mk(
            PrMonitorState::Active,
            snap(|s| {
                s.requirements.state = "draft".into();
                s.requirements.is_draft = true;
            }),
        );
        let unmergeable = mk(
            PrMonitorState::Active,
            snap(|s| s.requirements.mergeable = Some(false)),
        );
        let blocked = mk(PrMonitorState::Active, snap(|_| {}));
        // A `none` decision (provider reviewDecision unavailable) while the
        // branch rules still demand an approval the PR does not have.
        let missing_required_approval = mk(
            PrMonitorState::Active,
            snap(|s| {
                ready_requirements(&mut s.requirements);
                s.requirements.approvals.decision = "none".into();
                s.requirements.approvals.have = 0;
                s.requirements.approvals.needed = Some(1);
            }),
        );
        // GraphQL `UNKNOWN` merge state: mergeability not yet established.
        let unknown_state = mk(
            PrMonitorState::Active,
            snap(|s| {
                ready_requirements(&mut s.requirements);
                s.requirements.merge_state_status = Some("UNKNOWN".into());
            }),
        );
        for m in [
            &draft,
            &unmergeable,
            &blocked,
            &missing_required_approval,
            &unknown_state,
        ] {
            assert_eq!(
                fold_monitor_pr_signals(std::slice::from_ref(m), &[]),
                MonitorPrSignals {
                    queued: false,
                    open: true,
                    ready: false,
                    merged: false
                }
            );
        }
        // Completed + merged → merged; completed + closed → nothing.
        let merged = mk(
            PrMonitorState::Completed,
            snap(|s| s.requirements.state = "merged".into()),
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&merged), &[]),
            MonitorPrSignals {
                queued: false,
                open: false,
                ready: false,
                merged: true
            }
        );
        let closed = mk(
            PrMonitorState::Completed,
            snap(|s| s.requirements.state = "closed".into()),
        );
        // An active row already showing a terminal snapshot (lost the
        // terminalize write) contributes nothing either.
        let active_terminal = mk(
            PrMonitorState::Active,
            snap(|s| s.requirements.state = "merged".into()),
        );
        let no_snapshot = mk(PrMonitorState::Active, None);
        let bad_blob = mk(PrMonitorState::Active, Some("{not json".into()));
        assert_eq!(
            fold_monitor_pr_signals(&[closed, active_terminal, no_snapshot, bad_blob], &[]),
            MonitorPrSignals::default()
        );
        // Signals aggregate across rows.
        assert_eq!(
            fold_monitor_pr_signals(&[ready.clone(), queued, merged.clone()], &[]),
            MonitorPrSignals {
                queued: true,
                open: true,
                ready: true,
                merged: true
            }
        );
        // Latest-completed semantics (linked-PR step 6): an older merged
        // monitor never shadows a newer closed-unmerged one — only the most
        // recently updated completed row decides `merged`.
        let mut newer_closed = mk(
            PrMonitorState::Completed,
            snap(|s| s.requirements.state = "closed".into()),
        );
        newer_closed.updated_at = "2026-01-02T00:00:00Z".into();
        assert_eq!(
            fold_monitor_pr_signals(&[merged.clone(), newer_closed.clone()], &[]),
            MonitorPrSignals::default(),
            "newer closed-unmerged monitor wins over an older merged one"
        );
        // Order-independent: the fold picks the latest by updated_at, not
        // by slice position.
        assert_eq!(
            fold_monitor_pr_signals(&[newer_closed, merged.clone()], &[]),
            MonitorPrSignals::default()
        );
        // And the reverse: a newer merged monitor after an older closed one.
        let mut newer_merged = mk(
            PrMonitorState::Completed,
            snap(|s| s.requirements.state = "merged".into()),
        );
        newer_merged.updated_at = "2026-01-03T00:00:00Z".into();
        let older_closed = mk(
            PrMonitorState::Completed,
            snap(|s| s.requirements.state = "closed".into()),
        );
        assert_eq!(
            fold_monitor_pr_signals(&[older_closed, newer_merged], &[]),
            MonitorPrSignals {
                queued: false,
                open: false,
                ready: false,
                merged: true
            }
        );

        // Regression (intent-hq/intentd#1923 review): a workspace-owned
        // terminal copy of the monitored PR — written by the passive
        // `github.pulls.get` fold — supersedes the ACTIVE row's stale open
        // snapshot, so the rollup never waits for the monitor sweep.
        let copy = |status: PullRequestStatus, url: &str, updated_at: &str| PullRequestInfo {
            id: "42".into(),
            number: 42,
            url: url.into(),
            title: "Add thing".into(),
            status,
            created_at: String::new(),
            updated_at: updated_at.into(),
            base_ref: None,
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: None,
            mergeable_state: None,
            is_draft: None,
        };
        let mut polled = mk(
            PrMonitorState::Active,
            snap(|s| {
                ready_requirements(&mut s.requirements);
                s.observed_at = Some("2026-01-05T00:00:00Z".into());
            }),
        );
        polled.last_polled_at = Some("2026-01-05T00:00:00Z".into());
        // Merged is irreversible: it supersedes whatever the observation
        // timing, and the URL match folds ASCII case.
        let merged_copy = copy(
            PullRequestStatus::Merged,
            "https://github.com/O/R/pull/42",
            "2026-01-01T00:00:00Z",
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&polled), &[&merged_copy]),
            MonitorPrSignals::default(),
            "a merged workspace copy silences the stale open monitor"
        );
        // Closed can be reopened: only a copy fresher than the snapshot's
        // observation supersedes; an older one (or an unparseable timestamp)
        // does not.
        let fresh_closed = copy(
            PullRequestStatus::Closed,
            "https://github.com/o/r/pull/42",
            "2026-01-06T00:00:00Z",
        );
        let stale_closed = copy(
            PullRequestStatus::Closed,
            "https://github.com/o/r/pull/42",
            "2026-01-04T00:00:00Z",
        );
        let undated_closed = copy(
            PullRequestStatus::Closed,
            "https://github.com/o/r/pull/42",
            "",
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&polled), &[&fresh_closed]),
            MonitorPrSignals::default()
        );
        for stale in [&stale_closed, &undated_closed] {
            assert_eq!(
                fold_monitor_pr_signals(std::slice::from_ref(&polled), &[stale]),
                MonitorPrSignals {
                    queued: false,
                    open: true,
                    ready: true,
                    merged: false
                },
                "an older closed copy yields to the fresher open observation"
            );
        }
        // Regression (intentd#1923 re-review): freshness is the snapshot's
        // OWN observation time, not the last poll attempt. Open snapshot
        // observed at T1 → PR closed at T2 → a failed poll at T3 advanced
        // `last_polled_at` (with a recorded error) but kept the T1 snapshot:
        // the T2 copy is fresher than anything the monitor saw and wins.
        let open_signal = MonitorPrSignals {
            queued: false,
            open: true,
            ready: true,
            merged: false,
        };
        let mut errored = mk(
            PrMonitorState::Active,
            snap(|s| {
                ready_requirements(&mut s.requirements);
                s.observed_at = Some("2026-01-03T00:00:00Z".into());
            }),
        );
        errored.last_polled_at = Some("2026-01-05T00:00:00Z".into());
        errored.last_error = Some("rate limited".into());
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&errored), &[&stale_closed]),
            MonitorPrSignals::default(),
            "a closed copy newer than the last SUCCESSFUL observation supersedes"
        );
        // ...while a snapshot that re-observed the PR open AFTER the copy's
        // timestamp stays the fresher observation, failed poll or not.
        let mut reopened = errored.clone();
        reopened.last_snapshot = snap(|s| {
            ready_requirements(&mut s.requirements);
            s.observed_at = Some("2026-01-04T12:00:00Z".into());
        });
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&reopened), &[&stale_closed]),
            open_signal,
            "the reopened-state protection survives a later failed poll"
        );
        // Legacy snapshot without `observedAt`: unknown freshness. Neither a
        // clean `last_polled_at` nor a recorded error stands in for it — the
        // flush clears `last_error` while keeping the failed attempt's poll
        // time — so the copy wins regardless of the row's poll columns.
        let mut legacy = ready.clone();
        legacy.last_polled_at = Some("2026-01-05T00:00:00Z".into());
        let mut legacy_errored = legacy.clone();
        legacy_errored.last_error = Some("rate limited".into());
        for (row, case) in [(&legacy, "clean poll"), (&legacy_errored, "failed poll")] {
            assert_eq!(
                fold_monitor_pr_signals(std::slice::from_ref(row), &[&stale_closed]),
                MonitorPrSignals::default(),
                "legacy row, {case}: unknown freshness yields to the copy"
            );
        }
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&legacy), &[&undated_closed]),
            open_signal,
            "an unparseable copy timestamp never supersedes, legacy or not"
        );
        // A terminal copy of ANOTHER PR leaves the monitor's signal alone.
        let other = copy(
            PullRequestStatus::Merged,
            "https://github.com/o/r/pull/7",
            "2026-01-06T00:00:00Z",
        );
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&polled), &[&other]),
            MonitorPrSignals {
                queued: false,
                open: true,
                ready: true,
                merged: false
            }
        );
    }

    /// Regression (intent-hq/intentd#1923 review), end to end: a workspace
    /// whose linked PR #42 is also watched by an ACTIVE monitor (open
    /// snapshot) reads `pr_merged` right after `github.pulls.get` folds the
    /// merge — the stale monitor signal yields to the fresh terminal copy
    /// instead of holding the sidebar at `pr_open` until the next sweep —
    /// while the monitor row itself (snapshot, state) is left for the sweep.
    #[tokio::test]
    async fn pulls_get_terminal_fold_overrides_stale_active_monitor_signal() {
        use intent_core::WorkspaceApi;
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        let open =
            crate::pr_ops::build_pr_info(&forge.get_pr(&RepoRef::new("o", "r"), 42).await.unwrap());
        row.pr_number = Some(42);
        row.pr_url = Some(open.url.clone());
        row.pr_status = Some(PullRequestStatus::Open);
        row.active_pull_request = Some(open.clone());
        row.pull_requests = Some(vec![open]);
        svc.store().update_workspace_pr_linkage(&row).await.unwrap();
        let mut before = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut before).await;
        assert_eq!(
            before.display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrReady),
            "linked clean PR + open monitor read pr_ready before the fold"
        );

        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.github_pulls_get("o".into(), "r".into(), 42)
            .await
            .expect("pulls.get");

        let mut after = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(after.pr_status, Some(PullRequestStatus::Merged));
        svc.enrich_workspace_aggregates(&mut after).await;
        assert_eq!(
            after.display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrMerged),
            "the fresh terminal fold outranks the monitor's stale open snapshot"
        );
        let list = svc.list_workspaces(false).await.unwrap();
        assert_eq!(
            list.iter().find(|w| w.id == ws).unwrap().display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrMerged),
            "the list path folds the same way"
        );
        let untouched = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert_eq!(untouched.state, PrMonitorState::Active);
        assert_eq!(untouched.last_snapshot, monitor.last_snapshot);
        assert!(untouched.pending_changes.is_empty());
    }

    /// Regression (intentd#1923 re-review), end to end — the rate-limit
    /// pause case: the monitor observes the PR open (T1), the PR closes on
    /// the forge (T2), a poll FAILS (T3: `last_polled_at` advances, the T1
    /// snapshot stays), then `github.pulls.get` folds the closed copy (T4).
    /// The copy is newer than the monitor's last successful observation, so
    /// the rollup leaves the PR stage instead of holding `pr_ready` on the
    /// stale open snapshot until the forge answers again.
    #[tokio::test]
    async fn pulls_get_closed_fold_overrides_monitor_stale_across_failed_poll() {
        use intent_core::WorkspaceApi;
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let monitor = register(&svc, &ws, &owner).await;
        let observed_at: PrMonitorSnapshot =
            serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
        let observed_at = observed_at
            .observed_at
            .expect("registration stamps observedAt");
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        let open =
            crate::pr_ops::build_pr_info(&forge.get_pr(&RepoRef::new("o", "r"), 42).await.unwrap());
        row.pr_number = Some(42);
        row.pr_url = Some(open.url.clone());
        row.pr_status = Some(PullRequestStatus::Open);
        row.active_pull_request = Some(open.clone());
        row.pull_requests = Some(vec![open]);
        svc.store().update_workspace_pr_linkage(&row).await.unwrap();

        // T2: closed on the forge, strictly after the T1 observation.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let closed_at = now_iso();
        assert!(parse_iso(&closed_at) > parse_iso(&observed_at));
        forge.edit(|s| {
            s.pr_state = PrState::Closed;
            s.updated_at = Some(closed_at.clone());
        });
        // T3: the poll fails; the row records the error and the attempt
        // time, but the snapshot is still the T1 open one.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        forge.edit(|s| s.fail_get_pr = true);
        svc.poll_pr_monitors().await;
        let failed = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(failed.last_error.is_some(), "error recorded");
        assert_eq!(failed.last_snapshot, monitor.last_snapshot, "snapshot kept");
        assert!(
            parse_iso(failed.last_polled_at.as_deref().unwrap()) > parse_iso(&closed_at),
            "the failed attempt is later than the close"
        );

        // T4: the passive fold reads the closed PR straight from the forge.
        forge.edit(|s| s.fail_get_pr = false);
        svc.github_pulls_get("o".into(), "r".into(), 42)
            .await
            .expect("pulls.get");
        let mut after = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(after.pr_status, Some(PullRequestStatus::Closed));
        svc.enrich_workspace_aggregates(&mut after).await;
        assert!(
            !matches!(
                after.display_status,
                Some(
                    intent_core::WorkspaceDisplayStatus::PrReady
                        | intent_core::WorkspaceDisplayStatus::PrOpen
                        | intent_core::WorkspaceDisplayStatus::PrQueued
                )
            ),
            "the closed fold outranks the stale open snapshot despite the failed poll: {:?}",
            after.display_status
        );
    }

    /// Orthogonality with the PR stages: a workspace whose linked PR reads
    /// `pr_ready` keeps that rollup while an active monitor sets `waiting`.
    #[tokio::test]
    async fn waiting_coexists_with_pr_ready_display_status() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        row.active_pull_request = Some(intent_core::PullRequestInfo {
            id: "pr-42".into(),
            number: 42,
            url: "https://github.com/o/r/pull/42".into(),
            title: "Ready PR".into(),
            status: intent_core::PullRequestStatus::Open,
            created_at: now_iso(),
            updated_at: now_iso(),
            base_ref: None,
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            is_draft: Some(false),
        });
        svc.store().update_workspace(&row).await.expect("update");
        register(&svc, &ws, &owner).await;

        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert!(row.waiting, "the wait flag coexists with pr_ready");
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::PrReady),
        );
    }

    /// `workspace_has_active_pr_monitors` is the waiting signal: true only
    /// while a monitor is ACTIVE, false with no monitors and false again once
    /// every monitor is terminal (cancelled/completed).
    #[tokio::test]
    async fn workspace_has_active_pr_monitors_tracks_active_rows_only() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        assert!(!svc.workspace_has_active_pr_monitors(&ws).await);

        let monitor = register(&svc, &ws, &owner).await;
        assert!(svc.workspace_has_active_pr_monitors(&ws).await);

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        assert!(
            !svc.workspace_has_active_pr_monitors(&ws).await,
            "cancelled monitors never promote"
        );

        // A completed monitor (merged PR) is terminal too.
        register(&svc, &ws, &owner).await;
        assert!(svc.workspace_has_active_pr_monitors(&ws).await);
        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        assert!(
            !svc.workspace_has_active_pr_monitors(&ws).await,
            "completed monitors never promote"
        );
    }

    /// Persisted `workspace:displayStatus-changed` payload statuses for a
    /// workspace, oldest-first.
    async fn display_status_events(svc: &Services, ws: &WorkspaceId) -> Vec<String> {
        let mut evs =
            svc.store()
                .query_events(&intent_store::EventQuery {
                    workspace_id: Some(ws.clone()),
                    event_types: vec![
                        intent_core::events::WORKSPACE_DISPLAY_STATUS_CHANGED.to_string()
                    ],
                    ..Default::default()
                })
                .await
                .expect("query displayStatus events");
        evs.reverse();
        evs.into_iter()
            .map(|e| e.data["displayStatus"].as_str().unwrap().to_string())
            .collect()
    }

    /// Monitor lifecycle transitions move the derived `displayStatus`
    /// through the PR rungs (§6.5): registering on an open PR with a clear
    /// merge-requirements checklist emits the `pr_ready` promotion, a
    /// no-op recompute stays silent, and cancelling emits the demotion
    /// back to the base rollup (`idle` here: no tasks, no linked PR).
    #[tokio::test]
    async fn monitor_transitions_emit_display_status_through_the_pr_rungs() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        forge.edit(|s| {
            s.checks[0].state = CheckState::Success;
            s.approvals.push("reviewer".into());
        });
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_display_status_changed(&ws).await;
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        let monitor = register(&svc, &ws, &owner).await;
        assert!(svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string()],
            "an active monitor on an open truly-mergeable PR promotes to pr_ready"
        );

        // Re-running the recompute without a transition emits nothing.
        svc.maybe_emit_display_status_changed(&ws).await;
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string()]
        );

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        assert!(!svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string(), "idle".to_string()],
            "a cancelled monitor's open-PR signal lapses back to the base rollup"
        );
    }

    /// The poll loop's terminal completion (merged PR) drops the waiting
    /// flag and transitions the derived displayStatus from the open-PR rung
    /// to `pr_merged`; a rehydration cancel of an owner-gone monitor drops
    /// the flag while the completed monitor's merged signal persists.
    #[tokio::test]
    async fn completion_and_rehydration_cancel_drop_the_waiting_flag() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        forge.edit(|s| {
            s.checks[0].state = CheckState::Success;
            s.approvals.push("reviewer".into());
        });
        svc.maybe_emit_display_status_changed(&ws).await;

        register(&svc, &ws, &owner).await;
        assert!(svc.workspace_is_waiting(&ws).await);

        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        assert!(!svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string(), "pr_merged".to_string()],
            "completion transitions the derivation to pr_merged"
        );

        // Rehydration cancel (owner gone) drops the flag too; the completed
        // monitor's merged signal keeps the rollup at pr_merged.
        svc.pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .expect("register");
        assert!(svc.workspace_is_waiting(&ws).await);
        svc.store()
            .set_agent_session_status(&ws, &owner, AgentStatus::Deleted, false, &now_iso(), None)
            .await
            .expect("delete owner");
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 0);
        assert!(!svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string(), "pr_merged".to_string()]
        );
    }

    /// Persisted `workspace:waiting-changed` payload flags for a workspace,
    /// oldest-first.
    async fn waiting_events(svc: &Services, ws: &WorkspaceId) -> Vec<bool> {
        let mut evs = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![intent_core::events::WORKSPACE_WAITING_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query waiting events");
        evs.reverse();
        evs.into_iter()
            .map(|e| e.data["waiting"].as_bool().unwrap())
            .collect()
    }

    /// Monitor lifecycle transitions emit `workspace:waiting-changed`
    /// exactly once per actual transition: register raises the flag, a
    /// no-op recompute stays silent, cancel drops it, and the poll loop's
    /// terminal completion (merged PR) drops it too.
    #[tokio::test]
    async fn monitor_transitions_emit_waiting_changed_on_transition_only() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_waiting_changed(&ws).await;
        assert_eq!(waiting_events(&svc, &ws).await, Vec::<bool>::new());

        let monitor = register(&svc, &ws, &owner).await;
        assert_eq!(waiting_events(&svc, &ws).await, vec![true]);

        // Re-running the recompute without a transition emits nothing.
        svc.maybe_emit_waiting_changed(&ws).await;
        assert_eq!(waiting_events(&svc, &ws).await, vec![true]);

        svc.pr_monitor_cancel(&ws, &monitor.monitor_id, Some(&owner))
            .await
            .expect("cancel");
        assert_eq!(waiting_events(&svc, &ws).await, vec![true, false]);

        // The poll loop's terminal completion emits the drop transition too.
        forge.edit(|s| s.pr_state = PrState::Open);
        register(&svc, &ws, &owner).await;
        assert_eq!(waiting_events(&svc, &ws).await, vec![true, false, true]);
        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        assert!(!svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            waiting_events(&svc, &ws).await,
            vec![true, false, true, false]
        );
    }

    /// Regression for intent-hq/monorepo#1828: `workspace.archive` cancels
    /// every ACTIVE PR monitor in the workspace — state persisted to
    /// `cancelled`, `prMonitor:cancelled` emitted, owner told why — while
    /// terminal monitors are untouched, so an archived workspace never
    /// reads `waiting` off a stale monitor signal indefinitely.
    #[tokio::test]
    async fn archive_cancels_active_pr_monitors_and_drops_waiting() {
        use intent_core::WorkspaceApi;
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        forge.edit(|s| {
            s.checks[0].state = CheckState::Success;
            s.approvals.push("reviewer".into());
        });
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_display_status_changed(&ws).await;
        // A terminal (`completed`, not `cancelled`) monitor first — merged
        // via the poll path — so a sweep that (incorrectly) re-touched
        // terminal rows would be observable below.
        let terminal = register(&svc, &ws, &owner).await;
        forge.edit(|s| s.pr_state = PrState::Merged);
        svc.poll_pr_monitors().await;
        let completed_row = svc
            .store()
            .get_pr_monitor(&terminal.monitor_id)
            .await
            .unwrap();
        assert_eq!(completed_row.state, PrMonitorState::Completed);
        // And one ACTIVE monitor promoting the rollup.
        forge.edit(|s| s.pr_state = PrState::Open);
        let (active, _) = svc
            .pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .expect("register");
        assert!(svc.workspace_has_active_pr_monitors(&ws).await);

        let archived = svc
            .archive_workspace(ws.clone(), None)
            .await
            .expect("archive");
        assert!(archived.archived, "workspace archived");

        let row = svc
            .store()
            .get_pr_monitor(&active.monitor_id)
            .await
            .unwrap();
        assert_eq!(row.state, PrMonitorState::Cancelled);
        assert!(
            !svc.workspace_has_active_pr_monitors(&ws).await,
            "no active monitors survive the archive sweep"
        );
        // The terminal monitor is untouched: same state, same updated_at.
        let untouched = svc
            .store()
            .get_pr_monitor(&terminal.monitor_id)
            .await
            .unwrap();
        assert_eq!(untouched.state, PrMonitorState::Completed);
        assert_eq!(
            untouched.updated_at, completed_row.updated_at,
            "the sweep never re-touches terminal rows"
        );
        // The sweep emitted `prMonitor:cancelled` for the swept monitor only.
        let cancelled_events = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![PR_MONITOR_CANCELLED.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(cancelled_events.len(), 1, "one cancel, one event");
        assert_eq!(
            cancelled_events[0].data["monitorId"],
            json!(active.monitor_id.as_str())
        );
        assert_eq!(cancelled_events[0].data["state"], json!("cancelled"));
        // The owner learns why its watch stopped (store-only wake here: no
        // manager attached, so nothing can spawn a turn; the wake parks
        // behind the archived gate at most).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let text = owner_messages(&svc, &owner).await;
            if text.contains("workspace was archived") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "archive wake never delivered; last = {text}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // The lifecycle walked the PR rungs: register promoted to
        // `pr_ready`, completion flipped to `pr_merged`, the re-register
        // promoted again, and the archive sweep's cancel lapsed the open
        // signal back to `pr_merged` (the completed monitor's merged signal
        // persists). The wait flag dropped with the sweep.
        assert!(!svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready", "pr_merged", "pr_ready", "pr_merged"]
        );
    }

    #[tokio::test]
    async fn poll_and_debounce_intervals_clamp_to_their_floors() {
        use intent_core::config::{
            DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS, DEFAULT_PR_MONITOR_POLL_SECONDS,
        };
        let (_db, _root, svc, _forge, _ws, _owner) = setup().await;
        assert_eq!(
            svc.pr_monitor_poll_interval(),
            Duration::from_secs(DEFAULT_PR_MONITOR_POLL_SECONDS)
        );
        assert_eq!(
            svc.pr_monitor_debounce(),
            Duration::from_secs(DEFAULT_PR_MONITOR_DEBOUNCE_SECONDS)
        );
        let clamped = svc
            .clone()
            .with_pr_monitor_poll_seconds(1)
            .with_pr_monitor_debounce_seconds(1);
        assert_eq!(
            clamped.pr_monitor_poll_interval(),
            Duration::from_secs(MIN_PR_MONITOR_POLL_SECONDS)
        );
        assert_eq!(
            clamped.pr_monitor_debounce(),
            Duration::from_secs(MIN_PR_MONITOR_DEBOUNCE_SECONDS)
        );
    }

    /// The hourly request budget getter clamps into [floor, ceiling]: a
    /// hand-edited config below 60 reads as 60 and one above GitHub's
    /// 5,000/h core quota reads as 5000, so the cadence math never plans
    /// more polling than the forge serves.
    #[tokio::test]
    async fn hourly_request_budget_clamps_to_floor_and_ceiling() {
        use intent_core::config::DEFAULT_PR_MONITOR_HOURLY_REQUEST_BUDGET;
        let (_db, _root, svc, _forge, _ws, _owner) = setup().await;
        assert_eq!(
            svc.pr_monitor_hourly_request_budget(),
            DEFAULT_PR_MONITOR_HOURLY_REQUEST_BUDGET
        );
        assert_eq!(
            svc.clone()
                .with_pr_monitor_hourly_request_budget(0)
                .pr_monitor_hourly_request_budget(),
            MIN_PR_MONITOR_HOURLY_REQUEST_BUDGET
        );
        assert_eq!(
            svc.clone()
                .with_pr_monitor_hourly_request_budget(1_000_000)
                .pr_monitor_hourly_request_budget(),
            MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET
        );
        assert_eq!(
            svc.clone()
                .with_pr_monitor_hourly_request_budget(MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET)
                .pr_monitor_hourly_request_budget(),
            MAX_PR_MONITOR_HOURLY_REQUEST_BUDGET
        );
    }
}
