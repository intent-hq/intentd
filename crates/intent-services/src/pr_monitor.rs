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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::events::{
    PR_MONITOR_CANCELLED, PR_MONITOR_CHANGED, PR_MONITOR_COMPLETED, PR_MONITOR_EMITTED,
    PR_MONITOR_REGISTERED,
};
use intent_core::{
    now_iso, parse_iso, AgentId, AgentStatus, Error, PrMonitor, PrMonitorId, PrMonitorState,
    PullRequestInfo, PullRequestStatus, Result, WorkspaceId,
};
use intent_sourcecontrol::{RepoRef, SourceControl};
use intent_store::{NewEvent, PrMonitorPollUpdate};
use serde_json::{json, Value};

use crate::pr_ops::{self, MergeRequirements};
use crate::workspace_status::MonitorPrSignals;
use crate::{publish_event, system_actor, Services};

use intent_core::config::{MIN_PR_MONITOR_DEBOUNCE_SECONDS, MIN_PR_MONITOR_POLL_SECONDS};

/// Cap on concurrently ACTIVE monitors per agent (mirrors the background-hook
/// `maxPerAgent` convention).
pub(crate) const DEFAULT_PR_MONITORS_MAX_PER_AGENT: u32 = 5;

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
/// window: populated by boot rehydration so a baseline that moved (or a
/// pending emit that was persisted but never delivered) while the daemon was
/// down fires immediately. Shared across [`Services`] clones; an entry is
/// consumed by the first poll that acts on it.
pub(crate) type PrMonitorCatchUp = Arc<Mutex<HashSet<PrMonitorId>>>;

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
}

impl SharedPrSnapshot {
    /// Materialize a per-monitor snapshot: a degraded conversation-comment
    /// read keeps the monitor's previous count rather than fabricating a
    /// "comments removed" change.
    pub(crate) fn materialize(&self, previous: Option<&PrMonitorSnapshot>) -> PrMonitorSnapshot {
        PrMonitorSnapshot {
            title: self.title.clone(),
            url: self.url.clone(),
            head_sha: self.head_sha.clone(),
            conversation_count: self
                .conversation_count
                .unwrap_or_else(|| previous.map(|p| p.conversation_count).unwrap_or(0)),
            review_comment_count: self.review_comment_count,
            requirements: self.requirements.clone(),
        }
    }
}

/// Fetch the current shared state of one PR: the merge-requirements
/// checklist (which already degrades per-signal) plus the
/// conversation-comment count (`None` when that read fails).
pub(crate) async fn fetch_shared_snapshot(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    number: u64,
) -> Result<SharedPrSnapshot> {
    let (pr, requirements, review_comment_count) =
        pr_ops::fetch_merge_requirements_detailed(sc, repo_ref, number).await?;
    let conversation_count = match sc.list_comments(repo_ref, number).await {
        Ok(comments) => Some(comments.len() as i64),
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
        review_comment_count,
        requirements,
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

/// Fold a workspace's monitor rows into the displayStatus PR signals
/// (§6.5): an ACTIVE row whose persisted `last_snapshot` shows the PR
/// open/draft raises `open` — and `ready` when the snapshot says mergeable
/// and not draft (the same mapping as a linked open PR) — while the LATEST
/// (most recently updated) COMPLETED row raises `merged` when its final
/// snapshot shows `merged` — matching linked-PR step-6 "latest" semantics,
/// so an older merged monitor never shadows a newer closed-unmerged one.
/// A row with no snapshot or an unparseable blob contributes nothing
/// (never fails the derivation), and cancelled rows are excluded by the
/// caller's SQL filter (which also bounds completed rows to the latest
/// one). An ACTIVE row already showing a terminal snapshot (a poll
/// observed the merge but lost its guarded terminalize write) contributes
/// nothing — the next tick re-detects and completes it.
pub(crate) fn fold_monitor_pr_signals(monitors: &[PrMonitor]) -> MonitorPrSignals {
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
                let req = &snapshot.requirements;
                if matches!(req.state.as_str(), "open" | "draft") {
                    signals.open = true;
                    if req.mergeable == Some(true) && !req.is_draft {
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

/// Synthesize a [`PullRequestInfo`] from a monitor row — the monitor-derived
/// entry the `workspace.list` / `workspace.subscribe` seq-0 PR merge appends
/// when no persisted source already carries the PR. Everything is read off
/// the persisted row (the snapshot column is parsed, never re-fetched).
/// Mirrors the FE's `mergeMonitoredPRs` fallbacks: URL/title synthesized from
/// the repo identity when the monitor has no snapshot yet, and status
/// resolved as snapshot state → draft flag → `completed` ⇒ closed (terminal
/// covers both merged and closed; don't falsely claim merged) → open.
pub(crate) fn pr_monitor_pr_info(m: &PrMonitor) -> PullRequestInfo {
    let snapshot: Option<PrMonitorSnapshot> = m
        .last_snapshot
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let status = match snapshot.as_ref().map(|s| s.requirements.state.as_str()) {
        Some("merged") => PullRequestStatus::Merged,
        Some("closed") => PullRequestStatus::Closed,
        Some("draft") => PullRequestStatus::Draft,
        _ if snapshot.as_ref().is_some_and(|s| s.requirements.is_draft) => PullRequestStatus::Draft,
        _ if m.state == PrMonitorState::Completed => PullRequestStatus::Closed,
        _ => PullRequestStatus::Open,
    };
    let url = snapshot.as_ref().map(|s| s.url.clone()).unwrap_or_else(|| {
        format!(
            "https://github.com/{}/{}/pull/{}",
            m.repo_owner, m.repo_name, m.pr_number
        )
    });
    let title = snapshot
        .as_ref()
        .map(|s| s.title.clone())
        .unwrap_or_else(|| format!("{}/{}#{}", m.repo_owner, m.repo_name, m.pr_number));
    PullRequestInfo {
        id: m.pr_number.to_string(),
        number: m.pr_number as u64,
        url,
        title,
        status,
        // Monitor-row timestamps stand in for the PR's own (the snapshot
        // does not carry them), mirroring the FE merge.
        created_at: m.created_at.clone(),
        updated_at: m.updated_at.clone(),
        base_ref: None,
        head_ref: None,
        head_sha: snapshot.as_ref().and_then(|s| s.head_sha.clone()),
        author: None,
        mergeable: snapshot.as_ref().and_then(|s| s.requirements.mergeable),
        mergeable_state: None,
        is_draft: snapshot.as_ref().map(|s| s.requirements.is_draft),
    }
}

/// The `messageMetadata` payload attached to every PR-monitor wake delivery
/// (PROTOCOL §5.42): `{ type: "pr_monitor_wake", monitorId, repo, prNumber,
/// reason, url? }`. `url` is the PR's HTML URL read off the monitor's
/// persisted baseline snapshot; the key is OMITTED (never null) when the
/// monitor has no baseline yet.
fn pr_monitor_wake_metadata(m: &PrMonitor, reason: &str) -> Value {
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
    metadata
}

/// The list-surface projection of one monitor: identity + lifecycle plus the
/// hover/click fields the FE needs — PR title/URL and a compact summary of
/// the last-refresh snapshot, whether changes are accumulated awaiting the
/// debounce emit, and when the last change landed. Everything is read off the
/// persisted row (the baseline snapshot column is parsed, never re-fetched),
/// so a list stays O(rows returned).
fn pr_monitor_wire(m: &PrMonitor) -> Value {
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
    if let Some(s) = snapshot {
        let r = &s.requirements;
        obj.insert("title".to_string(), Value::String(s.title.clone()));
        obj.insert("url".to_string(), Value::String(s.url.clone()));
        obj.insert(
            "lastSnapshot".to_string(),
            json!({
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
                    "unresolved": r.threads.unresolved,
                    "resolutionRequired": r.threads.resolution_required,
                },
                "rulesKnown": r.rules_known,
            }),
        );
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
    /// The effective poll cadence for the centralized monitor loop
    /// (`prMonitor.pollSeconds`), clamped to [`MIN_PR_MONITOR_POLL_SECONDS`].
    /// Read live from the settings registry on every tick so a config change
    /// applies without a restart; an explicit override wins when wired.
    pub(crate) fn pr_monitor_poll_interval(&self) -> Duration {
        let secs = self
            .pr_monitor_poll_seconds
            .unwrap_or_else(|| self.effective_settings().pr_monitor.poll_seconds)
            .max(MIN_PR_MONITOR_POLL_SECONDS);
        Duration::from_secs(secs)
    }

    /// The effective debounce quiet window (`prMonitor.debounceSeconds`),
    /// clamped to [`MIN_PR_MONITOR_DEBOUNCE_SECONDS`]. Read live from the
    /// settings registry per evaluation so a config change applies to the
    /// next window; an explicit override wins when wired.
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
    /// The initial fetch is load-bearing — a forge that cannot read the PR
    /// (unsupported host, missing PR, no token) fails registration rather
    /// than persisting a monitor that could never poll.
    pub async fn pr_monitor_register(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: u64,
    ) -> Result<(PrMonitor, MergeRequirements)> {
        let existing = self
            .store
            .find_active_pr_monitor(agent_id, repo_owner, repo_name, pr_number as i64)
            .await?;
        if existing.is_none() {
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
        let snapshot = fetch_snapshot(sc.as_ref(), &repo_ref, pr_number, None).await?;
        let baseline = serde_json::to_string(&snapshot).ok();
        let now = now_iso();

        let mut monitor = match existing {
            Some(m) => self.rearm_pr_monitor(m, baseline.clone(), &now).await?,
            None => None,
        };
        if monitor.is_none() {
            let m = PrMonitor {
                monitor_id: PrMonitorId::new(),
                workspace_id: workspace_id.clone(),
                agent_id: agent_id.clone(),
                repo_owner: repo_owner.to_string(),
                repo_name: repo_name.to_string(),
                pr_number: pr_number as i64,
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
                .find_active_pr_monitor(agent_id, repo_owner, repo_name, pr_number as i64)
                .await?
            {
                // Lost an insert race against a concurrent register of the
                // same triple: re-arm the winner's row instead of surfacing
                // the unique-index violation (the call stays idempotent).
                monitor = self.rearm_pr_monitor(winner, baseline, &now).await?;
            }
        }
        let monitor = monitor.ok_or_else(|| {
            Error::Internal(
                "pr.monitor: registration raced a concurrent monitor mutation; retry".to_string(),
            )
        })?;
        self.emit_pr_monitor_event(PR_MONITOR_REGISTERED, &monitor, None)
            .await;
        // A newly persisted active monitor on an open PR can move the
        // derived displayStatus to `pr_open`/`pr_ready` (§6.5) and raise
        // the orthogonal `waiting` flag (§5.1).
        self.maybe_emit_display_status_changed(workspace_id).await;
        self.maybe_emit_waiting_changed(workspace_id).await;
        Ok((monitor, snapshot.requirements))
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
    /// is never wedged and PR stages are never fabricated.
    pub(crate) async fn workspace_monitor_pr_signals(
        &self,
        workspace_id: &WorkspaceId,
    ) -> MonitorPrSignals {
        match self
            .store
            .list_display_status_pr_monitors_by_workspace(workspace_id)
            .await
        {
            Ok(monitors) => fold_monitor_pr_signals(&monitors),
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
        let repo_ref = RepoRef::new(&monitor.repo_owner, &monitor.repo_name);
        let shared =
            match fetch_shared_snapshot(sc.as_ref(), &repo_ref, monitor.pr_number as u64).await {
                Ok(shared) => shared,
                Err(e) => {
                    self.record_pr_monitor_error(&monitor, &e.to_string()).await;
                    return Err(e);
                }
            };
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
    /// (re-read each tick), poll every DUE active monitor. Returns the task
    /// handle so the composition root can hold/abort it.
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
    /// monitors whose `lastPolledAt` is fresher than the poll interval —
    /// typically a monitor that was just registered or re-registered, whose
    /// registration fetch already stamped a current baseline. Catch-up-marked
    /// monitors (boot rehydration) are never skipped.
    ///
    /// `pub` for the same reason as [`Self::poll_pr_monitors`]: integration
    /// tests drive one deterministic due-sweep instead of racing the loop's
    /// timer.
    pub async fn poll_due_pr_monitors(&self) {
        self.sweep_pr_monitors(true).await;
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
    async fn sweep_pr_monitors(&self, skip_fresh: bool) {
        let monitors = match self.store.load_active_pr_monitors().await {
            Ok(monitors) => monitors,
            Err(e) => {
                tracing::warn!(error = %e, "pr monitor sweep: load failed; skipping tick");
                return;
            }
        };
        if monitors.is_empty() {
            return;
        }
        let sc = match pr_ops::resolve_source_control(self.source_control.clone()).await {
            Ok(sc) => sc,
            Err(e) => {
                tracing::debug!(error = %e, "pr monitor sweep: no source control; skipping tick");
                return;
            }
        };
        let mut shared: HashMap<
            (String, String, i64),
            std::result::Result<SharedPrSnapshot, String>,
        > = HashMap::new();
        for monitor in monitors {
            if skip_fresh && self.pr_monitor_recently_polled(&monitor) {
                continue;
            }
            let key = (
                monitor.repo_owner.clone(),
                monitor.repo_name.clone(),
                monitor.pr_number,
            );
            let fetched = match shared.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let repo_ref = RepoRef::new(&monitor.repo_owner, &monitor.repo_name);
                    // The timeout is defense in depth above the client-level
                    // network timeouts: a fetch that pends indefinitely maps
                    // to an error (recorded as `lastError` below) instead of
                    // wedging the sweep for every other monitor.
                    let fetched = match tokio::time::timeout(
                        self.pr_monitor_fetch_timeout,
                        fetch_shared_snapshot(sc.as_ref(), &repo_ref, monitor.pr_number as u64),
                    )
                    .await
                    {
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

    /// Whether a monitor was polled recently enough for the loop-driven
    /// sweep to skip it this tick: `lastPolledAt` is fresher than the poll
    /// interval (registration and re-registration fetch their own baseline
    /// and stamp the field). Catch-up-marked monitors are never fresh —
    /// their first post-restart poll must deliver promptly.
    fn pr_monitor_recently_polled(&self, monitor: &PrMonitor) -> bool {
        if self
            .pr_monitor_catch_up
            .lock()
            .unwrap()
            .contains(&monitor.monitor_id)
        {
            return false;
        }
        let Some(at) = monitor.last_polled_at.as_deref().and_then(parse_iso) else {
            return false;
        };
        let interval = time::Duration::seconds(self.pr_monitor_poll_interval().as_secs() as i64);
        time::OffsetDateTime::now_utc() - at < interval
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
        let previous: Option<PrMonitorSnapshot> = monitor
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let fresh = shared.materialize(previous.as_ref());

        // Per-poll activity (fresh vs the LAST POLL's snapshot) anchors the
        // debounce quiet-window; the pending set below is computed against
        // the EMIT baseline instead, so the two diffs serve distinct roles.
        let poll_activity = previous
            .as_ref()
            .map(|prev| !diff_snapshots(prev, &fresh).is_empty())
            .unwrap_or(false);

        // The emit baseline: the PR state as of the last delivered wake (or
        // registration). A row missing one (unparseable column) anchors on
        // the last poll's snapshot; a row with neither adopts the fresh
        // snapshot below, with nothing pending.
        let baseline: Option<PrMonitorSnapshot> = monitor
            .baseline_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .or(previous);

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
            .contains(&monitor.monitor_id);

        let now = now_iso();
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
        updated.last_error = None;
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
        let window = time::Duration::seconds(self.pr_monitor_debounce().as_secs() as i64);
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
    /// delivered — `false` when a lost guarded write (a concurrent
    /// flush/cancel/re-register moved the row) skipped it.
    async fn complete_pr_monitor(
        &self,
        monitor: &PrMonitor,
        snapshot: &PrMonitorSnapshot,
    ) -> Result<bool> {
        let changes = monitor
            .baseline_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str::<PrMonitorSnapshot>(s).ok())
            .map(|base| diff_snapshots(&base, snapshot))
            .unwrap_or_else(|| monitor.pending_changes.clone());
        let message = render_terminal_wake(monitor, &changes, snapshot);
        let now = now_iso();
        if !self
            .store
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: monitor.last_snapshot.as_deref(),
                    baseline_snapshot: monitor.baseline_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: monitor.last_polled_at.as_deref(),
                    updated_at: &now,
                    expected_updated_at: &monitor.updated_at,
                    ..Default::default()
                },
            )
            .await?
        {
            // The row moved under us (concurrent flush/cancel/re-register);
            // skip the wake — the next tick re-detects the terminal state.
            return Ok(false);
        }
        if !self
            .store
            .update_pr_monitor_state(&monitor.monitor_id, PrMonitorState::Completed, &now)
            .await?
        {
            // A concurrent cancel won; it already delivered its own notice.
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
    /// pending state. Best-effort — a store failure on the error path is
    /// logged, never propagated.
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
    pub async fn rehydrate_pr_monitors(&self) -> Result<usize> {
        let monitors = self.store.load_active_pr_monitors().await?;
        let mut resumed = 0;
        for mut monitor in monitors {
            let owner_gone = match self.store.get_agent_session_status(&monitor.agent_id).await {
                Ok(AgentStatus::Deleted) => true,
                Ok(_) => false,
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
                .insert(monitor.monitor_id.clone());
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
        let metadata = pr_monitor_wake_metadata(monitor, reason);
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

    /// Resolve the `(owner, name)` a monitor call targets: an explicit
    /// `"owner/name"` override wins, otherwise the workspace's own repo.
    async fn resolve_monitor_repo(
        &self,
        workspace_id: &WorkspaceId,
        repo: Option<String>,
    ) -> Result<(String, String)> {
        match repo {
            Some(slug) => pr_ops::parse_repo_slug(&slug),
            None => {
                let ws = self.store.get_workspace(workspace_id).await?;
                pr_ops::repo_of(&ws)
            }
        }
    }

    /// `ws.pr.monitor`: register (idempotently) a monitor and return
    /// `{ ok, monitor, requirements }` — the row the UI lists plus the
    /// freshly fetched merge-requirements checklist the model acts on.
    pub(crate) async fn pr_monitor_start_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        pr_number: u64,
        repo: Option<String>,
    ) -> Result<Value> {
        let (owner, name) = self.resolve_monitor_repo(workspace_id, repo).await?;
        let (monitor, requirements) = self
            .pr_monitor_register(workspace_id, agent_id, &owner, &name, pr_number)
            .await?;
        Ok(json!({
            "ok": true,
            "monitor": pr_monitor_wire(&monitor),
            "requirements": requirements,
        }))
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
            .find_active_pr_monitor(agent_id, &owner, &name, pr_number as i64)
            .await?
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "pr.unmonitor: no active monitor on {owner}/{name}#{pr_number}"
                ))
            })?;
        let monitor = self
            .pr_monitor_cancel(workspace_id, &existing.monitor_id, Some(agent_id))
            .await?;
        Ok(json!({ "ok": true, "monitor": pr_monitor_wire(&monitor) }))
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
        let monitors: Vec<Value> = monitors
            .into_iter()
            .filter(|m| &m.workspace_id == workspace_id)
            .map(|m| pr_monitor_wire(&m))
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
        Ok(json!({ "ok": true, "monitor": pr_monitor_wire(&monitor) }))
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
        publish_event(&self.event_bus, event).await;
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
        NewPullRequest, Page, PageParams, PrPatch, PrQuery, PrState, PullRequest, Repo, Review,
        ReviewComment, ReviewDecision, ReviewThread, ReviewVerdict, RollupCheck, ScCapabilities,
        UserIdentity,
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
        fail_get_pr: bool,
        fail_list_comments: bool,
        /// PR number whose `get_pr` pends forever (hung-connection regression).
        hang_get_pr: Option<u64>,
    }

    impl Default for ForgeState {
        fn default() -> Self {
            Self {
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
                    state: CheckState::Pending,
                    is_required: true,
                    url: None,
                }],
                fail_get_pr: false,
                fail_list_comments: false,
                hang_get_pr: None,
            }
        }
    }

    #[derive(Clone)]
    struct StubForge {
        state: Arc<Mutex<ForgeState>>,
        get_pr_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StubForge {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(ForgeState::default())),
                get_pr_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn edit(&self, f: impl FnOnce(&mut ForgeState)) {
            f(&mut self.state.lock().unwrap());
        }

        /// Snapshot-fetch attempts so far: `get_pr` is called exactly once
        /// per [`fetch_shared_snapshot`] attempt (successful or not).
        fn fetches(&self) -> usize {
            self.get_pr_calls.load(std::sync::atomic::Ordering::SeqCst)
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
        async fn get_pr(
            &self,
            _: &RepoRef,
            number: u64,
        ) -> intent_sourcecontrol::Result<PullRequest> {
            self.get_pr_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
            Ok(PullRequest {
                number,
                url: format!("https://github.com/o/r/pull/{number}"),
                title: "Add thing".into(),
                body: None,
                state: s.pr_state,
                draft: s.draft,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                author: "octocat".into(),
                mergeable: s.mergeable,
                mergeable_state: Some(s.mergeable_state.clone()),
                head_sha: Some(s.head_sha.clone()),
                created_at: String::new(),
                updated_at: String::new(),
            })
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
            Ok(self
                .state
                .lock()
                .unwrap()
                .approvals
                .iter()
                .map(|a| Review {
                    author: a.clone(),
                    verdict: ReviewVerdict::Approve,
                    body: None,
                    submitted_at: "2026-01-01T00:00:00Z".into(),
                })
                .collect())
        }
        async fn merge_requirements(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<MergeRequirementSignals> {
            let s = self.state.lock().unwrap().clone();
            Ok(MergeRequirementSignals {
                merge_state_status: Some(s.mergeable_state.to_uppercase()),
                review_decision: (!s.approvals.is_empty()).then_some(ReviewDecision::Approved),
                checks: s.checks.clone(),
                checks_known: true,
                branch_rules: Some(BranchRules {
                    required_approving_review_count: Some(1),
                    required_conversation_resolution: Some(true),
                    required_status_checks: vec!["build".into()],
                }),
            })
        }
        async fn list_comments(
            &self,
            _: &RepoRef,
            _: u64,
        ) -> intent_sourcecontrol::Result<Vec<Comment>> {
            let (n, fail) = {
                let s = self.state.lock().unwrap();
                (s.conversation_comments, s.fail_list_comments)
            };
            if fail {
                return Err(intent_sourcecontrol::Error::Unsupported(
                    "comments down".into(),
                ));
            }
            Ok((0..n)
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
        async fn list_review_comments(
            &self,
            _: &RepoRef,
            _: u64,
            _: PageParams,
        ) -> intent_sourcecontrol::Result<Page<ReviewComment>> {
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
            Ok(Page {
                items: self.state.lock().unwrap().threads.clone(),
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
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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
                    unresolved: 1,
                    resolution_required: Some(true),
                },
                merge_state_status: Some("BLOCKED".into()),
                merge_blocked_reason: None,
                rules_known: true,
            },
        };
        f(&mut s);
        s
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

        let resolved = snapshot(|s| s.requirements.threads.unresolved = 0);
        assert!(diff_snapshots(&base, &resolved)
            .iter()
            .any(|c| c.starts_with("thread(s) resolved")));

        let unresolved = snapshot(|s| s.requirements.threads.unresolved = 2);
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

        let metadata = pr_monitor_wake_metadata(&m, "changed");
        assert_eq!(metadata["type"], json!("pr_monitor_wake"));
        assert_eq!(metadata["repo"], json!("o/r"));
        assert_eq!(metadata["prNumber"], json!(42));
        assert_eq!(metadata["reason"], json!("changed"));
        assert_eq!(metadata["url"], json!("https://github.com/o/r/pull/42"));

        // No baseline yet: the key is ABSENT, never null.
        m.last_snapshot = None;
        let metadata = pr_monitor_wake_metadata(&m, "cancelled");
        assert!(metadata.get("url").is_none(), "{metadata}");
        assert_eq!(metadata["reason"], json!("cancelled"));
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

    /// Insert a second agent in the fixture workspace so a second monitor
    /// can watch the SAME PR (a monitor is unique per (agent, repo, pr)).
    async fn second_agent(svc: &Services, ws: &WorkspaceId, id: &str) -> AgentId {
        svc.store()
            .insert_agent_session(&agent(ws, id))
            .await
            .expect("second agent");
        AgentId::from(id)
    }

    #[tokio::test]
    async fn sweep_fetches_each_pr_once_across_sibling_monitors() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
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

    #[tokio::test]
    async fn sweep_dedupes_failed_fetches_and_records_the_error_on_every_sibling() {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let first = register(&svc, &ws, &owner).await;
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
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
        let sibling = second_agent(&svc, &ws, "agent-sibling").await;
        let second = svc
            .pr_monitor_register(&ws, &sibling, "o", "r", 42)
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
    /// rollup reads `pr_ready` (the stub PR is open + mergeable, not
    /// draft) instead of falling through to `complete`. Cancelling the
    /// monitor lapses both — the flag drops and the rollup returns to the
    /// base `complete`.
    #[tokio::test]
    async fn active_pr_monitor_sets_waiting_and_feeds_the_pr_rungs() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        svc.store()
            .insert_note(&task_note(&ws, "t1", intent_core::TaskStatus::Complete))
            .await
            .expect("insert task");
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

    /// The snapshot→signal fold: active open/draft rows raise `open` (and
    /// `ready` only when mergeable + not draft), completed merged rows
    /// raise `merged`, and rows with no/unparseable snapshots, non-merged
    /// completed rows, or active rows already showing a terminal snapshot
    /// contribute nothing.
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

        // Active + open + mergeable + not draft → open and ready.
        let ready = mk(PrMonitorState::Active, snap(|_| {}));
        assert_eq!(
            fold_monitor_pr_signals(std::slice::from_ref(&ready)),
            MonitorPrSignals {
                open: true,
                ready: true,
                merged: false
            }
        );
        // Draft, or not mergeable → open only.
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
        for m in [&draft, &unmergeable] {
            assert_eq!(
                fold_monitor_pr_signals(std::slice::from_ref(m)),
                MonitorPrSignals {
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
            fold_monitor_pr_signals(std::slice::from_ref(&merged)),
            MonitorPrSignals {
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
            fold_monitor_pr_signals(&[closed, active_terminal, no_snapshot, bad_blob]),
            MonitorPrSignals::default()
        );
        // Signals aggregate across rows.
        assert_eq!(
            fold_monitor_pr_signals(&[ready, merged.clone()]),
            MonitorPrSignals {
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
            fold_monitor_pr_signals(&[merged.clone(), newer_closed.clone()]),
            MonitorPrSignals::default(),
            "newer closed-unmerged monitor wins over an older merged one"
        );
        // Order-independent: the fold picks the latest by updated_at, not
        // by slice position.
        assert_eq!(
            fold_monitor_pr_signals(&[newer_closed, merged.clone()]),
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
            fold_monitor_pr_signals(&[older_closed, newer_merged]),
            MonitorPrSignals {
                open: false,
                ready: false,
                merged: true
            }
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
            mergeable_state: None,
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
    /// through the PR rungs (§6.5): registering on an open mergeable PR
    /// emits the `pr_ready` promotion, a no-op recompute stays silent, and
    /// cancelling emits the demotion back to the base rollup (`idle` here:
    /// no tasks, no linked PR).
    #[tokio::test]
    async fn monitor_transitions_emit_display_status_through_the_pr_rungs() {
        let (_db, _root, svc, _forge, ws, owner) = setup().await;
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_display_status_changed(&ws).await;
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        let monitor = register(&svc, &ws, &owner).await;
        assert!(svc.workspace_is_waiting(&ws).await);
        assert_eq!(
            display_status_events(&svc, &ws).await,
            vec!["pr_ready".to_string()],
            "an active monitor on an open mergeable PR promotes to pr_ready"
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
}
