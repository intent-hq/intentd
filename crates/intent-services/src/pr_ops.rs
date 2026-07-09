//! Wire-policy glue for the read-only `pr.*` methods (§5.7, §7.5).
//!
//! Pure mapping/aggregation ported from the TS ground truth (`ws-pr-api.ts`,
//! `github.service.ts`): the `pr.status` summary, the `pr.getReviews` decision
//! aggregation, the `pr.listCheckRuns` tally, and the `pr.listReviewComments`
//! thread shaping. The forge calls themselves go through the host-agnostic
//! [`SourceControl`] trait; this module owns only the parity-critical glue so it
//! stays unit-testable without a network.

use std::collections::HashMap;
use std::sync::Arc;

use intent_core::{Error, PullRequestInfo, PullRequestStatus, Result, Workspace};
use intent_sourcecontrol::{
    CheckRun, CheckState, MergeMethod, PrState, PullRequest, Review, ReviewComment, ReviewThread,
    ReviewThreadComment, ReviewVerdict, SourceControl, SourceControlRegistry,
    SourceControlSettings,
};
use serde_json::{json, Value};

/// TS `NO_ACTIVE_PR_ERROR`; every `pr.*` method needs an active PR (§5.7).
pub(crate) const NO_ACTIVE_PR: &str = "No active PR";

/// Map a forge error onto the domain `Internal` error (→ `-32603`): the TS
/// `pr.*` handlers wrap every underlying throw in `INTERNAL_ERROR` (§5.7), and
/// the graceful "not configured" path (§8.3) surfaces the same way.
pub(crate) fn map_sc_err(e: intent_sourcecontrol::Error) -> Error {
    Error::Internal(e.to_string())
}

/// Resolve the active [`SourceControl`]: the injected handle (tests / explicit
/// wiring) else the registry-built provider from default settings (token from
/// env / `gh` / keychain, §7.3). A missing token yields `Internal` (graceful).
/// Async because the keychain / `gh` lookups run on the blocking pool with
/// bounded timeouts so a wedged OS keychain or hung child never blocks the
/// async runtime.
pub(crate) async fn resolve_source_control(
    injected: Option<Arc<dyn SourceControl>>,
) -> Result<Arc<dyn SourceControl>> {
    match injected {
        Some(sc) => Ok(sc),
        None => SourceControlRegistry::from_settings(&SourceControlSettings::default())
            .await
            .map_err(map_sc_err),
    }
}

/// The `(owner, repo)` pair for the workspace's active provider, or
/// [`NO_ACTIVE_PR`] when either is unset (§7.6).
pub(crate) fn repo_of(ws: &Workspace) -> Result<(String, String)> {
    match (
        ws.repository_owner.as_deref().filter(|s| !s.is_empty()),
        ws.repository_name.as_deref().filter(|s| !s.is_empty()),
    ) {
        (Some(owner), Some(name)) => Ok((owner.to_string(), name.to_string())),
        _ => Err(Error::Internal(NO_ACTIVE_PR.to_string())),
    }
}

/// The workspace's active PR number, or [`NO_ACTIVE_PR`] when unlinked.
pub(crate) fn active_pr_number(ws: &Workspace) -> Result<u64> {
    ws.pr_number
        .ok_or_else(|| Error::Internal(NO_ACTIVE_PR.to_string()))
}

// ===========================================================================
// Workspace ↔ PR linkage (§7.6). The matching rule ports from the TS
// `Workspace` type: a PR belongs to a workspace when its head ref equals the
// workspace's OWN branch (`pr.head.ref === workspace.branch`), NOT `baseRef`.
//
// PARITY NOTE: the TS `performBackgroundEnrichment` additionally accepts a
// `baseRef` match (`matchesBaseRef`); IMPLEMENTATION_SPEC §626 mandates
// branch-only matching ("the workspace's own branch, NOT baseRef"), so this
// port follows the SPEC and ignores `baseRef`.
// ===========================================================================

/// Outcome of a single workspace PR refresh (§7.6). Drives which `pr:*` event
/// (if any) the caller emitted; `Skipped`/`Unchanged` emit nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrRefreshOutcome {
    /// Not eligible (remote/archived workspace, no repo, or no branch).
    Skipped,
    /// Eligible but nothing changed (linked PR snapshot identical; or no
    /// matching PR found during discovery).
    Unchanged,
    /// A previously unlinked workspace gained a PR link (`pr:linked`).
    Linked,
    /// A linked PR's persisted snapshot changed (`pr:updated`).
    Updated,
    /// A stale link was cleared after a positive branch mismatch (`pr:unlinked`).
    Unlinked,
}

/// True when `pr.head.ref` (the host-agnostic `source_branch`) equals the
/// workspace's own `branch`, and both are non-empty — the link/discovery rule.
pub(crate) fn pr_matches_branch(pr: &PullRequest, branch: &str) -> bool {
    !pr.source_branch.is_empty() && !branch.is_empty() && pr.source_branch == branch
}

/// True only on a POSITIVE mismatch: both branches are known (non-empty) and
/// differ. An empty `source_branch` (e.g. a forge that omits it) is treated as
/// "cannot determine" and never clears a link, mirroring the TS guard that only
/// clears a stale link when the source branch is present and differs.
pub(crate) fn pr_branch_mismatch(pr: &PullRequest, branch: &str) -> bool {
    !pr.source_branch.is_empty() && !branch.is_empty() && pr.source_branch != branch
}

/// Derive the persisted [`PullRequestStatus`] from a forge PR (draft wins over
/// open; merged/closed map directly), mirroring [`derive_status_state`].
pub(crate) fn derive_pr_status(pr: &PullRequest) -> PullRequestStatus {
    match pr.state {
        PrState::Merged => PullRequestStatus::Merged,
        PrState::Closed => PullRequestStatus::Closed,
        PrState::Open if pr.draft => PullRequestStatus::Draft,
        PrState::Open => PullRequestStatus::Open,
    }
}

/// Build the persisted [`PullRequestInfo`] snapshot from a forge PR (§7.6).
/// Empty strings on optional fields collapse to `None` so absent values are
/// omitted from the wire, matching the TS `PullRequestInfo` JSON shape.
pub(crate) fn build_pr_info(pr: &PullRequest) -> PullRequestInfo {
    let non_empty = |s: &str| (!s.is_empty()).then(|| s.to_string());
    PullRequestInfo {
        id: pr.number.to_string(),
        number: pr.number,
        url: pr.url.clone(),
        title: pr.title.clone(),
        status: derive_pr_status(pr),
        created_at: pr.created_at.clone(),
        updated_at: pr.updated_at.clone(),
        base_ref: non_empty(&pr.target_branch),
        head_ref: non_empty(&pr.source_branch),
        head_sha: pr.head_sha.clone().filter(|s| !s.is_empty()),
        author: non_empty(&pr.author),
        mergeable: pr.mergeable,
        mergeable_state: pr.mergeable_state.clone(),
        is_draft: Some(pr.draft),
    }
}

/// The 4-value wire state for `pr.status` (TS `getPullRequest` derivation:
/// merged wins, then draft, then open/closed).
pub(crate) fn derive_status_state(pr: &PullRequest) -> &'static str {
    if pr.state == PrState::Merged {
        "merged"
    } else if pr.draft {
        "draft"
    } else if pr.state == PrState::Closed {
        "closed"
    } else {
        "open"
    }
}

/// Port of `buildStatusSummary` (`ws-pr-api.ts`): a human-readable one-liner
/// from the derived `state`, `mergeable`, and raw `mergeable_state`.
pub(crate) fn build_status_summary(
    state: &str,
    mergeable: Option<bool>,
    mergeable_state: &str,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    let has_conflicts = mergeable_state == "dirty";
    if state == "merged" {
        parts.push("✅ PR is merged.".to_string());
    } else if state == "closed" {
        parts.push("🚫 PR is closed.".to_string());
    } else {
        if state == "draft" {
            parts.push("📝 PR is a draft.".to_string());
        }
        if has_conflicts {
            parts.push("⚠️ PR has merge conflicts that need to be resolved.".to_string());
        } else if mergeable == Some(true) && mergeable_state == "clean" {
            parts.push("✅ PR is mergeable with no conflicts.".to_string());
        } else if mergeable_state == "unknown" || mergeable.is_none() {
            parts.push("⏳ GitHub is still computing mergeability.".to_string());
        }
        if mergeable_state == "blocked" {
            parts.push(
                "🔒 PR is blocked (e.g., required reviews or branch protection rules not met)."
                    .to_string(),
            );
        } else if mergeable_state == "unstable" {
            parts.push("⚠️ PR has failing status checks.".to_string());
        } else if mergeable_state == "behind" {
            parts.push(
                "🔄 PR branch is behind the target branch and needs to be updated.".to_string(),
            );
        }
    }
    if parts.is_empty() {
        parts.push(format!(
            "PR is in state: {state}, mergeableState: {mergeable_state}."
        ));
    }
    parts.join(" ")
}

/// Aggregated review decision for `pr.getReviews`.
pub(crate) struct ReviewAggregate {
    pub review_decision: Option<String>,
    pub approval_count: i64,
    pub changes_requested_count: i64,
    pub approved_by: Vec<String>,
}

/// `pr.listCheckRuns` tally.
pub(crate) struct CheckRunSummary {
    pub total: i64,
    pub passed: i64,
    pub failed: i64,
    pub pending: i64,
}

/// Port of `github.service.getReviews`: keep the latest *actionable* review per
/// author (approve / request-changes), then derive the decision.
///
/// PARITY NOTE: GitHub's `DISMISSED` state collapses to the host-agnostic
/// `comment` verdict (§5.18 has only three verdicts), so a dismissal here is
/// treated as non-actionable rather than clearing a prior actionable review.
pub(crate) fn aggregate_reviews(reviews: &[Review]) -> ReviewAggregate {
    let mut order: Vec<String> = Vec::new();
    let mut latest: HashMap<String, (ReviewVerdict, String)> = HashMap::new();
    for r in reviews {
        if !matches!(
            r.verdict,
            ReviewVerdict::Approve | ReviewVerdict::RequestChanges
        ) {
            continue;
        }
        match latest.get(&r.author) {
            Some((_, prev_at)) if r.submitted_at <= *prev_at => {}
            Some(_) => {
                latest.insert(r.author.clone(), (r.verdict, r.submitted_at.clone()));
            }
            None => {
                order.push(r.author.clone());
                latest.insert(r.author.clone(), (r.verdict, r.submitted_at.clone()));
            }
        }
    }
    let mut approval_count = 0;
    let mut changes_requested_count = 0;
    let mut approved_by = Vec::new();
    for login in &order {
        match latest.get(login) {
            Some((ReviewVerdict::Approve, _)) => {
                approval_count += 1;
                approved_by.push(login.clone());
            }
            Some((ReviewVerdict::RequestChanges, _)) => changes_requested_count += 1,
            _ => {}
        }
    }
    let review_decision = if changes_requested_count > 0 {
        Some("CHANGES_REQUESTED".to_string())
    } else if approval_count > 0 {
        Some("APPROVED".to_string())
    } else {
        None
    };
    ReviewAggregate {
        review_decision,
        approval_count,
        changes_requested_count,
        approved_by,
    }
}

/// Port of `github.service.getCheckRuns` counting: pending when not completed,
/// passed for success/neutral, failed otherwise.
pub(crate) fn summarize_check_runs(runs: &[CheckRun]) -> CheckRunSummary {
    let mut summary = CheckRunSummary {
        total: runs.len() as i64,
        passed: 0,
        failed: 0,
        pending: 0,
    };
    for r in runs {
        match r.state {
            CheckState::Pending => summary.pending += 1,
            CheckState::Success | CheckState::Neutral => summary.passed += 1,
            CheckState::Failure | CheckState::Cancelled => summary.failed += 1,
        }
    }
    summary
}

/// Validate `pr.listReviewComments` `status` (default `unresolved`); an invalid
/// value throws in the TS builder → `-32603` here.
pub(crate) fn validate_review_comment_status(status: Option<String>) -> Result<String> {
    match status.as_deref() {
        None => Ok("unresolved".to_string()),
        Some(s @ ("unresolved" | "resolved" | "all")) => Ok(s.to_string()),
        Some(_) => Err(Error::Internal(
            "status must be one of: unresolved, resolved, all".to_string(),
        )),
    }
}

/// Clamp the `pr.listComments` `count` to `[1, 100]` (default 20), mirroring TS.
pub(crate) fn clamp_count(count: Option<i64>) -> usize {
    count.unwrap_or(20).clamp(1, 100) as usize
}

/// Render review threads to the wire shape (`author` nested as `{ login }`).
pub(crate) fn thread_list_json(threads: &[ReviewThread]) -> Vec<Value> {
    threads
        .iter()
        .map(|t| {
            json!({
                "id": t.id,
                "isResolved": t.is_resolved,
                "comments": t.comments.iter().map(|c| json!({
                    "id": c.id,
                    "body": c.body,
                    "author": { "login": c.author },
                    "path": c.path,
                    "line": c.line,
                    "createdAt": c.created_at,
                })).collect::<Vec<_>>(),
            })
        })
        .collect()
}

/// Retain only threads with a comment on `path` (the `pr.listReviewComments`
/// path filter).
pub(crate) fn retain_path(threads: &mut Vec<ReviewThread>, path: &str) {
    threads.retain(|t| t.comments.iter().any(|c| c.path == path));
}

/// Group flat REST review comments into synthetic threads via `in_reply_to_id`
/// (the GraphQL-unavailable fallback in `ws-pr-api.ts`).
pub(crate) fn fallback_threads(mut comments: Vec<ReviewComment>) -> Vec<ReviewThread> {
    comments.sort_by_key(|c| c.id);
    let mut order: Vec<u64> = Vec::new();
    let mut map: HashMap<u64, ReviewThread> = HashMap::new();
    let mut reply_root: HashMap<u64, u64> = HashMap::new();
    for c in comments {
        let root = if let Some(parent) = c.in_reply_to_id {
            let candidate = *reply_root.get(&parent).unwrap_or(&parent);
            if map.contains_key(&candidate) {
                candidate
            } else {
                c.id
            }
        } else {
            c.id
        };
        reply_root.insert(c.id, root);
        let tc = ReviewThreadComment {
            id: c.id.to_string(),
            body: c.body,
            author: c.author,
            path: c.path,
            line: c.line,
            created_at: c.created_at,
        };
        match map.get_mut(&root) {
            Some(thread) => thread.comments.push(tc),
            None => {
                order.push(root);
                map.insert(
                    root,
                    ReviewThread {
                        id: format!("rest-thread-{root}"),
                        is_resolved: false,
                        comments: vec![tc],
                    },
                );
            }
        }
    }
    order.into_iter().filter_map(|id| map.remove(&id)).collect()
}

// ===========================================================================
// `pr.*` write/action glue (PROTOCOL §5.7, IMPLEMENTATION_SPEC §7.5).
// ===========================================================================

/// `pr.waitForChanges` safety padding (TS `SAFETY_PADDING_SECONDS`).
pub(crate) const SAFETY_PADDING_SECONDS: u64 = 10;

/// Validate/default the `pr.merge` `mergeMethod` (TS `validateMergeMethod`,
/// default `merge`); an invalid value throws → `-32603`.
pub(crate) fn validate_merge_method(method: Option<String>) -> Result<MergeMethod> {
    match method.as_deref() {
        None | Some("merge") => Ok(MergeMethod::Merge),
        Some("squash") => Ok(MergeMethod::Squash),
        Some("rebase") => Ok(MergeMethod::Rebase),
        Some(_) => Err(Error::Internal(
            "mergeMethod must be one of: merge, squash, rebase".to_string(),
        )),
    }
}

/// Wire word for a [`MergeMethod`] (echoed back in the `pr.merge` result).
pub(crate) fn merge_method_word(method: MergeMethod) -> &'static str {
    match method {
        MergeMethod::Merge => "merge",
        MergeMethod::Squash => "squash",
        MergeMethod::Rebase => "rebase",
    }
}

/// Validate/default the `pr.waitForChanges` `watch` mode (TS
/// `validateWatchMode`, default `any`).
pub(crate) fn validate_watch_mode(watch: Option<String>) -> Result<String> {
    match watch.as_deref() {
        None => Ok("any".to_string()),
        Some(s @ ("any" | "checks" | "state" | "commits")) => Ok(s.to_string()),
        Some(_) => Err(Error::Internal(
            "watch must be one of: any, checks, state, commits".to_string(),
        )),
    }
}

/// Validate/default the `pr.resolveThread` `action` (TS
/// `validateResolveThreadAction`, default `resolve`).
pub(crate) fn validate_resolve_action(action: Option<String>) -> Result<String> {
    match action.as_deref() {
        None => Ok("resolve".to_string()),
        Some(s @ ("resolve" | "unresolve")) => Ok(s.to_string()),
        Some(_) => Err(Error::Internal(
            "action must be one of: resolve, unresolve".to_string(),
        )),
    }
}

/// Validate the `pr.createReview` `verdict` onto a [`ReviewVerdict`] (§5.18
/// kebab-case wire values).
pub(crate) fn validate_review_verdict(verdict: &str) -> Result<ReviewVerdict> {
    match verdict {
        "approve" => Ok(ReviewVerdict::Approve),
        "request-changes" => Ok(ReviewVerdict::RequestChanges),
        "comment" => Ok(ReviewVerdict::Comment),
        _ => Err(Error::Internal(
            "verdict must be one of: approve, request-changes, comment".to_string(),
        )),
    }
}

/// Clamp the `pr.waitForChanges` timeout to `[10, 600]` seconds (default 300).
pub(crate) fn clamp_timeout(secs: Option<i64>) -> u64 {
    secs.unwrap_or(300).clamp(10, 600) as u64
}

/// Clamp the `pr.waitForChanges` poll interval to `[10, 60]` seconds (default 15).
pub(crate) fn clamp_poll_interval(secs: Option<i64>) -> u64 {
    secs.unwrap_or(15).clamp(10, 60) as u64
}

/// Source for a snapshot's check-runs: not attempted (no head SHA), fetched, or
/// the fetch failed (TS distinguishes the empty-vs-failed cases).
pub(crate) enum CheckFetch {
    NotAttempted,
    Ok(Vec<CheckRun>),
    Failed,
}

/// A single check-run within a poll snapshot (name + normalized state word).
#[derive(Clone)]
pub(crate) struct CheckSnap {
    pub name: String,
    pub status: String,
}

/// A `pr.waitForChanges` poll snapshot (TS `PRSnapshot`).
///
/// PARITY NOTE: the host-agnostic [`CheckRun`] carries only the normalized
/// [`CheckState`], so `status` here is that derived word rather than GitHub's
/// raw `status`/`conclusion` pair; change detection compares the normalized
/// state per check name.
#[derive(Clone)]
pub(crate) struct PrSnapshot {
    pub head_sha: Option<String>,
    pub state: String,
    pub mergeable: Option<bool>,
    pub mergeable_state: Option<String>,
    pub updated_at: Option<String>,
    pub check_runs: Vec<CheckSnap>,
    pub check_runs_fetch_failed: bool,
}

/// Normalized lowercase word for a [`CheckState`].
pub(crate) fn check_state_word(state: CheckState) -> &'static str {
    match state {
        CheckState::Pending => "pending",
        CheckState::Success => "success",
        CheckState::Failure => "failure",
        CheckState::Neutral => "neutral",
        CheckState::Cancelled => "cancelled",
    }
}

/// Build a poll snapshot from a fetched PR and its check-runs (TS
/// `captureSnapshot`).
pub(crate) fn build_snapshot(pr: &PullRequest, checks: CheckFetch) -> PrSnapshot {
    let (check_runs, failed) = match checks {
        CheckFetch::NotAttempted => (Vec::new(), false),
        CheckFetch::Failed => (Vec::new(), true),
        CheckFetch::Ok(runs) => (
            runs.into_iter()
                .map(|r| CheckSnap {
                    name: r.name,
                    status: check_state_word(r.state).to_string(),
                })
                .collect(),
            false,
        ),
    };
    PrSnapshot {
        head_sha: pr.head_sha.clone().filter(|s| !s.is_empty()),
        state: derive_status_state(pr).to_string(),
        mergeable: pr.mergeable,
        mergeable_state: pr.mergeable_state.clone(),
        updated_at: Some(pr.updated_at.clone()).filter(|s| !s.is_empty()),
        check_runs,
        check_runs_fetch_failed: failed,
    }
}

fn short_sha(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

fn bool_word(b: Option<bool>) -> String {
    match b {
        Some(true) => "true".to_string(),
        Some(false) => "false".to_string(),
        None => "undefined".to_string(),
    }
}

/// Diff two snapshots under a `watch` mode (TS `detectChanges`).
pub(crate) fn detect_changes(
    initial: &PrSnapshot,
    current: &PrSnapshot,
    watch: &str,
) -> Vec<String> {
    let mut changes: Vec<String> = Vec::new();

    if watch == "any" || watch == "commits" {
        if let (Some(i), Some(c)) = (&initial.head_sha, &current.head_sha) {
            if i != c {
                changes.push(format!("New commit: {} → {}", short_sha(i), short_sha(c)));
            }
        }
    }

    if watch == "any" || watch == "state" {
        if initial.state != current.state {
            changes.push(format!(
                "State changed: {} → {}",
                initial.state, current.state
            ));
        }
        if initial.mergeable != current.mergeable {
            changes.push(format!(
                "Mergeable changed: {} → {}",
                bool_word(initial.mergeable),
                bool_word(current.mergeable)
            ));
        }
        if initial.mergeable_state != current.mergeable_state {
            let unknown = || "unknown".to_string();
            changes.push(format!(
                "Mergeable state changed: {} → {}",
                initial.mergeable_state.clone().unwrap_or_else(unknown),
                current.mergeable_state.clone().unwrap_or_else(unknown)
            ));
        }
    }

    if (watch == "any" || watch == "checks")
        && !initial.check_runs_fetch_failed
        && !current.check_runs_fetch_failed
    {
        let initial_map: HashMap<&str, &str> = initial
            .check_runs
            .iter()
            .map(|c| (c.name.as_str(), c.status.as_str()))
            .collect();
        for c in &current.check_runs {
            match initial_map.get(c.name.as_str()) {
                None => changes.push(format!("New check: {} ({})", c.name, c.status)),
                Some(prev) if *prev != c.status.as_str() => {
                    changes.push(format!("Check \"{}\": {} → {}", c.name, prev, c.status))
                }
                _ => {}
            }
        }
    }

    if watch == "any" {
        if let (Some(i), Some(c)) = (&initial.updated_at, &current.updated_at) {
            if i != c && changes.is_empty() {
                changes.push(format!("PR updated: {i} → {c}"));
            }
        }
    }

    changes
}

/// Emoji for a normalized check status (TS `getCheckIcon`, adapted to the
/// normalized [`CheckState`] words).
pub(crate) fn check_icon(status: &str) -> &'static str {
    match status {
        "success" => "✅",
        "failure" => "❌",
        "cancelled" => "🚫",
        "neutral" => "⏭️",
        "pending" => "🔄",
        _ => "•",
    }
}

/// Human-readable change summary (TS `formatChangeSummary`).
pub(crate) fn format_change_summary(
    changes: &[String],
    snapshot: &PrSnapshot,
    elapsed_seconds: u64,
) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "✅ PR changes detected after {elapsed_seconds} seconds:"
    ));
    lines.push(String::new());
    for c in changes {
        lines.push(format!("  • {c}"));
    }
    lines.push(String::new());
    lines.push("--- Current State ---".to_string());
    lines.push(format!("State: {}", snapshot.state));
    lines.push(format!(
        "Head SHA: {}",
        snapshot
            .head_sha
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    ));
    lines.push(format!(
        "Mergeable: {}",
        snapshot
            .mergeable
            .map(|b| b.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ));
    lines.push(format!(
        "Mergeable State: {}",
        snapshot
            .mergeable_state
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string())
    ));
    if !snapshot.check_runs.is_empty() {
        lines.push(String::new());
        lines.push("Check Runs:".to_string());
        for c in &snapshot.check_runs {
            lines.push(format!(
                "  {} {}: {}",
                check_icon(&c.status),
                c.name,
                c.status
            ));
        }
    }
    lines.join("\n")
}

/// Render a snapshot to the `pr.waitForChanges` wire shape (TS `PRSnapshot`).
pub(crate) fn snapshot_json(s: &PrSnapshot) -> Value {
    json!({
        "headSha": s.head_sha,
        "state": s.state,
        "mergeable": s.mergeable,
        "mergeableState": s.mergeable_state,
        "updatedAt": s.updated_at,
        "checkRuns": s.check_runs.iter().map(|c| json!({
            "name": c.name,
            "status": c.status,
        })).collect::<Vec<_>>(),
        "checkRunsFetchFailed": s.check_runs_fetch_failed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(state: PrState, draft: bool, mergeable: Option<bool>, ms: Option<&str>) -> PullRequest {
        PullRequest {
            number: 1,
            url: "u".into(),
            title: "t".into(),
            body: None,
            state,
            draft,
            source_branch: "feat".into(),
            target_branch: "main".into(),
            author: "a".into(),
            mergeable,
            mergeable_state: ms.map(str::to_string),
            head_sha: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    fn review(author: &str, verdict: ReviewVerdict, at: &str) -> Review {
        Review {
            author: author.into(),
            verdict,
            body: None,
            submitted_at: at.into(),
        }
    }

    #[test]
    fn matches_on_head_ref_not_baseref() {
        // The sample PR's `source_branch` (head.ref) is "feat".
        let p = pr(PrState::Open, false, Some(true), Some("clean"));
        assert!(pr_matches_branch(&p, "feat"));
        assert!(!pr_matches_branch(&p, "main"));
        // Empty branch / empty source branch never match.
        assert!(!pr_matches_branch(&p, ""));
        let mut empty = p.clone();
        empty.source_branch = String::new();
        assert!(!pr_matches_branch(&empty, "feat"));
    }

    #[test]
    fn branch_mismatch_only_on_positive_difference() {
        let p = pr(PrState::Open, false, Some(true), Some("clean"));
        // Same branch: no mismatch.
        assert!(!pr_branch_mismatch(&p, "feat"));
        // Different, both non-empty: positive mismatch.
        assert!(pr_branch_mismatch(&p, "other"));
        // Empty source branch: cannot determine → never a mismatch.
        let mut empty = p.clone();
        empty.source_branch = String::new();
        assert!(!pr_branch_mismatch(&empty, "feat"));
        // Empty workspace branch: cannot determine → never a mismatch.
        assert!(!pr_branch_mismatch(&p, ""));
    }

    #[test]
    fn derives_pr_status_draft_merged_closed_open() {
        assert_eq!(
            derive_pr_status(&pr(PrState::Merged, true, None, None)),
            PullRequestStatus::Merged
        );
        assert_eq!(
            derive_pr_status(&pr(PrState::Closed, false, None, None)),
            PullRequestStatus::Closed
        );
        assert_eq!(
            derive_pr_status(&pr(PrState::Open, true, None, None)),
            PullRequestStatus::Draft
        );
        assert_eq!(
            derive_pr_status(&pr(PrState::Open, false, None, None)),
            PullRequestStatus::Open
        );
    }

    #[test]
    fn builds_pr_info_snapshot_from_forge_pr() {
        let mut p = pr(PrState::Open, false, Some(true), Some("clean"));
        p.head_sha = Some("abc123".into());
        p.created_at = "2026-01-01".into();
        p.updated_at = "2026-01-02".into();
        let info = build_pr_info(&p);
        assert_eq!(info.id, "1");
        assert_eq!(info.number, 1);
        assert_eq!(info.status, PullRequestStatus::Open);
        assert_eq!(info.head_ref.as_deref(), Some("feat"));
        assert_eq!(info.base_ref.as_deref(), Some("main"));
        assert_eq!(info.head_sha.as_deref(), Some("abc123"));
        assert_eq!(info.is_draft, Some(false));
        // PascalCase status wire word matches the TS `PullRequestStatus` enum.
        let v = serde_json::to_value(&info).unwrap();
        assert_eq!(v["status"], serde_json::json!("Open"));
        assert_eq!(v["headRef"], serde_json::json!("feat"));
    }

    #[test]
    fn derives_four_value_state() {
        assert_eq!(
            derive_status_state(&pr(PrState::Merged, true, None, None)),
            "merged"
        );
        assert_eq!(
            derive_status_state(&pr(PrState::Open, true, None, None)),
            "draft"
        );
        assert_eq!(
            derive_status_state(&pr(PrState::Closed, false, None, None)),
            "closed"
        );
        assert_eq!(
            derive_status_state(&pr(PrState::Open, false, None, None)),
            "open"
        );
    }

    #[test]
    fn summary_branches_match_ts() {
        assert_eq!(
            build_status_summary("merged", None, "unknown"),
            "✅ PR is merged."
        );
        assert_eq!(
            build_status_summary("closed", None, "unknown"),
            "🚫 PR is closed."
        );
        assert_eq!(
            build_status_summary("open", Some(true), "clean"),
            "✅ PR is mergeable with no conflicts."
        );
        assert_eq!(
            build_status_summary("open", Some(false), "dirty"),
            "⚠️ PR has merge conflicts that need to be resolved."
        );
        assert_eq!(
            build_status_summary("draft", None, "blocked"),
            "📝 PR is a draft. ⏳ GitHub is still computing mergeability. 🔒 PR is blocked (e.g., required reviews or branch protection rules not met)."
        );
        assert_eq!(
            build_status_summary("open", Some(true), "weird"),
            "PR is in state: open, mergeableState: weird."
        );
    }

    #[test]
    fn aggregates_latest_actionable_review_per_user() {
        let reviews = vec![
            review("alice", ReviewVerdict::Approve, "2026-01-01"),
            review("alice", ReviewVerdict::RequestChanges, "2026-01-02"),
            review("bob", ReviewVerdict::Approve, "2026-01-03"),
            review("carol", ReviewVerdict::Comment, "2026-01-04"),
        ];
        let agg = aggregate_reviews(&reviews);
        assert_eq!(agg.approval_count, 1);
        assert_eq!(agg.changes_requested_count, 1);
        assert_eq!(agg.approved_by, vec!["bob".to_string()]);
        assert_eq!(agg.review_decision.as_deref(), Some("CHANGES_REQUESTED"));
    }

    #[test]
    fn aggregate_empty_is_null_decision() {
        let agg = aggregate_reviews(&[]);
        assert!(agg.review_decision.is_none());
        assert_eq!(agg.approval_count, 0);
    }

    #[test]
    fn summarizes_check_runs() {
        let runs = vec![
            CheckRun {
                name: "a".into(),
                state: CheckState::Success,
                url: None,
            },
            CheckRun {
                name: "b".into(),
                state: CheckState::Neutral,
                url: None,
            },
            CheckRun {
                name: "c".into(),
                state: CheckState::Failure,
                url: None,
            },
            CheckRun {
                name: "d".into(),
                state: CheckState::Cancelled,
                url: None,
            },
            CheckRun {
                name: "e".into(),
                state: CheckState::Pending,
                url: None,
            },
        ];
        let s = summarize_check_runs(&runs);
        assert_eq!((s.total, s.passed, s.failed, s.pending), (5, 2, 2, 1));
    }

    #[test]
    fn validates_status_and_clamps_count() {
        assert_eq!(validate_review_comment_status(None).unwrap(), "unresolved");
        assert_eq!(
            validate_review_comment_status(Some("all".into())).unwrap(),
            "all"
        );
        assert!(validate_review_comment_status(Some("bad".into())).is_err());
        assert_eq!(clamp_count(None), 20);
        assert_eq!(clamp_count(Some(0)), 1);
        assert_eq!(clamp_count(Some(500)), 100);
    }

    #[test]
    fn fallback_groups_replies_under_root() {
        let mk = |id: u64, reply: Option<u64>, path: &str| ReviewComment {
            id,
            body: "b".into(),
            path: path.into(),
            line: Some(1),
            author: "a".into(),
            created_at: String::new(),
            updated_at: String::new(),
            in_reply_to_id: reply,
            url: String::new(),
        };
        let threads = fallback_threads(vec![mk(10, None, "x.rs"), mk(11, Some(10), "x.rs")]);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, "rest-thread-10");
        assert_eq!(threads[0].comments.len(), 2);
    }

    #[test]
    fn validates_merge_method_with_default() {
        assert_eq!(validate_merge_method(None).unwrap(), MergeMethod::Merge);
        assert_eq!(
            validate_merge_method(Some("squash".into())).unwrap(),
            MergeMethod::Squash
        );
        assert_eq!(
            validate_merge_method(Some("rebase".into())).unwrap(),
            MergeMethod::Rebase
        );
        assert!(validate_merge_method(Some("bad".into())).is_err());
        assert_eq!(merge_method_word(MergeMethod::Squash), "squash");
    }

    #[test]
    fn validates_watch_action_verdict() {
        assert_eq!(validate_watch_mode(None).unwrap(), "any");
        assert_eq!(
            validate_watch_mode(Some("checks".into())).unwrap(),
            "checks"
        );
        assert!(validate_watch_mode(Some("nope".into())).is_err());
        assert_eq!(validate_resolve_action(None).unwrap(), "resolve");
        assert_eq!(
            validate_resolve_action(Some("unresolve".into())).unwrap(),
            "unresolve"
        );
        assert!(validate_resolve_action(Some("x".into())).is_err());
        assert_eq!(
            validate_review_verdict("request-changes").unwrap(),
            ReviewVerdict::RequestChanges
        );
        assert!(validate_review_verdict("nope").is_err());
    }

    #[test]
    fn clamps_wait_knobs() {
        assert_eq!(clamp_timeout(None), 300);
        assert_eq!(clamp_timeout(Some(5)), 10);
        assert_eq!(clamp_timeout(Some(9000)), 600);
        assert_eq!(clamp_poll_interval(None), 15);
        assert_eq!(clamp_poll_interval(Some(1)), 10);
        assert_eq!(clamp_poll_interval(Some(120)), 60);
    }

    fn snap(head: &str, state: &str, checks: Vec<(&str, &str)>, failed: bool) -> PrSnapshot {
        PrSnapshot {
            head_sha: Some(head.to_string()),
            state: state.to_string(),
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            updated_at: Some("2026-01-01".into()),
            check_runs: checks
                .into_iter()
                .map(|(n, s)| CheckSnap {
                    name: n.into(),
                    status: s.into(),
                })
                .collect(),
            check_runs_fetch_failed: failed,
        }
    }

    #[test]
    fn detects_commit_and_check_changes() {
        let a = snap("aaaaaaaa", "open", vec![("build", "pending")], false);
        let b = snap("bbbbbbbb", "open", vec![("build", "success")], false);
        let changes = detect_changes(&a, &b, "any");
        assert!(changes.iter().any(|c| c.starts_with("New commit:")));
        assert!(changes.iter().any(|c| c.contains("Check \"build\"")));

        // `commits` watch ignores check transitions.
        let only_commits = detect_changes(&a, &b, "commits");
        assert_eq!(only_commits.len(), 1);
        assert!(only_commits[0].starts_with("New commit:"));
    }

    #[test]
    fn detects_no_changes_when_identical() {
        let a = snap("aaaaaaaa", "open", vec![("build", "success")], false);
        assert!(detect_changes(&a, &a, "any").is_empty());
    }
}
