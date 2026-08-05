//! Wire-policy glue for the `pr.*` survivors and shared forge plumbing
//! (§5.7, §7.5, §7.6).
//!
//! Pure mapping/aggregation ported from the TS ground truth (`ws-pr-api.ts`,
//! `github.service.ts`): the `pr.status` summary, the workspace↔PR linkage
//! rules behind `pr.refresh` and the background sweep, and the review /
//! check-run / thread aggregation backing the MCP-only `ws.pr.snapshot`
//! engine. The forge calls themselves go through the host-agnostic
//! [`SourceControl`] trait; this module owns only the parity-critical glue so it
//! stays unit-testable without a network.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use intent_core::{parse_iso, Error, PullRequestInfo, PullRequestStatus, Result, Workspace};
use intent_sourcecontrol::{
    CheckRun, CheckState, MergeMethod, Page, PageParams, PrQuery, PrState, PullRequest, RepoRef,
    Review, ReviewComment, ReviewThread, ReviewThreadComment, ReviewVerdict, SourceControl,
    SourceControlRegistry, SourceControlSettings,
};
use time::OffsetDateTime;

/// TS `NO_ACTIVE_PR_ERROR`; every active-PR-scoped method needs one (§5.7).
pub(crate) const NO_ACTIVE_PR: &str = "No active PR";

/// Map a forge error onto the domain `Internal` error (→ `-32603`): the TS
/// `pr.*` handlers wrap every underlying throw in `INTERNAL_ERROR` (§5.7), and
/// the graceful "not configured" path (§8.3) surfaces the same way.
/// `Unsupported` (§7.2/§7.4 capability gating) maps onto a stable wire message
/// with the `unsupported by provider:` prefix so clients can match on it.
pub(crate) fn map_sc_err(e: intent_sourcecontrol::Error) -> Error {
    match e {
        intent_sourcecontrol::Error::Unsupported(msg) => {
            Error::Internal(format!("unsupported by provider: {msg}"))
        }
        other => Error::Internal(other.to_string()),
    }
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

/// Parse the `ws.pr.snapshot` cross-repo override: an `"owner/name"` slug
/// with exactly one `/` and both halves non-empty.
pub(crate) fn parse_repo_slug(slug: &str) -> Result<(String, String)> {
    let trimmed = slug.trim();
    if let Some((owner, name)) = trimmed.split_once('/') {
        if !owner.is_empty() && !name.is_empty() && !name.contains('/') {
            return Ok((owner.to_string(), name.to_string()));
        }
    }
    Err(Error::InvalidParams(format!(
        "repo must be an \"owner/name\" slug, got `{slug}`"
    )))
}

/// Background-sweep activity window (§7.6/§7.7): workspaces whose
/// `updatedAt`/`lastActivity` is within this many minutes are refreshed on
/// every sweep tick; colder workspaces only refresh on every
/// [`SWEEP_IDLE_TICK_MULTIPLE`]-th tick, trimming steady forge load.
pub(crate) const SWEEP_ACTIVE_WINDOW_MINUTES: i64 = 30;

/// Idle workspaces refresh on every Nth sweep tick (~30 minutes at the 180s
/// base interval wired in `intentd/src/main.rs`).
pub(crate) const SWEEP_IDLE_TICK_MULTIPLE: u64 = 10;

/// Whether the background sweep should refresh `ws` on this `tick` (§7.6 with
/// the §7.7 "defer non-urgent refreshes" trimming): every
/// [`SWEEP_IDLE_TICK_MULTIPLE`]-th tick (including tick 0, the first sweep
/// after startup) refreshes every workspace; ticks in between refresh only
/// workspaces active since `active_cutoff` (parsed once per sweep by the
/// caller). A sweep that persists a PR delta bumps `updatedAt`, so workspaces
/// with churning PRs stay on the every-tick cadence while quiet ones cool
/// down. Malformed workspace timestamps — and a `None` cutoff — fail open
/// (count as active) so a bad record never slows its own refreshes.
pub(crate) fn sweep_due(ws: &Workspace, active_cutoff: Option<OffsetDateTime>, tick: u64) -> bool {
    if tick.is_multiple_of(SWEEP_IDLE_TICK_MULTIPLE) {
        return true;
    }
    let Some(cutoff) = active_cutoff else {
        return true;
    };
    let active = |ts: &str| match parse_iso(ts) {
        Some(t) => t >= cutoff,
        None => true,
    };
    active(&ws.updated_at) || ws.last_activity.as_deref().is_some_and(active)
}

/// The workspace's active PR number, or [`NO_ACTIVE_PR`] when unlinked.
pub(crate) fn active_pr_number(ws: &Workspace) -> Result<u64> {
    ws.pr_number
        .ok_or_else(|| Error::Internal(NO_ACTIVE_PR.to_string()))
}

// ===========================================================================
// Workspace ↔ PR linkage (§7.6). The matching rule ports from the TS side:
// a PR belongs to a workspace when its head ref equals the workspace's OWN
// branch (`pr.head.ref === workspace.branch`) OR its source branch matches
// the workspace's `baseRef` (`matchesBaseRef` in the FE `baseref-matching.ts`
// — review workspaces created *for* a PR store that PR's head as `baseRef`).
// Branch match takes precedence where ordering matters. The baseRef arm only
// strips a conservative remote-name allowlist (`origin/`, `upstream/`,
// `fork/`) so slashed local branches are never over-stripped.
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
    /// A stale link was cleared after a positive mismatch against both the
    /// workspace's branch and its `baseRef` (`pr:unlinked`).
    Unlinked,
}

impl PrRefreshOutcome {
    /// The lowercase wire form of the outcome for the `pr.refresh` result
    /// (PROTOCOL §5.7 extension).
    pub fn as_wire_str(self) -> &'static str {
        match self {
            PrRefreshOutcome::Skipped => "skipped",
            PrRefreshOutcome::Unchanged => "unchanged",
            PrRefreshOutcome::Linked => "linked",
            PrRefreshOutcome::Updated => "updated",
            PrRefreshOutcome::Unlinked => "unlinked",
        }
    }
}

/// True when `pr.head.ref` (the host-agnostic `source_branch`) equals the
/// workspace's own `branch`, and both are non-empty — the link/discovery rule.
pub(crate) fn pr_matches_branch(pr: &PullRequest, branch: &str) -> bool {
    !pr.source_branch.is_empty() && !branch.is_empty() && pr.source_branch == branch
}

/// Port of the FE `baseref-matching.ts::matchesBaseRef` (§7.6): true when a
/// PR's `source_branch` matches the workspace's `baseRef`. Raw equality always
/// wins (covers plain branches and slashed local branches alike); when
/// `base_ref` starts with a known remote from [`intent_git::refs::CANONICAL_BASE_REF_REMOTES`]
/// the stripped remainder is also compared — defensive, covering legacy rows
/// persisted before write-side canonicalisation. First path segments outside
/// the allowlist are NOT stripped (`feature/foo` never matches `foo`);
/// empty/absent inputs never match.
pub(crate) fn matches_base_ref(pr_source_branch: &str, base_ref: Option<&str>) -> bool {
    let Some(base_ref) = base_ref.filter(|s| !s.is_empty()) else {
        return false;
    };
    if pr_source_branch.is_empty() {
        return false;
    }
    if pr_source_branch == base_ref {
        return true;
    }
    let stripped = crate::canonicalise_base_ref(base_ref);
    stripped != base_ref && stripped == pr_source_branch
}

/// Combined §7.6 link predicate: a PR matches a workspace when its head ref
/// equals the workspace's own `branch` OR its source branch matches the
/// workspace's `baseRef`. Branch match takes precedence where ordering
/// matters (see [`discover_matching_open_pr`]).
pub(crate) fn pr_matches_workspace(pr: &PullRequest, branch: &str, base_ref: Option<&str>) -> bool {
    pr_matches_branch(pr, branch) || matches_base_ref(&pr.source_branch, base_ref)
}

/// The §7.6 stale-unlink rule: true only on a POSITIVE mismatch against the
/// whole workspace — the PR's `source_branch` is known, at least one of the
/// workspace's `branch` / `baseRef` is known, and NEITHER matches. Unknown
/// inputs (empty `source_branch`, or both `branch` and `baseRef` empty) are
/// "cannot determine" and never clear a link, mirroring the TS guard that only
/// clears a stale link when the source branch is present and differs; a
/// branch-less workspace with a mismatching `baseRef` still unlinks.
pub(crate) fn pr_workspace_mismatch(
    pr: &PullRequest,
    branch: &str,
    base_ref: Option<&str>,
) -> bool {
    if pr.source_branch.is_empty() {
        return false;
    }
    let branch_known = !branch.is_empty();
    let base_ref_known = base_ref.is_some_and(|s| !s.is_empty());
    if !branch_known && !base_ref_known {
        return false;
    }
    !pr_matches_workspace(pr, branch, base_ref)
}

/// Port of the FE `getBaseRefMatchCandidates`: the head-query candidates for a
/// workspace `baseRef` — the raw value plus the allowlist-stripped remainder
/// when it differs (legacy remote-qualified rows). Empty/absent `baseRef`
/// yields no candidates.
pub(crate) fn base_ref_match_candidates(base_ref: Option<&str>) -> Vec<String> {
    let Some(base_ref) = base_ref.filter(|s| !s.is_empty()) else {
        return Vec::new();
    };
    let mut candidates = vec![base_ref.to_string()];
    let stripped = crate::canonicalise_base_ref(base_ref);
    if stripped != base_ref {
        candidates.push(stripped);
    }
    candidates
}

/// Discover the open PR a workspace should link (§7.6): query head = the
/// workspace's own `branch` first (branch match takes precedence); when that
/// yields no match, fall back to one open-PR query per `baseRef` candidate
/// (skipping candidates equal to `branch` — already covered) and accept PRs
/// matching the combined branch-OR-baseRef predicate. `exclude` drops a
/// just-merged/closed PR number so relink never loops back onto it. When
/// several PRs match, the highest number wins so selection is deterministic
/// regardless of forge sort order.
pub(crate) async fn discover_matching_open_pr(
    sc: &dyn SourceControl,
    repo_ref: &RepoRef,
    branch: &str,
    base_ref: Option<&str>,
    exclude: Option<u64>,
) -> std::result::Result<Option<PullRequest>, intent_sourcecontrol::Error> {
    if !branch.is_empty() {
        let query = PrQuery {
            state: Some(PrState::Open),
            head: Some(branch.to_string()),
            ..Default::default()
        };
        if let Some(pr) = sc
            .list_prs(repo_ref, query)
            .await?
            .items
            .into_iter()
            .filter(|p| Some(p.number) != exclude && pr_matches_branch(p, branch))
            .max_by_key(|p| p.number)
        {
            return Ok(Some(pr));
        }
    }
    let mut best: Option<PullRequest> = None;
    for candidate in base_ref_match_candidates(base_ref) {
        if candidate == branch {
            continue;
        }
        let query = PrQuery {
            state: Some(PrState::Open),
            head: Some(candidate),
            ..Default::default()
        };
        let matched = sc
            .list_prs(repo_ref, query)
            .await?
            .items
            .into_iter()
            .filter(|p| Some(p.number) != exclude && pr_matches_workspace(p, branch, base_ref))
            .max_by_key(|p| p.number);
        if let Some(p) = matched {
            if best.as_ref().is_none_or(|b| p.number > b.number) {
                best = Some(p);
            }
        }
    }
    Ok(best)
}

/// Upsert a PR snapshot into the daemon-owned `workspace.pull_requests` list
/// (keyed by PR number), returning `true` when the list actually changed.
/// Keeps merged/closed PRs recorded alongside the currently-linked one so the
/// FE can render the full per-branch PR history without a refetch (§7.6).
/// Pre-existing duplicates for the same number (e.g. written via
/// `workspace.update` before the daemon owned the list) are collapsed into the
/// single upserted entry.
pub(crate) fn upsert_pr_info(
    list: &mut Option<Vec<PullRequestInfo>>,
    info: &PullRequestInfo,
) -> bool {
    let items = list.get_or_insert_with(Vec::new);
    let matches = items.iter().filter(|p| p.number == info.number).count();
    // Unchanged only when exactly one entry for this number exists and it
    // already equals the snapshot.
    if matches == 1 {
        let existing = items.iter_mut().find(|p| p.number == info.number).unwrap();
        if *existing == *info {
            return false;
        }
        *existing = info.clone();
        return true;
    }
    // 0 matches: append. >1 matches: collapse the duplicates, keeping the
    // fresh snapshot at the first duplicate's position.
    if matches == 0 {
        items.push(info.clone());
        return true;
    }
    let first = items.iter().position(|p| p.number == info.number).unwrap();
    items.retain(|p| p.number != info.number);
    items.insert(first, info.clone());
    true
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

/// Aggregated actionable-review counts for the `ws.pr.snapshot` `reviews`
/// block.
pub(crate) struct ReviewAggregate {
    pub approval_count: i64,
    pub changes_requested_count: i64,
}

/// Check-run tally for the `ws.pr.snapshot` `checks` block.
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
    for login in &order {
        match latest.get(login) {
            Some((ReviewVerdict::Approve, _)) => approval_count += 1,
            Some((ReviewVerdict::RequestChanges, _)) => changes_requested_count += 1,
            _ => {}
        }
    }
    ReviewAggregate {
        approval_count,
        changes_requested_count,
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

/// Names of failing check runs (failure / cancelled) for the `ws.pr.snapshot`
/// `checks.failedNames` list, in run order.
pub(crate) fn failed_check_names(runs: &[CheckRun]) -> Vec<String> {
    runs.iter()
        .filter(|r| matches!(r.state, CheckState::Failure | CheckState::Cancelled))
        .map(|r| r.name.clone())
        .collect()
}

/// Comment tallies for the `ws.pr.snapshot` `comments` block: the total number
/// of inline review comments across `threads` (EVERY thread comment counts,
/// including replies inside a thread) and the number of unresolved threads.
pub(crate) fn count_thread_comments(threads: &[ReviewThread]) -> (i64, i64) {
    let review_comment_count = threads.iter().map(|t| t.comments.len() as i64).sum();
    let unresolved = threads.iter().filter(|t| !t.is_resolved).count() as i64;
    (review_comment_count, unresolved)
}

/// The `ws.pr.snapshot` `mergeBlockedReason` derivation: a human-readable
/// reason merging is blocked, non-`None` exactly when the PR is open (incl.
/// draft) and cannot be merged, from the [`derive_status_state`] `state`,
/// `mergeable`, and raw `mergeable_state`. Merged/closed PRs yield `None`;
/// for any other `mergeable_state` (e.g. still-computing `unknown`) a draft
/// PR or an explicit `mergeable == Some(false)` still produces a reason
/// before falling back to `None`.
pub(crate) fn merge_blocked_reason(
    state: &str,
    mergeable: Option<bool>,
    mergeable_state: &str,
) -> Option<String> {
    if state == "merged" || state == "closed" {
        return None;
    }
    let reason = match mergeable_state {
        "dirty" => Some("merge conflicts"),
        "blocked" => Some("blocked by required checks or reviews"),
        "behind" => Some("branch behind base"),
        _ if state == "draft" => Some("draft PRs cannot be merged"),
        _ if mergeable == Some(false) => Some("not mergeable"),
        _ => None,
    };
    reason.map(str::to_string)
}

/// The `ws.pr.snapshot` `reviews.decision` derivation: `changes_requested` /
/// `approved` from the aggregated actionable reviews; `review_required` when
/// an open (incl. draft) PR has no actionable reviews but the forge reports
/// the merge as `blocked` (required reviews / branch protection unmet); else
/// `none`.
pub(crate) fn snapshot_review_decision(
    agg: &ReviewAggregate,
    state: &str,
    mergeable_state: &str,
) -> &'static str {
    if agg.changes_requested_count > 0 {
        "changes_requested"
    } else if agg.approval_count > 0 {
        "approved"
    } else if (state == "open" || state == "draft") && mergeable_state == "blocked" {
        "review_required"
    } else {
        "none"
    }
}

/// Page size for the exhaustive review-thread fetch (`ws.pr.snapshot`).
pub(crate) const REVIEW_FETCH_PAGE_LIMIT: u8 = 100;

/// Page cap for the exhaustive review-thread fetch: at most 10 pages
/// (× [`REVIEW_FETCH_PAGE_LIMIT`] items) per request; when the cap stops the
/// loop early the reply reports `hasMore: true`.
pub(crate) const REVIEW_FETCH_MAX_PAGES: usize = 10;

/// Drain a cursor-paginated forge read (`ws.pr.snapshot` thread/comment
/// counting): fetch pages of [`REVIEW_FETCH_PAGE_LIMIT`] via `next_cursor`
/// until exhausted or [`REVIEW_FETCH_MAX_PAGES`] is hit. Returns `(items,
/// pages_fetched, has_more)`, `has_more` being true iff the cap stopped the
/// loop early.
pub(crate) async fn fetch_all_pages<T, F, Fut>(
    mut fetch: F,
) -> std::result::Result<(Vec<T>, usize, bool), intent_sourcecontrol::Error>
where
    F: FnMut(PageParams) -> Fut,
    Fut: Future<Output = std::result::Result<Page<T>, intent_sourcecontrol::Error>>,
{
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages_fetched = 0;
    while pages_fetched < REVIEW_FETCH_MAX_PAGES {
        let page = fetch(PageParams {
            limit: REVIEW_FETCH_PAGE_LIMIT,
            cursor: cursor.take(),
        })
        .await?;
        pages_fetched += 1;
        items.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((items, pages_fetched, false)),
        }
    }
    Ok((items, pages_fetched, true))
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
// Merge glue shared by `github.pulls.merge` and `accept-changes.mergePR`.
// ===========================================================================

/// Validate/default the `mergeMethod` argument (TS `validateMergeMethod`,
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
    fn parse_repo_slug_accepts_owner_name_and_trims() {
        assert_eq!(
            parse_repo_slug("acme/widgets").unwrap(),
            ("acme".to_string(), "widgets".to_string())
        );
        assert_eq!(
            parse_repo_slug("  acme/widgets  ").unwrap(),
            ("acme".to_string(), "widgets".to_string())
        );
    }

    #[test]
    fn parse_repo_slug_rejects_malformed_slugs() {
        for bad in ["", " ", "acme", "acme/", "/widgets", "a/b/c"] {
            let err = parse_repo_slug(bad).unwrap_err();
            assert!(
                matches!(&err, Error::InvalidParams(m) if m.contains("owner/name")),
                "slug `{bad}` should be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn branch_predicate_matches_on_head_ref() {
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
    fn base_ref_matches_on_raw_equality() {
        // Mirrors `baseref-matching.test.ts`: plain and slashed local
        // branches match raw-equal, without any stripping.
        assert!(matches_base_ref("main", Some("main")));
        assert!(matches_base_ref("feature/foo", Some("feature/foo")));
        assert!(!matches_base_ref("dev", Some("main")));
    }

    #[test]
    fn base_ref_strips_only_known_remote_prefixes() {
        // Allowlisted remotes are stripped for the comparison…
        assert!(matches_base_ref("main", Some("origin/main")));
        assert!(matches_base_ref(
            "release/1.0",
            Some("upstream/release/1.0")
        ));
        assert!(matches_base_ref("foo", Some("fork/foo")));
        // …but slashed local branches / unknown remotes are NOT stripped.
        assert!(!matches_base_ref("foo", Some("feature/foo")));
        assert!(!matches_base_ref("main", Some("myremote/main")));
        assert!(!matches_base_ref("dev", Some("origin/main")));
    }

    #[test]
    fn base_ref_empty_inputs_never_match() {
        assert!(!matches_base_ref("", Some("main")));
        assert!(!matches_base_ref("main", None));
        assert!(!matches_base_ref("main", Some("")));
        assert!(!matches_base_ref("", None));
    }

    #[test]
    fn base_ref_candidates_follow_allowlist_rule() {
        // Mirrors the FE `getBaseRefMatchCandidates` cases.
        assert_eq!(
            base_ref_match_candidates(Some("origin/foo")),
            vec!["origin/foo".to_string(), "foo".to_string()]
        );
        assert_eq!(
            base_ref_match_candidates(Some("upstream/release/1.0")),
            vec![
                "upstream/release/1.0".to_string(),
                "release/1.0".to_string()
            ]
        );
        assert_eq!(
            base_ref_match_candidates(Some("feature/foo")),
            vec!["feature/foo".to_string()]
        );
        assert_eq!(
            base_ref_match_candidates(Some("main")),
            vec!["main".to_string()]
        );
        assert!(base_ref_match_candidates(None).is_empty());
        assert!(base_ref_match_candidates(Some("")).is_empty());
    }

    #[test]
    fn combined_predicate_matches_branch_or_base_ref() {
        // The sample PR's `source_branch` is "feat".
        let p = pr(PrState::Open, false, Some(true), Some("clean"));
        // Branch match alone.
        assert!(pr_matches_workspace(&p, "feat", None));
        // baseRef match alone (plain and legacy remote-qualified).
        assert!(pr_matches_workspace(&p, "other", Some("feat")));
        assert!(pr_matches_workspace(&p, "other", Some("origin/feat")));
        // Neither matches.
        assert!(!pr_matches_workspace(&p, "other", Some("main")));
        assert!(!pr_matches_workspace(&p, "other", None));
        // Empty source branch matches nothing.
        let mut empty = p.clone();
        empty.source_branch = String::new();
        assert!(!pr_matches_workspace(&empty, "feat", Some("feat")));
    }

    #[test]
    fn workspace_mismatch_unlinks_only_on_positive_dual_mismatch() {
        // The unlink rule is `pr_workspace_mismatch`: a PR whose head equals
        // the workspace's `baseRef` (review workspace) stays linked despite a
        // positive branch mismatch.
        let p = pr(PrState::Open, false, Some(true), Some("clean"));
        assert!(matches_base_ref(&p.source_branch, Some("feat")));
        assert!(!pr_workspace_mismatch(&p, "review-ws", Some("feat")));
        // A positive mismatch against BOTH still unlinks.
        assert!(pr_workspace_mismatch(&p, "review-ws", Some("main")));
        // Branch-only workspaces keep the old branch-mismatch semantics.
        assert!(pr_workspace_mismatch(&p, "review-ws", None));
        assert!(!pr_workspace_mismatch(&p, "feat", None));
        // A branch-less workspace with a mismatching baseRef still unlinks…
        assert!(pr_workspace_mismatch(&p, "", Some("main")));
        assert!(!pr_workspace_mismatch(&p, "", Some("feat")));
        // …but fully-unknown inputs never do (cannot determine).
        assert!(!pr_workspace_mismatch(&p, "", None));
        assert!(!pr_workspace_mismatch(&p, "", Some("")));
        let mut empty = p.clone();
        empty.source_branch = String::new();
        assert!(!pr_workspace_mismatch(&empty, "review-ws", Some("main")));
    }

    #[test]
    fn upserts_pr_info_by_number() {
        let open = build_pr_info(&pr(PrState::Open, false, Some(true), Some("clean")));
        let mut list: Option<Vec<PullRequestInfo>> = None;

        // Insert into an absent list.
        assert!(upsert_pr_info(&mut list, &open));
        assert_eq!(list.as_ref().unwrap().len(), 1);

        // Identical snapshot: no change.
        assert!(!upsert_pr_info(&mut list, &open));
        assert_eq!(list.as_ref().unwrap().len(), 1);

        // Same number, different snapshot: replaced in place.
        let merged = build_pr_info(&pr(PrState::Merged, false, None, None));
        assert!(upsert_pr_info(&mut list, &merged));
        assert_eq!(list.as_ref().unwrap().len(), 1);
        assert_eq!(list.as_ref().unwrap()[0].status, PullRequestStatus::Merged);

        // A different number appends.
        let mut second = pr(PrState::Open, false, None, None);
        second.number = 2;
        assert!(upsert_pr_info(&mut list, &build_pr_info(&second)));
        assert_eq!(list.as_ref().unwrap().len(), 2);

        // Pre-existing duplicates (e.g. legacy workspace.update writes) are
        // collapsed into a single entry at the first duplicate's position.
        let dup = list.as_ref().unwrap()[0].clone();
        list.as_mut().unwrap().push(dup);
        assert_eq!(list.as_ref().unwrap().len(), 3);
        assert!(upsert_pr_info(&mut list, &merged));
        let items = list.as_ref().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].number, merged.number);
        assert_eq!(items[0].status, PullRequestStatus::Merged);
        assert_eq!(items[1].number, 2);
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
    }

    #[test]
    fn aggregate_empty_has_zero_counts() {
        let agg = aggregate_reviews(&[]);
        assert_eq!(agg.approval_count, 0);
        assert_eq!(agg.changes_requested_count, 0);
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
    fn failed_check_names_lists_failure_and_cancelled_in_order() {
        let mk = |name: &str, state: CheckState| CheckRun {
            name: name.into(),
            state,
            url: None,
        };
        let runs = vec![
            mk("build", CheckState::Success),
            mk("test", CheckState::Failure),
            mk("lint", CheckState::Pending),
            mk("e2e", CheckState::Cancelled),
            mk("docs", CheckState::Neutral),
        ];
        assert_eq!(
            failed_check_names(&runs),
            vec!["test".to_string(), "e2e".to_string()]
        );
        assert!(failed_check_names(&[]).is_empty());
    }

    #[test]
    fn thread_comment_count_includes_replies() {
        let comment = |id: &str| ReviewThreadComment {
            id: id.into(),
            body: "b".into(),
            author: "a".into(),
            path: "x.rs".into(),
            line: Some(1),
            created_at: String::new(),
        };
        let threads = vec![
            ReviewThread {
                id: "RT1".into(),
                is_resolved: false,
                // Root comment + two replies: all three count.
                comments: vec![comment("c1"), comment("c2"), comment("c3")],
            },
            ReviewThread {
                id: "RT2".into(),
                is_resolved: true,
                comments: vec![comment("c4")],
            },
        ];
        assert_eq!(count_thread_comments(&threads), (4, 1));
        assert_eq!(count_thread_comments(&[]), (0, 0));
    }

    #[test]
    fn merge_blocked_reason_matches_open_and_not_mergeable() {
        // Blocked states on an open PR derive a human-readable reason.
        assert_eq!(
            merge_blocked_reason("open", Some(false), "dirty").as_deref(),
            Some("merge conflicts")
        );
        assert_eq!(
            merge_blocked_reason("open", Some(true), "blocked").as_deref(),
            Some("blocked by required checks or reviews")
        );
        assert_eq!(
            merge_blocked_reason("open", Some(true), "behind").as_deref(),
            Some("branch behind base")
        );
        assert_eq!(
            merge_blocked_reason("draft", Some(true), "clean").as_deref(),
            Some("draft PRs cannot be merged")
        );
        // A blocked-state reason wins over the draft fallback.
        assert_eq!(
            merge_blocked_reason("draft", Some(false), "dirty").as_deref(),
            Some("merge conflicts")
        );
        assert_eq!(
            merge_blocked_reason("open", Some(false), "weird").as_deref(),
            Some("not mergeable")
        );
        // Not blocked: mergeable/clean, unstable (non-required checks), and
        // still-computing mergeability.
        assert!(merge_blocked_reason("open", Some(true), "clean").is_none());
        assert!(merge_blocked_reason("open", Some(true), "unstable").is_none());
        assert!(merge_blocked_reason("open", None, "unknown").is_none());
        // Merged/closed PRs never report a blocked reason.
        assert!(merge_blocked_reason("merged", Some(false), "dirty").is_none());
        assert!(merge_blocked_reason("closed", Some(false), "blocked").is_none());
    }

    #[test]
    fn snapshot_decision_orders_changes_requested_approved_required_none() {
        let agg = |approvals: i64, changes: i64| ReviewAggregate {
            approval_count: approvals,
            changes_requested_count: changes,
        };
        assert_eq!(
            snapshot_review_decision(&agg(1, 1), "open", "clean"),
            "changes_requested"
        );
        assert_eq!(
            snapshot_review_decision(&agg(2, 0), "open", "blocked"),
            "approved"
        );
        assert_eq!(
            snapshot_review_decision(&agg(0, 0), "open", "blocked"),
            "review_required"
        );
        assert_eq!(
            snapshot_review_decision(&agg(0, 0), "draft", "blocked"),
            "review_required"
        );
        assert_eq!(
            snapshot_review_decision(&agg(0, 0), "open", "clean"),
            "none"
        );
        // Merged/closed PRs never derive `review_required` from `blocked`.
        assert_eq!(
            snapshot_review_decision(&agg(0, 0), "merged", "blocked"),
            "none"
        );
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
    }
}
