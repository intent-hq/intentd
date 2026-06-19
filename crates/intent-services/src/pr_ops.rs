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

use intent_core::{Error, Result, Workspace};
use intent_sourcecontrol::{
    CheckRun, CheckState, PrState, PullRequest, Review, ReviewComment, ReviewThread,
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
pub(crate) fn resolve_source_control(
    injected: Option<Arc<dyn SourceControl>>,
) -> Result<Arc<dyn SourceControl>> {
    match injected {
        Some(sc) => Ok(sc),
        None => SourceControlRegistry::from_settings(&SourceControlSettings::default())
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
}
