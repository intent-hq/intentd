//! Pure snapshot projection for an already-qualified GitLab observation.
//! This neither establishes identity/permission nor fetches missing signals.

use intent_core::{
    Error, NativeReviewBranchIdentity, NativeReviewDetails, NativeReviewState, RepositoryProvider,
    RepositoryResourceKind, Result, ReviewTarget,
};
use intent_sourcecontrol::{
    ConfirmedReviewState, ProviderAvailability, ReviewBranchIdentity, ReviewDecision,
    ReviewDetails, ReviewObservation,
};
use serde_json::{json, Value};

use super::{aggregate_reviews, rollup_items, MergeRequirementsChecks};

/// Preserve the flat snapshot contract and add qualified resource/detail/availability.
/// Unknown qualified fields deliberately differ from legacy checklist defaults.
/// Quota and response attribution remain owned by the caller; no deadline is inferred.
///
/// # Errors
/// Rejects incompatible target identity or malformed required detail/count fields.
/// The caller must retain this error as a qualified outcome, never retry as ordinary.
pub(crate) fn qualified_review_snapshot(
    target: &ReviewTarget,
    observation: &ReviewObservation,
) -> Result<Value> {
    let details = native_details(target, &observation.details)?;
    let state = lifecycle(&observation.details);
    let signals = &observation.signals;
    let mut availability = observation.availability.clone();
    let coherent = signals
        .checks_head_sha
        .as_ref()
        .is_none_or(|head| !head.is_empty() && details.head_sha.as_ref() == Some(head));
    if !coherent {
        availability.checks = ProviderAvailability::Unknown;
    }
    let checks = (coherent
        && availability.checks == ProviderAvailability::Available
        && signals.checks_known)
        .then(|| MergeRequirementsChecks::from_items(rollup_items(&signals.checks), true));
    let checks_json = checks.as_ref().map_or_else(
        || {
            json!({"total":null,"passed":null,"failed":null,"pending":null,
            "items":null,"failingRequired":null,"pendingRequired":null,"requiredKnown":false})
        },
        |checks| json!(checks),
    );
    let failed_names = checks.as_ref().map(|checks| {
        checks
            .items
            .iter()
            .filter(|c| c.status == "failed")
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    });
    let aggregate = observation.reviews.as_deref().map(aggregate_reviews);
    // The legacy decision helper infers approved/none from aggregates. Qualified
    // results retain only a positively supplied provider decision.
    let decision = signals.review_decision.map(|decision| match decision {
        ReviewDecision::Approved => "approved",
        ReviewDecision::ChangesRequested => "changes_requested",
        ReviewDecision::ReviewRequired => "review_required",
    });
    let mut approvals = json!({
        "decision": decision,
        "have": aggregate.as_ref().map(|a| a.approval_count),
        "changesRequested": aggregate.as_ref().map(|a| a.changes_requested_count),
    });
    let rules = signals
        .branch_rules
        .as_ref()
        .filter(|_| availability.policy == ProviderAvailability::Available);
    if let Some(needed) = rules.and_then(|r| r.required_approving_review_count) {
        approvals["needed"] = json!(needed);
    }
    let conversation = observation.conversation_count;
    let thread_counts = observation.threads;
    if conversation.is_some_and(|n| n < 0)
        || thread_counts.is_some_and(|t| t.review_comment_count < 0 || t.unresolved < 0)
    {
        return Err(malformed());
    }
    let review_comments = thread_counts.map(|t| t.review_comment_count);
    let total = match (conversation, review_comments) {
        (Some(a), Some(b)) => Some(a.checked_add(b).ok_or_else(malformed)?),
        _ => None,
    };
    let mut comments = json!({"conversationCount":conversation,
        "reviewCommentCount":review_comments,"totalCount":total});
    let mut threads = json!({});
    if let Some(tally) = thread_counts {
        comments["unresolvedThreadCount"] = json!(tally.unresolved);
        threads["unresolved"] = json!(tally.unresolved);
    }
    if let Some(required) = rules.and_then(|r| r.required_conversation_resolution) {
        threads["resolutionRequired"] = json!(required);
    }
    let raw = signals.merge_state_status.as_deref();
    let normalized = details.mergeable_state.as_deref();
    let conflicts = condition(normalized, raw, "dirty", &["DIRTY", "conflict"]);
    let behind = condition(normalized, raw, "behind", &["BEHIND", "need_rebase"]);
    let blocked = if matches!(state, "open" | "draft" | "locked") {
        // A normalized `blocked` alone cannot name required checks/reviews.
        // Keep the existing pure reason fold, supplying only evidenced causes.
        super::merge_blocked_reason(
            state,
            details.mergeable,
            if conflicts == Some(true) {
                "dirty"
            } else if behind == Some(true) {
                "behind"
            } else {
                "unknown"
            },
        )
    } else {
        None
    };
    let mut requirements = json!({
        "state":state,"isDraft":details.draft,"hasConflicts":conflicts,"isBehind":behind,
        "checks":checks_json,"approvals":approvals,"threads":threads,"rulesKnown":rules.is_some(),
    });
    if let Some(mergeable) = details.mergeable {
        requirements["mergeable"] = json!(mergeable);
    }
    if let Some(status) = raw {
        requirements["mergeStateStatus"] = json!(status);
    }
    if let Some(reason) = &blocked {
        requirements["mergeBlockedReason"] = json!(reason);
    }
    if let Some(queued) = signals.is_in_merge_queue {
        requirements["isInMergeQueue"] = json!(queued);
    }
    if let Some(removal) = &signals.merge_queue_removal {
        requirements["mergeQueueEjection"] = json!(super::MergeQueueEjection {
            at: removal.at.clone(),
            reason: removal.reason.clone(),
        });
    }
    Ok(json!({
        "repo":target.repository.project_path,"prNumber":observation.details.review.number,
        "title":details.title,"url":details.url,"state":state,"isDraft":details.draft,
        "isMerged":details.state.map(|s| s == NativeReviewState::Merged),
        "isClosed":details.state.map(|s| s == NativeReviewState::Closed),
        "headSha":details.head_sha,"updatedAt":details.updated_at,"mergeable":details.mergeable,
        "mergeableState":details.mergeable_state,"mergeBlockedReason":blocked,
        "checks":{"total":checks.as_ref().map(|c| c.total),"passed":checks.as_ref().map(|c| c.passed),
            "failed":checks.as_ref().map(|c| c.failed),"pending":checks.as_ref().map(|c| c.pending),
            "failedNames":failed_names},
        "reviews":{"decision":decision,"approvals":aggregate.as_ref().map(|a| a.approval_count),
            "changesRequested":aggregate.as_ref().map(|a| a.changes_requested_count)},
        "comments":comments,"requirements":requirements,
        "resource":target,"details":details,"availability":availability,
    }))
}

fn malformed() -> Error {
    Error::Internal("Invalid qualified review observation".into())
}

fn native_details(target: &ReviewTarget, details: &ReviewDetails) -> Result<NativeReviewDetails> {
    let review = &details.review;
    if target.repository.provider != RepositoryProvider::Gitlab
        || target.kind != RepositoryResourceKind::MergeRequest
        || target.number == 0
        || review.number != target.number
        || target.repository.instance_base_url.is_empty()
        || target.repository.project_path.is_empty()
    {
        return Err(Error::InvalidParams(
            "Qualified review target mismatch".into(),
        ));
    }
    if [
        &review.url,
        &review.title,
        &review.created_at,
        &review.updated_at,
    ]
    .into_iter()
    .any(String::is_empty)
    {
        return Err(malformed());
    }
    let branch = |b: &ReviewBranchIdentity| -> Result<NativeReviewBranchIdentity> {
        if b.instance_base_url != target.repository.instance_base_url
            || b.project_id == 0
            || b.branch.is_empty()
        {
            return Err(malformed());
        }
        Ok(NativeReviewBranchIdentity {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: b.instance_base_url.clone(),
            project_id: b.project_id.to_string(),
            project_path: b.project_path.clone(),
            branch: b.branch.clone(),
        })
    };
    if details
        .target
        .as_ref()
        .and_then(|b| b.project_path.as_ref())
        .is_some_and(|p| p != &target.repository.project_path)
    {
        return Err(malformed());
    }
    Ok(NativeReviewDetails {
        resource: target.clone(),
        url: review.url.clone(),
        title: review.title.clone(),
        body: review.body.clone(),
        state: details.confirmed_state.map(|s| match s {
            ConfirmedReviewState::Open => NativeReviewState::Open,
            ConfirmedReviewState::Locked => NativeReviewState::Locked,
            ConfirmedReviewState::Closed => NativeReviewState::Closed,
            ConfirmedReviewState::Merged => NativeReviewState::Merged,
        }),
        draft: details.confirmed_draft,
        source_branch: nonempty(&review.source_branch),
        target_branch: nonempty(&review.target_branch),
        source: details.source.as_ref().map(branch).transpose()?,
        target: details.target.as_ref().map(branch).transpose()?,
        // Preserve the existing provider-normalized author. Its legacy `ghost`
        // fallback has no raw-presence flag in ReviewDetails.
        author: nonempty(&review.author),
        mergeable: review.mergeable,
        mergeable_state: review.mergeable_state.clone(),
        head_sha: review.head_sha.clone(),
        created_at: Some(review.created_at.clone()),
        updated_at: Some(review.updated_at.clone()),
    })
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn lifecycle(details: &ReviewDetails) -> &'static str {
    match details.confirmed_state {
        Some(ConfirmedReviewState::Open) if details.confirmed_draft == Some(true) => "draft",
        Some(ConfirmedReviewState::Open) => "open",
        Some(ConfirmedReviewState::Locked) => "locked",
        Some(ConfirmedReviewState::Closed) => "closed",
        Some(ConfirmedReviewState::Merged) => "merged",
        None => "unknown",
    }
}

fn condition(
    normalized: Option<&str>,
    raw: Option<&str>,
    positive: &str,
    raw_positive: &[&str],
) -> Option<bool> {
    if normalized == Some(positive) || raw.is_some_and(|s| raw_positive.contains(&s)) {
        Some(true)
    } else if normalized == Some("clean") || matches!(raw, Some("CLEAN" | "mergeable")) {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
