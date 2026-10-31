//! Shared review-monitor sentence patterns. Qualified facts preserve unknowns.
use serde_json::Value;
use std::fmt::Write as _;

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => "unknown".into(),
    }
}
fn names(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}
fn plural(n: i64) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

/// Names of the skipped checks in a wire `checks.items` array, in order.
pub(crate) fn skipped_names(items: &Value) -> Vec<&str> {
    items
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter(|item| item["status"] == "skipped")
                .filter_map(|item| item["name"].as_str())
                .collect()
        })
        .unwrap_or_default()
}

/// The one aggregate line for a check suite that finished. "all checks
/// passed" is reserved for suites where every check ran and passed; skipped
/// checks are counted and named on their own.
pub(crate) fn checks_completed(total: i64, passed: i64, failed: i64, skipped: &[&str]) -> String {
    if failed == 0 && skipped.is_empty() {
        return format!("all checks passed ({total})");
    }
    let mut line = format!("all checks completed: {passed} passed");
    if failed > 0 || skipped.is_empty() {
        let _ = write!(line, ", {failed} failed");
    }
    if !skipped.is_empty() {
        let _ = write!(line, ", {} skipped ({})", skipped.len(), skipped.join(", "));
    }
    line
}

/// The "where it stands now" checklist. `head` is the commit this read's
/// check results belong to: a wake can sit in the queue past a push, so the
/// row lets an agent tell a stale report from one about its latest commit
/// (intentd#2875). Empty or absent renders as unknown.
pub(crate) fn checklist(r: &Value, head: Option<&str>, ready: bool) -> String {
    let mut lines = vec![format!("state: {}", text(&r["state"]))];
    lines.push(match head.filter(|h| !h.is_empty()) {
        Some(head) => {
            let short: String = head.chars().take(8).collect();
            format!("head: {short} (checks below are for this commit)")
        }
        None => "head: unknown (cannot tell which commit the checks below are for)".into(),
    });
    let a = &r["approvals"];
    lines.push(match a["needed"].as_i64() {
        Some(needed) => format!(
            "approvals: {} ({}/{} required)",
            text(&a["decision"]),
            text(&a["have"]),
            needed
        ),
        None => format!(
            "approvals: {} ({} approving)",
            text(&a["decision"]),
            text(&a["have"])
        ),
    });
    if let Some(n) = a["changesRequested"].as_i64().filter(|n| *n > 0) {
        lines.push(format!("changes requested by {n} reviewer{}", plural(n)));
    }
    if let Some(reviews) = a["latestReviews"].as_array().filter(|r| !r.is_empty()) {
        let rendered: Vec<String> = reviews
            .iter()
            .map(|r| {
                let mut entry = format!(
                    "{} {}",
                    text(&r["reviewer"]),
                    text(&r["state"]).replace('_', " ")
                );
                if let Some(commit) = r["commitSha"].as_str() {
                    let short: String = commit.chars().take(8).collect();
                    let _ = write!(entry, " on {short}");
                    match r["onCurrentHead"].as_bool() {
                        Some(true) => entry.push_str(" (current head)"),
                        Some(false) => entry.push_str(" (older commit)"),
                        None => {}
                    }
                }
                entry
            })
            .collect();
        lines.push(format!("latest reviews: {}", rendered.join("; ")));
    }
    let c = &r["checks"];
    let skipped = skipped_names(&c["items"]);
    let mut checks = if ["passed", "failed", "pending", "total"]
        .iter()
        .all(|key| c[*key].is_number())
    {
        let skipped_count = if skipped.is_empty() {
            String::new()
        } else {
            format!(", {} skipped", skipped.len())
        };
        format!(
            "checks: {} passed, {} failed, {} pending{skipped_count} (of {})",
            text(&c["passed"]),
            text(&c["failed"]),
            text(&c["pending"]),
            text(&c["total"])
        )
    } else {
        "checks: unknown (check results unavailable)".into()
    };
    if !skipped.is_empty() {
        let _ = write!(checks, "; skipped: {}", skipped.join(", "));
    }
    for (key, label) in [
        ("failingRequired", "failing required"),
        ("pendingRequired", "pending required"),
    ] {
        let values = names(&c[key]);
        if !values.is_empty() {
            let _ = write!(checks, "; {label}: {}", values.join(", "));
        }
    }
    if c["requiredKnown"] != true {
        checks.push_str(" (required-check flags unavailable)");
    }
    lines.push(checks);
    let stall = &r["checksNotStarted"];
    if let Some(head) = stall["headSha"].as_str() {
        let short: String = head.chars().take(8).collect();
        lines.push(format!(
            "checks never started on head {short} (none reported {}+ min after its commit) \
             — push again or rebase to trigger CI",
            text(&stall["thresholdMinutes"])
        ));
    }
    let threads = &r["threads"];
    let count = threads["unresolved"].as_i64().map_or_else(
        || "unknown (thread resolution state unreadable)".into(),
        |n| n.to_string(),
    );
    lines.push(if threads["resolutionRequired"] == true {
        format!("unresolved threads: {count} (resolution required to merge)")
    } else {
        format!("unresolved threads: {count}")
    });
    if r["hasConflicts"] == true {
        lines.push("merge conflicts present".into());
    }
    lines.push(
        match r["ancestry"]["behindBy"]
            .as_u64()
            .filter(|_| r["ancestry"]["status"] == "known")
        {
            Some(n) => {
                let suffix = if n == 1 { "" } else { "s" };
                let mut line = format!("branch ancestry: {n} commit{suffix} behind base");
                if n > 0 && ready {
                    line.push_str(" (behind but mergeable)");
                }
                line
            }
            None => "branch ancestry: unknown".into(),
        },
    );
    lines.push(
        match r["branchUpdateRequired"].as_bool() {
            Some(true) => "forge requires a branch update before merging",
            Some(false) => "forge branch-update requirement: not required",
            None => "forge branch-update requirement: unknown",
        }
        .into(),
    );
    if r["isInMergeQueue"] == true {
        lines.push("in merge queue".into());
    }
    if let Some(reason) = r["mergeBlockedReason"].as_str() {
        lines.push(format!("blocked: {reason}"));
    }
    if r["rulesKnown"] != true {
        lines.push("(branch rules unreadable — approval/thread requirements unknown)".into());
    }
    lines
        .into_iter()
        .map(|line| format!("- {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn changed(
    kind: &str,
    label: &str,
    changes: &[String],
    title: &str,
    url: &str,
    checklist: &str,
) -> String {
    let bullets = changes
        .iter()
        .map(|c| format!("- {c}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("[{kind} monitor {label}] {} change{} detected on \"{title}\" ({url}):\n{bullets}\n\nWhere the {kind} stands now:\n{checklist}", changes.len(), if changes.len() == 1 { "" } else { "s" })
}

pub(crate) fn terminal(
    kind: &str,
    label: &str,
    changes: &[String],
    title: &str,
    url: &str,
    merged: bool,
) -> String {
    let outcome = if merged {
        "was MERGED"
    } else {
        "was CLOSED without merging"
    };
    let mut body = format!("[{kind} monitor {label}] \"{title}\" {outcome} ({url}).\n\nMonitoring has STOPPED — this monitor is retired and will not report again.");
    if !changes.is_empty() {
        let bullets = changes
            .iter()
            .map(|c| format!("- {c}"))
            .collect::<Vec<_>>()
            .join("\n");
        let _ = write!(body, "\n\nChanges since the last report:\n{bullets}");
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_wakes_keep_shared_sentences_and_actual_forge_links() {
        let changes = vec!["state: open → merged".to_string()];
        let mr = terminal(
            "MR",
            "Team/Sub/App!7",
            &changes,
            "Review",
            "https://git.example:8443/gitlab/Team/Sub/App/-/merge_requests/7",
            true,
        );
        assert_eq!(
            mr,
            include_str!("review_wake_gitlab_completed.txt").trim_end()
        );
        let pr = terminal(
            "PR",
            "team/app#7",
            &changes,
            "Review",
            "https://github.com/team/app/pull/7",
            true,
        );
        assert_eq!(
            pr,
            mr.replace("MR", "PR")
                .replace("Team/Sub/App!7", "team/app#7")
                .replace(
                    "https://git.example:8443/gitlab/Team/Sub/App/-/merge_requests/7",
                    "https://github.com/team/app/pull/7"
                )
        );
        assert!(!mr.contains("github.com"));
        let checklist = checklist(
            &serde_json::json!({"state":"open","approvals":{},"checks":{},"threads":{}}),
            None,
            false,
        );
        let wake = changed(
            "MR",
            "Team/Sub/App!7",
            &changes,
            "Review",
            "https://git.example/actual",
            &checklist,
        );
        assert!(wake.contains("Where the MR stands now:"));
        assert!(wake.contains("checks: unknown"));
        assert!(
            wake.contains("- head: unknown (cannot tell which commit the checks below are for)")
        );
        assert!(wake.contains("unresolved threads: unknown"));
        assert!(!wake.contains("0 passed"));
        let known_head = super::checklist(
            &serde_json::json!({"state":"open","approvals":{},"checks":{},"threads":{}}),
            Some("7ad538b0c1d2e3f4"),
            false,
        );
        assert_eq!(
            known_head.lines().nth(1),
            Some("- head: 7ad538b0 (checks below are for this commit)")
        );
    }
}
