use serde_json::Value;

/// Private comparison evidence, never a current checklist. A degraded read
/// cannot retract an already observed event. Checks remain tied to their head.
pub(super) fn observed(previous: &Value, fresh: &Value) -> Value {
    let mut result = fresh.clone();
    for path in [
        "/headSha",
        "/isDraft",
        "/requirements/approvals/have",
        "/requirements/approvals/decision",
        "/requirements/approvals/changesRequested",
        "/requirements/approvals/needed",
        "/comments/conversationCount",
        "/comments/reviewCommentCount",
        "/requirements/mergeable",
        "/requirements/hasConflicts",
    ] {
        if let Some(current) = result.pointer_mut(path) {
            if current.is_null() {
                if let Some(old) = previous.pointer(path).filter(|value| !value.is_null()) {
                    *current = old.clone();
                }
            }
        }
    }
    if fresh["requirements"]["checks"]["items"].is_null()
        && previous["headSha"] == result["headSha"]
        && !previous["headSha"].is_null()
    {
        result["requirements"]["checks"] = previous["requirements"]["checks"].clone();
    }
    result
}

/// Compare only observed facts. Nullable qualified values never pass through
/// legacy non-null checklist defaults. Ordering and sentences mirror GitHub.
pub(super) fn diff(old: &Value, new: &Value) -> Vec<String> {
    if old.is_null() || new.is_null() {
        return Vec::new();
    }
    let mut changes = Vec::new();
    if let (Some(before), Some(after)) = (old["state"].as_str(), new["state"].as_str()) {
        if before != after && after != "unknown" {
            changes.push(format!("state: {before} → {after}"));
        }
    }
    if let (Some(before), Some(after)) = (old["isDraft"].as_bool(), new["isDraft"].as_bool()) {
        if before != after {
            changes.push(
                if after {
                    "marked as draft"
                } else {
                    "marked ready for review"
                }
                .into(),
            );
        }
    }
    if let Some(head) = new["headSha"].as_str().filter(|head| !head.is_empty()) {
        if old["headSha"].as_str() != Some(head) {
            changes.push(format!(
                "new commits pushed (head is now {})",
                head.chars().take(8).collect::<String>()
            ));
        }
    }
    let (before, after) = (
        &old["requirements"]["approvals"],
        &new["requirements"]["approvals"],
    );
    if let (Some(a), Some(b)) = (before["decision"].as_str(), after["decision"].as_str()) {
        if a != b {
            changes.push(format!("review decision: {a} → {b}"));
        }
    }
    if let (Some(a), Some(b)) = (before["have"].as_i64(), after["have"].as_i64()) {
        if a != b {
            changes.push(format!(
                "{} ({a} → {b} approving)",
                if b > a {
                    "new approval"
                } else {
                    "approval withdrawn"
                }
            ));
        }
    }
    for (key, label) in [
        ("changesRequested", "changes-requested reviews"),
        ("needed", "required approvals"),
    ] {
        if let (Some(a), Some(b)) = (before[key].as_i64(), after[key].as_i64()) {
            if a != b {
                changes.push(format!("{label}: {a} → {b}"));
            }
        }
    }
    for (key, label) in [
        ("conversationCount", "conversation"),
        ("reviewCommentCount", "review"),
    ] {
        if let (Some(a), Some(b)) = (old["comments"][key].as_i64(), new["comments"][key].as_i64()) {
            if a != b {
                let delta = b - a;
                changes.push(format!(
                    "{delta:+} {label} comment{} ({b} total)",
                    if delta.abs() == 1 { "" } else { "s" }
                ));
            }
        }
    }
    match (
        old["requirements"]["threads"]["unresolved"].as_i64(),
        new["requirements"]["threads"]["unresolved"].as_i64(),
    ) {
        (Some(a), Some(b)) if a != b => changes.push(format!(
            "{}: {a} → {b} unresolved",
            if b < a {
                "thread(s) resolved"
            } else {
                "thread(s) unresolved/opened"
            }
        )),
        (Some(_), None) => {
            changes.push("review threads unreadable (resolution state unavailable)".into());
        }
        (None, Some(b)) => changes.push(format!("review threads readable again: {b} unresolved")),
        _ => {}
    }
    changes.extend(diff_checks(old, new));
    let before = &old["requirements"];
    let after = &new["requirements"];
    let known = after["mergeable"].is_boolean()
        && after["mergeStateStatus"]
            .as_str()
            .is_some_and(|s| !matches!(s, "UNKNOWN" | "checking" | "unchecked"));
    if let Some(conflicts) = after["hasConflicts"].as_bool() {
        if before["hasConflicts"].as_bool() != Some(conflicts) && (conflicts || known) {
            changes.push(
                if conflicts {
                    "merge conflicts appeared"
                } else {
                    "merge conflicts resolved"
                }
                .into(),
            );
        }
    }
    if before["mergeable"] != after["mergeable"] && after["mergeable"].is_boolean() {
        changes.push(format!(
            "mergeable: {} → {}",
            describe(&before["mergeable"]),
            after["mergeable"]
        ));
    }
    if before["mergeStateStatus"] != after["mergeStateStatus"] && known {
        changes.push(format!(
            "merge state: {} → {}",
            describe(&before["mergeStateStatus"]),
            describe(&after["mergeStateStatus"])
        ));
    }
    if before["mergeBlockedReason"] != after["mergeBlockedReason"]
        && (after["mergeBlockedReason"].is_string() || known)
    {
        changes.push(after["mergeBlockedReason"].as_str().map_or_else(
            || "merge is no longer blocked".into(),
            |s| format!("merge blocked: {s}"),
        ));
    }
    changes
}

fn describe(value: &Value) -> String {
    match value {
        Value::Null => "unknown".into(),
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

fn diff_checks(old: &Value, new: &Value) -> Vec<String> {
    let before = &old["requirements"]["checks"];
    let after = &new["requirements"]["checks"];
    let Some(items) = after["items"].as_array() else {
        return vec![];
    };
    // The first observable checks seed silently on the registration head,
    // matching GitHub. A new head's newly observable failures remain news.
    if before["items"].is_null() && old["headSha"] == new["headSha"] {
        return vec![];
    }
    let empty = Vec::new();
    let previous = before["items"].as_array().unwrap_or(&empty);
    let mut changes = Vec::new();
    for item in items {
        let (Some(name), Some(status)) = (item["name"].as_str(), item["status"].as_str()) else {
            continue;
        };
        if let Some(prior) = previous.iter().find(|p| p["name"] == name) {
            if let Some(was) = prior["status"].as_str() {
                if was != status && !(was == "pending" && matches!(status, "passed" | "skipped")) {
                    changes.push(format!("check {name}: {was} → {status}"));
                }
            }
            if before["requiredKnown"] == true && after["requiredKnown"] == true {
                if let (Some(a), Some(b)) =
                    (prior["required"].as_bool(), item["required"].as_bool())
                {
                    if a != b {
                        changes.push(format!(
                            "check {name} is {} required to merge",
                            if b { "now" } else { "no longer" }
                        ));
                    }
                }
            }
        } else if !matches!(status, "passed" | "skipped") {
            changes.push(format!("check started: {name} ({status})"));
        }
    }
    for prior in previous {
        if let Some(name) = prior["name"].as_str() {
            if !items.iter().any(|item| item["name"] == name) {
                changes.push(format!("check removed: {name}"));
            }
        }
    }
    if before["pending"].as_i64().is_none_or(|n| n > 0) && after["pending"] == 0 {
        if let (Some(total), Some(passed), Some(failed)) = (
            after["total"].as_i64(),
            after["passed"].as_i64(),
            after["failed"].as_i64(),
        ) {
            if total > 0 {
                let skipped = crate::harness::review_wake::skipped_names(&after["items"]);
                changes.push(crate::harness::review_wake::checks_completed(
                    total, passed, failed, &skipped,
                ));
            }
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(status: &str) -> Value {
        json!({"state":"open","isDraft":false,"headSha":"abc", "requirements":{
            "checks":{"items":[{"name":"build","status":status,"required":true}],"requiredKnown":true,
                "total":1,"passed":i64::from(status == "passed"),"failed":i64::from(status == "failed"),"pending":i64::from(status == "pending")}}})
    }

    #[test]
    fn qualified_diff_suppresses_unchanged_and_intermediate_success() {
        let pending = snapshot("pending");
        assert!(diff(&pending, &pending).is_empty());
        let mut before = pending.clone();
        before["requirements"]["checks"]["pending"] = json!(2);
        let mut after = snapshot("passed");
        after["requirements"]["checks"]["pending"] = json!(1);
        assert!(diff(&before, &after).is_empty());
        assert_eq!(
            diff(&pending, &snapshot("passed")),
            vec!["all checks passed (1)"]
        );
        assert_eq!(
            diff(&snapshot("failed"), &snapshot("passed")),
            vec!["check build: failed → passed"]
        );
    }

    #[test]
    fn qualified_diff_completion_names_skipped_checks() {
        let item = |name: &str, status: &str| json!({"name":name,"status":status,"required":true});
        let with = |items: Vec<Value>, passed: i64, skipped: i64, pending: i64| {
            json!({"state":"open","isDraft":false,"headSha":"abc","requirements":{"checks":{
                "items":items,"requiredKnown":true,"total":3,"passed":passed,"skipped":skipped,
                "failed":0,"pending":pending}}})
        };
        let before = with(
            vec![
                item("build", "pending"),
                item("coverage-all", "pending"),
                item("coverage-e2e", "pending"),
            ],
            0,
            0,
            3,
        );
        let after = with(
            vec![
                item("build", "passed"),
                item("coverage-all", "skipped"),
                item("coverage-e2e", "skipped"),
            ],
            1,
            2,
            0,
        );
        assert_eq!(
            diff(&before, &after),
            vec!["all checks completed: 1 passed, 2 skipped (coverage-all, coverage-e2e)"]
        );
    }

    #[test]
    fn qualified_diff_restricted_checks_never_report_success() {
        let before = snapshot("pending");
        let mut restricted = before.clone();
        restricted["requirements"]["checks"] = json!({"items":null,"total":null,"pending":null,"failed":null,"passed":null,"requiredKnown":false});
        assert!(diff(&before, &restricted).is_empty());
        assert!(diff(&restricted, &snapshot("passed")).is_empty());
        let mut new_head = snapshot("failed");
        new_head["headSha"] = json!("newhead");
        assert_eq!(
            diff(&restricted, &new_head),
            vec![
                "new commits pushed (head is now newhead)",
                "check started: build (failed)",
                "all checks completed: 0 passed, 1 failed"
            ]
        );
    }

    #[test]
    fn qualified_diff_threads_report_availability_without_zero() {
        let mut before = snapshot("passed");
        before["requirements"]["threads"] = json!({"unresolved":2});
        let mut after = before.clone();
        after["requirements"]["threads"] = json!({});
        assert_eq!(
            diff(&before, &after),
            vec!["review threads unreadable (resolution state unavailable)"]
        );
        assert_eq!(
            diff(&after, &before),
            vec!["review threads readable again: 2 unresolved"]
        );
    }

    #[test]
    fn qualified_diff_retained_checks_cannot_cross_heads_or_change_public_unknowns() {
        let previous = snapshot("failed");
        let mut fresh = snapshot("pending");
        fresh["requirements"]["checks"] = json!({"items":null,"total":null,"passed":null,"failed":null,"pending":null,"requiredKnown":false});
        let retained = observed(&previous, &fresh);
        assert!(fresh["requirements"]["checks"]["items"].is_null());
        assert_eq!(retained["requirements"]["checks"]["failed"], 1);
        fresh["headSha"] = json!("new-head");
        let next = observed(&previous, &fresh);
        assert!(next["requirements"]["checks"]["items"].is_null());
    }
}
