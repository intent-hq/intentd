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

pub(crate) fn checklist(r: &Value, ready: bool) -> String {
    let mut lines = vec![format!("state: {}", text(&r["state"]))];
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
    let c = &r["checks"];
    let mut checks = if ["passed", "failed", "pending", "total"]
        .iter()
        .all(|key| c[*key].is_number())
    {
        format!(
            "checks: {} passed, {} failed, {} pending (of {})",
            text(&c["passed"]),
            text(&c["failed"]),
            text(&c["pending"]),
            text(&c["total"])
        )
    } else {
        "checks: unknown (check results unavailable)".into()
    };
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
