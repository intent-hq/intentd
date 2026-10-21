//! Chain selection for automatic quota failover (PROTOCOL §5.54 Backup
//! failover): same base provider only, saved order, the failed reference and
//! every reference already tried excluded, disabled base skipped.

use std::collections::BTreeMap;

use super::*;

fn profile(base: &str, id: &str, backup: bool) -> ProfileMeta {
    ProfileMeta {
        id: id.to_owned(),
        base_provider: base.to_owned(),
        name: id.to_uppercase(),
        created_at: "2026-10-21T00:00:00.000Z".to_owned(),
        updated_at: "2026-10-21T00:00:00.000Z".to_owned(),
        backup,
    }
}

/// A saved list mixing both bases, backups and non-backups, in saved order.
fn saved() -> Vec<ProfileMeta> {
    vec![
        profile("claude-code", "pwork", false),
        profile("codex", "pcx1", true),
        profile("claude-code", "pb1", true),
        profile("claude-code", "pb2", true),
        profile("codex", "pcx2", true),
    ]
}

fn tried(refs: &[&str]) -> Vec<String> {
    refs.iter().map(|r| (*r).to_owned()).collect()
}

#[test]
fn default_fails_over_to_the_first_backup_of_its_base_in_saved_order() {
    assert_eq!(
        next_backup("claude-code", &[], &saved(), None).as_deref(),
        Some("claude-code@pb1")
    );
    assert_eq!(
        next_backup("codex", &[], &saved(), None).as_deref(),
        Some("codex@pcx1")
    );
}

#[test]
fn a_profile_fails_over_to_the_next_untried_backup_skipping_itself() {
    // The failed reference counts as tried even when the chain is empty.
    assert_eq!(
        next_backup("claude-code@pb1", &[], &saved(), None).as_deref(),
        Some("claude-code@pb2")
    );
    // A non-backup profile can be the current reference too.
    assert_eq!(
        next_backup("claude-code@pwork", &[], &saved(), None).as_deref(),
        Some("claude-code@pb1")
    );
}

#[test]
fn the_chain_walks_saved_order_and_ends_when_every_backup_was_tried() {
    let profiles = saved();
    let mut chain = tried(&["claude-code"]);
    let mut current = "claude-code".to_owned();
    let mut walked = Vec::new();
    while let Some(next) = next_backup(&current, &chain, &profiles, None) {
        chain.push(next.clone());
        walked.push(next.clone());
        current = next;
        assert!(walked.len() <= profiles.len(), "the chain must not loop");
    }
    assert_eq!(walked, tried(&["claude-code@pb1", "claude-code@pb2"]));
}

#[test]
fn an_earlier_backup_already_tried_this_turn_is_never_picked_again() {
    // pb2 failed after pb1 had already failed: nothing is left, even though
    // pb1 precedes pb2 in saved order.
    assert_eq!(
        next_backup(
            "claude-code@pb2",
            &tried(&["claude-code", "claude-code@pb1", "claude-code@pb2"]),
            &saved(),
            None
        ),
        None
    );
}

#[test]
fn no_backups_or_only_other_base_backups_is_a_no_op() {
    let none = vec![
        profile("claude-code", "pwork", false),
        profile("codex", "pcx1", true),
    ];
    assert_eq!(next_backup("claude-code", &[], &none, None), None);
    assert_eq!(next_backup("claude-code", &[], &[], None), None);
}

#[test]
fn a_disabled_base_provider_has_no_backup() {
    let enabled = BTreeMap::from([("claude-code".to_owned(), false)]);
    assert_eq!(
        next_backup("claude-code", &[], &saved(), Some(&enabled)),
        None
    );
    // Disabling another base leaves this one untouched.
    let enabled = BTreeMap::from([("codex".to_owned(), false)]);
    assert_eq!(
        next_backup("claude-code", &[], &saved(), Some(&enabled)).as_deref(),
        Some("claude-code@pb1")
    );
}

#[test]
fn providers_without_profiles_and_malformed_references_never_fail_over() {
    assert_eq!(next_backup("auggie", &[], &saved(), None), None);
    assert_eq!(next_backup("mock@pb1", &[], &saved(), None), None);
}

#[test]
fn notice_text_names_both_configurations_by_label() {
    assert_eq!(
        failover_notice_text("claude-code", "claude-code@pb1", &saved()),
        "claude-code ran out of quota. Switched to the backup claude-code (PB1)."
    );
}

#[test]
fn the_swallowed_attempt_marker_is_prefix_anchored() {
    let marked = intent_core::Error::Internal(format!("{PROMPT_QUOTA_FAILOVER_PREFIX} 429"));
    assert!(is_quota_failover_error(&marked));
    let mentioned = intent_core::Error::Internal(format!("boom: {PROMPT_QUOTA_FAILOVER_PREFIX}"));
    assert!(!is_quota_failover_error(&mentioned));
}

#[test]
fn the_failure_belongs_to_the_configuration_that_ran() {
    assert_eq!(
        failed_attempt_selection(Some("claude-code"), Some("claude-code")).as_deref(),
        Some("claude-code")
    );
    assert_eq!(
        failed_attempt_selection(Some("claude-code@pb1"), Some("claude-code@pb1")).as_deref(),
        Some("claude-code@pb1")
    );
    // The user switched (to a profile, or between profiles) mid-attempt.
    assert_eq!(
        failed_attempt_selection(Some("claude-code@pb1"), Some("claude-code")),
        None
    );
    assert_eq!(
        failed_attempt_selection(Some("claude-code"), Some("claude-code@pb1")),
        None
    );
    // Unattributable failures never fail over.
    assert_eq!(failed_attempt_selection(Some("claude-code"), None), None);
    assert_eq!(failed_attempt_selection(None, Some("claude-code")), None);
}

fn hop(from: &str, to: &str) -> serde_json::Value {
    serde_json::json!({ "type": "provider_failover", "fromProvider": from, "toProvider": to })
}

#[test]
fn a_single_failover_hop_announces_the_switch() {
    let notices = [hop("claude-code", "claude-code@pb1")];
    assert!(failover_chain_announces(
        &notices,
        "claude-code",
        "claude-code@pb1"
    ));
    assert!(!failover_chain_announces(
        &notices,
        "claude-code",
        "claude-code@pb2"
    ));
    assert!(!failover_chain_announces(
        &notices,
        "claude-code@pb2",
        "claude-code@pb1"
    ));
}

#[test]
fn a_multi_hop_chain_in_one_turn_announces_the_whole_switch() {
    // Committed on Default; pb1's session setup also ran out; pb2 succeeded.
    let notices = [
        hop("claude-code@pb1", "claude-code@pb2"),
        hop("claude-code", "claude-code@pb1"),
    ];
    assert!(failover_chain_announces(
        &notices,
        "claude-code",
        "claude-code@pb2"
    ));
    // The last hop alone is also a valid (shorter) announcement.
    assert!(failover_chain_announces(
        &notices,
        "claude-code@pb1",
        "claude-code@pb2"
    ));
}

#[test]
fn any_other_identity_row_breaks_the_chain() {
    let manual = serde_json::json!({
        "type": "model_changed", "fromProvider": "claude-code@pb1", "toProvider": "claude-code@pwork",
    });
    let notices = [
        hop("claude-code@pwork", "claude-code@pb2"),
        manual,
        hop("claude-code", "claude-code@pb1"),
    ];
    assert!(!failover_chain_announces(
        &notices,
        "claude-code",
        "claude-code@pb2"
    ));
    // A gap between hops (the chain does not connect) is not an announcement.
    let gap = [
        hop("claude-code@pb3", "claude-code@pb2"),
        hop("claude-code", "claude-code@pb1"),
    ];
    assert!(!failover_chain_announces(
        &gap,
        "claude-code",
        "claude-code@pb2"
    ));
    assert!(!failover_chain_announces(
        &[],
        "claude-code",
        "claude-code@pb1"
    ));
}

fn message(role: &str, content: serde_json::Value) -> intent_core::AgentMessage {
    intent_core::AgentMessage {
        id: uuid::Uuid::new_v4().to_string(),
        agent_id: intent_core::AgentId::from("agent-cut-short"),
        seq: 0,
        role: role.to_owned(),
        content,
        metadata: None,
        app_message_id: None,
        author: None,
        created_at: "2026-10-21T00:00:00.000Z".to_owned(),
    }
}

fn tool_use(call_id: &str, name: &str, command: &str, status: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "tool_use", "id": format!("m:{call_id}"), "name": name,
        "input": { "command": command }, "toolCallId": call_id,
        "metadata": { "toolKind": "execute", "status": status },
    })
}

/// Every tool call of the cut-short turn is listed — with an output, a
/// completed call that printed nothing, and one only started — even though
/// the ordinary history sanitizer would drop the last two (review
/// intentd#2581).
#[test]
fn the_cut_short_turn_lists_every_tool_call() {
    let current = [
        message(
            "user",
            serde_json::json!([{ "type": "text", "text": "migrate the db" }]),
        ),
        message(
            "assistant",
            serde_json::json!([
                { "type": "text", "text": "Starting." },
                tool_use("c1", "Bash", "pg_dump prod", "completed"),
                { "type": "tool_result", "id": "m:r1", "tool_use_id": "c1", "output": "dumped", "is_error": false },
                tool_use("c2", "Bash", "rm -rf build-cache", "completed"),
                tool_use("c3", "Edit", "schema.sql", "in_progress"),
            ]),
        ),
    ];
    let out = crate::history_xml::format_cut_short_turn(&[], &current, 200_000, 2_000);
    assert!(out.contains("<text>migrate the db</text>"), "{out}");
    assert!(out.contains("<text>Starting.</text>"), "{out}");
    for (name, command, status, result) in [
        (
            "Bash",
            "pg_dump prod",
            "completed",
            "<result>dumped</result>",
        ),
        (
            "Bash",
            "rm -rf build-cache",
            "completed",
            "<result>(no output)</result>",
        ),
        (
            "Edit",
            "schema.sql",
            "started",
            "<result>(no result recorded; it may have run)</result>",
        ),
    ] {
        assert!(out.contains(command), "{command}: {out}");
        assert!(
            out.contains(&format!("name=\"{name}\" status=\"{status}\"")),
            "{command}: {out}"
        );
        assert!(out.contains(result), "{command}: {out}");
    }
}

/// An oversized cut-short turn is never dropped: older history goes first,
/// then the partial work is compacted behind an explicit marker, while the
/// user request stays whole and every tool call stays listed with its
/// argument summary (review intentd#2581).
#[test]
fn an_oversized_cut_short_turn_keeps_the_request_and_every_tool_call() {
    let budget = 20_000;
    let prior = [
        message(
            "user",
            serde_json::json!([{ "type": "text", "text": "OLD QUESTION" }]),
        ),
        message(
            "assistant",
            serde_json::json!([{ "type": "text", "text": "OLD ANSWER ".repeat(500) }]),
        ),
    ];
    let request = format!("REQUEST-START {} REQUEST-END", "r".repeat(12_000));
    let current = [
        message(
            "user",
            serde_json::json!([{ "type": "text", "text": request }]),
        ),
        message(
            "assistant",
            serde_json::json!([
                { "type": "text", "text": format!("PARTIAL-START {}", "p".repeat(30_000)) },
                tool_use("c1", "Bash", "deploy --prod", "completed"),
                { "type": "tool_result", "id": "m:r1", "tool_use_id": "c1", "output": "o".repeat(30_000), "is_error": false },
                tool_use("c2", "Write", "config.yaml", "completed"),
            ]),
        ),
    ];
    let out = crate::history_xml::format_cut_short_turn(&prior, &current, budget, 2_000);
    assert!(
        out.contains("REQUEST-START") && out.contains("REQUEST-END"),
        "request kept whole"
    );
    assert!(out.contains(&"r".repeat(12_000)), "request kept whole");
    assert!(
        out.contains("deploy --prod") && out.contains("config.yaml"),
        "{out}"
    );
    assert!(
        out.contains("name=\"Bash\"") && out.contains("name=\"Write\""),
        "{out}"
    );
    assert!(out.contains("partial work truncated"), "explicit marker");
    assert!(
        out.contains("PARTIAL-START"),
        "the start of the partial text is kept"
    );
    assert!(
        !out.contains("OLD ANSWER"),
        "older history is dropped first"
    );
    assert!(
        out.contains("earlier conversation omitted"),
        "{}",
        &out[..200]
    );
    assert!(
        out.len() <= budget + 200,
        "stays near the budget: {}",
        out.len()
    );
}

/// Within budget, older history is kept ahead of the cut-short turn.
#[test]
fn a_cut_short_turn_within_budget_keeps_older_history() {
    let prior = [
        message(
            "user",
            serde_json::json!([{ "type": "text", "text": "OLD QUESTION" }]),
        ),
        message(
            "assistant",
            serde_json::json!([{ "type": "text", "text": "OLD ANSWER" }]),
        ),
    ];
    let current = [
        message(
            "user",
            serde_json::json!([{ "type": "text", "text": "new request" }]),
        ),
        message(
            "assistant",
            serde_json::json!([tool_use("c1", "Bash", "ls", "completed")]),
        ),
    ];
    let out = crate::history_xml::format_cut_short_turn(&prior, &current, 200_000, 2_000);
    let old = out.find("OLD ANSWER").expect("older history kept");
    let turn = out.find("<current_turn_cut_short>").expect("turn section");
    assert!(old < turn, "history comes first");
    assert!(!out.contains("partial work truncated"), "{out}");
}
