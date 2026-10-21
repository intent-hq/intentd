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
