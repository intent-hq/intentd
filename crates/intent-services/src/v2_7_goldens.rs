//! Harness v2.7 golden fixtures. This version keeps the v2.6 doctrine and
//! text surfaces and adds one surface: the consolidated archive-watch notice
//! ([`crate::harness::Harness::workspace_archived_watches_cancelled_notice`]),
//! the single wake an agent reads after its workspace was unarchived. The
//! bytes are pinned here for every shape: hooks only, monitors only, both,
//! and the singular / plural forms.

use crate::harness::Harness;

const PREFIX: &str = "[SYSTEM NOTICE] This workspace was archived and has since been \
    unarchived. While it was archived, ";

const RE_ARM_HOOKS: &str = "ws.hook.get(hookId) recovers a hook's script for ws.hook.schedule";
const RE_ARM_MONITORS: &str = "ws.pr.monitor re-registers a PR";

fn v2_7() -> &'static dyn Harness {
    let entry = crate::harness::resolve_entry("2.7");
    assert_eq!(entry.version, "2.7");
    entry.harness
}

#[test]
fn v2_7_is_the_latest_harness() {
    assert_eq!(crate::harness::latest_entry().version, "2.7");
    assert_eq!(intent_core::CURRENT_HARNESS_VERSION, "2.7");
}

/// The Spec's target wording: two hooks and one PR monitor, plural form,
/// both re-arm pointers.
#[test]
fn golden_v2_7_archive_watches_notice_hooks_and_monitors() {
    let notice = v2_7().workspace_archived_watches_cancelled_notice(
        &[
            ("Wait for shipped alpha", "hook-abc"),
            ("CI watch", "hook-def"),
        ],
        &["intent-hq/intentd#123"],
    );
    assert_eq!(
        notice,
        format!(
            "{PREFIX}these background watches were cancelled and were NOT resumed: \
             hook \"Wait for shipped alpha\" (hook-abc), hook \"CI watch\" (hook-def), \
             PR monitor intent-hq/intentd#123. If a condition still matters, re-arm it: \
             {RE_ARM_HOOKS}; {RE_ARM_MONITORS}."
        )
    );
}

/// Hooks only: the PR-monitor section and its re-arm pointer are omitted.
#[test]
fn golden_v2_7_archive_watches_notice_hooks_only() {
    let notice = v2_7().workspace_archived_watches_cancelled_notice(
        &[
            ("Wait for shipped alpha", "hook-abc"),
            ("CI watch", "hook-def"),
        ],
        &[],
    );
    assert_eq!(
        notice,
        format!(
            "{PREFIX}these background watches were cancelled and were NOT resumed: \
             hook \"Wait for shipped alpha\" (hook-abc), hook \"CI watch\" (hook-def). \
             If a condition still matters, re-arm it: {RE_ARM_HOOKS}."
        )
    );
}

/// Monitors only: the hook section and its re-arm pointer are omitted.
#[test]
fn golden_v2_7_archive_watches_notice_monitors_only() {
    let notice = v2_7().workspace_archived_watches_cancelled_notice(
        &[],
        &["intent-hq/intentd#123", "intent-hq/cloudlands-fe#7"],
    );
    assert_eq!(
        notice,
        format!(
            "{PREFIX}these background watches were cancelled and were NOT resumed: \
             PR monitor intent-hq/intentd#123, PR monitor intent-hq/cloudlands-fe#7. \
             If a condition still matters, re-arm it: {RE_ARM_MONITORS}."
        )
    );
}

/// Exactly one cancelled item reads in the singular, for either kind.
#[test]
fn golden_v2_7_archive_watches_notice_singular() {
    let h = v2_7();
    assert_eq!(
        h.workspace_archived_watches_cancelled_notice(&[("CI watch", "hook-def")], &[]),
        format!(
            "{PREFIX}this background watch was cancelled and was NOT resumed: \
             hook \"CI watch\" (hook-def). If a condition still matters, re-arm it: \
             {RE_ARM_HOOKS}."
        )
    );
    assert_eq!(
        h.workspace_archived_watches_cancelled_notice(&[], &["intent-hq/intentd#123"]),
        format!(
            "{PREFIX}this background watch was cancelled and was NOT resumed: \
             PR monitor intent-hq/intentd#123. If a condition still matters, re-arm it: \
             {RE_ARM_MONITORS}."
        )
    );
}

/// The notice is the same bytes through `harness::latest()` (what the archive
/// sweep tail calls) as through the v2.7 registry row.
#[test]
fn v2_7_archive_watches_notice_is_what_latest_emits() {
    let hooks = [("CI watch", "hook-def")];
    let monitors = ["intent-hq/intentd#123"];
    assert_eq!(
        crate::harness::latest().workspace_archived_watches_cancelled_notice(&hooks, &monitors),
        v2_7().workspace_archived_watches_cancelled_notice(&hooks, &monitors)
    );
}
