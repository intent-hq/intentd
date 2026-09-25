//! Harness v2.7 golden fixtures. This version keeps the v2.6 doctrine and
//! text surfaces and adds one surface: the consolidated archive-watch notice
//! ([`crate::harness::Harness::workspace_archived_watches_cancelled_notice`]),
//! the single wake an agent reads after its workspace was unarchived. The
//! bytes are pinned here for every shape: hooks only, monitors only, both,
//! and the singular / plural forms — plus the sweep-level pin: the exact
//! wake `workspace.archive` delivers to an owner (moved here from the v1
//! per-item archive goldens; the sweeps emit through `harness::latest()`).

use intent_core::{AgentId, HookState, PrMonitorState, WorkspaceApi};
use serde_json::json;

use crate::harness::Harness;
use crate::v1_goldens::{pr_monitor_row, seed_agent, setup, wake_texts_when};

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
fn v2_7_remains_registered() {
    assert_eq!(crate::harness::resolve_entry("2.7").version, "2.7");
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

/// Archive-sweep notice: exact wake bytes `workspace.archive` delivers to
/// the owner of one hook and one PR monitor — ONE consolidated wake (no
/// per-item notices), tagged `workspace_archive_wake` with both id arrays.
/// Store-only wake here: no manager attached, so nothing can spawn a turn.
#[intent_test_macros::daemon_test]
async fn golden_archive_sweep_consolidated_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-arch");
    seed_agent(&svc, &ws, &owner).await;
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "sweeper",
                "code": "return { dispatch: false };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
    let hook_id = out["hook"]["hookId"].as_str().expect("hookId").to_string();
    let mut monitor = pr_monitor_row();
    monitor.workspace_id = ws.clone();
    monitor.agent_id = owner.clone();
    assert!(svc
        .store()
        .insert_pr_monitor(&monitor)
        .await
        .expect("insert"));

    svc.archive_workspace(ws.clone(), None)
        .await
        .expect("archive");

    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![format!(
            "{PREFIX}these background watches were cancelled and were NOT resumed: \
             hook \"sweeper\" ({hook_id}), PR monitor o/r#42. If a condition still \
             matters, re-arm it: {RE_ARM_HOOKS}; {RE_ARM_MONITORS}."
        )]
    );
    let session = svc
        .store()
        .get_agent_session(&owner)
        .await
        .expect("session");
    assert_eq!(
        session.messages[0].metadata,
        Some(json!({
            "type": "workspace_archive_wake",
            "hookIds": [hook_id],
            "prMonitorIds": ["prmon-1"],
        }))
    );
}

/// Regression: archiving a workspace where agent A owns two hooks and one
/// PR monitor queues exactly ONE wake for A — naming all three items and
/// carrying every id in the metadata — while agent B, with no active
/// watches, receives nothing. The per-item cancels still persist
/// (`cancelled` rows) without their retired per-item notices.
#[intent_test_macros::daemon_test]
async fn archive_sweep_queues_one_consolidated_wake_per_affected_agent() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-a");
    let bystander = AgentId::from("agent-b");
    seed_agent(&svc, &ws, &owner).await;
    seed_agent(&svc, &ws, &bystander).await;
    let mut hook_ids = Vec::new();
    for name in ["Wait for shipped alpha", "CI watch"] {
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": name,
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        hook_ids.push(out["hook"]["hookId"].as_str().expect("hookId").to_string());
    }
    let mut monitor = pr_monitor_row();
    monitor.workspace_id = ws.clone();
    monitor.agent_id = owner.clone();
    assert!(svc
        .store()
        .insert_pr_monitor(&monitor)
        .await
        .expect("insert"));

    svc.archive_workspace(ws.clone(), None)
        .await
        .expect("archive");

    // Every watch is cancelled (state persisted), none resurrected.
    for id in &hook_ids {
        let row = svc
            .store()
            .get_hook(&intent_core::HookId::from(id.as_str()))
            .await
            .expect("hook row");
        assert_eq!(row.state, HookState::Cancelled, "{id}");
    }
    let row = svc
        .store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .expect("monitor row");
    assert_eq!(row.state, PrMonitorState::Cancelled);

    // Exactly one wake for A, naming all three items...
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts.len(),
        1,
        "one consolidated wake, no per-item notices: {texts:?}"
    );
    let text = &texts[0];
    assert!(text.starts_with(PREFIX), "{text}");
    assert!(
        text.contains("these background watches were cancelled and were NOT resumed"),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "hook \"Wait for shipped alpha\" ({})",
            hook_ids[0]
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!("hook \"CI watch\" ({})", hook_ids[1])),
        "{text}"
    );
    assert!(text.contains("PR monitor o/r#42"), "{text}");
    assert!(
        !text.contains("[Background hook"),
        "no per-item framing: {text}"
    );
    assert!(!text.contains("[PR monitor"), "no per-item framing: {text}");
    // ...with every id in the metadata (hook order follows the sweep's
    // `created_at` listing; compare as sets).
    let session = svc
        .store()
        .get_agent_session(&owner)
        .await
        .expect("session");
    let metadata = session.messages[0].metadata.clone().expect("wake metadata");
    assert_eq!(metadata["type"], json!("workspace_archive_wake"));
    let mut got_hook_ids: Vec<String> = metadata["hookIds"]
        .as_array()
        .expect("hookIds array")
        .iter()
        .map(|v| v.as_str().expect("hook id").to_string())
        .collect();
    got_hook_ids.sort();
    let mut want_hook_ids = hook_ids.clone();
    want_hook_ids.sort();
    assert_eq!(got_hook_ids, want_hook_ids);
    assert_eq!(metadata["prMonitorIds"], json!(["prmon-1"]));

    // B owned nothing the sweep touched: no wake at all.
    let bystander_session = svc
        .store()
        .get_agent_session(&bystander)
        .await
        .expect("bystander session");
    assert!(
        bystander_session.messages.is_empty(),
        "no wake for an agent with no cancelled watches: {:?}",
        bystander_session.messages
    );
}
