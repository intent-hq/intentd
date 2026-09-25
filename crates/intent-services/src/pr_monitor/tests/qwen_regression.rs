//! intent#5881: replay captured checks through HTTP, the production adapter,
//! full service refresh, persisted snapshots, pending diffs and wake dispatch.
//! No affected-daemon snapshots were available. Error/path alternation and
//! reordered pages below are controlled experiments, not observed GitHub churn.

#[path = "../../../../intent-sourcecontrol/tests/support/qwen.rs"]
mod qwen;

use std::collections::BTreeMap;

use qwen::{CheckFault, MockQwen, ReadMode};

use super::*;

async fn row(svc: &Services, monitor: &PrMonitor) -> PrMonitor {
    svc.store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap()
}

fn check_map(m: &PrMonitor) -> BTreeMap<String, String> {
    let snapshot: PrMonitorSnapshot =
        serde_json::from_str(m.last_snapshot.as_deref().unwrap()).unwrap();
    snapshot
        .requirements
        .checks
        .items
        .into_iter()
        .map(|c| (c.name, c.status))
        .collect()
}

async fn full_poll(svc: &Services, mock: &MockQwen) {
    // Keep captured updatedAt and head unchanged. Expire the *full* refresh
    // age, not merely the short cache TTL, so this cannot pass by cache reuse.
    svc.backdate_pr_cache(PR_MONITOR_MAX_CHEAP_AGE + Duration::from_secs(1));
    let before = mock.calls("GetPrObservation");
    let rules = mock.calls("/rules/branches/");
    let runs = mock.calls("/check-runs");
    svc.poll_pr_monitors().await;
    assert!(
        mock.calls("GetPrObservation") > before,
        "must re-read the forge"
    );
    assert!(
        mock.calls("/rules/branches/") > rules || mock.calls("/check-runs") > runs,
        "must compose a full checklist, not reuse the cached one"
    );
}

async fn assert_quiet(svc: &Services, monitor: &PrMonitor, owner: &AgentId) {
    let current = row(svc, monitor).await;
    assert!(
        current.pending_changes.is_empty(),
        "unexpected changes: {:?}",
        current.pending_changes
    );
    assert!(current.last_change_at.is_none(), "{current:?}");
    let text = owner_messages(svc, owner).await;
    assert!(!text.contains("pr_monitor_wake"), "unexpected wake: {text}");
}

#[tokio::test]
async fn qwen_40_unchanged_full_refreshes_stay_quiet_across_read_paths() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    assert_eq!(baseline.len(), 32);
    assert_eq!(baseline["review-pr"], "passed");
    assert_eq!(baseline["Test (ubuntu-latest, Node 22.x)"], "failed");
    for mode in [
        ReadMode::Folded,
        ReadMode::Standalone,
        ReadMode::Rest,
        ReadMode::Folded,
    ] {
        mock.edit(|s| {
            s.mode = mode;
            s.nodes.reverse();
        });
        full_poll(&svc, &mock).await;
        assert_eq!(check_map(&row(&svc, &monitor).await), baseline, "{mode:?}");
        assert_quiet(&svc, &monitor, &owner).await;
    }
}

#[tokio::test]
async fn qwen_136_identical_first_page_refreshes_stay_quiet() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    // This is a stability control, not a completeness assertion. The separate
    // adapter regression must still fail while it only reads the first page.
    for mode in [ReadMode::Folded, ReadMode::Standalone, ReadMode::Folded] {
        mock.edit(|s| s.mode = mode);
        full_poll(&svc, &mock).await;
        assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
        assert_quiet(&svc, &monitor, &owner).await;
    }
}

#[tokio::test]
async fn qwen_136_reordered_page_boundaries_do_not_create_check_changes() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    // REST drains all 136 captured records; GraphQL's first page must not
    // erase checks, and reordering the same records must not change results.
    mock.edit(|s| s.mode = ReadMode::Rest);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    assert_eq!(baseline.len(), 35);
    let mut trace = Vec::new();
    for mode in [ReadMode::Folded, ReadMode::Standalone, ReadMode::Rest] {
        mock.edit(|s| {
            s.mode = mode;
            s.nodes.reverse();
        });
        full_poll(&svc, &mock).await;
        let current = row(&svc, &monitor).await;
        let check_changes: Vec<_> = current
            .pending_changes
            .iter()
            .filter(|c| c.starts_with("check ") || c.starts_with("all checks "))
            .collect();
        // The REST fallback also lacks mergeStateStatus. Isolate check
        // correctness from its unrelated unknown-to-BLOCKED readability line.
        if !check_changes.is_empty() || check_map(&current) != baseline {
            trace.push(format!(
                "{mode:?}: {check_changes:?}; {} names",
                check_map(&current).len()
            ));
        }
    }
    assert!(
        trace.is_empty(),
        "unchanged checks across page boundaries:\n{}",
        trace.join("\n")
    );
}

#[tokio::test]
async fn qwen_136_rotated_pages_do_not_repeat_passed_to_failed_wakes() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let captured = qwen::State::captured(11506).nodes;
    let mut trace = Vec::new();
    // Deliberate, UNOBSERVED page reordering: rotate by 40 so the first 100
    // include older cancelled route/Signal runs while their newer passing
    // runs fall on page two. Restore and repeat without changing any node.
    for offset in [40, 0, 40, 0] {
        mock.edit(|s| {
            s.nodes = captured.clone();
            s.nodes.rotate_left(offset);
        });
        full_poll(&svc, &mock).await;
        let current = row(&svc, &monitor).await;
        trace.push(format!("rotation {offset}: {:?}", current.pending_changes));
        if !current.pending_changes.is_empty() {
            assert_eq!(
                svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
                    .await
                    .unwrap()["flushed"],
                true
            );
        }
    }
    let text = owner_messages(&svc, &owner).await;
    assert!(
        !text.contains("passed → failed"),
        "synthetic page reordering must not fabricate failure wakes:\n{}",
        trace.join("\n")
    );
}

async fn degraded_checks_keep_last_observation(number: u64) {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(number).await;
    mock.edit(|s| s.mode = ReadMode::Rest);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", number)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    // Retain the complete before/degraded/recovered trace even when the quiet
    // expectation fails; this separates disappearance/reappearance from the
    // reporter's specific passed-to-failed symptom.
    let mut trace = Vec::new();
    for mode in [
        ReadMode::Degraded,
        ReadMode::Rest,
        ReadMode::Degraded,
        ReadMode::Rest,
    ] {
        mock.edit(|s| s.mode = mode);
        full_poll(&svc, &mock).await;
        let current = row(&svc, &monitor).await;
        let changes = current.pending_changes.clone();
        let checks = check_map(&current);
        assert_eq!(
            checks, baseline,
            "{mode:?}: preserve the intermediate checklist"
        );
        if !changes.is_empty() {
            let flushed = svc
                .pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
                .await
                .unwrap();
            assert_eq!(flushed["flushed"], true, "{flushed}");
        }
        trace.push(format!("{mode:?}: {} checks; {changes:?}", checks.len()));
    }
    let text = owner_messages(&svc, &owner).await;
    assert!(
        !text.contains("passed → failed"),
        "this experiment has no changed check outcomes: {text}"
    );
    assert!(
        !text.contains("pr_monitor_wake"),
        "unavailable check reads must not manufacture wakes; baseline {} checks; trace:\n{}",
        baseline.len(),
        trace.join("\n")
    );
    assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
}

#[tokio::test]
async fn qwen_40_degraded_checks_do_not_manufacture_repeated_wakes() {
    degraded_checks_keep_last_observation(10978).await;
}

#[tokio::test]
async fn qwen_136_degraded_checks_do_not_manufacture_repeated_wakes() {
    degraded_checks_keep_last_observation(11506).await;
}

#[tokio::test]
async fn qwen_newer_failure_recovery_and_required_changes_are_reported_once() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    for (conclusion, when, transition) in [
        (
            "FAILURE",
            "2026-09-25T06:00:00Z",
            "check review-pr: passed → failed",
        ),
        (
            "SUCCESS",
            "2026-09-25T06:01:00Z",
            "check review-pr: failed → passed",
        ),
    ] {
        mock.edit(|s| {
            let mut newer = s
                .nodes
                .iter()
                .find(|n| n["name"] == "review-pr")
                .unwrap()
                .clone();
            newer["conclusion"] = json!(conclusion);
            newer["startedAt"] = json!(when);
            s.nodes.push(newer);
        });
        full_poll(&svc, &mock).await;
        assert_eq!(
            row(&svc, &monitor)
                .await
                .pending_changes
                .iter()
                .filter(|c| *c == transition)
                .count(),
            1
        );
        assert_eq!(
            svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, true)
                .await
                .unwrap()["flushed"],
            true
        );
        mock.edit(|s| s.nodes.reverse());
        full_poll(&svc, &mock).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(
            owner_messages(&svc, &owner)
                .await
                .matches(transition)
                .count(),
            1
        );
    }
    mock.edit(|s| {
        for node in &mut s.nodes {
            if node["name"] == "review-pr" {
                node["isRequired"] = json!(true);
            }
        }
    });
    full_poll(&svc, &mock).await;
    let transition = "check review-pr is now required to merge";
    assert_eq!(row(&svc, &monitor).await.pending_changes, vec![transition]);
    assert_eq!(
        svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, true)
            .await
            .unwrap()["flushed"],
        true
    );
    full_poll(&svc, &mock).await;
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    assert_eq!(
        owner_messages(&svc, &owner)
            .await
            .matches(transition)
            .count(),
        1
    );
}

fn newer_run(mock: &MockQwen, conclusion: &str, when: &str) {
    mock.edit(|s| {
        let mut newer = s
            .nodes
            .iter()
            .find(|n| n["name"] == "review-pr")
            .unwrap()
            .clone();
        newer["conclusion"] = json!(conclusion);
        newer["startedAt"] = json!(when);
        s.nodes.push(newer);
    });
}

#[tokio::test]
async fn qwen_recovery_reports_a_real_failure_after_holding_intermediate_checks() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.mode = ReadMode::Rest);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    mock.edit(|s| s.mode = ReadMode::Degraded);
    newer_run(&mock, "FAILURE", "2026-09-25T06:00:00Z");
    full_poll(&svc, &mock).await;
    assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
    assert_quiet(&svc, &monitor, &owner).await;
    mock.edit(|s| s.mode = ReadMode::Rest);
    full_poll(&svc, &mock).await;
    let transition = "check review-pr: passed → failed";
    assert_eq!(row(&svc, &monitor).await.pending_changes, vec![transition]);
    svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
        .await
        .unwrap();
    full_poll(&svc, &mock).await;
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    assert_eq!(
        owner_messages(&svc, &owner)
            .await
            .matches(transition)
            .count(),
        1
    );
}

#[tokio::test]
async fn qwen_continuation_failure_preserves_checks_but_allows_unrelated_changes() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    mock.edit(|s| {
        s.fault = Some(CheckFault::ContinuationError);
        s.rest_unreadable = true;
        s.pr["state"] = json!("CLOSED");
    });
    full_poll(&svc, &mock).await;
    let current = row(&svc, &monitor).await;
    assert_eq!(check_map(&current), baseline);
    assert_eq!(current.state, PrMonitorState::Completed);
    let text = owner_messages(&svc, &owner).await;
    assert!(text.contains("CLOSED"), "{text}");
    assert!(!text.contains("check removed:"), "{text}");
}

#[tokio::test]
async fn qwen_successful_empty_read_removes_checks_once() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    mock.edit(|s| s.nodes.clear());
    full_poll(&svc, &mock).await;
    let current = row(&svc, &monitor).await;
    assert!(check_map(&current).is_empty());
    assert_eq!(
        current
            .pending_changes
            .iter()
            .filter(|c| c.starts_with("check removed:"))
            .count(),
        32
    );
    svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
        .await
        .unwrap();
    full_poll(&svc, &mock).await;
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
}

#[tokio::test]
async fn qwen_degraded_new_head_does_not_relabel_old_checks_or_remove_them() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.mode = ReadMode::Rest);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    mock.edit(|s| {
        s.mode = ReadMode::Degraded;
        s.pr["headRefOid"] = json!("new-head");
    });
    full_poll(&svc, &mock).await;
    let current = row(&svc, &monitor).await;
    let snapshot: PrMonitorSnapshot =
        serde_json::from_str(current.last_snapshot.as_deref().unwrap()).unwrap();
    assert_eq!(snapshot.head_sha.as_deref(), Some("new-head"));
    assert!(
        check_map(&current).is_empty(),
        "old checks cannot be presented as new-head results"
    );
    assert!(
        !current.pending_changes.is_empty(),
        "the push still reports"
    );
    assert!(
        current
            .pending_changes
            .iter()
            .all(|c| !c.starts_with("check ") && !c.starts_with("all checks ")),
        "{:?}",
        current.pending_changes
    );
}

#[tokio::test]
async fn qwen_initial_unreadable_checks_seed_silently_on_recovery() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.mode = ReadMode::Degraded);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    mock.edit(|s| s.mode = ReadMode::Rest);
    full_poll(&svc, &mock).await;
    assert_quiet(&svc, &monitor, &owner).await;
    assert_eq!(check_map(&row(&svc, &monitor).await).len(), 32);
    newer_run(&mock, "FAILURE", "2026-09-25T06:00:00Z");
    full_poll(&svc, &mock).await;
    assert_eq!(
        row(&svc, &monitor).await.pending_changes,
        vec!["check review-pr: passed → failed"]
    );
}

#[tokio::test]
async fn qwen_standalone_probe_never_uses_checks_from_another_head() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.mode = ReadMode::Standalone);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    mock.edit(|s| {
        s.rest_head = s.pr["headRefOid"].as_str().map(String::from);
        s.pr["headRefOid"] = json!("moved-between-rest-and-probe");
        s.rest_unreadable = true;
    });
    newer_run(&mock, "FAILURE", "2026-09-25T06:00:00Z");
    full_poll(&svc, &mock).await;
    assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
    assert_quiet(&svc, &monitor, &owner).await;
}

#[tokio::test]
async fn qwen_preserves_pending_failure_through_degradation_and_reregistration() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.mode = ReadMode::Rest);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    newer_run(&mock, "FAILURE", "2026-09-25T06:00:00Z");
    full_poll(&svc, &mock).await;
    let failing = row(&svc, &monitor).await;
    assert_eq!(
        failing.pending_changes,
        vec!["check review-pr: passed → failed"]
    );
    mock.edit(|s| s.mode = ReadMode::Degraded);
    for _ in 0..2 {
        full_poll(&svc, &mock).await;
        let held = row(&svc, &monitor).await;
        assert_eq!(held.pending_changes, failing.pending_changes);
        assert_eq!(held.pending_since, failing.pending_since);
        assert_eq!(held.last_change_at, failing.last_change_at);
        assert_eq!(check_map(&held), check_map(&failing));
    }
    assert_eq!(
        svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
            .await
            .unwrap()["flushed"],
        true
    );
    mock.edit(|s| s.mode = ReadMode::Rest);
    full_poll(&svc, &mock).await;
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    assert_eq!(
        owner_messages(&svc, &owner)
            .await
            .matches("check review-pr: passed → failed")
            .count(),
        1
    );
    svc.backdate_pr_cache(PR_MONITOR_MAX_CHEAP_AGE + Duration::from_secs(1));
    let (rearmed, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 10978)
        .await
        .unwrap();
    assert_eq!(rearmed.monitor_id, monitor.monitor_id);
    full_poll(&svc, &mock).await;
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    assert_eq!(check_map(&rearmed), check_map(&failing));
}

#[tokio::test]
async fn qwen_paginated_legacy_status_and_lone_cancellation_remain_failures() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.nodes.push(json!({"__typename":"StatusContext", "context":"route", "state":"FAILURE", "isRequired":true, "targetUrl":"https://ci/status"}));
        s.nodes.push(json!({"__typename":"CheckRun", "name":"cancelled-only", "status":"COMPLETED", "conclusion":"CANCELLED", "startedAt":"2026-09-25T06:00:00Z"}));
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    assert_eq!(
        baseline["route"], "failed",
        "independent legacy failure wins over a successful run"
    );
    assert_eq!(baseline["cancelled-only"], "failed");
    mock.edit(|s| s.nodes.reverse());
    full_poll(&svc, &mock).await;
    assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
    assert_quiet(&svc, &monitor, &owner).await;
}
