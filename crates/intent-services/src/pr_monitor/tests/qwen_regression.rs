//! intent#5881: replay captured checks through HTTP, the production adapter,
//! full service refresh, persisted snapshots, pending diffs and wake dispatch.
//! No affected-daemon snapshots were available. Error/path alternation and
//! reordered pages below are controlled experiments, not observed GitHub churn.

#[path = "../../../../intent-sourcecontrol/tests/support/qwen.rs"]
mod qwen;

use std::collections::BTreeMap;

use qwen::{MockQwen, ReadMode};

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
