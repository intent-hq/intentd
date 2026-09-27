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

async fn poll_after_debounce(svc: &Services, mock: &MockQwen, monitor: &PrMonitor) {
    let current = row(svc, monitor).await;
    assert!(svc
        .store()
        .update_pr_monitor_poll(
            &monitor.monitor_id,
            intent_store::PrMonitorPollUpdate {
                last_snapshot: current.last_snapshot.as_deref(),
                baseline_snapshot: current.baseline_snapshot.as_deref(),
                pending_changes: &current.pending_changes,
                pending_since: Some("2020-01-01T00:00:00Z"),
                last_change_at: Some("2020-01-01T00:00:00Z"),
                last_polled_at: current.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &current.updated_at,
            },
        )
        .await
        .unwrap());
    full_poll(svc, mock).await;
}

fn legacy_failures(mock: &MockQwen) {
    mock.edit(|s| {
        s.pr["updatedAt"] = json!("");
        for name in ["route", "legacy-only"] {
            s.nodes
                .push(json!({"__typename":"StatusContext", "context":name,
                "state":"FAILURE", "isRequired":true, "targetUrl":"https://ci/status"}));
        }
    });
}

#[tokio::test]
async fn qwen_legacy_fallback_preserves_independent_status_and_required_evidence() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    legacy_failures(&mock);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline: PrMonitorSnapshot =
        serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
    assert_eq!(check_map(&monitor)["route"], "failed");
    assert!(baseline
        .requirements
        .checks
        .failing_required
        .contains(&"legacy-only".into()));
    let mut violations = Vec::new();
    for mode in [
        ReadMode::Rest,
        ReadMode::Folded,
        ReadMode::Rest,
        ReadMode::Folded,
    ] {
        mock.edit(|s| s.mode = mode);
        full_poll(&svc, &mock).await;
        let current = row(&svc, &monitor).await;
        let snapshot: PrMonitorSnapshot =
            serde_json::from_str(current.last_snapshot.as_deref().unwrap()).unwrap();
        if !current.pending_changes.is_empty()
            || snapshot.requirements.checks != baseline.requirements.checks
        {
            violations.push(format!(
                "{mode:?}: pending={:?}, checks={:?}",
                current.pending_changes, snapshot.requirements.checks
            ));
        }
        poll_after_debounce(&svc, &mock, &monitor).await;
    }
    let messages = owner_messages(&svc, &owner).await;
    assert!(
        violations.is_empty() && !messages.contains("pr_monitor_wake"),
        "R1: {violations:#?}\nDelivered: {messages}"
    );
    assert_quiet(&svc, &monitor, &owner).await;
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.pr["headRefOid"] = json!("new-head");
    });
    full_poll(&svc, &mock).await;
    let fresh = row(&svc, &monitor).await;
    assert_eq!(check_map(&fresh)["route"], "passed");
    assert!(
        !check_map(&fresh).contains_key("legacy-only"),
        "old-head status evidence must not move to a new head"
    );
}

#[tokio::test]
async fn qwen_recovery_after_delivered_push_reports_new_head_failure_once() {
    post_push_recovery("FAILURE", "check started: route (failed)").await;
}

#[tokio::test]
async fn qwen_recovery_after_delivered_push_reports_new_head_completion_once() {
    post_push_recovery("SUCCESS", "all checks passed (35)").await;
}

async fn post_push_recovery(conclusion: &str, expected: &str) {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.pr["updatedAt"] = json!("");
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    assert_eq!(check_map(&monitor)["route"], "passed");
    mock.edit(|s| {
        s.mode = ReadMode::Degraded;
        s.pr["headRefOid"] = json!("new-head");
    });
    full_poll(&svc, &mock).await;
    let unreadable = row(&svc, &monitor).await;
    assert!(
        check_map(&unreadable).is_empty(),
        "old-head checks must not follow the push"
    );
    assert!(!unreadable.pending_changes.is_empty());
    assert!(unreadable
        .pending_changes
        .iter()
        .all(|c| !c.starts_with("check ")));
    poll_after_debounce(&svc, &mock, &monitor).await;
    let pushed = owner_messages(&svc, &owner).await;
    assert!(
        pushed.contains("pr_monitor_wake"),
        "push must be delivered before recovery"
    );
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        if conclusion == "SUCCESS" {
            for node in &mut s.nodes {
                node["conclusion"] = json!("SUCCESS");
            }
        }
        let mut newer = s
            .nodes
            .iter()
            .find(|n| n["name"] == "route")
            .unwrap()
            .clone();
        newer["conclusion"] = json!(conclusion);
        newer["startedAt"] = json!("2026-09-25T06:00:00Z");
        s.nodes.push(newer);
    });
    full_poll(&svc, &mock).await;
    let recovered = row(&svc, &monitor).await;
    assert_eq!(
        check_map(&recovered)["route"],
        if conclusion == "FAILURE" {
            "failed"
        } else {
            "passed"
        }
    );
    poll_after_debounce(&svc, &mock, &monitor).await;
    let delivered = owner_messages(&svc, &owner).await;
    assert_eq!(
        delivered.matches(expected).count(),
        1,
        "R2: recovered pending={:?}\npush={pushed}\ndelivered={delivered}",
        recovered.pending_changes
    );
    assert_eq!(
        svc.store()
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .len(),
        2,
        "one push wake and one recovered-check wake"
    );
    for _ in 0..2 {
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
}

#[tokio::test]
async fn qwen_partial_fallback_reports_fresh_runs_and_legacy_recovery_once() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    legacy_failures(&mock);
    mock.edit(|s| {
        // Keep merge-state availability equal on both paths so this control
        // isolates the independent check evidence after a real REST wake.
        s.mode = ReadMode::Standalone;
        s.pr["mergeStateStatus"] = json!(null);
        s.nodes
            .push(json!({"__typename":"CheckRun", "name":"observed-run",
        "status":"COMPLETED", "conclusion":"SUCCESS", "isRequired":true,
        "startedAt":"2026-09-25T05:00:00Z"}));
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        let run = s
            .nodes
            .iter_mut()
            .find(|n| n["name"] == "observed-run")
            .unwrap();
        run["conclusion"] = json!("FAILURE");
        run["startedAt"] = json!("2026-09-25T06:00:00Z");
    });
    full_poll(&svc, &mock).await;
    let failed = row(&svc, &monitor).await;
    assert_eq!(
        failed.pending_changes,
        vec!["check observed-run: passed → failed"]
    );
    let snapshot: PrMonitorSnapshot =
        serde_json::from_str(failed.last_snapshot.as_deref().unwrap()).unwrap();
    assert!(snapshot.requirements.checks.required_known);
    for name in ["route", "legacy-only", "observed-run"] {
        assert!(snapshot
            .requirements
            .checks
            .failing_required
            .contains(&name.into()));
    }
    poll_after_debounce(&svc, &mock, &monitor).await;
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        for status in s
            .nodes
            .iter_mut()
            .filter(|n| n["__typename"] == "StatusContext")
        {
            status["state"] = json!("SUCCESS");
        }
    });
    full_poll(&svc, &mock).await;
    for expected in [
        "check route: failed → passed",
        "check legacy-only: failed → passed",
    ] {
        assert!(row(&svc, &monitor)
            .await
            .pending_changes
            .iter()
            .any(|c| c == expected));
    }
    poll_after_debounce(&svc, &mock, &monitor).await;
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        let run = s
            .nodes
            .iter_mut()
            .find(|n| n["name"] == "observed-run")
            .unwrap();
        run["conclusion"] = json!("SUCCESS");
        run["startedAt"] = json!("2026-09-25T06:01:00Z");
    });
    full_poll(&svc, &mock).await;
    assert_eq!(
        row(&svc, &monitor).await.pending_changes,
        vec!["check observed-run: failed → passed"]
    );
    poll_after_debounce(&svc, &mock, &monitor).await;
    let delivered = owner_messages(&svc, &owner).await;
    for transition in [
        "check observed-run: passed → failed",
        "check observed-run: failed → passed",
        "check route: failed → passed",
        "check legacy-only: failed → passed",
    ] {
        assert_eq!(delivered.matches(transition).count(), 1, "{delivered}");
    }
    for mode in [ReadMode::Rest, ReadMode::Standalone] {
        mock.edit(|s| s.mode = mode);
        full_poll(&svc, &mock).await;
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
}

#[tokio::test]
async fn qwen_old_snapshot_rest_failure_and_recovery_each_deliver_once() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.pr["updatedAt"] = json!("");
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    assert_eq!(check_map(&monitor)["route"], "passed");
    let mut old: serde_json::Value =
        serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
    for field in [
        "checksUnobserved",
        "checksSeedPending",
        "statusChecks",
        "requiredCheckNames",
    ] {
        old.as_object_mut().unwrap().remove(field);
    }
    let old = serde_json::to_string(&old).unwrap();
    assert!(svc
        .store()
        .update_pr_monitor_poll(
            &monitor.monitor_id,
            intent_store::PrMonitorPollUpdate {
                last_snapshot: Some(&old),
                baseline_snapshot: Some(&old),
                pending_changes: &[],
                pending_since: None,
                last_change_at: None,
                last_polled_at: monitor.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &monitor.updated_at,
            },
        )
        .await
        .unwrap());
    for (conclusion, when, expected) in [
        ("FAILURE", "2026-09-25T06:00:00Z", "failed"),
        ("SUCCESS", "2026-09-25T06:01:00Z", "passed"),
    ] {
        mock.edit(|s| {
            let mut run = s
                .nodes
                .iter()
                .find(|n| n["name"] == "route")
                .unwrap()
                .clone();
            run["conclusion"] = json!(conclusion);
            run["startedAt"] = json!(when);
            s.nodes.push(run);
        });
        full_poll(&svc, &mock).await;
        poll_after_debounce(&svc, &mock, &monitor).await;
        let current = row(&svc, &monitor).await;
        let messages = owner_messages(&svc, &owner).await;
        assert_eq!(check_map(&current)["route"], expected, "R3: {messages}");
        for saved in [
            current.last_snapshot.as_deref(),
            current.baseline_snapshot.as_deref(),
        ] {
            let saved: serde_json::Value = serde_json::from_str(saved.unwrap()).unwrap();
            let original: serde_json::Value = serde_json::from_str(&old).unwrap();
            assert_eq!(
                saved["statusChecks"],
                original["requirements"]["checks"]["items"]
            );
        }
        assert!(current.pending_changes.is_empty());
    }
    let delivered = owner_messages(&svc, &owner).await;
    for transition in [
        "check route: passed → failed",
        "check route: failed → passed",
    ] {
        assert_eq!(delivered.matches(transition).count(), 1, "R3: {delivered}");
    }
    assert_eq!(
        svc.store()
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .len(),
        2
    );
    for _ in 0..2 {
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
}

#[tokio::test]
async fn qwen_known_required_flip_survives_delivered_new_rest_check() {
    required_flip_after_rest_check("route", true).await;
}

#[tokio::test]
async fn qwen_unknown_required_flag_seeds_silently_then_reports_a_real_flip() {
    required_flip_after_rest_check("fresh-run", false).await;
}

async fn required_flip_after_rest_check(name: &str, was_known: bool) {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        s.pr["updatedAt"] = json!("");
        s.pr["mergeStateStatus"] = json!(null);
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let registered: PrMonitorSnapshot =
        serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
    assert!(registered.requirements.checks.required_known);
    assert!(
        !registered
            .requirements
            .checks
            .items
            .iter()
            .find(|c| c.name == "route")
            .unwrap()
            .required
    );
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.nodes
            .push(json!({"__typename":"CheckRun", "name":"fresh-run",
            "status":"COMPLETED", "conclusion":"FAILURE", "isRequired":false,
            "startedAt":"2026-09-25T06:00:00Z"}));
    });
    full_poll(&svc, &mock).await;
    poll_after_debounce(&svc, &mock, &monitor).await;
    let first = owner_messages(&svc, &owner).await;
    assert_eq!(
        first.matches("check started: fresh-run (failed)").count(),
        1
    );
    assert!(row(&svc, &monitor).await.pending_changes.is_empty());
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        for run in s.nodes.iter_mut().filter(|n| n["name"] == name) {
            run["isRequired"] = json!(true);
        }
    });
    full_poll(&svc, &mock).await;
    poll_after_debounce(&svc, &mock, &monitor).await;
    let delivered = owner_messages(&svc, &owner).await;
    let expected = format!("check {name} is now required to merge");
    assert_eq!(
        delivered.matches(&expected).count(),
        usize::from(was_known),
        "R4: {delivered}"
    );
    assert_eq!(
        svc.store()
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .len(),
        if was_known { 2 } else { 1 }
    );
    for mode in [ReadMode::Rest, ReadMode::Standalone] {
        mock.edit(|s| s.mode = mode);
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
    if !was_known {
        // No wake advanced the emit anchor when the unknown flag became known.
        // Its next genuine change must still be compared with that learned value.
        mock.edit(|s| {
            for run in s.nodes.iter_mut().filter(|n| n["name"] == name) {
                run["isRequired"] = json!(false);
            }
        });
        full_poll(&svc, &mock).await;
        poll_after_debounce(&svc, &mock, &monitor).await;
        let delivered = owner_messages(&svc, &owner).await;
        assert_eq!(
            delivered
                .matches("check fresh-run is no longer required to merge")
                .count(),
            1
        );
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.pr["headRefOid"] = json!("new-head");
    });
    full_poll(&svc, &mock).await;
    let changed = row(&svc, &monitor).await;
    let snapshot: PrMonitorSnapshot =
        serde_json::from_str(changed.last_snapshot.as_deref().unwrap()).unwrap();
    assert!(snapshot.known_required_checks().is_empty());
    assert!(snapshot
        .requirements
        .checks
        .items
        .iter()
        .all(|c| !c.required));
    assert!(!changed
        .pending_changes
        .iter()
        .any(|c| c.contains("required to merge")));
}

#[tokio::test]
async fn qwen_silent_passing_discovery_reports_later_required_flip_once() {
    silent_passing_discovery_required_flip(false).await;
}

#[tokio::test]
async fn qwen_silent_discovery_repairs_missing_emitted_evidence_from_last_poll() {
    silent_passing_discovery_required_flip(true).await;
}

async fn silent_passing_discovery_required_flip(missing_emitted_evidence: bool) {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        s.pr["updatedAt"] = json!("");
        s.pr["mergeStateStatus"] = json!(null);
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    assert_quiet(&svc, &monitor, &owner).await;
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.nodes
            .push(json!({"__typename":"CheckRun", "name":"fresh-green",
            "status":"COMPLETED", "conclusion":"SUCCESS", "isRequired":false,
            "startedAt":"2026-09-25T06:00:00Z"}));
    });
    full_poll(&svc, &mock).await;
    poll_after_debounce(&svc, &mock, &monitor).await;
    assert_quiet(&svc, &monitor, &owner).await;
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        s.nodes.last_mut().unwrap()["isRequired"] = json!(true);
    });
    full_poll(&svc, &mock).await;
    poll_after_debounce(&svc, &mock, &monitor).await;
    assert_quiet(&svc, &monitor, &owner).await;
    let learned = row(&svc, &monitor).await;
    if missing_emitted_evidence {
        // A monitor persisted before this repair can already know the flag in
        // its last poll while the emitted anchor still lacks the passing name.
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                intent_store::PrMonitorPollUpdate {
                    last_snapshot: learned.last_snapshot.as_deref(),
                    baseline_snapshot: monitor.baseline_snapshot.as_deref(),
                    pending_changes: &[],
                    pending_since: None,
                    last_change_at: None,
                    last_polled_at: learned.last_polled_at.as_deref(),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &learned.updated_at,
                },
            )
            .await
            .unwrap());
    }
    mock.edit(|s| s.nodes.last_mut().unwrap()["isRequired"] = json!(false));
    full_poll(&svc, &mock).await;
    poll_after_debounce(&svc, &mock, &monitor).await;
    let delivered = owner_messages(&svc, &owner).await;
    assert_eq!(
        delivered
            .matches("check fresh-green is no longer required to merge")
            .count(),
        1,
        "R5: no earlier wake may populate the comparison baseline: {delivered}"
    );
    assert_eq!(
        svc.store()
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    for saved in [learned.last_snapshot, learned.baseline_snapshot] {
        let snapshot: PrMonitorSnapshot = serde_json::from_str(saved.as_deref().unwrap()).unwrap();
        let check = snapshot
            .requirements
            .checks
            .items
            .iter()
            .find(|c| c.name == "fresh-green")
            .expect("persist silent discovery in both anchors");
        assert_eq!(check.status, "passed");
        assert!(check.required);
        assert!(snapshot.known_required_checks().contains("fresh-green"));
    }
    for mode in [ReadMode::Rest, ReadMode::Standalone] {
        mock.edit(|s| s.mode = mode);
        poll_after_debounce(&svc, &mock, &monitor).await;
        let current = row(&svc, &monitor).await;
        assert!(current.pending_changes.is_empty());
        assert!(current.last_change_at.is_none());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
}

#[tokio::test]
async fn qwen_silent_discovery_and_learning_preserve_mixed_pending_changes() {
    silent_discovery_with_pending_changes("passed").await;
}

#[tokio::test]
async fn qwen_silent_discovery_failure_before_learning_preserves_pending_changes() {
    silent_discovery_with_pending_changes("failed").await;
}

#[tokio::test]
async fn qwen_silent_discovery_removal_before_learning_preserves_pending_changes() {
    silent_discovery_with_pending_changes("removed").await;
}

async fn silent_discovery_with_pending_changes(outcome: &str) {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        s.pr["updatedAt"] = json!("");
        s.pr["mergeStateStatus"] = json!(null);
        s.nodes
            .push(json!({"__typename":"CheckRun", "name":"removed-old",
            "status":"COMPLETED", "conclusion":"SUCCESS", "isRequired":false,
            "startedAt":"2026-09-25T06:00:00Z"}));
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    mock.edit(|s| {
        s.mode = ReadMode::Rest;
        s.nodes.retain(|n| n["name"] != "removed-old");
        for (name, conclusion) in [("route", "FAILURE"), ("fresh-green", "SUCCESS")] {
            s.nodes.push(json!({"__typename":"CheckRun", "name":name,
                "status":"COMPLETED", "conclusion":conclusion, "isRequired":false,
                "startedAt":"2026-09-25T06:00:00Z"}));
        }
    });
    full_poll(&svc, &mock).await;
    let discovered = row(&svc, &monitor).await;
    let mut expected = vec!["check route: passed → failed", "check removed: removed-old"];
    assert_eq!(discovered.pending_changes, expected);
    let emitted: PrMonitorSnapshot =
        serde_json::from_str(discovered.baseline_snapshot.as_deref().unwrap()).unwrap();
    let green = emitted
        .requirements
        .checks
        .items
        .iter()
        .find(|c| c.name == "fresh-green")
        .unwrap();
    assert_eq!(green.status, "passed");
    assert!(!emitted.known_required_checks().contains("fresh-green"));

    // The newly discovered name can change again before a complete read learns
    // its flag. That must not erase its original passing comparison evidence.
    mock.edit(|s| match outcome {
        "failed" => s.nodes.last_mut().unwrap()["conclusion"] = json!("FAILURE"),
        "removed" => s.nodes.retain(|n| n["name"] != "fresh-green"),
        _ => {}
    });
    full_poll(&svc, &mock).await;
    match outcome {
        "failed" => expected.push("check fresh-green: passed → failed"),
        "removed" => expected.push("check removed: fresh-green"),
        _ => {}
    }
    let pending = row(&svc, &monitor).await;
    for change in &expected {
        assert!(
            pending.pending_changes.iter().any(|c| c == change),
            "{pending:?}"
        );
    }
    mock.edit(|s| {
        s.mode = ReadMode::Standalone;
        s.pr["mergeStateStatus"] = json!("BEHIND");
        for run in s.nodes.iter_mut().filter(|n| n["name"] == "fresh-green") {
            run["isRequired"] = json!(true);
        }
    });
    full_poll(&svc, &mock).await;
    let learned = row(&svc, &monitor).await;
    for change in &expected {
        assert!(
            learned.pending_changes.iter().any(|c| c == change),
            "{learned:?}"
        );
    }
    assert!(learned
        .pending_changes
        .iter()
        .any(|c| c == "branch is now behind its base"));
    assert!(!learned
        .pending_changes
        .iter()
        .any(|c| c.contains("required to merge")));
    assert_eq!(learned.pending_since, discovered.pending_since);
    assert!(!owner_messages(&svc, &owner)
        .await
        .contains("pr_monitor_wake"));

    // A second silent discovery must leave the full coalesced set (including
    // non-check changes) untouched, even as a now-known flag really changes.
    mock.edit(|s| {
        for run in s.nodes.iter_mut().filter(|n| n["name"] == "fresh-green") {
            run["isRequired"] = json!(false);
        }
        s.nodes
            .push(json!({"__typename":"CheckRun", "name":"later-green",
            "status":"COMPLETED", "conclusion":"SUCCESS", "isRequired":true,
            "startedAt":"2026-09-25T06:01:00Z"}));
    });
    full_poll(&svc, &mock).await;
    let pending = row(&svc, &monitor).await;
    let mut expected = learned.pending_changes;
    if outcome != "removed" {
        expected.push("check fresh-green is no longer required to merge".into());
    }
    expected.sort();
    let mut actual = pending.pending_changes.clone();
    actual.sort();
    assert_eq!(actual, expected);
    assert_eq!(pending.pending_since, discovered.pending_since);
    assert!(!owner_messages(&svc, &owner)
        .await
        .contains("pr_monitor_wake"));
    poll_after_debounce(&svc, &mock, &monitor).await;
    let delivered = owner_messages(&svc, &owner).await;
    for change in expected {
        assert_eq!(delivered.matches(&change).count(), 1, "{delivered}");
    }
    assert_eq!(
        svc.store()
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    for _ in 0..2 {
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert!(row(&svc, &monitor).await.pending_changes.is_empty());
        assert_eq!(owner_messages(&svc, &owner).await, delivered);
    }
}

#[test]
fn qwen_old_legacy_subset_stays_fixed_through_new_runs_and_head_changes() {
    let original = snapshot(|s| {
        s.status_checks = None;
        s.requirements.checks = pr_ops::MergeRequirementsChecks::from_items(
            [
                ("route", "failed", true),
                ("legacy-only", "failed", true),
                ("fresh-run", "passed", false),
            ]
            .into_iter()
            .map(|(name, status, required)| pr_ops::MergeRequirementCheck {
                name: name.into(),
                status: status.into(),
                required,
                url: None,
            })
            .collect(),
            true,
        );
    });
    let mut previous = original.clone();
    for status in ["failed", "passed", "failed", "passed"] {
        let mut rest = shared_from(&snapshot(|_| {}), false);
        rest.status_checks = None;
        rest.requirements.checks = pr_ops::MergeRequirementsChecks::from_items(
            [("route", "passed"), ("fresh-run", status)]
                .into_iter()
                .map(|(name, status)| pr_ops::MergeRequirementCheck {
                    name: name.into(),
                    status: status.into(),
                    required: false,
                    url: None,
                })
                .collect(),
            false,
        );
        let fresh = rest.materialize(Some(&previous));
        assert_eq!(
            fresh.status_checks.as_ref().unwrap(),
            &original.requirements.checks.items
        );
        let by_name: BTreeMap<_, _> = fresh
            .requirements
            .checks
            .items
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        assert_eq!(by_name["fresh-run"].status, status);
        for name in ["route", "legacy-only"] {
            assert_eq!(by_name[name].status, "failed");
            assert!(by_name[name].required);
        }
        previous = serde_json::from_str(&serde_json::to_string(&fresh).unwrap()).unwrap();
        rest.head_sha = Some("new-head".into());
        let new_head = rest.materialize(Some(&previous));
        assert!(new_head.status_checks.as_ref().unwrap().is_empty());
        assert!(new_head.known_required_checks().is_empty());
        assert_eq!(new_head.requirements.checks.items[0].status, "passed");
        assert!(new_head
            .requirements
            .checks
            .items
            .iter()
            .all(|c| !c.required && c.name != "legacy-only"));
    }
    let mut complete = shared_from(&snapshot(|_| {}), true);
    complete.requirements.checks = pr_ops::MergeRequirementsChecks::from_items(Vec::new(), true);
    let cleared = complete.materialize(Some(&previous));
    assert!(cleared.requirements.checks.items.is_empty());
    assert!(cleared.status_checks.as_ref().unwrap().is_empty());
    assert!(cleared.known_required_checks().is_empty());
}

#[tokio::test]
async fn qwen_older_snapshot_preserves_unknown_status_provenance_until_full_read() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(11506).await;
    legacy_failures(&mock);
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "QwenLM", "qwen-code", 11506)
        .await
        .unwrap();
    let baseline = check_map(&monitor);
    let mut old: serde_json::Value =
        serde_json::from_str(monitor.last_snapshot.as_deref().unwrap()).unwrap();
    for field in [
        "checksUnobserved",
        "checksSeedPending",
        "statusChecks",
        "requiredCheckNames",
    ] {
        old.as_object_mut().unwrap().remove(field);
    }
    let old = serde_json::to_string(&old).unwrap();
    assert!(svc
        .store()
        .update_pr_monitor_poll(
            &monitor.monitor_id,
            intent_store::PrMonitorPollUpdate {
                last_snapshot: Some(&old),
                baseline_snapshot: Some(&old),
                pending_changes: &[],
                pending_since: None,
                last_change_at: None,
                last_polled_at: monitor.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &monitor.updated_at,
            }
        )
        .await
        .unwrap());
    for mode in [ReadMode::Rest, ReadMode::Rest, ReadMode::Folded] {
        mock.edit(|s| s.mode = mode);
        full_poll(&svc, &mock).await;
        poll_after_debounce(&svc, &mock, &monitor).await;
        assert_eq!(check_map(&row(&svc, &monitor).await), baseline);
        assert_quiet(&svc, &monitor, &owner).await;
    }
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
