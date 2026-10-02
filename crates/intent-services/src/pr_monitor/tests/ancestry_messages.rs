//! Ancestry is informational; only the forge can require a branch update.
use super::*;
use intent_sourcecontrol::PrAncestry;

fn known(behind_by: u64) -> PrAncestry {
    PrAncestry::Known {
        base_sha: "a".repeat(40),
        head_sha: "b".repeat(40),
        behind_by,
    }
}

fn ready() -> PrMonitorSnapshot {
    snapshot(|s| {
        ready_requirements(&mut s.requirements);
        s.requirements.branch_update_required = Some(false);
        s.requirements.ancestry = known(1);
    })
}

fn row(s: &PrMonitorSnapshot) -> PrMonitor {
    let mut row = crate::v1_goldens::pr_monitor_row();
    row.last_snapshot = Some(serde_json::to_string(s).unwrap());
    row
}

#[test]
fn ancestry_message_readiness_preserves_all_other_gates() {
    let base = ready();
    assert!(requirements_ready(&base.requirements));
    assert!(render_checklist(&base).contains("behind but mergeable"));
    for block in 0..14 {
        let mut blocked = base.clone();
        let r = &mut blocked.requirements;
        match block {
            0 => r.branch_update_required = Some(true),
            1 => r.branch_update_required = None,
            2 => r.has_conflicts = true,
            3 => r.checks.failing_required.push("ci".into()),
            4 => r.checks.pending_required.push("ci".into()),
            5 => r.approvals.decision = "review_required".into(),
            6 => r.approvals.decision = "changes_requested".into(),
            7 => r.threads.unresolved = Some(1),
            8 => r.threads.unresolved = None,
            9 => r.mergeable = None,
            10 => r.merge_state_status = Some("UNKNOWN".into()),
            11 => r.is_in_merge_queue = Some(true),
            12 => r.is_draft = true,
            13 => r.merge_blocked_reason = Some("signed commits required".into()),
            _ => unreachable!(),
        }
        assert!(!requirements_ready(r), "blocker {block}");
        let text = render_checklist(&blocked);
        assert!(
            !text.contains("behind but mergeable"),
            "blocker {block}: {text}"
        );
        assert!(text.contains("branch ancestry: 1 commit behind base"));
    }
    for ancestry in [known(0), PrAncestry::Unknown] {
        let mut s = base.clone();
        s.requirements.ancestry = ancestry;
        assert!(requirements_ready(&s.requirements));
        s.requirements.branch_update_required = Some(true);
        assert!(!requirements_ready(&s.requirements));
        assert!(render_checklist(&s).contains("forge requires a branch update before merging"));
    }
}

#[test]
fn ancestry_message_transitions_never_invent_an_update_or_clear() {
    let mut old = ready();
    old.requirements.ancestry = PrAncestry::Unknown;
    old.requirements.branch_update_required = None;
    let first = ready();
    assert_eq!(
        diff_snapshots(&old, &first),
        vec![
            "branch ancestry available: 1 commit behind base",
            "forge branch-update requirement available: not required",
        ]
    );
    let mut moved = first.clone();
    moved.requirements.ancestry = known(2);
    assert_eq!(
        diff_snapshots(&first, &moved),
        vec!["branch ancestry: 1 → 2 commits behind base"]
    );
    let mut current = moved.clone();
    current.requirements.ancestry = known(0);
    assert_eq!(
        diff_snapshots(&moved, &current),
        vec!["branch ancestry: 2 → 0 commits behind base"]
    );
    assert_eq!(
        diff_snapshots(&first, &old),
        vec![
            "branch ancestry unavailable",
            "forge branch-update requirement unknown",
        ]
    );
    let mut required = first.clone();
    required.requirements.branch_update_required = Some(true);
    required.requirements.is_behind = true;
    assert_eq!(
        diff_snapshots(&first, &required),
        vec!["forge now requires a branch update before merging"]
    );
    assert_eq!(
        diff_snapshots(&required, &first),
        vec!["forge no longer requires a branch update before merging"]
    );
    assert_eq!(
        diff_snapshots(&required, &old),
        vec![
            "branch ancestry unavailable",
            "forge branch-update requirement unknown",
        ]
    );
    let mut other_pair = first.clone();
    other_pair.requirements.ancestry = PrAncestry::Known {
        base_sha: "c".repeat(40),
        head_sha: "b".repeat(40),
        behind_by: 1,
    };
    assert!(
        diff_snapshots(&first, &other_pair).is_empty(),
        "same count is not a change to readiness"
    );
}

#[test]
fn ancestry_message_summary_and_old_baseline_are_presence_safe() {
    for ancestry in [known(0), known(1), PrAncestry::Unknown] {
        for update in [Some(true), Some(false), None] {
            let mut s = ready();
            s.requirements.ancestry = ancestry.clone();
            s.requirements.branch_update_required = update;
            let wire = pr_monitor_wire(&row(&s), None);
            let full = serde_json::to_value(&s.requirements).unwrap();
            assert_eq!(wire["lastSnapshot"]["ancestry"], full["ancestry"]);
            assert_eq!(
                wire["lastSnapshot"].get("branchUpdateRequired"),
                full.get("branchUpdateRequired")
            );
            assert_eq!(wire["lastSnapshot"]["isBehind"], false);
        }
    }
    let mut old = serde_json::to_value(ready()).unwrap();
    old["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("ancestry");
    old["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("branchUpdateRequired");
    let decoded: PrMonitorSnapshot = serde_json::from_value(old).unwrap();
    let wire = pr_monitor_wire(&row(&decoded), None);
    assert_eq!(
        wire["lastSnapshot"]["ancestry"],
        json!({"status":"unknown"})
    );
    assert!(wire["lastSnapshot"].get("branchUpdateRequired").is_none());
    assert!(!requirements_ready(&decoded.requirements));
    let text = render_checklist(&decoded);
    assert!(text.contains("branch ancestry: unknown"));
    assert!(text.contains("forge branch-update requirement: unknown"));
    assert!(!text.contains("behind but mergeable"));
}

#[intent_test_macros::daemon_test]
async fn ancestry_message_service_projections_and_event_deltas_agree() {
    use super::qwen_regression::qwen::MockQwen;
    use intent_core::WorkspaceApi;

    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| {
        s.pr["baseRef"] = json!({"target":{"oid":"a".repeat(40)}});
        s.pr["mergeStateStatus"] = json!("CLEAN");
        s.compare = json!({"behind_by":1});
    });
    let svc = svc
        .with_source_control(mock.sc.clone())
        .with_pr_monitor_debounce_seconds(3600);
    let started = svc
        .pr_monitor_start_op(&ws, &owner, 10978, Some("o/r".into()))
        .await
        .unwrap();
    let full = &started["requirements"];
    assert_eq!(full["ancestry"]["behindBy"], 1);
    assert_eq!(full["branchUpdateRequired"], false);
    assert_eq!(full["isBehind"], false);
    let snapshot = svc
        .pr_state(ws.clone(), 10978, Some("o/r".into()))
        .await
        .unwrap();
    let listed = svc.pr_monitor_list_op(&ws, Some(&owner)).await.unwrap();
    for reduced in [
        &started["monitor"]["lastSnapshot"],
        &listed["monitors"][0]["lastSnapshot"],
        &snapshot["requirements"],
    ] {
        for field in ["ancestry", "branchUpdateRequired", "isBehind"] {
            assert_eq!(reduced[field], full[field], "{field}");
        }
    }
    // Only the base moves. A measured lag creates a neutral lifecycle event,
    // re-fetchable summary and wake; it cannot invent a branch-update blocker.
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!("c".repeat(40));
        s.compare = json!({"behind_by":2});
    });
    svc.poll_pr_monitors().await;
    let listed = svc.pr_monitor_list_op(&ws, None).await.unwrap();
    let current = &listed["monitors"][0];
    assert_eq!(current["lastSnapshot"]["ancestry"]["behindBy"], 2);
    assert_eq!(current["lastSnapshot"]["branchUpdateRequired"], false);
    let delta = json!(["branch ancestry: 1 → 2 commits behind base"]);
    assert_eq!(current["pendingChanges"], delta);
    let events = svc
        .store()
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(ws.clone()),
            event_types: vec![PR_MONITOR_CHANGED.to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data["changes"], delta);
    let id = PrMonitorId::from(current["monitorId"].as_str().unwrap());
    assert_eq!(
        svc.pr_monitor_flush_op(&ws, &id, false).await.unwrap()["flushed"],
        true
    );
    let wake = owner_messages(&svc, &owner).await;
    assert!(
        wake.contains("branch ancestry: 1 → 2 commits behind base"),
        "{wake}"
    );
    assert!(
        wake.contains("forge branch-update requirement: not required"),
        "{wake}"
    );
    assert!(!wake.contains("requires a branch update"), "{wake}");
}
