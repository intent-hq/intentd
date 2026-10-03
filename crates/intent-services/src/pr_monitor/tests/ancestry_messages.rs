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
        vec!["forge branch-update requirement available: not required",]
    );
    let mut moved = first.clone();
    moved.requirements.ancestry = known(2);
    assert_eq!(diff_snapshots(&first, &moved), Vec::<String>::new());
    let mut current = moved.clone();
    current.requirements.ancestry = known(0);
    assert_eq!(diff_snapshots(&moved, &current), Vec::<String>::new());
    assert_eq!(
        diff_snapshots(&first, &old),
        vec!["forge branch-update requirement unknown",]
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
        vec!["forge branch-update requirement unknown",]
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
fn ancestry_only_transitions_are_quiet() {
    for before in [PrAncestry::Unknown, known(0), known(1), known(2)] {
        for after in [PrAncestry::Unknown, known(0), known(1), known(2)] {
            let mut old = ready();
            old.requirements.ancestry = before.clone();
            let mut new = old.clone();
            new.requirements.ancestry = after;
            assert!(diff_snapshots(&old, &new).is_empty());
            new.requirements.branch_update_required = Some(true);
            assert!(diff_snapshots(&old, &new)
                .iter()
                .any(|s| s.contains("now requires")));
            new.requirements.has_conflicts = true;
            assert!(diff_snapshots(&old, &new)
                .iter()
                .any(|s| s == "merge conflicts appeared"));
        }
    }
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
    // Only the base moves: refresh visibility without creating a pending wake.
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!("c".repeat(40));
        s.compare = json!({"behind_by":2});
    });
    svc.poll_pr_monitors().await;
    let listed = svc.pr_monitor_list_op(&ws, None).await.unwrap();
    let current = &listed["monitors"][0];
    assert_eq!(current["lastSnapshot"]["ancestry"]["behindBy"], 2);
    assert_eq!(current["lastSnapshot"]["branchUpdateRequired"], false);
    assert_eq!(current["pendingChanges"], json!([]));
    assert_eq!(current["hasPendingChanges"], false);
    let events = svc
        .store()
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(ws.clone()),
            event_types: vec![
                PR_MONITOR_CHANGED.to_string(),
                PR_MONITOR_EMITTED.to_string(),
            ],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(events.is_empty());
    let id = PrMonitorId::from(current["monitorId"].as_str().unwrap());
    assert_eq!(
        svc.pr_monitor_flush_op(&ws, &id, false).await.unwrap()["flushed"],
        false
    );
    assert!(!owner_messages(&svc, &owner).await.contains("PR monitor"));
    let snapshot = svc
        .pr_state(ws.clone(), 10978, Some("o/r".into()))
        .await
        .unwrap();
    assert_eq!(snapshot["requirements"]["ancestry"]["behindBy"], 2);

    // A real update requirement starts debounce. Further ancestry churn must
    // leave both its net changes and its quiet-window anchor untouched.
    mock.edit(|s| s.pr["mergeStateStatus"] = json!("BEHIND"));
    svc.poll_pr_monitors().await;
    let held = svc.store().get_pr_monitor(&id).await.unwrap();
    assert!(held
        .pending_changes
        .iter()
        .any(|s| s.contains("now requires")));
    let anchor = (time::OffsetDateTime::now_utc() - time::Duration::seconds(120))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    assert!(svc
        .store()
        .update_pr_monitor_poll(
            &id,
            PrMonitorPollUpdate {
                last_snapshot: held.last_snapshot.as_deref(),
                baseline_snapshot: held.baseline_snapshot.as_deref(),
                pending_changes: &held.pending_changes,
                pending_since: Some(&anchor),
                last_change_at: Some(&anchor),
                last_polled_at: held.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &held.updated_at,
            }
        )
        .await
        .unwrap());
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!("d".repeat(40));
        s.compare = json!({"behind_by":3});
    });
    svc.poll_pr_monitors().await;
    let current = svc.store().get_pr_monitor(&id).await.unwrap();
    assert_eq!(current.pending_changes, held.pending_changes);
    assert_eq!(current.last_change_at.as_deref(), Some(anchor.as_str()));
    assert_eq!(current.pending_since.as_deref(), Some(anchor.as_str()));
    // 120 seconds is past one quiet window, but below the max-wait bound.
    let svc = svc.with_pr_monitor_debounce_seconds(60);
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!("e".repeat(40));
        s.compare = json!({"behind_by":4});
    });
    svc.poll_pr_monitors().await;
    let wake = owner_messages(&svc, &owner).await;
    assert_eq!(wake.matches("[PR monitor o/r#10978]").count(), 1, "{wake}");
    assert!(
        wake.contains("forge now requires a branch update"),
        "{wake}"
    );
    assert!(
        wake.contains("branch ancestry: 4 commits behind base"),
        "{wake}"
    );
    assert!(!wake.contains("→ 4 commits behind base"), "{wake}");
}

#[tokio::test]
async fn ancestry_persisted_pending_is_suppressed_on_restart_and_flush() {
    for restart in [false, true] {
        for mixed in [false, true] {
            let (_db, _root, svc, _forge, ws, owner) = setup().await;
            let monitor = register(&svc, &ws, &owner).await;
            let mut baseline = snapshot(|_| {});
            baseline.requirements.ancestry = known(1);
            let mut last = baseline.clone();
            last.requirements.ancestry = known(2);
            let baseline = serde_json::to_string(&baseline).unwrap();
            let last = serde_json::to_string(&last).unwrap();
            let mut pending = vec![
                "branch ancestry: 1 → 2 commits behind base".to_string(),
                "branch ancestry available: 2 commits behind base".to_string(),
                "branch ancestry unavailable".to_string(),
            ];
            if mixed {
                // A legacy accumulated actionable line cannot be reconstructed
                // from the snapshots; preserve it while removing ancestry noise.
                pending.push("check build: pending → failed".into());
            }
            assert!(svc
                .store()
                .update_pr_monitor_poll(
                    &monitor.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: Some(&last),
                        baseline_snapshot: Some(&baseline),
                        pending_changes: &pending,
                        pending_since: Some(&now_iso()),
                        last_change_at: Some(&now_iso()),
                        last_polled_at: monitor.last_polled_at.as_deref(),
                        last_error: None,
                        updated_at: &now_iso(),
                        expected_updated_at: &monitor.updated_at,
                    }
                )
                .await
                .unwrap());
            if restart {
                assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
            } else {
                assert_eq!(
                    svc.pr_monitor_flush_op(&ws, &monitor.monitor_id, false)
                        .await
                        .unwrap()["flushed"],
                    mixed
                );
            }
            let row = svc
                .store()
                .get_pr_monitor(&monitor.monitor_id)
                .await
                .unwrap();
            assert!(row.pending_changes.is_empty());
            assert!(row.pending_since.is_none());
            assert!(row.last_change_at.is_none());
            let messages = owner_messages(&svc, &owner).await;
            assert_eq!(messages.contains("PR monitor"), mixed, "{messages}");
            for obsolete in &pending[..3] {
                assert!(!messages.contains(obsolete), "{messages}");
            }
            if mixed {
                assert!(
                    messages.contains("check build: pending → failed"),
                    "{messages}"
                );
            }
        }
    }
}

#[tokio::test]
async fn ancestry_coalesced_pending_recomputes_downtime_changes_before_waking() {
    for comments_after_restart in [0, 2] {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let svc = svc.with_pr_monitor_debounce_seconds(3600);
        let monitor = register(&svc, &ws, &owner).await;
        forge.edit(|s| s.conversation_comments = 1);
        svc.poll_pr_monitors().await;
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        let mut baseline: PrMonitorSnapshot =
            serde_json::from_str(row.baseline_snapshot.as_deref().unwrap()).unwrap();
        let mut last: PrMonitorSnapshot =
            serde_json::from_str(row.last_snapshot.as_deref().unwrap()).unwrap();
        baseline.requirements.ancestry = known(1);
        last.requirements.ancestry = known(2);
        let baseline = serde_json::to_string(&baseline).unwrap();
        let last = serde_json::to_string(&last).unwrap();
        let mut pending = row.pending_changes.clone();
        pending.push("branch ancestry: 1 → 2 commits behind base".into());
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: Some(&last),
                    baseline_snapshot: Some(&baseline),
                    pending_changes: &pending,
                    pending_since: row.pending_since.as_deref(),
                    last_change_at: row.last_change_at.as_deref(),
                    last_polled_at: row.last_polled_at.as_deref(),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                }
            )
            .await
            .unwrap());
        forge.edit(|s| s.conversation_comments = comments_after_restart);
        assert_eq!(svc.rehydrate_pr_monitors().await.unwrap(), 1);
        assert!(
            !owner_messages(&svc, &owner).await.contains("PR monitor"),
            "a coalesced row must await the current forge state"
        );
        svc.poll_pr_monitors().await;
        let messages = owner_messages(&svc, &owner).await;
        assert_eq!(
            messages.matches("[PR monitor o/r#42]").count(),
            usize::from(comments_after_restart != 0),
            "{messages}"
        );
        if comments_after_restart != 0 {
            assert!(
                messages.contains("+2 conversation comments (2 total)"),
                "{messages}"
            );
        }
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(row.pending_changes.is_empty());
    }
}
