//! The merge-queue entry (position, queue size, state, time-to-merge
//! estimate) rides the checklist and the hover read, refreshes as the
//! queue moves, and is informational only: queue movement never yields a
//! diff line, a pending change or a wake, and the wake checklist never
//! prints it.
use super::*;
use intent_core::WorkspaceApi;
use intent_sourcecontrol::MergeQueueEntry;

fn entry(position: u32, estimate: Option<u64>) -> MergeQueueEntry {
    MergeQueueEntry {
        position,
        queue_size: Some(3),
        state: Some("queued".into()),
        enqueued_at: Some("2026-10-20T03:50:26Z".into()),
        estimated_time_to_merge: estimate,
    }
}

fn queued_at(position: u32, estimate: Option<u64>) -> PrMonitorSnapshot {
    snapshot(|s| {
        s.requirements.is_in_merge_queue = Some(true);
        s.requirements.merge_queue = Some(entry(position, estimate));
    })
}

#[test]
fn queue_movement_is_never_a_diff_line() {
    // Moving up, a new estimate, the estimate appearing or disappearing,
    // the queue size or entry state changing: no change line.
    let moved_on = snapshot(|s| {
        s.requirements.is_in_merge_queue = Some(true);
        s.requirements.merge_queue = Some(MergeQueueEntry {
            queue_size: Some(1),
            state: Some("awaiting_checks".into()),
            ..entry(1, Some(60))
        });
    });
    for (old, new) in [
        (queued_at(3, Some(2254)), queued_at(2, Some(1500))),
        (queued_at(2, None), queued_at(2, Some(1500))),
        (queued_at(2, Some(1500)), queued_at(2, None)),
        (queued_at(3, Some(2254)), moved_on),
    ] {
        assert!(diff_snapshots(&old, &new).is_empty(), "{old:?} → {new:?}");
    }

    // Entering / leaving the queue still reports exactly the existing lines;
    // the entry adds nothing of its own.
    let base = snapshot(|_| {});
    assert_eq!(
        diff_snapshots(&base, &queued_at(2, Some(1500))),
        vec!["entered the merge queue".to_string()]
    );
    assert_eq!(
        diff_snapshots(&queued_at(2, Some(1500)), &base),
        vec!["left the merge queue".to_string()]
    );
}

#[test]
fn the_wake_checklist_does_not_print_the_queue_entry() {
    let flag_only = snapshot(|s| s.requirements.is_in_merge_queue = Some(true));
    assert_eq!(
        render_checklist(&queued_at(2, Some(1500))),
        render_checklist(&flag_only)
    );
}

#[test]
fn persisted_baselines_without_the_entry_still_parse() {
    let mut wire = serde_json::to_value(snapshot(|_| {})).unwrap();
    assert!(wire["requirements"].get("mergeQueue").is_none());
    wire["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("mergeQueue");
    let parsed: PrMonitorSnapshot = serde_json::from_value(wire).unwrap();
    assert_eq!(parsed.requirements.merge_queue, None);

    let queued = serde_json::to_value(queued_at(2, Some(1500))).unwrap();
    assert_eq!(queued["requirements"]["mergeQueue"]["position"], json!(2));
    let round: PrMonitorSnapshot = serde_json::from_value(queued).unwrap();
    assert_eq!(round.requirements.merge_queue, Some(entry(2, Some(1500))));
}

/// A cached cheap poll (folded read, PR fingerprint unchanged) refreshes the
/// entry with the queue flag, without re-reading the branch rules and
/// without leaving anything pending; leaving the queue drops a stale entry.
#[intent_test_macros::daemon_test]
async fn cheap_polls_refresh_the_queue_entry_without_pending_changes() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    forge.edit_quiet(|s| s.folded = Some(FoldedRead::default()));
    let svc = svc.with_pr_monitor_debounce_seconds(3600);
    forge.edit_quiet(|s| {
        s.in_merge_queue = Some(true);
        s.merge_queue_entry = Some(entry(3, Some(2254)));
    });
    let monitor = register(&svc, &ws, &owner).await;
    let messages = owner_messages(&svc, &owner).await;
    for (position, estimate) in [(2, Some(1500)), (1, None)] {
        forge.edit_quiet(|s| s.merge_queue_entry = Some(entry(position, estimate)));
        let rules_before = forge.sub_fetches("branch_rules");
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.sub_fetches("branch_rules"),
            rules_before,
            "queue movement rides the cheap poll"
        );
        let (served, fetched) = svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
        assert!(!fetched, "served from the poll's cache entry");
        assert_eq!(
            served.snapshot.requirements.merge_queue,
            Some(entry(position, estimate))
        );
        let row = svc
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap();
        assert!(
            row.pending_changes.is_empty(),
            "queue movement is not a change: {:?}",
            row.pending_changes
        );
    }
    assert_eq!(
        owner_messages(&svc, &owner).await,
        messages,
        "queue movement never messages the owner"
    );

    // The host stops reporting the PR queued while a stale entry lingers:
    // the entry never outlives the flag.
    forge.edit_quiet(|s| s.in_merge_queue = Some(false));
    svc.poll_pr_monitors().await;
    let (served, _) = svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    assert_eq!(served.snapshot.requirements.is_in_merge_queue, None);
    assert_eq!(served.snapshot.requirements.merge_queue, None);
}

/// `github.pulls.get` (the hover card) carries `mergeQueue` alongside
/// `isInMergeQueue: true`, and omits it when the PR is not queued.
#[intent_test_macros::daemon_test]
async fn pulls_get_carries_the_queue_entry_only_while_queued() {
    let (_db, _root, svc, forge, _ws, _owner) = setup().await;
    forge.edit_quiet(|s| {
        s.in_merge_queue = Some(true);
        s.merge_queue_entry = Some(entry(2, Some(2254)));
    });
    let queued = svc
        .github_pulls_get("o".into(), "r".into(), 42)
        .await
        .expect("pulls.get");
    assert_eq!(queued["pull"]["isInMergeQueue"], json!(true));
    assert_eq!(
        queued["pull"]["mergeQueue"],
        json!({
            "position": 2,
            "queueSize": 3,
            "state": "queued",
            "enqueuedAt": "2026-10-20T03:50:26Z",
            "estimatedTimeToMerge": 2254
        }),
        "{queued}"
    );

    forge.edit_quiet(|s| s.in_merge_queue = Some(false));
    svc.backdate_pr_cache(svc.pr_cache_max_age() + Duration::from_secs(1));
    let left = svc
        .github_pulls_get("o".into(), "r".into(), 42)
        .await
        .expect("pulls.get after leaving");
    assert_eq!(left["pull"]["isInMergeQueue"], json!(false));
    assert!(left["pull"].get("mergeQueue").is_none(), "{left}");
}
