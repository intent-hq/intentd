//! A known value that briefly reads as unknown is not news. GitHub reports
//! the branch-update requirement unknown while it recomputes mergeability
//! after the base branch moves: on intent-hq/intent#8843 each blip cost two
//! empty wakes ("unknown", then "available: not required") with the head and
//! checks unchanged. A degraded review-thread read is the same shape.
use super::*;

const WAKE: &str = "[PR monitor o/r#42]";

async fn row(svc: &Services, id: &PrMonitorId) -> PrMonitor {
    svc.store().get_pr_monitor(id).await.unwrap()
}

async fn pending(svc: &Services, id: &PrMonitorId) -> Vec<String> {
    row(svc, id).await.pending_changes
}

async fn flush(svc: &Services, ws: &WorkspaceId, id: &PrMonitorId) -> bool {
    svc.pr_monitor_flush_op(ws, id, false).await.unwrap()["flushed"] == json!(true)
}

async fn wakes(svc: &Services, owner: &AgentId) -> usize {
    owner_messages(svc, owner).await.matches(WAKE).count()
}

/// REST `mergeable_state` lags as `unknown`; the GraphQL merge state either
/// stays known (the #8843 shape) or goes `UNKNOWN` as well.
fn blip(s: &mut ForgeState, graphql_known: bool) {
    s.mergeable_state = "unknown".into();
    s.merge_state_status = graphql_known.then(|| "CLEAN".into());
}

fn settle(s: &mut ForgeState, state: &str) {
    s.mergeable_state = state.into();
    s.merge_state_status = None;
}

#[tokio::test]
async fn branch_update_blip_false_unknown_false_never_wakes() {
    for graphql_known in [true, false] {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let id = register(&svc, &ws, &owner).await.monitor_id;
        for _ in 0..2 {
            forge.edit(|s| blip(s, graphql_known));
            svc.poll_pr_monitors().await;
            assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
            forge.edit(|s| settle(s, "clean"));
            svc.poll_pr_monitors().await;
            assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
        }
        assert!(!flush(&svc, &ws, &id).await);
        assert_eq!(
            wakes(&svc, &owner).await,
            0,
            "graphql_known={graphql_known}"
        );
    }
}

/// An unrelated wake delivered mid-blip moves the emit baseline onto the
/// unknown read. The agent was last told "not required", so settling back
/// there is still silent.
#[tokio::test]
async fn branch_update_blip_settling_after_an_unrelated_wake_stays_silent() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let id = register(&svc, &ws, &owner).await.monitor_id;
    forge.edit(|s| {
        blip(s, true);
        s.conversation_comments = 1;
    });
    svc.poll_pr_monitors().await;
    assert_eq!(
        pending(&svc, &id).await,
        vec!["+1 conversation comment (1 total)"]
    );
    assert!(flush(&svc, &ws, &id).await);
    forge.edit(|s| settle(s, "clean"));
    svc.poll_pr_monitors().await;
    assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
    assert_eq!(wakes(&svc, &owner).await, 1);
}

#[tokio::test]
async fn branch_update_real_changes_still_wake_across_a_blip() {
    let required = "forge now requires a branch update before merging";
    let cleared = "forge no longer requires a branch update before merging";
    for via_blip in [false, true] {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        let id = register(&svc, &ws, &owner).await.monitor_id;
        if via_blip {
            forge.edit(|s| blip(s, true));
            svc.poll_pr_monitors().await;
        }
        forge.edit(|s| settle(s, "behind"));
        svc.poll_pr_monitors().await;
        let changes = pending(&svc, &id).await;
        assert!(
            changes.iter().any(|c| c == required),
            "{via_blip}: {changes:?}"
        );
        assert!(flush(&svc, &ws, &id).await);
        assert!(owner_messages(&svc, &owner).await.contains(required));

        forge.edit(|s| blip(s, true));
        svc.poll_pr_monitors().await;
        let changes = pending(&svc, &id).await;
        assert!(
            !changes
                .iter()
                .any(|c| c.contains("branch update") || c.contains("branch-update")),
            "{via_blip}: {changes:?}"
        );
        forge.edit(|s| settle(s, "clean"));
        svc.poll_pr_monitors().await;
        let changes = pending(&svc, &id).await;
        assert!(
            changes.iter().any(|c| c == cleared),
            "{via_blip}: {changes:?}"
        );
    }
}

#[tokio::test]
async fn thread_read_blip_never_wakes_unless_the_count_changed() {
    for unrelated_wake in [false, true] {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        forge.edit(|s| s.threads = vec![thread("t1", false, 0)]);
        let id = register(&svc, &ws, &owner).await.monitor_id;

        forge.edit(|s| {
            s.fail_get_review_threads = true;
            if unrelated_wake {
                s.conversation_comments = 1;
            }
        });
        svc.poll_pr_monitors().await;
        assert!(
            !pending(&svc, &id)
                .await
                .iter()
                .any(|c| c.contains("thread")),
            "{unrelated_wake}"
        );
        if unrelated_wake {
            assert!(flush(&svc, &ws, &id).await);
        }
        forge.edit(|s| s.fail_get_review_threads = false);
        svc.poll_pr_monitors().await;
        assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
        assert!(!flush(&svc, &ws, &id).await);
        assert_eq!(wakes(&svc, &owner).await, usize::from(unrelated_wake));

        // A count that moved while unreadable reports against the last
        // known count.
        forge.edit(|s| s.fail_get_review_threads = true);
        svc.poll_pr_monitors().await;
        forge.edit(|s| {
            s.fail_get_review_threads = false;
            s.threads[0].is_resolved = true;
        });
        svc.poll_pr_monitors().await;
        assert_eq!(
            pending(&svc, &id).await,
            vec!["thread(s) resolved: 1 → 0 unresolved"],
            "{unrelated_wake}"
        );
    }
}

/// While GitHub recomputes, GraphQL `mergeStateStatus` reads `UNKNOWN` and
/// REST `mergeable` reads null as well.
fn full_blip(s: &mut ForgeState) {
    blip(s, false);
    s.mergeable = None;
}

fn full_settle(s: &mut ForgeState, state: &str, mergeable: bool) {
    settle(s, state);
    s.mergeable = Some(mergeable);
}

fn merge_lines(changes: &[String]) -> Vec<&String> {
    changes
        .iter()
        .filter(|c| c.starts_with("mergeable:") || c.starts_with("merge state:"))
        .collect()
}

/// An unrelated wake delivered mid-blip moves the emit baseline onto the
/// unknown merge state. Settling back at the value the agent was last told
/// is silent; a different value reports against that last known value.
#[tokio::test]
async fn merge_state_blip_settling_after_an_unrelated_wake_reports_only_a_change() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let id = register(&svc, &ws, &owner).await.monitor_id;
    forge.edit(|s| {
        full_blip(s);
        s.conversation_comments = 1;
    });
    svc.poll_pr_monitors().await;
    assert_eq!(
        pending(&svc, &id).await,
        vec!["+1 conversation comment (1 total)"]
    );
    assert!(flush(&svc, &ws, &id).await);
    forge.edit(|s| full_settle(s, "clean", true));
    svc.poll_pr_monitors().await;
    assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
    assert!(!flush(&svc, &ws, &id).await);

    forge.edit(|s| {
        full_blip(s);
        s.conversation_comments = 2;
    });
    svc.poll_pr_monitors().await;
    assert!(flush(&svc, &ws, &id).await);
    forge.edit(|s| full_settle(s, "dirty", false));
    svc.poll_pr_monitors().await;
    let changes = pending(&svc, &id).await;
    assert_eq!(
        merge_lines(&changes),
        vec!["mergeable: true → false", "merge state: CLEAN → DIRTY"],
        "{changes:?}"
    );
}

/// A re-register re-arms the row on a fresh read. When that read lands
/// mid-blip, the agent's last known values survive it: settling back at
/// them is not news.
#[tokio::test]
async fn re_register_mid_blip_keeps_the_last_known_values() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let id = register(&svc, &ws, &owner).await.monitor_id;
    forge.edit(full_blip);
    svc.poll_pr_monitors().await;
    assert_eq!(register(&svc, &ws, &owner).await.monitor_id, id);
    forge.edit(|s| full_settle(s, "clean", true));
    svc.poll_pr_monitors().await;
    assert_eq!(pending(&svc, &id).await, Vec::<String>::new());

    // Last known "required": a re-register mid-blip, then settling back at
    // "required", reports no branch-update or merge-state line.
    forge.edit(|s| full_settle(s, "behind", true));
    svc.poll_pr_monitors().await;
    assert!(flush(&svc, &ws, &id).await);
    forge.edit(full_blip);
    assert_eq!(register(&svc, &ws, &owner).await.monitor_id, id);
    forge.edit(|s| full_settle(s, "behind", true));
    svc.poll_pr_monitors().await;
    let changes = pending(&svc, &id).await;
    assert!(
        !changes.iter().any(|c| c.contains("branch-update")
            || c.contains("branch update")
            || c.starts_with("mergeable:")
            || c.starts_with("merge state:")),
        "{changes:?}"
    );
}
