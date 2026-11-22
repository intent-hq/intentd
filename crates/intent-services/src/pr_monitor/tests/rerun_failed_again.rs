//! A re-run of a failed check that fails again keeps the check's name and
//! status, so a diff by name and status missed it and the owner was never
//! woken (intent-hq/intent#9852). The new run's link is the evidence: one
//! wake per new failed run, and no wake for the same run read again.
use super::*;

const WAKE: &str = "[PR monitor o/r#42]";
const AGAIN: &str = "check build is still failing on a new run";

fn build(state: CheckState, url: Option<&str>) -> RollupCheck {
    RollupCheck {
        name: "build".into(),
        kind: RollupCheckKind::CheckRun,
        state,
        is_required: true,
        url: url.map(String::from),
        started_at: None,
    }
}

async fn pending(svc: &Services, id: &PrMonitorId) -> Vec<String> {
    svc.store()
        .get_pr_monitor(id)
        .await
        .unwrap()
        .pending_changes
}

async fn flush(svc: &Services, ws: &WorkspaceId, id: &PrMonitorId) -> bool {
    svc.pr_monitor_flush_op(ws, id, false).await.unwrap()["flushed"] == json!(true)
}

#[tokio::test]
async fn a_rerun_that_fails_again_wakes_once() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let id = register(&svc, &ws, &owner).await.monitor_id;

    forge.edit(|s| {
        s.checks = vec![build(
            CheckState::Failure,
            Some("https://github.com/o/r/runs/1"),
        )];
    });
    svc.poll_pr_monitors().await;
    let first = pending(&svc, &id).await;
    assert!(
        first.iter().any(|c| c == "check build: pending → failed"),
        "{first:?}"
    );
    assert!(flush(&svc, &ws, &id).await);

    // Re-run between polls: the pending attempt was never observed.
    forge.edit(|s| {
        s.checks = vec![build(
            CheckState::Failure,
            Some("https://github.com/o/r/runs/2"),
        )];
    });
    svc.poll_pr_monitors().await;
    assert_eq!(pending(&svc, &id).await, vec![AGAIN]);
    assert!(flush(&svc, &ws, &id).await);
    assert_eq!(owner_messages(&svc, &owner).await.matches(WAKE).count(), 2);

    // The same run read again, under its Actions job link, without a link,
    // or under a CI vendor's details link (the rollup read) next to GitHub's
    // own (the REST fallback), is not news.
    for url in [
        Some("https://github.com/o/r/runs/2"),
        Some("https://github.com/o/r/actions/runs/77/job/2"),
        None,
        Some("https://github.com/o/r/runs/2"),
        Some("https://app.circleci.com/pipelines/gh/o/r/5/workflows/w/jobs/9"),
        Some("https://github.com/o/r/runs/2"),
    ] {
        forge.edit(|s| s.checks = vec![build(CheckState::Failure, url)]);
        svc.poll_pr_monitors().await;
        assert_eq!(pending(&svc, &id).await, Vec::<String>::new(), "{url:?}");
    }
    assert!(!flush(&svc, &ws, &id).await);
    let text = owner_messages(&svc, &owner).await;
    assert_eq!(text.matches(WAKE).count(), 2, "{text}");
    assert_eq!(text.matches(AGAIN).count(), 1, "{text}");
}
