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
    // own (the REST fallback), is not news; neither is an empty link or a
    // vendor link on another host shaped like a check-run link.
    for url in [
        Some("https://github.com/o/r/runs/2"),
        Some("https://github.com/o/r/actions/runs/77/job/2"),
        None,
        Some("https://github.com/o/r/runs/2"),
        Some("https://app.circleci.com/pipelines/gh/o/r/5/workflows/w/jobs/9"),
        Some("https://github.com/o/r/runs/2"),
        Some(""),
        Some("https://app.circleci.com/pipelines/gh/o/r/5/workflows/w/jobs/9"),
        Some("https://github.com/o/r/runs/2"),
        // A vendor link shaped like a check-run link is another host's id.
        Some("https://ci.example/o/r/runs/7"),
        Some("https://github.com/o/r/runs/2"),
        Some("https://ci.example/o/r/job/7"),
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

/// A name with both a check run and a legacy commit status reports the worse
/// of the two, so its link moves from the run to the status when the run
/// passes and the status still fails. That is not a re-run.
#[tokio::test]
async fn a_failing_status_taking_over_from_a_passing_run_is_quiet() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let id = register(&svc, &ws, &owner).await.monitor_id;
    let status = |state: CheckState| RollupCheck {
        kind: RollupCheckKind::StatusContext,
        ..build(state, Some("https://ci.example/status/build/1"))
    };
    let app_run = |state: CheckState| build(state, Some("https://ci.example/app/build/7"));

    forge.edit(|s| s.checks = vec![app_run(CheckState::Failure), status(CheckState::Failure)]);
    svc.poll_pr_monitors().await;
    assert!(flush(&svc, &ws, &id).await);

    forge.edit(|s| s.checks = vec![app_run(CheckState::Success), status(CheckState::Failure)]);
    svc.poll_pr_monitors().await;
    assert_eq!(pending(&svc, &id).await, Vec::<String>::new());
    assert!(!flush(&svc, &ws, &id).await);
    let text = owner_messages(&svc, &owner).await;
    assert_eq!(text.matches(WAKE).count(), 1, "{text}");
}

#[derive(Clone, Copy, Debug)]
enum Link {
    ActionsJob,
    CheckRun,
    Vendor,
    Empty,
    Null,
}

const LINKS: [Link; 5] = [
    Link::ActionsJob,
    Link::CheckRun,
    Link::Vendor,
    Link::Empty,
    Link::Null,
];

fn rollup_link(link: Link, id: u32) -> Option<String> {
    match link {
        Link::ActionsJob => Some(format!("https://github.com/o/r/actions/runs/9/job/{id}")),
        Link::CheckRun => Some(format!("https://github.com/o/r/runs/{id}")),
        Link::Vendor => Some(format!("https://ci.example/build/{id}")),
        Link::Empty => Some(String::new()),
        Link::Null => None,
    }
}

/// One read of a `build` check run (id 100) beside a same-name legacy status
/// (its link built from id 200): the rollup reports both, the REST fallback
/// only the run's `/runs/<id>` link and keeps the last known status.
fn read(
    rollup: bool,
    run: (CheckState, Link),
    status: (CheckState, Link),
    previous: Option<&PrMonitorSnapshot>,
) -> PrMonitorSnapshot {
    let check = |kind, state, url: Option<String>| RollupCheck {
        kind,
        url,
        ..build(state, None)
    };
    let mut shared = shared_from(&snapshot(|_| {}), true);
    if rollup {
        let checks = vec![
            check(RollupCheckKind::CheckRun, run.0, rollup_link(run.1, 100)),
            check(
                RollupCheckKind::StatusContext,
                status.0,
                rollup_link(status.1, 200),
            ),
        ];
        let signals = MergeRequirementSignals {
            checks,
            checks_known: true,
            ..Default::default()
        };
        shared.requirements.checks = pr_ops::MergeRequirementsChecks::from_items(
            pr_ops::rollup_items(&signals.checks),
            true,
        );
        shared.status_checks = pr_ops::status_checks(Some(&signals));
    } else {
        let runs = [check(
            RollupCheckKind::CheckRun,
            run.0,
            Some("https://github.com/o/r/runs/100".into()),
        )];
        shared.requirements.checks =
            pr_ops::MergeRequirementsChecks::from_items(pr_ops::rollup_items(&runs), false);
        shared.status_checks = None;
    }
    shared.materialize(previous)
}

/// A check run and a same-name legacy status, each with every link kind,
/// read by either path before and after each pass/fail transition: the same
/// run and status never read as a new run, though the reported row moves
/// between them (a status link that looks like a check-run link included).
/// A real re-run of the check run still does.
#[test]
fn a_check_run_and_a_status_never_read_as_a_rerun() {
    use CheckState::{Failure as F, Success as P};
    let transitions = [
        ((F, F), (P, F)),
        ((F, F), (F, P)),
        ((F, F), (F, F)),
        ((P, F), (F, F)),
        ((F, P), (F, F)),
    ];
    for run_link in LINKS {
        for status_link in LINKS {
            for ((run_before, status_before), (run_after, status_after)) in transitions {
                for (rollup_before, rollup_after) in
                    [(true, true), (true, false), (false, true), (false, false)]
                {
                    let seed = read(
                        true,
                        (run_before, run_link),
                        (status_before, status_link),
                        None,
                    );
                    let before = read(
                        rollup_before,
                        (run_before, run_link),
                        (status_before, status_link),
                        Some(&seed),
                    );
                    let after = read(
                        rollup_after,
                        (run_after, run_link),
                        (status_after, status_link),
                        Some(&before),
                    );
                    let changes = diff_snapshots(&before, &after);
                    assert!(
                        !changes.iter().any(|c| c.contains("on a new run")),
                        "run {run_link:?} {run_before:?}→{run_after:?}, status {status_link:?} \
                         {status_before:?}→{status_after:?}, rollup {rollup_before}→{rollup_after}: \
                         {changes:?}"
                    );
                }
            }
        }
    }

    let failed_run = |id: u32| {
        let check = |kind, state, url: Option<String>| RollupCheck {
            kind,
            url,
            ..build(state, None)
        };
        let checks = vec![
            check(
                RollupCheckKind::CheckRun,
                F,
                rollup_link(Link::ActionsJob, id),
            ),
            check(
                RollupCheckKind::StatusContext,
                P,
                rollup_link(Link::Vendor, 200),
            ),
        ];
        let signals = MergeRequirementSignals {
            checks,
            checks_known: true,
            ..Default::default()
        };
        let mut shared = shared_from(&snapshot(|_| {}), true);
        shared.requirements.checks = pr_ops::MergeRequirementsChecks::from_items(
            pr_ops::rollup_items(&signals.checks),
            true,
        );
        shared.status_checks = pr_ops::status_checks(Some(&signals));
        shared
    };
    let first = failed_run(100).materialize(None);
    let rerun = failed_run(300).materialize(Some(&first));
    assert!(
        diff_snapshots(&first, &rerun).contains(&AGAIN.to_string()),
        "{:?}",
        diff_snapshots(&first, &rerun)
    );
}

/// A snapshot persisted before `status_checks` existed cannot tell a check
/// run's link from a legacy status's, so its links are never evidence.
#[test]
fn unknown_link_provenance_is_never_rerun_evidence() {
    let s = snapshot(|s| {
        s.status_checks = None;
        s.requirements.checks.items[0].url = Some("https://github.com/o/r/runs/1".into());
    });
    assert_eq!(s.check_run_link(&s.requirements.checks.items[0]), None);
}
