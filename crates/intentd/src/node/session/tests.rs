//! Fencing of replaced link sessions: frames an old generation already
//! received, and an old session adopting late, must not touch the runner or
//! the journal once a newer generation has bound.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use intent_acp::PermissionPolicy;
use intent_core::node_link::{JournalBody, JournalRecord, LaunchCommand};
use intent_core::{AgentId, WorkspaceId};
use intent_nodes::journal::Journal;

use super::*;
use crate::node::runner::RunnerConfig;
use crate::node::storage::Storage;

struct Harness {
    runner: Runner,
    _dir: tempfile::TempDir,
}

/// A runner with no providers: every launch is refused with one journaled
/// exit, which makes a launch's side effect observable without processes.
fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(&dir.path().join("node")).unwrap());
    let journal = Journal::open(&storage.journal_dir()).unwrap();
    let runner = Runner::new(RunnerConfig {
        max_agents: 1,
        policy: PermissionPolicy::DenyAll,
        providers: BTreeMap::new(),
        checkouts: crate::node::checkout::PlainCheckouts::new(storage),
        journal,
        mcp_bridge_exe: std::env::current_exe().unwrap(),
    });
    Harness { runner, _dir: dir }
}

fn binding(generation: u64, lease: &str) -> LeaseBinding {
    LeaseBinding {
        head_id: "head-1".into(),
        node_id: "node-1".into(),
        lease_id: lease.into(),
        generation: NodeCounter(generation),
    }
}

fn launch(id: &str) -> LinkFrame {
    LinkFrame::Launch(LaunchCommand {
        agent_id: AgentId::from(id),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: serde_json::json!({}),
        is_sub_agent: false,
    })
}

const OPEN: fn() -> bool = || false;

async fn records(runner: &Runner) -> Vec<JournalRecord> {
    runner
        .with_journal(|journal| journal.records_after(NodeCounter(0)))
        .await
        .unwrap()
}

/// Wait until the journal holds `count` records.
async fn wait_records(runner: &Runner, count: usize) -> Vec<JournalRecord> {
    let mut appended = runner.appended();
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(10));
    loop {
        let all = records(runner).await;
        if all.len() >= count {
            return all;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            tokio::time::timeout(remaining, appended.changed())
                .await
                .is_ok(),
            "timed out waiting for {count} records: {all:#?}"
        );
    }
}

fn exited_agents(all: &[JournalRecord]) -> Vec<&str> {
    all.iter()
        .filter(|r| matches!(r.body, JournalBody::Exited { .. }))
        .map(|r| r.agent_id.0.as_str())
        .collect()
}

#[tokio::test]
async fn queued_frames_of_a_replaced_generation_have_no_effect() {
    let h = harness();
    let old = binding(1, "lease-1");
    let new = binding(2, "lease-2");
    assert_eq!(
        adopt(&h.runner, &old, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    assert_eq!(
        adopt(&h.runner, &new, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    for id in ["a1", "a2", "a3"] {
        assert_eq!(
            apply(&h.runner, &new, OPEN, launch(id)).await,
            Applied::Done
        );
    }
    let all = wait_records(&h.runner, 3).await;
    assert_eq!(exited_agents(&all), ["a1", "a2", "a3"]);

    // Frames the old link had already received when it was replaced. Its
    // Ack names a sequence that exists again under the new lease.
    assert_eq!(
        apply(
            &h.runner,
            &old,
            OPEN,
            LinkFrame::Ack {
                seq: NodeCounter(3)
            }
        )
        .await,
        Applied::Stale
    );
    assert_eq!(
        apply(&h.runner, &old, OPEN, launch("old")).await,
        Applied::Stale
    );
    let stop = LinkFrame::Stop {
        agent_id: AgentId::from("a1"),
    };
    assert_eq!(apply(&h.runner, &old, OPEN, stop).await, Applied::Stale);

    let acked = h
        .runner
        .with_journal(|journal| Ok(journal.acked_seq()))
        .await
        .unwrap();
    assert_eq!(acked, NodeCounter(0), "the stale ack deleted nothing");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let all = records(&h.runner).await;
    assert_eq!(exited_agents(&all), ["a1", "a2", "a3"], "no stale launch");

    // The current session's own frames still apply.
    assert_eq!(
        apply(
            &h.runner,
            &new,
            OPEN,
            LinkFrame::Ack {
                seq: NodeCounter(3)
            }
        )
        .await,
        Applied::Done
    );
    assert!(records(&h.runner).await.is_empty());
}

#[tokio::test]
async fn a_closed_link_applies_nothing_even_before_its_successor_adopts() {
    let h = harness();
    let old = binding(1, "lease-1");
    assert_eq!(
        adopt(&h.runner, &old, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    // The listener closed the old link for a newer bind whose session has
    // not adopted yet.
    assert_eq!(
        apply(&h.runner, &old, || true, launch("old")).await,
        Applied::Stale
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(records(&h.runner).await.is_empty());
}

#[tokio::test]
async fn a_delayed_old_session_cannot_take_the_journal_back_to_its_lease() {
    let h = harness();
    let old = binding(1, "lease-1");
    let new = binding(2, "lease-2");
    assert_eq!(
        adopt(&h.runner, &new, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    assert_eq!(
        apply(&h.runner, &new, OPEN, launch("a1")).await,
        Applied::Done
    );
    wait_records(&h.runner, 1).await;

    // The old session reaches adoption only now, its link still open.
    assert_eq!(
        adopt(&h.runner, &old, NodeCounter(0), OPEN).await,
        Adopted::Replaced
    );
    let lease = h
        .runner
        .with_journal(|journal| Ok(journal.lease_id().map(str::to_owned)))
        .await
        .unwrap();
    assert_eq!(lease.as_deref(), Some("lease-2"));
    assert_eq!(records(&h.runner).await.len(), 1, "new-lease records kept");

    // A closed link never adopts either.
    assert_eq!(
        adopt(&h.runner, &binding(3, "lease-3"), NodeCounter(0), || true).await,
        Adopted::Replaced
    );
}

#[tokio::test]
async fn an_ack_past_the_journal_closes_the_link() {
    let h = harness();
    let current = binding(1, "lease-1");
    assert!(matches!(
        adopt(&h.runner, &current, NodeCounter(4), OPEN).await,
        Adopted::Refused(error) if error.contains("node-journal-ack-ahead")
    ));
    assert_eq!(
        adopt(&h.runner, &current, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    assert_eq!(
        apply(
            &h.runner,
            &current,
            OPEN,
            LinkFrame::Ack {
                seq: NodeCounter(9)
            }
        )
        .await,
        Applied::Close
    );
}

#[test]
fn admission_needs_an_open_link_and_no_newer_generation() {
    let one = binding(1, "lease-1");
    let two = binding(2, "lease-1");
    assert_eq!(admit(None, &one, false), Admission::Current);
    assert_eq!(admit(Some(&one), &one, false), Admission::Current);
    assert_eq!(admit(Some(&one), &two, false), Admission::Current);
    assert_eq!(admit(Some(&two), &one, false), Admission::Replaced);
    assert_eq!(admit(Some(&one), &one, true), Admission::Replaced);
}
