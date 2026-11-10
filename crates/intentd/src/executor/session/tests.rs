//! Fencing of replaced link sessions: frames an old generation already
//! received, and an old session adopting late, must not touch the runner or
//! the journal once a newer generation has bound.

use std::sync::Arc;
use std::time::Duration;

use intent_acp::PermissionPolicy;
use intent_core::node_link::{JournalBody, JournalRecord, LaunchCommand};
use intent_core::{AgentId, WorkspaceId};
use intent_nodes::journal::Journal;

use super::*;
use crate::executor::runner::{
    Admission as SharedLimits, MachineLaunch, ProviderLaunch, RunnerConfig,
};
use intent_core::node_link::MachineCapacity;
use intent_core::placement::CapacityRefusal;

/// Limits that never bind.
struct Unlimited;

impl SharedLimits for Unlimited {
    fn try_admit(&self, _agent_id: &AgentId) -> Result<(), CapacityRefusal> {
        Ok(())
    }
    fn release(&self, _agent_id: &AgentId) {}
    fn capacity(&self) -> MachineCapacity {
        MachineCapacity {
            running_agents: 0,
            agent_cap: 1,
            charged_memory_bytes: 0,
            memory_budget_bytes: None,
        }
    }
}

/// No provider is available.
struct NoProviders;

impl MachineLaunch for NoProviders {
    fn resolve(&self, _provider: &str) -> Result<ProviderLaunch, String> {
        Err("not installed".into())
    }
    fn policy(&self) -> PermissionPolicy {
        PermissionPolicy::DenyAll
    }
}
use crate::executor::storage::Storage;

struct Harness {
    runner: Runner,
    _dir: tempfile::TempDir,
}

/// A runner with no providers: every launch is refused with one journaled
/// exit, which makes a launch's side effect observable without processes.
fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(&dir.path().join("node"), None).unwrap());
    let journal = Journal::open(&storage.journal_dir()).unwrap();
    let runner = Runner::new(RunnerConfig {
        admission: Arc::new(Unlimited),
        launch: Arc::new(NoProviders),
        checkouts: crate::executor::checkout::PlainCheckouts::new(storage.clone()),
        journal,
        home: None,
        mcp_bridge_exe: std::env::current_exe().unwrap(),
        git_socket: None,
        pull_staging: storage.pull_staging_dir(),
        forge_git: None,
        auth_failed: Arc::new(|_: &str| {}),
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
        profile_env: None,
        agent_features: serde_json::json!({}),
        is_sub_agent: false,
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        harness_version: None,
        labels: Box::default(),
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
    // A replaced session's stop is dropped unacknowledged: the backend
    // resends it on its current link.
    let stop = LinkFrame::Stop {
        agent_id: AgentId::from("a1"),
        stop_id: intent_core::node_link::StopId::new_v4(),
        final_stop: false,
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

/// The current session answers a stop with a pending acknowledgement for
/// that stop id; an agent the machine is not running is acknowledged as
/// not running at once.
#[tokio::test]
async fn a_current_stop_yields_an_acknowledgement_for_its_own_id() {
    use intent_core::node_link::{StopId, StopOutcome};
    let h = harness();
    let current = binding(1, "lease-1");
    assert_eq!(
        adopt(&h.runner, &current, NodeCounter(0), OPEN).await,
        Adopted::Ready
    );
    let stop_id = StopId::new_v4();
    let frame = LinkFrame::Stop {
        agent_id: AgentId::from("ghost"),
        stop_id,
        final_stop: false,
    };
    let Applied::Stopping(pending) = apply(&h.runner, &current, OPEN, frame).await else {
        panic!("a current stop is acknowledged");
    };
    assert_eq!(
        pending.ack().await,
        Some(LinkFrame::StopAck {
            stop_id,
            agent_id: AgentId::from("ghost"),
            outcome: StopOutcome::NotRunning,
            final_push: None,
        })
    );
}

fn put(request: u128, agent: &str, name: &str) -> LinkFrame {
    LinkFrame::SecretPut(intent_core::node_link::SecretPut {
        request_id: intent_core::node_link::LinkId::from_u128(request),
        agent_id: AgentId::from(agent),
        name: name.into(),
        value: intent_core::node_link::SecretValue::from("canary-session-secret"),
    })
}

fn reply(frame: Option<LinkFrame>) -> intent_core::node_link::SecretReply {
    match frame {
        Some(LinkFrame::SecretReply(reply)) => reply,
        other => panic!("expected a secretReply, got {other:?}"),
    }
}

/// nodeProtocol 14 (node-link.md § Secrets on the machine): secret frames
/// hold, list and drop a value only for an agent on the binding's lease; a
/// replaced session changes nothing; a backend restart (a new
/// `headSession`) drops every value, and a final stop the agent's own.
#[tokio::test]
async fn secret_frames_hold_values_for_leased_agents_until_the_backend_restarts() {
    use intent_core::node_link::{SecretForget, SecretList, SecretOutcome};
    let h = harness();
    let b = binding(1, "lease-1");
    let a1 = AgentId::from("a1");
    let held = |agent: &AgentId| h.runner.head_link().secrets().names(agent);
    h.runner.adopt_head(Some("hs-1"), std::slice::from_ref(&a1));

    let stored = reply(apply_secret(&h.runner, &b, OPEN, put(1, "a1", "API_KEY")).await);
    assert_eq!(stored.outcome, SecretOutcome::Ok, "{stored:?}");
    assert_eq!(held(&a1), ["API_KEY"]);

    let stranger = reply(apply_secret(&h.runner, &b, OPEN, put(2, "a-stranger", "API_KEY")).await);
    assert_eq!(stranger.outcome, SecretOutcome::Failed);
    assert_eq!(stranger.error.as_deref(), Some("agent-not-on-lease"));
    assert!(held(&AgentId::from("a-stranger")).is_empty());

    let invalid = reply(apply_secret(&h.runner, &b, OPEN, put(3, "a1", "lower")).await);
    assert_eq!(invalid.error.as_deref(), Some("invalid-params"));
    assert!(!format!("{invalid:?}").contains("canary-session-secret"));

    let listed = reply(
        apply_secret(
            &h.runner,
            &b,
            OPEN,
            LinkFrame::SecretList(SecretList {
                request_id: intent_core::node_link::LinkId::from_u128(4),
                agent_id: a1.clone(),
            }),
        )
        .await,
    );
    assert_eq!(listed.names, Some(vec!["API_KEY".to_owned()]));

    assert!(
        apply_secret(&h.runner, &b, || true, put(5, "a1", "OTHER"))
            .await
            .is_none(),
        "a closed session answers and holds nothing"
    );
    assert_eq!(held(&a1), ["API_KEY"]);

    h.runner.adopt_head(Some("hs-1"), std::slice::from_ref(&a1));
    assert_eq!(
        held(&a1),
        ["API_KEY"],
        "a reconnect of the same backend keeps them"
    );
    h.runner.adopt_head(Some("hs-2"), std::slice::from_ref(&a1));
    assert!(held(&a1).is_empty(), "a restarted backend drops them");

    reply(apply_secret(&h.runner, &b, OPEN, put(6, "a1", "API_KEY")).await);
    let forget = |request: u128| {
        LinkFrame::SecretForget(SecretForget {
            request_id: intent_core::node_link::LinkId::from_u128(request),
            agent_id: a1.clone(),
            name: Some("API_KEY".into()),
        })
    };
    assert_eq!(
        reply(apply_secret(&h.runner, &b, OPEN, forget(7)).await).forgotten,
        Some(true)
    );
    assert_eq!(
        reply(apply_secret(&h.runner, &b, OPEN, forget(8)).await).forgotten,
        Some(false)
    );

    reply(apply_secret(&h.runner, &b, OPEN, put(9, "a1", "API_KEY")).await);
    h.runner.final_stop(&a1).await;
    assert!(
        held(&a1).is_empty(),
        "a final stop drops the agent's secrets"
    );
    let late = reply(apply_secret(&h.runner, &b, OPEN, put(10, "a1", "API_KEY")).await);
    assert_eq!(
        late.error.as_deref(),
        Some("agent-not-on-lease"),
        "a put still on its way after the final stop is refused"
    );
    assert!(held(&a1).is_empty());

    // A restored agent keeps its id: its launch lets it hold secrets again.
    h.runner
        .launch(match launch("a1") {
            LinkFrame::Launch(command) => command,
            _ => unreachable!(),
        })
        .await;
    h.runner.adopt_head(Some("hs-2"), std::slice::from_ref(&a1));
    let again = reply(apply_secret(&h.runner, &b, OPEN, put(11, "a1", "API_KEY")).await);
    assert_eq!(again.outcome, SecretOutcome::Ok, "{again:?}");
}

/// nodeProtocol 15: a `forgeToken` from a closed (replaced) session holds,
/// drops and answers nothing; a hello without a `headSession` drops it.
#[tokio::test]
async fn forge_token_frames_of_a_replaced_session_change_nothing() {
    use intent_core::node_link::{ForgeToken, LinkId, SecretOutcome, SecretValue};
    let h = harness();
    let b = binding(1, "lease-1");
    let frame = |token: Option<&str>| ForgeToken {
        request_id: LinkId::new_v4(),
        host: intent_core::node_link::FORGE_HOST_GITHUB.to_owned(),
        token: token.map(SecretValue::from),
    };
    h.runner.adopt_head(Some("hs-1"), &[]);
    let held = || {
        h.runner
            .head_link()
            .forge()
            .get()
            .map(|token| token.to_string())
    };
    let Some(LinkFrame::ForgeTokenReply(reply)) =
        apply_forge(&h.runner, &b, OPEN, frame(Some("ghp_one"))).await
    else {
        panic!("an open session answers");
    };
    assert_eq!(reply.outcome, SecretOutcome::Ok);
    assert_eq!(held().as_deref(), Some("ghp_one"));

    for token in [Some("ghp_two"), None] {
        assert!(apply_forge(&h.runner, &b, || true, frame(token))
            .await
            .is_none());
        assert_eq!(held().as_deref(), Some("ghp_one"), "{token:?}");
    }

    h.runner.adopt_head(None, &[]);
    assert_eq!(held(), None, "a hello without a session drops it");
}
