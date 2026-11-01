//! Runner behavior against real provider processes: the `node-test-agent.mjs`
//! fixture speaking ACP over stdio. Skips when `node` is not on PATH.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::executor::storage::Storage;

const PROVIDER: &str = "mock";

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(20))
}

/// The machine's shared limits as a plain counter, standing in for the
/// daemon's process registry.
struct CountAdmission {
    cap: u32,
    running: Mutex<Vec<AgentId>>,
}

impl Admission for CountAdmission {
    fn try_admit(&self, agent_id: &AgentId) -> Result<(), CapacityRefusal> {
        let mut running = lock(&self.running);
        let used = u32::try_from(running.len()).unwrap();
        if used >= self.cap {
            return Err(CapacityRefusal::Count {
                running_agents: used,
                agent_cap: self.cap,
            });
        }
        running.push(agent_id.clone());
        Ok(())
    }
    fn release(&self, agent_id: &AgentId) {
        lock(&self.running).retain(|id| id != agent_id);
    }
    fn capacity(&self) -> MachineCapacity {
        MachineCapacity {
            running_agents: u32::try_from(lock(&self.running).len()).unwrap(),
            agent_cap: self.cap,
            charged_memory_bytes: 0,
            memory_budget_bytes: None,
        }
    }
}

/// Only the fixture provider resolves.
struct FixtureLaunch(ProviderLaunch);

impl MachineLaunch for FixtureLaunch {
    fn resolve(&self, provider: &str) -> Result<ProviderLaunch, String> {
        if provider == PROVIDER {
            Ok(self.0.clone())
        } else {
            Err("not installed".into())
        }
    }
    fn policy(&self) -> PermissionPolicy {
        PermissionPolicy::AllowAll
    }
}

struct Harness {
    runner: Runner,
    admission: Arc<CountAdmission>,
    storage: Arc<Storage>,
    checkouts: Arc<crate::executor::checkout::PlainCheckouts>,
    /// The providers whose default account failed to authenticate, in
    /// report order.
    auth_failures: Arc<Mutex<Vec<String>>>,
    /// The node user's home a profile's `~` expands against.
    home: PathBuf,
    _dir: tempfile::TempDir,
}

/// A runner whose only provider runs the fixture agent with `args`, or
/// `None` (the test then skips) when `node` is unavailable.
fn harness(max_agents: u32, args: &[&str]) -> Option<Harness> {
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping node runner test: node not on PATH");
        return None;
    };
    let script = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut base_args: Vec<&'static str> = vec![Box::leak(script.into_boxed_str())];
    base_args.extend(
        args.iter()
            .map(|a| &*Box::leak((*a).to_owned().into_boxed_str())),
    );
    let config = ProviderConfig {
        base_args: Box::leak(base_args.into_boxed_slice()),
        ..*intent_providers::find_provider(PROVIDER).unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(&dir.path().join("node"), None).unwrap());
    let journal = Journal::open(&storage.journal_dir()).unwrap();
    let checkouts = crate::executor::checkout::PlainCheckouts::new(storage.clone());
    let admission = Arc::new(CountAdmission {
        cap: max_agents,
        running: Mutex::new(Vec::new()),
    });
    let auth_failures = Arc::new(Mutex::new(Vec::new()));
    let runner = Runner::new(RunnerConfig {
        admission: admission.clone(),
        launch: Arc::new(FixtureLaunch(ProviderLaunch {
            config,
            binary: Some(node),
            npx_fallback_binary: None,
            npx_fallback_package: None,
            extra_env: BTreeMap::new(),
        })),
        checkouts: checkouts.clone(),
        journal,
        mcp_bridge_exe: std::env::current_exe().unwrap(),
        home: Some(dir.path().join("home")),
        auth_failed: {
            let auth_failures = auth_failures.clone();
            Arc::new(move |provider: &str| lock(&auth_failures).push(provider.to_owned()))
        },
    });
    let home = dir.path().join("home");
    Some(Harness {
        runner,
        admission,
        storage,
        checkouts,
        auth_failures,
        home,
        _dir: dir,
    })
}

fn launch(id: &str) -> LaunchCommand {
    LaunchCommand {
        agent_id: AgentId::from(id),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: PROVIDER.into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: serde_json::json!({}),
        profile_env: None,
        is_sub_agent: false,
    }
}

fn text(prompt: &str) -> Vec<Value> {
    vec![json!({ "type": "text", "text": prompt })]
}

async fn records(runner: &Runner) -> Vec<JournalRecord> {
    runner
        .with_journal(|journal| journal.records_after(NodeCounter(0)))
        .await
        .unwrap()
}

/// Wait until the journal holds a record matching `found`; return all.
async fn wait_for(
    runner: &Runner,
    what: &str,
    found: impl Fn(&JournalRecord) -> bool,
) -> Vec<JournalRecord> {
    let mut appended = runner.appended();
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let all = records(runner).await;
        if all.iter().any(&found) {
            return all;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            tokio::time::timeout(remaining, appended.changed())
                .await
                .is_ok(),
            "timed out waiting for {what}; journal: {all:#?}"
        );
    }
}

fn exited(id: &str) -> impl Fn(&JournalRecord) -> bool + '_ {
    move |r| r.agent_id.0 == id && matches!(r.body, JournalBody::Exited { .. })
}

fn turn_ended<'a>(id: &'a str, turn: &'a str) -> impl Fn(&JournalRecord) -> bool + 'a {
    move |r| {
        r.agent_id.0 == id
            && r.turn_id.as_deref() == Some(turn)
            && matches!(r.body, JournalBody::TurnEnded { .. })
    }
}

fn find(all: &[JournalRecord], found: impl Fn(&JournalRecord) -> bool) -> &JournalRecord {
    all.iter().find(|r| found(r)).expect("record present")
}

fn exit_error(record: &JournalRecord) -> Option<&str> {
    match &record.body {
        JournalBody::Exited { error, .. } => error.as_deref(),
        other => panic!("not an exit: {other:?}"),
    }
}

fn turn_end(record: &JournalRecord) -> (&str, Option<&str>) {
    match &record.body {
        JournalBody::TurnEnded { stop_reason, error } => (stop_reason, error.as_deref()),
        other => panic!("not a turn end: {other:?}"),
    }
}

/// The fixture's pids, once it has started.
async fn agent_pids(storage: &Storage, id: &str) -> (i32, i32) {
    let path = storage
        .agent_dir(&AgentId::from(id))
        .unwrap()
        .join("agent.pid");
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(pids) = serde_json::from_slice::<Value>(&bytes) {
                let pid = |k: &str| i32::try_from(pids[k].as_i64().unwrap()).unwrap();
                return (pid("agent"), pid("child"));
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether `pid` has exited (a zombie awaiting its reaper counts).
fn gone(pid: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => stat
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z')),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
    }
}

async fn assert_gone(pid: i32) {
    let deadline = tokio::time::Instant::now() + wait();
    while !gone(pid) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "process {pid} still running"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn node_runner_runs_a_turn_and_stop_kills_the_process_tree() {
    let Some(h) = harness(2, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    // The turn's output is journaled before its end, tagged with the turn.
    let output = all
        .iter()
        .position(|r| {
            r.turn_id.as_deref() == Some("t1")
                && matches!(&r.body, JournalBody::Notification { notification }
                    if notification.to_string().contains("echo: hello"))
        })
        .expect("turn output journaled");
    let end = all.iter().position(turn_ended("a1", "t1")).unwrap();
    assert!(output < end, "{all:#?}");
    assert_eq!(turn_end(&all[end]), ("end_turn", None));
    assert_eq!(all[end].workspace_id, WorkspaceId::from("ws-1"));
    assert_eq!(h.runner.running(), vec![AgentId::from("a1")]);

    let (agent, child) = agent_pids(&h.storage, "a1").await;
    h.runner.stop(&AgentId::from("a1"));
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    assert_eq!(exit_error(find(&all, exited("a1"))), None);
    assert!(h.runner.running().is_empty());
    assert_gone(agent).await;
    assert_gone(child).await;
    // Sequences are contiguous from 1.
    let seqs: Vec<u64> = all.iter().map(|r| r.seq.0).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
}

/// The wait a stop returns resolves `stopped` only once the agent's whole
/// process tree is gone and its shared slot is free, so a stop
/// acknowledgement sent on it never confirms a still-running agent.
#[tokio::test]
async fn node_runner_stop_wait_resolves_only_once_the_agent_is_gone() {
    use intent_core::node_link::StopOutcome;
    let Some(h) = harness(2, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    let (agent, child) = agent_pids(&h.storage, "a1").await;
    let first = h.runner.stop(&AgentId::from("a1"));
    // A repeated stop while the first is still being applied waits too.
    let repeat = h.runner.stop(&AgentId::from("a1"));
    assert_eq!(
        tokio::time::timeout(wait(), first.wait()).await.unwrap(),
        Some(StopOutcome::Stopped)
    );
    // No polling: everything is already true when the wait resolves.
    assert!(gone(agent), "provider {agent} still running");
    assert!(gone(child), "helper {child} still running");
    assert!(h.runner.running().is_empty());
    assert!(lock(&h.admission.running).is_empty(), "slot not released");
    assert_eq!(
        tokio::time::timeout(wait(), repeat.wait()).await.unwrap(),
        Some(StopOutcome::Stopped)
    );
    // Once gone, a stop for the agent (or for one never launched) reports
    // that it is not running, at once.
    for id in ["a1", "ghost"] {
        assert_eq!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(500)),
                h.runner.stop(&AgentId::from(id)).wait()
            )
            .await
            .unwrap(),
            Some(StopOutcome::NotRunning),
            "{id}"
        );
    }
    wait_for(&h.runner, "exit", exited("a1")).await;
}

/// A `stopped` acknowledgement promises the exit record is journaled, so
/// when that append fails the wait settles without an outcome (no ack; the
/// backend resends). The agent is still torn down, and a later stop finds
/// it not running.
#[cfg(unix)]
#[tokio::test]
async fn node_runner_stop_wait_has_no_outcome_when_the_exit_record_is_not_journaled() {
    use std::os::unix::fs::PermissionsExt as _;

    use intent_core::node_link::StopOutcome;
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "start", started("a1")).await;
    let (agent, child) = agent_pids(&h.storage, "a1").await;
    let journal_dir = h.storage.journal_dir();
    let writable = std::fs::metadata(&journal_dir).unwrap().permissions();
    std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let outcome = tokio::time::timeout(wait(), h.runner.stop(&AgentId::from("a1")).wait())
        .await
        .unwrap();
    std::fs::set_permissions(&journal_dir, writable).unwrap();
    assert_eq!(outcome, None, "no exit record, so no stopped ack");
    assert!(gone(agent) && gone(child), "the agent is still torn down");
    assert!(lock(&h.admission.running).is_empty());
    assert_eq!(
        tokio::time::timeout(wait(), h.runner.stop(&AgentId::from("a1")).wait())
            .await
            .unwrap(),
        Some(StopOutcome::NotRunning)
    );
}

/// Only a journaled exit releases a successful stop wait: a lost exit
/// record, or a channel closed before the exit settled, gives no outcome.
#[tokio::test]
async fn a_stop_wait_succeeds_only_on_a_journaled_exit() {
    use intent_core::node_link::StopOutcome;
    let pending = |journaled| StopWait::Pending {
        outcome: StopOutcome::Stopped,
        journaled,
    };
    let (tx, rx) = watch::channel(ExitRecord::Pending);
    let wait = tokio::spawn(pending(rx).wait());
    tx.send_replace(ExitRecord::Lost);
    assert_eq!(wait.await.unwrap(), None, "a lost exit record");

    let (tx, rx) = watch::channel(ExitRecord::Pending);
    let wait = tokio::spawn(pending(rx).wait());
    drop(tx);
    assert_eq!(wait.await.unwrap(), None, "closed before the exit settled");

    let (tx, rx) = watch::channel(ExitRecord::Pending);
    let wait = tokio::spawn(pending(rx).wait());
    tx.send_replace(ExitRecord::Journaled);
    assert_eq!(wait.await.unwrap(), Some(StopOutcome::Stopped));
}

/// A journal pump that ends without an exit record settles the agent's
/// exit as lost (never journaled) and forgets it as exiting, so its stop
/// waits get no outcome and a later stop finds it not running.
#[tokio::test]
async fn a_pump_ending_without_an_exit_record_settles_it_as_lost() {
    use intent_core::node_link::StopOutcome;
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(&dir.path().join("node"), None).unwrap());
    let runner = Runner::new(RunnerConfig {
        admission: Arc::new(CountAdmission {
            cap: 1,
            running: Mutex::new(Vec::new()),
        }),
        launch: Arc::new(FixtureLaunch(ProviderLaunch {
            config: ProviderConfig {
                ..*intent_providers::find_provider(PROVIDER).unwrap()
            },
            binary: None,
            npx_fallback_binary: None,
            npx_fallback_package: None,
            extra_env: BTreeMap::new(),
        })),
        checkouts: crate::executor::checkout::PlainCheckouts::new(storage.clone()),
        journal: Journal::open(&storage.journal_dir()).unwrap(),
        mcp_bridge_exe: std::env::current_exe().unwrap(),
        home: None,
        auth_failed: Arc::new(|_: &str| {}),
    });
    let id = AgentId::from("a1");
    let (journaled, settled) = watch::channel(ExitRecord::Pending);
    lock(&runner.inner.state)
        .exiting
        .insert(id.clone(), settled.clone());
    let (notes_tx, notes) = mpsc::unbounded_channel();
    let (marks_tx, marks) = mpsc::unbounded_channel();
    drop((notes_tx, marks_tx));
    runner
        .clone()
        .pump(
            (id.clone(), WorkspaceId::from("ws-1")),
            (ActivityTracker::new(), Arc::new(Redactor::default())),
            notes,
            marks,
            journaled,
        )
        .await;
    assert_eq!(*settled.borrow(), ExitRecord::Lost);
    assert!(!lock(&runner.inner.state).exiting.contains_key(&id));
    assert_eq!(runner.stop(&id).wait().await, Some(StopOutcome::NotRunning));
}

/// A stop arriving while the agent is still being set up waits for it to be
/// torn down as well.
#[tokio::test]
async fn node_runner_stop_wait_covers_an_agent_stopped_during_setup() {
    use intent_core::node_link::StopOutcome;
    let Some(h) = harness(1, &["--hang-setup"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let (agent, child) = agent_pids(&h.storage, "a1").await;
    let outcome = tokio::time::timeout(wait(), h.runner.stop(&AgentId::from("a1")).wait())
        .await
        .unwrap();
    assert_eq!(outcome, Some(StopOutcome::Stopped));
    assert!(gone(agent) && gone(child));
    assert!(lock(&h.admission.running).is_empty());
}

#[tokio::test]
async fn node_runner_enforces_capacity_and_frees_the_slot_on_exit() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner.launch(launch("a2")).await;
    let all = wait_for(&h.runner, "capacity refusal", exited("a2")).await;
    let error = exit_error(find(&all, exited("a2"))).unwrap();
    assert!(error.starts_with(codes::CAPACITY), "{error}");
    assert!(error.contains("1 of 1"), "{error}");
    assert_eq!(h.runner.running(), vec![AgentId::from("a1")]);
    assert_eq!(h.runner.capacity().running_agents, 1);

    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "first exit", exited("a1")).await;
    assert!(
        lock(&h.admission.running).is_empty(),
        "the exit gave the shared slot back"
    );
    h.runner.launch(launch("a3")).await;
    h.runner
        .prompt(&AgentId::from("a3"), "t1".into(), text("hi"))
        .await;
    let all = wait_for(&h.runner, "turn on freed slot", turn_ended("a3", "t1")).await;
    assert_eq!(
        turn_end(find(&all, turn_ended("a3", "t1"))),
        ("end_turn", None)
    );
    h.runner.shutdown().await;
    assert!(h.runner.running().is_empty());
}

/// A launch the shared limits deny is refused at once with the structured
/// `node-capacity` failure the backend relays as `placement-unavailable`.
#[tokio::test]
async fn node_runner_refuses_over_the_shared_limits_with_the_structured_reason() {
    let Some(h) = harness(1, &[]) else { return };
    h.admission
        .try_admit(&AgentId::from("a-local-agent"))
        .unwrap();
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "capacity refusal", exited("a1")).await;
    match &find(&all, exited("a1")).body {
        JournalBody::Exited {
            error: Some(error),
            failure: Some(failure),
            ..
        } => {
            assert!(error.starts_with(codes::CAPACITY), "{error}");
            assert_eq!(failure.code, codes::CAPACITY);
            assert_eq!(
                failure.data,
                Some(CapacityRefusal::Count {
                    running_agents: 1,
                    agent_cap: 1,
                })
            );
        }
        other => panic!("expected a structured refusal, got {other:?}"),
    }
    assert!(h.runner.running().is_empty(), "the refusal was not queued");
}

#[tokio::test]
async fn node_runner_refuses_an_unconfigured_provider_by_name() {
    let Some(h) = harness(2, &[]) else { return };
    let mut command = launch("a1");
    command.provider = "codex".into();
    h.runner.launch(command).await;
    let all = wait_for(&h.runner, "provider refusal", exited("a1")).await;
    let error = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(error.starts_with(codes::PROVIDER), "{error}");
    assert!(error.contains("\"codex\""), "{error}");
    assert!(error.contains("not installed"), "{error}");
    assert!(h.runner.running().is_empty());
    assert!(
        lock(&h.admission.running).is_empty(),
        "a refused provider holds no shared slot"
    );
}

#[tokio::test]
async fn node_runner_journals_a_provider_that_dies_at_start() {
    let Some(h) = harness(1, &["--exit-on-start", "3"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let record = find(&all, exited("a1"));
    match &record.body {
        JournalBody::Exited {
            exit_code, error, ..
        } => assert!(
            *exit_code == Some(3)
                || error
                    .as_deref()
                    .is_some_and(|e| e.starts_with(codes::START)),
            "{record:?}"
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(all.iter().filter(|r| exited("a1")(r)).count(), 1);
    assert!(h.runner.running().is_empty());
}

#[tokio::test]
async fn node_runner_stop_during_setup_ends_waiting_turns() {
    let Some(h) = harness(1, &["--hang-setup"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let (agent, child) = agent_pids(&h.storage, "a1").await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    h.runner.stop(&AgentId::from("a1"));
    let all = wait_for(&h.runner, "waiting turn end", turn_ended("a1", "t1")).await;
    let (stop_reason, error) = turn_end(find(&all, turn_ended("a1", "t1")));
    assert_eq!(stop_reason, "refusal");
    assert!(error.unwrap().starts_with(codes::NOT_RUNNING), "{error:?}");
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    assert_eq!(exit_error(find(&all, exited("a1"))), None);
    assert_gone(agent).await;
    assert_gone(child).await;
}

#[tokio::test]
async fn node_runner_refuses_a_second_turn_and_cancels_the_first() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("block"))
        .await;
    wait_for(&h.runner, "first turn output", |r| {
        r.turn_id.as_deref() == Some("t1") && matches!(r.body, JournalBody::Notification { .. })
    })
    .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("second"))
        .await;
    let all = wait_for(&h.runner, "busy refusal", turn_ended("a1", "t2")).await;
    let (stop_reason, error) = turn_end(find(&all, turn_ended("a1", "t2")));
    assert_eq!(stop_reason, "refusal");
    assert!(error.unwrap().starts_with(codes::BUSY), "{error:?}");

    h.runner.cancel(&AgentId::from("a1")).await;
    let all = wait_for(&h.runner, "cancelled turn", turn_ended("a1", "t1")).await;
    assert_eq!(
        turn_end(find(&all, turn_ended("a1", "t1"))),
        ("cancelled", None)
    );

    // The agent takes turns again after the cancelled one.
    h.runner
        .prompt(&AgentId::from("a1"), "t3".into(), text("again"))
        .await;
    let all = wait_for(&h.runner, "next turn", turn_ended("a1", "t3")).await;
    assert_eq!(
        turn_end(find(&all, turn_ended("a1", "t3"))),
        ("end_turn", None)
    );
    h.runner.shutdown().await;
}

#[tokio::test]
async fn node_runner_answers_a_prompt_for_an_exited_agent() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t9".into(), text("late"))
        .await;
    let all = wait_for(&h.runner, "late turn end", turn_ended("a1", "t9")).await;
    let (_, error) = turn_end(find(&all, turn_ended("a1", "t9")));
    assert!(error.unwrap().starts_with(codes::NOT_RUNNING), "{error:?}");
    // An agent never launched here is not journaled at all.
    let before = records(&h.runner).await.len();
    h.runner
        .prompt(&AgentId::from("ghost"), "t1".into(), text("?"))
        .await;
    h.runner.stop(&AgentId::from("ghost"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(records(&h.runner).await.len(), before);
}

#[tokio::test]
async fn node_runner_tags_every_chunk_of_a_turn_even_when_the_journal_lags() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    // 201 notifications arrive faster than they are fsync'd; the turn ends
    // (and the next one starts) while most are still queued.
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("many"))
        .await;
    wait_for(&h.runner, "first turn end", turn_ended("a1", "t1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("next"))
        .await;
    let all = wait_for(&h.runner, "second turn end", turn_ended("a1", "t2")).await;
    let tag = |r: &JournalRecord| match &r.body {
        JournalBody::Notification { notification } => {
            let text = notification.to_string();
            if text.contains("chunk ") || text.contains("echo: many") {
                Some(("t1", r.turn_id.clone()))
            } else if text.contains("echo: next") {
                Some(("t2", r.turn_id.clone()))
            } else {
                None
            }
        }
        _ => None,
    };
    let tags: Vec<_> = all.iter().filter_map(tag).collect();
    assert_eq!(tags.len(), 202, "{tags:?}");
    for (expected, actual) in &tags {
        assert_eq!(actual.as_deref(), Some(*expected), "{tags:?}");
    }
    // Each turn's output precedes its end.
    let last_t1 = all
        .iter()
        .rposition(|r| {
            r.turn_id.as_deref() == Some("t1") && matches!(r.body, JournalBody::Notification { .. })
        })
        .unwrap();
    assert!(last_t1 < all.iter().position(turn_ended("a1", "t1")).unwrap());
    h.runner.shutdown().await;
}

#[tokio::test]
async fn node_runner_kills_leftover_children_when_the_provider_exits_by_itself() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    let (agent, child) = agent_pids(&h.storage, "a1").await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("exit"))
        .await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    match &find(&all, exited("a1")).body {
        JournalBody::Exited {
            exit_code, error, ..
        } => {
            assert_eq!((*exit_code, error.as_deref()), (Some(0), None));
        }
        other => panic!("{other:?}"),
    }
    assert_gone(agent).await;
    assert_gone(child).await;
    assert!(h.runner.running().is_empty());
}

#[tokio::test]
async fn node_runner_kills_a_helper_in_its_own_process_group_after_a_natural_exit() {
    let Some(h) = harness(1, &["--detached-helper"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let (agent, helper) = agent_pids(&h.storage, "a1").await;
    // Once the provider exits, the helper is reparented and no longer its
    // descendant; the runner must already know it.
    let deadline = tokio::time::Instant::now() + wait();
    while !h
        .runner
        .tracked_descendants(&AgentId::from("a1"))
        .contains(&helper)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "helper never tracked"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("exit"))
        .await;
    wait_for(&h.runner, "exit", exited("a1")).await;
    assert_gone(agent).await;
    assert_gone(helper).await;
}

/// The provider starts a helper in its own process group during startup and
/// exits on its first prompt, before any periodic snapshot can have run.
/// Nothing here waits on the tracker.
#[tokio::test]
async fn node_runner_kills_a_startup_helper_when_the_provider_exits_on_its_first_prompt() {
    let Some(h) = harness(1, &["--detached-helper"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("exit"))
        .await;
    let (agent, helper) = agent_pids(&h.storage, "a1").await;
    wait_for(&h.runner, "exit", exited("a1")).await;
    assert_gone(agent).await;
    assert_gone(helper).await;
}

/// A helper started in its own process group in the middle of a session,
/// immediately before the provider exits, is seen by no snapshot; on Linux
/// the run marker in its inherited environment still identifies it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn node_runner_kills_a_helper_started_just_before_the_provider_exits() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("spawn-exit"))
        .await;
    wait_for(&h.runner, "exit", exited("a1")).await;
    let path = h
        .storage
        .agent_dir(&AgentId::from("a1"))
        .unwrap()
        .join("late-helper.pid");
    let pid: i32 = std::fs::read_to_string(&path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_gone(pid).await;
}

#[tokio::test]
async fn node_runner_refuses_launches_once_shutdown_began() {
    let Some(h) = harness(2, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner.shutdown().await;
    h.runner.launch(launch("a2")).await;
    let all = wait_for(&h.runner, "refusal", exited("a2")).await;
    let error = exit_error(find(&all, exited("a2"))).unwrap();
    assert!(error.starts_with(codes::SHUTTING_DOWN), "{error}");
    assert!(h.runner.running().is_empty());
    assert!(
        !h.storage
            .agent_dir(&AgentId::from("a2"))
            .unwrap()
            .join("agent.pid")
            .exists(),
        "no provider was started"
    );
}

#[tokio::test]
async fn node_runner_shutdown_waits_until_queued_output_and_exit_are_journaled() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("many"))
        .await;
    // Stop as soon as the burst starts arriving, with most of it queued.
    wait_for(&h.runner, "first output", |r| {
        matches!(r.body, JournalBody::Notification { .. })
    })
    .await;
    h.runner.shutdown().await;
    let all = records(&h.runner).await;
    assert_eq!(
        all.iter().filter(|r| exited("a1")(r)).count(),
        1,
        "the exit is journaled before shutdown returns: {all:#?}"
    );
    // Nothing is appended after shutdown returned.
    let count = all.len();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(records(&h.runner).await.len(), count);
}

#[test]
fn node_test_agent_fixture_exists() {
    assert!(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/node-test-agent.mjs"
    ))
    .is_file());
}

fn started(id: &str) -> impl Fn(&JournalRecord) -> bool + '_ {
    move |r| r.agent_id.0 == id && matches!(r.body, JournalBody::Started { .. })
}

/// Wait until the runner asked for `count` WIP pushes.
async fn wait_pushes(h: &Harness, count: usize) -> Vec<AgentId> {
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let pushes = h.checkouts.pushes.lock().unwrap().clone();
        if pushes.len() >= count {
            return pushes;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {count} WIP pushes, got {pushes:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A launched agent reports `Started` (with the launched commit) once its
/// checkout and session are ready, before any turn output; its work is
/// pushed back after every turn and once more when it exits, before head
/// hears of the exit.
#[tokio::test]
async fn node_runner_reports_started_and_pushes_wip_after_turns_and_on_exit() {
    let Some(h) = harness(2, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "started", started("a1")).await;
    let record = find(&all, started("a1"));
    assert!(
        matches!(&record.body, JournalBody::Started { base_commit } if base_commit == "base"),
        "{record:#?}"
    );
    assert!(h.checkouts.pushes.lock().unwrap().is_empty());

    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    let start = all.iter().position(started("a1")).unwrap();
    let first_output = all
        .iter()
        .position(|r| r.turn_id.as_deref() == Some("t1"))
        .unwrap();
    assert!(start < first_output, "{all:#?}");
    assert_eq!(wait_pushes(&h, 1).await, vec![AgentId::from("a1")]);

    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
    assert_eq!(
        h.checkouts.pushes.lock().unwrap().len(),
        2,
        "the exit push happens before the exit is journaled"
    );
}

/// A checkout head cannot serve ends the launch with `node-checkout-failed`
/// and no `Started`, and frees the slot.
#[tokio::test]
async fn node_runner_reports_a_failed_checkout_as_the_agents_exit() {
    let Some(h) = harness(1, &[]) else { return };
    h.checkouts
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let error = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(error.starts_with(codes::CHECKOUT), "{error}");
    assert!(error.contains("head refused the fetch"), "{error}");
    assert!(!all.iter().any(started("a1")), "{all:#?}");
    assert!(h.runner.running().is_empty());
    assert!(h.checkouts.pushes.lock().unwrap().is_empty());
}

/// The text of the turn's `wsapi` line (the fixture's `workspace_api` result).
fn wsapi_line(all: &[JournalRecord], turn: &str) -> String {
    all.iter()
        .filter(|r| r.turn_id.as_deref() == Some(turn))
        .find_map(|r| match &r.body {
            JournalBody::Notification { notification } => notification["update"]["content"]["text"]
                .as_str()
                .filter(|text| text.starts_with("wsapi"))
                .map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no wsapi output in turn {turn}: {all:#?}"))
}

/// A launched agent's provider gets the workspace MCP server in
/// `session/new`: its file calls land in its own working directory on the
/// node, and a namespace the node cannot serve fails by name.
#[tokio::test]
async fn node_runner_gives_each_agent_its_workspace_api_on_the_node() {
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("w1");
    h.runner.launch(launch("w1")).await;
    h.runner
        .prompt(
            &id,
            "t1".into(),
            text(r#"wsapi return JSON.stringify(await ws.file.write("made.txt", "on the node"));"#),
        )
        .await;
    let all = wait_for(&h.runner, "first turn end", turn_ended("w1", "t1")).await;
    let line = wsapi_line(&all, "t1");
    assert!(line.starts_with("wsapi: "), "{line}");
    let dir = h.storage.agent_dir(&id).unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.join("made.txt")).unwrap(),
        "on the node"
    );

    h.runner
        .prompt(
            &id,
            "t2".into(),
            text("wsapi return await ws.script.list();"),
        )
        .await;
    let all = wait_for(&h.runner, "second turn end", turn_ended("w1", "t2")).await;
    let line = wsapi_line(&all, "t2");
    assert!(line.starts_with("wsapi-error: "), "{line}");
    assert!(
        line.contains("is not supported on remote agents yet"),
        "{line}"
    );
    h.runner.stop(&id);
    wait_for(&h.runner, "exit", exited("w1")).await;
}

const CANARY: &str = "canary-profile-secret-5b1e";
/// A seven-byte credential: masked because of its variable's name.
const SHORT_SECRET: &str = "s3cr3t!";

/// A launch with the selected profile's overlay, as head sends it.
fn launch_with_profile(id: &str, env: &Value) -> LaunchCommand {
    LaunchCommand {
        profile_env: Some(intent_core::node_link::LaunchProfileEnv(json!({
            "providerRef": "codex@work",
            "fingerprint": "f1",
            "env": env,
        }))),
        ..launch(id)
    }
}

/// The fixture's environment, once it has started with `--dump-env`.
async fn agent_env(storage: &Storage, id: &str) -> serde_json::Map<String, Value> {
    let path = storage
        .agent_dir(&AgentId::from(id))
        .unwrap()
        .join("agent.env.json");
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(Value::Object(env)) = serde_json::from_slice::<Value>(&bytes) {
                return env;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every log line written while the returned guard is held (this thread's
/// runtime only: run the test on a current-thread runtime).
fn capture_logs() -> (Arc<Mutex<Vec<u8>>>, tracing::subscriber::DefaultGuard) {
    #[derive(Clone)]
    struct Sink(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            lock(&self.0).extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let logs = Arc::new(Mutex::new(Vec::new()));
    let sink = Sink(logs.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
}

#[tokio::test]
async fn node_runner_applies_the_profile_with_local_launch_precedence_and_node_home() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    let home = &h.home;
    std::fs::create_dir_all(home.join(".codex-work")).unwrap();
    let inherited = std::env::vars()
        .map(|(key, _)| key)
        .find(|key| key != "PATH" && key != "HOME")
        .expect("an inherited variable");
    h.runner
        .launch(launch_with_profile(
            "a1",
            &json!([
                { "key": "CODEX_HOME", "value": "~/.codex-work" },
                { "key": "CODEX_ACCESS_TOKEN", "value": CANARY },
                { "key": "EMPTY_VALUE", "value": "" },
                { "key": inherited, "value": null },
                { "key": RUN_MARKER_ENV, "value": "profile-cannot-override-wiring" },
                { "key": "PATH", "value": "/profile/bin" },
            ]),
        ))
        .await;
    let env = agent_env(&h.storage, "a1").await;
    // `~` is the node's home, never head's.
    assert_eq!(
        env["CODEX_HOME"],
        home.join(".codex-work").to_string_lossy().as_ref()
    );
    // The profile's values replace inherited ones, empty values are real
    // values, and `unset` removes an inherited variable.
    assert_eq!(env["CODEX_ACCESS_TOKEN"], CANARY);
    assert_eq!(env["EMPTY_VALUE"], "");
    assert!(!env.contains_key(&inherited), "{inherited} kept");
    // Intent's own wiring wins over the profile, as in a local launch.
    assert_ne!(env[RUN_MARKER_ENV], "profile-cannot-override-wiring");
    assert_ne!(env["PATH"], "/profile/bin");
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
}

#[tokio::test]
async fn node_runner_without_a_profile_keeps_the_node_login_environment() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let env = agent_env(&h.storage, "a1").await;
    for (key, value) in std::env::vars() {
        if key != "PATH" && !key.starts_with("NODE_") && key != RUN_MARKER_ENV {
            assert_eq!(
                env.get(&key).and_then(Value::as_str),
                Some(value.as_str()),
                "{key}"
            );
        }
    }
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
}

#[tokio::test]
async fn node_runner_fails_a_launch_whose_profile_folder_is_missing_without_leaking_it() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    let (logs, _guard) = capture_logs();
    for (id, key) in [("a1", "CODEX_HOME"), ("a2", "CLAUDE_CONFIG_DIR")] {
        h.runner
            .launch(launch_with_profile(
                id,
                &json!([
                    { "key": key, "value": "~/.missing-account" },
                    { "key": "CODEX_ACCESS_TOKEN", "value": CANARY },
                ]),
            ))
            .await;
        let all = wait_for(&h.runner, "profile refusal", exited(id)).await;
        let error = exit_error(find(&all, exited(id))).unwrap();
        assert!(error.starts_with(codes::PROFILE), "{error}");
        assert!(error.contains(key), "{error}");
        assert!(error.contains("this node"), "{error}");
        assert!(error.contains("no other account was used"), "{error}");
        assert!(!error.contains(".missing-account"), "{error}");
    }
    // Never started: no provider ran on the node's own login instead.
    assert!(h.runner.running().is_empty());
    for id in ["a1", "a2"] {
        let dir = h.storage.agent_dir(&AgentId::from(id)).unwrap();
        assert!(!dir.join("agent.env.json").exists(), "{id} started");
    }
    let journal = serde_json::to_string(&records(&h.runner).await).unwrap();
    assert!(!journal.contains(CANARY), "{journal}");
    let logs = String::from_utf8(lock(&logs).clone()).unwrap();
    assert!(!logs.contains(CANARY), "{logs}");
}

#[tokio::test]
async fn node_runner_refuses_a_malformed_profile_overlay() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    let mut command = launch_with_profile("a1", &json!([]));
    command.profile_env = Some(intent_core::node_link::LaunchProfileEnv(json!({
        "providerRef": "codex@work",
        "env": [{ "key": "CODEX_ACCESS_TOKEN", "value": CANARY }],
    })));
    h.runner.launch(command).await;
    let all = wait_for(&h.runner, "profile refusal", exited("a1")).await;
    let error = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(error.starts_with(codes::PROFILE), "{error}");
    assert!(!error.contains(CANARY), "{error}");
    assert!(h.runner.running().is_empty());
}

#[tokio::test]
async fn node_runner_keeps_profile_secrets_out_of_logs_and_the_journal() {
    let Some(h) = harness(1, &["--dump-env"]) else {
        return;
    };
    let (logs, _guard) = capture_logs();
    h.runner
        .launch(launch_with_profile(
            "a1",
            &json!([{ "key": "CODEX_ACCESS_TOKEN", "value": CANARY }]),
        ))
        .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    assert_eq!(
        agent_env(&h.storage, "a1").await["CODEX_ACCESS_TOKEN"],
        CANARY
    );
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
    let journal = serde_json::to_string(&records(&h.runner).await).unwrap();
    assert!(!journal.contains(CANARY), "{journal}");
    let logs = String::from_utf8(lock(&logs).clone()).unwrap();
    assert!(!logs.contains(CANARY), "{logs}");
}

/// A provider that echoes a profile credential in its setup error, its
/// streamed output and its turn error: the journal carries none of it, and
/// short values and account folders are left alone.
#[tokio::test]
async fn node_runner_redacts_profile_secrets_a_provider_echoes() {
    for secret in [CANARY, SHORT_SECRET] {
        for (args, prompt) in [
            (&["--fail-setup-echo", "CODEX_ACCESS_TOKEN"][..], None),
            (&[][..], Some("echo-env CODEX_ACCESS_TOKEN")),
        ] {
            let Some(h) = harness(1, args) else {
                return;
            };
            let (logs, _guard) = capture_logs();
            h.runner
                .launch(launch_with_profile(
                    "a1",
                    &json!([
                        { "key": "CODEX_ACCESS_TOKEN", "value": secret },
                        { "key": "SHORT_FLAG", "value": "1" },
                    ]),
                ))
                .await;
            if let Some(prompt) = prompt {
                h.runner
                    .prompt(&AgentId::from("a1"), "t1".into(), text(prompt))
                    .await;
                let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
                let (_, error) = turn_end(find(&all, turn_ended("a1", "t1")));
                let error = error.expect("the turn failed");
                assert!(error.contains("rejected credential [redacted]"), "{error}");
                h.runner.stop(&AgentId::from("a1"));
            }
            let all = wait_for(&h.runner, "exit", exited("a1")).await;
            let journal = serde_json::to_string(&all).unwrap();
            if prompt.is_none() {
                let error = exit_error(find(&all, exited("a1"))).expect("setup failed");
                assert!(error.contains("[redacted]"), "{error}");
            } else {
                assert!(journal.contains("token [redacted]"), "{journal}");
            }
            assert!(!journal.contains(secret), "{secret}: {journal}");
            let logs = String::from_utf8(lock(&logs).clone()).unwrap();
            assert!(!logs.contains(secret), "{secret}: {logs}");
        }
    }
}

#[test]
fn the_redactor_masks_credentials_and_long_values_but_not_short_settings_or_account_folders() {
    let profile = ProfileEnv::new(
        "codex@work",
        "f1",
        vec![
            (
                "CODEX_ACCESS_TOKEN".into(),
                Some("abcdef-secret-long".into()),
            ),
            ("INNER".into(), Some("secret-long".into())),
            ("SHORT_FLAG".into(), Some("1".into())),
            ("SERVICE_PASSWORD".into(), Some("pw1".into())),
            ("CODEX_HOME".into(), Some("/home/node/.codex-work".into())),
            ("REMOVED".into(), None),
        ],
    );
    let redactor = Redactor::new(Some(&profile));
    assert_eq!(
        redactor.text("a abcdef-secret-long b secret-long 1 pw1 /home/node/.codex-work".into()),
        "a [redacted] b [redacted] 1 [redacted] /home/node/.codex-work"
    );
    let mut value = json!({
        "k": ["x secret-long"],
        "secret-long": 1,
        "rawOutput": { "secret-long": "secret-long" },
    });
    redactor.value(&mut value);
    assert_eq!(
        value,
        json!({
            "k": ["x [redacted]"],
            "secret-long": 1,
            "rawOutput": { "[redacted]": "[redacted]" },
        })
    );
    let none = Redactor::new(None);
    assert_eq!(none.text("secret-long".into()), "secret-long");
}

/// A short credential that collides with ACP vocabulary (`text`) masks the
/// provider's prose but never its field names or variant tags, so head can
/// still parse every update.
#[tokio::test]
async fn node_runner_masking_a_schema_word_keeps_updates_parseable() {
    let Some(h) = harness(1, &[]) else {
        return;
    };
    h.runner
        .launch(launch_with_profile(
            "a1",
            &json!([{ "key": "CODEX_ACCESS_TOKEN", "value": "text" }]),
        ))
        .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello text"))
        .await;
    let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    let notifications: Vec<&Value> = all
        .iter()
        .filter_map(|r| match &r.body {
            JournalBody::Notification { notification } => Some(notification),
            _ => None,
        })
        .collect();
    assert!(!notifications.is_empty(), "{all:#?}");
    for notification in notifications {
        let mapped = intent_acp::session::map_notification(&intent_acp::IncomingNotification {
            method: "session/update".into(),
            params: notification.clone(),
        });
        assert!(mapped.is_some(), "unparseable update: {notification}");
        assert_eq!(
            notification["update"]["content"],
            json!({ "type": "text", "text": "echo: hello [redacted]" })
        );
    }
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
}

/// Identifiers stay only at their ACP schema positions: a `status` or `type`
/// inside a tool's raw input or output is data and is masked, while the
/// update itself still parses on head.
#[test]
fn the_redactor_masks_tool_payloads_but_keeps_schema_identifiers() {
    for secret in ["s3cr3t!", "completed", "text"] {
        let profile = ProfileEnv::new(
            "codex@work",
            "f1",
            vec![("CODEX_ACCESS_TOKEN".into(), Some(secret.into()))],
        );
        let redactor = Redactor::new(Some(&profile));
        let mut params = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": "call-1",
                "status": "completed",
                "kind": "execute",
                "title": format!("login {secret}"),
                "content": [{
                    "type": "content",
                    "content": { "type": "text", "text": format!("rejected credential {secret}") },
                }],
                "rawInput": { "status": format!("token {secret}"), "type": secret },
                "rawOutput": {
                    "status": format!("rejected credential {secret}"),
                    "nested": [{ "type": secret, secret: 1 }],
                },
            },
        });
        redactor.notification(&mut params);
        let update = &params["update"];
        assert_eq!(update["sessionUpdate"], "tool_call_update", "{secret}");
        assert_eq!(update["status"], "completed", "{secret}");
        assert_eq!(update["content"][0]["type"], "content", "{secret}");
        assert_eq!(update["content"][0]["content"]["type"], "text", "{secret}");
        assert_eq!(
            update["content"][0]["content"]["text"],
            "rejected credential [redacted]"
        );
        assert_eq!(update["title"], "login [redacted]");
        assert_eq!(
            update["rawInput"],
            json!({ "status": "token [redacted]", "type": "[redacted]" })
        );
        assert_eq!(
            update["rawOutput"],
            json!({
                "status": "rejected credential [redacted]",
                "nested": [{ "type": "[redacted]", "[redacted]": 1 }],
            })
        );
        let mapped = intent_acp::session::map_notification(&intent_acp::IncomingNotification {
            method: "session/update".into(),
            params: params.clone(),
        });
        assert!(mapped.is_some(), "{secret}: unparseable update {params}");
    }
}

/// Outside a tool's free-form `rawInput` / `rawOutput` / `_meta`, field names
/// are never rewritten: an embedded resource and a tool call's locations
/// keep their required keys, and the update still parses, even when the
/// credential is the field name itself.
#[test]
fn the_redactor_keeps_resource_and_location_keys() {
    for secret in ["text", "path", "uri"] {
        let profile = ProfileEnv::new(
            "codex@work",
            "f1",
            vec![("CODEX_ACCESS_TOKEN".into(), Some(secret.into()))],
        );
        let redactor = Redactor::new(Some(&profile));
        let mut resource = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {
                    "type": "resource",
                    "resource": {
                        "uri": "file:///notes.md",
                        "mimeType": "text/markdown",
                        "text": format!("leaked {secret}"),
                    },
                },
            },
        });
        redactor.notification(&mut resource);
        let embedded = &resource["update"]["content"]["resource"];
        assert_eq!(
            embedded["text"], "leaked [redacted]",
            "{secret}: {resource}"
        );
        assert!(embedded.get("uri").is_some(), "{secret}: {resource}");
        let mut tool = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": "call-1",
                "title": "edit",
                "kind": "edit",
                "status": "pending",
                "locations": [{ "path": format!("/work/{secret}.md"), "line": 3 }],
                "rawInput": { secret: format!("value {secret}") },
                "_meta": { secret: 1 },
            },
        });
        redactor.notification(&mut tool);
        let update = &tool["update"];
        assert_eq!(update["locations"][0]["path"], "/work/[redacted].md");
        assert_eq!(
            update["rawInput"],
            json!({ "[redacted]": "value [redacted]" })
        );
        assert_eq!(update["_meta"], json!({ "[redacted]": 1 }));
        for (what, params) in [("resource", &resource), ("tool", &tool)] {
            let mapped = intent_acp::session::map_notification(&intent_acp::IncomingNotification {
                method: "session/update".into(),
                params: params.clone(),
            });
            assert!(mapped.is_some(), "{secret}: unparseable {what} {params}");
        }
        assert_eq!(
            tool["update"]["locations"].as_array().map(Vec::len),
            Some(1),
            "{secret}"
        );
    }
}

/// A launch without a profile whose provider refuses `session/new` as not
/// signed in reports the provider's default account as failed; a profile
/// launch's failure says nothing about the default account, and neither
/// does a setup failure that is not about authentication.
#[tokio::test]
async fn node_runner_reports_default_account_auth_failures_at_setup() {
    for (args, profile, reported) in [
        (&["--auth-required-setup"][..], false, true),
        (&["--auth-required-setup"][..], true, false),
        (&["--fail-setup-echo", "HOME"][..], false, false),
    ] {
        let Some(h) = harness(1, args) else {
            return;
        };
        let command = if profile {
            launch_with_profile("a1", &json!([{ "key": "SHORT_FLAG", "value": "1" }]))
        } else {
            launch("a1")
        };
        h.runner.launch(command).await;
        let all = wait_for(&h.runner, "exit", exited("a1")).await;
        assert!(exit_error(find(&all, exited("a1"))).is_some(), "{args:?}");
        let expected: Vec<String> = if reported {
            vec![PROVIDER.to_owned()]
        } else {
            Vec::new()
        };
        assert_eq!(
            *lock(&h.auth_failures),
            expected,
            "{args:?} profile={profile}"
        );
    }
}

/// A turn the provider refuses as not signed in reports the default
/// account as failed, unless the agent runs under a profile.
#[tokio::test]
async fn node_runner_reports_default_account_auth_failures_on_a_turn() {
    for (profile, prompt, reported) in [
        (false, "auth-required", true),
        (true, "auth-required", false),
        (false, "echo-env HOME", false),
    ] {
        let Some(h) = harness(1, &[]) else { return };
        let command = if profile {
            launch_with_profile("a1", &json!([{ "key": "SHORT_FLAG", "value": "1" }]))
        } else {
            launch("a1")
        };
        h.runner.launch(command).await;
        h.runner
            .prompt(&AgentId::from("a1"), "t1".into(), text(prompt))
            .await;
        let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
        let (_, error) = turn_end(find(&all, turn_ended("a1", "t1")));
        assert!(error.is_some(), "{prompt}: the turn failed");
        let expected: Vec<String> = if reported {
            vec![PROVIDER.to_owned()]
        } else {
            Vec::new()
        };
        assert_eq!(
            *lock(&h.auth_failures),
            expected,
            "{prompt} profile={profile}"
        );
        h.runner.stop(&AgentId::from("a1"));
        wait_for(&h.runner, "exit", exited("a1")).await;
    }
}

/// Review of intentd#2910: the periodic tracker and the setup snapshot run
/// concurrently. A snapshot that started earlier must never replace one that
/// started later, so an older periodic scan finishing after the setup scan
/// cannot erase a detached helper the setup scan captured.
#[test]
fn an_older_descendant_snapshot_never_replaces_a_newer_one() {
    let descendants = Descendants::default();
    let periodic = descendants.begin();
    let setup = descendants.begin();
    descendants.publish(setup, vec![10, 11]);
    descendants.publish(periodic, vec![10]);
    assert_eq!(
        descendants.pids(),
        vec![10, 11],
        "the setup snapshot's helper is kept"
    );

    // A later snapshot still replaces it: nothing accumulates.
    let next = descendants.begin();
    descendants.publish(next, vec![12]);
    assert_eq!(descendants.pids(), vec![12]);
}
